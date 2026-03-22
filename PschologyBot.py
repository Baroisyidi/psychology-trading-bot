import os
import json
import asyncio
import random
import time
import threading
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple, List, Dict
from dotenv import load_dotenv
from pybit.unified_trading import HTTP, WebSocket
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)
from aiohttp import web

# ────────────────────────────────────────────────
#  Load environment variables
# ────────────────────────────────────────────────
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
CHAT_ID_STR = os.getenv("CHAT_ID")

if not all([TELEGRAM_TOKEN, BYBIT_API_KEY, BYBIT_API_SECRET, CHAT_ID_STR]):
    raise ValueError("Missing required environment variables! Check .env or Render Environment Variables.")

try:
    CHAT_ID = int(CHAT_ID_STR)
except ValueError:
    raise ValueError(f"CHAT_ID must be an integer, got: {CHAT_ID_STR}")

# ────────────────────────────────────────────────
#  Bybit demo session
# ────────────────────────────────────────────────
bybit_session = HTTP(
    demo=True,
    api_key=BYBIT_API_KEY,
    api_secret=BYBIT_API_SECRET,
    recv_window=5000
)

# ────────────────────────────────────────────────
#  TEMPORARY DEBUG: Check time difference
# ────────────────────────────────────────────────
try:
    resp = bybit_session.get_server_time()
    server_ms = int(resp["result"]["timeNano"])
    local_ms = int(time.time() * 1_000_000_000)
    diff = abs(local_ms - server_ms) / 1_000_000_000
    print(f"Time diff: {diff:.2f} seconds")
    print("Time sync looks good" if diff <= 5 else "WARNING: Possible drift/latency (>5 seconds)")
except Exception as e:
    print(f"Debug failed: {e}")

# ────────────────────────────────────────────────
#  WebSocket for real-time price feeds
# ────────────────────────────────────────────────
ws_prices: dict[str, float] = {}
ws_prices_lock = threading.Lock()
ws_connection = None

def _ws_ticker_callback(message):
    """WebSocket callback for real-time ticker updates. Runs in WS background thread."""
    try:
        data = message.get("data", {})
        if isinstance(data, list):
            for item in data:
                sym = item.get("symbol", "")
                price = item.get("lastPrice", "")
                if sym and price:
                    with ws_prices_lock:
                        ws_prices[sym] = float(price)
        elif isinstance(data, dict):
            sym = data.get("symbol", "")
            price = data.get("lastPrice", "")
            if sym and price:
                with ws_prices_lock:
                    ws_prices[sym] = float(price)
    except Exception as e:
        print(f"[WS] Ticker callback error: {e}")

def start_websocket():
    """Initialize the Bybit WebSocket connection for real-time price feeds."""
    global ws_connection
    try:
        ws_connection = WebSocket(
            testnet=False,
            channel_type="linear",
        )
        print("[WS] WebSocket connection initialized for real-time price feeds")
        return True
    except Exception as e:
        print(f"[WS] Failed to start WebSocket: {e}")
        return False

def subscribe_ws_symbol(symbol: str):
    """Subscribe to real-time ticker updates for a symbol via WebSocket."""
    global ws_connection
    if ws_connection is None:
        if not start_websocket():
            return False
    try:
        ws_connection.ticker_stream(symbol=symbol, callback=_ws_ticker_callback)
        print(f"[WS] Subscribed to {symbol} ticker stream")
        return True
    except Exception as e:
        print(f"[WS] Failed to subscribe {symbol}: {e}")
        return False

def get_ws_price(symbol: str) -> Optional[float]:
    """Get the latest WebSocket price for a symbol. Returns None if not available."""
    with ws_prices_lock:
        return ws_prices.get(symbol)


# ────────────────────────────────────────────────
#  Strategy Constants (from BybitStrategyBacktester_v29)
# ────────────────────────────────────────────────
# Indicator periods
EMA_FAST = 8
EMA_MID = 21
EMA_SLOW = 50
EMA_TREND = 100
RSI_PERIOD = 14
STOCH_RSI_PERIOD = 14
STOCH_RSI_SMOOTH = 3
BB_PERIOD = 20
BB_STD = 2.0
VOLUME_MA_PERIOD = 20
ROC_PERIOD = 10
CONSOLIDATION_BARS = 15

# Default tunable parameters (v29 strategy)
DEFAULT_PARAMS = {
    # Stop Loss
    "sl_atr": 2.5,
    # Trailing Stop
    "trail_atr": 1.8,
    "trail_strong": 2.2,
    "trail_vstrong": 2.8,
    "trail_tighten_tp1": 0.85,
    "trail_tighten_tp2": 0.70,
    # Take Profit
    "tp0_atr": 0.7,
    "tp1_atr": 1.5,
    "tp2_atr": 3.5,
    "tp3_atr": 6.0,
    "tp0_pct": 0.10,
    "tp1_pct": 0.20,
    "tp2_pct": 0.20,
    "tp3_pct": 0.20,
    # Entry thresholds
    "sig_threshold": 5,
    "sig_strong": 4,
    "sig_vstrong": 3,
    # ADX thresholds
    "adx_entry": 18,
    "adx_strong": 28,
    "adx_vstrong": 42,
    "adx_4h_regime": 16,
    # Cooldown
    "cooldown_bars": 2,
    "cooldown_loss": 4,
    "cooldown_streak": 8,
    # RSI thresholds
    "rsi_ob": 72,
    "rsi_os": 28,
    "rsi_mr_high": 75,
    "rsi_mr_low": 25,
    # BB squeeze
    "bb_squeeze": 0.03,
    # Equity management
    "dd_reduce": 20.0,
    "dd_small": 30.0,
    "dd_pause": 40.0,
    "size_reduced": 0.5,
    "size_small": 0.25,
    # Time-based exit
    "flat_bars": 15,
    "flat_threshold": 0.3,
    # Max loss cap
    "max_loss": 4.5,
    # Win streak
    "win_streak_bonus": 0.15,
    # Volatility floor
    "min_atr_pct": 0.3,
    # Mean reversion position size
    "mr_size": 0.6,
}

# Per-Coin Deep-Optimized Overrides (from v29 grid search)
COIN_OVERRIDES = {
    "TURTLEUSDT": {
        "sl_atr": 3.0, "trail_atr": 1.575, "trail_strong": 2.2, "trail_vstrong": 2.8,
        "trail_tighten_tp1": 0.95, "trail_tighten_tp2": 0.70,
        "tp0_atr": 0.2, "tp1_atr": 1.5, "tp2_atr": 3.5, "tp3_atr": 6.0,
        "tp0_pct": 0.10, "tp1_pct": 0.20, "tp2_pct": 0.20, "tp3_pct": 0.20,
        "sig_threshold": 2, "sig_strong": 4, "sig_vstrong": 3,
        "adx_entry": 18, "adx_4h_regime": 16,
        "cooldown_bars": 0, "cooldown_loss": 4, "cooldown_streak": 8,
        "max_loss": 4.75, "dd_pause": 60.0, "dd_small": 30.0, "dd_reduce": 20.0,
        "win_streak_bonus": 0.15, "min_atr_pct": 0.3,
    },
    "DEXEUSDT": {
        "sl_atr": 6.0, "trail_atr": 2.2, "trail_strong": 5.0, "trail_vstrong": 6.0,
        "tp0_atr": 2.0, "tp1_atr": 4.0, "tp2_atr": 8.0, "tp3_atr": 15.0,
        "sig_threshold": 3, "sig_strong": 3, "sig_vstrong": 2,
        "adx_entry": 18, "adx_4h_regime": 16,
        "max_loss": 8.0, "dd_pause": 80.0, "dd_small": 60.0, "dd_reduce": 40.0,
        "win_streak_bonus": 0.15, "min_atr_pct": 0.3,
    },
    "RDNTUSDT": {
        "sl_atr": 1.0, "trail_atr": 1.89, "trail_strong": 2.2, "trail_vstrong": 2.8,
        "tp0_atr": 0.2, "tp1_atr": 3.0, "tp2_atr": 3.5, "tp3_atr": 6.0,
        "sig_threshold": 5, "sig_strong": 4, "sig_vstrong": 3,
        "adx_entry": 10, "adx_4h_regime": 16,
        "max_loss": 10.0, "dd_pause": 40.0, "dd_small": 30.0, "dd_reduce": 20.0,
        "win_streak_bonus": 0.15, "min_atr_pct": 0.3,
    },
}

POLL_INTERVAL_SECONDS = 10  # Poll every 10 seconds

def get_symbol_params(symbol: str) -> dict:
    """Get the tunable parameters for a symbol, with per-coin overrides."""
    params = DEFAULT_PARAMS.copy()
    if symbol in COIN_OVERRIDES:
        params.update(COIN_OVERRIDES[symbol])
    return params


# ────────────────────────────────────────────────
#  Global state
# ────────────────────────────────────────────────
symbol_trends: dict[str, Optional[str]] = {}
monitored_symbols: list[str] = []
symbol_open_trade_side: dict[str, Optional[str]] = {}
symbol_manual_quantities: dict[str, Optional[float]] = {}

# Per-symbol risk management tracking
symbol_entry_price: dict[str, Optional[float]] = {}
symbol_entry_atr: dict[str, Optional[float]] = {}
symbol_peak_price: dict[str, Optional[float]] = {}       # highest (long) or lowest (short) since entry
symbol_lowest_since_entry: dict[str, Optional[float]] = {}
symbol_highest_since_entry: dict[str, Optional[float]] = {}
symbol_trailing_sl: dict[str, Optional[float]] = {}      # current trailing stop price
symbol_sl_price: dict[str, Optional[float]] = {}          # current stop-loss price
symbol_server_sl_set: dict[str, bool] = {}

# Take-profit tracking
symbol_tp0_hit: dict[str, bool] = {}
symbol_tp1_hit: dict[str, bool] = {}
symbol_tp2_hit: dict[str, bool] = {}
symbol_tp3_hit: dict[str, bool] = {}
symbol_position_size_pct: dict[str, float] = {}  # remaining position as fraction of original (1.0 = full)
symbol_original_qty: dict[str, Optional[float]] = {}

# Cooldown & streak tracking
symbol_cooldown: dict[str, int] = {}
symbol_consecutive_losses: dict[str, int] = {}
symbol_consecutive_wins: dict[str, int] = {}
symbol_bars_in_trade: dict[str, int] = {}
symbol_last_candle_ts: dict[str, Optional[str]] = {}  # track candle changes for bar counting

# Equity curve tracking
symbol_peak_equity: dict[str, float] = {}


# ────────────────────────────────────────────────
#  State Persistence (JSON)
# ────────────────────────────────────────────────
STATE_FILE = os.getenv("STATE_FILE", "/opt/render/project/src/bot_state.json")

def save_state():
    """Save all bot state to JSON file for crash recovery."""
    try:
        state = {
            "symbol_trends": symbol_trends,
            "monitored_symbols": monitored_symbols,
            "symbol_open_trade_side": symbol_open_trade_side,
            "symbol_manual_quantities": symbol_manual_quantities,
            "symbol_entry_price": symbol_entry_price,
            "symbol_entry_atr": symbol_entry_atr,
            "symbol_peak_price": symbol_peak_price,
            "symbol_lowest_since_entry": symbol_lowest_since_entry,
            "symbol_highest_since_entry": symbol_highest_since_entry,
            "symbol_trailing_sl": symbol_trailing_sl,
            "symbol_sl_price": symbol_sl_price,
            "symbol_server_sl_set": symbol_server_sl_set,
            "symbol_tp0_hit": symbol_tp0_hit,
            "symbol_tp1_hit": symbol_tp1_hit,
            "symbol_tp2_hit": symbol_tp2_hit,
            "symbol_tp3_hit": symbol_tp3_hit,
            "symbol_position_size_pct": symbol_position_size_pct,
            "symbol_original_qty": symbol_original_qty,
            "symbol_cooldown": symbol_cooldown,
            "symbol_consecutive_losses": symbol_consecutive_losses,
            "symbol_consecutive_wins": symbol_consecutive_wins,
            "symbol_bars_in_trade": symbol_bars_in_trade,
            "symbol_last_candle_ts": symbol_last_candle_ts,
            "symbol_peak_equity": symbol_peak_equity,
        }
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2, default=str)
        print(f"[STATE] Saved state to {STATE_FILE}")
    except Exception as e:
        print(f"[STATE] Failed to save state: {e}")

def load_state():
    """Load bot state from JSON file on startup."""
    global monitored_symbols
    if not os.path.exists(STATE_FILE):
        print(f"[STATE] No state file found at {STATE_FILE}, starting fresh")
        return False
    try:
        with open(STATE_FILE, "r") as f:
            state = json.load(f)

        symbol_trends.update(state.get("symbol_trends", {}))
        loaded_symbols = state.get("monitored_symbols", [])
        for s in loaded_symbols:
            if s not in monitored_symbols:
                monitored_symbols.append(s)
        symbol_open_trade_side.update(state.get("symbol_open_trade_side", {}))
        symbol_manual_quantities.update(state.get("symbol_manual_quantities", {}))
        symbol_entry_price.update(state.get("symbol_entry_price", {}))
        symbol_entry_atr.update(state.get("symbol_entry_atr", {}))
        symbol_peak_price.update(state.get("symbol_peak_price", {}))
        symbol_lowest_since_entry.update(state.get("symbol_lowest_since_entry", {}))
        symbol_highest_since_entry.update(state.get("symbol_highest_since_entry", {}))
        symbol_trailing_sl.update(state.get("symbol_trailing_sl", {}))
        symbol_sl_price.update(state.get("symbol_sl_price", {}))
        symbol_server_sl_set.update(state.get("symbol_server_sl_set", {}))
        symbol_tp0_hit.update(state.get("symbol_tp0_hit", {}))
        symbol_tp1_hit.update(state.get("symbol_tp1_hit", {}))
        symbol_tp2_hit.update(state.get("symbol_tp2_hit", {}))
        symbol_tp3_hit.update(state.get("symbol_tp3_hit", {}))
        symbol_position_size_pct.update({k: float(v) for k, v in state.get("symbol_position_size_pct", {}).items()})
        symbol_original_qty.update(state.get("symbol_original_qty", {}))
        symbol_cooldown.update({k: int(v) for k, v in state.get("symbol_cooldown", {}).items()})
        symbol_consecutive_losses.update({k: int(v) for k, v in state.get("symbol_consecutive_losses", {}).items()})
        symbol_consecutive_wins.update({k: int(v) for k, v in state.get("symbol_consecutive_wins", {}).items()})
        symbol_bars_in_trade.update({k: int(v) for k, v in state.get("symbol_bars_in_trade", {}).items()})
        symbol_last_candle_ts.update(state.get("symbol_last_candle_ts", {}))
        symbol_peak_equity.update({k: float(v) for k, v in state.get("symbol_peak_equity", {}).items()})

        print(f"[STATE] Loaded state from {STATE_FILE} — {len(monitored_symbols)} symbols restored")
        return True
    except Exception as e:
        print(f"[STATE] Failed to load state: {e}")
        return False


# ────────────────────────────────────────────────
#  Motivational Quotes
# ────────────────────────────────────────────────
MOTIVATIONAL_QUOTES = [
    "\u201cThe market doesn\u2019t reward intelligence \U0001f9e0 \u2014 it rewards discipline \U0001f9f1.\nSmart traders survive, disciplined traders compound.\u201d",
    "\u201cConsistency \U0001f501 beats luck \U0001f3b2 every single time.\nWhat you repeat daily becomes your edge.\u201d",
    "\u201cYour greatest edge isn\u2019t your strategy \U0001f4ca \u2014 it\u2019s your psychology \U0001f9e0.\nMaster your mind, and the market follows.\u201d",
    "\u201cEvery loss is tuition \U0001f393.\nEvery lesson learned compounds into future strength \U0001f4c8.\u201d",
    "\u201cControl your emotions \U0001f3ad, and you control your outcomes \U0001f3af.\nLose emotional control, and the market decides for you.\u201d",
    "\u201cTrading is a marathon \U0001f3c3\u200d\u2642\ufe0f, not a sprint \u26a1.\nPatience is the fuel that finishes the race.\u201d",
    "\u201cSurvive first \U0001f6e1\ufe0f. Profit later \U0001f4b0.\nWinners are simply those who stayed long enough to win.\u201d",
    "\u201cThe market tests your discipline \U0001f9ea before it rewards your skill \U0001f3c6.\nPass the test, earn the payout.\u201d",
    "\u201cYou don\u2019t need to predict the market \U0001f52e \u2014\nYou need to react correctly \u2699\ufe0f when it reveals itself.\u201d",
    "\u201cMissed trades cost nothing \U0001f6ab.\nBad trades drain capital, confidence, and clarity \U0001f4a5.\u201d",
    "\u201cBoring, repeatable execution \U0001f504 is where real money is made \U0001f48e.\nExcitement is expensive.\u201d",
    "\u201cYour job isn\u2019t to win today \U0001f5d3\ufe0f \u2014\nIt\u2019s to still be profitable next year \U0001f4c6.\u201d",
    "\u201cEmotional trading feels good short-term \U0001f60c\nand destroys accounts long-term \U0001f4a3.\u201d",
    "\u201cThe best traders think in probabilities \U0001f3af, not predictions \U0001f3b2.\nCertainty is an illusion.\u201d",
    "\u201cCapital preservation \U0001f6e1\ufe0f is the first form of profit \U0001f4b0.\nProtect the account, and growth follows.\u201d",
    "\u201cDiscipline means doing the right thing \U0001f9f1\neven when every emotion tells you not to \U0001f624.\u201d",
    "\u201cThe market rewards patience \u23f3 more than bravery \U0001f981.\nWaiting is a position.\u201d",
    "\u201cOne good system \u2699\ufe0f executed flawlessly\nbeats ten impulsive ideas \U0001f4ad every time.\u201d",
    "\u201cIf you can wait \u23f8\ufe0f, you can win \U0001f3c6.\nTime favors the disciplined.\u201d",
    "\u201cProfessionals focus on process \U0001f6e0\ufe0f.\nAmateurs obsess over outcomes \U0001f3ad.\u201d"
]

async def send_motivational_quote(context: ContextTypes.DEFAULT_TYPE) -> None:
    quote = random.choice(MOTIVATIONAL_QUOTES)
    chat_id = context.job.chat_id if context.job else CHAT_ID
    await context.bot.send_message(chat_id=chat_id, text=quote)

async def get_current_price(symbol: str) -> Optional[float]:
    """Get current price: WebSocket first, REST ticker fallback."""
    ws_price = get_ws_price(symbol)
    if ws_price is not None:
        return ws_price
    try:
        ticker_resp = await asyncio.to_thread(
            bybit_session.get_tickers,
            category="linear",
            symbol=symbol
        )
        return float(ticker_resp["result"]["list"][0]["lastPrice"])
    except Exception as e:
        print(f"Error getting current price for {symbol}: {e}")
        return None


async def get_all_indicators(
    symbol: str,
    interval: str = "30",
    limit: int = 200,
) -> Optional[dict]:
    """
    Returns a dict with all v29 trading indicators computed from 30-min Bybit klines:
    - ATR, ATR%, EMAs (8/21/50/100), EMA slopes
    - DMI (+DI/-DI), ADX
    - RSI, Stochastic RSI (K/D)
    - MACD histogram + prev
    - Bollinger Bands (upper/lower/width/width_min)
    - Volume ratio, ROC, VWAP
    - trend_score (-4 to +4)
    - swing_high, swing_low (20-bar)
    - current_price (live), last_close, last_candle_ts
    """
    try:
        resp = await asyncio.to_thread(
            bybit_session.get_kline,
            category="linear",
            symbol=symbol,
            interval=interval,
            limit=limit,
        )
        klines = resp["result"]["list"]
        if not klines:
            raise ValueError("No klines returned")

        df = pd.DataFrame(
            klines,
            columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"],
        ).iloc[::-1].reset_index(drop=True)

        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        if df["close"].isna().any():
            raise ValueError("Invalid prices")

        # ATR (14)
        df["prev_close"] = df["close"].shift(1)
        df["tr1"] = df["high"] - df["low"]
        df["tr2"] = (df["high"] - df["prev_close"]).abs()
        df["tr3"] = (df["low"] - df["prev_close"]).abs()
        df["tr"] = df[["tr1", "tr2", "tr3"]].max(axis=1)
        df["atr"] = df["tr"].ewm(alpha=1/14, min_periods=14).mean()
        df["atr_pct"] = (df["atr"] / df["close"]) * 100

        # EMAs
        df["ema_fast"] = df["close"].ewm(span=EMA_FAST, adjust=False).mean()
        df["ema_mid"] = df["close"].ewm(span=EMA_MID, adjust=False).mean()
        df["ema_slow"] = df["close"].ewm(span=EMA_SLOW, adjust=False).mean()
        df["ema_trend"] = df["close"].ewm(span=EMA_TREND, adjust=False).mean()
        df["ema_fast_slope"] = (df["ema_fast"] - df["ema_fast"].shift(5)) / df["ema_fast"].shift(5) * 100

        # DMI / ADX
        df["up_move"] = df["high"] - df["high"].shift(1)
        df["down_move"] = df["low"].shift(1) - df["low"]
        df["+dm"] = 0.0
        df["-dm"] = 0.0
        plus_mask = (df["up_move"] > df["down_move"]) & (df["up_move"] > 0)
        minus_mask = (df["down_move"] > df["up_move"]) & (df["down_move"] > 0)
        df.loc[plus_mask, "+dm"] = df.loc[plus_mask, "up_move"]
        df.loc[minus_mask, "-dm"] = df.loc[minus_mask, "down_move"]
        df["+dm_s"] = df["+dm"].ewm(alpha=1/14, min_periods=14).mean()
        df["-dm_s"] = df["-dm"].ewm(alpha=1/14, min_periods=14).mean()
        df["+di"] = (df["+dm_s"] / df["atr"]) * 100
        df["-di"] = (df["-dm_s"] / df["atr"]) * 100
        di_sum = df["+di"] + df["-di"]
        di_sum = di_sum.replace(0, float("nan"))
        df["dx"] = (abs(df["+di"] - df["-di"]) / di_sum) * 100
        df["adx"] = df["dx"].ewm(alpha=1/14, min_periods=14).mean()

        # RSI
        delta = df["close"].diff()
        gain = delta.where(delta > 0, 0.0)
        loss_vals = (-delta.where(delta < 0, 0.0))
        avg_gain = gain.ewm(alpha=1/RSI_PERIOD, min_periods=RSI_PERIOD).mean()
        avg_loss = loss_vals.ewm(alpha=1/RSI_PERIOD, min_periods=RSI_PERIOD).mean()
        rs = avg_gain / avg_loss.replace(0, float("nan"))
        df["rsi"] = 100 - (100 / (1 + rs))

        # Stochastic RSI
        rsi_min = df["rsi"].rolling(STOCH_RSI_PERIOD).min()
        rsi_max = df["rsi"].rolling(STOCH_RSI_PERIOD).max()
        rsi_range = rsi_max - rsi_min
        rsi_range = rsi_range.replace(0, float("nan"))
        df["stoch_rsi_k"] = ((df["rsi"] - rsi_min) / rsi_range * 100).rolling(STOCH_RSI_SMOOTH).mean()
        df["stoch_rsi_d"] = df["stoch_rsi_k"].rolling(STOCH_RSI_SMOOTH).mean()

        # MACD
        df["macd_ema12"] = df["close"].ewm(span=12, adjust=False).mean()
        df["macd_ema26"] = df["close"].ewm(span=26, adjust=False).mean()
        df["macd_line"] = df["macd_ema12"] - df["macd_ema26"]
        df["macd_signal"] = df["macd_line"].ewm(span=9, adjust=False).mean()
        df["macd_hist"] = df["macd_line"] - df["macd_signal"]
        df["macd_hist_prev"] = df["macd_hist"].shift(1)

        # Bollinger Bands
        df["bb_mid"] = df["close"].rolling(BB_PERIOD).mean()
        df["bb_std"] = df["close"].rolling(BB_PERIOD).std()
        df["bb_upper"] = df["bb_mid"] + BB_STD * df["bb_std"]
        df["bb_lower"] = df["bb_mid"] - BB_STD * df["bb_std"]
        df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / df["bb_mid"]
        df["bb_width_min"] = df["bb_width"].rolling(CONSOLIDATION_BARS).min()

        # Volume
        df["vol_ma"] = df["volume"].rolling(VOLUME_MA_PERIOD).mean()
        df["vol_ratio"] = df["volume"] / df["vol_ma"].replace(0, float("nan"))

        # ROC
        df["roc"] = (df["close"] - df["close"].shift(ROC_PERIOD)) / df["close"].shift(ROC_PERIOD) * 100

        # VWAP (48-bar rolling)
        typical_price = (df["high"] + df["low"] + df["close"]) / 3
        cum_tp_vol = (typical_price * df["volume"]).rolling(48).sum()
        cum_vol = df["volume"].rolling(48).sum().replace(0, float("nan"))
        df["vwap"] = cum_tp_vol / cum_vol

        # Trend score
        df["trend_score"] = 0
        df.loc[df["ema_fast"] > df["ema_mid"], "trend_score"] += 1
        df.loc[df["ema_mid"] > df["ema_slow"], "trend_score"] += 1
        df.loc[df["ema_slow"] > df["ema_trend"], "trend_score"] += 1
        df.loc[df["close"] > df["ema_fast"], "trend_score"] += 1
        df.loc[df["ema_fast"] < df["ema_mid"], "trend_score"] -= 1
        df.loc[df["ema_mid"] < df["ema_slow"], "trend_score"] -= 1
        df.loc[df["ema_slow"] < df["ema_trend"], "trend_score"] -= 1
        df.loc[df["close"] < df["ema_fast"], "trend_score"] -= 1

        # Swing high/low (20-bar)
        df["swing_high"] = df["high"].rolling(20, center=False).max()
        df["swing_low"] = df["low"].rolling(20, center=False).min()

        # Previous values for crossover detection
        df["prev_ema_fast"] = df["ema_fast"].shift(1)
        df["prev_ema_mid"] = df["ema_mid"].shift(1)

        # Extract last row values (second-to-last = last CLOSED candle)
        last = df.iloc[-2]  # Last closed candle
        curr_forming = df.iloc[-1]  # Currently forming candle

        last_candle_ts = str(df["timestamp"].iloc[-1])
        last_close = float(last["close"])

        current_price = await get_current_price(symbol)
        if current_price is None:
            current_price = last_close

        def safe(val):
            return float(val) if not pd.isna(val) else None

        return {
            "atr": safe(last["atr"]),
            "atr_pct": safe(last["atr_pct"]),
            "ema_fast": safe(last["ema_fast"]),
            "ema_mid": safe(last["ema_mid"]),
            "ema_slow": safe(last["ema_slow"]),
            "ema_trend": safe(last["ema_trend"]),
            "ema_fast_slope": safe(last["ema_fast_slope"]),
            "prev_ema_fast": safe(last["prev_ema_fast"]),
            "prev_ema_mid": safe(last["prev_ema_mid"]),
            "plus_di": safe(last["+di"]),
            "minus_di": safe(last["-di"]),
            "adx": safe(last["adx"]),
            "rsi": safe(last["rsi"]) or 50.0,
            "stoch_rsi_k": safe(last["stoch_rsi_k"]) or 50.0,
            "stoch_rsi_d": safe(last["stoch_rsi_d"]) or 50.0,
            "macd_hist": safe(last["macd_hist"]) or 0.0,
            "macd_hist_prev": safe(last["macd_hist_prev"]) or 0.0,
            "bb_upper": safe(last["bb_upper"]),
            "bb_lower": safe(last["bb_lower"]),
            "bb_width": safe(last["bb_width"]),
            "bb_width_min": safe(last["bb_width_min"]),
            "vol_ratio": safe(last["vol_ratio"]) or 1.0,
            "roc": safe(last["roc"]) or 0.0,
            "vwap": safe(last["vwap"]),
            "trend_score": int(last["trend_score"]) if not pd.isna(last["trend_score"]) else 0,
            "swing_high": safe(last["swing_high"]),
            "swing_low": safe(last["swing_low"]),
            "current_price": current_price,
            "last_close": last_close,
            "last_candle_ts": last_candle_ts,
        }

    except Exception as exc:
        print(f"Error in get_all_indicators for {symbol}: {exc}")
        return None


async def compute_higher_tf(symbol: str) -> dict:
    """
    Compute higher-timeframe trend signals (1H and 4H).
    Returns trend direction for each TF based on EMA20/50 + MACD.
    """
    result = {"htf_1h_trend": 0, "htf_4h_trend": 0}
    for interval, key in [("60", "htf_1h_trend"), ("240", "htf_4h_trend")]:
        try:
            resp = await asyncio.to_thread(
                bybit_session.get_kline,
                category="linear",
                symbol=symbol,
                interval=interval,
                limit=100,
            )
            klines = resp["result"]["list"]
            if not klines or len(klines) < 60:
                continue

            df = pd.DataFrame(
                klines,
                columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"],
            ).iloc[::-1].reset_index(drop=True)
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            ema20 = df["close"].ewm(span=20, adjust=False).mean()
            ema50 = df["close"].ewm(span=50, adjust=False).mean()
            macd_line = df["close"].ewm(span=12, adjust=False).mean() - df["close"].ewm(span=26, adjust=False).mean()
            macd_signal = macd_line.ewm(span=9, adjust=False).mean()

            last = df.iloc[-2]  # Last closed candle
            last_idx = len(df) - 2
            trend = 0
            if ema20.iloc[last_idx] > ema50.iloc[last_idx]:
                trend += 1
            else:
                trend -= 1
            if macd_line.iloc[last_idx] > macd_signal.iloc[last_idx]:
                trend += 1
            else:
                trend -= 1
            result[key] = trend
        except Exception as e:
            print(f"[HTF] Error computing {interval}m for {symbol}: {e}")
    return result


# ────────────────────────────────────────────────
#  Infrastructure: Position & Order Management (preserved from v14)
# ────────────────────────────────────────────────

def get_open_position_side(symbol: str) -> Optional[str]:
    """Check Bybit for an open position. Returns 'Buy', 'Sell', or None."""
    try:
        resp = bybit_session.get_positions(category="linear", symbol=symbol, settleCoin="USDT")
        positions = resp.get("result", {}).get("list", [])
        for pos in positions:
            size = float(pos.get("size", 0))
            if size > 0:
                return pos.get("side")
        return None
    except Exception as e:
        print(f"Error getting position side for {symbol}: {e}")
        return None

def get_instrument_constraints(symbol: str) -> dict:
    """Fetch instrument tick size and lot size constraints from Bybit."""
    try:
        resp = bybit_session.get_instruments_info(category="linear", symbol=symbol)
        instruments = resp["result"]["list"]
        if not instruments:
            return {}
        info = instruments[0]
        lot_filter = info.get("lotSizeFilter", {})
        price_filter = info.get("priceFilter", {})
        return {
            "qtyStep": lot_filter.get("qtyStep", "0.001"),
            "minOrderQty": lot_filter.get("minOrderQty", "0.001"),
            "tickSize": price_filter.get("tickSize", "0.01"),
        }
    except Exception as e:
        print(f"Error getting instrument constraints for {symbol}: {e}")
        return {}

def round_qty_to_step(qty: float, step: float) -> float:
    """Round quantity down to the nearest valid step."""
    if step <= 0:
        return qty
    return float(int(qty / step) * step)

def format_qty_str(qty: float, step: str) -> str:
    """Format quantity string to match step precision."""
    decimals = len(step.rstrip('0').split('.')[-1]) if '.' in step else 0
    return f"{qty:.{decimals}f}"

def get_trade_qty(symbol: str, side: str, manual_qty: Optional[float] = None) -> Optional[str]:
    """Calculate the order quantity based on 95% of available wallet balance (or manual qty)."""
    try:
        constraints = get_instrument_constraints(symbol)
        if not constraints:
            return None

        step = float(constraints["qtyStep"])
        min_qty = float(constraints["minOrderQty"])
        step_str = constraints["qtyStep"]

        if manual_qty is not None:
            qty = round_qty_to_step(manual_qty, step)
            if qty < min_qty:
                print(f"[{symbol}] Manual qty {manual_qty} below minimum {min_qty}")
                return None
            return format_qty_str(qty, step_str)

        # Get current price
        ws_price = get_ws_price(symbol)
        if ws_price:
            price = ws_price
        else:
            try:
                ticker = bybit_session.get_tickers(category="linear", symbol=symbol)
                price = float(ticker["result"]["list"][0]["lastPrice"])
            except Exception:
                return None

        # Get available balance
        try:
            balance_resp = bybit_session.get_wallet_balance(accountType="UNIFIED", coin="USDT")
            coins = balance_resp["result"]["list"][0].get("coin", [])
            available = 0.0
            for c in coins:
                if c["coin"] == "USDT":
                    available = float(c.get("availableToWithdraw", 0))
                    break
        except Exception:
            return None

        notional = available * 0.95
        qty = notional / price
        qty = round_qty_to_step(qty, step)

        if qty < min_qty:
            print(f"[{symbol}] Calculated qty {qty} below minimum {min_qty}")
            return None

        return format_qty_str(qty, step_str)

    except Exception as e:
        print(f"Error calculating qty for {symbol}: {e}")
        return None

async def close_position(symbol: str, side: str, bot=None) -> bool:
    """Close an open position entirely via market order."""
    try:
        close_side = "Sell" if side == "Buy" else "Buy"
        resp = bybit_session.get_positions(category="linear", symbol=symbol, settleCoin="USDT")
        positions = resp.get("result", {}).get("list", [])
        qty_str = None
        for pos in positions:
            if pos.get("side") == side and float(pos.get("size", 0)) > 0:
                qty_str = pos["size"]
                break

        if not qty_str:
            print(f"[{symbol}] No open {side} position to close")
            return False

        result = bybit_session.place_order(
            category="linear",
            symbol=symbol,
            side=close_side,
            orderType="Market",
            qty=qty_str,
            reduceOnly=True,
        )
        if result.get("retCode") == 0:
            print(f"[{symbol}] Closed {side} position ({qty_str})")
            return True
        else:
            print(f"[{symbol}] Failed to close position: {result}")
            return False
    except Exception as e:
        print(f"[{symbol}] Error closing position: {e}")
        return False

async def close_partial_position(symbol: str, side: str, fraction: float, bot=None) -> bool:
    """Close a fraction of an open position via market order."""
    try:
        close_side = "Sell" if side == "Buy" else "Buy"
        resp = bybit_session.get_positions(category="linear", symbol=symbol, settleCoin="USDT")
        positions = resp.get("result", {}).get("list", [])
        total_size = 0.0
        for pos in positions:
            if pos.get("side") == side and float(pos.get("size", 0)) > 0:
                total_size = float(pos["size"])
                break

        if total_size <= 0:
            return False

        constraints = get_instrument_constraints(symbol)
        step = float(constraints.get("qtyStep", "0.001"))
        step_str = constraints.get("qtyStep", "0.001")
        min_qty = float(constraints.get("minOrderQty", "0.001"))

        close_qty = round_qty_to_step(total_size * fraction, step)
        if close_qty < min_qty:
            close_qty = min_qty
        if close_qty > total_size:
            close_qty = total_size

        qty_str = format_qty_str(close_qty, step_str)

        result = bybit_session.place_order(
            category="linear",
            symbol=symbol,
            side=close_side,
            orderType="Market",
            qty=qty_str,
            reduceOnly=True,
        )
        if result.get("retCode") == 0:
            print(f"[{symbol}] Partial close {side}: {qty_str} ({fraction*100:.0f}%)")
            return True
        else:
            print(f"[{symbol}] Failed partial close: {result}")
            return False
    except Exception as e:
        print(f"[{symbol}] Error partial close: {e}")
        return False


# ────────────────────────────────────────────────
#  Risk State Management
# ────────────────────────────────────────────────

def _clear_risk_state(symbol: str):
    """Clear all per-symbol risk/trade tracking state."""
    symbol_entry_price[symbol] = None
    symbol_entry_atr[symbol] = None
    symbol_peak_price[symbol] = None
    symbol_lowest_since_entry[symbol] = None
    symbol_highest_since_entry[symbol] = None
    symbol_trailing_sl[symbol] = None
    symbol_sl_price[symbol] = None
    symbol_server_sl_set[symbol] = False
    symbol_tp0_hit[symbol] = False
    symbol_tp1_hit[symbol] = False
    symbol_tp2_hit[symbol] = False
    symbol_tp3_hit[symbol] = False
    symbol_position_size_pct[symbol] = 1.0
    symbol_original_qty[symbol] = None
    symbol_bars_in_trade[symbol] = 0
    symbol_last_candle_ts[symbol] = None

def _set_entry_risk_state(symbol: str, entry_price: float, atr: float, side: str, qty: float, params: dict):
    """Initialize risk state when entering a new trade."""
    symbol_entry_price[symbol] = entry_price
    symbol_entry_atr[symbol] = atr
    symbol_peak_price[symbol] = entry_price
    symbol_lowest_since_entry[symbol] = entry_price
    symbol_highest_since_entry[symbol] = entry_price
    symbol_original_qty[symbol] = qty
    symbol_position_size_pct[symbol] = 1.0
    symbol_bars_in_trade[symbol] = 0
    symbol_tp0_hit[symbol] = False
    symbol_tp1_hit[symbol] = False
    symbol_tp2_hit[symbol] = False
    symbol_tp3_hit[symbol] = False

    # Compute initial SL
    sl_dist = atr * params["sl_atr"]
    if side == "Buy":
        raw_sl = entry_price - sl_dist
        # Structural SL: use swing low if available
        swing_low = symbol_lowest_since_entry.get(symbol, entry_price)
        if swing_low and swing_low < entry_price:
            structural_sl = swing_low - atr * 0.3
            raw_sl = max(raw_sl, structural_sl)
        # Max loss cap
        max_loss_pct = params["max_loss"] / 100
        floor_sl = entry_price * (1 - max_loss_pct)
        sl_price = max(raw_sl, floor_sl)
    else:
        raw_sl = entry_price + sl_dist
        swing_high = symbol_highest_since_entry.get(symbol, entry_price)
        if swing_high and swing_high > entry_price:
            structural_sl = swing_high + atr * 0.3
            raw_sl = min(raw_sl, structural_sl)
        max_loss_pct = params["max_loss"] / 100
        ceil_sl = entry_price * (1 + max_loss_pct)
        sl_price = min(raw_sl, ceil_sl)

    symbol_sl_price[symbol] = sl_price
    symbol_trailing_sl[symbol] = sl_price

def set_server_stop_loss(symbol: str, side: str, sl_price: float) -> bool:
    """Place a server-side stop-loss order on Bybit."""
    try:
        resp = bybit_session.get_positions(category="linear", symbol=symbol, settleCoin="USDT")
        positions = resp.get("result", {}).get("list", [])
        qty_str = None
        for pos in positions:
            if pos.get("side") == side and float(pos.get("size", 0)) > 0:
                qty_str = pos["size"]
                break
        if not qty_str:
            return False

        constraints = get_instrument_constraints(symbol)
        tick_size = float(constraints.get("tickSize", "0.01"))
        # Round SL to tick
        sl_rounded = round(sl_price / tick_size) * tick_size
        tick_decimals = len(constraints.get("tickSize", "0.01").rstrip('0').split('.')[-1]) if '.' in constraints.get("tickSize", "0.01") else 0
        sl_str = f"{sl_rounded:.{tick_decimals}f}"

        trigger_direction = 2 if side == "Buy" else 1  # 2=fall below, 1=rise above
        close_side = "Sell" if side == "Buy" else "Buy"

        result = bybit_session.place_order(
            category="linear",
            symbol=symbol,
            side=close_side,
            orderType="Market",
            qty=qty_str,
            reduceOnly=True,
            triggerPrice=sl_str,
            triggerDirection=trigger_direction,
            orderFilter="StopOrder",
        )
        if result.get("retCode") == 0:
            symbol_server_sl_set[symbol] = True
            print(f"[{symbol}] Server SL set at {sl_str}")
            return True
        else:
            print(f"[{symbol}] Failed to set server SL: {result}")
            return False
    except Exception as e:
        print(f"[{symbol}] Error setting server SL: {e}")
        return False

def get_position_size(symbol: str) -> Optional[float]:
    """Get current position size from Bybit."""
    try:
        resp = bybit_session.get_positions(category="linear", symbol=symbol, settleCoin="USDT")
        for pos in resp.get("result", {}).get("list", []):
            size = float(pos.get("size", 0))
            if size > 0:
                return size
        return 0.0
    except Exception as e:
        print(f"[{symbol}] Error getting position size: {e}")
        return None

def get_position_entry_price(symbol: str) -> Optional[float]:
    """Get the entry price of the current position from Bybit."""
    try:
        resp = bybit_session.get_positions(category="linear", symbol=symbol, settleCoin="USDT")
        for pos in resp.get("result", {}).get("list", []):
            size = float(pos.get("size", 0))
            if size > 0:
                return float(pos.get("avgPrice", 0))
        return None
    except Exception as e:
        print(f"[{symbol}] Error getting entry price: {e}")
        return None

def cancel_all_symbol_orders(symbol: str):
    """Cancel all open orders (conditional + active) for a symbol."""
    try:
        bybit_session.cancel_all_orders(category="linear", symbol=symbol)
        print(f"[{symbol}] Cancelled all open orders")
    except Exception as e:
        print(f"[{symbol}] Error cancelling orders: {e}")


async def place_trade(symbol: str, side: str, bot=None, manual_qty: Optional[float] = None) -> bool:
    """Place a new trade (market order) and set initial risk state."""
    try:
        qty_str = get_trade_qty(symbol, side, manual_qty=manual_qty)
        if not qty_str:
            print(f"[{symbol}] Could not calculate qty for {side}")
            return False

        result = bybit_session.place_order(
            category="linear",
            symbol=symbol,
            side=side,
            orderType="Market",
            qty=qty_str,
        )
        if result.get("retCode") == 0:
            print(f"[{symbol}] Placed {side} market order: {qty_str}")
            return True
        else:
            print(f"[{symbol}] Order failed: {result}")
            return False
    except Exception as e:
        print(f"[{symbol}] Error placing trade: {e}")
        return False


def compute_entry_signals(ind: dict, htf: dict, params: dict) -> Tuple[int, int, str]:
    """
    Compute entry signal counts for long and short based on v29 strategy.
    Returns (long_signals, short_signals, best_strategy_label).
    """
    trend_score = ind["trend_score"]
    adx = ind["adx"]
    rsi = ind["rsi"]
    macd_hist = ind["macd_hist"]
    macd_hist_prev = ind["macd_hist_prev"]
    stoch_k = ind["stoch_rsi_k"]
    stoch_d = ind["stoch_rsi_d"]
    vol_ratio = ind["vol_ratio"]
    roc = ind["roc"]
    ema_fast_slope = ind["ema_fast_slope"] or 0.0
    vwap = ind["vwap"]
    price = ind["current_price"]
    bb_upper = ind["bb_upper"]
    bb_lower = ind["bb_lower"]
    bb_width = ind["bb_width"]
    bb_width_min = ind["bb_width_min"]
    ema_fast = ind["ema_fast"]
    ema_mid = ind["ema_mid"]
    prev_ema_fast = ind["prev_ema_fast"]
    prev_ema_mid = ind["prev_ema_mid"]

    htf_1h = htf.get("htf_1h_trend", 0)
    htf_4h = htf.get("htf_4h_trend", 0)

    adx_entry = params["adx_entry"]
    rsi_ob = params["rsi_ob"]
    rsi_os = params["rsi_os"]
    bb_squeeze = params["bb_squeeze"]
    rsi_mr_high = params["rsi_mr_high"]
    rsi_mr_low = params["rsi_mr_low"]

    long_signals = 0
    short_signals = 0
    strategy_a_long = 0
    strategy_a_short = 0
    best_strategy = ""

    # ────────────────────────────
    # Strategy A: Trend Following (multi-signal scoring)
    # ────────────────────────────
    if adx is not None and adx >= adx_entry:
        # Signal 1: Trend score
        if trend_score >= 3: strategy_a_long += 1
        if trend_score <= -3: strategy_a_short += 1

        # Signal 2: HTF 1H trend
        if htf_1h >= 1: strategy_a_long += 1
        if htf_1h <= -1: strategy_a_short += 1

        # Signal 3: HTF 4H trend
        if htf_4h >= 1: strategy_a_long += 1
        if htf_4h <= -1: strategy_a_short += 1

        # Signal 4: MACD histogram rising
        if macd_hist > 0 and macd_hist > macd_hist_prev: strategy_a_long += 1
        if macd_hist < 0 and macd_hist < macd_hist_prev: strategy_a_short += 1

        # Signal 5: RSI confirming
        if 40 < rsi < rsi_ob: strategy_a_long += 1
        if rsi_os < rsi < 60: strategy_a_short += 1

        # Signal 6: VWAP
        if vwap and price > vwap: strategy_a_long += 1
        if vwap and price < vwap: strategy_a_short += 1

        # Signal 7: ROC
        if roc > 0: strategy_a_long += 1
        if roc < 0: strategy_a_short += 1

        # Signal 8: EMA slope
        if ema_fast_slope > 0.05: strategy_a_long += 1
        if ema_fast_slope < -0.05: strategy_a_short += 1

        # Signal 9: Stoch RSI
        if stoch_k > stoch_d and stoch_k < 80: strategy_a_long += 1
        if stoch_k < stoch_d and stoch_k > 20: strategy_a_short += 1

        # Signal 10: Volume confirmation
        if vol_ratio > 1.2: 
            strategy_a_long += 1
            strategy_a_short += 1

        # ADX-adaptive thresholds
        adx_strong = params.get("adx_strong", 28)
        adx_vstrong = params.get("adx_vstrong", 42)
        sig_threshold = params["sig_threshold"]
        sig_strong = params["sig_strong"]
        sig_vstrong = params["sig_vstrong"]

        if adx >= adx_vstrong:
            threshold = sig_vstrong
        elif adx >= adx_strong:
            threshold = sig_strong
        else:
            threshold = sig_threshold

        if strategy_a_long >= threshold:
            long_signals = max(long_signals, strategy_a_long)
            best_strategy = "A:Trend"
        if strategy_a_short >= threshold:
            short_signals = max(short_signals, strategy_a_short)
            best_strategy = "A:Trend"

    # ────────────────────────────
    # Strategy B: BB Squeeze Breakout
    # ────────────────────────────
    if bb_width is not None and bb_width_min is not None and bb_width_min < bb_squeeze:
        b_long = 0
        b_short = 0
        if bb_upper and price > bb_upper and rsi > 50 and trend_score >= 1:
            b_long = 5
        if bb_lower and price < bb_lower and rsi < 50 and trend_score <= -1:
            b_short = 5
        if b_long > long_signals:
            long_signals = b_long
            best_strategy = "B:BBSqueeze"
        if b_short > short_signals:
            short_signals = b_short
            best_strategy = "B:BBSqueeze"

    # ────────────────────────────
    # Strategy C: Mean Reversion
    # ────────────────────────────
    if adx is not None and adx < adx_entry:
        c_long = 0
        c_short = 0
        if bb_lower and price < bb_lower and rsi < rsi_mr_low and stoch_k < 20:
            c_long = 5
        if bb_upper and price > bb_upper and rsi > rsi_mr_high and stoch_k > 80:
            c_short = 5
        if c_long > long_signals:
            long_signals = c_long
            best_strategy = "C:MeanRev"
        if c_short > short_signals:
            short_signals = c_short
            best_strategy = "C:MeanRev"

    # ────────────────────────────
    # Strategy D: EMA Crossover + Volume Surge
    # ────────────────────────────
    if prev_ema_fast is not None and prev_ema_mid is not None and ema_fast is not None and ema_mid is not None:
        d_long = 0
        d_short = 0
        cross_up = prev_ema_fast <= prev_ema_mid and ema_fast > ema_mid
        cross_down = prev_ema_fast >= prev_ema_mid and ema_fast < ema_mid
        if cross_up and vol_ratio > 1.5 and rsi > 45 and trend_score >= 0:
            d_long = 5
        if cross_down and vol_ratio > 1.5 and rsi < 55 and trend_score <= 0:
            d_short = 5
        if d_long > long_signals:
            long_signals = d_long
            best_strategy = "D:EMACross"
        if d_short > short_signals:
            short_signals = d_short
            best_strategy = "D:EMACross"

    return long_signals, short_signals, best_strategy


def compute_exit_signals(ind: dict, htf: dict, side: str) -> int:
    """
    Count bearish (for longs) or bullish (for shorts) exit signals.
    Returns signal count (exit when >= 3).
    """
    trend_score = ind["trend_score"]
    rsi = ind["rsi"]
    macd_hist = ind["macd_hist"]
    macd_hist_prev = ind["macd_hist_prev"]
    stoch_k = ind["stoch_rsi_k"]
    ema_fast = ind["ema_fast"]
    ema_mid = ind["ema_mid"]
    price = ind["current_price"]

    htf_1h = htf.get("htf_1h_trend", 0)

    count = 0
    if side == "Buy":
        # Bearish signals for long exit
        if trend_score <= -2: count += 1
        if rsi > 75: count += 1
        if macd_hist < 0 and macd_hist < macd_hist_prev: count += 1
        if stoch_k > 80 and stoch_k < (ind["stoch_rsi_d"] or 50): count += 1
        if ema_fast and ema_mid and ema_fast < ema_mid: count += 1
        if htf_1h <= -1: count += 1
        if ema_fast and price < ema_fast: count += 1
    else:
        # Bullish signals for short exit
        if trend_score >= 2: count += 1
        if rsi < 25: count += 1
        if macd_hist > 0 and macd_hist > macd_hist_prev: count += 1
        if stoch_k < 20 and stoch_k > (ind["stoch_rsi_d"] or 50): count += 1
        if ema_fast and ema_mid and ema_fast > ema_mid: count += 1
        if htf_1h >= 1: count += 1
        if ema_fast and price > ema_fast: count += 1

    return count


# ────────────────────────────────────────────────
#  Core Trading Loop: monitor_symbol (v29 strategy)
# ────────────────────────────────────────────────

async def monitor_symbol(symbol: str, bot):
    """
    Main trading loop for a symbol. Implements the v29 backtester strategy:
    - 4 entry strategies (Trend Following, BB Squeeze, Mean Reversion, EMA Crossover)
    - Signal-based exits (bearish/bullish count >= 3)
    - ATR-adaptive trailing stops with progressive tightening
    - 4-level take-profit system (TP0-TP3)
    - Equity curve management (drawdown-based sizing)
    - Cooldown system (after losses)
    """
    last_status_time = 0
    STATUS_INTERVAL = 1800  # 30 minutes

    while symbol in monitored_symbols:
        try:
            params = get_symbol_params(symbol)

            # ──────────────────────────────────
            #  1. Fetch indicators + HTF trends
            # ──────────────────────────────────
            ind = await get_all_indicators(symbol, interval="30", limit=200)
            if ind is None:
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                continue

            htf = await compute_higher_tf(symbol)

            price = ind["current_price"]
            atr = ind["atr"]
            adx = ind["adx"]
            atr_pct = ind["atr_pct"]

            if price is None or atr is None or adx is None:
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                continue

            # Volatility floor check
            min_atr_pct = params.get("min_atr_pct", 0.3)
            if atr_pct is not None and atr_pct < min_atr_pct:
                # Too low volatility, skip this cycle
                await asyncio.sleep(POLL_INTERVAL_SECONDS)
                continue

            # Track bar changes (for bars_in_trade counting)
            candle_ts = ind["last_candle_ts"]
            prev_candle_ts = symbol_last_candle_ts.get(symbol)
            new_candle = (candle_ts != prev_candle_ts) and (prev_candle_ts is not None)
            symbol_last_candle_ts[symbol] = candle_ts

            # Check current position state
            current_side = symbol_open_trade_side.get(symbol)
            actual_side = get_open_position_side(symbol)

            # Sync state: if position was closed externally
            if current_side and not actual_side:
                print(f"[{symbol}] Position was closed externally, clearing state")
                _clear_risk_state(symbol)
                symbol_open_trade_side[symbol] = None
                symbol_trends[symbol] = None
                current_side = None
                save_state()

            # Sync state: if position exists but bot doesn't know about it
            if actual_side and not current_side:
                print(f"[{symbol}] Found existing {actual_side} position, syncing state")
                entry_price = get_position_entry_price(symbol)
                if entry_price:
                    _set_entry_risk_state(symbol, entry_price, atr, actual_side, 0, params)
                    symbol_open_trade_side[symbol] = actual_side
                    symbol_trends[symbol] = "LONG" if actual_side == "Buy" else "SHORT"
                    current_side = actual_side
                    save_state()

            # ──────────────────────────────────
            #  2. POSITION OPEN: Risk Management
            # ──────────────────────────────────
            if current_side:
                entry_price = symbol_entry_price.get(symbol)
                entry_atr = symbol_entry_atr.get(symbol) or atr
                sl_price = symbol_sl_price.get(symbol)
                trailing_sl = symbol_trailing_sl.get(symbol)

                if entry_price is None:
                    entry_price = get_position_entry_price(symbol) or price

                # Increment bars in trade on new candle
                if new_candle:
                    symbol_bars_in_trade[symbol] = symbol_bars_in_trade.get(symbol, 0) + 1

                # Update peak/trough tracking
                if current_side == "Buy":
                    if symbol_highest_since_entry.get(symbol) is None or price > symbol_highest_since_entry[symbol]:
                        symbol_highest_since_entry[symbol] = price
                    if symbol_lowest_since_entry.get(symbol) is None or price < symbol_lowest_since_entry[symbol]:
                        symbol_lowest_since_entry[symbol] = price
                else:
                    if symbol_lowest_since_entry.get(symbol) is None or price < symbol_lowest_since_entry[symbol]:
                        symbol_lowest_since_entry[symbol] = price
                    if symbol_highest_since_entry.get(symbol) is None or price > symbol_highest_since_entry[symbol]:
                        symbol_highest_since_entry[symbol] = price

                # ── Check stop-loss hit ──
                sl_hit = False
                if sl_price:
                    if current_side == "Buy" and price <= sl_price:
                        sl_hit = True
                    elif current_side == "Sell" and price >= sl_price:
                        sl_hit = True

                if sl_hit:
                    await close_position(symbol, current_side, bot)
                    cancel_all_symbol_orders(symbol)
                    pnl_pct = ((price - entry_price) / entry_price * 100) if current_side == "Buy" else ((entry_price - price) / entry_price * 100)
                    # Update streak tracking
                    if pnl_pct < 0:
                        symbol_consecutive_losses[symbol] = symbol_consecutive_losses.get(symbol, 0) + 1
                        symbol_consecutive_wins[symbol] = 0
                        cooldown = params["cooldown_loss"]
                        if symbol_consecutive_losses.get(symbol, 0) >= 3:
                            cooldown = params["cooldown_streak"]
                        symbol_cooldown[symbol] = cooldown
                    else:
                        symbol_consecutive_wins[symbol] = symbol_consecutive_wins.get(symbol, 0) + 1
                        symbol_consecutive_losses[symbol] = 0
                        symbol_cooldown[symbol] = params["cooldown_bars"]

                    await bot.send_message(
                        chat_id=CHAT_ID,
                        text=(
                            f"🛑 <b>{symbol}</b> STOP-LOSS HIT\n"
                            f"Side: {current_side} | PnL: {pnl_pct:+.2f}%\n"
                            f"Entry: {entry_price:.6g} → Exit: {price:.6g}\n"
                            f"Consecutive Losses: {symbol_consecutive_losses.get(symbol, 0)}\n"
                            f"Cooldown: {symbol_cooldown.get(symbol, 0)} bars"
                        ),
                        parse_mode="HTML",
                    )
                    _clear_risk_state(symbol)
                    symbol_open_trade_side[symbol] = None
                    symbol_trends[symbol] = None
                    save_state()
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
                    continue

                # ── Take-Profit checks (TP0-TP3) ──
                if current_side == "Buy":
                    unrealized_atr = (price - entry_price) / entry_atr if entry_atr else 0
                else:
                    unrealized_atr = (entry_price - price) / entry_atr if entry_atr else 0

                remaining_pct = symbol_position_size_pct.get(symbol, 1.0)

                # TP0
                if not symbol_tp0_hit.get(symbol, False) and unrealized_atr >= params["tp0_atr"]:
                    close_frac = params["tp0_pct"] / remaining_pct if remaining_pct > 0 else 0
                    close_frac = min(close_frac, 0.99)
                    if close_frac > 0:
                        success = await close_partial_position(symbol, current_side, close_frac, bot)
                        if success:
                            symbol_tp0_hit[symbol] = True
                            symbol_position_size_pct[symbol] = remaining_pct - params["tp0_pct"]
                            await bot.send_message(
                                chat_id=CHAT_ID,
                                text=f"✅ <b>{symbol}</b> TP0 hit ({params['tp0_atr']:.1f}x ATR) — closed {params['tp0_pct']*100:.0f}%",
                                parse_mode="HTML",
                            )
                            save_state()

                # TP1
                remaining_pct = symbol_position_size_pct.get(symbol, 1.0)
                if not symbol_tp1_hit.get(symbol, False) and unrealized_atr >= params["tp1_atr"]:
                    close_frac = params["tp1_pct"] / remaining_pct if remaining_pct > 0 else 0
                    close_frac = min(close_frac, 0.99)
                    if close_frac > 0:
                        success = await close_partial_position(symbol, current_side, close_frac, bot)
                        if success:
                            symbol_tp1_hit[symbol] = True
                            symbol_position_size_pct[symbol] = remaining_pct - params["tp1_pct"]
                            await bot.send_message(
                                chat_id=CHAT_ID,
                                text=f"✅ <b>{symbol}</b> TP1 hit ({params['tp1_atr']:.1f}x ATR) — closed {params['tp1_pct']*100:.0f}%",
                                parse_mode="HTML",
                            )
                            save_state()

                # TP2
                remaining_pct = symbol_position_size_pct.get(symbol, 1.0)
                if not symbol_tp2_hit.get(symbol, False) and unrealized_atr >= params["tp2_atr"]:
                    close_frac = params["tp2_pct"] / remaining_pct if remaining_pct > 0 else 0
                    close_frac = min(close_frac, 0.99)
                    if close_frac > 0:
                        success = await close_partial_position(symbol, current_side, close_frac, bot)
                        if success:
                            symbol_tp2_hit[symbol] = True
                            symbol_position_size_pct[symbol] = remaining_pct - params["tp2_pct"]
                            await bot.send_message(
                                chat_id=CHAT_ID,
                                text=f"✅ <b>{symbol}</b> TP2 hit ({params['tp2_atr']:.1f}x ATR) — closed {params['tp2_pct']*100:.0f}%",
                                parse_mode="HTML",
                            )
                            save_state()

                # TP3
                remaining_pct = symbol_position_size_pct.get(symbol, 1.0)
                if not symbol_tp3_hit.get(symbol, False) and unrealized_atr >= params["tp3_atr"]:
                    close_frac = params["tp3_pct"] / remaining_pct if remaining_pct > 0 else 0
                    close_frac = min(close_frac, 0.99)
                    if close_frac > 0:
                        success = await close_partial_position(symbol, current_side, close_frac, bot)
                        if success:
                            symbol_tp3_hit[symbol] = True
                            symbol_position_size_pct[symbol] = remaining_pct - params["tp3_pct"]
                            await bot.send_message(
                                chat_id=CHAT_ID,
                                text=f"✅ <b>{symbol}</b> TP3 hit ({params['tp3_atr']:.1f}x ATR) — closed {params['tp3_pct']*100:.0f}%",
                                parse_mode="HTML",
                            )
                            save_state()

                # ── Trailing Stop Update (ADX-adaptive with progressive tightening) ──
                if trailing_sl is not None and entry_atr:
                    # Determine trail multiplier based on ADX
                    adx_strong = params.get("adx_strong", 28)
                    adx_vstrong = params.get("adx_vstrong", 42)
                    if adx >= adx_vstrong:
                        trail_mult = params["trail_vstrong"]
                    elif adx >= adx_strong:
                        trail_mult = params["trail_strong"]
                    else:
                        trail_mult = params["trail_atr"]

                    # Progressive tightening after TP hits
                    if symbol_tp2_hit.get(symbol, False):
                        trail_mult *= params["trail_tighten_tp2"]
                    elif symbol_tp1_hit.get(symbol, False):
                        trail_mult *= params["trail_tighten_tp1"]

                    trail_dist = entry_atr * trail_mult

                    if current_side == "Buy":
                        new_trail = price - trail_dist
                        if new_trail > trailing_sl:
                            symbol_trailing_sl[symbol] = new_trail
                            # Also update SL (never move SL backwards)
                            if new_trail > (symbol_sl_price.get(symbol) or 0):
                                symbol_sl_price[symbol] = new_trail
                    else:
                        new_trail = price + trail_dist
                        if new_trail < trailing_sl:
                            symbol_trailing_sl[symbol] = new_trail
                            if symbol_sl_price.get(symbol) is None or new_trail < symbol_sl_price[symbol]:
                                symbol_sl_price[symbol] = new_trail

                # ── Signal-based exit check ──
                exit_signals = compute_exit_signals(ind, htf, current_side)
                bars_in_trade = symbol_bars_in_trade.get(symbol, 0)

                should_exit = False
                exit_reason = ""

                # Exit if enough bearish/bullish signals
                if exit_signals >= 3:
                    should_exit = True
                    exit_reason = f"Signal exit ({exit_signals} opposing signals)"

                # Time-based flat trade exit
                flat_bars = params.get("flat_bars", 15)
                flat_threshold = params.get("flat_threshold", 0.3)
                if bars_in_trade >= flat_bars and abs(unrealized_atr) < flat_threshold:
                    should_exit = True
                    exit_reason = f"Flat trade exit ({bars_in_trade} bars, {unrealized_atr:.2f}x ATR)"

                if should_exit:
                    await close_position(symbol, current_side, bot)
                    cancel_all_symbol_orders(symbol)
                    pnl_pct = ((price - entry_price) / entry_price * 100) if current_side == "Buy" else ((entry_price - price) / entry_price * 100)

                    # Update streaks
                    if pnl_pct < 0:
                        symbol_consecutive_losses[symbol] = symbol_consecutive_losses.get(symbol, 0) + 1
                        symbol_consecutive_wins[symbol] = 0
                        cooldown = params["cooldown_loss"]
                        if symbol_consecutive_losses.get(symbol, 0) >= 3:
                            cooldown = params["cooldown_streak"]
                        symbol_cooldown[symbol] = cooldown
                    else:
                        symbol_consecutive_wins[symbol] = symbol_consecutive_wins.get(symbol, 0) + 1
                        symbol_consecutive_losses[symbol] = 0
                        symbol_cooldown[symbol] = params["cooldown_bars"]

                    emoji = "📉" if pnl_pct < 0 else "📈"
                    await bot.send_message(
                        chat_id=CHAT_ID,
                        text=(
                            f"{emoji} <b>{symbol}</b> EXIT — {exit_reason}\n"
                            f"Side: {current_side} | PnL: {pnl_pct:+.2f}%\n"
                            f"Entry: {entry_price:.6g} → Exit: {price:.6g}\n"
                            f"Bars in trade: {bars_in_trade}\n"
                            f"Losses streak: {symbol_consecutive_losses.get(symbol, 0)} | "
                            f"Wins streak: {symbol_consecutive_wins.get(symbol, 0)}"
                        ),
                        parse_mode="HTML",
                    )

                    old_side = current_side
                    _clear_risk_state(symbol)
                    symbol_open_trade_side[symbol] = None
                    symbol_trends[symbol] = None
                    save_state()

                    # Quick reversal check after signal exit
                    if exit_signals >= 3 and symbol_cooldown.get(symbol, 0) <= 0:
                        long_sig, short_sig, strat = compute_entry_signals(ind, htf, params)
                        rev_side = None
                        if old_side == "Buy" and short_sig >= params["sig_threshold"]:
                            rev_side = "Sell"
                        elif old_side == "Sell" and long_sig >= params["sig_threshold"]:
                            rev_side = "Buy"

                        if rev_side:
                            manual_qty = symbol_manual_quantities.get(symbol)
                            success = await place_trade(symbol, rev_side, bot, manual_qty=manual_qty)
                            if success:
                                entry_p = get_position_entry_price(symbol) or price
                                pos_size = get_position_size(symbol) or 0
                                _set_entry_risk_state(symbol, entry_p, atr, rev_side, pos_size, params)
                                symbol_open_trade_side[symbol] = rev_side
                                symbol_trends[symbol] = "LONG" if rev_side == "Buy" else "SHORT"
                                set_server_stop_loss(symbol, rev_side, symbol_sl_price.get(symbol, 0))

                                await bot.send_message(
                                    chat_id=CHAT_ID,
                                    text=(
                                        f"🔄 <b>{symbol}</b> QUICK REVERSAL → {rev_side}\n"
                                        f"Strategy: {strat}\n"
                                        f"Entry: {entry_p:.6g} | SL: {symbol_sl_price.get(symbol, 0):.6g}\n"
                                        f"ATR: {atr:.6g} | ADX: {adx:.1f}"
                                    ),
                                    parse_mode="HTML",
                                )
                                save_state()

                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
                    continue

            # ──────────────────────────────────
            #  3. NO POSITION: Entry Logic
            # ──────────────────────────────────
            else:
                # Decrement cooldown on new candle
                if new_candle and symbol_cooldown.get(symbol, 0) > 0:
                    symbol_cooldown[symbol] = symbol_cooldown.get(symbol, 0) - 1

                # Skip if in cooldown
                if symbol_cooldown.get(symbol, 0) > 0:
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
                    continue

                # Equity curve management: check drawdown
                try:
                    balance_resp = await asyncio.to_thread(
                        bybit_session.get_wallet_balance,
                        accountType="UNIFIED",
                        coin="USDT",
                    )
                    coins = balance_resp["result"]["list"][0].get("coin", [])
                    equity = 0.0
                    for c in coins:
                        if c["coin"] == "USDT":
                            equity = float(c.get("equity", c.get("walletBalance", 0)))
                            break
                except Exception:
                    equity = 0.0

                peak_eq = symbol_peak_equity.get(symbol, equity)
                if equity > peak_eq:
                    peak_eq = equity
                    symbol_peak_equity[symbol] = peak_eq

                dd_pct = ((peak_eq - equity) / peak_eq * 100) if peak_eq > 0 else 0

                # Drawdown-based position sizing
                if dd_pct >= params["dd_pause"]:
                    # Too much drawdown, pause trading
                    await asyncio.sleep(POLL_INTERVAL_SECONDS)
                    continue

                size_mult = 1.0
                if dd_pct >= params["dd_small"]:
                    size_mult = params["size_small"]
                elif dd_pct >= params["dd_reduce"]:
                    size_mult = params["size_reduced"]

                # Win streak bonus
                wins = symbol_consecutive_wins.get(symbol, 0)
                if wins >= 3:
                    size_mult += params["win_streak_bonus"]
                    size_mult = min(size_mult, 1.5)  # cap at 150%

                # Compute entry signals
                long_sig, short_sig, strategy = compute_entry_signals(ind, htf, params)

                # Determine entry side
                entry_side = None
                sig_count = 0

                # ADX-adaptive threshold
                adx_strong = params.get("adx_strong", 28)
                adx_vstrong = params.get("adx_vstrong", 42)
                if adx >= adx_vstrong:
                    threshold = params["sig_vstrong"]
                elif adx >= adx_strong:
                    threshold = params["sig_strong"]
                else:
                    threshold = params["sig_threshold"]

                if long_sig >= threshold and long_sig > short_sig:
                    entry_side = "Buy"
                    sig_count = long_sig
                elif short_sig >= threshold and short_sig > long_sig:
                    entry_side = "Sell"
                    sig_count = short_sig

                if entry_side:
                    # Calculate quantity with size multiplier
                    manual_qty = symbol_manual_quantities.get(symbol)
                    if manual_qty and size_mult != 1.0:
                        adjusted_qty = manual_qty * size_mult
                        # Mean reversion uses smaller size
                        if strategy == "C:MeanRev":
                            adjusted_qty *= params.get("mr_size", 0.6)
                    elif manual_qty:
                        adjusted_qty = manual_qty
                        if strategy == "C:MeanRev":
                            adjusted_qty *= params.get("mr_size", 0.6)
                    else:
                        adjusted_qty = None  # Will use wallet-based sizing

                    success = await place_trade(symbol, entry_side, bot, manual_qty=adjusted_qty)

                    if success:
                        entry_p = get_position_entry_price(symbol) or price
                        pos_size = get_position_size(symbol) or 0

                        # Store swing levels before setting risk state
                        symbol_lowest_since_entry[symbol] = ind["swing_low"] or price
                        symbol_highest_since_entry[symbol] = ind["swing_high"] or price

                        _set_entry_risk_state(symbol, entry_p, atr, entry_side, pos_size, params)
                        symbol_open_trade_side[symbol] = entry_side
                        symbol_trends[symbol] = "LONG" if entry_side == "Buy" else "SHORT"

                        # Set server-side stop-loss
                        set_server_stop_loss(symbol, entry_side, symbol_sl_price.get(symbol, 0))

                        # Compute TP levels for display
                        tp0_price = entry_p + atr * params["tp0_atr"] if entry_side == "Buy" else entry_p - atr * params["tp0_atr"]
                        tp1_price = entry_p + atr * params["tp1_atr"] if entry_side == "Buy" else entry_p - atr * params["tp1_atr"]
                        tp2_price = entry_p + atr * params["tp2_atr"] if entry_side == "Buy" else entry_p - atr * params["tp2_atr"]
                        tp3_price = entry_p + atr * params["tp3_atr"] if entry_side == "Buy" else entry_p - atr * params["tp3_atr"]

                        direction = "\U0001f7e2 LONG" if entry_side == "Buy" else "\U0001f534 SHORT"
                        await bot.send_message(
                            chat_id=CHAT_ID,
                            text=(
                                f"{direction} <b>{symbol}</b> \u2014 {strategy}\n"
                                f"Entry: {entry_p:.6g} | Signals: {sig_count}\n"
                                f"SL: {symbol_sl_price.get(symbol, 0):.6g}\n"
                                f"TP0: {tp0_price:.6g} ({params['tp0_pct']*100:.0f}%)\n"
                                f"TP1: {tp1_price:.6g} ({params['tp1_pct']*100:.0f}%)\n"
                                f"TP2: {tp2_price:.6g} ({params['tp2_pct']*100:.0f}%)\n"
                                f"TP3: {tp3_price:.6g} ({params['tp3_pct']*100:.0f}%)\n"
                                f"ATR: {atr:.6g} ({atr_pct:.2f}%) | ADX: {adx:.1f}\n"
                                f"Trend: {ind['trend_score']} | HTF 1H: {htf.get('htf_1h_trend', 0)} | 4H: {htf.get('htf_4h_trend', 0)}\n"
                                f"Size mult: {size_mult:.2f}"
                            ),
                            parse_mode="HTML",
                        )
                        save_state()

            # ──────────────────────────────────
            #  4. Periodic Status Report (every 30 min)
            # ──────────────────────────────────
            now = time.time()
            if now - last_status_time >= STATUS_INTERVAL:
                last_status_time = now
                current_side = symbol_open_trade_side.get(symbol)
                if current_side:
                    entry_p = symbol_entry_price.get(symbol, 0) or 0
                    pnl = ((price - entry_p) / entry_p * 100) if current_side == "Buy" else ((entry_p - price) / entry_p * 100) if entry_p else 0
                    entry_atr_val = symbol_entry_atr.get(symbol) or atr
                    unreal_atr = ((price - entry_p) / entry_atr_val) if current_side == "Buy" else ((entry_p - price) / entry_atr_val) if entry_atr_val else 0

                    tp_status = []
                    if symbol_tp0_hit.get(symbol): tp_status.append("TP0\u2713")
                    if symbol_tp1_hit.get(symbol): tp_status.append("TP1\u2713")
                    if symbol_tp2_hit.get(symbol): tp_status.append("TP2\u2713")
                    if symbol_tp3_hit.get(symbol): tp_status.append("TP3\u2713")
                    tp_str = " ".join(tp_status) if tp_status else "None hit"

                    remaining = symbol_position_size_pct.get(symbol, 1.0)

                    await bot.send_message(
                        chat_id=CHAT_ID,
                        text=(
                            f"\U0001f4ca <b>{symbol}</b> STATUS\n"
                            f"Side: {current_side} | PnL: {pnl:+.2f}% ({unreal_atr:+.2f}x ATR)\n"
                            f"Entry: {entry_p:.6g} | Now: {price:.6g}\n"
                            f"SL: {symbol_sl_price.get(symbol, 0):.6g} | Trail: {symbol_trailing_sl.get(symbol, 0):.6g}\n"
                            f"TP: {tp_str} | Remaining: {remaining*100:.0f}%\n"
                            f"Bars: {symbol_bars_in_trade.get(symbol, 0)} | ADX: {adx:.1f}\n"
                            f"Trend: {ind['trend_score']} | RSI: {ind['rsi']:.1f}"
                        ),
                        parse_mode="HTML",
                    )
                else:
                    cooldown_left = symbol_cooldown.get(symbol, 0)
                    cd_str = f" | Cooldown: {cooldown_left} bars" if cooldown_left > 0 else ""
                    await bot.send_message(
                        chat_id=CHAT_ID,
                        text=(
                            f"\U0001f4ca <b>{symbol}</b> WATCHING (no position)\n"
                            f"Price: {price:.6g} | ATR: {atr:.6g} ({atr_pct:.2f}%)\n"
                            f"ADX: {adx:.1f} | RSI: {ind['rsi']:.1f} | Trend: {ind['trend_score']}\n"
                            f"HTF 1H: {htf.get('htf_1h_trend', 0)} | 4H: {htf.get('htf_4h_trend', 0)}{cd_str}\n"
                            f"Losses: {symbol_consecutive_losses.get(symbol, 0)} | Wins: {symbol_consecutive_wins.get(symbol, 0)}"
                        ),
                        parse_mode="HTML",
                    )

        except Exception as e:
            print(f"[{symbol}] Monitor error: {e}")
            import traceback
            traceback.print_exc()

        await asyncio.sleep(POLL_INTERVAL_SECONDS)


# ────────────────────────────────────────────────
#  Telegram Commands: add_symbol, remove_symbol, start
# ────────────────────────────────────────────────

async def add_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Add a symbol to monitor. Usage: /add_symbol BTCUSDT [optional_qty]"""
    if update.effective_chat.id != CHAT_ID:
        await update.message.reply_text("Unauthorized.")
        return

    args = context.args
    if not args:
        await update.message.reply_text("Usage: /add_symbol SYMBOL [qty]\nExample: /add_symbol BTCUSDT 0.5")
        return

    symbol = args[0].upper()
    manual_qty = None
    if len(args) > 1:
        try:
            manual_qty = float(args[1])
        except ValueError:
            await update.message.reply_text(f"Invalid quantity: {args[1]}")
            return

    if symbol in monitored_symbols:
        await update.message.reply_text(f"{symbol} is already being monitored.")
        return

    # Validate symbol exists on Bybit
    constraints = get_instrument_constraints(symbol)
    if not constraints:
        await update.message.reply_text(f"Could not find {symbol} on Bybit. Check the symbol name.")
        return

    # Initialize state
    monitored_symbols.append(symbol)
    symbol_trends[symbol] = None
    symbol_open_trade_side[symbol] = None
    symbol_manual_quantities[symbol] = manual_qty
    _clear_risk_state(symbol)
    symbol_cooldown[symbol] = 0
    symbol_consecutive_losses[symbol] = 0
    symbol_consecutive_wins[symbol] = 0
    symbol_peak_equity[symbol] = 0.0

    # Subscribe to WebSocket
    subscribe_ws_symbol(symbol)

    # Check for existing position
    existing_side = get_open_position_side(symbol)
    if existing_side:
        entry_price = get_position_entry_price(symbol)
        params = get_symbol_params(symbol)
        ind = await get_all_indicators(symbol)
        atr_val = ind["atr"] if ind else 1.0
        if entry_price:
            _set_entry_risk_state(symbol, entry_price, atr_val, existing_side, 0, params)
        symbol_open_trade_side[symbol] = existing_side
        symbol_trends[symbol] = "LONG" if existing_side == "Buy" else "SHORT"

    save_state()

    params = get_symbol_params(symbol)
    has_overrides = symbol in COIN_OVERRIDES
    override_str = " (custom params)" if has_overrides else " (default params)"

    await update.message.reply_text(
        f"\u2705 <b>{symbol}</b> added to monitoring{override_str}\n"
        f"Qty: {'Auto (95% wallet)' if manual_qty is None else f'{manual_qty}'}\n"
        f"{'Existing ' + existing_side + ' position detected!' if existing_side else 'No existing position'}\n\n"
        f"Strategy: v29 (4 strategies)\n"
        f"\u2022 A: Trend Following (multi-signal scoring)\n"
        f"\u2022 B: BB Squeeze Breakout\n"
        f"\u2022 C: Mean Reversion\n"
        f"\u2022 D: EMA Crossover + Volume Surge\n\n"
        f"Risk Management:\n"
        f"\u2022 SL: {params['sl_atr']:.1f}x ATR (max {params['max_loss']:.1f}%)\n"
        f"\u2022 Trail: {params['trail_atr']:.1f}x ATR (ADX-adaptive)\n"
        f"\u2022 TP0: {params['tp0_atr']:.1f}x ATR ({params['tp0_pct']*100:.0f}%)\n"
        f"\u2022 TP1: {params['tp1_atr']:.1f}x ATR ({params['tp1_pct']*100:.0f}%)\n"
        f"\u2022 TP2: {params['tp2_atr']:.1f}x ATR ({params['tp2_pct']*100:.0f}%)\n"
        f"\u2022 TP3: {params['tp3_atr']:.1f}x ATR ({params['tp3_pct']*100:.0f}%)\n"
        f"\u2022 Cooldown: {params['cooldown_bars']} bars (loss: {params['cooldown_loss']})\n"
        f"\u2022 DD pause: {params['dd_pause']:.0f}%",
        parse_mode="HTML",
    )

    # Start monitoring loop
    asyncio.create_task(monitor_symbol(symbol, context.bot))

async def remove_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove a symbol from monitoring. Usage: /remove_symbol BTCUSDT"""
    if update.effective_chat.id != CHAT_ID:
        await update.message.reply_text("Unauthorized.")
        return

    args = context.args
    if not args:
        await update.message.reply_text("Usage: /remove_symbol SYMBOL")
        return

    symbol = args[0].upper()
    if symbol not in monitored_symbols:
        await update.message.reply_text(f"{symbol} is not being monitored.")
        return

    # Check for open position
    current_side = symbol_open_trade_side.get(symbol)
    if current_side:
        await close_position(symbol, current_side)
        cancel_all_symbol_orders(symbol)

    monitored_symbols.remove(symbol)
    _clear_risk_state(symbol)
    symbol_open_trade_side.pop(symbol, None)
    symbol_trends.pop(symbol, None)
    symbol_manual_quantities.pop(symbol, None)
    symbol_cooldown.pop(symbol, None)
    symbol_consecutive_losses.pop(symbol, None)
    symbol_consecutive_wins.pop(symbol, None)
    symbol_peak_equity.pop(symbol, None)
    symbol_bars_in_trade.pop(symbol, None)
    symbol_last_candle_ts.pop(symbol, None)

    save_state()

    closed_msg = f" Position closed ({current_side})." if current_side else ""
    await update.message.reply_text(f"\u274c <b>{symbol}</b> removed from monitoring.{closed_msg}", parse_mode="HTML")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.id != CHAT_ID:
        await update.message.reply_text("Unauthorized.")
        return

    active = ", ".join(monitored_symbols) if monitored_symbols else "None"
    await update.message.reply_text(
        f"\U0001f916 <b>Trading Bot v14 (v29 Strategy)</b>\n\n"
        f"Active symbols: {active}\n\n"
        "Commands:\n"
        "\u2022 /add_symbol SYMBOL [qty] \u2014 Start monitoring\n"
        "\u2022 /remove_symbol SYMBOL \u2014 Stop monitoring\n\n"
        "Strategy: BybitStrategyBacktester v29\n"
        "\u2022 4 entry strategies (Trend, BB Squeeze, Mean Rev, EMA Cross)\n"
        "\u2022 Multi-signal scoring with ADX-adaptive thresholds\n"
        "\u2022 Signal-based exits (bearish/bullish count >= 3)\n"
        "\u2022 ATR-adaptive trailing stops\n"
        "\u2022 4-level take-profit (TP0-TP3)\n"
        "\u2022 Equity curve management (drawdown-based sizing)\n"
        "\u2022 Cooldown system after losses\n"
        "\u2022 Higher timeframe confirmation (1H + 4H)\n\n"
        "Infrastructure:\n"
        "\u2022 WebSocket real-time price feeds (REST fallback)\n"
        f"\u2022 State persistence: {STATE_FILE} (crash recovery)\n"
        f"\u2022 Monitoring every {POLL_INTERVAL_SECONDS}s\n"
        "\u2022 Bot is PRIVATE \u2014 only accessible by owner\n\n"
        "Examples:\n"
        "\u2022 /add_symbol BTCUSDT\n"
        "\u2022 /add_symbol ETHUSDT 0.5",
        parse_mode="HTML",
    )

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    print(f"Exception while handling update: {context.error}")
    if CHAT_ID:
        await context.bot.send_message(
            chat_id=CHAT_ID,
            text=f"\u26a0\ufe0f Bot encountered an error:\n<code>{context.error}</code>",
            parse_mode="HTML"
        )

async def main():
    print("=== Render startup: Initializing ===")
    print(f"TELEGRAM_TOKEN loaded (length: {len(TELEGRAM_TOKEN)} chars)")
    print(f"CHAT_ID loaded: {CHAT_ID}")

    # Load persisted state from disk (crash recovery)
    if load_state():
        print(f"[STARTUP] Restored {len(monitored_symbols)} monitored symbols from state file")

    # Initialize WebSocket for real-time price feeds
    if start_websocket():
        # Subscribe to all monitored symbols
        for sym in monitored_symbols:
            subscribe_ws_symbol(sym)
        print(f"[STARTUP] WebSocket subscribed to {len(monitored_symbols)} symbols")

    application = Application.builder().token(TELEGRAM_TOKEN).updater(None).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("add_symbol", add_symbol))
    application.add_handler(CommandHandler("remove_symbol", remove_symbol))
    application.add_error_handler(error_handler)

    if application.job_queue:
        application.job_queue.run_repeating(
            callback=send_motivational_quote,
            interval=12 * 3600,
            first=0,
            chat_id=CHAT_ID,
            name="motivational_quote_sender",
        )
    print("Telegram application built.")

    port = int(os.environ.get("PORT", 10000))
    base_url = os.environ.get("RENDER_EXTERNAL_HOSTNAME", "psychology-trading-bot.onrender.com")
    webhook_url = f"https://{base_url}/webhook"

    await application.bot.set_webhook(url=webhook_url, drop_pending_updates=True)
    print(f"Webhook set to: {webhook_url}")

    await application.initialize()
    await application.start()

    # Resume monitoring for all loaded symbols
    for sym in monitored_symbols:
        asyncio.create_task(monitor_symbol(sym, application.bot))
    print(f"[STARTUP] Started monitoring {len(monitored_symbols)} symbols")

    async def health(request):
        return web.Response(text="OK")

    async def webhook_handler(request):
        try:
            update = Update.de_json(data=await request.json(), bot=application.bot)
            await application.update_queue.put(update)
            return web.Response(text="OK", status=200)
        except Exception as e:
            print(f"Webhook error: {e}")
            return web.Response(text="Error", status=500)

    aio_app = web.Application()
    aio_app.router.add_get("/health", health)
    aio_app.router.add_post("/webhook", webhook_handler)

    runner = web.AppRunner(aio_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"Async server running on port {port} (health + webhook)")
    print("Bot is fully running with webhook... Waiting for updates.")

    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        print("Shutting down gracefully...")
        await application.stop()
        await application.shutdown()
        await runner.cleanup()

if __name__ == "__main__":
    print("Entering main block...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot stopped by user.")
    except Exception as exc:
        print(f"Fatal startup error: {exc}")
        import traceback
        traceback.print_exc()
