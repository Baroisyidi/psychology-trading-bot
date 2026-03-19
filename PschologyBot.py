
import os
import json
import asyncio
import random
import time
import threading
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
            "symbol_trailing_active": symbol_trailing_active,
            "symbol_partial_taken": symbol_partial_taken,
            "symbol_server_sl_set": symbol_server_sl_set,
            "symbol_server_ts_set": symbol_server_ts_set,
            "symbol_trail_order_id": symbol_trail_order_id,
            "symbol_trail_qty": symbol_trail_qty,
            "symbol_trail_price": symbol_trail_price,
            "symbol_stage2_active": symbol_stage2_active,
            "symbol_stage2_taken": symbol_stage2_taken,
            "symbol_trail2_order_id": symbol_trail2_order_id,
            "symbol_trail2_qty": symbol_trail2_qty,
            "symbol_trail2_price": symbol_trail2_price,
            "symbol_peak_stage2": symbol_peak_stage2,
            "symbol_sl1_order_id": symbol_sl1_order_id,
            "symbol_sl1_qty": symbol_sl1_qty,
            "symbol_sl1_taken": symbol_sl1_taken,
            "symbol_original_qty": symbol_original_qty,
            "symbol_macd_partial_closed": symbol_macd_partial_closed,
            "symbol_macd_partial_qty": symbol_macd_partial_qty,
            "symbol_macd_last_action_candle": symbol_macd_last_action_candle,
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

        # Restore all state dicts
        symbol_trends.update(state.get("symbol_trends", {}))
        loaded_symbols = state.get("monitored_symbols", [])
        for s in loaded_symbols:
            if s not in monitored_symbols:
                monitored_symbols.append(s)
        symbol_open_trade_side.update(state.get("symbol_open_trade_side", {}))
        symbol_manual_quantities.update({k: v for k, v in state.get("symbol_manual_quantities", {}).items()})
        symbol_entry_price.update({k: v for k, v in state.get("symbol_entry_price", {}).items()})
        symbol_entry_atr.update({k: v for k, v in state.get("symbol_entry_atr", {}).items()})
        symbol_peak_price.update({k: v for k, v in state.get("symbol_peak_price", {}).items()})
        symbol_trailing_active.update({k: v for k, v in state.get("symbol_trailing_active", {}).items()})
        symbol_partial_taken.update({k: v for k, v in state.get("symbol_partial_taken", {}).items()})
        symbol_server_sl_set.update({k: v for k, v in state.get("symbol_server_sl_set", {}).items()})
        symbol_server_ts_set.update({k: v for k, v in state.get("symbol_server_ts_set", {}).items()})
        symbol_trail_order_id.update({k: v for k, v in state.get("symbol_trail_order_id", {}).items()})
        symbol_trail_qty.update({k: v for k, v in state.get("symbol_trail_qty", {}).items()})
        symbol_trail_price.update({k: v for k, v in state.get("symbol_trail_price", {}).items()})
        symbol_stage2_active.update({k: v for k, v in state.get("symbol_stage2_active", {}).items()})
        symbol_stage2_taken.update({k: v for k, v in state.get("symbol_stage2_taken", {}).items()})
        symbol_trail2_order_id.update({k: v for k, v in state.get("symbol_trail2_order_id", {}).items()})
        symbol_trail2_qty.update({k: v for k, v in state.get("symbol_trail2_qty", {}).items()})
        symbol_trail2_price.update({k: v for k, v in state.get("symbol_trail2_price", {}).items()})
        symbol_peak_stage2.update({k: v for k, v in state.get("symbol_peak_stage2", {}).items()})
        symbol_sl1_order_id.update({k: v for k, v in state.get("symbol_sl1_order_id", {}).items()})
        symbol_sl1_qty.update({k: v for k, v in state.get("symbol_sl1_qty", {}).items()})
        symbol_sl1_taken.update({k: v for k, v in state.get("symbol_sl1_taken", {}).items()})
        symbol_original_qty.update({k: v for k, v in state.get("symbol_original_qty", {}).items()})
        symbol_macd_partial_closed.update({k: v for k, v in state.get("symbol_macd_partial_closed", {}).items()})
        symbol_macd_partial_qty.update({k: v for k, v in state.get("symbol_macd_partial_qty", {}).items()})
        symbol_macd_last_action_candle.update({k: v for k, v in state.get("symbol_macd_last_action_candle", {}).items()})

        print(f"[STATE] Loaded state from {STATE_FILE} — {len(monitored_symbols)} symbols restored")
        return True
    except Exception as e:
        print(f"[STATE] Failed to load state: {e}")
        return False

# ────────────────────────────────────────────────
#  Global state
# ────────────────────────────────────────────────
symbol_trends: dict[str, Optional[str]] = {}
monitored_symbols: list[str] = []
symbol_open_trade_side: dict[str, Optional[str]] = {}
symbol_manual_quantities: dict[str, Optional[float]] = {}
symbol_last_di_cross_candle: dict[str, Optional[str]] = {}

# Per-symbol risk management tracking
symbol_entry_price: dict[str, Optional[float]] = {}
symbol_entry_atr: dict[str, Optional[float]] = {}
symbol_peak_price: dict[str, Optional[float]] = {}
symbol_trailing_active: dict[str, bool] = {}
symbol_partial_taken: dict[str, bool] = {}  # Track if 60% trailing profit has been taken
symbol_server_sl_set: dict[str, bool] = {}  # Track if server-side stop-loss is active
symbol_server_ts_set: dict[str, bool] = {}  # Track if server-side trailing stop is active
symbol_trail_order_id: dict[str, Optional[str]] = {}  # Conditional order ID for 60% trailing SL
symbol_trail_qty: dict[str, Optional[float]] = {}     # 60% qty under trailing stop order
symbol_trail_price: dict[str, Optional[float]] = {}   # Current trailing stop trigger price

# Stage 2 trailing stop (30% of original position, after 60% closed)
symbol_stage2_active: dict[str, bool] = {}             # Is stage 2 trailing active?
symbol_stage2_taken: dict[str, bool] = {}              # Has stage 2 trail been filled?
symbol_trail2_order_id: dict[str, Optional[str]] = {}  # Conditional order ID for stage 2 trail
symbol_trail2_qty: dict[str, Optional[float]] = {}     # Stage 2 trail qty
symbol_trail2_price: dict[str, Optional[float]] = {}   # Stage 2 trail trigger price
symbol_peak_stage2: dict[str, Optional[float]] = {}    # Peak price tracking for stage 2

# Tiered stop-loss tracking
symbol_sl1_order_id: dict[str, Optional[str]] = {}     # Conditional order for 30% at 1.5x ATR
symbol_sl1_qty: dict[str, Optional[float]] = {}        # Qty for the 30% SL order
symbol_sl1_taken: dict[str, bool] = {}                 # Has 30% SL at 1.5x ATR been taken?
symbol_original_qty: dict[str, Optional[float]] = {}   # Original position qty at entry

# MACD-based partial exit tracking
symbol_macd_partial_closed: dict[str, bool] = {}       # Whether 60% was closed due to MACD crossover
symbol_macd_partial_qty: dict[str, Optional[float]] = {} # Qty that was closed (to add back)
symbol_macd_last_action_candle: dict[str, Optional[str]] = {}  # Timestamp of last 10-min candle acted upon (prevents re-triggering)

# ────────────────────────────────────────────────
#  Constants
# ────────────────────────────────────────────────
ATR_MULTIPLIER = 1.5        # Legacy (kept for reference)
ADX_THRESHOLD = 22           # Only trade when ADX > 22 (trending)
ADX_REVERSAL_THRESHOLD = 24  # Reversal requires ADX > 24
STOP_LOSS_ATR_MULT = 2.0    # Ultimate stop-loss at 2x ATR from entry (closes remaining 70%)
SL_STAGE1_ATR_MULT = 1.5    # First tier SL at 1.5x ATR (closes 30%)
SL_STAGE1_CLOSE_PCT = 0.30  # Close 30% of position at first SL level
TRAILING_ACTIVATE_ATR = 1.5  # Activate trailing stop after 1.5x ATR profit
TRAILING_STOP_ATR = 1.0      # Stage 1 trailing stop distance: 1x ATR from peak
TRAILING_STOP_ATR_STAGE2 = 1.0  # Stage 2 trailing stop distance: 1x ATR from peak
TRAILING_CLOSE_PCT = 0.60    # Close 60% of position on stage 1 trail
TRAILING_STAGE2_PCT = 0.75   # Close 75% of remaining 40% (= 30% of original) on stage 2
POLL_INTERVAL_SECONDS = 10   # Poll every 10 seconds (faster for risk management)
RSI_OVERBOUGHT = 70          # Block long entries when RSI > 70
RSI_OVERSOLD = 30            # Block short entries when RSI < 30

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
    limit: int = 100,
) -> Optional[dict]:
    """
    Returns a dict with all trading indicators:
    - atr, ema_20
    - plus_di, minus_di (current candle)
    - prev_plus_di, prev_minus_di (previous candle)
    - di_crossed (bool), di_cross_type ("bullish"/"bearish"/None)
    - adx, adx_prev2 (ADX 2 candles ago for rising check)
    - rsi
    - macd_line, signal_line, macd_histogram, prev_macd_histogram
    - macd_hist_dying_long (bool), macd_hist_dying_short (bool)
    - current_price (live tick via WebSocket or REST)
    - last_close (closed candle price - used for decisions)
    - last_candle_ts (for dedup)
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
        df["high"] = pd.to_numeric(df["high"], errors="coerce")
        df["low"] = pd.to_numeric(df["low"], errors="coerce")
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

        if df["close"].isna().any():
            raise ValueError("Invalid prices")

        # 1. ATR (14) using Wilder's smoothing
        df["prev_close"] = df["close"].shift(1)
        df["tr1"] = df["high"] - df["low"]
        df["tr2"] = (df["high"] - df["prev_close"]).abs()
        df["tr3"] = (df["low"] - df["prev_close"]).abs()
        df["tr"] = df[["tr1", "tr2", "tr3"]].max(axis=1)
        df["atr"] = df["tr"].ewm(alpha=1/14, min_periods=14).mean()
        atr = float(df["atr"].iloc[-1])

        # 2. 20 EMA (30-minute chart)
        df["ema_20"] = df["close"].ewm(span=20, adjust=False).mean()
        ema_20 = float(df["ema_20"].iloc[-1])

        # 3. DMI: +DI / -DI (14-period, Wilder's smoothing)
        df["up_move"] = df["high"] - df["high"].shift(1)
        df["down_move"] = df["low"].shift(1) - df["low"]

        df["+dm"] = 0.0
        df["-dm"] = 0.0
        plus_dm_mask = (df["up_move"] > df["down_move"]) & (df["up_move"] > 0)
        minus_dm_mask = (df["down_move"] > df["up_move"]) & (df["down_move"] > 0)
        df.loc[plus_dm_mask, "+dm"] = df.loc[plus_dm_mask, "up_move"]
        df.loc[minus_dm_mask, "-dm"] = df.loc[minus_dm_mask, "down_move"]

        df["+dm_smooth"] = df["+dm"].ewm(alpha=1/14, min_periods=14).mean()
        df["-dm_smooth"] = df["-dm"].ewm(alpha=1/14, min_periods=14).mean()

        df["+di"] = (df["+dm_smooth"] / df["atr"]) * 100
        df["-di"] = (df["-dm_smooth"] / df["atr"]) * 100

        plus_di = float(df["+di"].iloc[-1])
        minus_di = float(df["-di"].iloc[-1])
        prev_plus_di = float(df["+di"].iloc[-2])
        prev_minus_di = float(df["-di"].iloc[-2])

        # 4. ADX (Average Directional Index) - 14-period Wilder's smoothing of DX
        di_sum = df["+di"] + df["-di"]
        di_sum = di_sum.replace(0, float('nan'))
        df["dx"] = (abs(df["+di"] - df["-di"]) / di_sum) * 100
        df["adx"] = df["dx"].ewm(alpha=1/14, min_periods=14).mean()
        adx = float(df["adx"].iloc[-1])
        # ADX value from 2 candles ago (for reference/logging)
        adx_prev2 = float(df["adx"].iloc[-3]) if len(df) >= 3 else adx

        # 5. RSI (14-period, Wilder's smoothing)
        delta = df["close"].diff()
        gain = delta.where(delta > 0, 0.0)
        loss = (-delta.where(delta < 0, 0.0))
        avg_gain = gain.ewm(alpha=1/14, min_periods=14).mean()
        avg_loss = loss.ewm(alpha=1/14, min_periods=14).mean()
        rs = avg_gain / avg_loss.replace(0, float('nan'))
        df["rsi"] = 100 - (100 / (1 + rs))
        rsi = float(df["rsi"].iloc[-1]) if not pd.isna(df["rsi"].iloc[-1]) else 50.0

        # 6. MACD (12, 26, 9)
        df["ema_12"] = df["close"].ewm(span=12, adjust=False).mean()
        df["ema_26"] = df["close"].ewm(span=26, adjust=False).mean()
        df["macd_line"] = df["ema_12"] - df["ema_26"]
        df["signal_line"] = df["macd_line"].ewm(span=9, adjust=False).mean()
        df["macd_histogram"] = df["macd_line"] - df["signal_line"]

        macd_line = float(df["macd_line"].iloc[-1])
        signal_line = float(df["signal_line"].iloc[-1])
        macd_histogram = float(df["macd_histogram"].iloc[-1])
        prev_macd_histogram = float(df["macd_histogram"].iloc[-2]) if len(df) >= 2 else macd_histogram
        prev2_macd_histogram = float(df["macd_histogram"].iloc[-3]) if len(df) >= 3 else prev_macd_histogram

        # Histogram momentum: block only on 2+ consecutive declining/rising bars
        # For longs: block if histogram declined for 2 consecutive bars (dying momentum)
        #   e.g., 5 -> 3 -> 1 = blocked, but 5 -> 4 -> 6 = allowed
        # For shorts: block if histogram rose for 2 consecutive bars (dying downward momentum)
        #   e.g., -5 -> -3 -> -1 = blocked, but -5 -> -4 -> -6 = allowed
        macd_hist_dying_long = (macd_histogram < prev_macd_histogram) and (prev_macd_histogram < prev2_macd_histogram)
        macd_hist_dying_short = (macd_histogram > prev_macd_histogram) and (prev_macd_histogram > prev2_macd_histogram)

        # Detect DI crossover on the latest candle
        prev_diff = prev_plus_di - prev_minus_di
        curr_diff = plus_di - minus_di

        di_crossed = False
        di_cross_type = None
        if prev_diff <= 0 and curr_diff > 0:
            di_crossed = True
            di_cross_type = "bullish"
        elif prev_diff >= 0 and curr_diff < 0:
            di_crossed = True
            di_cross_type = "bearish"

        last_candle_ts = str(df["timestamp"].iloc[-1])

        # Last closed candle price (used for all trading decisions)
        last_close = float(df["close"].iloc[-2])

        # Current price from WebSocket (instant) or REST ticker (fallback)
        current_price = await get_current_price(symbol)
        if current_price is None:
            current_price = last_close  # ultimate fallback

        return {
            "atr": atr,
            "ema_20": ema_20,
            "plus_di": plus_di,
            "minus_di": minus_di,
            "prev_plus_di": prev_plus_di,
            "prev_minus_di": prev_minus_di,
            "di_crossed": di_crossed,
            "di_cross_type": di_cross_type,
            "adx": adx,
            "adx_prev2": adx_prev2,
            "rsi": rsi,
            "macd_line": macd_line,
            "signal_line": signal_line,
            "macd_histogram": macd_histogram,
            "prev_macd_histogram": prev_macd_histogram,
            "prev2_macd_histogram": prev2_macd_histogram,
            "macd_hist_dying_long": macd_hist_dying_long,
            "macd_hist_dying_short": macd_hist_dying_short,
            "current_price": current_price,
            "last_close": last_close,
            "last_candle_ts": last_candle_ts,
        }

    except Exception as exc:
        print(f"Error in get_all_indicators for {symbol}: {exc}")
        return None


async def get_macd_10min(symbol: str) -> Optional[dict]:
    """
    Fetch 10-minute klines and compute MACD (12, 26, 9).
    Returns:
    - Current (forming) candle's MACD/Signal values (for entry filter & reversal checks)
    - Last two CLOSED candles' MACD/Signal values + crossover detection (for partial exit/re-entry)
    This prevents whipsaw from intra-candle noise for partial exits,
    while still allowing instant entry/reversal filter checks.
    """
    try:
        resp = await asyncio.to_thread(
            bybit_session.get_kline,
            category="linear",
            symbol=symbol,
            interval="10",
            limit=60,
        )
        klines = resp["result"]["list"]
        if not klines:
            raise ValueError("No 10-min klines returned")

        df = pd.DataFrame(
            klines,
            columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"],
        ).iloc[::-1].reset_index(drop=True)
        df["close"] = pd.to_numeric(df["close"], errors="coerce")

        if df["close"].isna().any():
            raise ValueError("Invalid 10-min prices")

        # MACD (12, 26, 9)
        df["ema_12"] = df["close"].ewm(span=12, adjust=False).mean()
        df["ema_26"] = df["close"].ewm(span=26, adjust=False).mean()
        df["macd_line"] = df["ema_12"] - df["ema_26"]
        df["signal_line"] = df["macd_line"].ewm(span=9, adjust=False).mean()

        # Current (forming) candle — used for entry filters and reversal checks
        current_macd = float(df["macd_line"].iloc[-1])
        current_signal = float(df["signal_line"].iloc[-1])

        # Use iloc[-2] = last CLOSED candle, iloc[-3] = candle before that
        # iloc[-1] is the currently forming (incomplete) candle — ignored for crossover detection
        prev_macd = float(df["macd_line"].iloc[-3])
        prev_signal = float(df["signal_line"].iloc[-3])
        last_macd = float(df["macd_line"].iloc[-2])
        last_signal = float(df["signal_line"].iloc[-2])
        last_closed_ts = str(df["timestamp"].iloc[-2])

        # Detect confirmed crossovers on closed candles
        # Bullish crossover: MACD was below Signal, now above
        bullish_cross = (prev_macd <= prev_signal) and (last_macd > last_signal)
        # Bearish crossover: MACD was above Signal, now below
        bearish_cross = (prev_macd >= prev_signal) and (last_macd < last_signal)

        return {
            # Current (forming) candle values — for entry/reversal filter checks
            "macd_10m_current": current_macd,
            "signal_10m_current": current_signal,
            # Last closed candle values — for crossover confirmation
            "macd_10m": last_macd,
            "signal_10m": last_signal,
            "prev_macd_10m": prev_macd,
            "prev_signal_10m": prev_signal,
            "bullish_cross_10m": bullish_cross,
            "bearish_cross_10m": bearish_cross,
            "last_closed_ts_10m": last_closed_ts,
        }

    except Exception as exc:
        print(f"Error in get_macd_10min for {symbol}: {exc}")
        return None




async def get_open_position_side(symbol: str) -> Optional[str]:
    try:
        positions_resp = await asyncio.to_thread(
            bybit_session.get_positions,
            category="linear",
            symbol=symbol
        )
        positions = positions_resp["result"]["list"]
        for pos in positions:
            if float(pos["size"]) > 0:
                return pos["side"]
        return None
    except Exception as e:
        print(f"Error getting open position for {symbol}: {e}")
        return None

async def get_instrument_constraints(symbol: str) -> Tuple[float, float, float, float]:
    try:
        info = await asyncio.to_thread(
            bybit_session.get_instruments_info,
            category="linear",
            symbol=symbol
        )
        if info["retCode"] != 0:
            raise ValueError(info["retMsg"])

        instrument = info["result"]["list"][0]
        lot_filter = instrument["lotSizeFilter"]
        min_qty = float(lot_filter["minOrderQty"])
        max_qty = float(lot_filter["maxMktOrderQty"])
        qty_step = float(lot_filter["qtyStep"])
        min_notional = float(lot_filter.get("minNotionalValue", 5.0))

        print(f"[DEBUG] {symbol} constraints -> min_qty={min_qty}, max_qty={max_qty}, qty_step={qty_step}, min_notional={min_notional}")

        return min_qty, max_qty, qty_step, min_notional

    except Exception as e:
        print(f"Failed to get constraints for {symbol}: {e}")
        return 0.001, 1000000.0, 0.001, 5.0

def round_qty_to_step(qty: float, qty_step: float, min_qty: float) -> float:
    """Round quantity DOWN to the nearest valid step increment, ensuring it meets the minimum."""
    if qty_step > 0:
        qty = (qty // qty_step) * qty_step
    return max(qty, min_qty)

def format_qty_str(qty: float, qty_step: float) -> str:
    """Format quantity as a string with the correct decimal precision derived from qtyStep."""
    if qty_step >= 1:
        return str(int(qty))
    step_str = f"{qty_step:.10f}".rstrip('0')
    decimals = len(step_str.split('.')[1]) if '.' in step_str else 0
    return f"{qty:.{decimals}f}"

async def get_trade_qty(symbol: str) -> float:
    try:
        if symbol in symbol_manual_quantities and symbol_manual_quantities[symbol] is not None:
            manual_qty = symbol_manual_quantities[symbol]
            min_qty, max_qty, qty_step, _ = await get_instrument_constraints(symbol)
            return round_qty_to_step(manual_qty, qty_step, min_qty)

        balance_resp = await asyncio.to_thread(
            bybit_session.get_wallet_balance,
            accountType="UNIFIED"
        )
        if balance_resp["retCode"] != 0:
            raise ValueError(f"Balance API error: {balance_resp['retMsg']}")
        result = balance_resp["result"]
        if not result["list"]:
            raise ValueError("No account data in response")
        account = result["list"][0]
        total_avail_usd_str = account.get("totalAvailableBalance", "0")
        total_avail_usd = float(total_avail_usd_str)

        current_price = await get_current_price(symbol)
        min_qty, max_qty, qty_step, min_notional = await get_instrument_constraints(symbol)

        if current_price is None or current_price <= 0:
            return min_qty

        risk_amount_usd = total_avail_usd * 0.20
        risk_amount_usd = max(risk_amount_usd, min_notional + 1.0)

        raw_qty = risk_amount_usd / current_price

        if qty_step > 0:
            adjusted_qty = max(min_qty, (raw_qty // qty_step) * qty_step)
        else:
            adjusted_qty = max(min_qty, raw_qty)

        if adjusted_qty * current_price < min_notional:
            adjusted_qty = (min_notional / current_price) + qty_step
            if qty_step > 0:
                adjusted_qty = (adjusted_qty // qty_step) * qty_step
            adjusted_qty = max(min_qty, adjusted_qty)

        return adjusted_qty
    except Exception as exc:
        print(f"Failed to calculate qty for {symbol}: {exc}")
        min_qty, _, _, _ = await get_instrument_constraints(symbol)
        return min_qty

async def close_position(symbol: str, side_to_close: str = "Buy") -> Optional[float]:
    """
    Closes the position and returns the total closed quantity.
    Also clears risk management state for the symbol.
    """
    try:
        positions_resp = await asyncio.to_thread(
            bybit_session.get_positions,
            category="linear",
            symbol=symbol
        )
        positions = positions_resp["result"]["list"]

        for pos in positions:
            if pos["side"] == side_to_close and float(pos["size"]) > 0:
                close_side = "Sell" if side_to_close == "Buy" else "Buy"
                total_qty = float(pos["size"])

                min_qty, max_qty, qty_step, _ = await get_instrument_constraints(symbol)

                remaining_qty = total_qty

                while remaining_qty > 0:
                    current_batch_qty = min(remaining_qty, max_qty)
                    current_batch_qty = round_qty_to_step(current_batch_qty, qty_step, min_qty)

                    qty_str = format_qty_str(current_batch_qty, qty_step)

                    order = await asyncio.to_thread(
                        bybit_session.place_order,
                        category="linear",
                        symbol=symbol,
                        side=close_side,
                        orderType="Market",
                        qty=qty_str,
                        reduceOnly=True,
                    )

                    if order.get("retCode") != 0:
                        print(f"Close order batch failed for {symbol}: {order.get('retMsg')}")
                        return None

                    remaining_qty -= current_batch_qty

                    if remaining_qty > 0:
                        await asyncio.sleep(0.5)

                # Clear risk management state on close
                _clear_risk_state(symbol)

                return total_qty
        return None
    except Exception as exc:
        print(f"Position close failed for {symbol}: {exc}")
        return None


async def close_partial_position(symbol: str, side_to_close: str = "Buy", close_pct: float = 0.50) -> Optional[float]:
    """
    Closes a percentage of the position (e.g., 50%) and returns the closed quantity.
    Does NOT clear risk management state — the remaining position stays open.
    """
    try:
        positions_resp = await asyncio.to_thread(
            bybit_session.get_positions,
            category="linear",
            symbol=symbol
        )
        positions = positions_resp["result"]["list"]

        for pos in positions:
            if pos["side"] == side_to_close and float(pos["size"]) > 0:
                close_side = "Sell" if side_to_close == "Buy" else "Buy"
                total_qty = float(pos["size"])

                min_qty, max_qty, qty_step, _ = await get_instrument_constraints(symbol)

                # Calculate the partial quantity to close
                partial_qty = total_qty * close_pct
                partial_qty = round_qty_to_step(partial_qty, qty_step, min_qty)

                # If partial_qty equals total position (e.g., position too small to split), close all
                if partial_qty >= total_qty:
                    partial_qty = total_qty

                remaining_to_close = partial_qty

                while remaining_to_close > 0:
                    current_batch_qty = min(remaining_to_close, max_qty)
                    current_batch_qty = round_qty_to_step(current_batch_qty, qty_step, min_qty)

                    qty_str = format_qty_str(current_batch_qty, qty_step)

                    order = await asyncio.to_thread(
                        bybit_session.place_order,
                        category="linear",
                        symbol=symbol,
                        side=close_side,
                        orderType="Market",
                        qty=qty_str,
                        reduceOnly=True,
                    )

                    if order.get("retCode") != 0:
                        print(f"Partial close batch failed for {symbol}: {order.get('retMsg')}")
                        return None

                    remaining_to_close -= current_batch_qty

                    if remaining_to_close > 0:
                        await asyncio.sleep(0.5)

                print(f"[INFO] {symbol}: Partially closed {partial_qty} of {total_qty} ({close_pct*100:.0f}%)")
                return partial_qty
        return None
    except Exception as exc:
        print(f"Partial position close failed for {symbol}: {exc}")
        return None


def _clear_risk_state(symbol: str) -> None:
    """Clear per-symbol risk management tracking after closing a position."""
    symbol_entry_price[symbol] = None
    symbol_entry_atr[symbol] = None
    symbol_peak_price[symbol] = None
    symbol_trailing_active[symbol] = False
    symbol_partial_taken[symbol] = False
    symbol_server_sl_set[symbol] = False
    symbol_server_ts_set[symbol] = False
    symbol_trail_order_id[symbol] = None
    symbol_trail_qty[symbol] = None
    symbol_trail_price[symbol] = None
    # Stage 2 trailing
    symbol_stage2_active[symbol] = False
    symbol_stage2_taken[symbol] = False
    symbol_trail2_order_id[symbol] = None
    symbol_trail2_qty[symbol] = None
    symbol_trail2_price[symbol] = None
    symbol_peak_stage2[symbol] = None
    # Tiered stop-loss
    symbol_sl1_order_id[symbol] = None
    symbol_sl1_qty[symbol] = None
    symbol_sl1_taken[symbol] = False
    symbol_original_qty[symbol] = None
    # MACD partial exit
    symbol_macd_partial_closed[symbol] = False
    symbol_macd_partial_qty[symbol] = None
    symbol_macd_last_action_candle[symbol] = None
    save_state()


def _set_entry_risk_state(symbol: str, entry_price: float, atr: float) -> None:
    """Set per-symbol risk management tracking after opening a position."""
    symbol_entry_price[symbol] = entry_price
    symbol_entry_atr[symbol] = atr
    symbol_peak_price[symbol] = entry_price
    symbol_trailing_active[symbol] = False
    symbol_partial_taken[symbol] = False
    symbol_server_sl_set[symbol] = False
    symbol_server_ts_set[symbol] = False
    symbol_trail_order_id[symbol] = None
    symbol_trail_qty[symbol] = None
    symbol_trail_price[symbol] = None
    # Stage 2 trailing
    symbol_stage2_active[symbol] = False
    symbol_stage2_taken[symbol] = False
    symbol_trail2_order_id[symbol] = None
    symbol_trail2_qty[symbol] = None
    symbol_trail2_price[symbol] = None
    symbol_peak_stage2[symbol] = None
    # Tiered stop-loss
    symbol_sl1_order_id[symbol] = None
    symbol_sl1_qty[symbol] = None
    symbol_sl1_taken[symbol] = False
    symbol_original_qty[symbol] = None
    # MACD partial exit
    symbol_macd_partial_closed[symbol] = False
    symbol_macd_partial_qty[symbol] = None
    symbol_macd_last_action_candle[symbol] = None
    save_state()


# ────────────────────────────────────────────────
#  Server-Side Stop-Loss & Trailing Stop (Bybit)
# ────────────────────────────────────────────────

async def set_server_stop_loss(symbol: str, side: str, stop_price: float) -> bool:
    """
    Set a server-side stop-loss on Bybit for the full position.
    Bybit's engine will execute this instantly when price hits the stop level.
    """
    try:
        resp = await asyncio.to_thread(
            bybit_session.set_trading_stop,
            category="linear",
            symbol=symbol,
            stopLoss=str(stop_price),
            slTriggerBy="LastPrice",
            tpslMode="Full",
            positionIdx=0,
        )
        if resp.get("retCode") == 0:
            symbol_server_sl_set[symbol] = True
            print(f"[SERVER-SL] {symbol}: Stop-loss set at {stop_price:.6g} (Full position)")
            return True
        else:
            print(f"[SERVER-SL] {symbol}: Failed to set SL - {resp.get('retMsg')}")
            return False
    except Exception as e:
        print(f"[SERVER-SL] {symbol}: Error setting stop-loss: {e}")
        return False


async def get_position_size(symbol: str) -> Optional[float]:
    """Get current position size (qty) for a symbol. Returns None if no position."""
    try:
        resp = await asyncio.to_thread(
            bybit_session.get_positions,
            category="linear",
            symbol=symbol,
        )
        positions = resp.get("result", {}).get("list", [])
        for pos in positions:
            size = float(pos.get("size", 0))
            if size > 0:
                return size
        return None
    except Exception as e:
        print(f"Error fetching position size for {symbol}: {e}")
        return None


async def get_position_entry_price(symbol: str) -> Optional[float]:
    """Get the actual average entry price of the current position from Bybit.
    This is the real fill price, not the candle close price at signal time."""
    try:
        resp = await asyncio.to_thread(
            bybit_session.get_positions,
            category="linear",
            symbol=symbol,
        )
        positions = resp.get("result", {}).get("list", [])
        for pos in positions:
            size = float(pos.get("size", 0))
            if size > 0:
                avg_price = float(pos.get("avgPrice", 0))
                if avg_price > 0:
                    print(f"[INFO] {symbol}: Bybit avgPrice = {avg_price}")
                    return avg_price
        return None
    except Exception as e:
        print(f"Error fetching position entry price for {symbol}: {e}")
        return None


async def place_trail_stop_order(symbol: str, side: str, qty: float, trigger_price: float) -> Optional[str]:
    """
    Place a conditional stop-market order for partial trailing close (60%).
    Returns the order ID if successful, None otherwise.
    - For Long positions: triggers when price FALLS BELOW trigger_price
    - For Short positions: triggers when price RISES ABOVE trigger_price
    """
    close_side = "Sell" if side == "Buy" else "Buy"
    # triggerDirection: 1 = trigger when price rises above, 2 = trigger when price falls below
    trigger_dir = 2 if side == "Buy" else 1

    try:
        # Round qty to instrument step size
        min_qty, max_qty, qty_step, _ = await get_instrument_constraints(symbol)
        rounded_qty = round_qty_to_step(qty, qty_step, min_qty)
        qty_str = format_qty_str(rounded_qty, qty_step)

        resp = await asyncio.to_thread(
            bybit_session.place_order,
            category="linear",
            symbol=symbol,
            side=close_side,
            orderType="Market",
            qty=qty_str,
            triggerPrice=str(trigger_price),
            triggerDirection=trigger_dir,
            triggerBy="LastPrice",
            reduceOnly=True,
        )
        if resp.get("retCode") == 0:
            order_id = resp.get("result", {}).get("orderId")
            symbol_trail_order_id[symbol] = order_id
            symbol_trail_qty[symbol] = rounded_qty
            symbol_trail_price[symbol] = trigger_price
            symbol_server_ts_set[symbol] = True
            print(f"[TRAIL-ORDER] {symbol}: Conditional stop placed for {rounded_qty} @ {trigger_price:.6g} (order: {order_id})")
            return order_id
        else:
            print(f"[TRAIL-ORDER] {symbol}: Failed to place - {resp.get('retMsg')}")
            return None
    except Exception as e:
        print(f"[TRAIL-ORDER] {symbol}: Error placing trail order: {e}")
        return None


async def amend_trail_stop_price(symbol: str, new_trigger_price: float) -> bool:
    """Update the trigger price of the trailing stop conditional order."""
    order_id = symbol_trail_order_id.get(symbol)
    if not order_id:
        print(f"[TRAIL-ORDER] {symbol}: No trail order to amend")
        return False
    try:
        resp = await asyncio.to_thread(
            bybit_session.amend_order,
            category="linear",
            symbol=symbol,
            orderId=order_id,
            triggerPrice=str(new_trigger_price),
        )
        if resp.get("retCode") == 0:
            symbol_trail_price[symbol] = new_trigger_price
            print(f"[TRAIL-ORDER] {symbol}: Trail price updated to {new_trigger_price:.6g}")
            return True
        else:
            print(f"[TRAIL-ORDER] {symbol}: Amend failed - {resp.get('retMsg')}")
            return False
    except Exception as e:
        print(f"[TRAIL-ORDER] {symbol}: Error amending: {e}")
        return False


async def cancel_trail_stop_order(symbol: str) -> bool:
    """Cancel the active trailing stop conditional order."""
    order_id = symbol_trail_order_id.get(symbol)
    if not order_id:
        return True  # Nothing to cancel
    try:
        resp = await asyncio.to_thread(
            bybit_session.cancel_order,
            category="linear",
            symbol=symbol,
            orderId=order_id,
        )
        if resp.get("retCode") == 0:
            print(f"[TRAIL-ORDER] {symbol}: Trail stop order cancelled (order: {order_id})")
        else:
            print(f"[TRAIL-ORDER] {symbol}: Cancel failed - {resp.get('retMsg')} (may already be filled)")
        symbol_trail_order_id[symbol] = None
        symbol_trail_qty[symbol] = None
        symbol_trail_price[symbol] = None
        symbol_server_ts_set[symbol] = False
        return True
    except Exception as e:
        print(f"[TRAIL-ORDER] {symbol}: Error cancelling: {e}")
        return False


async def place_conditional_close_order(symbol: str, side: str, qty: float, trigger_price: float) -> Optional[str]:
    """
    Place a server-side conditional stop-market order to close a portion of position.
    Generic function used for SL tiers and stage 2 trailing.
    Returns order_id if successful, None otherwise.
    """
    close_side = "Sell" if side == "Buy" else "Buy"
    trigger_dir = 2 if side == "Buy" else 1  # 2=falls below, 1=rises above

    try:
        min_qty, max_qty, qty_step, _ = await get_instrument_constraints(symbol)
        rounded_qty = round_qty_to_step(qty, qty_step, min_qty)
        qty_str = format_qty_str(rounded_qty, qty_step)

        resp = await asyncio.to_thread(
            bybit_session.place_order,
            category="linear",
            symbol=symbol,
            side=close_side,
            orderType="Market",
            qty=qty_str,
            triggerPrice=str(trigger_price),
            triggerDirection=trigger_dir,
            triggerBy="LastPrice",
            reduceOnly=True,
        )
        if resp.get("retCode") == 0:
            order_id = resp.get("result", {}).get("orderId")
            print(f"[CONDITIONAL] {symbol}: Stop order placed for {rounded_qty} @ {trigger_price:.6g} (order: {order_id})")
            return order_id
        else:
            print(f"[CONDITIONAL] {symbol}: Failed to place - {resp.get('retMsg')}")
            return None
    except Exception as e:
        print(f"[CONDITIONAL] {symbol}: Error placing order: {e}")
        return None


async def amend_conditional_order(symbol: str, order_id: str, new_trigger_price: float) -> bool:
    """Amend trigger price of any conditional order by ID."""
    try:
        resp = await asyncio.to_thread(
            bybit_session.amend_order,
            category="linear",
            symbol=symbol,
            orderId=order_id,
            triggerPrice=str(new_trigger_price),
        )
        if resp.get("retCode") == 0:
            print(f"[CONDITIONAL] {symbol}: Order {order_id} amended to {new_trigger_price:.6g}")
            return True
        else:
            print(f"[CONDITIONAL] {symbol}: Amend failed - {resp.get('retMsg')}")
            return False
    except Exception as e:
        print(f"[CONDITIONAL] {symbol}: Error amending: {e}")
        return False


async def cancel_conditional_order_by_id(symbol: str, order_id: str) -> bool:
    """Cancel a specific conditional order by ID."""
    try:
        resp = await asyncio.to_thread(
            bybit_session.cancel_order,
            category="linear",
            symbol=symbol,
            orderId=order_id,
        )
        if resp.get("retCode") == 0:
            print(f"[CONDITIONAL] {symbol}: Order {order_id} cancelled")
            return True
        else:
            print(f"[CONDITIONAL] {symbol}: Cancel failed - {resp.get('retMsg')} (may already be filled)")
            return False
    except Exception as e:
        print(f"[CONDITIONAL] {symbol}: Error cancelling order: {e}")
        return False


async def cancel_stage2_trail_order(symbol: str) -> bool:
    """Cancel the stage 2 trailing stop conditional order."""
    order_id = symbol_trail2_order_id.get(symbol)
    if not order_id:
        return True
    await cancel_conditional_order_by_id(symbol, order_id)
    symbol_trail2_order_id[symbol] = None
    symbol_trail2_qty[symbol] = None
    symbol_trail2_price[symbol] = None
    symbol_stage2_active[symbol] = False
    return True


async def cancel_sl1_order(symbol: str) -> bool:
    """Cancel the SL stage 1 (30% at 1.5x ATR) conditional order."""
    order_id = symbol_sl1_order_id.get(symbol)
    if not order_id:
        return True
    await cancel_conditional_order_by_id(symbol, order_id)
    symbol_sl1_order_id[symbol] = None
    symbol_sl1_qty[symbol] = None
    return True


async def cancel_server_stops(symbol: str) -> bool:
    """
    Cancel all server-side TP/SL/trailing stops + any conditional trail/SL orders.
    """
    # Cancel conditional orders first
    await cancel_trail_stop_order(symbol)
    await cancel_stage2_trail_order(symbol)
    await cancel_sl1_order(symbol)
    try:
        resp = await asyncio.to_thread(
            bybit_session.set_trading_stop,
            category="linear",
            symbol=symbol,
            stopLoss="0",
            trailingStop="0",
            tpslMode="Full",
            positionIdx=0,
        )
        if resp.get("retCode") == 0:
            symbol_server_sl_set[symbol] = False
            symbol_server_ts_set[symbol] = False
            print(f"[SERVER] {symbol}: All server-side stops cancelled")
            return True
        else:
            print(f"[SERVER] {symbol}: Failed to cancel stops - {resp.get('retMsg')}")
            return False
    except Exception as e:
        print(f"[SERVER] {symbol}: Error cancelling stops: {e}")
        return False


async def place_trade(symbol: str, side: str = "Buy", manual_qty_override: Optional[float] = None) -> Optional[dict]:
    """
    Places a trade, optionally using a specific manual quantity override for reversals.
    """
    if manual_qty_override:
        total_qty = manual_qty_override
    else:
        total_qty = await get_trade_qty(symbol)

    try:
        min_qty, max_qty, qty_step, _ = await get_instrument_constraints(symbol)

        total_qty = round_qty_to_step(total_qty, qty_step, min_qty)

        remaining_qty = total_qty
        successful_orders = []

        print(f"[DEBUG] {symbol} place_trade -> total_qty={total_qty}, max_qty={max_qty}, qty_step={qty_step}")

        while remaining_qty > 0:
            current_batch_qty = min(remaining_qty, max_qty)
            current_batch_qty = round_qty_to_step(current_batch_qty, qty_step, min_qty)

            qty_str = format_qty_str(current_batch_qty, qty_step)

            print(f"[DEBUG] {symbol} batch -> qty_str={qty_str}, remaining_after={remaining_qty - current_batch_qty}")

            order = await asyncio.to_thread(
                bybit_session.place_order,
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Market",
                qty=qty_str,
            )

            if order.get("retCode") != 0:
                print(f"Trade order batch rejected for {symbol} (qty={qty_str}): {order.get('retMsg')}")
                return {"error": order.get("retMsg"), "partial": len(successful_orders) > 0}

            successful_orders.append(order.get("result"))
            remaining_qty -= current_batch_qty

            if remaining_qty > 0:
                await asyncio.sleep(0.5)

        return successful_orders[-1] if successful_orders else {"error": "No orders placed"}

    except Exception as exc:
        print(f"Trade placement failed for {symbol}: {exc}")
        return {"error": str(exc)}


async def monitor_symbol(context: ContextTypes.DEFAULT_TYPE) -> None:
    symbol = context.job.data["symbol"]
    previous_trend = symbol_trends.get(symbol, None)
    current_open_trade_side = await get_open_position_side(symbol)
    symbol_open_trade_side[symbol] = current_open_trade_side

    indicators = await get_all_indicators(symbol)
    if indicators is None:
        print(f"Skipping monitoring for {symbol}: Could not fetch indicators")
        return

    current_price = indicators["current_price"]
    last_close = indicators["last_close"]       # CLOSED candle price (for decisions)
    ema_20 = indicators["ema_20"]
    atr = indicators["atr"]
    adx = indicators["adx"]
    adx_prev2 = indicators["adx_prev2"]
    rsi = indicators["rsi"]
    macd_line = indicators["macd_line"]
    signal_line = indicators["signal_line"]
    macd_histogram = indicators["macd_histogram"]
    prev_macd_histogram = indicators["prev_macd_histogram"]
    prev2_macd_histogram = indicators["prev2_macd_histogram"]
    macd_hist_dying_long = indicators["macd_hist_dying_long"]
    macd_hist_dying_short = indicators["macd_hist_dying_short"]
    plus_di = indicators["plus_di"]
    minus_di = indicators["minus_di"]
    di_crossed = indicators["di_crossed"]
    di_cross_type = indicators["di_cross_type"]
    last_candle_ts = indicators["last_candle_ts"]

    # Fetch 10-minute MACD for partial exit/re-entry decisions (closed-candle confirmation)
    macd_10m_indicators = await get_macd_10min(symbol)

    chat_id = context.job.chat_id if context.job else CHAT_ID

    # Use CLOSED candle price for all market state decisions
    if last_close > ema_20:
        current_market_state = "ABOVE 20 EMA (Bullish)"
    elif last_close < ema_20:
        current_market_state = "BELOW 20 EMA (Bearish)"
    else:
        current_market_state = "AT 20 EMA (Neutral)"

    # ──────────────────────────────────────────────
    # 1. RISK MANAGEMENT: Multi-stage trailing + tiered stop-loss
    #    STOP-LOSS (price goes against us):
    #      - Server-side SL at 2x ATR (safety net, closes remaining 70%)
    #      - Conditional order at 1.5x ATR closes 30% INSTANTLY
    #      - If price continues to 2x ATR, server SL closes remaining 70%
    #    TRAILING STOP (price goes in our favor):
    #      - Stage 1: At 1.5x ATR profit, trail on 60%. Bybit closes INSTANTLY when hit.
    #      - Stage 2: After 60% closed, trail on 30% (1x ATR distance). Bybit closes INSTANTLY.
    #      - Stage 3: Remaining 10% has NO trail — exits only on Exit Rules (EMA+MACD).
    #    Uses current_price (real-time) for all risk checks
    # ──────────────────────────────────────────────
    if current_open_trade_side is not None:
        entry_price = symbol_entry_price.get(symbol)
        entry_atr = symbol_entry_atr.get(symbol)

        # If we have a position but no tracked entry (e.g., bot restarted), set defaults
        if entry_price is None or entry_atr is None:
            entry_price = current_price
            entry_atr = atr
            _set_entry_risk_state(symbol, entry_price, entry_atr)
            print(f"[INFO] {symbol}: Initialized risk state from current values (entry={entry_price}, atr={entry_atr})")
            # Set server-side stop-loss for recovered position
            if current_open_trade_side == "Buy":
                sl_price = entry_price - (STOP_LOSS_ATR_MULT * entry_atr)
            else:
                sl_price = entry_price + (STOP_LOSS_ATR_MULT * entry_atr)
            await set_server_stop_loss(symbol, current_open_trade_side, sl_price)

        # Ensure server-side stop-loss is set (retry if not) — skip if trailing is active
        if not symbol_server_sl_set.get(symbol, False) and not symbol_partial_taken.get(symbol, False) and not symbol_trailing_active.get(symbol, False):
            if current_open_trade_side == "Buy":
                sl_price = entry_price - (STOP_LOSS_ATR_MULT * entry_atr)
            else:
                sl_price = entry_price + (STOP_LOSS_ATR_MULT * entry_atr)
            await set_server_stop_loss(symbol, current_open_trade_side, sl_price)

        # Ensure SL1 conditional order (30% at 1.5x ATR) is placed — skip if trailing active or SL1 already taken/set
        if not symbol_sl1_order_id.get(symbol) and not symbol_sl1_taken.get(symbol, False) and not symbol_trailing_active.get(symbol, False) and not symbol_partial_taken.get(symbol, False):
            pos_qty = await get_position_size(symbol)
            if pos_qty:
                sl1_qty = pos_qty * SL_STAGE1_CLOSE_PCT
                if current_open_trade_side == "Buy":
                    sl1_price = entry_price - (SL_STAGE1_ATR_MULT * entry_atr)
                else:
                    sl1_price = entry_price + (SL_STAGE1_ATR_MULT * entry_atr)
                sl1_order_id = await place_conditional_close_order(symbol, current_open_trade_side, sl1_qty, sl1_price)
                if sl1_order_id:
                    min_qty, _, qty_step, _ = await get_instrument_constraints(symbol)
                    symbol_sl1_order_id[symbol] = sl1_order_id
                    symbol_sl1_qty[symbol] = round_qty_to_step(sl1_qty, qty_step, min_qty)
                    symbol_original_qty[symbol] = pos_qty
                    print(f"[SL-TIER1] {symbol}: Placed 30% SL order ({symbol_sl1_qty[symbol]}) at {sl1_price:.6g} (1.5x ATR)")

        # Update peak price tracking using REAL-TIME price (not last_close)
        peak = symbol_peak_price.get(symbol, entry_price)
        if current_open_trade_side == "Buy":
            peak = max(peak, current_price)
        else:  # Sell
            peak = min(peak, current_price)
        symbol_peak_price[symbol] = peak

        # ── TIERED STOP-LOSS: Detect SL1 (30% at 1.5x ATR) fill ──
        # Only check if SL1 order was placed and not yet confirmed as taken, and trailing not active
        sl1_taken = symbol_sl1_taken.get(symbol, False)
        if not sl1_taken and symbol_sl1_order_id.get(symbol) and not symbol_trailing_active.get(symbol, False):
            current_qty = await get_position_size(symbol)
            original_qty = symbol_original_qty.get(symbol)
            sl1_qty_val = symbol_sl1_qty.get(symbol)

            if current_qty is not None and original_qty and sl1_qty_val:
                # If position reduced by roughly the SL1 amount, SL1 has fired
                if current_qty <= original_qty - (sl1_qty_val * 0.8):
                    symbol_sl1_taken[symbol] = True
                    sl1_taken = True
                    symbol_sl1_order_id[symbol] = None
                    symbol_sl1_qty[symbol] = None
                    save_state()
                    side_label = "Long" if current_open_trade_side == "Buy" else "Short"
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            f"\U0001f6d1 SL TIER 1 HIT for {symbol} ({side_label})\n"
                            f"30% closed at 1.5x ATR loss\n"
                            f"Entry: {entry_price:.6g} | Current: {current_price:.6g}\n"
                            f"Remaining 70% protected by server SL at 2x ATR ({STOP_LOSS_ATR_MULT}x ATR)\n"
                            f"If price recovers, trailing can still activate."
                        )
                    )
            elif current_qty is None:
                # Position fully gone — both SL tiers may have fired
                symbol_sl1_taken[symbol] = True
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"\U0001f6d1 FULL STOP-LOSS for {symbol}\n"
                        f"Position fully closed by stop-loss orders. Going FLAT."
                    )
                )
                symbol_trends[symbol] = None
                symbol_open_trade_side[symbol] = None
                _clear_risk_state(symbol)
                return

        # ── CLIENT-SIDE BACKUP: ATR Stop-Loss Checks ──
        # Server-side orders should fire first, but this is a safety net.
        # Skip if trailing is active (SL was cancelled, trail order handles risk).
        if not symbol_trailing_active.get(symbol, False) and not symbol_partial_taken.get(symbol, False):
            if current_open_trade_side == "Buy":
                # Check 2x ATR (full remaining close)
                stop_loss_price_full = entry_price - (STOP_LOSS_ATR_MULT * entry_atr)
                if current_price <= stop_loss_price_full:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            f"\U0001f6d1 STOP-LOSS HIT for {symbol} (Long) [Backup check]\n"
                            f"Entry: {entry_price} | Stop: {stop_loss_price_full:.6g}\n"
                            f"Current Price: {current_price} | ATR: {entry_atr:.6g}\n"
                            f"Loss at 2x ATR. Closing remaining position.\n"
                            f"(Server-side SL should have fired first)"
                        )
                    )
                    await cancel_server_stops(symbol)
                    await close_position(symbol, side_to_close="Buy")
                    symbol_trends[symbol] = None
                    symbol_open_trade_side[symbol] = None
                    return

                # Check 1.5x ATR (30% close) — backup if conditional order missed
                if not sl1_taken:
                    stop_loss_price_30 = entry_price - (SL_STAGE1_ATR_MULT * entry_atr)
                    if current_price <= stop_loss_price_30:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"\U0001f6d1 SL TIER 1 (Backup) for {symbol} (Long)\n"
                                f"Closing 30% at 1.5x ATR loss\n"
                                f"Entry: {entry_price} | Current: {current_price}"
                            )
                        )
                        await cancel_sl1_order(symbol)
                        await close_partial_position(symbol, side_to_close="Buy", close_pct=SL_STAGE1_CLOSE_PCT)
                        symbol_sl1_taken[symbol] = True
                        sl1_taken = True

            elif current_open_trade_side == "Sell":
                # Check 2x ATR (full remaining close)
                stop_loss_price_full = entry_price + (STOP_LOSS_ATR_MULT * entry_atr)
                if current_price >= stop_loss_price_full:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            f"\U0001f6d1 STOP-LOSS HIT for {symbol} (Short) [Backup check]\n"
                            f"Entry: {entry_price} | Stop: {stop_loss_price_full:.6g}\n"
                            f"Current Price: {current_price} | ATR: {entry_atr:.6g}\n"
                            f"Loss at 2x ATR. Closing remaining position.\n"
                            f"(Server-side SL should have fired first)"
                        )
                    )
                    await cancel_server_stops(symbol)
                    await close_position(symbol, side_to_close="Sell")
                    symbol_trends[symbol] = None
                    symbol_open_trade_side[symbol] = None
                    return

                # Check 1.5x ATR (30% close) — backup if conditional order missed
                if not sl1_taken:
                    stop_loss_price_30 = entry_price + (SL_STAGE1_ATR_MULT * entry_atr)
                    if current_price >= stop_loss_price_30:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"\U0001f6d1 SL TIER 1 (Backup) for {symbol} (Short)\n"
                                f"Closing 30% at 1.5x ATR loss\n"
                                f"Entry: {entry_price} | Current: {current_price}"
                            )
                        )
                        await cancel_sl1_order(symbol)
                        await close_partial_position(symbol, side_to_close="Sell", close_pct=SL_STAGE1_CLOSE_PCT)
                        symbol_sl1_taken[symbol] = True
                        sl1_taken = True

        # ── TRAILING STOP: Multi-stage trailing ──
        # Stage 1: At 1.5x ATR profit, trail on 60%. Bybit closes INSTANTLY when hit.
        # Stage 2: After 60% closed, trail on 30% (1x ATR distance).
        # Stage 3: Remaining 10% — no trail, exits on Exit Rules only.
        trailing_active = symbol_trailing_active.get(symbol, False)
        partial_taken = symbol_partial_taken.get(symbol, False)
        stage2_active = symbol_stage2_active.get(symbol, False)
        stage2_taken = symbol_stage2_taken.get(symbol, False)

        if current_open_trade_side == "Buy":
            unrealized_profit = peak - entry_price

            # ── Stage 1: Activate 60% trail stop ──
            if unrealized_profit >= TRAILING_ACTIVATE_ATR * entry_atr and not trailing_active:
                symbol_trailing_active[symbol] = True
                trailing_active = True
                save_state()

                pos_qty = await get_position_size(symbol)
                if pos_qty:
                    trail_qty = pos_qty * TRAILING_CLOSE_PCT
                    trail_stop_price = peak - (TRAILING_STOP_ATR * entry_atr)

                    # Cancel SL1 tier order (trailing now manages exit)
                    # Keep server-side SL active to protect remaining 40% (and later 10%)
                    await cancel_sl1_order(symbol)

                    # Place conditional stop order for 60% at trail price
                    order_id = await place_trail_stop_order(symbol, "Buy", trail_qty, trail_stop_price)

                    if order_id:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"\U0001f4c8 TRAILING ACTIVATED for {symbol} (Long)\n"
                                f"Profit reached {TRAILING_ACTIVATE_ATR}x ATR ({unrealized_profit:.6g})\n"
                                f"Peak: {peak:.6g} | Current: {current_price}\n"
                                f"Stage 1: Trail on {TRAILING_CLOSE_PCT*100:.0f}% at {trail_stop_price:.6g} ({TRAILING_STOP_ATR}x ATR)\n"
                                f"Trail moves UP as price rises. Bybit executes INSTANTLY when hit.\n"
                                f"After 60% closed: 30% gets trail ({TRAILING_STOP_ATR_STAGE2}x ATR), 10% rides until Exit Rules."
                            )
                        )
                    else:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=f"\u26a0\ufe0f {symbol}: Trail activated but failed to place stop order. Will retry."
                        )

            # ── Stage 1: Update trail price if peak moved (trail only moves UP for longs) ──
            if trailing_active and not partial_taken:
                current_trail = symbol_trail_price.get(symbol)
                new_trail_price = peak - (TRAILING_STOP_ATR * entry_atr)

                if current_trail is not None and new_trail_price > current_trail:
                    success = await amend_trail_stop_price(symbol, new_trail_price)
                    if not success:
                        pass

                # ── Detect if the 60% trail order has been filled ──
                current_qty = await get_position_size(symbol)
                trail_qty = symbol_trail_qty.get(symbol)
                if current_qty is not None and trail_qty is not None:
                    if trail_qty > 0 and current_qty <= trail_qty * 0.15:
                        # Position is now ~0 — trail closed everything (too small to split)
                        symbol_partial_taken[symbol] = True
                        trail_price_display = symbol_trail_price.get(symbol, 0)
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"\U0001f3c1 TRAILING STOP HIT for {symbol} (Long)\n"
                                f"Trail price: {trail_price_display:.6g}\n"
                                f"Full position closed (too small to split). Going FLAT."
                            )
                        )
                        symbol_trends[symbol] = None
                        symbol_open_trade_side[symbol] = None
                        _clear_risk_state(symbol)
                        return
                elif current_qty is None and trailing_active:
                    symbol_partial_taken[symbol] = True
                    trail_price_display = symbol_trail_price.get(symbol, 0)
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            f"\U0001f3c1 TRAILING STOP HIT for {symbol} (Long)\n"
                            f"Trail price: {trail_price_display:.6g}\n"
                            f"Full position closed. Going FLAT."
                        )
                    )
                    symbol_trends[symbol] = None
                    symbol_open_trade_side[symbol] = None
                    _clear_risk_state(symbol)
                    return

                # Check if trail order filled but position still exists = 60% was closed
                if current_qty is not None and trail_qty is not None:
                    order_id = symbol_trail_order_id.get(symbol)
                    if order_id and current_qty < trail_qty:
                        # 60% closed, 40% remains — activate stage 2
                        symbol_partial_taken[symbol] = True
                        partial_taken = True
                        trail_price_display = symbol_trail_price.get(symbol, 0)
                        symbol_trail_order_id[symbol] = None
                        symbol_trail_qty[symbol] = None
                        symbol_server_ts_set[symbol] = False
                        save_state()

                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"\U0001f3c1 STAGE 1 TRAIL closed {TRAILING_CLOSE_PCT*100:.0f}% for {symbol} (Long)\n"
                                f"Trail price hit: {trail_price_display:.6g}\n"
                                f"Now placing Stage 2 trail on 30% (1x ATR distance).\n"
                                f"Remaining 10% will ride until Exit Rules exit."
                            )
                        )

                        # ── Immediately activate Stage 2: trail on 30% (75% of remaining 40%) ──
                        stage2_qty = current_qty * TRAILING_STAGE2_PCT
                        # Stage 2 peak starts at current price (price just dropped to hit stage 1 trail)
                        symbol_peak_stage2[symbol] = current_price
                        stage2_trail_price = current_price - (TRAILING_STOP_ATR_STAGE2 * entry_atr)
                        # Ensure stage 2 trail is never below entry price (would be a loss, not profit)
                        if stage2_trail_price < entry_price:
                            stage2_trail_price = entry_price
                            print(f"[STAGE2] {symbol}: Trail price clamped to entry price {entry_price:.6g} (cannot trail below entry for longs)")

                        s2_order_id = await place_conditional_close_order(symbol, "Buy", stage2_qty, stage2_trail_price)
                        if s2_order_id:
                            min_qty, _, qty_step, _ = await get_instrument_constraints(symbol)
                            symbol_stage2_active[symbol] = True
                            stage2_active = True
                            symbol_trail2_order_id[symbol] = s2_order_id
                            symbol_trail2_qty[symbol] = round_qty_to_step(stage2_qty, qty_step, min_qty)
                            symbol_trail2_price[symbol] = stage2_trail_price
                            await context.bot.send_message(
                                chat_id=chat_id,
                                text=(
                                    f"\U0001f4c8 STAGE 2 TRAIL placed for {symbol} (Long)\n"
                                    f"Trail on 30% at {stage2_trail_price:.6g} ({TRAILING_STOP_ATR_STAGE2}x ATR distance)\n"
                                    f"Trail moves UP as price rises. Bybit executes INSTANTLY when hit.\n"
                                    f"Remaining 10% continues until Exit Rules exit."
                                )
                            )
                        else:
                            await context.bot.send_message(
                                chat_id=chat_id,
                                text=f"\u26a0\ufe0f {symbol}: Stage 2 trail failed to place. Will retry next cycle."
                            )

            # ── Stage 2: Update trail and detect fill (after 60% closed) ──
            if stage2_active and not stage2_taken:
                # Update stage 2 peak (only moves UP for longs)
                stage2_peak = symbol_peak_stage2.get(symbol, current_price)
                stage2_peak = max(stage2_peak, current_price)
                symbol_peak_stage2[symbol] = stage2_peak

                # Update stage 2 trail price if peak moved
                current_s2_trail = symbol_trail2_price.get(symbol)
                new_s2_trail = stage2_peak - (TRAILING_STOP_ATR_STAGE2 * entry_atr)
                s2_order_id = symbol_trail2_order_id.get(symbol)

                if current_s2_trail is not None and new_s2_trail > current_s2_trail and s2_order_id:
                    success = await amend_conditional_order(symbol, s2_order_id, new_s2_trail)
                    if success:
                        symbol_trail2_price[symbol] = new_s2_trail

                # Detect stage 2 fill
                current_qty = await get_position_size(symbol)
                s2_trail_qty = symbol_trail2_qty.get(symbol)

                if current_qty is None:
                    # Position fully gone
                    symbol_stage2_taken[symbol] = True
                    trail_price_display = symbol_trail2_price.get(symbol, 0)
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            f"\U0001f3c1 STAGE 2 TRAIL HIT for {symbol} (Long)\n"
                            f"Trail price: {trail_price_display:.6g}\n"
                            f"Full remaining position closed. Going FLAT."
                        )
                    )
                    symbol_trends[symbol] = None
                    symbol_open_trade_side[symbol] = None
                    _clear_risk_state(symbol)
                    return

                if current_qty is not None and s2_trail_qty is not None:
                    if s2_trail_qty > 0 and current_qty <= s2_trail_qty * 0.15:
                        # Position too small — stage 2 closed everything remaining
                        symbol_stage2_taken[symbol] = True
                        trail_price_display = symbol_trail2_price.get(symbol, 0)
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"\U0001f3c1 STAGE 2 TRAIL HIT for {symbol} (Long)\n"
                                f"Trail price: {trail_price_display:.6g}\n"
                                f"Remaining position closed (too small to split). Going FLAT."
                            )
                        )
                        symbol_trends[symbol] = None
                        symbol_open_trade_side[symbol] = None
                        _clear_risk_state(symbol)
                        return

                    if s2_order_id and current_qty < s2_trail_qty:
                        # Stage 2 closed 30%, remaining 10% continues
                        symbol_stage2_taken[symbol] = True
                        stage2_taken = True
                        trail_price_display = symbol_trail2_price.get(symbol, 0)
                        symbol_trail2_order_id[symbol] = None
                        symbol_trail2_qty[symbol] = None
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"\U0001f3c1 STAGE 2 TRAIL closed 30% for {symbol} (Long)\n"
                                f"Trail price hit: {trail_price_display:.6g}\n"
                                f"Remaining 10% continues trading.\n"
                                f"No trailing stop on final 10% — exits on Exit Rules crossover only."
                            )
                        )

            # ── Stage 2 retry: If 60% was closed but stage 2 order wasn't placed yet ──
            if partial_taken and not stage2_active and not stage2_taken:
                current_qty = await get_position_size(symbol)
                if current_qty and current_qty > 0:
                    stage2_qty = current_qty * TRAILING_STAGE2_PCT
                    s2_peak = symbol_peak_stage2.get(symbol, current_price)
                    s2_peak = max(s2_peak, current_price)
                    symbol_peak_stage2[symbol] = s2_peak
                    stage2_trail_price = s2_peak - (TRAILING_STOP_ATR_STAGE2 * entry_atr)
                    # Ensure stage 2 trail is never below entry price (would be a loss, not profit)
                    if stage2_trail_price < entry_price:
                        stage2_trail_price = entry_price
                        print(f"[STAGE2-RETRY] {symbol}: Trail price clamped to entry price {entry_price:.6g} (cannot trail below entry for longs)")

                    s2_order_id = await place_conditional_close_order(symbol, "Buy", stage2_qty, stage2_trail_price)
                    if s2_order_id:
                        min_qty, _, qty_step, _ = await get_instrument_constraints(symbol)
                        symbol_stage2_active[symbol] = True
                        symbol_trail2_order_id[symbol] = s2_order_id
                        symbol_trail2_qty[symbol] = round_qty_to_step(stage2_qty, qty_step, min_qty)
                        symbol_trail2_price[symbol] = stage2_trail_price
                        print(f"[STAGE2-RETRY] {symbol}: Stage 2 trail placed (retry) at {stage2_trail_price:.6g}")

        elif current_open_trade_side == "Sell":
            unrealized_profit = entry_price - peak  # peak is min for shorts

            # ── Stage 1: Activate 60% trail stop ──
            if unrealized_profit >= TRAILING_ACTIVATE_ATR * entry_atr and not trailing_active:
                symbol_trailing_active[symbol] = True
                trailing_active = True
                save_state()

                pos_qty = await get_position_size(symbol)
                if pos_qty:
                    trail_qty = pos_qty * TRAILING_CLOSE_PCT
                    trail_stop_price = peak + (TRAILING_STOP_ATR * entry_atr)

                    # Cancel SL1 tier order (trailing now manages exit)
                    # Keep server-side SL active to protect remaining 40% (and later 10%)
                    await cancel_sl1_order(symbol)

                    order_id = await place_trail_stop_order(symbol, "Sell", trail_qty, trail_stop_price)

                    if order_id:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"\U0001f4c8 TRAILING ACTIVATED for {symbol} (Short)\n"
                                f"Profit reached {TRAILING_ACTIVATE_ATR}x ATR ({unrealized_profit:.6g})\n"
                                f"Trough: {peak:.6g} | Current: {current_price}\n"
                                f"Stage 1: Trail on {TRAILING_CLOSE_PCT*100:.0f}% at {trail_stop_price:.6g} ({TRAILING_STOP_ATR}x ATR)\n"
                                f"Trail moves DOWN as price falls. Bybit executes INSTANTLY when hit.\n"
                                f"After 60% closed: 30% gets trail ({TRAILING_STOP_ATR_STAGE2}x ATR), 10% rides until Exit Rules."
                            )
                        )
                    else:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=f"\u26a0\ufe0f {symbol}: Trail activated but failed to place stop order. Will retry."
                        )

            # ── Stage 1: Update trail price if trough moved (trail only moves DOWN for shorts) ──
            if trailing_active and not partial_taken:
                current_trail = symbol_trail_price.get(symbol)
                new_trail_price = peak + (TRAILING_STOP_ATR * entry_atr)

                if current_trail is not None and new_trail_price < current_trail:
                    success = await amend_trail_stop_price(symbol, new_trail_price)
                    if not success:
                        pass

                # ── Detect if the 60% trail order has been filled ──
                current_qty = await get_position_size(symbol)
                trail_qty = symbol_trail_qty.get(symbol)
                if current_qty is not None and trail_qty is not None:
                    if trail_qty > 0 and current_qty <= trail_qty * 0.15:
                        symbol_partial_taken[symbol] = True
                        trail_price_display = symbol_trail_price.get(symbol, 0)
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"\U0001f3c1 TRAILING STOP HIT for {symbol} (Short)\n"
                                f"Trail price: {trail_price_display:.6g}\n"
                                f"Full position closed (too small to split). Going FLAT."
                            )
                        )
                        symbol_trends[symbol] = None
                        symbol_open_trade_side[symbol] = None
                        _clear_risk_state(symbol)
                        return
                elif current_qty is None and trailing_active:
                    symbol_partial_taken[symbol] = True
                    trail_price_display = symbol_trail_price.get(symbol, 0)
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            f"\U0001f3c1 TRAILING STOP HIT for {symbol} (Short)\n"
                            f"Trail price: {trail_price_display:.6g}\n"
                            f"Full position closed. Going FLAT."
                        )
                    )
                    symbol_trends[symbol] = None
                    symbol_open_trade_side[symbol] = None
                    _clear_risk_state(symbol)
                    return

                # Check if trail order filled but position still exists = 60% was closed
                if current_qty is not None and trail_qty is not None:
                    order_id = symbol_trail_order_id.get(symbol)
                    if order_id and current_qty < trail_qty:
                        # 60% closed, 40% remains — activate stage 2
                        symbol_partial_taken[symbol] = True
                        partial_taken = True
                        trail_price_display = symbol_trail_price.get(symbol, 0)
                        symbol_trail_order_id[symbol] = None
                        symbol_trail_qty[symbol] = None
                        symbol_server_ts_set[symbol] = False
                        save_state()

                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"\U0001f3c1 STAGE 1 TRAIL closed {TRAILING_CLOSE_PCT*100:.0f}% for {symbol} (Short)\n"
                                f"Trail price hit: {trail_price_display:.6g}\n"
                                f"Now placing Stage 2 trail on 30% (1x ATR distance).\n"
                                f"Remaining 10% will ride until Exit Rules exit."
                            )
                        )

                        # ── Immediately activate Stage 2: trail on 30% (75% of remaining) ──
                        stage2_qty = current_qty * TRAILING_STAGE2_PCT
                        symbol_peak_stage2[symbol] = current_price
                        stage2_trail_price = current_price + (TRAILING_STOP_ATR_STAGE2 * entry_atr)
                        # Ensure stage 2 trail is never above entry price (would be a loss, not profit)
                        if stage2_trail_price > entry_price:
                            stage2_trail_price = entry_price
                            print(f"[STAGE2] {symbol}: Trail price clamped to entry price {entry_price:.6g} (cannot trail above entry for shorts)")

                        s2_order_id = await place_conditional_close_order(symbol, "Sell", stage2_qty, stage2_trail_price)
                        if s2_order_id:
                            min_qty, _, qty_step, _ = await get_instrument_constraints(symbol)
                            symbol_stage2_active[symbol] = True
                            stage2_active = True
                            symbol_trail2_order_id[symbol] = s2_order_id
                            symbol_trail2_qty[symbol] = round_qty_to_step(stage2_qty, qty_step, min_qty)
                            symbol_trail2_price[symbol] = stage2_trail_price
                            await context.bot.send_message(
                                chat_id=chat_id,
                                text=(
                                    f"\U0001f4c8 STAGE 2 TRAIL placed for {symbol} (Short)\n"
                                    f"Trail on 30% at {stage2_trail_price:.6g} ({TRAILING_STOP_ATR_STAGE2}x ATR distance)\n"
                                    f"Trail moves DOWN as price falls. Bybit executes INSTANTLY when hit.\n"
                                    f"Remaining 10% continues until Exit Rules exit."
                                )
                            )
                        else:
                            await context.bot.send_message(
                                chat_id=chat_id,
                                text=f"\u26a0\ufe0f {symbol}: Stage 2 trail failed to place. Will retry next cycle."
                            )

            # ── Stage 2: Update trail and detect fill (after 60% closed) ──
            if stage2_active and not stage2_taken:
                # Update stage 2 peak (only moves DOWN for shorts)
                stage2_peak = symbol_peak_stage2.get(symbol, current_price)
                stage2_peak = min(stage2_peak, current_price)
                symbol_peak_stage2[symbol] = stage2_peak

                # Update stage 2 trail price if trough moved
                current_s2_trail = symbol_trail2_price.get(symbol)
                new_s2_trail = stage2_peak + (TRAILING_STOP_ATR_STAGE2 * entry_atr)
                s2_order_id = symbol_trail2_order_id.get(symbol)

                if current_s2_trail is not None and new_s2_trail < current_s2_trail and s2_order_id:
                    success = await amend_conditional_order(symbol, s2_order_id, new_s2_trail)
                    if success:
                        symbol_trail2_price[symbol] = new_s2_trail

                # Detect stage 2 fill
                current_qty = await get_position_size(symbol)
                s2_trail_qty = symbol_trail2_qty.get(symbol)

                if current_qty is None:
                    symbol_stage2_taken[symbol] = True
                    trail_price_display = symbol_trail2_price.get(symbol, 0)
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            f"\U0001f3c1 STAGE 2 TRAIL HIT for {symbol} (Short)\n"
                            f"Trail price: {trail_price_display:.6g}\n"
                            f"Full remaining position closed. Going FLAT."
                        )
                    )
                    symbol_trends[symbol] = None
                    symbol_open_trade_side[symbol] = None
                    _clear_risk_state(symbol)
                    return

                if current_qty is not None and s2_trail_qty is not None:
                    if s2_trail_qty > 0 and current_qty <= s2_trail_qty * 0.15:
                        symbol_stage2_taken[symbol] = True
                        trail_price_display = symbol_trail2_price.get(symbol, 0)
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"\U0001f3c1 STAGE 2 TRAIL HIT for {symbol} (Short)\n"
                                f"Trail price: {trail_price_display:.6g}\n"
                                f"Remaining position closed (too small to split). Going FLAT."
                            )
                        )
                        symbol_trends[symbol] = None
                        symbol_open_trade_side[symbol] = None
                        _clear_risk_state(symbol)
                        return

                    if s2_order_id and current_qty < s2_trail_qty:
                        symbol_stage2_taken[symbol] = True
                        stage2_taken = True
                        trail_price_display = symbol_trail2_price.get(symbol, 0)
                        symbol_trail2_order_id[symbol] = None
                        symbol_trail2_qty[symbol] = None
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"\U0001f3c1 STAGE 2 TRAIL closed 30% for {symbol} (Short)\n"
                                f"Trail price hit: {trail_price_display:.6g}\n"
                                f"Remaining 10% continues trading.\n"
                                f"No trailing stop on final 10% — exits on Exit Rules crossover only."
                            )
                        )

            # ── Stage 2 retry: If 60% was closed but stage 2 order wasn't placed yet ──
            if partial_taken and not stage2_active and not stage2_taken:
                current_qty = await get_position_size(symbol)
                if current_qty and current_qty > 0:
                    stage2_qty = current_qty * TRAILING_STAGE2_PCT
                    s2_peak = symbol_peak_stage2.get(symbol, current_price)
                    s2_peak = min(s2_peak, current_price)
                    symbol_peak_stage2[symbol] = s2_peak
                    stage2_trail_price = s2_peak + (TRAILING_STOP_ATR_STAGE2 * entry_atr)
                    # Ensure stage 2 trail is never above entry price (would be a loss, not profit)
                    if stage2_trail_price > entry_price:
                        stage2_trail_price = entry_price
                        print(f"[STAGE2-RETRY] {symbol}: Trail price clamped to entry price {entry_price:.6g} (cannot trail above entry for shorts)")

                    s2_order_id = await place_conditional_close_order(symbol, "Sell", stage2_qty, stage2_trail_price)
                    if s2_order_id:
                        min_qty, _, qty_step, _ = await get_instrument_constraints(symbol)
                        symbol_stage2_active[symbol] = True
                        symbol_trail2_order_id[symbol] = s2_order_id
                        symbol_trail2_qty[symbol] = round_qty_to_step(stage2_qty, qty_step, min_qty)
                        symbol_trail2_price[symbol] = stage2_trail_price
                        print(f"[STAGE2-RETRY] {symbol}: Stage 2 trail placed (retry) at {stage2_trail_price:.6g}")

    # ──────────────────────────────────────────────
    # 2. MACD-BASED PARTIAL EXIT RULES (10-MIN CLOSED-CANDLE CONFIRMATION)
    #    Uses 10-minute chart MACD with CLOSED candle crossover confirmation.
    #    This prevents whipsaw from intra-candle noise — the bot only acts when
    #    a confirmed crossover is detected on a fully closed 10-min candle.
    #    - Long: Bearish MACD cross on closed 10m candle → close 60%
    #            Bullish MACD cross on closed 10m candle → add back 60%
    #    - Short: Bullish MACD cross on closed 10m candle → close 60%
    #             Bearish MACD cross on closed 10m candle → add back 60%
    #    Only applies when trailing stop is NOT active (to avoid conflicts)
    #    Does NOT trigger if exit conditions are already met (let exit handle it)
    # ──────────────────────────────────────────────
    if current_open_trade_side is not None:
        trailing_is_active = symbol_trailing_active.get(symbol, False)
        macd_partial_closed = symbol_macd_partial_closed.get(symbol, False)

        # Reuse the 10-minute MACD data already fetched at the top of monitor_symbol
        macd_10m_data = macd_10m_indicators if not trailing_is_active else None

        if macd_10m_data and current_open_trade_side == "Buy" and not trailing_is_active:
            # Check if exit conditions are met — if so, skip MACD partial (exit will handle)
            exit_conditions_met = (last_close < ema_20 and macd_line < signal_line)

            # Only act if this is a NEW closed candle (prevent re-triggering on the same candle)
            last_acted_candle = symbol_macd_last_action_candle.get(symbol)
            current_10m_candle = macd_10m_data["last_closed_ts_10m"]

            if not exit_conditions_met:
                if not macd_partial_closed and macd_10m_data["bearish_cross_10m"] and current_10m_candle != last_acted_candle:
                    # Confirmed bearish MACD crossover on closed 10-min candle → close 60% of long position
                    pos_qty = await get_position_size(symbol)
                    if pos_qty:
                        close_qty = pos_qty * 0.60
                        min_qty, _, qty_step, _ = await get_instrument_constraints(symbol)
                        close_qty = round_qty_to_step(close_qty, qty_step, min_qty)

                        closed = await close_partial_position(symbol, side_to_close="Buy", close_pct=0.60)
                        if closed:
                            symbol_macd_partial_closed[symbol] = True
                            symbol_macd_partial_qty[symbol] = closed
                            symbol_macd_last_action_candle[symbol] = current_10m_candle
                            save_state()
                            await context.bot.send_message(
                                chat_id=chat_id,
                                text=(
                                    f"\U0001f4c9 MACD PARTIAL EXIT for {symbol} (Long)\n"
                                    f"Confirmed on 10-min closed candle\n"
                                    f"MACD Line ({macd_10m_data['macd_10m']:.6g}) < Signal Line ({macd_10m_data['signal_10m']:.6g})\n"
                                    f"Prev MACD ({macd_10m_data['prev_macd_10m']:.6g}) was above Signal ({macd_10m_data['prev_signal_10m']:.6g})\n"
                                    f"Closed 60% of position ({closed} qty)\n"
                                    f"Remaining 40% still in Long trade.\n"
                                    f"If MACD crosses back above Signal on a closed 10-min candle, 60% will be added back."
                                )
                            )

                elif macd_partial_closed and macd_10m_data["bullish_cross_10m"] and current_10m_candle != last_acted_candle:
                    # Confirmed bullish MACD crossover on closed 10-min candle → add back the 60% that was closed
                    add_back_qty = symbol_macd_partial_qty.get(symbol)
                    if add_back_qty and add_back_qty > 0:
                        result = await place_trade(symbol, side="Buy", manual_qty_override=add_back_qty)
                        if result and "error" not in result:
                            symbol_macd_partial_closed[symbol] = False
                            symbol_macd_partial_qty[symbol] = None
                            symbol_macd_last_action_candle[symbol] = current_10m_candle
                            save_state()
                            await context.bot.send_message(
                                chat_id=chat_id,
                                text=(
                                    f"\U0001f4c8 MACD RE-ENTRY for {symbol} (Long)\n"
                                    f"Confirmed on 10-min closed candle\n"
                                    f"MACD Line ({macd_10m_data['macd_10m']:.6g}) > Signal Line ({macd_10m_data['signal_10m']:.6g})\n"
                                    f"Added back 60% ({add_back_qty} qty) to Long position.\n"
                                    f"Full position restored."
                                )
                            )
                        else:
                            err = result.get("error", "Unknown") if result else "Unknown"
                            await context.bot.send_message(
                                chat_id=chat_id,
                                text=f"\u26a0\ufe0f {symbol}: Failed to add back 60% on MACD re-cross: {err}"
                            )

        elif macd_10m_data and current_open_trade_side == "Sell" and not trailing_is_active:
            # Check if exit conditions are met — if so, skip MACD partial (exit will handle)
            exit_conditions_met = (last_close > ema_20 and macd_line > signal_line)

            # Only act if this is a NEW closed candle (prevent re-triggering on the same candle)
            last_acted_candle = symbol_macd_last_action_candle.get(symbol)
            current_10m_candle = macd_10m_data["last_closed_ts_10m"]

            if not exit_conditions_met:
                if not macd_partial_closed and macd_10m_data["bullish_cross_10m"] and current_10m_candle != last_acted_candle:
                    # Confirmed bullish MACD crossover on closed 10-min candle → close 60% of short position
                    pos_qty = await get_position_size(symbol)
                    if pos_qty:
                        close_qty = pos_qty * 0.60
                        min_qty, _, qty_step, _ = await get_instrument_constraints(symbol)
                        close_qty = round_qty_to_step(close_qty, qty_step, min_qty)

                        closed = await close_partial_position(symbol, side_to_close="Sell", close_pct=0.60)
                        if closed:
                            symbol_macd_partial_closed[symbol] = True
                            symbol_macd_partial_qty[symbol] = closed
                            symbol_macd_last_action_candle[symbol] = current_10m_candle
                            save_state()
                            await context.bot.send_message(
                                chat_id=chat_id,
                                text=(
                                    f"\U0001f4c9 MACD PARTIAL EXIT for {symbol} (Short)\n"
                                    f"Confirmed on 10-min closed candle\n"
                                    f"MACD Line ({macd_10m_data['macd_10m']:.6g}) > Signal Line ({macd_10m_data['signal_10m']:.6g})\n"
                                    f"Prev MACD ({macd_10m_data['prev_macd_10m']:.6g}) was below Signal ({macd_10m_data['prev_signal_10m']:.6g})\n"
                                    f"Closed 60% of position ({closed} qty)\n"
                                    f"Remaining 40% still in Short trade.\n"
                                    f"If MACD crosses back below Signal on a closed 10-min candle, 60% will be added back."
                                )
                            )

                elif macd_partial_closed and macd_10m_data["bearish_cross_10m"] and current_10m_candle != last_acted_candle:
                    # Confirmed bearish MACD crossover on closed 10-min candle → add back the 60% that was closed
                    add_back_qty = symbol_macd_partial_qty.get(symbol)
                    if add_back_qty and add_back_qty > 0:
                        result = await place_trade(symbol, side="Sell", manual_qty_override=add_back_qty)
                        if result and "error" not in result:
                            symbol_macd_partial_closed[symbol] = False
                            symbol_macd_partial_qty[symbol] = None
                            symbol_macd_last_action_candle[symbol] = current_10m_candle
                            save_state()
                            await context.bot.send_message(
                                chat_id=chat_id,
                                text=(
                                    f"\U0001f4c8 MACD RE-ENTRY for {symbol} (Short)\n"
                                    f"Confirmed on 10-min closed candle\n"
                                    f"MACD Line ({macd_10m_data['macd_10m']:.6g}) < Signal Line ({macd_10m_data['signal_10m']:.6g})\n"
                                    f"Added back 60% ({add_back_qty} qty) to Short position.\n"
                                    f"Full position restored."
                                )
                            )
                        else:
                            err = result.get("error", "Unknown") if result else "Unknown"
                            await context.bot.send_message(
                                chat_id=chat_id,
                                text=f"\u26a0\ufe0f {symbol}: Failed to add back 60% on MACD re-cross: {err}"
                            )

    # ──────────────────────────────────────────────
    # 3. EXIT + POSSIBLE REVERSAL (Decoupled)
    #    Exit requires: price crosses EMA AND MACD Line/Signal (30m).
    #    Reverse ONLY if MACD + ADX > 24 confirm. Otherwise go FLAT.
    # ──────────────────────────────────────────────
    if current_open_trade_side is not None:

        # ── Long Exit: close price below 20 EMA AND MACD Line < Signal Line (30m) ──
        if current_open_trade_side == "Buy" and last_close < ema_20 and macd_line < signal_line:
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"\u26a0\ufe0f EXIT SIGNAL for {symbol} (Long)\n"
                    f"Last Close ({last_close}) < 20 EMA ({ema_20:.6g})\n"
                    f"MACD Line ({macd_line:.6g}) < Signal Line ({signal_line:.6g}) — bearish alignment\n"
                    f"ADX: {adx:.2f}\n"
                    f"Closing Long position..."
                )
            )

            await cancel_server_stops(symbol)  # Cancel server-side stops before closing
            closed_qty = await close_position(symbol, side_to_close="Buy")
            manual_override = None
            if symbol in symbol_manual_quantities and symbol_manual_quantities[symbol] is not None:
                manual_override = closed_qty

            # Check if we should REVERSE to Short (requires: Price < 20 EMA AND MACD < Signal (30m) AND ADX > 24)
            if last_close < ema_20 and macd_line < signal_line and adx > ADX_REVERSAL_THRESHOLD:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"\U0001f504 REVERSE to Short for {symbol}\n"
                        f"Price ({last_close}) < 20 EMA ({ema_20:.6g})\n"
                        f"MACD ({macd_line:.6g}) < Signal ({signal_line:.6g}) | ADX: {adx:.2f} > {ADX_REVERSAL_THRESHOLD}\n"
                        f"Opening Short..."
                    )
                )
                result = await place_trade(symbol, side="Sell", manual_qty_override=manual_override)
                if result and "error" not in result:
                    order_id = result.get("orderId", "\u2014")
                    # Get actual fill price from Bybit (not candle close)
                    await asyncio.sleep(1)
                    real_entry = await get_position_entry_price(symbol)
                    fill_price = real_entry if real_entry else last_close
                    _set_entry_risk_state(symbol, fill_price, atr)
                    # Set server-side stop-loss based on actual fill price
                    sl_price = fill_price + (STOP_LOSS_ATR_MULT * atr)
                    await set_server_stop_loss(symbol, "Sell", sl_price)
                    await context.bot.send_message(chat_id=chat_id, text=f"\u2705 Short order(s) placed for {symbol}\nLast Order ID: <code>{order_id}</code>\nFill Price: {fill_price:.6g}\nServer-side SL: {sl_price:.6g}", parse_mode="HTML")
                    symbol_trends[symbol] = "downward"
                    symbol_open_trade_side[symbol] = "Sell"
                elif result and "error" in result:
                    await context.bot.send_message(chat_id=chat_id, text=f"\u274c Failed to place Short for {symbol}: {result['error']}")
                    symbol_trends[symbol] = None
                    symbol_open_trade_side[symbol] = None
            else:
                # No reverse signal -> go FLAT
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"\U0001f7e1 GOING FLAT for {symbol}\n"
                            f"No reverse signal (need: Price < 20 EMA + MACD < Signal (30m) + ADX > {ADX_REVERSAL_THRESHOLD})\n"
                            f"MACD: {macd_line:.6g} / Signal: {signal_line:.6g} | ADX: {adx:.2f}\n"
                        f"Waiting for next entry signal..."
                    )
                )
                symbol_trends[symbol] = None
                symbol_open_trade_side[symbol] = None
                _clear_risk_state(symbol)
            return

        # ── Short Exit: close price above 20 EMA AND MACD Line > Signal Line (30m) ──
        elif current_open_trade_side == "Sell" and last_close > ema_20 and macd_line > signal_line:
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"\u26a0\ufe0f EXIT SIGNAL for {symbol} (Short)\n"
                    f"Last Close ({last_close}) > 20 EMA ({ema_20:.6g})\n"
                    f"MACD Line ({macd_line:.6g}) > Signal Line ({signal_line:.6g}) — bullish alignment\n"
                    f"ADX: {adx:.2f}\n"
                    f"Closing Short position..."
                )
            )

            await cancel_server_stops(symbol)  # Cancel server-side stops before closing
            closed_qty = await close_position(symbol, side_to_close="Sell")
            manual_override = None
            if symbol in symbol_manual_quantities and symbol_manual_quantities[symbol] is not None:
                manual_override = closed_qty

            # Check if we should REVERSE to Long (requires: Price > 20 EMA AND MACD > Signal (30m) AND ADX > 24)
            if last_close > ema_20 and macd_line > signal_line and adx > ADX_REVERSAL_THRESHOLD:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"\U0001f504 REVERSE to Long for {symbol}\n"
                        f"Price ({last_close}) > 20 EMA ({ema_20:.6g})\n"
                        f"MACD ({macd_line:.6g}) > Signal ({signal_line:.6g}) | ADX: {adx:.2f} > {ADX_REVERSAL_THRESHOLD}\n"
                        f"Opening Long..."
                    )
                )
                result = await place_trade(symbol, side="Buy", manual_qty_override=manual_override)
                if result and "error" not in result:
                    order_id = result.get("orderId", "\u2014")
                    # Get actual fill price from Bybit (not candle close)
                    await asyncio.sleep(1)
                    real_entry = await get_position_entry_price(symbol)
                    fill_price = real_entry if real_entry else last_close
                    _set_entry_risk_state(symbol, fill_price, atr)
                    # Set server-side stop-loss based on actual fill price
                    sl_price = fill_price - (STOP_LOSS_ATR_MULT * atr)
                    await set_server_stop_loss(symbol, "Buy", sl_price)
                    await context.bot.send_message(chat_id=chat_id, text=f"\u2705 Long order(s) placed for {symbol}\nLast Order ID: <code>{order_id}</code>\nFill Price: {fill_price:.6g}\nServer-side SL: {sl_price:.6g}", parse_mode="HTML")
                    symbol_trends[symbol] = "upward"
                    symbol_open_trade_side[symbol] = "Buy"
                elif result and "error" in result:
                    await context.bot.send_message(chat_id=chat_id, text=f"\u274c Failed to place Long for {symbol}: {result['error']}")
                    symbol_trends[symbol] = None
                    symbol_open_trade_side[symbol] = None
            else:
                # No reverse signal -> go FLAT
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"\U0001f7e1 GOING FLAT for {symbol}\n"
                        f"No reverse signal (need: Price > 20 EMA + MACD > Signal (30m) + ADX > {ADX_REVERSAL_THRESHOLD})\n"
                        f"MACD: {macd_line:.6g} / Signal: {signal_line:.6g} | ADX: {adx:.2f}\n"
                        f"Waiting for next entry signal..."
                    )
                )
                symbol_trends[symbol] = None
                symbol_open_trade_side[symbol] = None
                _clear_risk_state(symbol)
            return

    # ──────────────────────────────────────────────
    # 4. INITIAL TRADE ENTRY (no open position)
    #    4-layer filter (ALL must pass):
    #      1. ADX > 22
    #      2. Long: Price > 20 EMA (30m) | Short: Price < 20 EMA (30m)
    #      3. RSI NOT extreme (not > 70 for longs, not < 30 for shorts)
    #      4. Long: MACD Line > Signal Line (30m) | Short: MACD Line < Signal Line (30m)
    # ──────────────────────────────────────────────
    if current_open_trade_side is None:
        # Filter 1: ADX must be above threshold
        if adx < ADX_THRESHOLD:
            return

        new_trend = None
        side = None
        # Long: last_close > 20 EMA (30m)
        if last_close > ema_20:
            # Filter 3: RSI not overbought for longs
            if rsi > RSI_OVERBOUGHT:
                print(f"[FILTER] {symbol}: Long entry blocked — RSI overbought ({rsi:.2f} > {RSI_OVERBOUGHT})")
                return
            # Filter 4: MACD Line must be above Signal Line (30m chart) for longs
            if macd_line <= signal_line:
                print(f"[FILTER] {symbol}: Long entry blocked — MACD Line 30m ({macd_line:.6g}) <= Signal Line 30m ({signal_line:.6g})")
                return
            new_trend = "upward"
            side = "Buy"
        # Short: last_close < 20 EMA (30m)
        elif last_close < ema_20:
            # Filter 3: RSI not oversold for shorts
            if rsi < RSI_OVERSOLD:
                print(f"[FILTER] {symbol}: Short entry blocked — RSI oversold ({rsi:.2f} < {RSI_OVERSOLD})")
                return
            # Filter 4: MACD Line must be below Signal Line (30m chart) for shorts
            if macd_line >= signal_line:
                print(f"[FILTER] {symbol}: Short entry blocked — MACD Line 30m ({macd_line:.6g}) >= Signal Line 30m ({signal_line:.6g})")
                return
            new_trend = "downward"
            side = "Sell"

        if new_trend and side and (previous_trend is None or new_trend != previous_trend):
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            side_text = "Long (Buy)" if side == "Buy" else "Short (Sell)"
            msg = (
                f"{ts} \u2014 {symbol} ENTRY SIGNAL: <b>{new_trend.upper()}</b>\n"
                f"Last Close: {last_close} | 20 EMA: {ema_20:.6g}\n"
                f"+DI: {plus_di:.2f} | -DI: {minus_di:.2f} | ADX: {adx:.2f}\n"
                f"RSI: {rsi:.2f} | MACD 30m: {macd_line:.6g} / Signal 30m: {signal_line:.6g}\n"
                f"MACD Hist (30m): {macd_histogram:.6g} (prev: {prev_macd_histogram:.6g})\n"
                f"Direction: {side_text}"
            )
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")

            result = await place_trade(symbol, side=side)

            if result and "error" not in result:
                order_id = result.get("orderId", "\u2014")
                # Get actual fill price from Bybit (not candle close)
                await asyncio.sleep(1)
                real_entry = await get_position_entry_price(symbol)
                fill_price = real_entry if real_entry else last_close
                _set_entry_risk_state(symbol, fill_price, atr)
                # Set server-side stop-loss based on actual fill price
                if side == "Buy":
                    sl_price = fill_price - (STOP_LOSS_ATR_MULT * atr)
                else:
                    sl_price = fill_price + (STOP_LOSS_ATR_MULT * atr)
                await set_server_stop_loss(symbol, side, sl_price)
                await context.bot.send_message(chat_id=chat_id, text=f"\u2705 {side_text} order(s) placed for {symbol}\nLast Order ID: <code>{order_id}</code>\nFill Price: {fill_price:.6g}\nServer-side SL: {sl_price:.6g}", parse_mode="HTML")
            elif result and "error" in result:
                await context.bot.send_message(chat_id=chat_id, text=f"\u274c Failed to place {side_text} for {symbol}: {result['error']}")

            symbol_trends[symbol] = new_trend
            symbol_open_trade_side[symbol] = await get_open_position_side(symbol)
            save_state()

    # ──────────────────────────────────────────────
    # 5. 30-Minute Status Report
    # ──────────────────────────────────────────────
    current_time = time.time()
    last_report_time = context.job.data.get("last_report_time", 0)

    if current_time - last_report_time >= 1800:
        trade_status = "No open trade"
        if current_open_trade_side:
            trade_status = f"\u2705 {current_open_trade_side} trade open for {symbol}"
        else:
            trade_status = f"\u274c No open trade for {symbol} (FLAT)"

        entry_price = symbol_entry_price.get(symbol)
        entry_atr = symbol_entry_atr.get(symbol)
        peak = symbol_peak_price.get(symbol)
        trailing = symbol_trailing_active.get(symbol, False)

        risk_info = ""
        if entry_price and entry_atr:
            if current_open_trade_side == "Buy":
                sl = entry_price - (STOP_LOSS_ATR_MULT * entry_atr)
            elif current_open_trade_side == "Sell":
                sl = entry_price + (STOP_LOSS_ATR_MULT * entry_atr)
            else:
                sl = None
            risk_info = (
                f"\n\u2022 Entry Price: {entry_price:.6g}"
                f"\n\u2022 Entry ATR: {entry_atr:.6g}"
                f"\n\u2022 Stop-Loss: {sl:.6g}" if sl else ""
            )
            if peak:
                risk_info += f"\n\u2022 Peak Price: {peak:.6g}"
            partial_taken_flag = symbol_partial_taken.get(symbol, False)
            stage2_active_flag = symbol_stage2_active.get(symbol, False)
            stage2_taken_flag = symbol_stage2_taken.get(symbol, False)
            sl1_taken_flag = symbol_sl1_taken.get(symbol, False)
            server_sl = symbol_server_sl_set.get(symbol, False)
            server_ts = symbol_server_ts_set.get(symbol, False)
            trail_price = symbol_trail_price.get(symbol)
            trail2_price = symbol_trail2_price.get(symbol)
            if stage2_taken_flag:
                trailing_status = '60% + 30% CLOSED by trail. 10% riding until Exit Rules exit'
            elif stage2_active_flag and trail2_price:
                trailing_status = f'Stage 2 ACTIVE - trail on 30% @ {trail2_price:.6g} (1x ATR)'
            elif partial_taken_flag:
                trailing_status = f'60% CLOSED by trail. Placing 30% trail...'
            elif trailing and trail_price:
                trailing_status = f'Stage 1 ACTIVE - trail on 60% @ {trail_price:.6g} (1x ATR)'
            elif trailing:
                trailing_status = 'Stage 1 ACTIVE (placing trail order...)'
            else:
                trailing_status = 'Not yet'
            risk_info += f"\n\u2022 Trailing Stop: {trailing_status}"
            # SL status
            if sl1_taken_flag:
                sl_status = f'Tier 1 HIT (30% closed at 1.5x ATR). Tier 2 at 2x ATR protects remaining 70%'
            else:
                sl_status = f"Tier 1: {'SET' if symbol_sl1_order_id.get(symbol) else 'Not set'} (30% @ 1.5x ATR) | Tier 2: {'SET' if server_sl else 'Not set'} (70% @ 2x ATR)"
            risk_info += f"\n\u2022 Stop-Loss: {sl_status}"
            risk_info += f"\n\u2022 Server-Side SL: {'SET' if server_sl else 'Not set'}"
            risk_info += f"\n\u2022 Server-Side TS: {'SET' if server_ts else 'Not set'}"
            # MACD partial exit status
            macd_partial_flag = symbol_macd_partial_closed.get(symbol, False)
            macd_partial_qty_val = symbol_macd_partial_qty.get(symbol)
            if macd_partial_flag and macd_partial_qty_val:
                risk_info += f"\n\u2022 MACD Partial: 60% CLOSED ({macd_partial_qty_val} qty) — waiting for re-cross to add back"
            else:
                risk_info += "\n\u2022 MACD Partial: Not triggered"

        report_msg = (
            f"Still monitoring {symbol}\n"
            f"\u2022 Entry: 4-layer filter (ADX>{ADX_THRESHOLD}, EMA (30m), RSI, MACD 30m Line/Signal)\n"
            f"\u2022 Exit: Long exits when Price < 20 EMA AND MACD < Signal (30m) | Short exits when Price > 20 EMA AND MACD > Signal (30m)\n"
            f"\u2022 Reverse: Only if ADX > {ADX_REVERSAL_THRESHOLD} + EMA/MACD alignment (30m), else FLAT\n"
            f"\u2022 SL: 30% at {SL_STAGE1_ATR_MULT}x ATR + 70% at {STOP_LOSS_ATR_MULT}x ATR (server SL always active)\n"
            f"\u2022 MACD Partial: Close 60% on adverse MACD cross, add back on re-cross\n"
            f"\u2022 Trail: 60% at {TRAILING_ACTIVATE_ATR}x ATR, then 30% at 1x ATR, 10% rides\n"
            f"\u2022 WebSocket prices + polling every {POLL_INTERVAL_SECONDS}s\n"
            f"\u2022 20 EMA: {ema_20:.6g} | +DI: {plus_di:.2f} | -DI: {minus_di:.2f} | ADX: {adx:.2f}\n"
            f"\u2022 RSI: {rsi:.2f} | MACD: {macd_line:.6g} / Signal: {signal_line:.6g}\n"
            f"\u2022 MACD Hist: {macd_histogram:.6g} (prev: {prev_macd_histogram:.6g})\n"
            f"\u2022 Last Close: {last_close} | Current Price: {current_price}\n"
            f"\u2022 {symbol} state: {current_market_state}\n"
            f"\u2022 {trade_status}"
            f"{risk_info}"
        )
        if symbol in symbol_manual_quantities and symbol_manual_quantities[symbol] is not None:
            report_msg += f"\n\u2022 Manual Quantity: {symbol_manual_quantities[symbol]}"
        else:
            report_msg += "\n\u2022 Auto-sizing: 20% Balance"

        await context.bot.send_message(chat_id=chat_id, text=report_msg)
        context.job.data["last_report_time"] = current_time


async def add_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Privacy: only the owner can use this bot
    if update.effective_chat.id != CHAT_ID:
        await update.message.reply_text("\u26d4 Unauthorized. This bot is private.")
        return
    if not context.args:
        await update.message.reply_text(
            "Usage:\n"
            "/add_symbol <symbol> (Auto-size 20% balance)\n"
            "/add_symbol <symbol> <quantity> (Manual quantity + INSTANT TRADE)"
        )
        return

    symbol = context.args[0].upper()
    manual_qty = None

    if len(context.args) >= 2:
        try:
            manual_qty = float(context.args[1])
            if manual_qty <= 0:
                await update.message.reply_text("Quantity must be greater than 0.")
                return
        except ValueError:
            await update.message.reply_text("Quantity must be a valid number.")
            return

    # Store settings
    symbol_manual_quantities[symbol] = manual_qty
    symbol_trends[symbol] = None
    _clear_risk_state(symbol)
    if symbol not in monitored_symbols:
        monitored_symbols.append(symbol)
    save_state()

    # Subscribe to WebSocket ticker for real-time price feeds
    subscribe_ws_symbol(symbol)

    chat_id = update.effective_chat.id

    # Handle instant trade for manual quantity
    if manual_qty:
        await update.message.reply_text(f"\U0001f680 INSTANT TRADE for {symbol} with quantity {manual_qty}...")

        indicators = await get_all_indicators(symbol)
        if indicators and indicators["current_price"]:
            last_close = indicators["last_close"]
            ema_20 = indicators["ema_20"]
            plus_di = indicators["plus_di"]
            minus_di = indicators["minus_di"]
            adx = indicators["adx"]
            atr = indicators["atr"]

            adx_prev2 = indicators["adx_prev2"]
            rsi = indicators["rsi"]
            macd_line = indicators["macd_line"]
            signal_line = indicators["signal_line"]
            macd_histogram = indicators["macd_histogram"]
            prev_macd_histogram = indicators["prev_macd_histogram"]
            macd_hist_dying_long = indicators["macd_hist_dying_long"]
            macd_hist_dying_short = indicators["macd_hist_dying_short"]

            # 4-layer entry filter for instant trade
            entry_blocked = False
            block_reason = ""

            # Filter 1: ADX threshold
            if adx < ADX_THRESHOLD:
                entry_blocked = True
                block_reason = f"ADX ({adx:.2f}) < {ADX_THRESHOLD} — market is sideways"

            if entry_blocked:
                await update.message.reply_text(
                    f"\u26a0\ufe0f Instant trade blocked for {symbol}: {block_reason}\n"
                    f"Monitoring will wait for proper conditions."
                )
            else:
                # Require EMA + RSI + MACD (30m) filters
                side = None
                if last_close > ema_20:
                    if rsi > RSI_OVERBOUGHT:
                        entry_blocked = True
                        block_reason = f"RSI overbought ({rsi:.2f} > {RSI_OVERBOUGHT})"
                    elif macd_line <= signal_line:
                        entry_blocked = True
                        block_reason = f"MACD Line 30m ({macd_line:.6g}) <= Signal Line 30m ({signal_line:.6g})"
                    else:
                        side = "Buy"
                elif last_close < ema_20:
                    if rsi < RSI_OVERSOLD:
                        entry_blocked = True
                        block_reason = f"RSI oversold ({rsi:.2f} < {RSI_OVERSOLD})"
                    elif macd_line >= signal_line:
                        entry_blocked = True
                        block_reason = f"MACD Line 30m ({macd_line:.6g}) >= Signal Line 30m ({signal_line:.6g})"
                    else:
                        side = "Sell"

                if entry_blocked:
                    await update.message.reply_text(
                        f"\u26a0\ufe0f Instant trade blocked for {symbol}: {block_reason}\n"
                        f"Monitoring will wait for proper conditions."
                    )
                elif side is None:
                    await update.message.reply_text(
                        f"\u26a0\ufe0f No aligned signal for {symbol}.\n"
                        f"Last Close: {last_close} | EMA: {ema_20:.6g}\n"
                        f"MACD: {macd_line:.6g} / Signal: {signal_line:.6g} | ADX: {adx:.2f}\n"
                        f"Need: Price > EMA + MACD > Signal (30m) for Long, or Price < EMA + MACD < Signal (30m) for Short.\n"
                        f"Monitoring will wait for proper alignment."
                    )
                else:
                    side_text = "Long (Buy)" if side == "Buy" else "Short (Sell)"

                    await update.message.reply_text(
                        f"\U0001f4ca {symbol}: Last Close={last_close} | 20 EMA={ema_20:.6g}\n"
                        f"+DI: {plus_di:.2f} | -DI: {minus_di:.2f} | ADX: {adx:.2f}\n"
                        f"MACD 30m: {macd_line:.6g} / Signal 30m: {signal_line:.6g} | Hist (30m): {macd_histogram:.6g}\n"
                        f"Direction: {side_text}"
                    )

                    result = await place_trade(symbol, side=side, manual_qty_override=manual_qty)

                    if result and "error" not in result:
                        order_id = result.get("orderId", "\u2014")
                        # Get actual fill price from Bybit (not candle close)
                        await asyncio.sleep(1)
                        real_entry = await get_position_entry_price(symbol)
                        fill_price = real_entry if real_entry else last_close
                        _set_entry_risk_state(symbol, fill_price, atr)
                        # Set server-side stop-loss based on actual fill price
                        if side == "Buy":
                            sl_price = fill_price - (STOP_LOSS_ATR_MULT * atr)
                        else:
                            sl_price = fill_price + (STOP_LOSS_ATR_MULT * atr)
                        await set_server_stop_loss(symbol, side, sl_price)
                        await update.message.reply_text(
                            f"\u2705 Instant {side_text} order(s) placed for {symbol}\n"
                            f"Last Order ID: <code>{order_id}</code>\n"
                            f"Fill Price: {fill_price:.6g}\n"
                            f"Server-side SL: {sl_price:.6g} ({STOP_LOSS_ATR_MULT}x ATR)\n"
                            f"Bybit will execute SL instantly at exact price.",
                            parse_mode="HTML"
                        )
                        symbol_trends[symbol] = "upward" if side == "Buy" else "downward"
                    elif result and "error" in result:
                        await update.message.reply_text(f"\u274c Failed to place instant {side_text} for {symbol}: {result['error']}")
        else:
            await update.message.reply_text(f"\u26a0\ufe0f Could not fetch indicators for {symbol}. Monitoring will take over.")

    qty_info = f"Manual Quantity: {manual_qty} (Instant Trade Attempted)" if manual_qty else "Auto-sizing: 20% of available USDT balance"

    await update.message.reply_text(
        f"Monitoring {symbol}!\n"
        f"\u2022 {qty_info}\n"
        f"\u2022 Entry filters: ADX > {ADX_THRESHOLD} + EMA (30m) + RSI + MACD Line/Signal (30m)\n"
        f"\u2022 Exit: Long exits when Price < 20 EMA AND MACD < Signal (30m) | Short exits when Price > 20 EMA AND MACD > Signal (30m)\n"
        f"\u2022 Reverse only if ADX > {ADX_REVERSAL_THRESHOLD} + EMA/MACD alignment (30m), else FLAT\n"
        f"\u2022 SL: 30% at {SL_STAGE1_ATR_MULT}x ATR + 70% at {STOP_LOSS_ATR_MULT}x ATR (server-side SL always active)\n"
        f"\u2022 MACD Partial: Close 60% on adverse MACD cross, add back on favorable re-cross\n"
        f"\u2022 Trail: 60% at {TRAILING_ACTIVATE_ATR}x ATR profit ({TRAILING_STOP_ATR}x ATR dist), then 30% ({TRAILING_STOP_ATR_STAGE2}x ATR dist), 10% rides until Exit Rules exit\n"
        f"\u2022 WebSocket real-time price feeds + polling every {POLL_INTERVAL_SECONDS}s\n"
        f"\u2022 State persistence: crash recovery via {STATE_FILE}\n"
        f"\u2022 Status report every 30 minutes"
    )

    if context.job_queue:
        existing_jobs = context.job_queue.get_jobs_by_name(f"{symbol}_monitor")
        if not existing_jobs:
            context.job_queue.run_repeating(
                callback=monitor_symbol,
                interval=POLL_INTERVAL_SECONDS,
                first=POLL_INTERVAL_SECONDS,
                chat_id=chat_id,
                name=f"{symbol}_monitor",
                data={"symbol": symbol, "last_report_time": time.time()},
            )

async def remove_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Privacy: only the owner can use this bot
    if update.effective_chat.id != CHAT_ID:
        await update.message.reply_text("\u26d4 Unauthorized. This bot is private.")
        return
    if not context.args:
        await update.message.reply_text("Usage: /remove_symbol <symbol>")
        return

    symbol = context.args[0].upper()
    if symbol not in monitored_symbols:
        await update.message.reply_text(f"{symbol} is not being monitored.")
        return

    monitored_symbols.remove(symbol)
    if symbol in symbol_trends:
        del symbol_trends[symbol]
    if symbol in symbol_manual_quantities:
        del symbol_manual_quantities[symbol]
    if symbol in symbol_last_di_cross_candle:
        del symbol_last_di_cross_candle[symbol]
    # Cancel any server-side stops before removing
    await cancel_server_stops(symbol)
    # Clean up risk management state
    for state_dict in [symbol_entry_price, symbol_entry_atr, symbol_peak_price, symbol_trailing_active,
                        symbol_partial_taken, symbol_server_sl_set, symbol_server_ts_set,
                        symbol_trail_order_id, symbol_trail_qty, symbol_trail_price,
                        symbol_stage2_active, symbol_stage2_taken, symbol_trail2_order_id,
                        symbol_trail2_qty, symbol_trail2_price, symbol_peak_stage2,
                        symbol_sl1_order_id, symbol_sl1_qty, symbol_sl1_taken, symbol_original_qty,
                        symbol_macd_partial_closed, symbol_macd_partial_qty,
                        symbol_macd_last_action_candle]:
        if symbol in state_dict:
            del state_dict[symbol]

    if context.job_queue:
        jobs = context.job_queue.get_jobs_by_name(f"{symbol}_monitor")
        for job in jobs:
            job.schedule_removal()

    save_state()
    await update.message.reply_text(f"Stopped monitoring {symbol}.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Privacy: only the owner can use this bot
    if update.effective_chat.id != CHAT_ID:
        await update.message.reply_text("\u26d4 Unauthorized. This bot is private.")
        return
    await update.message.reply_text(
        "Bot ready! (v13 - 4-Layer Filter + 30m MACD Entry/Exit/Reversal + 10-min MACD Partial Exit + WebSocket)\n\n"
        "Commands:\n"
        "1. /start - Display this message\n"
        "2. /add_symbol <symbol> - Start monitoring with auto-sizing (20% balance)\n"
        "3. /add_symbol <symbol> <quantity> - INSTANT TRADE + Manual quantity monitoring\n"
        "4. /remove_symbol <symbol> - Stop monitoring a symbol\n\n"
        "Entry Filters (ALL must pass):\n"
        f"\u2022 1. ADX > {ADX_THRESHOLD}\n"
        "\u2022 2. Long: Price > 20 EMA (30m) | Short: Price < 20 EMA (30m)\n"
        f"\u2022 3. RSI NOT extreme (not > {RSI_OVERBOUGHT} for longs, not < {RSI_OVERSOLD} for shorts)\n"
        "\u2022 4. Long: MACD Line > Signal Line (30m) | Short: MACD Line < Signal Line (30m)\n\n"
        "Partial Exit Rules (MACD-based, 10-min closed-candle confirmation):\n"
        "\u2022 Long: If MACD crosses below Signal on a CLOSED 10-min candle \u2192 close 60%\n"
        "\u2022 If MACD crosses back above Signal on a CLOSED 10-min candle \u2192 add back 60%\n"
        "\u2022 Short: If MACD crosses above Signal on a CLOSED 10-min candle \u2192 close 60%\n"
        "\u2022 If MACD crosses back below Signal on a CLOSED 10-min candle \u2192 add back 60%\n"
        "\u2022 Prevents whipsaw: only acts on confirmed crossovers, not intra-candle noise\n\n"
        "Stop-Loss (Tiered):\n"
        f"\u2022 Tier 1: 30% closed INSTANTLY at {SL_STAGE1_ATR_MULT}x ATR loss\n"
        f"\u2022 Tier 2: Remaining 70% closed at {STOP_LOSS_ATR_MULT}x ATR loss (server-side, always active)\n\n"
        "Trailing Stop (Multi-Stage):\n"
        f"\u2022 At {TRAILING_ACTIVATE_ATR}x ATR profit: Stage 1 trail activates on {TRAILING_CLOSE_PCT*100:.0f}%\n"
        f"\u2022 Stage 1 trail: {TRAILING_STOP_ATR}x ATR distance. Bybit closes {TRAILING_CLOSE_PCT*100:.0f}% INSTANTLY when hit\n"
        f"\u2022 Stage 2 trail: After 60% closed, 30% gets trail ({TRAILING_STOP_ATR_STAGE2}x ATR distance)\n"
        f"\u2022 Stage 2 trail will never be placed below entry price (long) or above entry price (short)\n"
        f"\u2022 Remaining 10% protected by server-side SL + Exit Rules\n\n"
        "Exit Rules:\n"
        "\u2022 Long exits: Price < 20 EMA (30m) AND MACD Line < Signal Line (30m)\n"
        "\u2022 Short exits: Price > 20 EMA (30m) AND MACD Line > Signal Line (30m)\n\n"
        "Reversal Rules:\n"
        f"\u2022 ADX > {ADX_REVERSAL_THRESHOLD} + EMA/MACD alignment (30m) required\n"
        "\u2022 Long reversal: Price > 20 EMA (30m) + MACD > Signal (30m)\n"
        "\u2022 Short reversal: Price < 20 EMA (30m) + MACD < Signal (30m)\n"
        "\u2022 If conditions not met, bot goes FLAT\n\n"
        "Infrastructure:\n"
        "\u2022 WebSocket real-time price feeds (REST fallback)\n"
        f"\u2022 State persistence: {STATE_FILE} (crash recovery)\n"
        f"\u2022 Monitoring every {POLL_INTERVAL_SECONDS}s\n"
        "\u2022 Bot is PRIVATE \u2014 only accessible by owner\n\n"
        "Examples:\n"
        "\u2022 /add_symbol BTCUSDT\n"
        "\u2022 /add_symbol ETHUSDT 0.5"
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
