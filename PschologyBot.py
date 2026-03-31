
import os
import json
import asyncio
import random
import time
import threading
import concurrent.futures
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
#  Thread pool & concurrency controls
# ────────────────────────────────────────────────
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=32)
_api_semaphore: asyncio.Semaphore = asyncio.Semaphore(10)


async def _api_call(func, *args, **kwargs):
    """Wrap ANY synchronous Bybit API call with semaphore + thread."""
    async with _api_semaphore:
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(func, *args, **kwargs),
                timeout=15.0,
            )
        except asyncio.TimeoutError:
            print(f"[API-TIMEOUT] {func.__name__} timed out after 15s")
            raise

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
    """WebSocket callback for real-time ticker updates."""
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
    """Get the latest WebSocket price for a symbol."""
    with ws_prices_lock:
        return ws_prices.get(symbol)


# ════════════════════════════════════════════════════════════════════════
#  ACMS (Adaptive Confluence Momentum System) v2.0 — STRATEGY PARAMETERS
# ════════════════════════════════════════════════════════════════════════
ACMS_PARAMS = {
    "macd_fast": 12, "macd_slow": 26, "macd_signal": 9,
    "vnm_period": 14, "vnm_atr_period": 14, "vnm_threshold": 0.3,
    "vwdi_period": 14, "vwdi_smooth": 5,
    "tpi_short_ema": 8, "tpi_mid_ema": 21, "tpi_long_ema": 55, "tpi_lookback": 10,
    "apo_period": 20, "apo_mult": 2.0, "apo_smooth": 5,
    "msm_fast": 3, "msm_slow": 8, "msm_vol_period": 10,
    "long_entry_threshold": 70, "short_entry_threshold": 70,
    "atr_period": 14,
    "stop_loss_atr_mult": 2.0, "take_profit_atr_mult": 3.0,
    "trailing_stop_atr_mult": 1.5, "trailing_activation_atr_mult": 1.0,
    "trail_accel": 0.02, "trail_accel_max": 0.12,
    "max_hold_bars": 60,
    "cooldown_bars": 3,
    "confirm_bars": 2,
    "adx_period": 14, "adx_trend_threshold": 20,
    "regime_boost": 8,
}

POLL_INTERVAL_SECONDS = 10    # Poll every 10 seconds for risk management
KLINE_INTERVAL = "15"         # 15-minute candles (Bybit format)
KLINE_LIMIT = 200             # Number of candles to fetch
BAR_SECONDS = 900             # 15 minutes in seconds

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


# ════════════════════════════════════════════════════════════════════════
#  ACMS CUSTOM INDICATOR FUNCTIONS (vectorized numpy for speed)
# ════════════════════════════════════════════════════════════════════════

def np_ema(arr, period):
    """Fast EMA using pandas on numpy array."""
    return pd.Series(arr).ewm(span=period, adjust=False).mean().values

def np_sma(arr, period):
    """Fast SMA using pandas on numpy array."""
    return pd.Series(arr).rolling(window=period, min_periods=1).mean().values

def np_atr(high, low, close, period):
    """Average True Range on numpy arrays."""
    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    tr1 = high - low
    tr2 = np.abs(high - prev_close)
    tr3 = np.abs(low - prev_close)
    tr = np.maximum(np.maximum(tr1, tr2), tr3)
    return pd.Series(tr).rolling(window=period, min_periods=1).mean().values

def np_rsi(close, period=14):
    """RSI on numpy array."""
    delta = np.diff(close, prepend=close[0])
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)
    avg_gain = np_ema(gain, period)
    avg_loss = np_ema(loss, period)
    avg_loss = np.where(avg_loss == 0, 1e-10, avg_loss)
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))

def np_adx(high, low, close, period=14):
    """Average Directional Index for regime detection."""
    prev_high = np.roll(high, 1); prev_high[0] = high[0]
    prev_low = np.roll(low, 1); prev_low[0] = low[0]
    prev_close = np.roll(close, 1); prev_close[0] = close[0]
    plus_dm = np.where((high - prev_high) > (prev_low - low),
                       np.maximum(high - prev_high, 0), 0.0)
    minus_dm = np.where((prev_low - low) > (high - prev_high),
                        np.maximum(prev_low - low, 0), 0.0)
    tr1 = high - low
    tr2 = np.abs(high - prev_close)
    tr3 = np.abs(low - prev_close)
    tr = np.maximum(np.maximum(tr1, tr2), tr3)
    atr_val = np_ema(tr, period)
    atr_val = np.where(atr_val == 0, 1e-10, atr_val)
    plus_di = np_ema(plus_dm, period) / atr_val * 100
    minus_di = np_ema(minus_dm, period) / atr_val * 100
    di_sum = plus_di + minus_di
    di_sum = np.where(di_sum == 0, 1e-10, di_sum)
    dx = np.abs(plus_di - minus_di) / di_sum * 100
    adx = np_ema(dx, period)
    return adx, plus_di, minus_di

def indicator_macd(close, fast, slow, signal_period):
    """MACD - Moving Average Convergence Divergence."""
    ema_fast = np_ema(close, fast)
    ema_slow = np_ema(close, slow)
    macd_line = ema_fast - ema_slow
    signal_line = np_ema(macd_line, signal_period)
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def indicator_vnm(close, high_arr, low_arr, period=14, atr_period=14):
    """Volatility-Normalized Momentum (VNM) - CUSTOM INDICATOR #1"""
    roc = np.zeros_like(close)
    safe_prev = np.where(close[:-period] == 0, 1e-10, close[:-period])
    roc[period:] = (close[period:] - close[:-period]) / safe_prev
    atr_val = np_atr(high_arr, low_arr, close, atr_period)
    safe_close = np.where(close == 0, 1e-10, close)
    atr_ratio = atr_val / safe_close
    atr_ratio = np.where(atr_ratio == 0, 1e-10, atr_ratio)
    vnm_raw = roc / atr_ratio
    return np_ema(vnm_raw, 5)

def indicator_vwdi(close, high_arr, low_arr, volume, period=14, smooth=5):
    """Volume-Weighted Directional Index (VWDI) - CUSTOM INDICATOR #2"""
    prev_close = np.roll(close, 1); prev_close[0] = close[0]
    direction = np.sign(close - prev_close)
    bar_range = high_arr - low_arr
    bar_range = np.where(bar_range == 0, 1e-10, bar_range)
    move_quality = np.abs(close - prev_close) / bar_range
    avg_vol = np_sma(volume, period)
    avg_vol = np.where(np.isnan(avg_vol) | (avg_vol == 0), 1e-10, avg_vol)
    vol_weight = volume / avg_vol
    raw = direction * move_quality * vol_weight
    return np_sma(raw, smooth) * 100

def indicator_tpi(close, short_p=8, mid_p=21, long_p=55, lookback=10):
    """Trend Persistence Index (TPI) - CUSTOM INDICATOR #3"""
    ema_s = np_ema(close, short_p)
    ema_m = np_ema(close, mid_p)
    ema_l = np_ema(close, long_p)
    safe_close = np.where(close == 0, 1e-10, close)
    spread_sm = (ema_s - ema_m) / safe_close
    spread_ml = (ema_m - ema_l) / safe_close
    alignment = (spread_sm + spread_ml) / 2.0
    prev_close = np.roll(close, 1); prev_close[0] = close[0]
    direction = np.sign(close - prev_close)
    consistency = np_sma(direction, lookback)
    consistency = np.nan_to_num(consistency, nan=0.0)
    tpi_raw = alignment * np.abs(consistency) * np.sign(consistency) * 10000
    tpi_raw = np.clip(tpi_raw, -100, 100)
    return np_ema(tpi_raw, 5)

def indicator_apo(close, high_arr, low_arr, period=20, mult=2.0, smooth=5):
    """Adaptive Pressure Oscillator (APO) - CUSTOM INDICATOR #4"""
    mid_band = np_ema(close, period)
    atr_val = np_atr(high_arr, low_arr, close, period)
    channel = atr_val * mult
    channel = np.where(channel == 0, 1e-10, channel)
    position = (close - mid_band) / channel
    pressure = np_ema(position, smooth) * 100
    return np.clip(pressure, -100, 100)

def indicator_msm(close, high_arr, low_arr, volume, fast=3, slow=8, vol_period=10):
    """Micro Structure Momentum (MSM) - CUSTOM INDICATOR #5"""
    ema_f = np_ema(close, fast)
    ema_s = np_ema(close, slow)
    safe_close = np.where(close == 0, 1e-10, close)
    vel_fast = np.zeros_like(close)
    vel_fast[1:] = (ema_f[1:] - ema_f[:-1]) / safe_close[1:]
    vel_slow = np.zeros_like(close)
    vel_slow[1:] = (ema_s[1:] - ema_s[:-1]) / safe_close[1:]
    vel_slow_abs = np.where(np.abs(vel_slow) == 0, 1e-10, np.abs(vel_slow))
    velocity_ratio = vel_fast / vel_slow_abs
    velocity_ratio = np.clip(velocity_ratio, -10, 10)
    avg_vol = np_sma(volume, vol_period)
    avg_vol = np.where(avg_vol == 0, 1e-10, avg_vol)
    vol_accel = volume / avg_vol
    bar_range = high_arr - low_arr
    bar_range = np.where(bar_range == 0, 1e-10, bar_range)
    prev_close = np.roll(close, 1); prev_close[0] = close[0]
    body = close - prev_close
    body_ratio = body / bar_range
    msm_raw = (velocity_ratio * 0.4 + body_ratio * vol_accel * 0.6) * 50
    msm = np_ema(msm_raw, fast)
    return np.clip(msm, -100, 100)


def compute_acms_indicators(close, high_arr, low_arr, volume, params):
    """Compute all ACMS indicators and confluence scores as numpy arrays."""
    macd_line, signal_line, histogram = indicator_macd(
        close, params["macd_fast"], params["macd_slow"], params["macd_signal"])
    hist_prev = np.roll(histogram, 1); hist_prev[0] = 0
    hist_rising = histogram > hist_prev
    hist_falling = histogram < hist_prev

    vnm = indicator_vnm(close, high_arr, low_arr, params["vnm_period"], params["vnm_atr_period"])
    vnm_prev = np.roll(vnm, 1); vnm_prev[0] = 0

    vwdi = indicator_vwdi(close, high_arr, low_arr, volume, params["vwdi_period"], params["vwdi_smooth"])
    vwdi_prev = np.roll(vwdi, 1); vwdi_prev[0] = 0

    tpi = indicator_tpi(close, params["tpi_short_ema"], params["tpi_mid_ema"],
                        params["tpi_long_ema"], params["tpi_lookback"])
    tpi_prev = np.roll(tpi, 1); tpi_prev[0] = 0

    apo = indicator_apo(close, high_arr, low_arr, params["apo_period"],
                        params["apo_mult"], params["apo_smooth"])
    apo_prev = np.roll(apo, 1); apo_prev[0] = 0

    msm = indicator_msm(close, high_arr, low_arr, volume,
                        params["msm_fast"], params["msm_slow"], params["msm_vol_period"])
    msm_prev = np.roll(msm, 1); msm_prev[0] = 0

    atr_val = np_atr(high_arr, low_arr, close, params["atr_period"])
    rsi_val = np_rsi(close, 14)

    adx, plus_di, minus_di = np_adx(high_arr, low_arr, close, params["adx_period"])
    is_trending = adx > params["adx_trend_threshold"]
    trend_bullish = plus_di > minus_di
    trend_bearish = minus_di > plus_di

    threshold = params["vnm_threshold"]
    regime_boost = params.get("regime_boost", 8)

    # LONG SCORES
    macd_long = np.clip(
        (histogram > 0).astype(float) * 8 +
        (macd_line > signal_line).astype(float) * 7 +
        hist_rising.astype(float) * 5, 0, 20)
    vnm_long = np.clip(
        (vnm > 0).astype(float) * 8 +
        (vnm > threshold).astype(float) * 7 +
        (vnm > vnm_prev).astype(float) * 5, 0, 20)
    vwdi_long = np.clip(
        (vwdi > 0).astype(float) * 8 +
        (vwdi > vwdi_prev).astype(float) * 7 +
        (vwdi > 15).astype(float) * 5, 0, 20)
    tpi_long = np.clip(
        (tpi > 0).astype(float) * 8 +
        (tpi > 15).astype(float) * 7 +
        (tpi > tpi_prev).astype(float) * 5, 0, 20)
    msm_long = np.clip(
        (msm > 0).astype(float) * 4 +
        (msm > msm_prev).astype(float) * 3 +
        (msm > 10).astype(float) * 3, 0, 10)
    apo_long_bonus = np.clip(
        ((apo > -50) & (apo < 50)).astype(float) * 5 +
        (apo > apo_prev).astype(float) * 5, 0, 10)
    regime_long_bonus = (is_trending & trend_bullish).astype(float) * regime_boost
    rsi_long_ok = ((rsi_val > 30) & (rsi_val < 75)).astype(float)
    long_score = (macd_long + vnm_long + vwdi_long + tpi_long +
                  msm_long + apo_long_bonus + regime_long_bonus) * rsi_long_ok

    # SHORT SCORES
    macd_short = np.clip(
        (histogram < 0).astype(float) * 8 +
        (macd_line < signal_line).astype(float) * 7 +
        hist_falling.astype(float) * 5, 0, 20)
    vnm_short = np.clip(
        (vnm < 0).astype(float) * 8 +
        (vnm < -threshold).astype(float) * 7 +
        (vnm < vnm_prev).astype(float) * 5, 0, 20)
    vwdi_short = np.clip(
        (vwdi < 0).astype(float) * 8 +
        (vwdi < vwdi_prev).astype(float) * 7 +
        (vwdi < -15).astype(float) * 5, 0, 20)
    tpi_short = np.clip(
        (tpi < 0).astype(float) * 8 +
        (tpi < -15).astype(float) * 7 +
        (tpi < tpi_prev).astype(float) * 5, 0, 20)
    msm_short = np.clip(
        (msm < 0).astype(float) * 4 +
        (msm < msm_prev).astype(float) * 3 +
        (msm < -10).astype(float) * 3, 0, 10)
    apo_short_bonus = np.clip(
        ((apo > -50) & (apo < 50)).astype(float) * 5 +
        (apo < apo_prev).astype(float) * 5, 0, 10)
    regime_short_bonus = (is_trending & trend_bearish).astype(float) * regime_boost
    rsi_short_ok = ((rsi_val > 25) & (rsi_val < 70)).astype(float)
    short_score = (macd_short + vnm_short + vwdi_short + tpi_short +
                   msm_short + apo_short_bonus + regime_short_bonus) * rsi_short_ok

    return {
        "long_score": long_score, "short_score": short_score, "atr": atr_val,
        "macd_line": macd_line, "macd_signal": signal_line, "macd_hist": histogram,
        "vnm": vnm, "vwdi": vwdi, "tpi": tpi, "apo": apo, "msm": msm,
        "rsi": rsi_val, "adx": adx, "plus_di": plus_di, "minus_di": minus_di,
    }


# ────────────────────────────────────────────────
#  State Persistence (JSON)
# ────────────────────────────────────────────────
STATE_FILE = os.getenv("STATE_FILE", "/opt/render/project/src/bot_state.json")

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
symbol_peak_price: dict[str, Optional[float]] = {}       # highest for long, lowest for short
symbol_trailing_active: dict[str, bool] = {}
symbol_trail_price: dict[str, Optional[float]] = {}       # current trailing stop trigger price
symbol_trail_start_time: dict[str, Optional[float]] = {}  # when trailing was activated
symbol_server_sl_set: dict[str, bool] = {}
symbol_original_qty: dict[str, Optional[float]] = {}

# ACMS signal persistence tracking (only count once per completed bar)
symbol_long_signal_count: dict[str, int] = {}
symbol_short_signal_count: dict[str, int] = {}
symbol_last_counted_bar: dict[str, Optional[int]] = {}     # timestamp of last counted bar

# ACMS trade management
symbol_entry_time: dict[str, Optional[float]] = {}         # timestamp of entry
symbol_last_exit_time: dict[str, Optional[float]] = {}     # timestamp of last exit
symbol_last_was_loss: dict[str, bool] = {}                  # whether last trade was a loss

# Per-symbol monitor execution guard
symbol_monitor_running: dict[str, bool] = {}


def _save_state_sync():
    """Synchronous disk write."""
    try:
        state = {
            "symbol_trends": dict(symbol_trends),
            "monitored_symbols": list(monitored_symbols),
            "symbol_open_trade_side": dict(symbol_open_trade_side),
            "symbol_manual_quantities": dict(symbol_manual_quantities),
            "symbol_entry_price": dict(symbol_entry_price),
            "symbol_entry_atr": dict(symbol_entry_atr),
            "symbol_peak_price": dict(symbol_peak_price),
            "symbol_trailing_active": dict(symbol_trailing_active),
            "symbol_trail_price": dict(symbol_trail_price),
            "symbol_trail_start_time": dict(symbol_trail_start_time),
            "symbol_server_sl_set": dict(symbol_server_sl_set),
            "symbol_original_qty": dict(symbol_original_qty),
            "symbol_long_signal_count": dict(symbol_long_signal_count),
            "symbol_short_signal_count": dict(symbol_short_signal_count),
            "symbol_last_counted_bar": dict(symbol_last_counted_bar),
            "symbol_entry_time": dict(symbol_entry_time),
            "symbol_last_exit_time": dict(symbol_last_exit_time),
            "symbol_last_was_loss": dict(symbol_last_was_loss),
        }
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2, default=str)
        print(f"[STATE] Saved state to {STATE_FILE}")
    except Exception as e:
        print(f"[STATE] Failed to save state: {e}")


def save_state():
    """Non-blocking state save."""
    try:
        loop = asyncio.get_running_loop()
        loop.run_in_executor(_executor, _save_state_sync)
    except RuntimeError:
        _save_state_sync()

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
        symbol_trailing_active.update(state.get("symbol_trailing_active", {}))
        symbol_trail_price.update(state.get("symbol_trail_price", {}))
        symbol_trail_start_time.update(state.get("symbol_trail_start_time", {}))
        symbol_server_sl_set.update(state.get("symbol_server_sl_set", {}))
        symbol_original_qty.update(state.get("symbol_original_qty", {}))
        symbol_long_signal_count.update(state.get("symbol_long_signal_count", {}))
        symbol_short_signal_count.update(state.get("symbol_short_signal_count", {}))
        symbol_last_counted_bar.update(state.get("symbol_last_counted_bar", {}))
        symbol_entry_time.update(state.get("symbol_entry_time", {}))
        symbol_last_exit_time.update(state.get("symbol_last_exit_time", {}))
        symbol_last_was_loss.update(state.get("symbol_last_was_loss", {}))

        print(f"[STATE] Loaded state from {STATE_FILE} -- {len(monitored_symbols)} symbols restored")
        return True
    except Exception as e:
        print(f"[STATE] Failed to load state: {e}")
        return False


# ────────────────────────────────────────────────
#  Motivational Quotes
# ────────────────────────────────────────────────

async def send_motivational_quote(context: ContextTypes.DEFAULT_TYPE) -> None:
    quote = random.choice(MOTIVATIONAL_QUOTES)
    chat_id = context.job.chat_id if context.job else CHAT_ID
    await context.bot.send_message(chat_id=chat_id, text=quote)


# ────────────────────────────────────────────────
#  Price Functions
# ────────────────────────────────────────────────

async def get_current_price(symbol: str) -> Optional[float]:
    """Get current price: WebSocket first, REST ticker fallback."""
    ws_price = get_ws_price(symbol)
    if ws_price is not None:
        return ws_price
    try:
        ticker_resp = await _api_call(
            bybit_session.get_tickers,
            category="linear",
            symbol=symbol
        )
        return float(ticker_resp["result"]["list"][0]["lastPrice"])
    except Exception as e:
        print(f"Error getting current price for {symbol}: {e}")
        return None


# ────────────────────────────────────────────────
#  ACMS Indicator Computation (Bybit klines)
# ────────────────────────────────────────────────

def _compute_acms_indicators_sync(symbol: str) -> dict:
    """
    Synchronous helper: fetches 15-minute klines from Bybit AND computes
    all ACMS indicators (VNM, VWDI, TPI, APO, MSM, MACD, confluence scores)
    in ONE thread call so neither the API call nor the numpy math blocks
    the asyncio event loop.
    """
    resp = bybit_session.get_kline(
        category="linear",
        symbol=symbol,
        interval=KLINE_INTERVAL,
        limit=KLINE_LIMIT,
    )
    klines = resp["result"]["list"]
    if not klines:
        raise ValueError("No klines returned")

    df = pd.DataFrame(
        klines,
        columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"],
    ).iloc[::-1].reset_index(drop=True).copy()

    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if df["close"].isna().any():
        raise ValueError("Invalid prices in kline data")

    close = df["close"].values.astype(np.float64)
    high_arr = df["high"].values.astype(np.float64)
    low_arr = df["low"].values.astype(np.float64)
    volume = df["volume"].values.astype(np.float64)

    indicators = compute_acms_indicators(close, high_arr, low_arr, volume, ACMS_PARAMS)

    # Use second-to-last index (last COMPLETED bar) for decision making
    last_idx = len(close) - 2  # last completed bar
    bar_timestamp = int(df["timestamp"].iloc[last_idx])  # timestamp of completed bar

    return {
        "long_score": float(indicators["long_score"][last_idx]),
        "short_score": float(indicators["short_score"][last_idx]),
        "atr": float(indicators["atr"][last_idx]),
        "adx": float(indicators["adx"][last_idx]),
        "rsi": float(indicators["rsi"][last_idx]),
        "macd_line": float(indicators["macd_line"][last_idx]),
        "macd_signal": float(indicators["macd_signal"][last_idx]),
        "macd_hist": float(indicators["macd_hist"][last_idx]),
        "vnm": float(indicators["vnm"][last_idx]),
        "vwdi": float(indicators["vwdi"][last_idx]),
        "tpi": float(indicators["tpi"][last_idx]),
        "apo": float(indicators["apo"][last_idx]),
        "msm": float(indicators["msm"][last_idx]),
        "plus_di": float(indicators["plus_di"][last_idx]),
        "minus_di": float(indicators["minus_di"][last_idx]),
        "last_close": float(close[last_idx]),
        "bar_timestamp": bar_timestamp,
    }


async def get_all_indicators(symbol: str) -> Optional[dict]:
    """
    Returns a dict with ACMS trading indicators from 15-minute klines.
    API call + numpy computation run entirely in a background thread.
    """
    try:
        result = await _api_call(_compute_acms_indicators_sync, symbol)

        current_price = await get_current_price(symbol)
        if current_price is None:
            current_price = result["last_close"]
        result["current_price"] = current_price

        return result
    except Exception as exc:
        print(f"Error in get_all_indicators for {symbol}: {exc}")
        return None


# ────────────────────────────────────────────────
#  Position Management
# ────────────────────────────────────────────────

async def get_open_position_side(symbol: str) -> Optional[str]:
    try:
        positions_resp = await _api_call(
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
        info = await _api_call(
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
    """Round quantity DOWN to the nearest valid step increment."""
    if qty_step > 0:
        qty = (qty // qty_step) * qty_step
    return max(qty, min_qty)

def format_qty_str(qty: float, qty_step: float) -> str:
    """Format quantity as a string with the correct decimal precision."""
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

        balance_resp = await _api_call(
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
    """Closes the entire position. Also clears risk management state."""
    try:
        positions_resp = await _api_call(
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

                    order = await _api_call(
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

                _clear_risk_state(symbol)

                return total_qty
        return None
    except Exception as exc:
        print(f"Position close failed for {symbol}: {exc}")
        return None


# ────────────────────────────────────────────────
#  Risk State Management
# ────────────────────────────────────────────────

def _clear_risk_state(symbol: str) -> None:
    """Clear per-symbol risk management tracking after closing a position."""
    symbol_entry_price[symbol] = None
    symbol_entry_atr[symbol] = None
    symbol_peak_price[symbol] = None
    symbol_trailing_active[symbol] = False
    symbol_trail_price[symbol] = None
    symbol_trail_start_time[symbol] = None
    symbol_server_sl_set[symbol] = False
    symbol_original_qty[symbol] = None
    symbol_entry_time[symbol] = None
    # Reset signal persistence
    symbol_long_signal_count[symbol] = 0
    symbol_short_signal_count[symbol] = 0
    symbol_last_counted_bar[symbol] = None
    save_state()


def _set_entry_risk_state(symbol: str, entry_price: float, atr: float) -> None:
    """Set per-symbol risk management tracking after opening a position."""
    symbol_entry_price[symbol] = entry_price
    symbol_entry_atr[symbol] = atr
    symbol_peak_price[symbol] = entry_price
    symbol_trailing_active[symbol] = False
    symbol_trail_price[symbol] = None
    symbol_trail_start_time[symbol] = None
    symbol_server_sl_set[symbol] = False
    symbol_original_qty[symbol] = None
    symbol_entry_time[symbol] = time.time()
    # Reset signal persistence
    symbol_long_signal_count[symbol] = 0
    symbol_short_signal_count[symbol] = 0
    save_state()


# ────────────────────────────────────────────────
#  Server-Side Stop-Loss (Bybit)
# ────────────────────────────────────────────────

async def set_server_stop_loss(symbol: str, side: str, stop_price: float) -> bool:
    """Set/update a server-side stop-loss on Bybit for the full position."""
    try:
        resp = await _api_call(
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
            print(f"[SERVER-SL] {symbol}: Stop-loss set at {stop_price:.6g}")
            return True
        else:
            print(f"[SERVER-SL] {symbol}: Failed to set SL - {resp.get('retMsg')}")
            return False
    except Exception as e:
        print(f"[SERVER-SL] {symbol}: Error setting stop-loss: {e}")
        return False


async def get_position_size(symbol: str) -> Optional[float]:
    """Get current position size (qty) for a symbol."""
    try:
        resp = await _api_call(
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
    """Get the actual average entry price of the current position from Bybit."""
    try:
        resp = await _api_call(
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


async def cancel_server_stops(symbol: str) -> bool:
    """Cancel all server-side TP/SL/trailing stops."""
    try:
        resp = await _api_call(
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
            print(f"[SERVER] {symbol}: All server-side stops cancelled")
            return True
        else:
            print(f"[SERVER] {symbol}: Failed to cancel stops - {resp.get('retMsg')}")
            return False
    except Exception as e:
        print(f"[SERVER] {symbol}: Error cancelling stops: {e}")
        return False


# ────────────────────────────────────────────────
#  Trade Execution
# ────────────────────────────────────────────────

async def place_trade(symbol: str, side: str = "Buy", manual_qty_override: Optional[float] = None) -> Optional[dict]:
    """Places a trade, optionally using a specific manual quantity override."""
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

            order = await _api_call(
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


# ════════════════════════════════════════════════════════════════════════
#  MONITOR — ACMS STRATEGY EXECUTION
# ════════════════════════════════════════════════════════════════════════

async def monitor_symbol(context: ContextTypes.DEFAULT_TYPE) -> None:
    symbol = context.job.data["symbol"]

    if symbol_monitor_running.get(symbol, False):
        print(f"[GUARD] {symbol}: Previous monitor cycle still running, skipping this tick")
        return
    symbol_monitor_running[symbol] = True

    try:
        await asyncio.wait_for(
            _monitor_symbol_inner(context, symbol),
            timeout=55.0,
        )
    except asyncio.TimeoutError:
        print(f"[MONITOR] {symbol}: Cycle TIMED OUT after 55s")
    except Exception as exc:
        print(f"[MONITOR] {symbol}: Unhandled error: {exc}")
    finally:
        symbol_monitor_running[symbol] = False


async def _monitor_symbol_inner(context: ContextTypes.DEFAULT_TYPE, symbol: str) -> None:
    """Inner monitor logic: ACMS trading strategy."""
    params = ACMS_PARAMS

    # Fetch market data in PARALLEL
    current_open_trade_side, indicators = await asyncio.gather(
        get_open_position_side(symbol),
        get_all_indicators(symbol),
    )
    symbol_open_trade_side[symbol] = current_open_trade_side

    if indicators is None:
        print(f"Skipping monitoring for {symbol}: Could not fetch indicators")
        return

    current_price = indicators["current_price"]
    last_close = indicators["last_close"]
    long_score = indicators["long_score"]
    short_score = indicators["short_score"]
    atr = indicators["atr"]
    adx = indicators["adx"]
    rsi = indicators["rsi"]
    bar_timestamp = indicators["bar_timestamp"]

    chat_id = context.job.chat_id if context.job else CHAT_ID

    # ──────────────────────────────────────────────
    # 1. RISK MANAGEMENT (if in trade)
    #    - Stop Loss: ATR-based (server-side + client backup)
    #    - Take Profit: ATR-based target
    #    - Trailing Stop: ATR-based with parabolic acceleration
    #    - Reversal: Opposite confluence signal closes position
    #    - Max Hold: Close after max_hold_bars elapsed
    # ──────────────────────────────────────────────
    if current_open_trade_side is not None:
        entry_price = symbol_entry_price.get(symbol)
        entry_atr = symbol_entry_atr.get(symbol)

        # If we have a position but no tracked entry (bot restarted), set defaults
        if entry_price is None or entry_atr is None:
            entry_price = current_price
            entry_atr = atr
            _set_entry_risk_state(symbol, entry_price, entry_atr)
            print(f"[INFO] {symbol}: Initialized risk state (entry={entry_price}, atr={entry_atr})")
            if current_open_trade_side == "Buy":
                sl_price = entry_price - (params["stop_loss_atr_mult"] * entry_atr)
            else:
                sl_price = entry_price + (params["stop_loss_atr_mult"] * entry_atr)
            await set_server_stop_loss(symbol, current_open_trade_side, sl_price)

        # Ensure server-side stop-loss is set (retry if not)
        if not symbol_server_sl_set.get(symbol, False):
            if current_open_trade_side == "Buy":
                sl_price = entry_price - (params["stop_loss_atr_mult"] * entry_atr)
            else:
                sl_price = entry_price + (params["stop_loss_atr_mult"] * entry_atr)
            await set_server_stop_loss(symbol, current_open_trade_side, sl_price)

        # Update peak price tracking using REAL-TIME price
        peak = symbol_peak_price.get(symbol, entry_price)
        if current_open_trade_side == "Buy":
            peak = max(peak, current_price)
        else:
            peak = min(peak, current_price)
        symbol_peak_price[symbol] = peak

        # ── STOP-LOSS CHECK (client-side backup) ──
        if current_open_trade_side == "Buy":
            sl = entry_price - (params["stop_loss_atr_mult"] * entry_atr)
            if current_price <= sl:
                pnl = (current_price - entry_price) / entry_price * 100
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"\U0001f6d1 STOP-LOSS HIT for {symbol} (Long)\n"
                        f"Entry: {entry_price:.6g} | Exit: {current_price:.6g}\n"
                        f"PnL: {pnl:.2f}%"
                    )
                )
                await cancel_server_stops(symbol)
                await close_position(symbol, "Buy")
                symbol_last_was_loss[symbol] = True
                symbol_last_exit_time[symbol] = time.time()
                symbol_trends[symbol] = None
                symbol_open_trade_side[symbol] = None
                _clear_risk_state(symbol)
                return
        else:
            sl = entry_price + (params["stop_loss_atr_mult"] * entry_atr)
            if current_price >= sl:
                pnl = (entry_price - current_price) / entry_price * 100
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"\U0001f6d1 STOP-LOSS HIT for {symbol} (Short)\n"
                        f"Entry: {entry_price:.6g} | Exit: {current_price:.6g}\n"
                        f"PnL: {pnl:.2f}%"
                    )
                )
                await cancel_server_stops(symbol)
                await close_position(symbol, "Sell")
                symbol_last_was_loss[symbol] = True
                symbol_last_exit_time[symbol] = time.time()
                symbol_trends[symbol] = None
                symbol_open_trade_side[symbol] = None
                _clear_risk_state(symbol)
                return

        # ── TAKE-PROFIT CHECK ──
        if current_open_trade_side == "Buy":
            tp = entry_price + (params["take_profit_atr_mult"] * entry_atr)
            if current_price >= tp:
                pnl = (tp - entry_price) / entry_price * 100
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"\U0001f3af TAKE-PROFIT HIT for {symbol} (Long)\n"
                        f"Entry: {entry_price:.6g} | TP: {tp:.6g}\n"
                        f"PnL: {pnl:.2f}%"
                    )
                )
                await cancel_server_stops(symbol)
                await close_position(symbol, "Buy")
                symbol_last_was_loss[symbol] = False
                symbol_last_exit_time[symbol] = time.time()
                symbol_trends[symbol] = None
                symbol_open_trade_side[symbol] = None
                _clear_risk_state(symbol)
                return
        else:
            tp = entry_price - (params["take_profit_atr_mult"] * entry_atr)
            if current_price <= tp:
                pnl = (entry_price - tp) / entry_price * 100
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"\U0001f3af TAKE-PROFIT HIT for {symbol} (Short)\n"
                        f"Entry: {entry_price:.6g} | TP: {tp:.6g}\n"
                        f"PnL: {pnl:.2f}%"
                    )
                )
                await cancel_server_stops(symbol)
                await close_position(symbol, "Sell")
                symbol_last_was_loss[symbol] = False
                symbol_last_exit_time[symbol] = time.time()
                symbol_trends[symbol] = None
                symbol_open_trade_side[symbol] = None
                _clear_risk_state(symbol)
                return

        # ── TRAILING STOP with parabolic acceleration ──
        trailing_active = symbol_trailing_active.get(symbol, False)

        if current_open_trade_side == "Buy":
            profit_dist = peak - entry_price
            if profit_dist >= entry_atr * params["trailing_activation_atr_mult"]:
                if not trailing_active:
                    symbol_trailing_active[symbol] = True
                    trailing_active = True
                    symbol_trail_start_time[symbol] = time.time()
                    save_state()
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            f"\U0001f4c8 TRAILING STOP ACTIVATED for {symbol} (Long)\n"
                            f"Peak: {peak:.6g} | Profit: {profit_dist / entry_atr:.2f}x ATR"
                        )
                    )

                # Compute acceleration based on bars since trail activation
                trail_start = symbol_trail_start_time.get(symbol, time.time())
                bars_since = int((time.time() - trail_start) / BAR_SECONDS)
                accel = min(bars_since * params["trail_accel"], params["trail_accel_max"])
                effective_trail = params["trailing_stop_atr_mult"] * (1.0 - accel)
                new_trail = peak - entry_atr * effective_trail

                current_trail = symbol_trail_price.get(symbol)
                if current_trail is None or new_trail > current_trail:
                    symbol_trail_price[symbol] = new_trail
                    # Update server-side SL to trailing level
                    await set_server_stop_loss(symbol, "Buy", new_trail)

                trail_price = symbol_trail_price.get(symbol, 0)
                if trail_price > 0 and current_price <= trail_price:
                    pnl = (trail_price - entry_price) / entry_price * 100
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            f"\U0001f3c1 TRAILING STOP HIT for {symbol} (Long)\n"
                            f"Trail: {trail_price:.6g} | Peak: {peak:.6g}\n"
                            f"PnL: {pnl:.2f}%"
                        )
                    )
                    await cancel_server_stops(symbol)
                    await close_position(symbol, "Buy")
                    symbol_last_was_loss[symbol] = pnl < 0
                    symbol_last_exit_time[symbol] = time.time()
                    symbol_trends[symbol] = None
                    symbol_open_trade_side[symbol] = None
                    _clear_risk_state(symbol)
                    return

        else:  # Short
            profit_dist = entry_price - peak  # peak is the trough for shorts
            if profit_dist >= entry_atr * params["trailing_activation_atr_mult"]:
                if not trailing_active:
                    symbol_trailing_active[symbol] = True
                    trailing_active = True
                    symbol_trail_start_time[symbol] = time.time()
                    save_state()
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            f"\U0001f4c8 TRAILING STOP ACTIVATED for {symbol} (Short)\n"
                            f"Trough: {peak:.6g} | Profit: {profit_dist / entry_atr:.2f}x ATR"
                        )
                    )

                trail_start = symbol_trail_start_time.get(symbol, time.time())
                bars_since = int((time.time() - trail_start) / BAR_SECONDS)
                accel = min(bars_since * params["trail_accel"], params["trail_accel_max"])
                effective_trail = params["trailing_stop_atr_mult"] * (1.0 - accel)
                new_trail = peak + entry_atr * effective_trail

                current_trail = symbol_trail_price.get(symbol)
                if current_trail is None or new_trail < current_trail:
                    symbol_trail_price[symbol] = new_trail
                    await set_server_stop_loss(symbol, "Sell", new_trail)

                trail_price = symbol_trail_price.get(symbol, float('inf'))
                if current_price >= trail_price:
                    pnl = (entry_price - trail_price) / entry_price * 100
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            f"\U0001f3c1 TRAILING STOP HIT for {symbol} (Short)\n"
                            f"Trail: {trail_price:.6g} | Trough: {peak:.6g}\n"
                            f"PnL: {pnl:.2f}%"
                        )
                    )
                    await cancel_server_stops(symbol)
                    await close_position(symbol, "Sell")
                    symbol_last_was_loss[symbol] = pnl < 0
                    symbol_last_exit_time[symbol] = time.time()
                    symbol_trends[symbol] = None
                    symbol_open_trade_side[symbol] = None
                    _clear_risk_state(symbol)
                    return

        # ── REVERSAL: Opposite confluence signal >= threshold + 10 ──
        if current_open_trade_side == "Buy" and short_score >= params["short_entry_threshold"] + 10:
            pnl = (current_price - entry_price) / entry_price * 100
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"\U0001f504 REVERSAL SIGNAL for {symbol} (Long \u2192 Flat)\n"
                    f"Short score: {short_score:.1f} (threshold+10: {params['short_entry_threshold']+10})\n"
                    f"Entry: {entry_price:.6g} | Current: {current_price:.6g}\n"
                    f"PnL: {pnl:.2f}%"
                )
            )
            await cancel_server_stops(symbol)
            await close_position(symbol, "Buy")
            symbol_last_was_loss[symbol] = pnl < 0
            symbol_last_exit_time[symbol] = time.time()
            symbol_trends[symbol] = None
            symbol_open_trade_side[symbol] = None
            _clear_risk_state(symbol)
            return

        elif current_open_trade_side == "Sell" and long_score >= params["long_entry_threshold"] + 10:
            pnl = (entry_price - current_price) / entry_price * 100
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"\U0001f504 REVERSAL SIGNAL for {symbol} (Short \u2192 Flat)\n"
                    f"Long score: {long_score:.1f} (threshold+10: {params['long_entry_threshold']+10})\n"
                    f"Entry: {entry_price:.6g} | Current: {current_price:.6g}\n"
                    f"PnL: {pnl:.2f}%"
                )
            )
            await cancel_server_stops(symbol)
            await close_position(symbol, "Sell")
            symbol_last_was_loss[symbol] = pnl < 0
            symbol_last_exit_time[symbol] = time.time()
            symbol_trends[symbol] = None
            symbol_open_trade_side[symbol] = None
            _clear_risk_state(symbol)
            return

        # ── MAX HOLD CHECK ──
        entry_time = symbol_entry_time.get(symbol)
        if entry_time:
            bars_held = (time.time() - entry_time) / BAR_SECONDS
            if bars_held >= params["max_hold_bars"]:
                if current_open_trade_side == "Buy":
                    pnl = (current_price - entry_price) / entry_price * 100
                else:
                    pnl = (entry_price - current_price) / entry_price * 100
                side_label = "Long" if current_open_trade_side == "Buy" else "Short"
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"\u23f0 MAX HOLD reached for {symbol} ({side_label})\n"
                        f"Bars held: {int(bars_held)}/{params['max_hold_bars']}\n"
                        f"Entry: {entry_price:.6g} | Current: {current_price:.6g}\n"
                        f"PnL: {pnl:.2f}%"
                    )
                )
                await cancel_server_stops(symbol)
                await close_position(symbol, current_open_trade_side)
                symbol_last_was_loss[symbol] = pnl < 0
                symbol_last_exit_time[symbol] = time.time()
                symbol_trends[symbol] = None
                symbol_open_trade_side[symbol] = None
                _clear_risk_state(symbol)
                return

        save_state()

    # ──────────────────────────────────────────────
    # 2. TRADE ENTRY (no open position)
    #    ACMS confluence scoring with signal persistence
    #    - Requires long_score or short_score >= threshold
    #    - Must persist for confirm_bars consecutive COMPLETED bars
    #    - Cooldown after losses (doubled cooldown)
    # ──────────────────────────────────────────────
    if current_open_trade_side is None:

        # Cooldown check
        last_exit = symbol_last_exit_time.get(symbol)
        was_loss = symbol_last_was_loss.get(symbol, False)
        cooldown = params["cooldown_bars"] * 2 if was_loss else params["cooldown_bars"]
        cooldown_seconds = cooldown * BAR_SECONDS

        if last_exit and (time.time() - last_exit) < cooldown_seconds:
            # Still in cooldown — reset signal counts
            symbol_long_signal_count[symbol] = 0
            symbol_short_signal_count[symbol] = 0
            return

        # Signal persistence: only count once per completed bar
        lt = params["long_entry_threshold"]
        st = params["short_entry_threshold"]
        confirm = params["confirm_bars"]

        last_bar = symbol_last_counted_bar.get(symbol)
        if bar_timestamp != last_bar:
            # New completed bar — count signal
            symbol_last_counted_bar[symbol] = bar_timestamp

            if long_score >= lt and long_score > short_score:
                symbol_long_signal_count[symbol] = symbol_long_signal_count.get(symbol, 0) + 1
                symbol_short_signal_count[symbol] = 0
            elif short_score >= st and short_score > long_score:
                symbol_short_signal_count[symbol] = symbol_short_signal_count.get(symbol, 0) + 1
                symbol_long_signal_count[symbol] = 0
            else:
                symbol_long_signal_count[symbol] = 0
                symbol_short_signal_count[symbol] = 0

        # ── LONG ENTRY ──
        if symbol_long_signal_count.get(symbol, 0) >= confirm:
            symbol_long_signal_count[symbol] = 0
            symbol_short_signal_count[symbol] = 0

            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"{ts} \u2014 {symbol} ENTRY SIGNAL: <b>LONG</b>\n"
                    f"Long Score: {long_score:.1f} (threshold: {lt})\n"
                    f"Short Score: {short_score:.1f}\n"
                    f"ADX: {adx:.2f} | RSI: {rsi:.2f}\n"
                    f"Confirmed for {confirm} consecutive bars"
                ),
                parse_mode="HTML"
            )

            result = await place_trade(symbol, side="Buy")
            if result and "error" not in result:
                order_id = result.get("orderId", "\u2014")
                await asyncio.sleep(1)
                real_entry = await get_position_entry_price(symbol)
                fill_price = real_entry if real_entry else current_price
                _set_entry_risk_state(symbol, fill_price, atr)
                pos_qty = await get_position_size(symbol)
                if pos_qty:
                    symbol_original_qty[symbol] = pos_qty
                sl_price = fill_price - (params["stop_loss_atr_mult"] * atr)
                tp_price = fill_price + (params["take_profit_atr_mult"] * atr)
                await set_server_stop_loss(symbol, "Buy", sl_price)
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"\u2705 Long (Buy) placed for {symbol}\n"
                        f"Order ID: <code>{order_id}</code>\n"
                        f"Fill: {fill_price:.6g}\n"
                        f"SL: {sl_price:.6g} ({params['stop_loss_atr_mult']}x ATR)\n"
                        f"TP: {tp_price:.6g} ({params['take_profit_atr_mult']}x ATR)\n"
                        f"Trail activates at {params['trailing_activation_atr_mult']}x ATR profit"
                    ),
                    parse_mode="HTML"
                )
            elif result and "error" in result:
                await context.bot.send_message(chat_id=chat_id, text=f"\u274c Failed to place Long for {symbol}: {result['error']}")

            symbol_trends[symbol] = "upward"
            symbol_open_trade_side[symbol] = await get_open_position_side(symbol)
            save_state()

        # ── SHORT ENTRY ──
        elif symbol_short_signal_count.get(symbol, 0) >= confirm:
            symbol_long_signal_count[symbol] = 0
            symbol_short_signal_count[symbol] = 0

            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"{ts} \u2014 {symbol} ENTRY SIGNAL: <b>SHORT</b>\n"
                    f"Short Score: {short_score:.1f} (threshold: {st})\n"
                    f"Long Score: {long_score:.1f}\n"
                    f"ADX: {adx:.2f} | RSI: {rsi:.2f}\n"
                    f"Confirmed for {confirm} consecutive bars"
                ),
                parse_mode="HTML"
            )

            result = await place_trade(symbol, side="Sell")
            if result and "error" not in result:
                order_id = result.get("orderId", "\u2014")
                await asyncio.sleep(1)
                real_entry = await get_position_entry_price(symbol)
                fill_price = real_entry if real_entry else current_price
                _set_entry_risk_state(symbol, fill_price, atr)
                pos_qty = await get_position_size(symbol)
                if pos_qty:
                    symbol_original_qty[symbol] = pos_qty
                sl_price = fill_price + (params["stop_loss_atr_mult"] * atr)
                tp_price = fill_price - (params["take_profit_atr_mult"] * atr)
                await set_server_stop_loss(symbol, "Sell", sl_price)
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"\u2705 Short (Sell) placed for {symbol}\n"
                        f"Order ID: <code>{order_id}</code>\n"
                        f"Fill: {fill_price:.6g}\n"
                        f"SL: {sl_price:.6g} ({params['stop_loss_atr_mult']}x ATR)\n"
                        f"TP: {tp_price:.6g} ({params['take_profit_atr_mult']}x ATR)\n"
                        f"Trail activates at {params['trailing_activation_atr_mult']}x ATR profit"
                    ),
                    parse_mode="HTML"
                )
            elif result and "error" in result:
                await context.bot.send_message(chat_id=chat_id, text=f"\u274c Failed to place Short for {symbol}: {result['error']}")

            symbol_trends[symbol] = "downward"
            symbol_open_trade_side[symbol] = await get_open_position_side(symbol)
            save_state()

    # ──────────────────────────────────────────────
    # 3. 30-Minute Status Report
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
        if entry_price and entry_atr and current_open_trade_side:
            if current_open_trade_side == "Buy":
                sl = entry_price - (params["stop_loss_atr_mult"] * entry_atr)
                tp = entry_price + (params["take_profit_atr_mult"] * entry_atr)
                unrealized = (current_price - entry_price) / entry_atr
            else:
                sl = entry_price + (params["stop_loss_atr_mult"] * entry_atr)
                tp = entry_price - (params["take_profit_atr_mult"] * entry_atr)
                unrealized = (entry_price - current_price) / entry_atr

            trail_price = symbol_trail_price.get(symbol)
            trail_info = f"ACTIVE @ {trail_price:.6g}" if trailing and trail_price else f"Not yet (at {params['trailing_activation_atr_mult']}x ATR)"

            entry_time = symbol_entry_time.get(symbol)
            bars_held = int((time.time() - entry_time) / BAR_SECONDS) if entry_time else 0

            risk_info = (
                f"\n\u2022 Entry: {entry_price:.6g} | ATR: {entry_atr:.6g}"
                f"\n\u2022 SL: {sl:.6g} ({params['stop_loss_atr_mult']}x ATR)"
                f"\n\u2022 TP: {tp:.6g} ({params['take_profit_atr_mult']}x ATR)"
                f"\n\u2022 Unrealized: {unrealized:.2f}x ATR"
                f"\n\u2022 Peak: {peak:.6g}" if peak else ""
                f"\n\u2022 Trailing: {trail_info}"
                f"\n\u2022 Bars held: {bars_held}/{params['max_hold_bars']}"
                f"\n\u2022 Server SL: {'SET' if symbol_server_sl_set.get(symbol, False) else 'Not set'}"
            )

        # Pending signal info
        pending_info = ""
        l_count = symbol_long_signal_count.get(symbol, 0)
        s_count = symbol_short_signal_count.get(symbol, 0)
        if l_count > 0:
            pending_info = f"\n\u2022 PENDING: Long signal {l_count}/{params['confirm_bars']} bars confirmed"
        elif s_count > 0:
            pending_info = f"\n\u2022 PENDING: Short signal {s_count}/{params['confirm_bars']} bars confirmed"

        # Cooldown info
        cooldown_info = ""
        last_exit = symbol_last_exit_time.get(symbol)
        if last_exit and current_open_trade_side is None:
            was_loss = symbol_last_was_loss.get(symbol, False)
            cd = params["cooldown_bars"] * 2 if was_loss else params["cooldown_bars"]
            cd_seconds = cd * BAR_SECONDS
            remaining = cd_seconds - (time.time() - last_exit)
            if remaining > 0:
                cooldown_info = f"\n\u2022 COOLDOWN: {int(remaining/60)}min remaining ({'loss' if was_loss else 'normal'})"

        report_msg = (
            f"Still monitoring {symbol} (ACMS v2.0)\n"
            f"\u2022 Strategy: Multi-indicator confluence (VNM, VWDI, TPI, APO, MSM, MACD)\n"
            f"\u2022 Entry: Score >= {params['long_entry_threshold']} + {params['confirm_bars']}-bar confirm\n"
            f"\u2022 SL: {params['stop_loss_atr_mult']}x ATR | TP: {params['take_profit_atr_mult']}x ATR\n"
            f"\u2022 Trail: {params['trailing_stop_atr_mult']}x ATR (activates at {params['trailing_activation_atr_mult']}x ATR, accel {params['trail_accel']}\u2192{params['trail_accel_max']})\n"
            f"\u2022 Reversal: opposite score >= threshold+10\n"
            f"\u2022 Max hold: {params['max_hold_bars']} bars | Cooldown: {params['cooldown_bars']} bars\n"
            f"\u2022 15m candles + polling every {POLL_INTERVAL_SECONDS}s\n"
            f"\u2022 Long Score: {long_score:.1f} | Short Score: {short_score:.1f}\n"
            f"\u2022 ADX: {adx:.2f} | RSI: {rsi:.2f}\n"
            f"\u2022 MACD: {indicators['macd_line']:.6g} / Signal: {indicators['macd_signal']:.6g}\n"
            f"\u2022 VNM: {indicators['vnm']:.2f} | VWDI: {indicators['vwdi']:.2f} | TPI: {indicators['tpi']:.2f}\n"
            f"\u2022 APO: {indicators['apo']:.2f} | MSM: {indicators['msm']:.2f}\n"
            f"\u2022 +DI: {indicators['plus_di']:.2f} | -DI: {indicators['minus_di']:.2f}\n"
            f"\u2022 Last Close: {last_close:.6g} | Current: {current_price:.6g}\n"
            f"\u2022 {trade_status}"
            f"{risk_info}"
            f"{pending_info}"
            f"{cooldown_info}"
        )
        if symbol in symbol_manual_quantities and symbol_manual_quantities[symbol] is not None:
            report_msg += f"\n\u2022 Manual Quantity: {symbol_manual_quantities[symbol]}"
        else:
            report_msg += "\n\u2022 Auto-sizing: 20% Balance"

        await context.bot.send_message(chat_id=chat_id, text=report_msg)
        context.job.data["last_report_time"] = current_time


# ════════════════════════════════════════════════════════════════════════
#  TELEGRAM COMMANDS
# ════════════════════════════════════════════════════════════════════════

async def add_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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

    # Subscribe to WebSocket ticker
    subscribe_ws_symbol(symbol)

    chat_id = update.effective_chat.id

    # Handle instant trade for manual quantity
    if manual_qty:
        await update.message.reply_text(f"\U0001f680 INSTANT TRADE for {symbol} with quantity {manual_qty}...")

        indicators = await get_all_indicators(symbol)
        if indicators and indicators["current_price"]:
            long_score = indicators["long_score"]
            short_score = indicators["short_score"]
            atr = indicators["atr"]
            adx = indicators["adx"]
            rsi = indicators["rsi"]
            current_price = indicators["current_price"]

            lt = ACMS_PARAMS["long_entry_threshold"]
            st = ACMS_PARAMS["short_entry_threshold"]

            # Determine direction based on confluence scores
            side = None
            if long_score >= lt and long_score > short_score:
                side = "Buy"
            elif short_score >= st and short_score > long_score:
                side = "Sell"

            if side is None:
                await update.message.reply_text(
                    f"\u26a0\ufe0f No aligned signal for {symbol}.\n"
                    f"Long Score: {long_score:.1f} (threshold: {lt})\n"
                    f"Short Score: {short_score:.1f} (threshold: {st})\n"
                    f"ADX: {adx:.2f} | RSI: {rsi:.2f}\n"
                    f"Need: confluence score >= threshold.\n"
                    f"Monitoring will wait for {ACMS_PARAMS['confirm_bars']}-bar confirmation."
                )
            else:
                side_text = "Long (Buy)" if side == "Buy" else "Short (Sell)"

                await update.message.reply_text(
                    f"\U0001f4ca {symbol}: Score={'Long' if side=='Buy' else 'Short'}={long_score if side=='Buy' else short_score:.1f}\n"
                    f"ADX: {adx:.2f} | RSI: {rsi:.2f}\n"
                    f"VNM: {indicators['vnm']:.2f} | VWDI: {indicators['vwdi']:.2f}\n"
                    f"Direction: {side_text}"
                )

                result = await place_trade(symbol, side=side, manual_qty_override=manual_qty)

                if result and "error" not in result:
                    order_id = result.get("orderId", "\u2014")
                    await asyncio.sleep(1)
                    real_entry = await get_position_entry_price(symbol)
                    fill_price = real_entry if real_entry else current_price
                    _set_entry_risk_state(symbol, fill_price, atr)
                    pos_qty = await get_position_size(symbol)
                    if pos_qty:
                        symbol_original_qty[symbol] = pos_qty
                    if side == "Buy":
                        sl_price = fill_price - (ACMS_PARAMS["stop_loss_atr_mult"] * atr)
                        tp_price = fill_price + (ACMS_PARAMS["take_profit_atr_mult"] * atr)
                    else:
                        sl_price = fill_price + (ACMS_PARAMS["stop_loss_atr_mult"] * atr)
                        tp_price = fill_price - (ACMS_PARAMS["take_profit_atr_mult"] * atr)
                    await set_server_stop_loss(symbol, side, sl_price)
                    await update.message.reply_text(
                        f"\u2705 Instant {side_text} placed for {symbol}\n"
                        f"Order ID: <code>{order_id}</code>\n"
                        f"Fill: {fill_price:.6g}\n"
                        f"SL: {sl_price:.6g} ({ACMS_PARAMS['stop_loss_atr_mult']}x ATR)\n"
                        f"TP: {tp_price:.6g} ({ACMS_PARAMS['take_profit_atr_mult']}x ATR)",
                        parse_mode="HTML"
                    )
                    symbol_trends[symbol] = "upward" if side == "Buy" else "downward"
                elif result and "error" in result:
                    await update.message.reply_text(f"\u274c Failed to place instant {side_text} for {symbol}: {result['error']}")
        else:
            await update.message.reply_text(f"\u26a0\ufe0f Could not fetch indicators for {symbol}. Monitoring will take over.")

    qty_info = f"Manual Quantity: {manual_qty} (Instant Trade Attempted)" if manual_qty else "Auto-sizing: 20% of available USDT balance"

    await update.message.reply_text(
        f"Monitoring {symbol}! (ACMS v2.0)\n"
        f"\u2022 {qty_info}\n"
        f"\u2022 Strategy: Adaptive Confluence Momentum System\n"
        f"\u2022 Indicators: VNM, VWDI, TPI, APO, MSM + MACD + ADX/DI regime\n"
        f"\u2022 Entry: Score >= {ACMS_PARAMS['long_entry_threshold']} + {ACMS_PARAMS['confirm_bars']}-bar confirmation\n"
        f"\u2022 SL: {ACMS_PARAMS['stop_loss_atr_mult']}x ATR | TP: {ACMS_PARAMS['take_profit_atr_mult']}x ATR\n"
        f"\u2022 Trail: {ACMS_PARAMS['trailing_stop_atr_mult']}x ATR (accel {ACMS_PARAMS['trail_accel']}\u2192{ACMS_PARAMS['trail_accel_max']})\n"
        f"\u2022 Reversal: opposite score >= threshold+10\n"
        f"\u2022 Max hold: {ACMS_PARAMS['max_hold_bars']} bars | Cooldown: {ACMS_PARAMS['cooldown_bars']} bars\n"
        f"\u2022 15m candles + polling every {POLL_INTERVAL_SECONDS}s\n"
        f"\u2022 State persistence: {STATE_FILE}\n"
        f"\u2022 Status report every 30 minutes"
    )

    if context.job_queue:
        existing_jobs = context.job_queue.get_jobs_by_name(f"{symbol}_monitor")
        if not existing_jobs:
            symbol_index = monitored_symbols.index(symbol) if symbol in monitored_symbols else 0
            stagger_delay = POLL_INTERVAL_SECONDS + (symbol_index * 1.25)
            context.job_queue.run_repeating(
                callback=monitor_symbol,
                interval=POLL_INTERVAL_SECONDS,
                first=stagger_delay,
                chat_id=chat_id,
                name=f"{symbol}_monitor",
                data={"symbol": symbol, "last_report_time": time.time()},
            )

async def remove_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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
    # Cancel any server-side stops before removing
    await cancel_server_stops(symbol)
    # Clean up all state dicts
    for state_dict in [symbol_entry_price, symbol_entry_atr, symbol_peak_price, symbol_trailing_active,
                        symbol_trail_price, symbol_trail_start_time, symbol_server_sl_set,
                        symbol_original_qty, symbol_long_signal_count, symbol_short_signal_count,
                        symbol_last_counted_bar, symbol_entry_time, symbol_last_exit_time,
                        symbol_last_was_loss, symbol_monitor_running, symbol_open_trade_side]:
        if symbol in state_dict:
            del state_dict[symbol]

    if context.job_queue:
        jobs = context.job_queue.get_jobs_by_name(f"{symbol}_monitor")
        for job in jobs:
            job.schedule_removal()

    save_state()
    await update.message.reply_text(f"Stopped monitoring {symbol}.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_chat.id != CHAT_ID:
        await update.message.reply_text("\u26d4 Unauthorized. This bot is private.")
        return
    await update.message.reply_text(
        "Bot ready! (ACMS v2.0 - Adaptive Confluence Momentum System)\n\n"
        "Commands:\n"
        "1. /start - Display this message\n"
        "2. /add_symbol <symbol> - Start monitoring with auto-sizing (20% balance)\n"
        "3. /add_symbol <symbol> <quantity> - INSTANT TRADE + Manual quantity monitoring\n"
        "4. /remove_symbol <symbol> - Stop monitoring a symbol\n\n"
        "Strategy: ACMS v2.0\n"
        "\u2022 5 Custom Indicators: VNM, VWDI, TPI, APO, MSM\n"
        "\u2022 + MACD + ADX/DI regime detection\n"
        "\u2022 Multi-indicator confluence scoring (0-100)\n\n"
        "Entry Rules:\n"
        f"\u2022 Confluence score >= {ACMS_PARAMS['long_entry_threshold']} (long or short)\n"
        f"\u2022 Signal must persist for {ACMS_PARAMS['confirm_bars']} consecutive bars\n"
        f"\u2022 RSI filter: 30-75 for longs, 25-70 for shorts\n"
        f"\u2022 ADX regime boost when ADX > {ACMS_PARAMS['adx_trend_threshold']}\n\n"
        "Stop-Loss:\n"
        f"\u2022 {ACMS_PARAMS['stop_loss_atr_mult']}x ATR (server-side on Bybit)\n\n"
        "Take-Profit:\n"
        f"\u2022 {ACMS_PARAMS['take_profit_atr_mult']}x ATR target\n\n"
        "Trailing Stop:\n"
        f"\u2022 Activates at {ACMS_PARAMS['trailing_activation_atr_mult']}x ATR profit\n"
        f"\u2022 {ACMS_PARAMS['trailing_stop_atr_mult']}x ATR distance with parabolic acceleration\n"
        f"\u2022 Accel: {ACMS_PARAMS['trail_accel']} per bar, max {ACMS_PARAMS['trail_accel_max']}\n\n"
        "Reversal:\n"
        "\u2022 Opposite confluence score >= threshold + 10 triggers close\n\n"
        "Trade Management:\n"
        f"\u2022 Max hold: {ACMS_PARAMS['max_hold_bars']} bars (15min each)\n"
        f"\u2022 Cooldown: {ACMS_PARAMS['cooldown_bars']} bars (doubled after loss)\n\n"
        "Infrastructure:\n"
        "\u2022 WebSocket real-time price feeds (REST fallback)\n"
        f"\u2022 15-minute candles + polling every {POLL_INTERVAL_SECONDS}s\n"
        f"\u2022 State persistence: {STATE_FILE}\n"
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


# ════════════════════════════════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════════════════════════════════

async def main():
    print("=== Render startup: Initializing (ACMS v2.0) ===")
    print(f"TELEGRAM_TOKEN loaded (length: {len(TELEGRAM_TOKEN)} chars)")
    print(f"CHAT_ID loaded: {CHAT_ID}")

    loop = asyncio.get_running_loop()
    loop.set_default_executor(_executor)
    print(f"[STARTUP] Thread pool set to {_executor._max_workers} workers")

    if load_state():
        print(f"[STARTUP] Restored {len(monitored_symbols)} monitored symbols from state file")

    if start_websocket():
        for sym in monitored_symbols:
            subscribe_ws_symbol(sym)
        print(f"[STARTUP] WebSocket subscribed to {len(monitored_symbols)} symbols")

    application = Application.builder().token(TELEGRAM_TOKEN).updater(None).concurrent_updates(True).build()

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

        for i, sym in enumerate(monitored_symbols):
            existing = application.job_queue.get_jobs_by_name(f"{sym}_monitor")
            if not existing:
                stagger_delay = POLL_INTERVAL_SECONDS + (i * 1.25)
                application.job_queue.run_repeating(
                    callback=monitor_symbol,
                    interval=POLL_INTERVAL_SECONDS,
                    first=stagger_delay,
                    chat_id=CHAT_ID,
                    name=f"{sym}_monitor",
                    data={"symbol": sym, "last_report_time": time.time()},
                )
                print(f"[STARTUP] Recreated monitor job for {sym} (first run in {stagger_delay:.1f}s)")
        if monitored_symbols:
            print(f"[STARTUP] {len(monitored_symbols)} monitor jobs recreated with staggered starts")

    print("Telegram application built.")

    port = int(os.environ.get("PORT", 10000))
    base_url = os.environ.get("RENDER_EXTERNAL_HOSTNAME", "psychology-trading-bot.onrender.com")
    webhook_url = f"https://{base_url}/webhook"

    await application.initialize()
    await application.start()

    await application.bot.set_webhook(url=webhook_url, drop_pending_updates=True)
    print(f"Webhook set to: {webhook_url}")

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
