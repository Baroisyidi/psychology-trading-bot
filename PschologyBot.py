"""
PsychologyBot v1
─────────────────────────────────────────────────────────────────────────────
Multi-symbol Bybit Demo trading bot controlled via Telegram.
Strategy: Multi-timeframe confluence (Daily + 4H + 1H)
Leverage : 10x on all trades
Position : 20% of account balance per symbol
─────────────────────────────────────────────────────────────────────────────
"""

import os
import json
import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import pandas_ta as ta
import numpy as np
from dotenv import load_dotenv
from pybit.unified_trading import HTTP
from aiohttp import web
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv()

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
OWNER_ID         = int(os.getenv("CHAT_ID", "0"))
BYBIT_KEY        = os.getenv("BYBIT_API_KEY")
BYBIT_SECRET     = os.getenv("BYBIT_API_SECRET")
BYBIT_TESTNET    = os.getenv("BYBIT_TESTNET", "True").lower() == "true"

STATE_FILE       = os.getenv("STATE_FILE", "/opt/render/project/src/bot_state.json")
LEVERAGE         = 10
POSITION_PCT     = 0.20          # 20 % of balance per symbol
MONITOR_INTERVAL = 10            # seconds between price/position checks
CANDLE_1H_SEC    = 3600
CANDLE_4H_SEC    = 14400
CANDLE_1D_SEC    = 86400

# Indicator parameters
ADX_PERIOD       = 14
RSI_PERIOD       = 14
EMA_PERIODS      = [20, 50, 100, 200]
MACD_FAST        = 12
MACD_SLOW        = 26
MACD_SIG         = 9
ST_PERIOD        = 10            # Supertrend ATR period
ST_MULTIPLIER    = 3.0           # Supertrend multiplier
ICHI_CONV        = 9
ICHI_BASE        = 26
ICHI_SPAN        = 52

# Risk / SL
SL_BUFFER_PCT    = 0.02          # 2 % breathing room on initial SL
TRAIL_BUFFER_PCT = 0.003         # 0.3 % buffer on trailing SL
DEAD_TRADE_DAYS  = 7             # close if TP1 not hit after 7 days

# ─────────────────────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  BYBIT CLIENT
#  Bybit Demo Trading uses endpoint: https://api-demo.bybit.com
#  pybit 5.6.2 builds URL from self.endpoint — we override it after init.
# ─────────────────────────────────────────────────────────────────────────────
bybit = HTTP(
    testnet=False,
    api_key=BYBIT_KEY,
    api_secret=BYBIT_SECRET,
)
# Override endpoint to point at Bybit Demo Trading (not live, not testnet)
bybit.endpoint = "https://api-demo.bybit.com"

# ─────────────────────────────────────────────────────────────────────────────
#  STATE MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────
# Schema for each open position stored in state:
# {
#   "symbol":        str,
#   "direction":     "LONG" | "SHORT",
#   "entry_price":   float,
#   "sl_price":      float,
#   "tp1_price":     float,
#   "tp2_price":     float,
#   "original_qty":  float,
#   "remaining_qty": float,
#   "tp1_hit":       bool,
#   "tp2_hit":       bool,
#   "entry_time":    float (unix timestamp),
#   "score":         int,
#   "filters_passed": list[str],
# }

def _default_state() -> dict:
    return {
        "symbols":    [],          # monitored symbols
        "positions":  {},          # symbol -> position dict
        "paused":     False,       # if True no new entries
        "daily_loss": 0.0,         # realised loss today (negative)
        "weekly_loss":0.0,
        "consec_losses": 0,
        "last_reset_day":  "",     # date string for daily reset
        "last_reset_week": "",     # ISO week string
    }


def load_state() -> dict:
    try:
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
        # Back-fill missing keys from schema upgrades
        default = _default_state()
        for k, v in default.items():
            state.setdefault(k, v)
        return state
    except (FileNotFoundError, json.JSONDecodeError):
        return _default_state()


def save_state(state: dict) -> None:
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ─────────────────────────────────────────────────────────────────────────────
#  OWNER-ONLY GUARD
# ─────────────────────────────────────────────────────────────────────────────
def owner_only(func):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != OWNER_ID:
            await update.message.reply_text("⛔ Unauthorised.")
            return
        return await func(update, ctx)
    wrapper.__name__ = func.__name__
    return wrapper


# ─────────────────────────────────────────────────────────────────────────────
#  BYBIT HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def get_klines(symbol: str, interval: str, limit: int = 300) -> Optional[pd.DataFrame]:
    """
    Fetch OHLCV candles from Bybit.
    interval: "D" | "240" | "60"
    Returns DataFrame with columns: open, high, low, close, volume (float)
    sorted oldest → newest.
    """
    try:
        resp = bybit.get_kline(
            category="linear",
            symbol=symbol,
            interval=interval,
            limit=limit,
        )
        raw = resp["result"]["list"]
        if not raw:
            return None
        df = pd.DataFrame(raw, columns=[
            "timestamp", "open", "high", "low", "close", "volume", "turnover"
        ])
        df = df.astype({
            "timestamp": "int64",
            "open": "float64",
            "high": "float64",
            "low":  "float64",
            "close":"float64",
            "volume":"float64",
        })
        df.sort_values("timestamp", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df
    except Exception as e:
        logger.error(f"get_klines({symbol},{interval}): {e}")
        return None


def get_balance_usdt() -> float:
    """Return available USDT balance on Bybit Demo (unified account)."""
    try:
        resp = bybit.get_wallet_balance(accountType="UNIFIED", coin="USDT")
        coins = resp["result"]["list"][0]["coin"]
        logger.info(f"[BALANCE] Raw coins response: {coins}")
        for c in coins:
            if c["coin"] == "USDT":
                # Bybit Demo sometimes returns "" instead of "0" for unused fields
                raw = c.get("availableToWithdraw") or c.get("walletBalance") or "0"
                val = float(raw) if str(raw).strip() != "" else 0.0
                logger.info(f"[BALANCE] USDT balance: {val}")
                return val
        # Coin list empty — Demo account not yet funded or wrong accountType
        # Try CONTRACT as fallback (some Demo accounts use this)
        resp2 = bybit.get_wallet_balance(accountType="CONTRACT", coin="USDT")
        coins2 = resp2["result"]["list"][0]["coin"]
        for c in coins2:
            if c["coin"] == "USDT":
                raw = c.get("availableToWithdraw") or c.get("walletBalance") or "0"
                return float(raw) if str(raw).strip() != "" else 0.0
        return 0.0
    except Exception as e:
        logger.error(f"get_balance_usdt: {e}")
        return 0.0


def get_current_price(symbol: str) -> float:
    try:
        resp = bybit.get_tickers(category="linear", symbol=symbol)
        return float(resp["result"]["list"][0]["lastPrice"])
    except Exception as e:
        logger.error(f"get_current_price({symbol}): {e}")
        return 0.0


def set_leverage(symbol: str) -> None:
    try:
        bybit.set_leverage(
            category="linear",
            symbol=symbol,
            buyLeverage=str(LEVERAGE),
            sellLeverage=str(LEVERAGE),
        )
    except Exception as e:
        logger.warning(f"set_leverage({symbol}): {e}")


def place_market_order(symbol: str, side: str, qty: float) -> Optional[dict]:
    """side: 'Buy' | 'Sell'"""
    try:
        resp = bybit.place_order(
            category="linear",
            symbol=symbol,
            side=side,
            orderType="Market",
            qty=str(qty),
            timeInForce="GoodTillCancel",
            reduceOnly=False,
        )
        return resp["result"]
    except Exception as e:
        logger.error(f"place_market_order({symbol},{side},{qty}): {e}")
        return None


def place_reduce_order(symbol: str, side: str, qty: float) -> Optional[dict]:
    """Partial close (reduce-only)."""
    try:
        resp = bybit.place_order(
            category="linear",
            symbol=symbol,
            side=side,
            orderType="Market",
            qty=str(qty),
            timeInForce="GoodTillCancel",
            reduceOnly=True,
        )
        return resp["result"]
    except Exception as e:
        logger.error(f"place_reduce_order({symbol},{side},{qty}): {e}")
        return None


def set_trading_stop(symbol: str, stop_loss: float, position_idx: int = 0) -> None:
    try:
        bybit.set_trading_stop(
            category="linear",
            symbol=symbol,
            stopLoss=str(round(stop_loss, 6)),
            positionIdx=position_idx,
        )
    except Exception as e:
        logger.error(f"set_trading_stop({symbol},{stop_loss}): {e}")


def close_full_position(symbol: str, direction: str) -> None:
    """Market-close the entire position."""
    close_side = "Sell" if direction == "LONG" else "Buy"
    try:
        resp = bybit.get_positions(category="linear", symbol=symbol)
        positions = resp["result"]["list"]
        for pos in positions:
            size = float(pos.get("size", 0))
            if size > 0:
                place_reduce_order(symbol, close_side, size)
                return
    except Exception as e:
        logger.error(f"close_full_position({symbol}): {e}")


def get_min_qty(symbol: str) -> float:
    """Fetch minimum order quantity for a symbol."""
    try:
        resp = bybit.get_instruments_info(category="linear", symbol=symbol)
        lot = resp["result"]["list"][0]["lotSizeFilter"]
        return float(lot["minOrderQty"])
    except Exception:
        return 0.001


def round_qty(qty: float, symbol: str) -> float:
    min_q = get_min_qty(symbol)
    # Round down to the step size
    step = min_q
    return max(min_q, round(qty // step * step, 8))


# ─────────────────────────────────────────────────────────────────────────────
#  INDICATOR CALCULATIONS
# ─────────────────────────────────────────────────────────────────────────────
def calc_ema(df: pd.DataFrame, period: int) -> pd.Series:
    return df["close"].ewm(span=period, adjust=False).mean()


def calc_adx(df: pd.DataFrame, period: int = ADX_PERIOD) -> float:
    adx = ta.adx(df["high"], df["low"], df["close"], length=period)
    col = f"ADX_{period}"
    if adx is None or col not in adx.columns:
        return 0.0
    return float(adx[col].iloc[-1])


def calc_rsi(df: pd.DataFrame, period: int = RSI_PERIOD) -> float:
    rsi = ta.rsi(df["close"], length=period)
    if rsi is None:
        return 50.0
    return float(rsi.iloc[-1])


def calc_supertrend(df: pd.DataFrame) -> str:
    """Returns 'BUY' (green/up) or 'SELL' (red/down)."""
    st = ta.supertrend(
        df["high"], df["low"], df["close"],
        length=ST_PERIOD, multiplier=ST_MULTIPLIER
    )
    if st is None:
        return "NONE"
    dir_col = f"SUPERTd_{ST_PERIOD}_{ST_MULTIPLIER}"
    if dir_col not in st.columns:
        return "NONE"
    val = int(st[dir_col].iloc[-1])
    return "BUY" if val == 1 else "SELL"


def calc_macd(df: pd.DataFrame):
    """Returns (macd_line, signal_line, histogram) as floats for latest bar."""
    macd = ta.macd(df["close"], fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIG)
    if macd is None:
        return 0.0, 0.0, 0.0
    m = float(macd[f"MACD_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"].iloc[-1])
    s = float(macd[f"MACDs_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"].iloc[-1])
    h = float(macd[f"MACDh_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"].iloc[-1])
    return m, s, h


def calc_macd_crossed(df: pd.DataFrame, direction: str) -> bool:
    """
    True if MACD crossed bullish (for LONG) or bearish (for SHORT)
    on the last completed candle, confirmed by histogram direction.
    We look at the last 2 bars.
    """
    macd = ta.macd(df["close"], fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIG)
    if macd is None or len(macd) < 2:
        return False
    mcol = f"MACD_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"
    scol = f"MACDs_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"
    hcol = f"MACDh_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"
    m_prev = float(macd[mcol].iloc[-2])
    s_prev = float(macd[scol].iloc[-2])
    m_cur  = float(macd[mcol].iloc[-1])
    s_cur  = float(macd[scol].iloc[-1])
    h_cur  = float(macd[hcol].iloc[-1])
    if direction == "LONG":
        crossed = (m_prev < s_prev) and (m_cur >= s_cur)
        return crossed and h_cur > 0
    else:
        crossed = (m_prev > s_prev) and (m_cur <= s_cur)
        return crossed and h_cur < 0


def calc_ichimoku_cloud(df: pd.DataFrame):
    """
    Returns (senkou_a, senkou_b) for the CURRENT bar
    by shifting span_b forward 26 periods (standard Ichimoku).
    We use the close-side cloud values.
    """
    conv  = (df["high"].rolling(ICHI_CONV).max() + df["low"].rolling(ICHI_CONV).min()) / 2
    base  = (df["high"].rolling(ICHI_BASE).max() + df["low"].rolling(ICHI_BASE).min()) / 2
    span_a = ((conv + base) / 2).shift(ICHI_BASE)
    span_b = ((df["high"].rolling(ICHI_SPAN).max() + df["low"].rolling(ICHI_SPAN).min()) / 2).shift(ICHI_BASE)
    sa = float(span_a.iloc[-1])
    sb = float(span_b.iloc[-1])
    return sa, sb


def calc_emas(df: pd.DataFrame):
    """Returns dict {period: latest_ema_value}."""
    return {p: float(calc_ema(df, p).iloc[-1]) for p in EMA_PERIODS}


def is_bullish_1h_candle(df_1h: pd.DataFrame) -> bool:
    """
    Bullish 1H candle:
    - close > open (green)
    - body size >= 0.3% of price
    - close in upper 50% of candle range
    """
    c = df_1h.iloc[-1]
    body  = c["close"] - c["open"]
    rng   = c["high"] - c["low"]
    if body <= 0:
        return False
    if rng == 0:
        return False
    body_pct = body / c["open"]
    upper_half = c["close"] >= (c["high"] + c["low"]) / 2
    return body_pct >= 0.003 and upper_half


def is_bearish_1h_candle(df_1h: pd.DataFrame) -> bool:
    """
    Bearish 1H candle:
    - close < open (red)
    - body size >= 0.3% of price
    - close in lower 50% of candle range
    """
    c = df_1h.iloc[-1]
    body  = c["open"] - c["close"]
    rng   = c["high"] - c["low"]
    if body <= 0:
        return False
    if rng == 0:
        return False
    body_pct = body / c["open"]
    lower_half = c["close"] <= (c["high"] + c["low"]) / 2
    return body_pct >= 0.003 and lower_half


def price_near_ema(price: float, ema_val: float, threshold: float = 0.01) -> bool:
    """True if price is within threshold% of the EMA."""
    return abs(price - ema_val) / ema_val <= threshold


# ─────────────────────────────────────────────────────────────────────────────
#  SIGNAL SCORING ENGINE
# ─────────────────────────────────────────────────────────────────────────────
def evaluate_filters(symbol: str, direction: str) -> tuple[int, list[str], list[str], dict]:
    """
    Evaluates all 8 entry filters for the given direction.
    Returns:
        score          : total filters passed (int)
        passed         : list of filter names that passed
        failed         : list of filter names that failed
        debug          : dict with raw indicator values
    """
    passed = []
    failed = []
    debug  = {}

    # ── Fetch candles ───────────────────────────────────────────────────────
    df_1d  = get_klines(symbol, "D",   300)
    df_4h  = get_klines(symbol, "240", 300)
    df_1h  = get_klines(symbol, "60",  100)

    if df_1d is None or df_4h is None or df_1h is None:
        logger.warning(f"[{symbol}] Failed to fetch candles")
        return 0, [], ["DATA_FETCH_FAILED"], {}

    price = float(df_1d["close"].iloc[-1])
    debug["price"] = price

    # ── Filter 1: ADX > 20 (use 4H for responsiveness) ─────────────────────
    adx = calc_adx(df_4h)
    debug["adx"] = round(adx, 2)
    if adx > 20:
        passed.append("ADX>20")
    else:
        failed.append("ADX>20")

    # ── Filter 2: Ichimoku Cloud (Daily) ───────────────────────────────────
    sa, sb = calc_ichimoku_cloud(df_1d)
    cloud_top = max(sa, sb)
    cloud_bot = min(sa, sb)
    debug["cloud_top"] = round(cloud_top, 2)
    debug["cloud_bot"] = round(cloud_bot, 2)
    if direction == "LONG":
        ichi_ok = price > cloud_top
        label = "IchiAboveCloud"
    else:
        ichi_ok = price < cloud_bot
        label = "IchiBelowCloud"
    if ichi_ok:
        passed.append(label)
    else:
        failed.append(label)

    # ── Filter 3: RSI not extreme (4H) ─────────────────────────────────────
    rsi = calc_rsi(df_4h)
    debug["rsi_4h"] = round(rsi, 2)
    if direction == "LONG":
        rsi_ok = rsi <= 70
        label = "RSI<=70"
    else:
        rsi_ok = rsi >= 30
        label = "RSI>=30"
    if rsi_ok:
        passed.append(label)
    else:
        failed.append(label)

    # ── Filter 4: Supertrend (4H) ──────────────────────────────────────────
    st = calc_supertrend(df_4h)
    debug["supertrend_4h"] = st
    if direction == "LONG":
        st_ok = st == "BUY"
        label = "SupertrendBUY"
    else:
        st_ok = st == "SELL"
        label = "SupertrendSELL"
    if st_ok:
        passed.append(label)
    else:
        failed.append(label)

    # ── Filter 5 (MANDATORY): EMA alignment (Daily) ────────────────────────
    emas = calc_emas(df_1d)
    debug["emas_daily"] = {k: round(v, 2) for k, v in emas.items()}
    e20, e50, e100, e200 = emas[20], emas[50], emas[100], emas[200]
    if direction == "LONG":
        ema_ok = (e20 > e50 > e100 > e200) and (price > e50)
        label = "EMA_Bullish_Stack"
    else:
        ema_ok = (e20 < e50 < e100 < e200) and (price < e50)
        label = "EMA_Bearish_Stack"
    if ema_ok:
        passed.append(label)
    else:
        failed.append(label)

    # ── Filter 6: MACD cross (Daily) ───────────────────────────────────────
    macd_crossed = calc_macd_crossed(df_1d, direction)
    # Also accept: MACD already in correct position (not just the cross moment)
    m, s, h = calc_macd(df_1d)
    debug["macd_line"] = round(m, 4)
    debug["macd_signal"] = round(s, 4)
    debug["macd_hist"] = round(h, 4)
    if direction == "LONG":
        macd_ok = (m > s) and (h > 0)
        label = "MACD_Bullish"
    else:
        macd_ok = (m < s) and (h < 0)
        label = "MACD_Bearish"
    if macd_ok:
        passed.append(label)
    else:
        failed.append(label)

    # ── Filter 7: Price near 4H EMA 50 or EMA 100 (pullback/rally) ─────────
    emas_4h = calc_emas(df_4h)
    e50_4h  = emas_4h[50]
    e100_4h = emas_4h[100]
    debug["ema50_4h"]  = round(e50_4h, 2)
    debug["ema100_4h"] = round(e100_4h, 2)
    near_50  = price_near_ema(price, e50_4h,  threshold=0.015)
    near_100 = price_near_ema(price, e100_4h, threshold=0.015)
    pullback_ok = near_50 or near_100
    if direction == "LONG":
        label = "PullbackToEMA"
    else:
        label = "RallyToEMA"
    if pullback_ok:
        passed.append(label)
    else:
        failed.append(label)

    # ── Filter 8 (MANDATORY): 1H candle trigger ────────────────────────────
    if direction == "LONG":
        candle_ok = is_bullish_1h_candle(df_1h)
        label = "Bullish1HCandle"
    else:
        candle_ok = is_bearish_1h_candle(df_1h)
        label = "Bearish1HCandle"
    if candle_ok:
        passed.append(label)
    else:
        failed.append(label)

    # ── Total score ─────────────────────────────────────────────────────────
    score = len(passed)
    return score, passed, failed, debug


def check_entry_signal(symbol: str, direction: str) -> tuple[bool, int, list[str], list[str], dict]:
    """
    Returns (should_enter, score, passed, failed, debug).

    Rules:
    - Filter 5 (EMA_*_Stack) MUST be in passed
    - Filter 8 (1H candle)   MUST be in passed
    - At least 3 more from the remaining 6 must pass
    - Total minimum = 5 (2 mandatory + 3 optional)
    """
    score, passed, failed, debug = evaluate_filters(symbol, direction)

    mandatory_5_name = "EMA_Bullish_Stack" if direction == "LONG" else "EMA_Bearish_Stack"
    mandatory_8_name = "Bullish1HCandle"   if direction == "LONG" else "Bearish1HCandle"

    mandatory_5_ok = mandatory_5_name in passed
    mandatory_8_ok = mandatory_8_name in passed

    optional_names = {
        "ADX>20", "IchiAboveCloud", "IchiBelowCloud",
        "RSI<=70", "RSI>=30", "SupertrendBUY", "SupertrendSELL",
        "MACD_Bullish", "MACD_Bearish", "PullbackToEMA", "RallyToEMA",
    }
    optional_passed = [p for p in passed if p in optional_names]

    should_enter = (
        mandatory_5_ok and
        mandatory_8_ok and
        len(optional_passed) >= 3
    )
    return should_enter, score, passed, failed, debug


# ─────────────────────────────────────────────────────────────────────────────
#  STOP LOSS CALCULATOR
# ─────────────────────────────────────────────────────────────────────────────
def calc_initial_sl(entry: float, direction: str, symbol: str) -> float:
    """
    Tier 1 SL: Below EMA 100 on 4H (long) / Above EMA 100 on 4H (short).
    With 1.5–2% breathing room.
    """
    df_4h = get_klines(symbol, "240", 200)
    if df_4h is None:
        if direction == "LONG":
            return entry * (1 - SL_BUFFER_PCT)
        else:
            return entry * (1 + SL_BUFFER_PCT)

    emas_4h = calc_emas(df_4h)
    e100_4h = emas_4h[100]

    if direction == "LONG":
        sl = min(e100_4h, entry * (1 - 0.015)) * (1 - SL_BUFFER_PCT)
    else:
        sl = max(e100_4h, entry * (1 + 0.015)) * (1 + SL_BUFFER_PCT)
    return round(sl, 6)


def calc_tp_levels(entry: float, sl: float, direction: str) -> tuple[float, float]:
    """TP1 = 1:1.5 RR, TP2 = 1:3 RR."""
    risk = abs(entry - sl)
    if direction == "LONG":
        tp1 = entry + risk * 1.5
        tp2 = entry + risk * 3.0
    else:
        tp1 = entry - risk * 1.5
        tp2 = entry - risk * 3.0
    return round(tp1, 6), round(tp2, 6)


# ─────────────────────────────────────────────────────────────────────────────
#  POSITION SIZING
# ─────────────────────────────────────────────────────────────────────────────
def calc_position_qty(symbol: str, entry_price: float) -> float:
    """
    Use 20% of account balance with 10x leverage.
    qty = (balance * 0.20 * leverage) / entry_price
    """
    balance = get_balance_usdt()
    notional = balance * POSITION_PCT * LEVERAGE
    qty = notional / entry_price
    return round_qty(qty, symbol)


# ─────────────────────────────────────────────────────────────────────────────
#  EXIT RULE CHECK (EMA inversion on Daily)
# ─────────────────────────────────────────────────────────────────────────────
def check_ema_exit(symbol: str, direction: str) -> bool:
    """
    Exit LONG  when any EMA inverts downward  (e20 < e50 OR e50 < e100 OR e100 < e200) on Daily.
    Exit SHORT when any EMA inverts upward    (e20 > e50 OR e50 > e100 OR e100 > e200) on Daily.
    We require the inversion to be present on the LAST COMPLETED Daily candle
    (not intraday) to reduce fakeouts.
    """
    df_1d = get_klines(symbol, "D", 50)
    if df_1d is None or len(df_1d) < 2:
        return False

    # Use closed candles only (all except the last forming one)
    df_closed = df_1d.iloc[:-1]

    e20  = float(calc_ema(df_closed, 20).iloc[-1])
    e50  = float(calc_ema(df_closed, 50).iloc[-1])
    e100 = float(calc_ema(df_closed, 100).iloc[-1])
    e200 = float(calc_ema(df_closed, 200).iloc[-1])

    if direction == "LONG":
        # Any bearish inversion
        return (e20 < e50) or (e50 < e100) or (e100 < e200)
    else:
        # Any bullish inversion
        return (e20 > e50) or (e50 > e100) or (e100 > e200)


# ─────────────────────────────────────────────────────────────────────────────
#  TRAILING STOP (Tier 3) — called at every 4H candle close
# ─────────────────────────────────────────────────────────────────────────────
def update_trailing_sl(pos: dict) -> Optional[float]:
    """
    For LONG:  if latest 4H candle low > current SL → new SL = candle low
    For SHORT: if latest 4H candle high < current SL → new SL = candle high
    Returns new SL if updated, else None.
    """
    symbol    = pos["symbol"]
    direction = pos["direction"]
    cur_sl    = pos["sl_price"]

    df_4h = get_klines(symbol, "240", 10)
    if df_4h is None or len(df_4h) < 2:
        return None

    # Use the last COMPLETED 4H candle (second-to-last row)
    last_closed = df_4h.iloc[-2]

    if direction == "LONG":
        new_sl_candidate = float(last_closed["low"])
        if new_sl_candidate > cur_sl:
            return round(new_sl_candidate, 6)
    else:
        new_sl_candidate = float(last_closed["high"])
        if new_sl_candidate < cur_sl:
            return round(new_sl_candidate, 6)
    return None


# ─────────────────────────────────────────────────────────────────────────────
#  TRADE ENTRY
# ─────────────────────────────────────────────────────────────────────────────
async def open_trade(
    symbol: str,
    direction: str,
    score: int,
    passed: list[str],
    state: dict,
    app,
) -> None:
    """Full trade entry sequence."""
    set_leverage(symbol)

    entry_price = get_current_price(symbol)
    if entry_price == 0:
        await send_alert(app, f"⚠️ [{symbol}] Could not fetch price. Trade aborted.")
        return

    qty = calc_position_qty(symbol, entry_price)
    if qty <= 0:
        await send_alert(app, f"⚠️ [{symbol}] Position size too small. Check balance.")
        return

    side = "Buy" if direction == "LONG" else "Sell"
    result = place_market_order(symbol, side, qty)
    if not result:
        await send_alert(app, f"⚠️ [{symbol}] Order placement failed.")
        return

    sl  = calc_initial_sl(entry_price, direction, symbol)
    tp1, tp2 = calc_tp_levels(entry_price, sl, direction)

    set_trading_stop(symbol, sl)

    pos = {
        "symbol":         symbol,
        "direction":      direction,
        "entry_price":    entry_price,
        "sl_price":       sl,
        "tp1_price":      tp1,
        "tp2_price":      tp2,
        "original_qty":   qty,
        "remaining_qty":  qty,
        "tp1_hit":        False,
        "tp2_hit":        False,
        "entry_time":     time.time(),
        "score":          score,
        "filters_passed": passed,
    }
    state["positions"][symbol] = pos
    save_state(state)

    emoji = "🟢" if direction == "LONG" else "🔴"
    filters_str = " ".join([f"✅{p}" for p in passed])
    msg = (
        f"{emoji} <b>TRADE OPENED — {symbol} {direction}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 Entry  : <code>{entry_price:.4f}</code>\n"
        f"🛡 SL     : <code>{sl:.4f}</code>\n"
        f"🎯 TP1    : <code>{tp1:.4f}</code>  (close 40%)\n"
        f"🎯 TP2    : <code>{tp2:.4f}</code>  (close 40%)\n"
        f"🌊 TP3    : Trailing (last 20%)\n"
        f"📊 Score  : {score}/8\n"
        f"✅ Filters: {filters_str}\n"
        f"⚡ Qty    : {qty}  |  Leverage: {LEVERAGE}x"
    )
    await send_alert(app, msg)
    logger.info(f"[{symbol}] Trade opened {direction} @ {entry_price}")


# ─────────────────────────────────────────────────────────────────────────────
#  TRADE MANAGEMENT — TP / SL / EXIT
# ─────────────────────────────────────────────────────────────────────────────
async def manage_position(pos: dict, state: dict, app) -> None:
    symbol    = pos["symbol"]
    direction = pos["direction"]
    price     = get_current_price(symbol)
    if price == 0:
        return

    # ── Dead trade check ────────────────────────────────────────────────────
    days_open = (time.time() - pos["entry_time"]) / 86400
    if not pos["tp1_hit"] and days_open >= DEAD_TRADE_DAYS:
        close_full_position(symbol, direction)
        pnl = _estimate_pnl(pos, price)
        await send_alert(
            app,
            f"⏱ <b>DEAD TRADE CLOSED — {symbol}</b>\n"
            f"Open {days_open:.1f} days without hitting TP1.\n"
            f"Est. PnL: {pnl:+.2f} USDT"
        )
        _close_position_state(symbol, state, pnl)
        return

    # ── EMA inversion exit ──────────────────────────────────────────────────
    if check_ema_exit(symbol, direction):
        close_full_position(symbol, direction)
        pnl = _estimate_pnl(pos, price)
        await send_alert(
            app,
            f"📉 <b>EMA EXIT — {symbol} {direction}</b>\n"
            f"EMA structure inverted on Daily.\n"
            f"Closed at <code>{price:.4f}</code>\n"
            f"Est. PnL: {pnl:+.2f} USDT"
        )
        _close_position_state(symbol, state, pnl)
        return

    # ── SL check ────────────────────────────────────────────────────────────
    sl = pos["sl_price"]
    sl_hit = (direction == "LONG" and price <= sl) or \
             (direction == "SHORT" and price >= sl)
    if sl_hit:
        close_full_position(symbol, direction)
        pnl = _estimate_pnl(pos, price)
        state["consec_losses"] += 1
        await send_alert(
            app,
            f"🛑 <b>STOP LOSS HIT — {symbol} {direction}</b>\n"
            f"SL: <code>{sl:.4f}</code>  |  Price: <code>{price:.4f}</code>\n"
            f"Est. PnL: {pnl:+.2f} USDT\n"
            f"Consecutive losses: {state['consec_losses']}"
        )
        _close_position_state(symbol, state, pnl)
        return

    # ── TP1 check ───────────────────────────────────────────────────────────
    if not pos["tp1_hit"]:
        tp1_hit = (direction == "LONG"  and price >= pos["tp1_price"]) or \
                  (direction == "SHORT" and price <= pos["tp1_price"])
        if tp1_hit:
            close_qty = round_qty(pos["original_qty"] * 0.40, symbol)
            place_reduce_order(symbol, "Sell" if direction == "LONG" else "Buy", close_qty)
            pos["remaining_qty"] = round(pos["original_qty"] * 0.60, 8)
            pos["tp1_hit"] = True
            # Move SL to breakeven
            pos["sl_price"] = pos["entry_price"]
            set_trading_stop(symbol, pos["entry_price"])
            state["consec_losses"] = 0   # reset on win
            save_state(state)
            pnl_partial = _estimate_pnl_partial(pos, price, close_qty)
            await send_alert(
                app,
                f"🎯 <b>TP1 HIT — {symbol} {direction}</b>\n"
                f"Closed 40% at <code>{price:.4f}</code>\n"
                f"Est. partial PnL: {pnl_partial:+.2f} USDT\n"
                f"🔒 SL moved to breakeven: <code>{pos['entry_price']:.4f}</code>\n"
                f"Remaining qty: {pos['remaining_qty']}"
            )
            return

    # ── TP2 check ───────────────────────────────────────────────────────────
    if pos["tp1_hit"] and not pos["tp2_hit"]:
        tp2_hit = (direction == "LONG"  and price >= pos["tp2_price"]) or \
                  (direction == "SHORT" and price <= pos["tp2_price"])
        if tp2_hit:
            close_qty = round_qty(pos["original_qty"] * 0.40, symbol)
            place_reduce_order(symbol, "Sell" if direction == "LONG" else "Buy", close_qty)
            pos["remaining_qty"] = round(pos["original_qty"] * 0.20, 8)
            pos["tp2_hit"] = True
            save_state(state)
            pnl_partial = _estimate_pnl_partial(pos, price, close_qty)
            await send_alert(
                app,
                f"🎯 <b>TP2 HIT — {symbol} {direction}</b>\n"
                f"Closed 40% at <code>{price:.4f}</code>\n"
                f"Est. partial PnL: {pnl_partial:+.2f} USDT\n"
                f"🌊 Trailing stop now active on last 20%\n"
                f"Remaining qty: {pos['remaining_qty']}"
            )
            return

    # ── Trailing SL update (Tier 3, after TP2) ─────────────────────────────
    if pos["tp2_hit"]:
        new_sl = update_trailing_sl(pos)
        if new_sl is not None:
            pos["sl_price"] = new_sl
            set_trading_stop(symbol, new_sl)
            save_state(state)
            logger.info(f"[{symbol}] Trailing SL updated to {new_sl}")


def _estimate_pnl(pos: dict, current_price: float) -> float:
    qty  = pos["remaining_qty"]
    diff = (current_price - pos["entry_price"]) if pos["direction"] == "LONG" \
           else (pos["entry_price"] - current_price)
    return diff * qty * LEVERAGE


def _estimate_pnl_partial(pos: dict, current_price: float, qty: float) -> float:
    diff = (current_price - pos["entry_price"]) if pos["direction"] == "LONG" \
           else (pos["entry_price"] - current_price)
    return diff * qty * LEVERAGE


def _close_position_state(symbol: str, state: dict, pnl: float) -> None:
    state["daily_loss"]  = state.get("daily_loss", 0) + min(pnl, 0)
    state["weekly_loss"] = state.get("weekly_loss", 0) + min(pnl, 0)
    if symbol in state["positions"]:
        del state["positions"][symbol]
    save_state(state)


# ─────────────────────────────────────────────────────────────────────────────
#  TELEGRAM HELPERS
# ─────────────────────────────────────────────────────────────────────────────
async def send_alert(app, text: str) -> None:
    try:
        await app.bot.send_message(
            chat_id=OWNER_ID,
            text=text,
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error(f"send_alert: {e}")


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN MONITORING LOOP
# ─────────────────────────────────────────────────────────────────────────────
async def monitor_loop(app) -> None:
    """Runs forever. Checks prices every MONITOR_INTERVAL seconds."""
    logger.info("Monitor loop started.")
    last_4h_check: dict[str, float] = {}
    last_1d_check: dict[str, float] = {}

    while True:
        try:
            state = load_state()
            now   = time.time()

            # ── Reset daily/weekly counters ─────────────────────────────────
            today    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            iso_week = datetime.now(timezone.utc).strftime("%Y-W%W")
            if state.get("last_reset_day") != today:
                state["daily_loss"]      = 0.0
                state["last_reset_day"]  = today
            if state.get("last_reset_week") != iso_week:
                state["weekly_loss"]     = 0.0
                state["last_reset_week"] = iso_week

            # ── Circuit breakers ────────────────────────────────────────────
            paused_by_circuit = False
            if state["daily_loss"] <= -0.04 * get_balance_usdt():
                paused_by_circuit = True
            if state["weekly_loss"] <= -0.06 * get_balance_usdt():
                paused_by_circuit = True
            if state["consec_losses"] >= 3:
                paused_by_circuit = True

            # ── Manage open positions ───────────────────────────────────────
            for symbol, pos in list(state["positions"].items()):
                await manage_position(pos, state, app)

            # ── Scan for new entries ────────────────────────────────────────
            if not state["paused"] and not paused_by_circuit:
                for symbol in state["symbols"]:
                    if symbol in state["positions"]:
                        continue  # already in a trade on this symbol

                    # Try LONG
                    ok, score, passed, failed, dbg = check_entry_signal(symbol, "LONG")
                    if ok:
                        logger.info(f"[{symbol}] LONG signal confirmed (score {score})")
                        await open_trade(symbol, "LONG", score, passed, state, app)
                        continue

                    # Try SHORT
                    ok, score, passed, failed, dbg = check_entry_signal(symbol, "SHORT")
                    if ok:
                        logger.info(f"[{symbol}] SHORT signal confirmed (score {score})")
                        await open_trade(symbol, "SHORT", score, passed, state, app)

            save_state(state)

        except Exception as e:
            logger.error(f"Monitor loop error: {e}", exc_info=True)

        await asyncio.sleep(MONITOR_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
#  TELEGRAM COMMAND HANDLERS
# ─────────────────────────────────────────────────────────────────────────────
@owner_only
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "🤖 <b>PsychologyBot v1 — Ready!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<b>Commands:</b>\n"
        "• /start              — Show this message\n"
        "• /add_symbol &lt;SYMBOL&gt;  — Start monitoring a coin\n"
        "• /remove_symbol &lt;SYMBOL&gt; — Stop monitoring a coin\n"
        "• /status             — Show all positions &amp; scores\n"
        "• /pause              — Pause new trade entries\n"
        "• /resume             — Resume new trade entries\n"
        "• /balance            — Show current USDT balance\n"
        "\n"
        "<b>Strategy:</b>\n"
        "• Timeframes : Daily + 4H + 1H\n"
        "• Leverage   : 10x\n"
        "• Position   : 20% balance per symbol\n"
        "• Min score  : 5/8 (conditions 5 &amp; 8 mandatory)\n"
        "• TP1 : 1:1.5 RR → 40% close\n"
        "• TP2 : 1:3  RR → 40% close\n"
        "• TP3 : Trailing last 20%\n"
        "\n"
        "<b>Examples:</b>\n"
        "<code>/add_symbol BTCUSDT</code>\n"
        "<code>/add_symbol ETHUSDT</code>"
    )
    await update.message.reply_text(text, parse_mode="HTML")


@owner_only
async def cmd_add_symbol(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not ctx.args:
        await update.message.reply_text("Usage: /add_symbol <SYMBOL>  e.g. /add_symbol BTCUSDT")
        return
    symbol = ctx.args[0].upper()
    state  = load_state()
    if symbol in state["symbols"]:
        await update.message.reply_text(f"⚠️ {symbol} is already being monitored.")
        return
    state["symbols"].append(symbol)
    save_state(state)
    await update.message.reply_text(
        f"✅ <b>{symbol}</b> added to watchlist.\n"
        f"Bot will monitor every {MONITOR_INTERVAL}s and alert you when entry conditions are met.",
        parse_mode="HTML",
    )
    logger.info(f"Added symbol: {symbol}")


@owner_only
async def cmd_remove_symbol(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not ctx.args:
        await update.message.reply_text("Usage: /remove_symbol <SYMBOL>")
        return
    symbol = ctx.args[0].upper()
    state  = load_state()
    if symbol not in state["symbols"]:
        await update.message.reply_text(f"⚠️ {symbol} is not in the watchlist.")
        return
    state["symbols"].remove(symbol)
    # Also warn if there's an open position
    if symbol in state["positions"]:
        await update.message.reply_text(
            f"⚠️ <b>Warning:</b> {symbol} has an open position! "
            f"It has been removed from monitoring but the position remains open on Bybit. "
            f"Manage it manually.",
            parse_mode="HTML",
        )
    save_state(state)
    await update.message.reply_text(f"✅ <b>{symbol}</b> removed from watchlist.", parse_mode="HTML")
    logger.info(f"Removed symbol: {symbol}")


@owner_only
async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    state   = load_state()
    balance = get_balance_usdt()
    lines   = [
        f"📊 <b>Bot Status</b>",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"Balance : <code>{balance:.2f} USDT</code>",
        f"Paused  : {'🔴 YES' if state['paused'] else '🟢 NO'}",
        f"Consec losses: {state['consec_losses']}",
        f"Daily loss: {state['daily_loss']:.2f} USDT",
        f"",
        f"<b>Monitored Symbols:</b> {', '.join(state['symbols']) or 'None'}",
        f"",
    ]
    if state["positions"]:
        lines.append("<b>Open Positions:</b>")
        for sym, pos in state["positions"].items():
            price  = get_current_price(sym)
            pnl    = _estimate_pnl(pos, price)
            days   = (time.time() - pos["entry_time"]) / 86400
            emoji  = "🟢" if pos["direction"] == "LONG" else "🔴"
            lines.append(
                f"{emoji} <b>{sym}</b> {pos['direction']}\n"
                f"   Entry: {pos['entry_price']:.4f}  |  Now: {price:.4f}\n"
                f"   SL: {pos['sl_price']:.4f}\n"
                f"   TP1: {'✅' if pos['tp1_hit'] else '⬜'}  TP2: {'✅' if pos['tp2_hit'] else '⬜'}\n"
                f"   Est PnL: {pnl:+.2f} USDT  |  Open: {days:.1f}d\n"
                f"   Score: {pos['score']}/8"
            )
    else:
        lines.append("<b>No open positions.</b>")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


@owner_only
async def cmd_pause(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    state = load_state()
    state["paused"] = True
    save_state(state)
    await update.message.reply_text(
        "⏸ <b>Bot paused.</b>\nNo new trades will be opened.\n"
        "Existing positions continue to be managed (SL/TP).",
        parse_mode="HTML",
    )


@owner_only
async def cmd_resume(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    state = load_state()
    state["paused"]       = False
    state["consec_losses"] = 0
    save_state(state)
    await update.message.reply_text(
        "▶️ <b>Bot resumed.</b>\nMonitoring for new entries.",
        parse_mode="HTML",
    )


@owner_only
async def cmd_balance(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    bal = get_balance_usdt()
    await update.message.reply_text(
        f"💰 Available Balance: <code>{bal:.2f} USDT</code>",
        parse_mode="HTML",
    )


# ─────────────────────────────────────────────────────────────────────────────
#  STARTUP — Webhook + aiohttp web server (Render Web Service compatible)
# ─────────────────────────────────────────────────────────────────────────────
async def main() -> None:
    logger.info("Starting PsychologyBot v1...")

    # ── Build Telegram application ──────────────────────────────────────────
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # Register command handlers
    app.add_handler(CommandHandler("start",          cmd_start))
    app.add_handler(CommandHandler("add_symbol",     cmd_add_symbol))
    app.add_handler(CommandHandler("remove_symbol",  cmd_remove_symbol))
    app.add_handler(CommandHandler("status",         cmd_status))
    app.add_handler(CommandHandler("pause",          cmd_pause))
    app.add_handler(CommandHandler("resume",         cmd_resume))
    app.add_handler(CommandHandler("balance",        cmd_balance))

    # ── Set webhook on Telegram ─────────────────────────────────────────────
    # Render injects RENDER_EXTERNAL_HOSTNAME automatically
    base_url = os.environ.get(
        "RENDER_EXTERNAL_HOSTNAME",
        "psychology-trading-bot.onrender.com"
    )
    webhook_url = f"https://{base_url}/webhook"
    port = int(os.environ.get("PORT", 10000))

    await app.bot.set_webhook(url=webhook_url, drop_pending_updates=True)
    logger.info(f"Webhook set to: {webhook_url}")

    # ── Initialise and start the Telegram app ───────────────────────────────
    await app.initialize()
    await app.start()

    # ── Launch the trading monitor loop as a background task ────────────────
    asyncio.create_task(monitor_loop(app))
    logger.info("Monitor loop task created.")

    # ── aiohttp web server — binds the port Render requires ─────────────────
    async def health_handler(request):
        return web.Response(text="OK")

    async def webhook_handler(request):
        try:
            data   = await request.json()
            update = Update.de_json(data=data, bot=app.bot)
            await app.update_queue.put(update)
            return web.Response(text="OK", status=200)
        except Exception as e:
            logger.error(f"Webhook handler error: {e}")
            return web.Response(text="Error", status=500)

    aio_app = web.Application()
    aio_app.router.add_get("/",         health_handler)
    aio_app.router.add_get("/health",   health_handler)
    aio_app.router.add_post("/webhook", webhook_handler)

    runner = web.AppRunner(aio_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Web server running on port {port}")
    logger.info("Bot fully running. Waiting for Telegram updates via webhook.")

    # ── Keep running forever ─────────────────────────────────────────────────
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("Shutting down gracefully...")
        await app.stop()
        await app.shutdown()
        await runner.cleanup()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user.")
    except Exception as exc:
        logger.error(f"Fatal startup error: {exc}", exc_info=True)