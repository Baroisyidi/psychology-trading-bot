"""
PsychologyBot v3
─────────────────────────────────────────────────────────────────────────────
Multi-symbol Bybit Demo trading bot controlled via Telegram.
Strategy  : Multi-timeframe confluence (Daily + 4H + 1H)
Leverage  : 10x | Position: 20% of balance per symbol

Infrastructure (learned from reference code):
- WebSocket real-time prices (REST fallback)
- asyncio.to_thread() for ALL Bybit/pandas calls — event loop never blocked
- ThreadPoolExecutor(32 workers) set as default executor
- API semaphore limits concurrent Bybit calls
- Per-symbol execution guard prevents overlapping monitor runs
- 30-minute status reports per symbol
- Motivational quotes every 12 hours
- State persistence for crash recovery
- Webhook + aiohttp web server (Render Web Service)
─────────────────────────────────────────────────────────────────────────────
"""

import os
import json
import asyncio
import logging
import time
import random
import threading
import concurrent.futures
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import pandas_ta as ta
import numpy as np
from dotenv import load_dotenv
from pybit.unified_trading import HTTP, WebSocket
from aiohttp import web
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ─────────────────────────────────────────────────────────────────────────────
#  THREAD POOL & CONCURRENCY
# ─────────────────────────────────────────────────────────────────────────────
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=32)
_api_semaphore: Optional[asyncio.Semaphore] = None   # initialised after event loop starts

# Per-symbol execution guard — prevents overlapping monitor runs
symbol_monitor_running: dict[str, bool] = {}

# ─────────────────────────────────────────────────────────────────────────────
#  ENVIRONMENT & CONFIG
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
BYBIT_KEY      = os.getenv("BYBIT_API_KEY")
BYBIT_SECRET   = os.getenv("BYBIT_API_SECRET")
CHAT_ID_STR    = os.getenv("CHAT_ID")
STATE_FILE     = os.getenv("STATE_FILE", "/opt/render/project/src/bot_state.json")

if not all([TELEGRAM_TOKEN, BYBIT_KEY, BYBIT_SECRET, CHAT_ID_STR]):
    raise ValueError("Missing required environment variables! Check .env or Render Environment Variables.")

try:
    CHAT_ID = int(CHAT_ID_STR)
except ValueError:
    raise ValueError(f"CHAT_ID must be an integer, got: {CHAT_ID_STR}")

# Trading params
LEVERAGE         = 10
POSITION_PCT     = 0.20     # 20% of balance per symbol
POLL_INTERVAL    = 10       # seconds between monitor cycles
DEAD_TRADE_DAYS  = 7        # close dead trade after 7 days
SL_BUFFER        = 0.02     # 2% breathing room on initial SL

# Indicator params
ADX_PERIOD   = 14
RSI_PERIOD   = 14
EMA_PERIODS  = [20, 50, 100, 200]
MACD_FAST, MACD_SLOW, MACD_SIG = 12, 26, 9
ST_PERIOD, ST_MULT = 10, 3.0
ICHI_CONV, ICHI_BASE, ICHI_SPAN = 9, 26, 52

# ─────────────────────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  BYBIT CLIENT — Demo Trading endpoint
# ─────────────────────────────────────────────────────────────────────────────
bybit_session = HTTP(
    testnet=False,
    api_key=BYBIT_KEY,
    api_secret=BYBIT_SECRET,
    recv_window=5000,
)
# pybit 5.6.2 has no demo= param — override endpoint directly
bybit_session.endpoint = "https://api-demo.bybit.com"

# ─────────────────────────────────────────────────────────────────────────────
#  WEBSOCKET — Real-time price feeds
# ─────────────────────────────────────────────────────────────────────────────
ws_prices: dict[str, float] = {}
ws_prices_lock = threading.Lock()
ws_connection  = None

def _ws_ticker_callback(message):
    try:
        data = message.get("data", {})
        if isinstance(data, list):
            for item in data:
                sym   = item.get("symbol", "")
                price = item.get("lastPrice", "")
                if sym and price:
                    with ws_prices_lock:
                        ws_prices[sym] = float(price)
        elif isinstance(data, dict):
            sym   = data.get("symbol", "")
            price = data.get("lastPrice", "")
            if sym and price:
                with ws_prices_lock:
                    ws_prices[sym] = float(price)
    except Exception as e:
        logger.warning(f"[WS] ticker callback: {e}")

def start_websocket() -> bool:
    global ws_connection
    try:
        ws_connection = WebSocket(testnet=False, channel_type="linear")
        logger.info("[WS] WebSocket connection initialised")
        return True
    except Exception as e:
        logger.warning(f"[WS] Failed to start WebSocket: {e}")
        return False

def subscribe_ws_symbol(symbol: str) -> bool:
    global ws_connection
    if ws_connection is None:
        if not start_websocket():
            return False
    try:
        ws_connection.ticker_stream(symbol=symbol, callback=_ws_ticker_callback)
        logger.info(f"[WS] Subscribed to {symbol}")
        return True
    except Exception as e:
        logger.warning(f"[WS] Failed to subscribe {symbol}: {e}")
        return False

def get_ws_price(symbol: str) -> Optional[float]:
    with ws_prices_lock:
        return ws_prices.get(symbol)

# ─────────────────────────────────────────────────────────────────────────────
#  STATE PERSISTENCE
# ─────────────────────────────────────────────────────────────────────────────
# Global state dicts
monitored_symbols: list[str]              = []
symbol_positions:  dict[str, dict]        = {}   # symbol -> position dict
bot_paused:        bool                   = False
daily_loss:        float                  = 0.0
weekly_loss:       float                  = 0.0
consec_losses:     int                    = 0
last_reset_day:    str                    = ""
last_reset_week:   str                    = ""

def save_state() -> None:
    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        state = {
            "monitored_symbols": monitored_symbols,
            "symbol_positions":  symbol_positions,
            "bot_paused":        bot_paused,
            "daily_loss":        daily_loss,
            "weekly_loss":       weekly_loss,
            "consec_losses":     consec_losses,
            "last_reset_day":    last_reset_day,
            "last_reset_week":   last_reset_week,
        }
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2, default=str)
    except Exception as e:
        logger.error(f"[STATE] save failed: {e}")

def load_state() -> bool:
    global monitored_symbols, symbol_positions, bot_paused
    global daily_loss, weekly_loss, consec_losses, last_reset_day, last_reset_week
    if not os.path.exists(STATE_FILE):
        logger.info("[STATE] No state file found — starting fresh")
        return False
    try:
        with open(STATE_FILE) as f:
            s = json.load(f)
        monitored_symbols = s.get("monitored_symbols", [])
        symbol_positions  = s.get("symbol_positions",  {})
        bot_paused        = s.get("bot_paused",        False)
        daily_loss        = s.get("daily_loss",        0.0)
        weekly_loss       = s.get("weekly_loss",       0.0)
        consec_losses     = s.get("consec_losses",     0)
        last_reset_day    = s.get("last_reset_day",    "")
        last_reset_week   = s.get("last_reset_week",   "")
        logger.info(f"[STATE] Loaded — {len(monitored_symbols)} symbols, {len(symbol_positions)} open positions")
        return True
    except Exception as e:
        logger.error(f"[STATE] load failed: {e}")
        return False

# ─────────────────────────────────────────────────────────────────────────────
#  MOTIVATIONAL QUOTES
# ─────────────────────────────────────────────────────────────────────────────
MOTIVATIONAL_QUOTES = [
    "\"The market doesn't reward intelligence — it rewards discipline.\nSmart traders survive, disciplined traders compound.\"",
    "\"Consistency beats luck every single time.\nWhat you repeat daily becomes your edge.\"",
    "\"Your greatest edge isn't your strategy — it's your psychology.\nMaster your mind, and the market follows.\"",
    "\"Every loss is tuition.\nEvery lesson learned compounds into future strength.\"",
    "\"Control your emotions, and you control your outcomes.\nLose emotional control, and the market decides for you.\"",
    "\"Trading is a marathon, not a sprint.\nPatience is the fuel that finishes the race.\"",
    "\"Survive first. Profit later.\nWinners are simply those who stayed long enough to win.\"",
    "\"Boring, repeatable execution is where real money is made.\nExcitement is expensive.\"",
    "\"Missed trades cost nothing.\nBad trades drain capital, confidence, and clarity.\"",
    "\"Capital preservation is the first form of profit.\nProtect the account, and growth follows.\"",
]

async def send_motivational_quote(context: ContextTypes.DEFAULT_TYPE) -> None:
    quote = random.choice(MOTIVATIONAL_QUOTES)
    await context.bot.send_message(chat_id=CHAT_ID, text=quote)

# ─────────────────────────────────────────────────────────────────────────────
#  OWNER GUARD
# ─────────────────────────────────────────────────────────────────────────────
def owner_only(func):
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_chat.id != CHAT_ID:
            await update.message.reply_text("⛔ Unauthorised. This bot is private.")
            return
        return await func(update, ctx)
    wrapper.__name__ = func.__name__
    return wrapper

# ─────────────────────────────────────────────────────────────────────────────
#  SYNCHRONOUS BYBIT HELPERS — ALL run via asyncio.to_thread()
#  Never call these directly from async code
# ─────────────────────────────────────────────────────────────────────────────
def _sync_get_balance() -> float:
    try:
        resp  = bybit_session.get_wallet_balance(accountType="UNIFIED", coin="USDT")
        coins = resp["result"]["list"][0]["coin"]
        for c in coins:
            if c["coin"] == "USDT":
                raw = c.get("walletBalance") or c.get("availableToWithdraw") or "0"
                return float(raw) if str(raw).strip() else 0.0
        return 0.0
    except Exception as e:
        logger.error(f"_sync_get_balance: {e}")
        return 0.0

def _sync_get_price(symbol: str) -> float:
    # WebSocket first (instant), REST fallback
    ws = get_ws_price(symbol)
    if ws:
        return ws
    try:
        resp = bybit_session.get_tickers(category="linear", symbol=symbol)
        return float(resp["result"]["list"][0]["lastPrice"])
    except Exception as e:
        logger.error(f"_sync_get_price({symbol}): {e}")
        return 0.0

def _sync_get_klines(symbol: str, interval: str, limit: int = 300) -> Optional[pd.DataFrame]:
    try:
        resp = bybit_session.get_kline(
            category="linear", symbol=symbol, interval=interval, limit=limit)
        raw = resp["result"]["list"]
        if not raw:
            return None
        df = pd.DataFrame(raw, columns=[
            "timestamp","open","high","low","close","volume","turnover"])
        for col in ["open","high","low","close","volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["timestamp"] = df["timestamp"].astype("int64")
        df.sort_values("timestamp", inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df
    except Exception as e:
        logger.error(f"_sync_get_klines({symbol},{interval}): {e}")
        return None

def _sync_get_min_qty(symbol: str) -> float:
    try:
        resp = bybit_session.get_instruments_info(category="linear", symbol=symbol)
        lot  = resp["result"]["list"][0]["lotSizeFilter"]
        return float(lot["minOrderQty"])
    except Exception:
        return 0.001

def _sync_set_leverage(symbol: str) -> None:
    try:
        bybit_session.set_leverage(
            category="linear", symbol=symbol,
            buyLeverage=str(LEVERAGE), sellLeverage=str(LEVERAGE))
    except Exception as e:
        logger.warning(f"set_leverage({symbol}): {e}")

def _sync_place_order(symbol: str, side: str, qty: float,
                      reduce_only: bool = False) -> Optional[dict]:
    try:
        resp = bybit_session.place_order(
            category="linear", symbol=symbol,
            side=side, orderType="Market", qty=str(qty),
            timeInForce="GoodTillCancel", reduceOnly=reduce_only)
        return resp["result"]
    except Exception as e:
        logger.error(f"_sync_place_order({symbol},{side},{qty}): {e}")
        return None

def _sync_set_sl(symbol: str, sl_price: float) -> None:
    try:
        bybit_session.set_trading_stop(
            category="linear", symbol=symbol,
            stopLoss=str(round(sl_price, 6)), positionIdx=0)
    except Exception as e:
        logger.error(f"_sync_set_sl({symbol},{sl_price}): {e}")

def _sync_get_position_entry(symbol: str) -> Optional[float]:
    try:
        resp = bybit_session.get_positions(category="linear", symbol=symbol)
        for pos in resp["result"]["list"]:
            if float(pos.get("size", 0)) > 0:
                avg = float(pos.get("avgPrice", 0))
                return avg if avg > 0 else None
        return None
    except Exception as e:
        logger.error(f"_sync_get_position_entry({symbol}): {e}")
        return None

def _sync_get_open_side(symbol: str) -> Optional[str]:
    try:
        resp = bybit_session.get_positions(category="linear", symbol=symbol)
        for pos in resp["result"]["list"]:
            if float(pos.get("size", 0)) > 0:
                return pos["side"]
        return None
    except Exception as e:
        logger.error(f"_sync_get_open_side({symbol}): {e}")
        return None

def _sync_close_full(symbol: str, direction: str) -> None:
    try:
        close_side = "Sell" if direction == "LONG" else "Buy"
        resp = bybit_session.get_positions(category="linear", symbol=symbol)
        for pos in resp["result"]["list"]:
            size = float(pos.get("size", 0))
            if size > 0:
                _sync_place_order(symbol, close_side, size, reduce_only=True)
    except Exception as e:
        logger.error(f"_sync_close_full({symbol}): {e}")

# ─────────────────────────────────────────────────────────────────────────────
#  ASYNC WRAPPERS — safe to await from event loop
# ─────────────────────────────────────────────────────────────────────────────
async def get_balance() -> float:
    async with _api_semaphore:
        return await asyncio.to_thread(_sync_get_balance)

async def get_price(symbol: str) -> float:
    async with _api_semaphore:
        return await asyncio.to_thread(_sync_get_price, symbol)

async def get_klines(symbol: str, interval: str, limit: int = 300) -> Optional[pd.DataFrame]:
    async with _api_semaphore:
        return await asyncio.to_thread(_sync_get_klines, symbol, interval, limit)

async def get_min_qty(symbol: str) -> float:
    async with _api_semaphore:
        return await asyncio.to_thread(_sync_get_min_qty, symbol)

async def place_order(symbol: str, side: str, qty: float,
                      reduce_only: bool = False) -> Optional[dict]:
    async with _api_semaphore:
        return await asyncio.to_thread(_sync_place_order, symbol, side, qty, reduce_only)

async def set_sl(symbol: str, sl_price: float) -> None:
    async with _api_semaphore:
        await asyncio.to_thread(_sync_set_sl, symbol, sl_price)

async def close_full(symbol: str, direction: str) -> None:
    async with _api_semaphore:
        await asyncio.to_thread(_sync_close_full, symbol, direction)

async def get_open_side(symbol: str) -> Optional[str]:
    async with _api_semaphore:
        return await asyncio.to_thread(_sync_get_open_side, symbol)

# ─────────────────────────────────────────────────────────────────────────────
#  INDICATOR ENGINE — pure sync, runs in thread via asyncio.to_thread()
# ─────────────────────────────────────────────────────────────────────────────
def _compute_indicators(
    df_1d: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_1h: pd.DataFrame,
    direction: str,
) -> dict:
    """
    All heavy pandas math in one synchronous function.
    Called via asyncio.to_thread() — never blocks event loop.
    """
    passed = []
    failed = []
    debug  = {}
    price  = float(df_1d["close"].iloc[-1])
    debug["price"] = price

    def ema(df: pd.DataFrame, p: int) -> pd.Series:
        return df["close"].ewm(span=p, adjust=False).mean()

    # Filter 1 — ADX > 20 (4H)
    try:
        adx_df = ta.adx(df_4h["high"], df_4h["low"], df_4h["close"], length=ADX_PERIOD)
        adx = float(adx_df[f"ADX_{ADX_PERIOD}"].iloc[-1])
    except Exception:
        adx = 0.0
    debug["adx"] = round(adx, 2)
    (passed if adx > 20 else failed).append("ADX>20")

    # Filter 2 — Ichimoku Cloud (Daily)
    conv  = (df_1d["high"].rolling(ICHI_CONV).max()  + df_1d["low"].rolling(ICHI_CONV).min())  / 2
    base  = (df_1d["high"].rolling(ICHI_BASE).max()  + df_1d["low"].rolling(ICHI_BASE).min())  / 2
    spa   = ((conv + base) / 2).shift(ICHI_BASE)
    spb   = ((df_1d["high"].rolling(ICHI_SPAN).max() + df_1d["low"].rolling(ICHI_SPAN).min()) / 2).shift(ICHI_BASE)
    ct = max(float(spa.iloc[-1]), float(spb.iloc[-1]))
    cb = min(float(spa.iloc[-1]), float(spb.iloc[-1]))
    debug["cloud_top"] = round(ct, 2)
    debug["cloud_bot"] = round(cb, 2)
    if direction == "LONG":
        (passed if price > ct else failed).append("IchiAboveCloud")
    else:
        (passed if price < cb else failed).append("IchiBelowCloud")

    # Filter 3 — RSI not extreme (4H)
    rsi_s = ta.rsi(df_4h["close"], length=RSI_PERIOD)
    rsi   = float(rsi_s.iloc[-1]) if rsi_s is not None else 50.0
    debug["rsi_4h"] = round(rsi, 2)
    if direction == "LONG":
        (passed if rsi <= 70 else failed).append("RSI<=70")
    else:
        (passed if rsi >= 30 else failed).append("RSI>=30")

    # Filter 4 — Supertrend (4H)
    try:
        st    = ta.supertrend(df_4h["high"], df_4h["low"], df_4h["close"],
                              length=ST_PERIOD, multiplier=ST_MULT)
        st_dir = int(st[f"SUPERTd_{ST_PERIOD}_{ST_MULT}"].iloc[-1])
    except Exception:
        st_dir = 0
    debug["supertrend_4h"] = "BUY" if st_dir == 1 else "SELL"
    if direction == "LONG":
        (passed if st_dir == 1 else failed).append("SupertrendBUY")
    else:
        (passed if st_dir == -1 else failed).append("SupertrendSELL")

    # Filter 5 (MANDATORY) — EMA alignment (Daily)
    e20  = float(ema(df_1d, 20).iloc[-1])
    e50  = float(ema(df_1d, 50).iloc[-1])
    e100 = float(ema(df_1d, 100).iloc[-1])
    e200 = float(ema(df_1d, 200).iloc[-1])
    debug["emas_daily"] = {20: round(e20,2), 50: round(e50,2),
                           100: round(e100,2), 200: round(e200,2)}
    if direction == "LONG":
        ema_ok = (e20 > e50 > e100 > e200) and (price > e50)
        (passed if ema_ok else failed).append("EMA_Bullish_Stack")
    else:
        ema_ok = (e20 < e50 < e100 < e200) and (price < e50)
        (passed if ema_ok else failed).append("EMA_Bearish_Stack")

    # Filter 6 — MACD (Daily)
    macd = ta.macd(df_1d["close"], fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIG)
    if macd is not None:
        m   = float(macd[f"MACD_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"].iloc[-1])
        sig = float(macd[f"MACDs_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"].iloc[-1])
        h   = float(macd[f"MACDh_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"].iloc[-1])
    else:
        m = sig = h = 0.0
    debug["macd"] = {"line": round(m,4), "signal": round(sig,4), "hist": round(h,4)}
    if direction == "LONG":
        (passed if (m > sig and h > 0) else failed).append("MACD_Bullish")
    else:
        (passed if (m < sig and h < 0) else failed).append("MACD_Bearish")

    # Filter 7 — Pullback to 4H EMA 50 or 100
    e50_4h  = float(ema(df_4h, 50).iloc[-1])
    e100_4h = float(ema(df_4h, 100).iloc[-1])
    debug["ema50_4h"]  = round(e50_4h, 2)
    debug["ema100_4h"] = round(e100_4h, 2)
    near = abs(price - e50_4h)/e50_4h <= 0.015 or abs(price - e100_4h)/e100_4h <= 0.015
    lbl  = "PullbackToEMA" if direction == "LONG" else "RallyToEMA"
    (passed if near else failed).append(lbl)

    # Filter 8 (MANDATORY) — 1H candle
    c    = df_1h.iloc[-1]
    body = c["close"] - c["open"]
    rng  = c["high"] - c["low"]
    if rng == 0:
        candle_ok = False
    else:
        bpct     = abs(body) / c["open"]
        cpos     = (c["close"] - c["low"]) / rng
        if direction == "LONG":
            candle_ok = body > 0 and bpct >= 0.003 and cpos >= 0.5
            (passed if candle_ok else failed).append("Bullish1HCandle")
        else:
            candle_ok = body < 0 and bpct >= 0.003 and cpos <= 0.5
            (passed if candle_ok else failed).append("Bearish1HCandle")

    # Entry decision: both mandatory filters + 3 of remaining 6
    m5 = "EMA_Bullish_Stack" if direction == "LONG" else "EMA_Bearish_Stack"
    m8 = "Bullish1HCandle"   if direction == "LONG" else "Bearish1HCandle"
    opt = [p for p in passed if p not in (m5, m8)]
    should_enter = (m5 in passed) and (m8 in passed) and len(opt) >= 3

    return {
        "should_enter": should_enter,
        "score":   len(passed),
        "passed":  passed,
        "failed":  failed,
        "debug":   debug,
        "e100_4h": e100_4h,
    }

def _check_ema_exit(df_1d: pd.DataFrame, direction: str) -> bool:
    """EMA inversion on completed Daily candles (sync)."""
    try:
        df = df_1d.iloc[:-1]
        def e(p): return float(df["close"].ewm(span=p, adjust=False).mean().iloc[-1])
        e20, e50, e100, e200 = e(20), e(50), e(100), e(200)
        if direction == "LONG":
            return (e20 < e50) or (e50 < e100) or (e100 < e200)
        else:
            return (e20 > e50) or (e50 > e100) or (e100 > e200)
    except Exception:
        return False

def _compute_trailing_sl(df_4h: pd.DataFrame, direction: str, cur_sl: float) -> Optional[float]:
    try:
        last = df_4h.iloc[-2]
        if direction == "LONG":
            c = float(last["low"])
            return round(c, 6) if c > cur_sl else None
        else:
            c = float(last["high"])
            return round(c, 6) if c < cur_sl else None
    except Exception:
        return None

# ─────────────────────────────────────────────────────────────────────────────
#  POSITION SIZING & SL/TP HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _round_qty(qty: float, min_q: float) -> float:
    return max(min_q, round(qty // min_q * min_q, 8))

def _calc_sl(entry: float, direction: str, e100_4h: float) -> float:
    if direction == "LONG":
        base = min(e100_4h, entry * (1 - 0.015))
        return round(base * (1 - SL_BUFFER), 6)
    else:
        base = max(e100_4h, entry * (1 + 0.015))
        return round(base * (1 + SL_BUFFER), 6)

def _calc_tps(entry: float, sl: float, direction: str):
    risk = abs(entry - sl)
    if direction == "LONG":
        return round(entry + risk * 1.5, 6), round(entry + risk * 3.0, 6)
    else:
        return round(entry - risk * 1.5, 6), round(entry - risk * 3.0, 6)

def _est_pnl(pos: dict, price: float) -> float:
    diff = (price - pos["entry_price"]) if pos["direction"] == "LONG" \
           else (pos["entry_price"] - price)
    return diff * pos["remaining_qty"] * LEVERAGE

# ─────────────────────────────────────────────────────────────────────────────
#  TELEGRAM ALERT
# ─────────────────────────────────────────────────────────────────────────────
async def alert(bot, text: str) -> None:
    try:
        await bot.send_message(chat_id=CHAT_ID, text=text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"alert: {e}")

# ─────────────────────────────────────────────────────────────────────────────
#  TRADE ENTRY
# ─────────────────────────────────────────────────────────────────────────────
async def open_trade(symbol: str, direction: str, indicators: dict, bot) -> None:
    global consec_losses

    await asyncio.to_thread(_sync_set_leverage, symbol)

    entry = await get_price(symbol)
    if not entry:
        await alert(bot, f"⚠️ [{symbol}] Cannot fetch price — trade skipped.")
        return

    balance = await get_balance()
    if not balance:
        await alert(bot, f"⚠️ [{symbol}] Cannot fetch balance — trade skipped.")
        return

    sl       = _calc_sl(entry, direction, indicators["e100_4h"])
    tp1, tp2 = _calc_tps(entry, sl, direction)
    min_q    = await get_min_qty(symbol)
    qty      = _round_qty((balance * POSITION_PCT * LEVERAGE) / entry, min_q)

    if qty <= 0:
        await alert(bot, f"⚠️ [{symbol}] Position size too small.")
        return

    side   = "Buy" if direction == "LONG" else "Sell"
    result = await place_order(symbol, side, qty)
    if not result:
        await alert(bot, f"⚠️ [{symbol}] Order placement failed.")
        return

    # Get actual fill price from Bybit
    await asyncio.sleep(1)
    real_entry = await asyncio.to_thread(_sync_get_position_entry, symbol)
    fill_price = real_entry if real_entry else entry

    # Recalculate SL/TP from actual fill
    sl       = _calc_sl(fill_price, direction, indicators["e100_4h"])
    tp1, tp2 = _calc_tps(fill_price, sl, direction)
    await set_sl(symbol, sl)

    pos = {
        "symbol":         symbol,
        "direction":      direction,
        "entry_price":    fill_price,
        "sl_price":       sl,
        "tp1_price":      tp1,
        "tp2_price":      tp2,
        "original_qty":   qty,
        "remaining_qty":  qty,
        "tp1_hit":        False,
        "tp2_hit":        False,
        "entry_time":     time.time(),
        "score":          indicators["score"],
        "filters_passed": indicators["passed"],
    }
    symbol_positions[symbol] = pos
    consec_losses = 0
    save_state()

    emoji  = "🟢" if direction == "LONG" else "🔴"
    f_str  = " ".join([f"✅{p}" for p in indicators["passed"]])
    fail_s = " ".join([f"❌{p}" for p in indicators["failed"]])

    await alert(bot,
        f"{emoji} <b>TRADE OPENED — {symbol} {direction}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 Entry  : <code>{fill_price:.4f}</code>\n"
        f"🛡 SL     : <code>{sl:.4f}</code>\n"
        f"🎯 TP1    : <code>{tp1:.4f}</code>  (close 40%)\n"
        f"🎯 TP2    : <code>{tp2:.4f}</code>  (close 40%)\n"
        f"🌊 TP3    : Trailing last 20%\n"
        f"📊 Score  : {indicators['score']}/8\n"
        f"✅ Passed : {f_str}\n"
        f"❌ Failed : {fail_s}\n"
        f"⚡ Qty    : {qty}  |  Leverage: {LEVERAGE}x  |  Balance: {balance:.2f} USDT"
    )
    logger.info(f"[{symbol}] Trade opened {direction} @ {fill_price}")

# ─────────────────────────────────────────────────────────────────────────────
#  POSITION MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────
async def manage_position(pos: dict, bot) -> None:
    global consec_losses, daily_loss, weekly_loss
    symbol    = pos["symbol"]
    direction = pos["direction"]

    # Fetch price, daily candles, 4H candles concurrently in background threads
    price, df_1d, df_4h = await asyncio.gather(
        asyncio.to_thread(_sync_get_price, symbol),
        asyncio.to_thread(_sync_get_klines, symbol, "D", 100),
        asyncio.to_thread(_sync_get_klines, symbol, "240", 50),
    )

    if not price or df_1d is None or df_4h is None:
        return

    current_price = price

    # ── Dead trade ───────────────────────────────────────────────────────────
    if not pos["tp1_hit"] and (time.time() - pos["entry_time"]) / 86400 >= DEAD_TRADE_DAYS:
        await close_full(symbol, direction)
        pnl = _est_pnl(pos, current_price)
        await alert(bot,
            f"⏱ <b>DEAD TRADE CLOSED — {symbol} {direction}</b>\n"
            f"No TP1 after {DEAD_TRADE_DAYS} days with no movement.\n"
            f"Closed @ <code>{current_price:.4f}</code>\n"
            f"Est. PnL: {pnl:+.2f} USDT\n"
            f"Bot goes FLAT on {symbol}. Watching for next signal.")
        _close_pos(symbol, pnl)
        return

    # ── EMA inversion exit ───────────────────────────────────────────────────
    ema_exit = await asyncio.to_thread(_check_ema_exit, df_1d, direction)
    if ema_exit:
        await close_full(symbol, direction)
        pnl = _est_pnl(pos, current_price)
        await alert(bot,
            f"📉 <b>EMA INVERSION EXIT — {symbol} {direction}</b>\n"
            f"Daily EMA structure inverted — trade logic no longer valid.\n"
            f"Closed @ <code>{current_price:.4f}</code>\n"
            f"Est. PnL: {pnl:+.2f} USDT\n"
            f"Bot goes FLAT on {symbol}. Checking for reversal entry...")
        # Check for reversal entry
        await check_reversal_after_exit(symbol, direction, bot)
        _close_pos(symbol, pnl)
        return

    # ── Stop loss hit ────────────────────────────────────────────────────────
    sl = pos["sl_price"]
    sl_hit = (direction == "LONG" and current_price <= sl) or \
             (direction == "SHORT" and current_price >= sl)
    if sl_hit:
        await close_full(symbol, direction)
        pnl = _est_pnl(pos, current_price)
        consec_losses += 1
        loss_warn = f"\n⚠️ {consec_losses} consecutive losses! Bot will pause after 3." if consec_losses >= 2 else ""
        await alert(bot,
            f"🛑 <b>STOP LOSS HIT — {symbol} {direction}</b>\n"
            f"SL  : <code>{sl:.4f}</code>\n"
            f"Price: <code>{current_price:.4f}</code>\n"
            f"Est. PnL: {pnl:+.2f} USDT\n"
            f"Consecutive losses: {consec_losses}{loss_warn}\n"
            f"Bot goes FLAT on {symbol}. Waiting for next valid signal.")
        _close_pos(symbol, pnl)
        save_state()
        return

    # ── TP1 ──────────────────────────────────────────────────────────────────
    if not pos["tp1_hit"]:
        tp1_hit = (direction == "LONG"  and current_price >= pos["tp1_price"]) or \
                  (direction == "SHORT" and current_price <= pos["tp1_price"])
        if tp1_hit:
            min_q     = await get_min_qty(symbol)
            close_qty = _round_qty(pos["original_qty"] * 0.40, min_q)
            await place_order(symbol, "Sell" if direction == "LONG" else "Buy",
                              close_qty, reduce_only=True)
            pos["remaining_qty"] = round(pos["original_qty"] * 0.60, 8)
            pos["tp1_hit"]  = True
            pos["sl_price"] = pos["entry_price"]
            await set_sl(symbol, pos["entry_price"])
            consec_losses = 0
            pnl_p = abs(pos["tp1_price"] - pos["entry_price"]) * close_qty * LEVERAGE
            save_state()
            await alert(bot,
                f"🎯 <b>TP1 HIT — {symbol} {direction}</b>\n"
                f"Closed 40% @ <code>{current_price:.4f}</code>\n"
                f"Est. partial PnL: +{pnl_p:.2f} USDT\n"
                f"🔒 SL moved to breakeven: <code>{pos['entry_price']:.4f}</code>\n"
                f"Remaining qty: {pos['remaining_qty']}\n"
                f"Riding to TP2 @ <code>{pos['tp2_price']:.4f}</code>")
            return

    # ── TP2 ──────────────────────────────────────────────────────────────────
    if pos["tp1_hit"] and not pos["tp2_hit"]:
        tp2_hit = (direction == "LONG"  and current_price >= pos["tp2_price"]) or \
                  (direction == "SHORT" and current_price <= pos["tp2_price"])
        if tp2_hit:
            min_q     = await get_min_qty(symbol)
            close_qty = _round_qty(pos["original_qty"] * 0.40, min_q)
            await place_order(symbol, "Sell" if direction == "LONG" else "Buy",
                              close_qty, reduce_only=True)
            pos["remaining_qty"] = round(pos["original_qty"] * 0.20, 8)
            pos["tp2_hit"] = True
            pnl_p = abs(pos["tp2_price"] - pos["entry_price"]) * close_qty * LEVERAGE
            save_state()
            await alert(bot,
                f"🎯 <b>TP2 HIT — {symbol} {direction}</b>\n"
                f"Closed 40% @ <code>{current_price:.4f}</code>\n"
                f"Est. partial PnL: +{pnl_p:.2f} USDT\n"
                f"🌊 Trailing stop now active on last 20%\n"
                f"Remaining qty: {pos['remaining_qty']}")
            return

    # ── Trailing SL after TP2 ────────────────────────────────────────────────
    if pos["tp2_hit"]:
        new_sl = await asyncio.to_thread(
            _compute_trailing_sl, df_4h, direction, pos["sl_price"])
        if new_sl:
            old_sl = pos["sl_price"]
            pos["sl_price"] = new_sl
            await set_sl(symbol, new_sl)
            save_state()
            logger.info(f"[{symbol}] Trailing SL {old_sl:.4f} → {new_sl:.4f}")

def _close_pos(symbol: str, pnl: float) -> None:
    global daily_loss, weekly_loss
    daily_loss  += min(pnl, 0)
    weekly_loss += min(pnl, 0)
    symbol_positions.pop(symbol, None)
    save_state()

# ─────────────────────────────────────────────────────────────────────────────
#  REVERSAL CHECK — called after exit, checks if opposite trade is valid
# ─────────────────────────────────────────────────────────────────────────────
async def check_reversal_after_exit(symbol: str, old_direction: str, bot) -> None:
    new_direction = "SHORT" if old_direction == "LONG" else "LONG"
    df_1d, df_4h, df_1h = await asyncio.gather(
        get_klines(symbol, "D",   300),
        get_klines(symbol, "240", 300),
        get_klines(symbol, "60",  100),
    )
    if df_1d is None or df_4h is None or df_1h is None:
        return
    indicators = await asyncio.to_thread(
        _compute_indicators, df_1d, df_4h, df_1h, new_direction)
    if indicators["should_enter"]:
        await alert(bot,
            f"🔄 <b>REVERSAL SIGNAL — {symbol} {new_direction}</b>\n"
            f"After exiting {old_direction}, {new_direction} conditions confirmed.\n"
            f"Score: {indicators['score']}/8 — Opening reversal trade...")
        await open_trade(symbol, new_direction, indicators, bot)
    else:
        await alert(bot,
            f"🟡 <b>GOING FLAT — {symbol}</b>\n"
            f"Exited {old_direction} but {new_direction} conditions not met "
            f"(score {indicators['score']}/8 — need 5).\n"
            f"Bot watches and waits for next valid signal.")

# ─────────────────────────────────────────────────────────────────────────────
#  MAIN MONITOR LOOP — per symbol, with execution guard
# ─────────────────────────────────────────────────────────────────────────────
async def _monitor_symbol_inner(symbol: str, bot, job_data: dict) -> None:
    """Inner monitor logic — all heavy calls off event loop."""
    global daily_loss, weekly_loss, consec_losses
    global last_reset_day, last_reset_week, bot_paused

    now      = datetime.now(timezone.utc)
    today    = now.strftime("%Y-%m-%d")
    iso_week = now.strftime("%Y-W%W")

    if last_reset_day != today:
        daily_loss    = 0.0
        last_reset_day = today
    if last_reset_week != iso_week:
        weekly_loss    = 0.0
        last_reset_week = iso_week

    # Circuit breakers
    balance = await get_balance()
    paused_circuit = (
        daily_loss  <= -0.04 * balance or
        weekly_loss <= -0.06 * balance or
        consec_losses >= 3
    )

    if paused_circuit and not job_data.get("circuit_alerted"):
        await alert(bot,
            f"⚠️ <b>CIRCUIT BREAKER — {symbol}</b>\n"
            f"Daily loss: {daily_loss:.2f} USDT | Weekly loss: {weekly_loss:.2f} USDT\n"
            f"Consecutive losses: {consec_losses}\n"
            f"No new entries until conditions reset. Existing positions still managed.")
        job_data["circuit_alerted"] = True
    elif not paused_circuit:
        job_data["circuit_alerted"] = False

    # ── Manage open position if exists ───────────────────────────────────────
    if symbol in symbol_positions:
        await manage_position(symbol_positions[symbol], bot)
        # Send 30-min status report
        await _maybe_send_status_report(symbol, bot, job_data, balance)
        return

    # ── Scan for new entry ───────────────────────────────────────────────────
    if bot_paused or paused_circuit:
        return

    df_1d, df_4h, df_1h = await asyncio.gather(
        get_klines(symbol, "D",   300),
        get_klines(symbol, "240", 300),
        get_klines(symbol, "60",  100),
    )
    if df_1d is None or df_4h is None or df_1h is None:
        logger.warning(f"[{symbol}] Failed to fetch candles — skipping cycle")
        return

    price = float(df_1d["close"].iloc[-1])

    # Check LONG and SHORT in a single thread call
    for direction in ("LONG", "SHORT"):
        indicators = await asyncio.to_thread(
            _compute_indicators, df_1d, df_4h, df_1h, direction)
        if indicators["should_enter"]:
            logger.info(f"[{symbol}] {direction} entry confirmed (score {indicators['score']})")
            await open_trade(symbol, direction, indicators, bot)
            return

    # Send 30-min status report even when flat
    await _maybe_send_status_report(symbol, bot, job_data, balance, df_1d=df_1d, df_4h=df_4h, price=price)

async def _maybe_send_status_report(
    symbol: str, bot, job_data: dict, balance: float,
    df_1d=None, df_4h=None, price: float = 0.0
) -> None:
    """Send a 30-minute status update for a symbol."""
    now = time.time()
    last_report = job_data.get("last_report_time", 0)
    if now - last_report < 1800:
        return
    job_data["last_report_time"] = now

    pos = symbol_positions.get(symbol)

    if pos:
        current_price = await get_price(symbol)
        pnl = _est_pnl(pos, current_price)
        days = (time.time() - pos["entry_time"]) / 86400
        emoji = "🟢" if pos["direction"] == "LONG" else "🔴"
        trade_info = (
            f"{emoji} <b>Open {pos['direction']} position</b>\n"
            f"   Entry  : <code>{pos['entry_price']:.4f}</code>\n"
            f"   Current: <code>{current_price:.4f}</code>\n"
            f"   SL     : <code>{pos['sl_price']:.4f}</code>\n"
            f"   TP1    : {'✅' if pos['tp1_hit'] else '⬜'} <code>{pos['tp1_price']:.4f}</code>\n"
            f"   TP2    : {'✅' if pos['tp2_hit'] else '⬜'} <code>{pos['tp2_price']:.4f}</code>\n"
            f"   Est PnL: {pnl:+.2f} USDT  |  Open: {days:.1f}d\n"
            f"   Score  : {pos['score']}/8"
        )
    else:
        trade_info = "⏳ No open position — watching for entry signal"

    # Quick indicator snapshot
    ind_text = ""
    if df_1d is not None and df_4h is not None:
        try:
            for direction in ("LONG", "SHORT"):
                ind = await asyncio.to_thread(
                    _compute_indicators, df_1d, df_4h,
                    await asyncio.to_thread(_sync_get_klines, symbol, "60", 50) or df_4h,
                    direction)
                d = ind["debug"]
                ind_text = (
                    f"\n📊 <b>Current Indicators ({symbol})</b>\n"
                    f"   Price    : <code>{price:.2f}</code>\n"
                    f"   ADX(4H)  : <code>{d.get('adx','?')}</code>\n"
                    f"   RSI(4H)  : <code>{d.get('rsi_4h','?')}</code>\n"
                    f"   ST(4H)   : <code>{d.get('supertrend_4h','?')}</code>\n"
                    f"   EMA20(D) : <code>{d.get('emas_daily',{}).get(20,'?'):.0f}</code>  "
                    f"EMA50(D): <code>{d.get('emas_daily',{}).get(50,'?'):.0f}</code>\n"
                    f"   Cloud top: <code>{d.get('cloud_top','?'):.0f}</code>  "
                    f"bot: <code>{d.get('cloud_bot','?'):.0f}</code>\n"
                    f"   MACD hist: <code>{d.get('macd',{}).get('hist','?')}</code>\n"
                    f"   LONG score : {ind['score']}/8 — "
                    f"{'🚀 SIGNAL!' if ind['should_enter'] else 'waiting'}"
                )
                break
        except Exception:
            pass

    await alert(bot,
        f"📋 <b>30-min Report — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Balance: <code>{balance:.2f} USDT</code>  |  "
        f"Paused: {'🔴 YES' if bot_paused else '🟢 NO'}\n"
        f"Consec losses: {consec_losses}  |  Daily loss: {daily_loss:.2f}\n"
        f"\n{trade_info}"
        f"{ind_text}\n\n"
        f"⚙️ Strategy: Daily+4H+1H  |  10x lev  |  20% balance\n"
        f"🔁 Monitoring every {POLL_INTERVAL}s via webhook"
    )

async def monitor_symbol(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Job callback — wraps inner logic with execution guard."""
    symbol   = context.job.data["symbol"]
    job_data = context.job.data

    if symbol_monitor_running.get(symbol, False):
        logger.debug(f"[GUARD] {symbol}: previous cycle still running, skipping")
        return
    symbol_monitor_running[symbol] = True
    try:
        await _monitor_symbol_inner(symbol, context.bot, job_data)
    except Exception as e:
        logger.error(f"[MONITOR] {symbol}: unhandled error: {e}", exc_info=True)
    finally:
        symbol_monitor_running[symbol] = False

# ─────────────────────────────────────────────────────────────────────────────
#  TELEGRAM COMMAND HANDLERS
# ─────────────────────────────────────────────────────────────────────────────
@owner_only
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "🤖 <b>PsychologyBot v3 — Ready!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>Commands:</b>\n"
        "• /start               — Show this message\n"
        "• /add_symbol &lt;SYMBOL&gt;   — Start monitoring a coin\n"
        "• /remove_symbol &lt;SYMBOL&gt; — Stop monitoring a coin\n"
        "• /status              — All positions &amp; scores\n"
        "• /scan                — Live indicator scan now\n"
        "• /pause               — Pause new entries\n"
        "• /resume              — Resume new entries\n"
        "• /balance             — Show USDT balance\n\n"
        "<b>Entry Filters (5/8 required — conditions 5 &amp; 8 mandatory):</b>\n"
        "• 1. ADX &gt; 20 (trending market)\n"
        "• 2. Price above/below Ichimoku Cloud (Daily)\n"
        "• 3. RSI not extreme (not &gt;70 long / not &lt;30 short) (4H)\n"
        "• 4. Supertrend green=LONG / red=SHORT (4H)\n"
        "• 5. ✅ MANDATORY: EMA 20&gt;50&gt;100&gt;200 stack (Daily)\n"
        "• 6. MACD histogram green=LONG / red=SHORT (Daily)\n"
        "• 7. Price pulled back to 4H EMA 50 or 100\n"
        "• 8. ✅ MANDATORY: Bullish/Bearish 1H candle trigger\n\n"
        "<b>Stop Loss (Tiered):</b>\n"
        "• Tier 1: Below 4H EMA 100 − 2% (long) / above + 2% (short)\n"
        "• Tier 2: After TP1 hit → SL moves to entry (breakeven)\n"
        "• Tier 3: After TP2 hit → Trailing below each 4H candle low\n\n"
        "<b>Take Profit:</b>\n"
        "• TP1: 1:1.5 RR → close 40% → SL to breakeven\n"
        "• TP2: 1:3 RR   → close 40% → start trailing\n"
        "• TP3: Trailing last 20% until trend ends\n\n"
        "<b>Exit Rules:</b>\n"
        "• Exit immediately when Daily EMA structure inverts\n"
        "• Dead trade: close if TP1 not hit after 7 days\n\n"
        "<b>Reversal Rules:</b>\n"
        "• After exit, if opposite trade scores 5/8 → reverse\n"
        "• If not → go FLAT and wait for next valid setup\n\n"
        "<b>Infrastructure:</b>\n"
        "• WebSocket real-time prices (REST fallback)\n"
        "• All Bybit/pandas in background threads (never freezes)\n"
        "• 30-min status reports per symbol\n"
        "• State persistence for crash recovery\n"
        "• Bot is PRIVATE — owner only\n\n"
        "<b>Examples:</b>\n"
        "<code>/add_symbol BTCUSDT</code>\n"
        "<code>/add_symbol ETHUSDT</code>",
        parse_mode="HTML",
    )

@owner_only
async def cmd_add_symbol(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not ctx.args:
        await update.message.reply_text("Usage: /add_symbol BTCUSDT")
        return
    symbol = ctx.args[0].upper()
    if symbol in monitored_symbols:
        await update.message.reply_text(f"⚠️ {symbol} is already being monitored.")
        return
    monitored_symbols.append(symbol)
    save_state()
    subscribe_ws_symbol(symbol)

    # Stagger first run
    idx    = monitored_symbols.index(symbol)
    delay  = POLL_INTERVAL + idx * 1.25

    if ctx.job_queue:
        ctx.job_queue.run_repeating(
            callback=monitor_symbol,
            interval=POLL_INTERVAL,
            first=delay,
            chat_id=CHAT_ID,
            name=f"{symbol}_monitor",
            data={"symbol": symbol, "last_report_time": 0,
                  "circuit_alerted": False},
        )

    await update.message.reply_text(
        f"✅ <b>{symbol}</b> added!\n"
        f"Bot will scan every {POLL_INTERVAL}s.\n"
        f"You'll receive a 30-min status report and alerts on every trade event.\n"
        f"Use /scan to see current indicator scores.",
        parse_mode="HTML")

@owner_only
async def cmd_remove_symbol(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not ctx.args:
        await update.message.reply_text("Usage: /remove_symbol BTCUSDT")
        return
    symbol = ctx.args[0].upper()
    if symbol not in monitored_symbols:
        await update.message.reply_text(f"⚠️ {symbol} not in watchlist.")
        return
    monitored_symbols.remove(symbol)
    if symbol in symbol_positions:
        await update.message.reply_text(
            f"⚠️ {symbol} has an open position — removed from monitoring "
            f"but position still open on Bybit. Manage manually.")
    if ctx.job_queue:
        for job in ctx.job_queue.get_jobs_by_name(f"{symbol}_monitor"):
            job.schedule_removal()
    save_state()
    await update.message.reply_text(f"✅ <b>{symbol}</b> removed.", parse_mode="HTML")

@owner_only
async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    balance = await get_balance()
    lines   = [
        "📊 <b>Bot Status</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"Balance      : <code>{balance:.2f} USDT</code>",
        f"Paused       : {'🔴 YES' if bot_paused else '🟢 NO'}",
        f"Consec losses: {consec_losses}",
        f"Daily loss   : {daily_loss:.2f} USDT",
        f"Weekly loss  : {weekly_loss:.2f} USDT",
        f"",
        f"<b>Watching:</b> {', '.join(monitored_symbols) or 'None'}",
        "",
    ]
    if symbol_positions:
        lines.append("<b>Open Positions:</b>")
        for sym, pos in symbol_positions.items():
            price = await get_price(sym)
            pnl   = _est_pnl(pos, price)
            days  = (time.time() - pos["entry_time"]) / 86400
            e     = "🟢" if pos["direction"] == "LONG" else "🔴"
            lines.append(
                f"{e} <b>{sym}</b> {pos['direction']}\n"
                f"   Entry:{pos['entry_price']:.4f}  Now:{price:.4f}\n"
                f"   SL:{pos['sl_price']:.4f}\n"
                f"   TP1:{'✅' if pos['tp1_hit'] else '⬜'} {pos['tp1_price']:.4f}  "
                f"TP2:{'✅' if pos['tp2_hit'] else '⬜'} {pos['tp2_price']:.4f}\n"
                f"   PnL:{pnl:+.2f} USDT  Open:{days:.1f}d  Score:{pos['score']}/8"
            )
    else:
        lines.append("No open positions.")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

@owner_only
async def cmd_scan(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not monitored_symbols:
        await update.message.reply_text("No symbols monitored. Use /add_symbol first.")
        return
    await update.message.reply_text(f"🔍 Scanning {len(monitored_symbols)} symbol(s)...")

    for symbol in monitored_symbols:
        df_1d, df_4h, df_1h = await asyncio.gather(
            get_klines(symbol, "D",   300),
            get_klines(symbol, "240", 300),
            get_klines(symbol, "60",  100),
        )
        if df_1d is None or df_4h is None or df_1h is None:
            await update.message.reply_text(f"⚠️ {symbol}: Failed to fetch candles.")
            continue

        for direction in ("LONG", "SHORT"):
            ind = await asyncio.to_thread(
                _compute_indicators, df_1d, df_4h, df_1h, direction)
            d   = ind["debug"]
            e   = "🟢" if direction == "LONG" else "🔴"
            status = "🚀 ENTRY SIGNAL!" if ind["should_enter"] else "⏳ Waiting"
            p_str  = "\n   ".join([f"✅ {p}" for p in ind["passed"]]) or "none"
            f_str  = "\n   ".join([f"❌ {p}" for p in ind["failed"]]) or "none"
            emas   = d.get("emas_daily", {})

            await update.message.reply_text(
                f"{e} <b>{symbol} — {direction}</b>  {status}\n"
                f"Score: <b>{ind['score']}/8</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>Passed:</b>\n   {p_str}\n"
                f"<b>Failed:</b>\n   {f_str}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>Raw Values:</b>\n"
                f"Price    : <code>{d.get('price',0):.2f}</code>\n"
                f"ADX(4H)  : <code>{d.get('adx','?')}</code>\n"
                f"RSI(4H)  : <code>{d.get('rsi_4h','?')}</code>\n"
                f"ST(4H)   : <code>{d.get('supertrend_4h','?')}</code>\n"
                f"EMA20(D) : <code>{emas.get(20,'?'):.0f}</code>  "
                f"EMA50(D): <code>{emas.get(50,'?'):.0f}</code>\n"
                f"EMA100(D): <code>{emas.get(100,'?'):.0f}</code>  "
                f"EMA200(D): <code>{emas.get(200,'?'):.0f}</code>\n"
                f"Cloud top: <code>{d.get('cloud_top','?')}</code>  "
                f"bot: <code>{d.get('cloud_bot','?')}</code>\n"
                f"EMA50(4H): <code>{d.get('ema50_4h','?')}</code>  "
                f"EMA100(4H): <code>{d.get('ema100_4h','?')}</code>\n"
                f"MACD hist: <code>{d.get('macd',{}).get('hist','?')}</code>",
                parse_mode="HTML",
            )

@owner_only
async def cmd_pause(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    global bot_paused
    bot_paused = True
    save_state()
    await update.message.reply_text(
        "⏸ <b>Bot paused.</b>\n"
        "No new trade entries. Existing positions continue to be managed (SL/TP).",
        parse_mode="HTML")

@owner_only
async def cmd_resume(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    global bot_paused, consec_losses
    bot_paused    = False
    consec_losses = 0
    save_state()
    await update.message.reply_text("▶️ <b>Bot resumed.</b> Monitoring for entries.", parse_mode="HTML")

@owner_only
async def cmd_balance(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    bal = await get_balance()
    await update.message.reply_text(
        f"💰 Balance: <code>{bal:.2f} USDT</code>", parse_mode="HTML")

async def error_handler(update: object, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Telegram error: {ctx.error}")
    try:
        await ctx.bot.send_message(
            chat_id=CHAT_ID,
            text=f"⚠️ Bot error:\n<code>{ctx.error}</code>",
            parse_mode="HTML")
    except Exception:
        pass

# ─────────────────────────────────────────────────────────────────────────────
#  STARTUP — Webhook + aiohttp (Render Web Service)
# ─────────────────────────────────────────────────────────────────────────────
async def main() -> None:
    global _api_semaphore

    # Set thread pool as default executor FIRST
    loop = asyncio.get_running_loop()
    loop.set_default_executor(_executor)

    # Initialise semaphore
    _api_semaphore = asyncio.Semaphore(5)

    print("=== PsychologyBot v3 startup ===")
    print(f"TELEGRAM_TOKEN loaded ({len(TELEGRAM_TOKEN)} chars)")
    print(f"CHAT_ID: {CHAT_ID}")
    print(f"Thread pool: {_executor._max_workers} workers")

    # Load persisted state
    load_state()

    # Start WebSocket
    if start_websocket():
        for sym in monitored_symbols:
            subscribe_ws_symbol(sym)
        print(f"[WS] Subscribed to {len(monitored_symbols)} symbols")

    # Build Telegram app
    app = Application.builder().token(TELEGRAM_TOKEN).updater(None).build()
    app.add_handler(CommandHandler("start",          cmd_start))
    app.add_handler(CommandHandler("add_symbol",     cmd_add_symbol))
    app.add_handler(CommandHandler("remove_symbol",  cmd_remove_symbol))
    app.add_handler(CommandHandler("status",         cmd_status))
    app.add_handler(CommandHandler("scan",           cmd_scan))
    app.add_handler(CommandHandler("pause",          cmd_pause))
    app.add_handler(CommandHandler("resume",         cmd_resume))
    app.add_handler(CommandHandler("balance",        cmd_balance))
    app.add_error_handler(error_handler)

    # Recurring jobs
    if app.job_queue:
        app.job_queue.run_repeating(
            callback=send_motivational_quote,
            interval=12 * 3600,
            first=3600,
            chat_id=CHAT_ID,
            name="motivational_quotes",
        )
        # Recreate monitoring jobs for restored symbols (staggered)
        for i, sym in enumerate(monitored_symbols):
            existing = app.job_queue.get_jobs_by_name(f"{sym}_monitor")
            if not existing:
                delay = POLL_INTERVAL + i * 1.25
                app.job_queue.run_repeating(
                    callback=monitor_symbol,
                    interval=POLL_INTERVAL,
                    first=delay,
                    chat_id=CHAT_ID,
                    name=f"{sym}_monitor",
                    data={"symbol": sym, "last_report_time": 0,
                          "circuit_alerted": False},
                )
                print(f"[STARTUP] Monitor job for {sym} (first run in {delay:.1f}s)")

    # Webhook
    base_url    = os.environ.get("RENDER_EXTERNAL_HOSTNAME",
                                 "psychology-trading-bot.onrender.com")
    webhook_url = f"https://{base_url}/webhook"
    port        = int(os.environ.get("PORT", 10000))

    await app.bot.set_webhook(url=webhook_url, drop_pending_updates=True)
    print(f"Webhook → {webhook_url}")

    await app.initialize()
    await app.start()

    # aiohttp web server — binds port for Render Web Service
    async def health(request):
        return web.Response(text="OK")

    async def webhook_handler(request):
        try:
            update = Update.de_json(await request.json(), app.bot)
            await app.update_queue.put(update)
            return web.Response(text="OK", status=200)
        except Exception as e:
            logger.error(f"Webhook handler: {e}")
            return web.Response(text="Error", status=500)

    aio_app = web.Application()
    aio_app.router.add_get("/",         health)
    aio_app.router.add_get("/health",   health)
    aio_app.router.add_post("/webhook", webhook_handler)

    runner = web.AppRunner(aio_app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    print(f"Web server on port {port} — Bot v3 fully running!")

    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        print("Shutting down gracefully...")
        await app.stop()
        await app.shutdown()
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