"""
PsychologyBot v2
─────────────────────────────────────────────────────────────────────────────
Multi-symbol Bybit Demo trading bot controlled via Telegram.
Strategy : Multi-timeframe confluence (Daily + 4H + 1H)
Leverage : 10x on all trades
Position : 20% of account balance per symbol

KEY FIX v2: All Bybit API calls and pandas calculations run in a background
thread pool so they NEVER block the asyncio event loop. Telegram commands
respond instantly even while the bot is scanning indicators.
─────────────────────────────────────────────────────────────────────────────
"""

import os
import json
import asyncio
import logging
import time
import concurrent.futures
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
import pandas_ta as ta
import numpy as np
from dotenv import load_dotenv
from pybit.unified_trading import HTTP
from aiohttp import web
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# ─────────────────────────────────────────────────────────────────────────────
#  THREAD POOL — all blocking calls (Bybit REST + pandas) run here
#  This is the core fix: keeps asyncio event loop free for Telegram commands
# ─────────────────────────────────────────────────────────────────────────────
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=16)
_api_semaphore: Optional[asyncio.Semaphore] = None   # init after event loop starts

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────
load_dotenv()

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN")
OWNER_ID         = int(os.getenv("CHAT_ID", "0"))
BYBIT_KEY        = os.getenv("BYBIT_API_KEY")
BYBIT_SECRET     = os.getenv("BYBIT_API_SECRET")
STATE_FILE       = os.getenv("STATE_FILE", "/opt/render/project/src/bot_state.json")

LEVERAGE         = 10
POSITION_PCT     = 0.20       # 20% of balance per symbol
MONITOR_INTERVAL = 10         # seconds per monitoring cycle

# Indicator params
ADX_PERIOD    = 14
RSI_PERIOD    = 14
EMA_PERIODS   = [20, 50, 100, 200]
MACD_FAST, MACD_SLOW, MACD_SIG = 12, 26, 9
ST_PERIOD     = 10
ST_MULT       = 3.0
ICHI_CONV     = 9
ICHI_BASE     = 26
ICHI_SPAN     = 52

SL_BUFFER     = 0.02    # 2% breathing room on SL
DEAD_DAYS     = 7       # close dead trade after 7 days

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
bybit = HTTP(testnet=False, api_key=BYBIT_KEY, api_secret=BYBIT_SECRET)
bybit.endpoint = "https://api-demo.bybit.com"   # Demo Trading URL

# ─────────────────────────────────────────────────────────────────────────────
#  STATE
# ─────────────────────────────────────────────────────────────────────────────
def _default_state() -> dict:
    return {
        "symbols":        [],
        "positions":      {},
        "paused":         False,
        "daily_loss":     0.0,
        "weekly_loss":    0.0,
        "consec_losses":  0,
        "last_reset_day": "",
        "last_reset_week":"",
    }

def load_state() -> dict:
    try:
        with open(STATE_FILE) as f:
            s = json.load(f)
        for k, v in _default_state().items():
            s.setdefault(k, v)
        return s
    except Exception:
        return _default_state()

def save_state(state: dict) -> None:
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

# ─────────────────────────────────────────────────────────────────────────────
#  OWNER GUARD
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
#  SYNCHRONOUS BYBIT HELPERS — called via asyncio.to_thread()
#  NEVER call these directly from async code without to_thread()
# ─────────────────────────────────────────────────────────────────────────────
def _sync_get_balance() -> float:
    """Fetch USDT wallet balance (synchronous — run in thread)."""
    try:
        resp  = bybit.get_wallet_balance(accountType="UNIFIED", coin="USDT")
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
    """Fetch latest price (synchronous — run in thread)."""
    try:
        resp = bybit.get_tickers(category="linear", symbol=symbol)
        return float(resp["result"]["list"][0]["lastPrice"])
    except Exception as e:
        logger.error(f"_sync_get_price({symbol}): {e}")
        return 0.0

def _sync_get_klines(symbol: str, interval: str, limit: int = 300) -> Optional[pd.DataFrame]:
    """Fetch OHLCV candles (synchronous — run in thread)."""
    try:
        resp = bybit.get_kline(category="linear", symbol=symbol,
                               interval=interval, limit=limit)
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
        resp = bybit.get_instruments_info(category="linear", symbol=symbol)
        lot  = resp["result"]["list"][0]["lotSizeFilter"]
        return float(lot["minOrderQty"])
    except Exception:
        return 0.001

def _sync_set_leverage(symbol: str) -> None:
    try:
        bybit.set_leverage(category="linear", symbol=symbol,
                           buyLeverage=str(LEVERAGE), sellLeverage=str(LEVERAGE))
    except Exception as e:
        logger.warning(f"set_leverage({symbol}): {e}")

def _sync_place_order(symbol: str, side: str, qty: float,
                      reduce_only: bool = False) -> Optional[dict]:
    try:
        resp = bybit.place_order(
            category="linear", symbol=symbol,
            side=side, orderType="Market", qty=str(qty),
            timeInForce="GoodTillCancel", reduceOnly=reduce_only,
        )
        return resp["result"]
    except Exception as e:
        logger.error(f"_sync_place_order({symbol},{side},{qty}): {e}")
        return None

def _sync_set_sl(symbol: str, sl_price: float) -> None:
    try:
        bybit.set_trading_stop(
            category="linear", symbol=symbol,
            stopLoss=str(round(sl_price, 6)), positionIdx=0,
        )
    except Exception as e:
        logger.error(f"_sync_set_sl({symbol},{sl_price}): {e}")

def _sync_close_position(symbol: str, direction: str) -> None:
    try:
        close_side = "Sell" if direction == "LONG" else "Buy"
        resp = bybit.get_positions(category="linear", symbol=symbol)
        for pos in resp["result"]["list"]:
            size = float(pos.get("size", 0))
            if size > 0:
                _sync_place_order(symbol, close_side, size, reduce_only=True)
                return
    except Exception as e:
        logger.error(f"_sync_close_position({symbol}): {e}")

# ─────────────────────────────────────────────────────────────────────────────
#  ASYNC WRAPPERS — these are safe to await from the event loop
# ─────────────────────────────────────────────────────────────────────────────
async def get_balance() -> float:
    async with _api_semaphore:
        return await asyncio.to_thread(_sync_get_balance)

async def get_price(symbol: str) -> float:
    async with _api_semaphore:
        return await asyncio.to_thread(_sync_get_price, symbol)

async def get_klines(symbol: str, interval: str,
                     limit: int = 300) -> Optional[pd.DataFrame]:
    async with _api_semaphore:
        return await asyncio.to_thread(_sync_get_klines, symbol, interval, limit)

async def set_leverage(symbol: str) -> None:
    async with _api_semaphore:
        await asyncio.to_thread(_sync_set_leverage, symbol)

async def place_order(symbol: str, side: str, qty: float,
                      reduce_only: bool = False) -> Optional[dict]:
    async with _api_semaphore:
        return await asyncio.to_thread(_sync_place_order, symbol, side, qty, reduce_only)

async def set_sl(symbol: str, sl_price: float) -> None:
    async with _api_semaphore:
        await asyncio.to_thread(_sync_set_sl, symbol, sl_price)

async def close_position(symbol: str, direction: str) -> None:
    async with _api_semaphore:
        await asyncio.to_thread(_sync_close_position, symbol, direction)

async def get_min_qty(symbol: str) -> float:
    async with _api_semaphore:
        return await asyncio.to_thread(_sync_get_min_qty, symbol)

# ─────────────────────────────────────────────────────────────────────────────
#  INDICATOR ENGINE — all heavy pandas work runs in thread via to_thread()
# ─────────────────────────────────────────────────────────────────────────────
def _compute_all_indicators(
    df_1d: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_1h: pd.DataFrame,
    direction: str,
) -> dict:
    """
    Pure synchronous function — heavy pandas math.
    Called via asyncio.to_thread() so it never blocks the event loop.
    Returns a dict with all filter results and raw values.
    """
    passed  = []
    failed  = []
    debug   = {}
    price   = float(df_1d["close"].iloc[-1])
    debug["price"] = price

    # ── Helper: EMA ───────────────────────────────────────────────────────────
    def ema(df: pd.DataFrame, p: int) -> pd.Series:
        return df["close"].ewm(span=p, adjust=False).mean()

    # ── Filter 1: ADX > 20 (4H) ──────────────────────────────────────────────
    try:
        adx_df = ta.adx(df_4h["high"], df_4h["low"], df_4h["close"],
                        length=ADX_PERIOD)
        adx = float(adx_df[f"ADX_{ADX_PERIOD}"].iloc[-1])
    except Exception:
        adx = 0.0
    debug["adx"] = round(adx, 2)
    if adx > 20:
        passed.append("ADX>20")
    else:
        failed.append("ADX>20")

    # ── Filter 2: Ichimoku Cloud (Daily) ─────────────────────────────────────
    conv  = (df_1d["high"].rolling(ICHI_CONV).max()  + df_1d["low"].rolling(ICHI_CONV).min())  / 2
    base  = (df_1d["high"].rolling(ICHI_BASE).max()  + df_1d["low"].rolling(ICHI_BASE).min())  / 2
    spa   = ((conv + base) / 2).shift(ICHI_BASE)
    spb   = ((df_1d["high"].rolling(ICHI_SPAN).max() + df_1d["low"].rolling(ICHI_SPAN).min()) / 2).shift(ICHI_BASE)
    cloud_top = max(float(spa.iloc[-1]), float(spb.iloc[-1]))
    cloud_bot = min(float(spa.iloc[-1]), float(spb.iloc[-1]))
    debug["cloud_top"] = round(cloud_top, 2)
    debug["cloud_bot"] = round(cloud_bot, 2)
    if direction == "LONG":
        ichi_ok = price > cloud_top; lbl = "IchiAboveCloud"
    else:
        ichi_ok = price < cloud_bot; lbl = "IchiBelowCloud"
    (passed if ichi_ok else failed).append(lbl)

    # ── Filter 3: RSI not extreme (4H) ───────────────────────────────────────
    rsi_s = ta.rsi(df_4h["close"], length=RSI_PERIOD)
    rsi   = float(rsi_s.iloc[-1]) if rsi_s is not None else 50.0
    debug["rsi_4h"] = round(rsi, 2)
    if direction == "LONG":
        rsi_ok = rsi <= 70; lbl = "RSI<=70"
    else:
        rsi_ok = rsi >= 30; lbl = "RSI>=30"
    (passed if rsi_ok else failed).append(lbl)

    # ── Filter 4: Supertrend (4H) ─────────────────────────────────────────────
    try:
        st = ta.supertrend(df_4h["high"], df_4h["low"], df_4h["close"],
                           length=ST_PERIOD, multiplier=ST_MULT)
        st_dir = int(st[f"SUPERTd_{ST_PERIOD}_{ST_MULT}"].iloc[-1])
    except Exception:
        st_dir = 0
    debug["supertrend_4h"] = "BUY" if st_dir == 1 else "SELL"
    if direction == "LONG":
        st_ok = st_dir == 1; lbl = "SupertrendBUY"
    else:
        st_ok = st_dir == -1; lbl = "SupertrendSELL"
    (passed if st_ok else failed).append(lbl)

    # ── Filter 5 (MANDATORY): EMA alignment (Daily) ──────────────────────────
    e20  = float(ema(df_1d, 20).iloc[-1])
    e50  = float(ema(df_1d, 50).iloc[-1])
    e100 = float(ema(df_1d, 100).iloc[-1])
    e200 = float(ema(df_1d, 200).iloc[-1])
    debug["emas_daily"] = {20: round(e20,2), 50: round(e50,2),
                           100: round(e100,2), 200: round(e200,2)}
    if direction == "LONG":
        ema_ok = (e20 > e50 > e100 > e200) and (price > e50)
        lbl = "EMA_Bullish_Stack"
    else:
        ema_ok = (e20 < e50 < e100 < e200) and (price < e50)
        lbl = "EMA_Bearish_Stack"
    (passed if ema_ok else failed).append(lbl)

    # ── Filter 6: MACD (Daily) ────────────────────────────────────────────────
    macd = ta.macd(df_1d["close"], fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIG)
    if macd is not None:
        m   = float(macd[f"MACD_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"].iloc[-1])
        sig = float(macd[f"MACDs_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"].iloc[-1])
        h   = float(macd[f"MACDh_{MACD_FAST}_{MACD_SLOW}_{MACD_SIG}"].iloc[-1])
    else:
        m = sig = h = 0.0
    debug["macd"] = {"line": round(m,4), "signal": round(sig,4), "hist": round(h,4)}
    if direction == "LONG":
        macd_ok = (m > sig) and (h > 0); lbl = "MACD_Bullish"
    else:
        macd_ok = (m < sig) and (h < 0); lbl = "MACD_Bearish"
    (passed if macd_ok else failed).append(lbl)

    # ── Filter 7: Pullback to 4H EMA 50 or 100 ───────────────────────────────
    e50_4h  = float(ema(df_4h, 50).iloc[-1])
    e100_4h = float(ema(df_4h, 100).iloc[-1])
    debug["ema50_4h"]  = round(e50_4h, 2)
    debug["ema100_4h"] = round(e100_4h, 2)
    near_50  = abs(price - e50_4h)  / e50_4h  <= 0.015
    near_100 = abs(price - e100_4h) / e100_4h <= 0.015
    pullback_ok = near_50 or near_100
    lbl = "PullbackToEMA" if direction == "LONG" else "RallyToEMA"
    (passed if pullback_ok else failed).append(lbl)

    # ── Filter 8 (MANDATORY): 1H candle ──────────────────────────────────────
    c = df_1h.iloc[-1]
    body = c["close"] - c["open"]
    rng  = c["high"] - c["low"]
    if rng == 0:
        candle_ok = False
    else:
        body_pct   = abs(body) / c["open"]
        close_pos  = (c["close"] - c["low"]) / rng  # 0=bottom, 1=top
        if direction == "LONG":
            candle_ok = body > 0 and body_pct >= 0.003 and close_pos >= 0.5
            lbl = "Bullish1HCandle"
        else:
            candle_ok = body < 0 and body_pct >= 0.003 and close_pos <= 0.5
            lbl = "Bearish1HCandle"
    (passed if candle_ok else failed).append(lbl)

    # ── Check entry conditions ────────────────────────────────────────────────
    m5_name = "EMA_Bullish_Stack" if direction == "LONG" else "EMA_Bearish_Stack"
    m8_name = "Bullish1HCandle"   if direction == "LONG" else "Bearish1HCandle"
    optional = [p for p in passed if p not in (m5_name, m8_name)]
    should_enter = (m5_name in passed) and (m8_name in passed) and len(optional) >= 3

    return {
        "should_enter": should_enter,
        "score":        len(passed),
        "passed":       passed,
        "failed":       failed,
        "debug":        debug,
        "ema_daily":    {"e20": e20, "e50": e50, "e100": e100, "e200": e200},
        "e50_4h":       e50_4h,
        "e100_4h":      e100_4h,
    }

def _compute_ema_exit(df_1d: pd.DataFrame, direction: str) -> bool:
    """Check EMA inversion on completed Daily candles (synchronous)."""
    try:
        df = df_1d.iloc[:-1]   # exclude forming candle
        def ema(p): return float(df["close"].ewm(span=p, adjust=False).mean().iloc[-1])
        e20, e50, e100, e200 = ema(20), ema(50), ema(100), ema(200)
        if direction == "LONG":
            return (e20 < e50) or (e50 < e100) or (e100 < e200)
        else:
            return (e20 > e50) or (e50 > e100) or (e100 > e200)
    except Exception:
        return False

def _compute_trailing_sl(df_4h: pd.DataFrame, direction: str, cur_sl: float) -> Optional[float]:
    """Compute new trailing SL from last completed 4H candle (synchronous)."""
    try:
        last = df_4h.iloc[-2]
        if direction == "LONG":
            candidate = float(last["low"])
            return round(candidate, 6) if candidate > cur_sl else None
        else:
            candidate = float(last["high"])
            return round(candidate, 6) if candidate < cur_sl else None
    except Exception:
        return None

# ─────────────────────────────────────────────────────────────────────────────
#  POSITION SIZING
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

def _estimate_pnl(pos: dict, price: float) -> float:
    diff = (price - pos["entry_price"]) if pos["direction"] == "LONG" \
           else (pos["entry_price"] - price)
    return diff * pos["remaining_qty"] * LEVERAGE

# ─────────────────────────────────────────────────────────────────────────────
#  TELEGRAM ALERT
# ─────────────────────────────────────────────────────────────────────────────
async def alert(app, text: str) -> None:
    try:
        await app.bot.send_message(chat_id=OWNER_ID, text=text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"alert: {e}")

# ─────────────────────────────────────────────────────────────────────────────
#  TRADE ENTRY
# ─────────────────────────────────────────────────────────────────────────────
async def open_trade(symbol: str, direction: str, indicators: dict,
                     state: dict, app) -> None:
    await set_leverage(symbol)

    entry = await get_price(symbol)
    if not entry:
        await alert(app, f"⚠️ [{symbol}] Cannot fetch price. Skipping.")
        return

    balance = await get_balance()
    if not balance:
        await alert(app, f"⚠️ [{symbol}] Cannot fetch balance. Skipping.")
        return

    e100_4h = indicators["e100_4h"]
    sl       = _calc_sl(entry, direction, e100_4h)
    tp1, tp2 = _calc_tps(entry, sl, direction)

    min_q   = await get_min_qty(symbol)
    notional = balance * POSITION_PCT * LEVERAGE
    qty      = _round_qty(notional / entry, min_q)

    if qty <= 0:
        await alert(app, f"⚠️ [{symbol}] Position size too small.")
        return

    side   = "Buy" if direction == "LONG" else "Sell"
    result = await place_order(symbol, side, qty)
    if not result:
        await alert(app, f"⚠️ [{symbol}] Order failed.")
        return

    await set_sl(symbol, sl)

    pos = {
        "symbol":         symbol,
        "direction":      direction,
        "entry_price":    entry,
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
    state["positions"][symbol] = pos
    save_state(state)

    emoji  = "🟢" if direction == "LONG" else "🔴"
    f_str  = " ".join([f"✅{p}" for p in indicators["passed"]])
    msg = (
        f"{emoji} <b>TRADE OPENED — {symbol} {direction}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 Entry : <code>{entry:.4f}</code>\n"
        f"🛡 SL    : <code>{sl:.4f}</code>\n"
        f"🎯 TP1   : <code>{tp1:.4f}</code>  (close 40%)\n"
        f"🎯 TP2   : <code>{tp2:.4f}</code>  (close 40%)\n"
        f"🌊 TP3   : Trailing last 20%\n"
        f"📊 Score : {indicators['score']}/8\n"
        f"✅ Filters: {f_str}\n"
        f"⚡ Qty   : {qty}  |  Leverage: {LEVERAGE}x"
    )
    await alert(app, msg)
    logger.info(f"[{symbol}] Trade opened {direction} @ {entry}")

# ─────────────────────────────────────────────────────────────────────────────
#  POSITION MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────
async def manage_position(pos: dict, state: dict, app) -> None:
    symbol    = pos["symbol"]
    direction = pos["direction"]

    # All heavy work in one thread call
    price, df_1d, df_4h = await asyncio.gather(
        asyncio.to_thread(_sync_get_price, symbol),
        asyncio.to_thread(_sync_get_klines, symbol, "D", 100),
        asyncio.to_thread(_sync_get_klines, symbol, "240", 50),
    )

    if not price or df_1d is None or df_4h is None:
        return

    # ── Dead trade ───────────────────────────────────────────────────────────
    if not pos["tp1_hit"] and (time.time() - pos["entry_time"]) / 86400 >= DEAD_DAYS:
        await close_position(symbol, direction)
        pnl = _estimate_pnl(pos, price)
        await alert(app, f"⏱ <b>DEAD TRADE CLOSED — {symbol}</b>\nNo TP1 after {DEAD_DAYS} days.\nEst. PnL: {pnl:+.2f} USDT")
        _remove_pos(symbol, state, pnl)
        return

    # ── EMA exit ─────────────────────────────────────────────────────────────
    ema_exit = await asyncio.to_thread(_compute_ema_exit, df_1d, direction)
    if ema_exit:
        await close_position(symbol, direction)
        pnl = _estimate_pnl(pos, price)
        await alert(app, f"📉 <b>EMA EXIT — {symbol} {direction}</b>\nEMA structure inverted on Daily.\nClosed @ <code>{price:.4f}</code>\nEst. PnL: {pnl:+.2f} USDT")
        _remove_pos(symbol, state, pnl)
        return

    # ── SL hit ───────────────────────────────────────────────────────────────
    sl = pos["sl_price"]
    sl_hit = (direction == "LONG" and price <= sl) or \
             (direction == "SHORT" and price >= sl)
    if sl_hit:
        await close_position(symbol, direction)
        pnl = _estimate_pnl(pos, price)
        state["consec_losses"] += 1
        await alert(app, f"🛑 <b>STOP LOSS HIT — {symbol} {direction}</b>\nSL: <code>{sl:.4f}</code>  Price: <code>{price:.4f}</code>\nEst. PnL: {pnl:+.2f} USDT\nConsecutive losses: {state['consec_losses']}")
        _remove_pos(symbol, state, pnl)
        return

    # ── TP1 ──────────────────────────────────────────────────────────────────
    if not pos["tp1_hit"]:
        tp1_hit = (direction == "LONG"  and price >= pos["tp1_price"]) or \
                  (direction == "SHORT" and price <= pos["tp1_price"])
        if tp1_hit:
            close_qty = _round_qty(pos["original_qty"] * 0.40,
                                   await get_min_qty(symbol))
            await place_order(symbol, "Sell" if direction=="LONG" else "Buy",
                              close_qty, reduce_only=True)
            pos["remaining_qty"] = round(pos["original_qty"] * 0.60, 8)
            pos["tp1_hit"]  = True
            pos["sl_price"] = pos["entry_price"]   # breakeven
            await set_sl(symbol, pos["entry_price"])
            state["consec_losses"] = 0
            save_state(state)
            pnl = abs(pos["tp1_price"] - pos["entry_price"]) * close_qty * LEVERAGE
            await alert(app, f"🎯 <b>TP1 HIT — {symbol} {direction}</b>\nClosed 40% @ <code>{price:.4f}</code>\nEst. PnL: +{pnl:.2f} USDT\n🔒 SL → Breakeven <code>{pos['entry_price']:.4f}</code>")
            return

    # ── TP2 ──────────────────────────────────────────────────────────────────
    if pos["tp1_hit"] and not pos["tp2_hit"]:
        tp2_hit = (direction == "LONG"  and price >= pos["tp2_price"]) or \
                  (direction == "SHORT" and price <= pos["tp2_price"])
        if tp2_hit:
            close_qty = _round_qty(pos["original_qty"] * 0.40,
                                   await get_min_qty(symbol))
            await place_order(symbol, "Sell" if direction=="LONG" else "Buy",
                              close_qty, reduce_only=True)
            pos["remaining_qty"] = round(pos["original_qty"] * 0.20, 8)
            pos["tp2_hit"] = True
            save_state(state)
            pnl = abs(pos["tp2_price"] - pos["entry_price"]) * close_qty * LEVERAGE
            await alert(app, f"🎯 <b>TP2 HIT — {symbol} {direction}</b>\nClosed 40% @ <code>{price:.4f}</code>\nEst. PnL: +{pnl:.2f} USDT\n🌊 Trailing last 20%")
            return

    # ── Trailing SL (after TP2) ───────────────────────────────────────────────
    if pos["tp2_hit"]:
        new_sl = await asyncio.to_thread(
            _compute_trailing_sl, df_4h, direction, pos["sl_price"])
        if new_sl:
            pos["sl_price"] = new_sl
            await set_sl(symbol, new_sl)
            save_state(state)
            logger.info(f"[{symbol}] Trailing SL → {new_sl}")

def _remove_pos(symbol: str, state: dict, pnl: float) -> None:
    state["daily_loss"]  = state.get("daily_loss", 0) + min(pnl, 0)
    state["weekly_loss"] = state.get("weekly_loss", 0) + min(pnl, 0)
    state["positions"].pop(symbol, None)
    save_state(state)

# ─────────────────────────────────────────────────────────────────────────────
#  MAIN MONITOR LOOP
# ─────────────────────────────────────────────────────────────────────────────
async def monitor_loop(app) -> None:
    global _api_semaphore
    _api_semaphore = asyncio.Semaphore(5)   # max 5 concurrent Bybit calls
    logger.info("Monitor loop started.")

    while True:
        try:
            state = load_state()
            now   = datetime.now(timezone.utc)

            # Reset daily/weekly counters
            today    = now.strftime("%Y-%m-%d")
            iso_week = now.strftime("%Y-W%W")
            if state.get("last_reset_day") != today:
                state["daily_loss"] = 0.0
                state["last_reset_day"] = today
            if state.get("last_reset_week") != iso_week:
                state["weekly_loss"] = 0.0
                state["last_reset_week"] = iso_week

            # Circuit breakers — fetch balance ONCE
            balance = await get_balance()
            paused_circuit = (
                state["daily_loss"]  <= -0.04 * balance or
                state["weekly_loss"] <= -0.06 * balance or
                state["consec_losses"] >= 3
            )

            # Manage open positions concurrently
            if state["positions"]:
                await asyncio.gather(*[
                    manage_position(pos, state, app)
                    for pos in list(state["positions"].values())
                ])

            # Scan for new entries
            if not state["paused"] and not paused_circuit:
                for symbol in state["symbols"]:
                    if symbol in state["positions"]:
                        continue

                    # Fetch all candles concurrently
                    df_1d, df_4h, df_1h = await asyncio.gather(
                        get_klines(symbol, "D",   300),
                        get_klines(symbol, "240", 300),
                        get_klines(symbol, "60",  100),
                    )
                    if df_1d is None or df_4h is None or df_1h is None:
                        continue

                    # Compute indicators in background thread
                    for direction in ("LONG", "SHORT"):
                        indicators = await asyncio.to_thread(
                            _compute_all_indicators, df_1d, df_4h, df_1h, direction)
                        if indicators["should_enter"]:
                            logger.info(f"[{symbol}] {direction} signal (score {indicators['score']})")
                            await open_trade(symbol, direction, indicators, state, app)
                            break   # one trade per symbol per cycle

            save_state(state)

        except Exception as e:
            logger.error(f"Monitor loop error: {e}", exc_info=True)

        await asyncio.sleep(MONITOR_INTERVAL)

# ─────────────────────────────────────────────────────────────────────────────
#  TELEGRAM COMMAND HANDLERS
# ─────────────────────────────────────────────────────────────────────────────
@owner_only
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "🤖 <b>PsychologyBot v2 — Ready!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "<b>Commands:</b>\n"
        "• /start               — Show this message\n"
        "• /add_symbol &lt;SYMBOL&gt;   — Monitor a coin\n"
        "• /remove_symbol &lt;SYMBOL&gt; — Stop monitoring\n"
        "• /status              — Open positions &amp; scores\n"
        "• /pause               — Pause new entries\n"
        "• /resume              — Resume new entries\n"
        "• /balance             — Show USDT balance\n\n"
        "<b>Strategy:</b>\n"
        "• Daily + 4H + 1H timeframes\n"
        "• 10x leverage  |  20% balance per trade\n"
        "• Min 5/8 signals (EMA stack + 1H candle mandatory)\n"
        "• TP1: 1:1.5 RR → 40%  |  TP2: 1:3 RR → 40%  |  TP3: Trailing\n\n"
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
    state  = load_state()
    if symbol in state["symbols"]:
        await update.message.reply_text(f"⚠️ {symbol} already monitored.")
        return
    state["symbols"].append(symbol)
    save_state(state)
    await update.message.reply_text(
        f"✅ <b>{symbol}</b> added. Bot will scan every {MONITOR_INTERVAL}s.",
        parse_mode="HTML")

@owner_only
async def cmd_remove_symbol(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not ctx.args:
        await update.message.reply_text("Usage: /remove_symbol BTCUSDT")
        return
    symbol = ctx.args[0].upper()
    state  = load_state()
    if symbol not in state["symbols"]:
        await update.message.reply_text(f"⚠️ {symbol} not in watchlist.")
        return
    state["symbols"].remove(symbol)
    if symbol in state["positions"]:
        await update.message.reply_text(
            f"⚠️ {symbol} has an open position — removed from monitoring "
            f"but position still open on Bybit. Manage manually.")
    save_state(state)
    await update.message.reply_text(f"✅ <b>{symbol}</b> removed.", parse_mode="HTML")

@owner_only
async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    state   = load_state()
    balance = await get_balance()
    lines   = [
        "📊 <b>Bot Status</b>",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"Balance  : <code>{balance:.2f} USDT</code>",
        f"Paused   : {'🔴 YES' if state['paused'] else '🟢 NO'}",
        f"Con. losses: {state['consec_losses']}",
        f"Daily loss : {state['daily_loss']:.2f} USDT",
        f"",
        f"<b>Watching:</b> {', '.join(state['symbols']) or 'None'}",
        "",
    ]
    if state["positions"]:
        lines.append("<b>Open Positions:</b>")
        for sym, pos in state["positions"].items():
            price = await get_price(sym)
            pnl   = _estimate_pnl(pos, price)
            days  = (time.time() - pos["entry_time"]) / 86400
            e     = "🟢" if pos["direction"] == "LONG" else "🔴"
            lines.append(
                f"{e} <b>{sym}</b> {pos['direction']}\n"
                f"   Entry:{pos['entry_price']:.4f}  Now:{price:.4f}\n"
                f"   SL:{pos['sl_price']:.4f}\n"
                f"   TP1:{'✅' if pos['tp1_hit'] else '⬜'}  TP2:{'✅' if pos['tp2_hit'] else '⬜'}\n"
                f"   PnL:{pnl:+.2f} USDT  Open:{days:.1f}d  Score:{pos['score']}/8"
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
        "⏸ <b>Bot paused.</b> No new entries.\nExisting positions still managed.",
        parse_mode="HTML")

@owner_only
async def cmd_resume(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    state = load_state()
    state["paused"] = False
    state["consec_losses"] = 0
    save_state(state)
    await update.message.reply_text("▶️ <b>Bot resumed.</b>", parse_mode="HTML")

@owner_only
async def cmd_balance(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    bal = await get_balance()
    await update.message.reply_text(
        f"💰 Balance: <code>{bal:.2f} USDT</code>", parse_mode="HTML")

# ─────────────────────────────────────────────────────────────────────────────
#  STARTUP — Webhook + aiohttp (Render Web Service)
# ─────────────────────────────────────────────────────────────────────────────
async def main() -> None:
    global _api_semaphore
    _api_semaphore = asyncio.Semaphore(5)

    # Set thread pool as default executor
    loop = asyncio.get_running_loop()
    loop.set_default_executor(_executor)
    logger.info(f"Thread pool: {_executor._max_workers} workers")

    logger.info("Starting PsychologyBot v2...")

    app = Application.builder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start",          cmd_start))
    app.add_handler(CommandHandler("add_symbol",     cmd_add_symbol))
    app.add_handler(CommandHandler("remove_symbol",  cmd_remove_symbol))
    app.add_handler(CommandHandler("status",         cmd_status))
    app.add_handler(CommandHandler("pause",          cmd_pause))
    app.add_handler(CommandHandler("resume",         cmd_resume))
    app.add_handler(CommandHandler("balance",        cmd_balance))

    base_url    = os.environ.get("RENDER_EXTERNAL_HOSTNAME",
                                 "psychology-trading-bot.onrender.com")
    webhook_url = f"https://{base_url}/webhook"
    port        = int(os.environ.get("PORT", 10000))

    await app.bot.set_webhook(url=webhook_url, drop_pending_updates=True)
    logger.info(f"Webhook → {webhook_url}")

    await app.initialize()
    await app.start()

    # Start monitor loop as background task
    asyncio.create_task(monitor_loop(app))
    logger.info("Monitor loop task created.")

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
    logger.info(f"Web server on port {port}. Bot fully running.")

    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("Shutting down...")
        await app.stop()
        await app.shutdown()
        await runner.cleanup()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by user.")
    except Exception as e:
        logger.error(f"Fatal: {e}", exc_info=True)