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

TELEGRAM_TOKEN     = os.getenv("TELEGRAM_TOKEN")
BYBIT_KEY          = os.getenv("BYBIT_API_KEY")
BYBIT_SECRET       = os.getenv("BYBIT_API_SECRET")
CHAT_ID_STR        = os.getenv("CHAT_ID")
# Render persistent disk mounts at /var/data by default.
# If you add a Render Disk (Dashboard → your service → Disks → Add Disk,
# mount path: /var/data), state survives redeploys automatically.
# Without a disk: state is lost on redeploy (free tier limitation).
_DEFAULT_STATE_DIR = "/var/data" if os.path.isdir("/var/data") else "/opt/render/project/src"
STATE_FILE         = os.getenv("STATE_FILE", f"{_DEFAULT_STATE_DIR}/bot_state.json")
CRYPTOPANIC_TOKEN  = os.getenv("CRYPTOPANIC_TOKEN", "")   # Free at cryptopanic.com/developers/api/keys

if not all([TELEGRAM_TOKEN, BYBIT_KEY, BYBIT_SECRET, CHAT_ID_STR]):
    raise ValueError("Missing required environment variables! Check .env or Render Environment Variables.")

try:
    CHAT_ID = int(CHAT_ID_STR)
except ValueError:
    raise ValueError(f"CHAT_ID must be an integer, got: {CHAT_ID_STR}")

# ── PsychologyBot trading params ──────────────────────────────────────────
LEVERAGE         = 10
POSITION_PCT     = 0.20     # 20% of balance per symbol
POLL_INTERVAL    = 10       # seconds between monitor cycles
DEAD_TRADE_DAYS  = 7        # close dead trade after 7 days
SL_BUFFER        = 0.02     # 2% breathing room on initial SL

# ── MemeBot trading params (volatile coins — 5m/3m timeframes) ────────────
MEME_LEVERAGE        = 5        # Hard cap 5x — memecoins wick massively
MEME_POSITION_PCT    = 0.08     # 8% of balance — smaller, safer
MEME_POLL_INTERVAL   = 10       # seconds between monitor cycles
MEME_DEAD_MINUTES    = 30       # close dead trade after 30 minutes
MEME_MAX_POSITIONS   = 2        # max simultaneous memecoin positions
MEME_VOL_MULT        = 3.0      # volume spike: 3× 20-period average
MEME_RSI_LONG_MIN    = 45       # RSI floor for long entries
MEME_RSI_LONG_MAX    = 75       # RSI ceiling for long entries
MEME_RSI_SHORT_MIN   = 25       # RSI floor for short entries
MEME_RSI_SHORT_MAX   = 55       # RSI ceiling for short entries
MEME_RSI_EXIT_HIGH   = 85       # RSI emergency exit (long overbought)
MEME_RSI_EXIT_LOW    = 15       # RSI emergency exit (short oversold)
MEME_TP1_RR          = 1.0      # TP1 at 1:1 RR → close 50%
MEME_TP2_RR          = 2.0      # TP2 at 1:2 RR → close 30%
MEME_SL_ATR_MULT     = 1.5      # SL at 1.5× ATR(14) on 5m chart

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
# ── PsychologyBot global state ────────────────────────────────────────────
monitored_symbols: list[str]              = []
symbol_positions:  dict[str, dict]        = {}
bot_paused:        bool                   = False
daily_loss:        float                  = 0.0
weekly_loss:       float                  = 0.0
consec_losses:     int                    = 0
last_reset_day:    str                    = ""
last_reset_week:   str                    = ""

# ── MemeBot global state ───────────────────────────────────────────────────
meme_symbols:       list[str]             = []
meme_positions:     dict[str, dict]       = {}   # symbol -> memecoin position
meme_paused:        bool                  = False
meme_daily_loss:    float                 = 0.0
meme_consec_losses: int                   = 0
meme_monitor_running: dict[str, bool]     = {}   # per-symbol execution guard

# ── NewsBot global state ───────────────────────────────────────────────────────
news_seen_ids:      set                   = set()   # dedup — never alert same news twice
news_enabled:       bool                  = True    # can be paused with /news_pause
NEWS_POLL_INTERVAL: int                   = 120     # check every 2 minutes
NEWS_MAX_AGE_HOURS: int                   = 1       # ignore news older than 1 hour

# ── Trade history global state ─────────────────────────────────────────────────
HISTORY_FILE      = STATE_FILE.replace("bot_state.json", "trade_history.json")
HISTORY_KEEP_DAYS = 60    # keep last 60 days of trade records
trade_history:    list    = []   # list of closed trade dicts

def save_state() -> None:
    try:
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        state = {
            # PsychologyBot state
            "monitored_symbols": monitored_symbols,
            "symbol_positions":  symbol_positions,
            "bot_paused":        bot_paused,
            "daily_loss":        daily_loss,
            "weekly_loss":       weekly_loss,
            "consec_losses":     consec_losses,
            "last_reset_day":    last_reset_day,
            "last_reset_week":   last_reset_week,
            # MemeBot state
            "meme_symbols":      meme_symbols,
            "meme_positions":    meme_positions,
            "meme_paused":       meme_paused,
            "meme_daily_loss":   meme_daily_loss,
            "meme_consec_losses":meme_consec_losses,
        }
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2, default=str)
    except Exception as e:
        logger.error(f"[STATE] save failed: {e}")

def load_state() -> bool:
    global monitored_symbols, symbol_positions, bot_paused
    global daily_loss, weekly_loss, consec_losses, last_reset_day, last_reset_week
    global meme_symbols, meme_positions, meme_paused, meme_daily_loss, meme_consec_losses
    if not os.path.exists(STATE_FILE):
        logger.info("[STATE] No state file found — starting fresh")
        return False
    try:
        with open(STATE_FILE) as f:
            s = json.load(f)
        # PsychologyBot state
        monitored_symbols = s.get("monitored_symbols", [])
        symbol_positions  = s.get("symbol_positions",  {})
        bot_paused        = s.get("bot_paused",        False)
        daily_loss        = s.get("daily_loss",        0.0)
        weekly_loss       = s.get("weekly_loss",       0.0)
        consec_losses     = s.get("consec_losses",     0)
        last_reset_day    = s.get("last_reset_day",    "")
        last_reset_week   = s.get("last_reset_week",   "")
        # MemeBot state
        meme_symbols      = s.get("meme_symbols",      [])
        meme_positions    = s.get("meme_positions",    {})
        meme_paused       = s.get("meme_paused",       False)
        meme_daily_loss   = s.get("meme_daily_loss",   0.0)
        meme_consec_losses= s.get("meme_consec_losses",0)
        logger.info(f"[STATE] Loaded — psych:{len(monitored_symbols)} symbols, "
                    f"meme:{len(meme_symbols)} symbols")
        return True
    except Exception as e:
        logger.error(f"[STATE] load failed: {e}")
        return False

# ─────────────────────────────────────────────────────────────────────────────
#  TRADE HISTORY — persisted to a separate JSON file, 60-day rolling window
# ─────────────────────────────────────────────────────────────────────────────
def _load_history() -> None:
    """Load trade history from disk on startup."""
    global trade_history
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE) as f:
                trade_history = json.load(f)
            logger.info(f"[HISTORY] Loaded {len(trade_history)} trade records")
        else:
            trade_history = []
            logger.info("[HISTORY] No history file found — starting fresh")
    except Exception as e:
        logger.error(f"[HISTORY] load failed: {e}")
        trade_history = []

def _save_history() -> None:
    """Persist trade history to disk. Prunes records older than 60 days."""
    global trade_history
    try:
        from datetime import timezone as _tz, datetime as _dt, timedelta as _td
        cutoff = (_dt.now(_tz.utc) - _td(days=HISTORY_KEEP_DAYS)).isoformat()
        trade_history = [t for t in trade_history if t.get("close_time", "") >= cutoff]
        os.makedirs(os.path.dirname(HISTORY_FILE), exist_ok=True)
        with open(HISTORY_FILE, "w") as f:
            json.dump(trade_history, f, indent=2, default=str)
    except Exception as e:
        logger.error(f"[HISTORY] save failed: {e}")

def _record_trade(pos: dict, exit_price: float, pnl: float,
                  exit_reason: str, bot_type: str = "psych") -> None:
    """
    Record a completed trade to history.
    Called whenever a position is fully or partially closed.
    bot_type: 'psych' for PsychologyBot, 'meme' for MemeBot
    """
    from datetime import timezone as _tz, datetime as _dt
    entry_time = pos.get("entry_time", 0)
    duration_min = (time.time() - entry_time) / 60 if entry_time else 0

    record = {
        "id":           f"{pos['symbol']}_{int(entry_time)}",
        "bot":          bot_type,
        "symbol":       pos["symbol"],
        "direction":    pos["direction"],
        "entry_price":  round(pos.get("entry_price", 0), 6),
        "exit_price":   round(exit_price, 6),
        "sl_price":     round(pos.get("sl_price", 0), 6),
        "tp1_price":    round(pos.get("tp1_price", 0), 6),
        "tp2_price":    round(pos.get("tp2_price", 0), 6),
        "tp1_hit":      pos.get("tp1_hit", False),
        "tp2_hit":      pos.get("tp2_hit", False),
        "qty":          pos.get("original_qty", 0),
        "leverage":     pos.get("leverage", 10),
        "score":        pos.get("score", 0),
        "pnl":          round(pnl, 4),
        "pnl_pct":      round((pnl / (pos.get("entry_price", 1) * pos.get("original_qty", 1))) * 100, 2) if pos.get("entry_price") and pos.get("original_qty") else 0,
        "exit_reason":  exit_reason,   # "SL", "TP1", "TP2", "EMA_EXIT", "DEAD", "EMERGENCY", "MANUAL"
        "duration_min": round(duration_min, 1),
        "entry_time":   _dt.fromtimestamp(entry_time, tz=_tz.utc).isoformat() if entry_time else "",
        "close_time":   _dt.now(_tz.utc).isoformat(),
    }

    # Remove duplicate if re-recording same trade
    trade_history[:] = [t for t in trade_history if t["id"] != record["id"]]
    trade_history.append(record)
    _save_history()

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
    """Restrict trading/admin commands to the owner only."""
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        if update.effective_chat.id != CHAT_ID:
            await update.message.reply_text(
                "This command is restricted to the bot owner.\n"
                "Use /start to see what this bot does.")
            return
        return await func(update, ctx)
    wrapper.__name__ = func.__name__
    return wrapper

def public_command(func):
    """Allow any user to run this command — no restriction."""
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
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

def _sync_get_instrument_constraints(symbol: str) -> tuple:
    """Returns (min_qty, max_qty, qty_step, min_notional, max_leverage)."""
    try:
        resp = bybit_session.get_instruments_info(category="linear", symbol=symbol)
        info         = resp["result"]["list"][0]
        lot          = info["lotSizeFilter"]
        lever_filter = info.get("leverageFilter", {})
        min_qty      = float(lot["minOrderQty"])
        max_qty      = float(lot.get("maxMktOrderQty", lot.get("maxOrderQty", 1_000_000)))
        qty_step     = float(lot["qtyStep"])
        min_notional = float(lot.get("minNotionalValue", 5.0))
        max_leverage = float(lever_filter.get("maxLeverage", 10.0))
        logger.info(f"[CONSTRAINTS] {symbol}: min={min_qty} max={max_qty} step={qty_step} "
                    f"min_notional={min_notional} max_lev={max_leverage}")
        return min_qty, max_qty, qty_step, min_notional, max_leverage
    except Exception as e:
        logger.error(f"_sync_get_instrument_constraints({symbol}): {e}")
        return 0.001, 1_000_000.0, 0.001, 5.0, 10.0

def _sync_get_min_qty(symbol: str) -> float:
    min_qty, _, _, _, _ = _sync_get_instrument_constraints(symbol)
    return min_qty

def _round_qty_step(qty: float, qty_step: float, min_qty: float) -> float:
    """Round qty DOWN to nearest valid step, respecting minimum."""
    if qty_step > 0:
        qty = (qty // qty_step) * qty_step
    return max(min_qty, qty)

def _format_qty_str(qty: float, qty_step: float) -> str:
    """Format qty string with correct decimal precision from step size."""
    if qty_step >= 1:
        return str(int(qty))
    step_str = f"{qty_step:.10f}".rstrip("0")
    decimals = len(step_str.split(".")[1]) if "." in step_str else 0
    return f"{qty:.{decimals}f}"

def _sync_set_leverage(symbol: str) -> int:
    """
    Set leverage for symbol, strictly capped at the symbol's max allowed leverage.
    Uses floor() to stay safely under fractional limits (e.g. 9.5 → 9).
    Returns the actual leverage successfully set.
    """
    import math
    try:
        _, _, _, _, max_lev = _sync_get_instrument_constraints(symbol)
        # Hard cap: never exceed 10x OR the symbol's max, whichever is lower
        actual_lev = min(10, math.floor(max_lev))
        if actual_lev < 1:
            actual_lev = 1
        resp = bybit_session.set_leverage(
            category="linear", symbol=symbol,
            buyLeverage=str(actual_lev), sellLeverage=str(actual_lev))
        ret_code = resp.get("retCode", 0) if isinstance(resp, dict) else 0
        if ret_code not in (0, 110043):  # 110043 = leverage not modified (already set)
            logger.warning(f"[{symbol}] set_leverage retCode={ret_code}: {resp.get('retMsg','')}")
        if actual_lev < LEVERAGE:
            logger.warning(f"[{symbol}] Leverage capped: {actual_lev}x used "
                           f"(symbol max={max_lev}x, bot default={LEVERAGE}x)")
        else:
            logger.info(f"[{symbol}] Leverage set to {actual_lev}x")
        return actual_lev
    except Exception as e:
        logger.warning(f"set_leverage({symbol}): {e}")
        return 1   # safe fallback: 1x if we can't determine the max

def _sync_place_order(symbol: str, side: str, qty: float,
                      reduce_only: bool = False) -> Optional[dict]:
    """
    Place a market order. Automatically batches into multiple orders if qty
    exceeds maxMktOrderQty (learned from reference code to handle low-price coins).
    """
    try:
        min_qty, max_qty, qty_step, _, _ = _sync_get_instrument_constraints(symbol)
        total_qty = _round_qty_step(qty, qty_step, min_qty)
        remaining  = total_qty
        last_result = None

        while remaining > 0:
            batch_qty = min(remaining, max_qty)
            batch_qty = _round_qty_step(batch_qty, qty_step, min_qty)
            qty_str   = _format_qty_str(batch_qty, qty_step)

            resp = bybit_session.place_order(
                category="linear", symbol=symbol,
                side=side, orderType="Market", qty=qty_str,
                timeInForce="GoodTillCancel", reduceOnly=reduce_only)

            ret_code = resp.get("retCode", 0)
            if ret_code != 0:
                logger.error(f"_sync_place_order({symbol}): retCode={ret_code} msg={resp.get('retMsg')} qty={qty_str}")
                # 110090 = position exceeds max limit — reduce qty by 20% and retry once
                if ret_code == 110090 and batch_qty > min_qty:
                    reduced = _round_qty_step(batch_qty * 0.80, qty_step, min_qty)
                    if reduced < min_qty or reduced >= batch_qty:
                        return {"__error__": resp.get("retMsg", "Unknown error"), "__code__": ret_code}
                    logger.warning(f"_sync_place_order({symbol}): 110090 hit, retrying with reduced qty={reduced}")
                    qty_str2 = _format_qty_str(reduced, qty_step)
                    resp2 = bybit_session.place_order(
                        category="linear", symbol=symbol,
                        side=side, orderType="Market", qty=qty_str2,
                        timeInForce="GoodTillCancel", reduceOnly=reduce_only)
                    if resp2.get("retCode", 0) == 0:
                        last_result = resp2["result"]
                        remaining -= reduced
                        if remaining > 0:
                            import time as _time
                            _time.sleep(0.5)
                        continue
                return {"__error__": resp.get("retMsg", "Unknown error"), "__code__": ret_code}

            last_result = resp["result"]
            remaining  -= batch_qty

            if remaining > 0:
                import time as _time
                _time.sleep(0.5)   # small pause between batches

        return last_result
    except Exception as e:
        logger.error(f"_sync_place_order({symbol},{side},{qty}): {e}")
        return {"__error__": str(e)}

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

async def get_instrument_constraints(symbol: str) -> tuple:
    """Returns (min_qty, max_qty, qty_step, min_notional, max_leverage)."""
    async with _api_semaphore:
        return await asyncio.to_thread(_sync_get_instrument_constraints, symbol)

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
def _round_qty(qty: float, min_q: float, step: float = 0.0) -> float:
    """Round qty to step size, respecting minimum."""
    if step > 0:
        qty = (qty // step) * step
    return max(min_q, round(qty, 8))

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

    # Set leverage capped at symbol's max — get back actual leverage used
    actual_lev = await asyncio.to_thread(_sync_set_leverage, symbol)

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
    min_q, max_q, step, min_notional, max_lev = await get_instrument_constraints(symbol)
    # effective_lev is what was actually set on Bybit (already floored to symbol max)
    effective_lev = actual_lev

    # Position notional = qty × entry_price / leverage  (this is what Bybit checks)
    # We target 20% of balance as margin, so notional = margin × leverage
    target_margin   = balance * POSITION_PCT
    target_notional = target_margin * effective_lev
    raw_qty         = target_notional / entry
    raw_qty         = max(raw_qty, min_notional / entry)   # respect min notional

    # Cap qty so each individual batch order never exceeds maxMktOrderQty
    qty = _round_qty(raw_qty, min_q, step)

    logger.info(f"[{symbol}] Sizing: balance={balance:.2f} lev={effective_lev}x "
                f"target_notional={target_notional:.2f} raw_qty={raw_qty:.6f} "
                f"final_qty={qty} max_single_order={max_q}")

    if qty <= 0:
        await alert(bot, f"⚠️ [{symbol}] Position size too small.")
        return

    side   = "Buy" if direction == "LONG" else "Sell"
    result = await place_order(symbol, side, qty)
    if not result or "__error__" in result:
        err_msg = result.get("__error__", "Unknown") if result else "No response from Bybit"
        err_code = result.get("__code__", "") if result else ""
        hint = ""
        if "symbol" in str(err_msg).lower() or err_code == 10001:
            hint = "\n💡 Check the symbol name is correct on Bybit (e.g. BTCUSDT not BTCUSD)"
        elif err_code == 110090:
            hint = "\n💡 Position exceeds Bybit's max limit for this symbol — leverage was already capped"
        elif err_code in (110007, 110043):
            hint = "\n💡 Leverage or margin issue — check your Bybit account settings"
        elif "insufficient" in str(err_msg).lower():
            hint = "\n💡 Insufficient balance for this position size"
        await alert(bot,
            f"⚠️ <b>Order failed — {symbol} {direction}</b>\n"
            f"Reason: <code>{err_msg}</code>\n"
            f"Code: {err_code}{hint}\n"
            f"Bot goes FLAT on {symbol} and will retry next valid signal.")
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
        f"⚡ Qty    : {qty}  |  Leverage: {effective_lev}x  |  Balance: {balance:.2f} USDT"
        + (f"\n⚠️ Note: Leverage capped at {effective_lev}x (symbol max, requested {LEVERAGE}x)" if effective_lev < LEVERAGE else "")
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
        _close_pos(symbol, pnl, exit_price=current_price, exit_reason="DEAD")
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
        _close_pos(symbol, pnl, exit_price=current_price, exit_reason="EMA_EXIT")
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
        _close_pos(symbol, pnl, exit_price=current_price, exit_reason="SL")
        return

    # ── TP1 ──────────────────────────────────────────────────────────────────
    if not pos["tp1_hit"]:
        tp1_hit = (direction == "LONG"  and current_price >= pos["tp1_price"]) or \
                  (direction == "SHORT" and current_price <= pos["tp1_price"])
        if tp1_hit:
            min_q, _, step, _, _ = await get_instrument_constraints(symbol)
            close_qty = _round_qty(pos["original_qty"] * 0.40, min_q, step)
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
            min_q, _, step, _, _ = await get_instrument_constraints(symbol)
            close_qty = _round_qty(pos["original_qty"] * 0.40, min_q, step)
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

def _close_pos(symbol: str, pnl: float, exit_price: float = 0.0,
               exit_reason: str = "UNKNOWN") -> None:
    global daily_loss, weekly_loss
    daily_loss  += min(pnl, 0)
    weekly_loss += min(pnl, 0)
    pos = symbol_positions.get(symbol)
    if pos and exit_price > 0:
        _record_trade(pos, exit_price, pnl, exit_reason, bot_type="psych")
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
@public_command
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    is_owner = update.effective_chat.id == CHAT_ID
    await update.message.reply_text(
        "🤖 <b>PsychologyBot v3 — Ready!</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>Commands:</b>\n"
        "• /start               — Show this message (public)\n"
        "• /status              — Open positions &amp; scores (public)\n"
        "• /scan                — Live indicator scan (public)\n"
        "• /balance             — Show USDT balance (public)\n"
        "• /history             — Trade history (public)\n"
        "• /memebot             — MemeBot info (public)\n"
        "• /meme_status         — MemeBot positions (public)\n"
        "• /news_status         — NewsBot status (public)\n\n"
        "• /add_symbol &lt;SYM&gt;     — Monitor a coin 🔒 owner\n"
        "• /remove_symbol &lt;SYM&gt;  — Stop monitoring 🔒 owner\n"
        "• /pause / /resume     — Pause bot 🔒 owner\n"
        "• /add_meme &lt;SYM&gt;       — Add meme coin 🔒 owner\n\n"
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
        "• Public bot — anyone can check status and scan signals\n"
        "• Trading controls are owner-only for security\n\n"
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

    # Reply FIRST — user sees confirmation immediately

    # Non-blocking background tasks after reply
    await asyncio.to_thread(save_state)
    asyncio.create_task(asyncio.to_thread(subscribe_ws_symbol, symbol))

    idx   = monitored_symbols.index(symbol)
    delay = POLL_INTERVAL + idx * 1.25

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

@public_command
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

@public_command
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
            # Escape filter names so Telegram HTML parser doesn't choke on < > symbols
            def esc(s): return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
            p_str  = "\n   ".join([f"✅ {esc(p)}" for p in ind["passed"]]) or "none"
            f_str  = "\n   ".join([f"❌ {esc(p)}" for p in ind["failed"]]) or "none"
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

@public_command
async def cmd_balance(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    bal = await get_balance()
    await update.message.reply_text(
        f"💰 Balance: <code>{bal:.2f} USDT</code>", parse_mode="HTML")



# =============================================================================
#  MEMEBOT — Fast momentum strategy for highly volatile coins
#  Timeframes : 5m (entry) + 3m (trailing exit)
#  Leverage   : 5x hard cap | Position: 8% of balance
#  Filters    : Volume spike + RSI zone + VWAP + Candle body +
#               EMA micro-trend + No wick trap (ALL 6 required)
#  Exits      : TP1 1:1 (50%) -> TP2 1:2 (30%) -> Trail 20% on 3m
#  Emergency  : Volume collapse / wick spike / RSI extreme / 30min timeout
# =============================================================================

# ─────────────────────────────────────────────────────────────────────────────
#  MEMEBOT — SYNCHRONOUS INDICATOR ENGINE (runs in thread)
# ─────────────────────────────────────────────────────────────────────────────
def _meme_compute_indicators(df_5m: pd.DataFrame, direction: str) -> dict:
    """
    Compute all 6 MemeBot entry filters on 5-minute candles.
    Pure synchronous — called via asyncio.to_thread().
    """
    passed = []
    failed = []
    debug  = {}

    price  = float(df_5m["close"].iloc[-1])
    debug["price"] = price

    # Filter 1: Volume spike >= 3x 20-period average
    vol     = df_5m["volume"]
    avg_vol = float(vol.iloc[-21:-1].mean())
    cur_vol = float(vol.iloc[-2])
    vol_ratio = cur_vol / avg_vol if avg_vol > 0 else 0
    debug["vol_ratio"] = round(vol_ratio, 2)
    debug["avg_vol"]   = round(avg_vol, 2)
    lbl = f"VolSpike({vol_ratio:.1f}x)"
    (passed if vol_ratio >= MEME_VOL_MULT else failed).append(lbl)

    # Filter 2: RSI(7) in valid zone
    rsi_s = ta.rsi(df_5m["close"], length=7)
    rsi   = float(rsi_s.iloc[-1]) if rsi_s is not None and not rsi_s.isna().iloc[-1] else 50.0
    debug["rsi"] = round(rsi, 2)
    if direction == "LONG":
        rsi_ok = MEME_RSI_LONG_MIN <= rsi <= MEME_RSI_LONG_MAX
        lbl    = f"RSI({MEME_RSI_LONG_MIN}-{MEME_RSI_LONG_MAX})"
    else:
        rsi_ok = MEME_RSI_SHORT_MIN <= rsi <= MEME_RSI_SHORT_MAX
        lbl    = f"RSI({MEME_RSI_SHORT_MIN}-{MEME_RSI_SHORT_MAX})"
    (passed if rsi_ok else failed).append(lbl)

    # Filter 3: Price above/below VWAP
    tp_col   = (df_5m["high"] + df_5m["low"] + df_5m["close"]) / 3
    cum_vol  = df_5m["volume"].cumsum()
    vwap_val = float((tp_col * df_5m["volume"]).cumsum().iloc[-1] / cum_vol.iloc[-1]) if cum_vol.iloc[-1] > 0 else price
    debug["vwap"] = round(vwap_val, 6)
    if direction == "LONG":
        (passed if price > vwap_val else failed).append("AboveVWAP")
    else:
        (passed if price < vwap_val else failed).append("BelowVWAP")

    # Filter 4 (MANDATORY): Strong 5m candle body >= 60% of range
    c        = df_5m.iloc[-2]
    body     = abs(c["close"] - c["open"])
    rng      = c["high"] - c["low"]
    body_pct = body / rng if rng > 0 else 0
    debug["body_pct"] = round(body_pct, 3)
    if direction == "LONG":
        candle_ok = (c["close"] > c["open"]) and (body_pct >= 0.60)
        lbl = "BullishBody60pct"
    else:
        candle_ok = (c["close"] < c["open"]) and (body_pct >= 0.60)
        lbl = "BearishBody60pct"
    (passed if candle_ok else failed).append(lbl)

    # Filter 5 (MANDATORY): EMA9 above EMA21 (micro-trend)
    ema9  = float(df_5m["close"].ewm(span=9,  adjust=False).mean().iloc[-1])
    ema21 = float(df_5m["close"].ewm(span=21, adjust=False).mean().iloc[-1])
    debug["ema9"]  = round(ema9,  6)
    debug["ema21"] = round(ema21, 6)
    if direction == "LONG":
        (passed if ema9 > ema21 else failed).append("EMA9aboveEMA21")
    else:
        (passed if ema9 < ema21 else failed).append("EMA9belowEMA21")

    # Filter 6: No wick trap
    if rng > 0:
        if direction == "LONG":
            upper_wick = c["high"] - max(c["open"], c["close"])
            wick_ok    = (upper_wick / rng) < 0.40
            lbl        = "NoUpperWickTrap"
        else:
            lower_wick = min(c["open"], c["close"]) - c["low"]
            wick_ok    = (lower_wick / rng) < 0.40
            lbl        = "NoLowerWickTrap"
    else:
        wick_ok = False
        lbl     = "NoWickTrap"
    (passed if wick_ok else failed).append(lbl)

    # ATR(14) on 5m for SL calculation
    atr_s = ta.atr(df_5m["high"], df_5m["low"], df_5m["close"], length=14)
    atr   = float(atr_s.iloc[-1]) if atr_s is not None and not atr_s.isna().iloc[-1] else price * 0.01
    debug["atr"] = round(atr, 6)

    # Both mandatory filters (4 & 5) + all 6 total required
    m4 = "BullishBody60pct" if direction == "LONG" else "BearishBody60pct"
    m5 = "EMA9aboveEMA21"   if direction == "LONG" else "EMA9belowEMA21"
    should_enter = (m4 in passed) and (m5 in passed) and len(passed) == 6

    return {
        "should_enter": should_enter,
        "score":   len(passed),
        "passed":  passed,
        "failed":  failed,
        "debug":   debug,
        "atr":     atr,
        "ema9":    ema9,
        "ema21":   ema21,
        "vwap":    vwap_val,
    }


def _meme_check_emergency_exit(df_5m: pd.DataFrame, direction: str,
                               entry_vol: float) -> tuple:
    """Check 3 emergency exit conditions. Returns (should_exit, reason)."""
    c    = df_5m.iloc[-2]
    body = abs(c["close"] - c["open"])
    rng  = c["high"] - c["low"]
    cur_vol = float(df_5m["volume"].iloc[-2])

    if entry_vol > 0 and cur_vol < entry_vol * 0.30:
        return True, f"Volume collapsed to {cur_vol/entry_vol*100:.0f}% of entry vol"

    if rng > 0 and body > 0 and (rng / body) > 3.0:
        return True, f"Wick spike — range {rng/body:.1f}x body (manipulation)"

    rsi_s = ta.rsi(df_5m["close"], length=7)
    if rsi_s is not None and not rsi_s.isna().iloc[-1]:
        rsi = float(rsi_s.iloc[-1])
        if direction == "LONG" and rsi > MEME_RSI_EXIT_HIGH:
            return True, f"RSI(7) overbought at {rsi:.1f}"
        if direction == "SHORT" and rsi < MEME_RSI_EXIT_LOW:
            return True, f"RSI(7) oversold at {rsi:.1f}"

    return False, ""


def _meme_check_trailing_sl(df_3m: pd.DataFrame, direction: str,
                            cur_sl: float) -> Optional[float]:
    """Trail SL on 3m candle lows/highs. Returns new SL or None."""
    try:
        last = df_3m.iloc[-2]
        if direction == "LONG":
            candidate = float(last["low"])
            return round(candidate, 6) if candidate > cur_sl else None
        else:
            candidate = float(last["high"])
            return round(candidate, 6) if candidate < cur_sl else None
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  MEMEBOT — SET LEVERAGE (hard cap 5x)
# ─────────────────────────────────────────────────────────────────────────────
def _meme_sync_set_leverage(symbol: str) -> int:
    import math
    try:
        _, _, _, _, max_lev = _sync_get_instrument_constraints(symbol)
        actual_lev = min(MEME_LEVERAGE, math.floor(max_lev))
        if actual_lev < 1:
            actual_lev = 1
        bybit_session.set_leverage(
            category="linear", symbol=symbol,
            buyLeverage=str(actual_lev), sellLeverage=str(actual_lev))
        logger.info(f"[MEME][{symbol}] Leverage set to {actual_lev}x")
        return actual_lev
    except Exception as e:
        logger.warning(f"[MEME] set_leverage({symbol}): {e}")
        return 1


# ─────────────────────────────────────────────────────────────────────────────
#  MEMEBOT — TRADE ENTRY
# ─────────────────────────────────────────────────────────────────────────────
async def meme_open_trade(symbol: str, direction: str,
                          indicators: dict, bot) -> None:
    global meme_consec_losses

    if len(meme_positions) >= MEME_MAX_POSITIONS:
        logger.info(f"[MEME][{symbol}] Max positions reached — skipping")
        return

    actual_lev = await asyncio.to_thread(_meme_sync_set_leverage, symbol)

    entry = await get_price(symbol)
    if not entry:
        await alert(bot, f"[MEME][{symbol}] Cannot fetch price — skipped.")
        return

    balance = await get_balance()
    if not balance:
        await alert(bot, f"[MEME][{symbol}] Cannot fetch balance — skipped.")
        return

    atr     = indicators["atr"]
    sl_dist = atr * MEME_SL_ATR_MULT
    if direction == "LONG":
        sl  = round(entry - sl_dist, 6)
        tp1 = round(entry + sl_dist * MEME_TP1_RR, 6)
        tp2 = round(entry + sl_dist * MEME_TP2_RR, 6)
    else:
        sl  = round(entry + sl_dist, 6)
        tp1 = round(entry - sl_dist * MEME_TP1_RR, 6)
        tp2 = round(entry - sl_dist * MEME_TP2_RR, 6)

    min_q, max_q, step, min_notional, max_lev = await get_instrument_constraints(symbol)
    raw_qty = (balance * MEME_POSITION_PCT * actual_lev) / entry
    raw_qty = max(raw_qty, min_notional / entry)
    qty     = _round_qty(raw_qty, min_q, step)

    if qty <= 0:
        await alert(bot, f"[MEME][{symbol}] Position size too small.")
        return

    side   = "Buy" if direction == "LONG" else "Sell"
    result = await place_order(symbol, side, qty)
    if not result or "__error__" in result:
        err = result.get("__error__", "Unknown") if result else "No response"
        await alert(bot,
            f"[MEME] Order failed — {symbol} {direction}\n"
            f"Reason: {err}")
        return

    await asyncio.sleep(1)
    real_entry = await asyncio.to_thread(_sync_get_position_entry, symbol)
    fill_price = real_entry if real_entry else entry

    await set_sl(symbol, sl)

    entry_vol = indicators["debug"].get("avg_vol", 0) * MEME_VOL_MULT

    pos = {
        "symbol":       symbol,
        "direction":    direction,
        "entry_price":  fill_price,
        "sl_price":     sl,
        "tp1_price":    tp1,
        "tp2_price":    tp2,
        "original_qty": qty,
        "remaining_qty":qty,
        "tp1_hit":      False,
        "tp2_hit":      False,
        "entry_time":   time.time(),
        "entry_vol":    entry_vol,
        "score":        indicators["score"],
        "leverage":     actual_lev,
    }
    meme_positions[symbol] = pos
    meme_consec_losses = 0
    save_state()

    def esc(s): return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    f_str   = " ".join([f"✅{esc(p)}" for p in indicators["passed"]])
    fail_s  = " ".join([f"❌{esc(p)}" for p in indicators["failed"]])
    emoji   = "🟢" if direction == "LONG" else "🔴"
    bal_used = balance * MEME_POSITION_PCT
    lev_note = f"\n⚠️ Leverage capped at {actual_lev}x (symbol max, requested {MEME_LEVERAGE}x)" if actual_lev < MEME_LEVERAGE else ""

    msg = (
        f"{emoji} <b>MEME TRADE OPENED — {symbol} {direction}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 Entry  : <code>{fill_price:.6g}</code>\n"
        f"🛡 SL     : <code>{sl:.6g}</code>  (1.5x ATR)\n"
        f"🎯 TP1    : <code>{tp1:.6g}</code>  (1:1 RR → close 50%)\n"
        f"🎯 TP2    : <code>{tp2:.6g}</code>  (1:2 RR → close 30%)\n"
        f"🌊 TP3    : Trailing 20% on 3m candle lows\n"
        f"📊 Score  : {indicators['score']}/6\n"
        f"✅ Passed : {f_str}\n"
        f"❌ Failed : {fail_s}\n"
        f"⚡ Qty    : {qty}  |  Leverage: {actual_lev}x  |  Bal used: {bal_used:.2f} USDT\n"
        f"⏱ Dead trade close in 30 min if no TP1{lev_note}"
    )
    await alert(bot, msg)
    logger.info(f"[MEME][{symbol}] Trade opened {direction} @ {fill_price}")


# ─────────────────────────────────────────────────────────────────────────────
#  MEMEBOT — POSITION MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────
async def meme_manage_position(pos: dict, bot) -> None:
    global meme_consec_losses, meme_daily_loss
    symbol    = pos["symbol"]
    direction = pos["direction"]

    price, df_5m, df_3m = await asyncio.gather(
        asyncio.to_thread(_sync_get_price, symbol),
        asyncio.to_thread(_sync_get_klines, symbol, "5",  100),
        asyncio.to_thread(_sync_get_klines, symbol, "3",   60),
    )

    if not price or df_5m is None:
        return

    def est_pnl():
        diff = (price - pos["entry_price"]) if direction == "LONG" \
               else (pos["entry_price"] - price)
        return diff * pos["remaining_qty"] * pos["leverage"]

    # Dead trade: 30-minute timeout
    if not pos["tp1_hit"]:
        elapsed_min = (time.time() - pos["entry_time"]) / 60
        if elapsed_min >= MEME_DEAD_MINUTES:
            await close_full(symbol, direction)
            pnl = est_pnl()
            meme_daily_loss += min(pnl, 0)
            _record_trade(pos, price, pnl, "DEAD", bot_type="meme")
            meme_positions.pop(symbol, None)
            save_state()
            await alert(bot,
                f"⏱ <b>[MEME] DEAD TRADE — {symbol} {direction}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"No TP1 after {MEME_DEAD_MINUTES} minutes.\n"
                f"Closed @ <code>{price:.6g}</code>\n"
                f"Est PnL: {pnl:+.2f} USDT\n"
                f"🟡 Memecoin lost momentum. Bot goes FLAT on {symbol}.")
            return

    # Emergency exits
    should_exit, reason = await asyncio.to_thread(
        _meme_check_emergency_exit, df_5m, direction, pos["entry_vol"])
    if should_exit:
        min_q, _, step, _, _ = await get_instrument_constraints(symbol)
        close_qty  = _round_qty(pos["remaining_qty"] * 0.50, min_q, step)
        close_side = "Sell" if direction == "LONG" else "Buy"
        await place_order(symbol, close_side, close_qty, reduce_only=True)
        pos["remaining_qty"] = round(pos["remaining_qty"] * 0.50, 8)
        save_state()
        await alert(bot,
            f"🚨 <b>[MEME] EMERGENCY EXIT 50% — {symbol} {direction}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ Reason: {reason}\n"
            f"Closed 50% @ <code>{price:.6g}</code>\n"
            f"🔍 Remaining 50% still open — monitoring closely")
        return

    # Hard SL
    sl = pos["sl_price"]
    sl_hit = (direction == "LONG"  and price <= sl) or \
             (direction == "SHORT" and price >= sl)
    if sl_hit:
        await close_full(symbol, direction)
        pnl = est_pnl()
        meme_consec_losses += 1
        meme_daily_loss    += min(pnl, 0)
        _record_trade(pos, price, pnl, "SL", bot_type="meme")
        meme_positions.pop(symbol, None)
        save_state()
        warn = f"\n{meme_consec_losses} consecutive losses!" if meme_consec_losses >= 2 else ""
        await alert(bot,
            f"🛑 <b>[MEME] STOP LOSS — {symbol} {direction}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"SL    : <code>{sl:.6g}</code>\n"
            f"Price : <code>{price:.6g}</code>\n"
            f"Est PnL: {pnl:+.2f} USDT\n"
            f"Consecutive losses: {meme_consec_losses}{warn}")
        return

    # TP1: 1:1 RR -> close 50%
    if not pos["tp1_hit"]:
        tp1_hit = (direction == "LONG"  and price >= pos["tp1_price"]) or \
                  (direction == "SHORT" and price <= pos["tp1_price"])
        if tp1_hit:
            min_q, _, step, _, _ = await get_instrument_constraints(symbol)
            close_qty  = _round_qty(pos["original_qty"] * 0.50, min_q, step)
            close_side = "Sell" if direction == "LONG" else "Buy"
            await place_order(symbol, close_side, close_qty, reduce_only=True)
            pos["remaining_qty"] = round(pos["original_qty"] * 0.50, 8)
            pos["tp1_hit"]  = True
            pos["sl_price"] = pos["entry_price"]
            await set_sl(symbol, pos["entry_price"])
            meme_consec_losses = 0
            pnl_p = abs(pos["tp1_price"] - pos["entry_price"]) * close_qty * pos["leverage"]
            save_state()
            await alert(bot,
                f"🎯 <b>[MEME] TP1 HIT — {symbol} {direction}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"Closed 50% @ <code>{price:.6g}</code>\n"
                f"Est partial PnL: +{pnl_p:.2f} USDT\n"
                f"🔒 SL → Breakeven <code>{pos['entry_price']:.6g}</code>\n"
                f"Riding to TP2 @ <code>{pos['tp2_price']:.6g}</code>")
            return

    # TP2: 1:2 RR -> close 30%
    if pos["tp1_hit"] and not pos["tp2_hit"]:
        tp2_hit = (direction == "LONG"  and price >= pos["tp2_price"]) or \
                  (direction == "SHORT" and price <= pos["tp2_price"])
        if tp2_hit:
            min_q, _, step, _, _ = await get_instrument_constraints(symbol)
            close_qty  = _round_qty(pos["original_qty"] * 0.30, min_q, step)
            close_side = "Sell" if direction == "LONG" else "Buy"
            await place_order(symbol, close_side, close_qty, reduce_only=True)
            pos["remaining_qty"] = round(pos["original_qty"] * 0.20, 8)
            pos["tp2_hit"] = True
            pnl_p = abs(pos["tp2_price"] - pos["entry_price"]) * close_qty * pos["leverage"]
            save_state()
            await alert(bot,
                f"🎯 <b>[MEME] TP2 HIT — {symbol} {direction}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"Closed 30% @ <code>{price:.6g}</code>\n"
                f"Est partial PnL: +{pnl_p:.2f} USDT\n"
                f"🌊 Trailing last 20% on 3m candle lows/highs")
            return

    # TP3: Trail last 20% on 3m candles
    if pos["tp2_hit"] and df_3m is not None:
        new_sl = await asyncio.to_thread(
            _meme_check_trailing_sl, df_3m, direction, pos["sl_price"])
        if new_sl:
            pos["sl_price"] = new_sl
            await set_sl(symbol, new_sl)
            save_state()
            logger.info(f"[MEME][{symbol}] Trailing SL -> {new_sl:.6g}")


# ─────────────────────────────────────────────────────────────────────────────
#  MEMEBOT — MAIN MONITOR LOOP
# ─────────────────────────────────────────────────────────────────────────────
async def _meme_monitor_inner(symbol: str, bot, job_data: dict) -> None:
    global meme_daily_loss, meme_consec_losses, meme_paused

    balance = await get_balance()

    paused_circuit = (
        meme_daily_loss    <= -0.03 * balance or
        meme_consec_losses >= 3
    )

    if paused_circuit and not job_data.get("circuit_alerted"):
        await alert(bot,
            f"⚠️ <b>[MEME] CIRCUIT BREAKER</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Daily loss   : {meme_daily_loss:.2f} USDT\n"
            f"Consec losses: {meme_consec_losses}\n"
            f"🔴 No new MEME entries until conditions reset.\n"
            f"Use /meme_resume to reset counter.")
        job_data["circuit_alerted"] = True
    elif not paused_circuit:
        job_data["circuit_alerted"] = False

    if symbol in meme_positions:
        await meme_manage_position(meme_positions[symbol], bot)
        await _meme_status_report(symbol, bot, job_data, balance)
        return

    if meme_paused or paused_circuit:
        return

    if len(meme_positions) >= MEME_MAX_POSITIONS:
        return

    df_5m = await get_klines(symbol, "5", 100)
    if df_5m is None or len(df_5m) < 30:
        return

    for direction in ("LONG", "SHORT"):
        indicators = await asyncio.to_thread(_meme_compute_indicators, df_5m, direction)
        if indicators["should_enter"]:
            logger.info(f"[MEME][{symbol}] {direction} signal ({indicators['score']}/6)")
            await meme_open_trade(symbol, direction, indicators, bot)
            return

    await _meme_status_report(symbol, bot, job_data, balance, df_5m=df_5m)


async def _meme_status_report(symbol: str, bot, job_data: dict,
                               balance: float, df_5m=None) -> None:
    now = time.time()
    if now - job_data.get("last_meme_report", 0) < 1800:
        return
    job_data["last_meme_report"] = now

    pos = meme_positions.get(symbol)
    if pos:
        price = await get_price(symbol)
        diff  = (price - pos["entry_price"]) if pos["direction"] == "LONG" \
                else (pos["entry_price"] - price)
        pnl   = diff * pos["remaining_qty"] * pos["leverage"]
        mins  = (time.time() - pos["entry_time"]) / 60
        e     = "LONG" if pos["direction"] == "LONG" else "SHORT"
        direction_emoji = "🟢" if pos["direction"] == "LONG" else "🔴"
        trade_info = (
            f"{direction_emoji} <b>Open {pos['direction']} position</b>\n"
            f"   📌 Entry  : <code>{pos['entry_price']:.6g}</code>\n"
            f"   💹 Current: <code>{price:.6g}</code>\n"
            f"   🛡 SL     : <code>{pos['sl_price']:.6g}</code>\n"
            f"   🎯 TP1    : {'✅' if pos['tp1_hit'] else '⬜'}  "
            f"TP2: {'✅' if pos['tp2_hit'] else '⬜'}\n"
            f"   Est PnL: {pnl:+.2f} USDT  |  Open: {mins:.0f}min"
        )
    else:
        trade_info = "No open position — watching for momentum signal"

    ind_text = ""
    if df_5m is not None:
        try:
            ind = await asyncio.to_thread(_meme_compute_indicators, df_5m, "LONG")
            d   = ind["debug"]
            ind_text = (
                f"\n📊 <b>Indicators (5m)</b>\n"
                f"   Price    : <code>{d.get('price',0):.6g}</code>\n"
                f"   RSI(7)   : <code>{d.get('rsi','?')}</code>\n"
                f"   Vol ratio: <code>{d.get('vol_ratio','?')}x</code>  (need {MEME_VOL_MULT}x+)\n"
                f"   VWAP     : <code>{d.get('vwap','?'):.6g}</code>\n"
                f"   ATR(14)  : <code>{d.get('atr','?'):.6g}</code>\n"
                f"   🟢 LONG score: {ind['score']}/6"
            )
        except Exception:
            pass

    await alert(bot,
        f"⚡ <b>[MEME] 30-min Report — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Balance : <code>{balance:.2f} USDT</code>\n"
        f"⏸ Paused  : {'🔴 YES' if meme_paused else '🟢 NO'}\n"
        f"📉 Consec losses: {meme_consec_losses}  |  Daily loss: {meme_daily_loss:.2f} USDT\n"
        f"📊 Open positions: {len(meme_positions)}/{MEME_MAX_POSITIONS}\n"
        f"\n{trade_info}"
        f"{ind_text}\n\n"
        f"⚙️ 5x lev  |  8% bal  |  TP1=1:1 RR  |  30min dead close")


async def meme_monitor_symbol(context: ContextTypes.DEFAULT_TYPE) -> None:
    symbol   = context.job.data["symbol"]
    job_data = context.job.data
    if meme_monitor_running.get(symbol, False):
        return
    meme_monitor_running[symbol] = True
    try:
        await _meme_monitor_inner(symbol, context.bot, job_data)
    except Exception as e:
        logger.error(f"[MEME][{symbol}] Error: {e}", exc_info=True)
    finally:
        meme_monitor_running[symbol] = False


# ─────────────────────────────────────────────────────────────────────────────
#  MEMEBOT — TELEGRAM COMMAND HANDLERS
# ─────────────────────────────────────────────────────────────────────────────
@public_command
async def cmd_meme_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "<b>MemeBot — Volatile Coin Strategy</b>\n"
        "Timeframes: 5m entry + 3m trailing\n\n"
        "<b>Commands:</b>\n"
        "/add_meme SYMBOL — Monitor a memecoin\n"
        "/remove_meme SYMBOL — Stop monitoring\n"
        "/meme_status — Positions and scores\n"
        "/scan_meme SYMBOL — Live indicator scan\n"
        "/meme_pause — Pause new entries\n"
        "/meme_resume — Resume entries\n\n"
        "<b>Entry Filters (ALL 6 required):</b>\n"
        "1. Volume spike 3x+ above 20-period avg (5m)\n"
        "2. RSI(7) zone: 45-75 long / 25-55 short\n"
        "3. Price above VWAP (long) / below (short)\n"
        "4. MANDATORY: Strong 5m body 60%+ of range\n"
        "5. MANDATORY: EMA9 above EMA21 (long)\n"
        "6. No wick trap (wick below 40% of range)\n\n"
        "<b>Sizing:</b> 8% balance | Max 5x leverage | Max 2 positions\n\n"
        "<b>Take Profit:</b>\n"
        "TP1: 1:1 RR close 50% then SL to breakeven\n"
        "TP2: 1:2 RR close 30%\n"
        "TP3: Trail last 20% on 3m candle lows\n\n"
        "<b>Emergency Exits:</b>\n"
        "Volume collapse 70% below entry vol\n"
        "Wick spike range 3x+ the candle body\n"
        "RSI extreme above 85 long / below 15 short\n"
        "30-minute dead trade timeout\n\n"
        "<b>Examples:</b>\n"
        "<code>/add_meme PEPEUSDT</code>\n"
        "<code>/scan_meme DOGEUSDT</code>",
        parse_mode="HTML")


@owner_only
async def cmd_add_meme(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not ctx.args:
        await update.message.reply_text("Usage: /add_meme PEPEUSDT")
        return
    symbol = ctx.args[0].upper()
    if symbol in meme_symbols:
        await update.message.reply_text(f"{symbol} already in MemeBot watchlist.")
        return

    meme_symbols.append(symbol)

    # Reply FIRST so Telegram never appears frozen
    await update.message.reply_text(
        f"<b>{symbol}</b> added to MemeBot!\n"
        f"Scanning every {MEME_POLL_INTERVAL}s on 5m timeframe.\n"
        f"Max 5x leverage | 8% position | TP1 at 1:1 RR\n"
        f"30-min dead trade timeout active.",
        parse_mode="HTML")

    # Background tasks after reply
    await asyncio.to_thread(save_state)
    asyncio.create_task(asyncio.to_thread(subscribe_ws_symbol, symbol))

    idx   = meme_symbols.index(symbol)
    delay = MEME_POLL_INTERVAL + idx * 1.5

    if ctx.job_queue:
        ctx.job_queue.run_repeating(
            callback=meme_monitor_symbol,
            interval=MEME_POLL_INTERVAL,
            first=delay,
            chat_id=CHAT_ID,
            name=f"meme_{symbol}_monitor",
            data={"symbol": symbol, "last_meme_report": 0,
                  "circuit_alerted": False},
        )


@owner_only
async def cmd_remove_meme(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not ctx.args:
        await update.message.reply_text("Usage: /remove_meme PEPEUSDT")
        return
    symbol = ctx.args[0].upper()
    if symbol not in meme_symbols:
        await update.message.reply_text(f"{symbol} not in MemeBot watchlist.")
        return
    meme_symbols.remove(symbol)
    if symbol in meme_positions:
        await update.message.reply_text(
            f"{symbol} has open meme position — removed from monitoring "
            f"but position still open on Bybit.")
    if ctx.job_queue:
        for job in ctx.job_queue.get_jobs_by_name(f"meme_{symbol}_monitor"):
            job.schedule_removal()
    save_state()
    await update.message.reply_text(
        f"<b>{symbol}</b> removed from MemeBot.", parse_mode="HTML")


@public_command
async def cmd_meme_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    balance = await get_balance()
    lines = [
        "<b>MemeBot Status</b>",
        "Balance: <code>" + f"{balance:.2f} USDT</code>",
        f"Paused: {'YES' if meme_paused else 'NO'}",
        f"Consec losses: {meme_consec_losses}",
        f"Daily loss: {meme_daily_loss:.2f} USDT",
        f"Open positions: {len(meme_positions)}/{MEME_MAX_POSITIONS}",
        "",
        "<b>Watching:</b> " + (", ".join(meme_symbols) or "None"),
        "",
    ]
    if meme_positions:
        lines.append("<b>Open Meme Positions:</b>")
        for sym, pos in meme_positions.items():
            price = await get_price(sym)
            diff  = (price - pos["entry_price"]) if pos["direction"] == "LONG" \
                    else (pos["entry_price"] - price)
            pnl   = diff * pos["remaining_qty"] * pos["leverage"]
            mins  = (time.time() - pos["entry_time"]) / 60
            dead  = " DEAD-SOON" if not pos["tp1_hit"] and mins >= 25 else ""
            lines.append(
                f"<b>{sym}</b> {pos['direction']}{dead}\n"
                f"Entry:{pos['entry_price']:.6g}  Now:{price:.6g}\n"
                f"SL:{pos['sl_price']:.6g}\n"
                f"TP1:{'YES' if pos['tp1_hit'] else 'NO'}  "
                f"TP2:{'YES' if pos['tp2_hit'] else 'NO'}\n"
                f"PnL:{pnl:+.2f} USDT  Open:{mins:.0f}min  Lev:{pos['leverage']}x"
            )
    else:
        lines.append("No open meme positions.")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


@public_command
async def cmd_scan_meme(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not ctx.args and not meme_symbols:
        await update.message.reply_text(
            "Usage: /scan_meme PEPEUSDT\nOr add symbols first with /add_meme")
        return

    symbols = [ctx.args[0].upper()] if ctx.args else meme_symbols
    await update.message.reply_text(f"Scanning {len(symbols)} meme symbol(s)...")

    def esc(s): return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    for symbol in symbols:
        df_5m = await get_klines(symbol, "5", 100)
        if df_5m is None or len(df_5m) < 30:
            await update.message.reply_text(f"{symbol}: Failed to fetch 5m candles.")
            continue

        for direction in ("LONG", "SHORT"):
            ind    = await asyncio.to_thread(_meme_compute_indicators, df_5m, direction)
            d      = ind["debug"]
            status = "ENTRY SIGNAL!" if ind["should_enter"] else "Waiting"
            p_str  = "\n   ".join([f"✅{esc(p)}" for p in ind["passed"]]) or "none"
            f_str  = "\n   ".join([f"❌{esc(p)}" for p in ind["failed"]]) or "none"
            e_emoji = "🟢" if direction == "LONG" else "🔴"

            await update.message.reply_text(
                f"{e_emoji} <b>{symbol} — {direction}</b>  {status}\n"
                f"Score: <b>{ind['score']}/6</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>Passed:</b>\n   {p_str}\n"
                f"<b>Failed:</b>\n   {f_str}\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"<b>Raw Values:</b>\n"
                f"Price    : <code>{d.get('price',0):.6g}</code>\n"
                f"RSI(7)   : <code>{d.get('rsi','?')}</code>\n"
                f"Vol ratio: <code>{d.get('vol_ratio','?'):.6g}x</code>  (need {MEME_VOL_MULT}x+)\n"
                f"VWAP     : <code>{d.get('vwap','?'):.6g}</code>\n"
                f"Body pct : <code>{d.get('body_pct','?')}</code>  (need 0.60+)\n"
                f"EMA9/21  : <code>{d.get('ema9','?'):.6g}</code> / <code>{d.get('ema21','?'):.6g}</code>\n"
                f"ATR(14)  : <code>{d.get('atr','?'):.6g}</code>",
                parse_mode="HTML",
            )

@owner_only
async def cmd_meme_pause(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    global meme_paused
    meme_paused = True
    save_state()
    await update.message.reply_text(
        "<b>MemeBot paused.</b> No new meme entries.\n"
        "Existing positions still managed.",
        parse_mode="HTML")


@owner_only
async def cmd_meme_resume(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    global meme_paused, meme_consec_losses
    meme_paused        = False
    meme_consec_losses = 0
    save_state()
    await update.message.reply_text(
        "<b>MemeBot resumed.</b> Scanning for momentum signals.",
        parse_mode="HTML")

# =============================================================================
#  END OF MEMEBOT MODULE
# =============================================================================


# =============================================================================
#  NEWSBOT — Real-time Crypto News Intelligence
#  Source   : CryptoPanic API (free tier — sign up at cryptopanic.com)
#  Polling  : Every 2 minutes
#  Logic    : Fetch "important" news → score each headline for impact →
#             alert only if HIGH impact (hack, ETF, regulation, collapse etc.)
#  Coins    : Auto-detects affected coins from CryptoPanic currency tags
# =============================================================================

# ── Impact keyword scoring ────────────────────────────────────────────────────
# Each keyword adds points to the news impact score.
# Only news scoring >= 3 gets sent to Telegram.
# This filters out routine news and only catches market-moving events.

NEWS_IMPACT_KEYWORDS = {
    # 🚨 CRASH / CRISIS events — very high impact
    "hack":          5, "hacked":        5, "exploit":      5, "stolen":        5,
    "breach":        5, "attack":        5, "vulnerability":4, "collapse":      5,
    "bankrupt":      5, "insolvent":     5, "fraud":        5, "scam":          5,
    "rug pull":      5, "exit scam":     5, "ponzi":        5, "shutdown":      4,
    "suspended":     4, "halted":        4, "seized":       4, "delisted":      4,
    "insolvency":    5, "liquidation":   4, "crash":        4, "plunge":        3,
    "dump":          3, "sell-off":      3, "panic":        3,

    # 📈 BULLISH / POSITIVE events — high impact
    "etf":           5, "etf approved":  5, "spot etf":     5, "etf launch":    5,
    "approved":      4, "approval":      4, "listed":       3, "partnership":   3,
    "institutional": 4, "reserve":       4, "strategic":    4, "treasury":      3,
    "adoption":      4, "legal tender":  5, "regulation":   4, "regulated":     4,
    "sec":           4, "cftc":          4, "congress":     3, "senate":        3,
    "halving":       4, "upgrade":       3, "mainnet":      3, "launch":        3,
    "ipo":           4, "acquisition":   4, "merger":       4, "investment":    3,
    "all-time high": 5, "ath":           4, "record":       3, "billion":       3,
    "whale":         3, "accumulation":  3,

    # ⚖️ REGULATORY events
    "ban":           5, "banned":        5, "illegal":      5, "lawsuit":       4,
    "sued":          4, "indicted":      5, "arrested":     5, "investigated":  4,
    "subpoena":      4, "crackdown":     5, "sanction":     5, "blacklist":     4,
    "cbdc":          3, "stablecoin":    3, "aml":          3,

    # 🏦 EXCHANGE / CUSTODIAL events
    "exchange":      2, "outage":        4, "offline":      4, "withdrawal":    4,
    "frozen":        5, "ftx":           5, "binance":      3, "coinbase":      3,
    "kraken":        3, "bybit":         3,

    # 🌐 MACRO events
    "fed":           3, "interest rate": 4, "inflation":    3, "recession":     4,
    "war":           4, "conflict":      3, "sanctions":    4, "trump":         3,
    "china":         3, "us":            2, "network":      2,
}

# Minimum score to trigger an alert
NEWS_MIN_SCORE = 6

# These coin symbols map to full names for clearer alerts
COIN_NAMES = {
    "BTC": "Bitcoin",  "ETH": "Ethereum", "SOL": "Solana",
    "BNB": "BNB",      "XRP": "XRP",      "ADA": "Cardano",
    "DOGE":"Dogecoin", "MATIC":"Polygon", "DOT": "Polkadot",
    "AVAX":"Avalanche","LINK":"Chainlink","UNI": "Uniswap",
    "LTC": "Litecoin", "ATOM":"Cosmos",   "FTT": "FTX Token",
    "LUNA":"Terra",    "UST": "TerraUSD", "USDT":"Tether",
    "USDC":"USD Coin", "PEPE":"Pepe",     "WIF": "dogwifhat",
    "BONK":"Bonk",     "SHIB":"Shiba Inu","TON": "Toncoin",
    "SUI": "Sui",      "APT": "Aptos",    "ARB": "Arbitrum",
    "OP":  "Optimism", "NEAR":"Near",     "FIL": "Filecoin",
}


def _sync_fetch_news() -> list:
    """
    Fetch latest important crypto news from CryptoPanic API.
    Runs in background thread — never blocks event loop.
    Returns list of raw news items.
    """
    import urllib.request, json as _json
    if not CRYPTOPANIC_TOKEN:
        return []
    try:
        url = (
            f"https://cryptopanic.com/api/free/v1/posts/"
            f"?auth_token={CRYPTOPANIC_TOKEN}"
            f"&filter=important&public=true&kind=news"
        )
        with urllib.request.urlopen(url, timeout=15) as resp:
            data = _json.loads(resp.read().decode())
            return data.get("results", [])
    except Exception as e:
        logger.error(f"[NEWS] CryptoPanic fetch error: {e}")
        return []


def _score_news(title: str, body: str = "") -> tuple:
    """
    Score a news item based on impact keywords.
    Returns (score: int, matched_keywords: list, sentiment: str)
    """
    text   = (title + " " + body).lower()
    score  = 0
    matched = []

    for keyword, points in NEWS_IMPACT_KEYWORDS.items():
        if keyword in text:
            score += points
            matched.append(keyword)

    # Determine sentiment from keywords
    bullish_keys = {"etf","approved","approval","adoption","reserve","strategic",
                    "launch","mainnet","upgrade","ath","all-time high","partnership",
                    "institutional","listed","halving","acquisition","investment"}
    bearish_keys = {"hack","hacked","exploit","stolen","collapse","bankrupt",
                    "fraud","scam","ban","banned","lawsuit","crash","plunge",
                    "frozen","seized","insolvent","crackdown","sanction",
                    "exit scam","rug pull","indicted","arrested"}

    b_hits = sum(1 for k in matched if k in bullish_keys)
    n_hits = sum(1 for k in matched if k in bearish_keys)

    if n_hits > b_hits:
        sentiment = "BEARISH"
    elif b_hits > n_hits:
        sentiment = "BULLISH"
    else:
        sentiment = "NEUTRAL"

    return score, matched, sentiment


def _format_news_alert(item: dict, score: int, matched: list,
                       sentiment: str) -> str:
    """Build the Telegram alert message for a high-impact news item."""
    from datetime import timezone as _tz

    title      = item.get("title", "No title")
    url        = item.get("url", "")
    source     = item.get("source", {}).get("title", "Unknown source")
    published  = item.get("published_at", "")
    currencies = item.get("currencies") or []
    votes      = item.get("votes", {})

    # Coin tags
    coin_tags = []
    for c in currencies:
        code  = c.get("code", "")
        cname = COIN_NAMES.get(code, c.get("title", code))
        coin_tags.append(f"${code} ({cname})")

    coins_str = " | ".join(coin_tags) if coin_tags else "General Market"

    # Sentiment emoji
    if sentiment == "BULLISH":
        s_emoji = "📈 BULLISH"
        border  = "🟢"
    elif sentiment == "BEARISH":
        s_emoji = "📉 BEARISH"
        border  = "🔴"
    else:
        s_emoji = "⚪ NEUTRAL"
        border  = "🟡"

    # Community votes
    bull_votes = votes.get("bullish", 0) or 0
    bear_votes = votes.get("bearish", 0) or 0
    imp_votes  = votes.get("important", 0) or 0

    # Score bar
    score_bar = "🔥" * min(score // 3, 5)

    # Top matched keywords
    top_keys = ", ".join(matched[:5])

    msg = (
        f"{border} <b>\U0001f5de\ufe0f MARKET NEWS ALERT</b> {border}\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"\U0001f4f0 <b>{title}</b>\n\n"
        f"\U0001f4a5 Impact    : {score_bar} ({score} pts)\n"
        f"\U0001f4ca Sentiment : {s_emoji}\n"
        f"\U0001f3af Coins     : {coins_str}\n"
        f"\U0001f511 Keywords  : {top_keys}\n"
        f"\U0001f4e1 Source    : {source}\n"
        f"\U0001f465 Community : {bull_votes} bullish  |  {bear_votes} bearish  |  {imp_votes} important\n"
        f"\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
        f"<a href=\"{url}\">Read full article</a>\n\n"
        f"<i>You can act on this — check /status and /scan for affected coins</i>"
    )
    return msg


# ─────────────────────────────────────────────────────────────────────────────
#  NEWSBOT — MAIN POLL LOOP (job callback)
# ─────────────────────────────────────────────────────────────────────────────
async def news_monitor(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Runs every 2 minutes. Fetches CryptoPanic important news,
    scores each item, and sends alerts for high-impact stories.
    """
    global news_seen_ids
    if not news_enabled:
        return
    if not CRYPTOPANIC_TOKEN:
        return

    try:
        items = await asyncio.to_thread(_sync_fetch_news)
        if not items:
            return

        from datetime import timezone as _tz, datetime as _dt

        for item in items:
            item_id = item.get("id")
            if not item_id or item_id in news_seen_ids:
                continue

            # Age check — ignore stale news
            pub = item.get("published_at", "")
            try:
                pub_dt  = _dt.fromisoformat(pub.replace("Z", "+00:00"))
                age_hrs = (_dt.now(_tz.utc) - pub_dt).total_seconds() / 3600
                if age_hrs > NEWS_MAX_AGE_HOURS:
                    news_seen_ids.add(item_id)
                    continue
            except Exception:
                pass

            title = item.get("title", "")
            score, matched, sentiment = _score_news(title)

            # Mark as seen regardless of whether we alert
            news_seen_ids.add(item_id)

            # Keep seen_ids from growing forever
            if len(news_seen_ids) > 2000:
                news_seen_ids = set(list(news_seen_ids)[-1000:])

            if score < NEWS_MIN_SCORE:
                continue

            msg = _format_news_alert(item, score, matched, sentiment)
            await alert(context.bot, msg)
            logger.info(f"[NEWS] Alert sent: score={score} sentiment={sentiment} '{title[:60]}'")

    except Exception as e:
        logger.error(f"[NEWS] monitor error: {e}", exc_info=True)


# ─────────────────────────────────────────────────────────────────────────────
#  NEWSBOT — TELEGRAM COMMAND HANDLERS
# ─────────────────────────────────────────────────────────────────────────────
@public_command
async def cmd_news_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Show NewsBot status and recent news summary."""
    token_set = "✅ SET" if CRYPTOPANIC_TOKEN else "❌ NOT SET (add CRYPTOPANIC_TOKEN to Render env vars)"
    await update.message.reply_text(
        "<b>NewsBot Status</b>\n"
        "Active       : " + ("YES" if news_enabled else "PAUSED") + "\n"
        f"API Token    : {token_set}\n"
        f"Poll interval: Every {NEWS_POLL_INTERVAL}s (2 min)\n"
        f"Max news age : {NEWS_MAX_AGE_HOURS} hour(s)\n"
        f"Min score    : {NEWS_MIN_SCORE} pts to trigger alert\n"
        f"News seen    : {len(news_seen_ids)} articles (dedup cache)\n\n"
        "<b>What triggers an alert:</b>\n"
        "- Exchange hacks or exploits\n"
        "- ETF approvals or rejections\n"
        "- Exchange collapses (FTX-style)\n"
        "- Government bans or regulations\n"
        "- Whale moves or institutional buys\n"
        "- Network outages or attacks\n"
        "- Strategic reserve announcements\n"
        "- All-time highs or major crashes\n\n"
        "<b>Commands:</b>\n"
        "/news_status  - This screen\n"
        "/news_pause   - Pause alerts\n"
        "/news_resume  - Resume alerts\n"
        "/news_scan    - Fetch latest news NOW",
        parse_mode="HTML")

@owner_only
async def cmd_news_pause(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    global news_enabled
    news_enabled = False
    await update.message.reply_text(
        "NewsBot paused. No more news alerts until you use /news_resume.",
        parse_mode="HTML")

@owner_only
async def cmd_news_resume(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    global news_enabled
    news_enabled = True
    await update.message.reply_text(
        "▶️ <b>NewsBot resumed.</b> Watching for market-moving news.",
        parse_mode="HTML")

@public_command
async def cmd_news_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    status = "YES" if news_enabled else "PAUSED"
    token_ok = "SET" if CRYPTOPANIC_TOKEN else "NOT SET — add CRYPTOPANIC_TOKEN to Render env vars"
    await update.message.reply_text(
        f"<b>NewsBot Status</b>\n"
        f"Active: {status}\n"
        f"API Token: {token_ok}\n"
        f"Poll: every {NEWS_POLL_INTERVAL}s\n"
        f"Max age: {NEWS_MAX_AGE_HOURS}h\n"
        f"Min score: {NEWS_MIN_SCORE} pts\n"
        f"Seen: {len(news_seen_ids)} articles\n\n"
        "<b>Triggers alerts for:</b>\n"
        "- Hacks / exploits\n"
        "- ETF approvals / rejections\n"
        "- Exchange collapses\n"
        "- Government bans / regulations\n"
        "- Institutional buys / whale moves\n"
        "- Network outages / attacks\n"
        "- Strategic reserve announcements\n"
        "- ATH or major crashes\n\n"
        "<b>Commands:</b>\n"
        "/news_status — this screen\n"
        "/news_pause  — pause alerts\n"
        "/news_resume — resume alerts\n"
        "/news_scan   — fetch news now",
        parse_mode="HTML")

@owner_only
async def cmd_news_pause(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    global news_enabled
    news_enabled = False
    await update.message.reply_text(
        "<b>NewsBot paused.</b> No news alerts until /news_resume.",
        parse_mode="HTML")

@owner_only
async def cmd_news_resume(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    global news_enabled
    news_enabled = True
    await update.message.reply_text(
        "<b>NewsBot resumed.</b> Watching for market-moving news.",
        parse_mode="HTML")

@owner_only
async def cmd_news_scan(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not CRYPTOPANIC_TOKEN:
        await update.message.reply_text(
            "<b>CRYPTOPANIC_TOKEN not set.</b>\n\n"
            "How to set it up (FREE):\n"
            "1. Go to cryptopanic.com/developers/api/keys\n"
            "2. Create a free account\n"
            "3. Copy your auth token\n"
            "4. Add CRYPTOPANIC_TOKEN to Render Environment Variables\n"
            "5. Redeploy — done!",
            parse_mode="HTML")
        return

    await update.message.reply_text("Fetching latest important crypto news...")

    items = await asyncio.to_thread(_sync_fetch_news)
    if not items:
        await update.message.reply_text("No news returned. Try again in a moment.")
        return

    from datetime import timezone as _tz, datetime as _dt
    sent = 0
    for item in items[:20]:
        title = item.get("title", "")
        score, matched, sentiment = _score_news(title)
        if score < NEWS_MIN_SCORE:
            continue

        pub = item.get("published_at", "")
        try:
            pub_dt  = _dt.fromisoformat(pub.replace("Z", "+00:00"))
            age_min = int((_dt.now(_tz.utc) - pub_dt).total_seconds() / 60)
            age_str = f"{age_min}m ago"
        except Exception:
            age_str = "unknown"

        msg = _format_news_alert(item, score, matched, sentiment)
        msg += f"\n<i>Published: {age_str}</i>"
        await update.message.reply_text(msg, parse_mode="HTML",
                                        disable_web_page_preview=True)
        sent += 1
        if sent >= 5:
            break

    if sent == 0:
        await update.message.reply_text(
            "No high-impact news right now (score below threshold).\n"
            "Markets appear calm. Bot will alert you when something significant breaks.")


# =============================================================================
#  END OF NEWSBOT MODULE
# =============================================================================

@public_command
async def cmd_history(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Show trade history summary for last 60 days."""
    if not trade_history:
        await update.message.reply_text(
            "No trade history yet.\n"
            "History is recorded from this version onwards.\n"
            "Every closed trade will appear here automatically.")
        return
        return

    from datetime import timezone as _tz, datetime as _dt, timedelta as _td

    # Parse days filter from args e.g. /history 7
    days = 60
    if ctx.args:
        try:
            days = min(int(ctx.args[0]), 60)
        except ValueError:
            pass

    cutoff = (_dt.now(_tz.utc) - _td(days=days)).isoformat()
    trades = [t for t in trade_history if t.get("close_time", "") >= cutoff]

    if not trades:
        await update.message.reply_text(f"No trades in the last {days} days.")
        return

    # Split by bot
    for bot_label, bot_key in [("PsychologyBot", "psych"), ("MemeBot", "meme")]:
        bt = [t for t in trades if t.get("bot") == bot_key]
        if not bt:
            continue

        wins   = [t for t in bt if t["pnl"] > 0]
        losses = [t for t in bt if t["pnl"] <= 0]
        total_pnl = sum(t["pnl"] for t in bt)
        win_rate  = round(len(wins) / len(bt) * 100) if bt else 0
        best  = max(bt, key=lambda x: x["pnl"]) if bt else None
        worst = min(bt, key=lambda x: x["pnl"]) if bt else None
        avg_dur = round(sum(t.get("duration_min",0) for t in bt) / len(bt)) if bt else 0

        # Exit breakdown
        sl_count   = sum(1 for t in bt if t.get("exit_reason") == "SL")
        tp1_count  = sum(1 for t in bt if t.get("exit_reason") == "TP1")
        tp2_count  = sum(1 for t in bt if t.get("exit_reason") == "TP2")
        dead_count = sum(1 for t in bt if t.get("exit_reason") == "DEAD")
        ema_count  = sum(1 for t in bt if t.get("exit_reason") == "EMA_EXIT")

        # Recent 5 trades
        recent = sorted(bt, key=lambda x: x.get("close_time",""), reverse=True)[:5]
        recent_lines = []
        for t in recent:
            e = "✅" if t["pnl"] > 0 else "❌"
            dt_str = t.get("close_time","")[:10]
            recent_lines.append(
                f"  {e} {t['symbol']} {t['direction']}  "
                f"PnL:{t['pnl']:+.2f}  via {t.get('exit_reason','?')}  {dt_str}")

        msg = (
            f"<b>{bot_label} — Last {days} days</b>\n"
            f"Total trades  : {len(bt)}\n"
            f"Wins / Losses : {len(wins)} / {len(losses)}\n"
            f"Win rate      : {win_rate}%\n"
            f"Total PnL     : {total_pnl:+.2f} USDT\n"
            f"Avg duration  : {avg_dur} min\n\n"
            "<b>Exit reasons:</b>\n"
            f"  Stop Loss  : {sl_count}\n"
            f"  TP1 hit    : {tp1_count}\n"
            f"  TP2 hit    : {tp2_count}\n"
            f"  Dead trade : {dead_count}\n"
            f"  EMA exit   : {ema_count}\n"
        )
        if best:
            msg += f"Best trade  : {best['symbol']} {best['direction']} {best['pnl']:+.2f} USDT\n"
        if worst:
            msg += f"Worst trade : {worst['symbol']} {worst['direction']} {worst['pnl']:+.2f} USDT\n"
        msg += "\nRecent 5 trades:\n" + "\n".join(recent_lines)
        await update.message.reply_text(msg, parse_mode="HTML")

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

    print("=== PsychologyBot v3 + MemeBot startup ===")
    print(f"TELEGRAM_TOKEN loaded ({len(TELEGRAM_TOKEN)} chars)")
    print(f"CHAT_ID: {CHAT_ID}")
    print(f"Thread pool: {_executor._max_workers} workers")
    print(f"STATE_FILE: {STATE_FILE}")
    print(f"State file exists: {os.path.exists(STATE_FILE)}")

    # Load persisted state
    state_existed = os.path.exists(STATE_FILE)
    load_state()
    _load_history()

    # Start WebSocket
    if start_websocket():
        all_syms = list(set(monitored_symbols + meme_symbols))
        # subscribe_ws_symbol can block — run all in thread pool at startup
        for sym in all_syms:
            try:
                subscribe_ws_symbol(sym)
            except Exception as e:
                print(f"[WS] subscribe {sym} error: {e}")
        print(f"[WS] Subscribed to {len(all_syms)} symbols "
              f"(psych:{len(monitored_symbols)} meme:{len(meme_symbols)})")

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
    # ── MemeBot commands ──────────────────────────────────────────────────────
    app.add_handler(CommandHandler("memebot",        cmd_meme_start))
    app.add_handler(CommandHandler("add_meme",       cmd_add_meme))
    app.add_handler(CommandHandler("remove_meme",    cmd_remove_meme))
    app.add_handler(CommandHandler("meme_status",    cmd_meme_status))
    app.add_handler(CommandHandler("scan_meme",      cmd_scan_meme))
    app.add_handler(CommandHandler("meme_pause",     cmd_meme_pause))
    app.add_handler(CommandHandler("meme_resume",    cmd_meme_resume))
    # ── NewsBot commands ──────────────────────────────────────────────────────────
    app.add_handler(CommandHandler("news_status",    cmd_news_status))
    app.add_handler(CommandHandler("news_pause",     cmd_news_pause))
    app.add_handler(CommandHandler("news_resume",    cmd_news_resume))
    app.add_handler(CommandHandler("news_scan",      cmd_news_scan))
    app.add_handler(CommandHandler("history",         cmd_history))
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

        # Recreate MemeBot monitoring jobs for restored symbols
        for i, sym in enumerate(meme_symbols):
            existing = app.job_queue.get_jobs_by_name(f"meme_{sym}_monitor")
            if not existing:
                delay = MEME_POLL_INTERVAL + i * 1.5
                app.job_queue.run_repeating(
                    callback=meme_monitor_symbol,
                    interval=MEME_POLL_INTERVAL,
                    first=delay,
                    chat_id=CHAT_ID,
                    name=f"meme_{sym}_monitor",
                    data={"symbol": sym, "last_meme_report": 0,
                          "circuit_alerted": False},
                )
                print(f"[STARTUP] MemeBot monitor for {sym} (first run in {delay:.1f}s)")

        # NewsBot — polls CryptoPanic every 2 minutes for high-impact news
        if CRYPTOPANIC_TOKEN:
            app.job_queue.run_repeating(
                callback=news_monitor,
                interval=NEWS_POLL_INTERVAL,
                first=30,   # first check 30s after startup
                chat_id=CHAT_ID,
                name="news_monitor",
            )
            print(f"[STARTUP] NewsBot active — polling every {NEWS_POLL_INTERVAL}s")
        else:
            print("[STARTUP] NewsBot INACTIVE — set CRYPTOPANIC_TOKEN in Render env vars")

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

    # Send startup notification to owner
    from datetime import timezone as _tz, datetime as _dt
    now_str = _dt.now(_tz.utc).strftime("%Y-%m-%d %H:%M UTC")
    disk_ok = os.path.isdir("/var/data")

    if state_existed and (monitored_symbols or meme_symbols):
        # State was restored successfully
        psych_syms = ", ".join(monitored_symbols) if monitored_symbols else "none"
        meme_syms  = ", ".join(meme_symbols) if meme_symbols else "none"
        open_pos   = len(symbol_positions) + len(meme_positions)
        startup_msg = (
            f"🔄 <b>Bot restarted &amp; state restored!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🕒 Time: {now_str}\n"
            f"💾 State: ✅ Loaded from disk\n"
            f"💽 Persistent disk: {'✅ /var/data' if disk_ok else '⚠️ Not mounted — state may be lost on next redeploy'}\n\n"
            f"<b>Restored:</b>\n"
            f"📊 PsychologyBot: {psych_syms}\n"
            f"⚡ MemeBot: {meme_syms}\n"
            f"📈 Open positions: {open_pos}\n"
            f"📋 Trade history: {len(trade_history)} records\n\n"
            f"✅ All monitoring jobs restarted automatically.\n"
            f"No action needed — bot is watching your symbols."
        )
    else:
        # Fresh start — no state file or empty
        startup_msg = (
            f"🚀 <b>Bot started fresh</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🕒 Time: {now_str}\n"
            f"💾 State: {'⚠️ No state file found' if not state_existed else '📭 State file empty'}\n"
            f"💽 Persistent disk: {'✅ /var/data available' if disk_ok else '❌ Not mounted — ADD A RENDER DISK to survive redeploys'}\n\n"
            f"<b>Action needed:</b>\n"
            f"• Use /add_symbol BTCUSDT to start monitoring\n"
            f"• Use /add_meme PEPEUSDT for MemeBot\n\n"
            f"<b>To fix state loss permanently:</b>\n"
            f"Render Dashboard → Your service → Disks → Add Disk\n"
            f"Mount path: <code>/var/data</code> | Size: 1 GB (free tier available)"
        )

    try:
        await app.bot.send_message(chat_id=CHAT_ID, text=startup_msg, parse_mode="HTML")
    except Exception as e:
        print(f"[STARTUP] Could not send startup notification: {e}")

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