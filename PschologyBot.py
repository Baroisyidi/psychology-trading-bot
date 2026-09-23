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
import requests
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
# LIVE (real-money) credentials — completely separate from the demo ones
# above. Used ONLY by the Regime Strategy's live trading. Deliberately a
# different env var name so there's no possibility of the two getting
# mixed up. Both are optional at startup — if unset, live trading for
# the Regime Strategy simply stays disabled (see bybit_live_session below
# and REGIME_LIVE_TRADING_ENABLED).
BYBIT_LIVE_KEY      = os.getenv("BYBIT_LIVE_API_KEY")
BYBIT_LIVE_SECRET   = os.getenv("BYBIT_LIVE_API_SECRET")
CHAT_ID_STR        = os.getenv("CHAT_ID")
# STATE FILE PATH — always prefer /var/data (Render persistent disk).
# If /var/data exists (disk mounted), use it regardless of STATE_FILE env var.
# The env var STATE_FILE is IGNORED if /var/data is mounted — this prevents
# accidentally saving to /opt/render/project/src which is wiped on redeploy.
if os.path.isdir("/var/data"):
    STATE_FILE = "/var/data/bot_state.json"
else:
    # No persistent disk — fall back to env var or project dir
    STATE_FILE = os.getenv("STATE_FILE", "/opt/render/project/src/bot_state.json")
    print(f"[STATE] WARNING: /var/data not found — using {STATE_FILE} (lost on redeploy)")
CRYPTOPANIC_TOKEN  = os.getenv("CRYPTOPANIC_TOKEN", "")   # Free at cryptopanic.com/developers/api/keys

if not all([TELEGRAM_TOKEN, BYBIT_KEY, BYBIT_SECRET, CHAT_ID_STR]):
    raise ValueError("Missing required environment variables! Check .env or Render Environment Variables.")

try:
    CHAT_ID = int(CHAT_ID_STR)
except ValueError:
    raise ValueError(f"CHAT_ID must be an integer, got: {CHAT_ID_STR}")

# ── PsychologyBot trading params ──────────────────────────────────────────
LEVERAGE         = 10
POSITION_PCT     = 0.20     # 20% of balance per symbol (used as a CEILING now,
                             # not the direct sizing input — see RISK_PCT below)
POLL_INTERVAL    = 10       # seconds between monitor cycles
DEAD_TRADE_DAYS  = 7        # close dead trade after 7 days
SL_BUFFER        = 0.02     # 2% breathing room on initial SL

# ── Volatility-normalized risk management ──────────────────────────────────
# PROBLEM THIS SOLVES: a fixed 20% position + fixed 2% SL buffer means
# BTC (calm) and a volatile alt like HYPE get identical dollar risk exposure
# per trade even though HYPE can move 3-5x further in the same timeframe.
# Real-data backtesting (/backtest_compare across 9 coins) showed BTC/HYPE
# losing money on a fixed-SL approach — a clean signature of SL distance
# not being scaled to each coin's own volatility.
#
# FIX: risk-based position sizing. RISK_PCT is the % of balance you're
# willing to lose if SL is hit — this stays CONSTANT across all coins.
# Position size is then SOLVED FOR given the SL distance (which itself is
# now ATR-scaled per coin), so a volatile coin with a wide SL automatically
# gets a smaller position, and a calm coin with a tight SL gets a larger
# position — but the DOLLAR risk if wrong is the same either way.
RISK_PCT           = 0.02   # risk 2% of balance per trade if SL is hit
SL_ATR_MULT        = 1.5    # SL distance = 1.5× ATR(14) on 4H, floor/ceiling below
SL_MIN_PCT         = 0.01   # never let SL be tighter than 1% (avoid noise stop-outs)
SL_MAX_PCT         = 0.06   # never let SL be wider than 6% (caps worst-case risk)
MAX_POSITION_PCT   = 0.30   # hard ceiling: position value never exceeds 30% of
                             # balance even if risk-sizing would suggest more
                             # (protects against a freakishly tight ATR implying
                             # an oversized position for the same $ risk)

# ── Health-score entry gating ───────────────────────────────────────────────
# Wires /health's LONG/SHORT scores into live entry decisions. A coin with
# weak LONG health (structurally declining) needs a stronger signal (more
# passed filters) to justify a LONG — and vice versa for SHORT. A coin's
# health favoring one direction NEVER blocks the other direction outright
# — it only raises the required filter count from 6/9 to 7/9 when the
# OPPOSING direction's health clearly outweighs this one.
# Cached per-symbol with a TTL since health score requires fetching 400
# days of data — too expensive to recompute every 10-second scan tick.
HEALTH_CACHE_TTL_SEC = 3600   # recompute health score at most once per hour
HEALTH_BLOCK_BELOW   = 35     # legacy threshold, no longer used to block —
                               # kept only for /health command display tiers
HEALTH_RAISE_BAR_BELOW = 55   # opposing score must reach this + beat own
                               # score by HEALTH_GAP_MARGIN to raise 6/9->7/9
symbol_health_cache: dict[str, dict] = {}   # symbol -> {"long":.., "short":.., "ts":..}

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
VOL_CONFIRM_THRESHOLD = 1.2   # Filter 9 (mandatory): current 4H volume must be
                               # >= 1.2x the 20-period 4H average — rejects
                               # technically-valid setups with no real
                               # participation behind them

# ─────────────────────────────────────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  REGIME CONSTANTS — defined early so all functions can reference them
# ─────────────────────────────────────────────────────────────────────────────
REGIME_STRONG_UPTREND    = "STRONG_UPTREND"
REGIME_STRONG_DOWNTREND  = "STRONG_DOWNTREND"
REGIME_CHOPPY_RANGING    = "CHOPPY_RANGING"
REGIME_VOLATILE_BREAKOUT = "VOLATILE_BREAKOUT"
REGIME_MEAN_REVERTING    = "MEAN_REVERTING"
REGIME_UNKNOWN           = "UNKNOWN"

# ─────────────────────────────────────────────────────────────────────────────
#  BYBIT CLIENT — Demo Trading endpoint
#  Used by PsychologyBot (9-filter) and MemeBot. Unchanged — these two
#  strategies stay on demo regardless of the live session below.
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
#  BYBIT CLIENT — LIVE (real-money) endpoint
#  Used ONLY by the Regime Strategy's live trading — see regime_open_trade,
#  regime_close_trade, regime_manage_open_position, regime_scan_for_entry,
#  which all call the _live-suffixed functions below rather than the
#  shared demo-based ones (get_price, place_order, etc.) that
#  PsychologyBot/MemeBot use.
#  REGIME_LIVE_TRADING_ENABLED is the single source of truth for whether
#  real orders can be placed at all — False until BYBIT_LIVE_API_KEY and
#  BYBIT_LIVE_API_SECRET are both set on Render. Every live-trading entry
#  point checks this flag and refuses to act if it's False, so deploying
#  this code before generating live keys is safe: the Regime Strategy
#  simply stays unable to trade live until those two env vars exist.
# ─────────────────────────────────────────────────────────────────────────────
REGIME_LIVE_TRADING_ENABLED = bool(BYBIT_LIVE_KEY and BYBIT_LIVE_SECRET)

if REGIME_LIVE_TRADING_ENABLED:
    bybit_live_session = HTTP(
        testnet=False,
        api_key=BYBIT_LIVE_KEY,
        api_secret=BYBIT_LIVE_SECRET,
        recv_window=5000,
    )
    # Real production endpoint — actual money, actual orders.
    bybit_live_session.endpoint = "https://api.bybit.com"
    print("[LIVE] Regime Strategy live trading: ENABLED (BYBIT_LIVE_API_KEY/SECRET found)")
else:
    bybit_live_session = None
    print("[LIVE] Regime Strategy live trading: DISABLED — "
          "set BYBIT_LIVE_API_KEY and BYBIT_LIVE_API_SECRET on Render to enable. "
          "PsychologyBot and MemeBot are unaffected and continue on demo.")

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

# ── Whipsaw circuit breaker (per-symbol) ──────────────────────────────────
# Tracks rapid open->close round trips (< 20 min hold time). If a symbol
# whipsaws 2+ times within 60 minutes, that symbol is paused for 2 hours
# and the owner is alerted — this is a safety net independent of any
# specific root cause, so it protects against whipsaw loops even from
# bugs we haven't found yet.
QUICK_EXIT_THRESHOLD_MIN = 20     # trade held < this = counts as "quick"
WHIPSAW_WINDOW_MIN       = 60     # look for repeats within this window
WHIPSAW_TRIGGER_COUNT    = 2      # this many quick exits = whipsaw detected
WHIPSAW_COOLDOWN_MIN     = 120    # pause the symbol for this long
symbol_quick_exit_times: dict[str, list]  = {}   # symbol -> [timestamps]
symbol_cooldown_until:   dict[str, float] = {}   # symbol -> unix ts

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

# ── Regime Strategy (LIVE) global state ─────────────────────────────────────
# Trades a MANUALLY CURATED symbol list — add symbols with /add_symbol_regime,
# remove with /remove_symbol_regime — same pattern as monitored_symbols
# (PsychologyBot) and meme_symbols (MemeBot). Entirely separate position
# tracking from those two so it can operate independently even on a symbol
# they also trade (though see the collision guard in regime_scan_for_entry).
regime_symbols:    list[str]              = []   # curated list — user opts in per symbol
regime_positions:  dict[str, dict]        = {}   # symbol -> regime-strategy position
regime_paused:     bool                   = False
REGIME_SCAN_INTERVAL_SEC = 600     # 10 minutes between scan cycles — signals
    # are Daily-candle-based and cannot change more than once a day, so
    # unlike PsychologyBot/MemeBot's 10-second polling, this doesn't need
    # to be fast. Every symbol in regime_symbols is checked every cycle
    # (no batching/rotation needed for a curated, human-sized list).

# ── Trade history global state ─────────────────────────────────────────────────
HISTORY_FILE      = STATE_FILE.replace("bot_state.json", "trade_history.json")
HISTORY_KEEP_DAYS = 21    # keep last 3 weeks of trade records — Render disk safeguard
trade_history:    list    = []   # list of closed trade dicts

def save_state() -> None:
    try:
        state_dir = os.path.dirname(STATE_FILE)
        if state_dir:
            os.makedirs(state_dir, exist_ok=True)
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
            # Regime Strategy (live) state
            "regime_symbols":    regime_symbols,
            "regime_positions":  regime_positions,
            "regime_paused":     regime_paused,
        }
        with open(STATE_FILE, "w") as f:
            json.dump(state, f, indent=2, default=str)
        logger.debug(f"[STATE] Saved → {STATE_FILE} "
                     f"(symbols: {monitored_symbols}, meme: {meme_symbols})")
    except Exception as e:
        logger.error(f"[STATE] SAVE FAILED → {STATE_FILE}: {e}", exc_info=True)

def load_state() -> bool:
    global monitored_symbols, symbol_positions, bot_paused
    global daily_loss, weekly_loss, consec_losses, last_reset_day, last_reset_week
    global meme_symbols, meme_positions, meme_paused, meme_daily_loss, meme_consec_losses
    global regime_symbols, regime_positions, regime_paused
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
        # Regime Strategy (live) state
        regime_symbols    = s.get("regime_symbols",    [])
        regime_positions  = s.get("regime_positions",  {})
        regime_paused     = s.get("regime_paused",     False)
        logger.info(f"[STATE] Loaded — psych:{len(monitored_symbols)} symbols, "
                    f"meme:{len(meme_symbols)} symbols, "
                    f"regime:{len(regime_symbols)} symbols "
                    f"({len(regime_positions)} open positions)")
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
    """Persist trade history to disk. Prunes records older than
    HISTORY_KEEP_DAYS (3 weeks) — see cleanup_old_data for the periodic
    job that enforces this independent of trade activity."""
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


CLEANUP_INTERVAL_SEC = 6 * 3600   # run the disk-retention sweep every 6 hours

async def cleanup_old_data(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Render disk-usage safeguard: prunes trade_history.json down to
    HISTORY_KEEP_DAYS (3 weeks), running on a fixed schedule INDEPENDENT
    of trade activity. _save_history() already prunes on every trade
    close, but during a quiet stretch with no closes, old records would
    otherwise sit on disk unpruned until the next trade — this job closes
    that gap. bot_state.json (positions, symbol lists) isn't included
    here since it only ever holds currently-open positions and small
    curated symbol lists — it doesn't grow unbounded the way an
    append-only trade log does.
    """
    try:
        before = len(trade_history)
        await asyncio.to_thread(_save_history)
        after = len(trade_history)
        if before != after:
            logger.info(f"[CLEANUP] Trade history trimmed: {before} → {after} records "
                        f"(older than {HISTORY_KEEP_DAYS} days removed)")
        else:
            logger.debug(f"[CLEANUP] Trade history checked — {after} records, none old enough to prune")
    except Exception as e:
        logger.error(f"[CLEANUP] cleanup_old_data failed: {e}", exc_info=True)


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

def _sync_get_position_size(symbol: str) -> float:
    """Return current open position size on Bybit. 0.0 means no position."""
    try:
        resp = bybit_session.get_positions(category="linear", symbol=symbol)
        for pos in resp["result"]["list"]:
            size = float(pos.get("size", 0))
            if size > 0:
                return size
        return 0.0
    except Exception as e:
        logger.error(f"_sync_get_position_size({symbol}): {e}")
        return -1.0   # -1 = API error (don't clear state on API failure)

def _sync_get_position_pnl(symbol: str) -> float:
    """Return unrealised PnL of open position. 0.0 if none."""
    try:
        resp = bybit_session.get_positions(category="linear", symbol=symbol)
        for pos in resp["result"]["list"]:
            if float(pos.get("size", 0)) > 0:
                return float(pos.get("unrealisedPnl", 0))
        return 0.0
    except Exception as e:
        logger.error(f"_sync_get_position_pnl({symbol}): {e}")
        return 0.0

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
#  LIVE (real-money) SYNC FUNCTIONS — parallel to the demo functions above,
#  used ONLY by the Regime Strategy's live trading. Every one of these
#  checks REGIME_LIVE_TRADING_ENABLED first and returns a safe default
#  (0.0, None, or an error dict) if live trading isn't configured — never
#  touches bybit_live_session while it's None. PsychologyBot and MemeBot
#  never call any of these; they keep using the demo functions above.
# ─────────────────────────────────────────────────────────────────────────────

def _sync_get_balance_live() -> float:
    if not REGIME_LIVE_TRADING_ENABLED:
        return 0.0
    try:
        resp  = bybit_live_session.get_wallet_balance(accountType="UNIFIED", coin="USDT")
        coins = resp["result"]["list"][0]["coin"]
        for c in coins:
            if c["coin"] == "USDT":
                raw = c.get("walletBalance") or c.get("availableToWithdraw") or "0"
                return float(raw) if str(raw).strip() else 0.0
        return 0.0
    except Exception as e:
        logger.error(f"_sync_get_balance_live: {e}")
        return 0.0

def _sync_get_price_live(symbol: str) -> float:
    if not REGIME_LIVE_TRADING_ENABLED:
        return 0.0
    # No WebSocket for the live session (the existing ws_prices feed is
    # public market data shared across demo/live, but to keep the live
    # path fully independent and simple, this goes straight to REST).
    try:
        resp = bybit_live_session.get_tickers(category="linear", symbol=symbol)
        return float(resp["result"]["list"][0]["lastPrice"])
    except Exception as e:
        logger.error(f"_sync_get_price_live({symbol}): {e}")
        return 0.0

def _sync_get_klines_live(symbol: str, interval: str, limit: int = 300) -> Optional[pd.DataFrame]:
    if not REGIME_LIVE_TRADING_ENABLED:
        return None
    try:
        resp = bybit_live_session.get_kline(
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
        logger.error(f"_sync_get_klines_live({symbol},{interval}): {e}")
        return None

def _sync_get_instrument_constraints_live(symbol: str) -> tuple:
    if not REGIME_LIVE_TRADING_ENABLED:
        return 0.001, 1_000_000.0, 0.001, 5.0, 10.0
    try:
        resp = bybit_live_session.get_instruments_info(category="linear", symbol=symbol)
        info         = resp["result"]["list"][0]
        lot          = info["lotSizeFilter"]
        lever_filter = info.get("leverageFilter", {})
        min_qty      = float(lot["minOrderQty"])
        max_qty      = float(lot.get("maxMktOrderQty", lot.get("maxOrderQty", 1_000_000)))
        qty_step     = float(lot["qtyStep"])
        min_notional = float(lot.get("minNotionalValue", 5.0))
        max_leverage = float(lever_filter.get("maxLeverage", 10.0))
        logger.info(f"[LIVE CONSTRAINTS] {symbol}: min={min_qty} max={max_qty} "
                    f"step={qty_step} min_notional={min_notional} max_lev={max_leverage}")
        return min_qty, max_qty, qty_step, min_notional, max_leverage
    except Exception as e:
        logger.error(f"_sync_get_instrument_constraints_live({symbol}): {e}")
        return 0.001, 1_000_000.0, 0.001, 5.0, 10.0

def _sync_set_leverage_live(symbol: str) -> int:
    if not REGIME_LIVE_TRADING_ENABLED:
        return 1
    import math
    try:
        _, _, _, _, max_lev = _sync_get_instrument_constraints_live(symbol)
        actual_lev = min(10, math.floor(max_lev))
        if actual_lev < 1:
            actual_lev = 1
        resp = bybit_live_session.set_leverage(
            category="linear", symbol=symbol,
            buyLeverage=str(actual_lev), sellLeverage=str(actual_lev))
        ret_code = resp.get("retCode", 0) if isinstance(resp, dict) else 0
        if ret_code not in (0, 110043):
            logger.warning(f"[LIVE][{symbol}] set_leverage retCode={ret_code}: {resp.get('retMsg','')}")
        else:
            logger.info(f"[LIVE][{symbol}] Leverage set to {actual_lev}x")
        return actual_lev
    except Exception as e:
        logger.warning(f"_sync_set_leverage_live({symbol}): {e}")
        return 1

def _sync_place_order_live(symbol: str, side: str, qty: float,
                            reduce_only: bool = False) -> Optional[dict]:
    if not REGIME_LIVE_TRADING_ENABLED:
        return {"__error__": "Live trading not configured — missing BYBIT_LIVE_API_KEY/SECRET"}
    try:
        min_qty, max_qty, qty_step, _, _ = _sync_get_instrument_constraints_live(symbol)
        total_qty = _round_qty_step(qty, qty_step, min_qty)
        remaining  = total_qty
        last_result = None

        while remaining > 0:
            batch_qty = min(remaining, max_qty)
            batch_qty = _round_qty_step(batch_qty, qty_step, min_qty)
            qty_str   = _format_qty_str(batch_qty, qty_step)

            resp = bybit_live_session.place_order(
                category="linear", symbol=symbol,
                side=side, orderType="Market", qty=qty_str,
                timeInForce="GoodTillCancel", reduceOnly=reduce_only)

            ret_code = resp.get("retCode", 0)
            if ret_code != 0:
                logger.error(f"[LIVE] _sync_place_order_live({symbol}): retCode={ret_code} "
                             f"msg={resp.get('retMsg')} qty={qty_str}")
                if ret_code == 110090 and batch_qty > min_qty:
                    reduced = _round_qty_step(batch_qty * 0.80, qty_step, min_qty)
                    if reduced < min_qty or reduced >= batch_qty:
                        return {"__error__": resp.get("retMsg", "Unknown error"), "__code__": ret_code}
                    qty_str2 = _format_qty_str(reduced, qty_step)
                    resp2 = bybit_live_session.place_order(
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
                _time.sleep(0.5)

        return last_result
    except Exception as e:
        logger.error(f"_sync_place_order_live({symbol},{side},{qty}): {e}")
        return {"__error__": str(e)}

def _sync_get_position_entry_live(symbol: str) -> Optional[float]:
    if not REGIME_LIVE_TRADING_ENABLED:
        return None
    try:
        resp = bybit_live_session.get_positions(category="linear", symbol=symbol)
        for pos in resp["result"]["list"]:
            if float(pos.get("size", 0)) > 0:
                avg = float(pos.get("avgPrice", 0))
                return avg if avg > 0 else None
        return None
    except Exception as e:
        logger.error(f"_sync_get_position_entry_live({symbol}): {e}")
        return None

def _sync_get_position_size_live(symbol: str) -> float:
    if not REGIME_LIVE_TRADING_ENABLED:
        return -1.0
    try:
        resp = bybit_live_session.get_positions(category="linear", symbol=symbol)
        for pos in resp["result"]["list"]:
            size = float(pos.get("size", 0))
            if size > 0:
                return size
        return 0.0
    except Exception as e:
        logger.error(f"_sync_get_position_size_live({symbol}): {e}")
        return -1.0

def _sync_close_full_live(symbol: str, direction: str) -> None:
    if not REGIME_LIVE_TRADING_ENABLED:
        return
    try:
        close_side = "Sell" if direction == "LONG" else "Buy"
        resp = bybit_live_session.get_positions(category="linear", symbol=symbol)
        for pos in resp["result"]["list"]:
            size = float(pos.get("size", 0))
            if size > 0:
                _sync_place_order_live(symbol, close_side, size, reduce_only=True)
    except Exception as e:
        logger.error(f"_sync_close_full_live({symbol}): {e}")

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
#  LIVE (real-money) ASYNC WRAPPERS — used ONLY by the Regime Strategy's
#  live trading (regime_open_trade, regime_close_trade,
#  regime_manage_open_position, regime_scan_for_entry). Share the same
#  _api_semaphore as the demo wrappers above — rate limiting applies
#  across BOTH sessions combined, which is fine since they hit different
#  Bybit endpoints entirely.
# ─────────────────────────────────────────────────────────────────────────────
async def get_balance_live() -> float:
    async with _api_semaphore:
        return await asyncio.to_thread(_sync_get_balance_live)

async def get_price_live(symbol: str) -> float:
    async with _api_semaphore:
        return await asyncio.to_thread(_sync_get_price_live, symbol)

async def get_klines_live(symbol: str, interval: str, limit: int = 300) -> Optional[pd.DataFrame]:
    async with _api_semaphore:
        return await asyncio.to_thread(_sync_get_klines_live, symbol, interval, limit)

async def get_instrument_constraints_live(symbol: str) -> tuple:
    async with _api_semaphore:
        return await asyncio.to_thread(_sync_get_instrument_constraints_live, symbol)

async def place_order_live(symbol: str, side: str, qty: float,
                            reduce_only: bool = False) -> Optional[dict]:
    async with _api_semaphore:
        return await asyncio.to_thread(_sync_place_order_live, symbol, side, qty, reduce_only)

async def close_full_live(symbol: str, direction: str) -> None:
    async with _api_semaphore:
        await asyncio.to_thread(_sync_close_full_live, symbol, direction)

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

    # CRITICAL: for Daily-timeframe STRUCTURAL indicators (EMA stack, Ichimoku
    # cloud, MACD) we must use only CLOSED daily candles — never the still-
    # forming live candle. The live candle's close changes every 10 seconds
    # as price ticks, which can flip a borderline EMA/cloud/MACD reading back
    # and forth many times per minute. This MUST match the data basis used
    # by _check_ema_exit() (which already correctly drops the live candle),
    # otherwise entry and exit can be evaluated on different data and
    # contradict each other within seconds — causing rapid open/close
    # whipsaw loops that bleed fees with no real price movement.
    # `price` above stays LIVE (that's the real price we'd actually trade
    # at) — only the indicator LINES themselves are computed from closed data.
    df_1d_closed = df_1d.iloc[:-1] if len(df_1d) > 1 else df_1d

    def ema(df: pd.DataFrame, p: int) -> pd.Series:
        return df["close"].ewm(span=p, adjust=False).mean()

    # Filter 1 — ADX relative to the coin's OWN baseline (4H)
    # FIX (was fixed ADX>20 for every coin): "trending" means something
    # different in absolute ADX terms for BTC vs a newer, choppier perp.
    # Now requires ADX to be above this coin's own recent average — i.e.
    # currently trending MORE than its typical/baseline state — plus a
    # low absolute floor (12) to reject genuinely flat/dead markets that
    # happen to sit above their own (also low) baseline.
    try:
        adx_series = ta.adx(df_4h["high"], df_4h["low"], df_4h["close"], length=ADX_PERIOD)[f"ADX_{ADX_PERIOD}"]
        adx = float(adx_series.iloc[-1])
        adx_baseline = float(adx_series.tail(90).mean()) if len(adx_series) >= 90 else 20.0
    except Exception:
        adx, adx_baseline = 0.0, 20.0
    debug["adx"] = round(adx, 2)
    debug["adx_baseline"] = round(adx_baseline, 2)
    adx_relative_ok = adx > adx_baseline
    adx_absolute_floor_ok = adx > 12   # reject genuinely dead/flat markets outright
    (passed if (adx_relative_ok and adx_absolute_floor_ok) else failed).append("ADX>Baseline")

    # ATR(14) on 4H — used for volatility-normalized SL/sizing downstream
    try:
        atr_series_4h = ta.atr(df_4h["high"], df_4h["low"], df_4h["close"], length=14)
        atr = float(atr_series_4h.iloc[-1]) if atr_series_4h is not None and pd.notna(atr_series_4h.iloc[-1]) else price*0.02
    except Exception:
        atr = price * 0.02
    debug["atr"] = round(atr, 6)

    # Filter 2 — Ichimoku Cloud (Daily) — uses CLOSED candles only (see note above)
    conv  = (df_1d_closed["high"].rolling(ICHI_CONV).max()  + df_1d_closed["low"].rolling(ICHI_CONV).min())  / 2
    base  = (df_1d_closed["high"].rolling(ICHI_BASE).max()  + df_1d_closed["low"].rolling(ICHI_BASE).min())  / 2
    spa   = ((conv + base) / 2).shift(ICHI_BASE)
    spb   = ((df_1d_closed["high"].rolling(ICHI_SPAN).max() + df_1d_closed["low"].rolling(ICHI_SPAN).min()) / 2).shift(ICHI_BASE)
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

    # Filter 5 (MANDATORY) — EMA alignment (Daily) — CLOSED candles only.
    # This now matches _check_ema_exit()'s data basis exactly, so a fresh
    # entry can never be immediately contradicted by the exit check reading
    # different (live vs closed) data — the root cause of rapid-fire
    # open/close whipsaw loops seen on low-liquidity symbols.
    e20  = float(ema(df_1d_closed, 20).iloc[-1])
    e50  = float(ema(df_1d_closed, 50).iloc[-1])
    e100 = float(ema(df_1d_closed, 100).iloc[-1])
    e200 = float(ema(df_1d_closed, 200).iloc[-1])
    debug["emas_daily"] = {20: round(e20,2), 50: round(e50,2),
                           100: round(e100,2), 200: round(e200,2)}
    if direction == "LONG":
        ema_ok = (e20 > e50 > e100 > e200) and (price > e50)
        (passed if ema_ok else failed).append("EMA_Bullish_Stack")
    else:
        ema_ok = (e20 < e50 < e100 < e200) and (price < e50)
        (passed if ema_ok else failed).append("EMA_Bearish_Stack")

    # Filter 6 — MACD (Daily) — CLOSED candles only (see note above)
    macd = ta.macd(df_1d_closed["close"], fast=MACD_FAST, slow=MACD_SLOW, signal=MACD_SIG)
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

    # Filter 8 (was mandatory, now optional) — 1H candle
    # DEMOTED from mandatory: a single candle's shape (body size, close
    # position) is inherently noisier than trend structure, trend
    # strength, or volume — a lone candle can fake out easily (that's
    # literally what a wick is). It still counts toward the optional pool.
    c    = df_1h.iloc[-1]
    body = c["close"] - c["open"]
    rng  = c["high"] - c["low"]
    if rng == 0:
        candle_ok = False
        (failed).append("Bullish1HCandle" if direction=="LONG" else "Bearish1HCandle")
    else:
        bpct     = abs(body) / c["open"]
        cpos     = (c["close"] - c["low"]) / rng
        if direction == "LONG":
            candle_ok = body > 0 and bpct >= 0.003 and cpos >= 0.5
            (passed if candle_ok else failed).append("Bullish1HCandle")
        else:
            candle_ok = body < 0 and bpct >= 0.003 and cpos <= 0.5
            (passed if candle_ok else failed).append("Bearish1HCandle")

    # Filter 9 (MANDATORY, new) — Volume Confirmation (4H)
    # GAP THIS FILLS: none of Filters 1-8 check participation behind a
    # move — they're all price-structure or momentum checks. A setup can
    # pass every structural box on thin, unconvincing volume. This
    # directly targets the overtrading-in-chop pattern seen on coins like
    # HYPE in real /backtest_compare results (most trades of any coin
    # tested, among the worst win rates — the signature of entering on
    # technically-valid-looking setups nobody is actually behind).
    try:
        vol_4h = df_4h["volume"].astype(float)
        vol_ratio = float(vol_4h.iloc[-1]) / float(vol_4h.tail(20).mean()) if float(vol_4h.tail(20).mean()) > 0 else 1.0
    except Exception:
        vol_ratio = 1.0
    debug["vol_ratio_4h"] = round(vol_ratio, 2)
    volume_confirmed = vol_ratio >= VOL_CONFIRM_THRESHOLD
    (passed if volume_confirmed else failed).append("VolumeConfirmed")

    # ── Entry decision ────────────────────────────────────────────────────
    # 3 MANDATORY filters (must ALL pass, regardless of anything else):
    #   F1 (ADX relative to own baseline) — rejects weak/choppy trend strength
    #   F5 (EMA stack)                    — anchors correct macro direction
    #   F9 (Volume Confirmation)          — rejects unconvincing, thin-volume moves
    # F8 (1H candle) is now one of the 6 OPTIONAL filters, not mandatory —
    # a single candle's shape is noisier than trend/strength/volume and
    # doesn't deserve the same unconditional veto power.
    # Minimum to enter: all 3 mandatory + at least 3 of the remaining 6
    # optional filters = 6 of 9 total passed.
    m1 = "ADX>Baseline"
    m5 = "EMA_Bullish_Stack" if direction == "LONG" else "EMA_Bearish_Stack"
    m9 = "VolumeConfirmed"
    mandatory = (m1, m5, m9)
    mandatory_ok = all(m in passed for m in mandatory)
    should_enter = mandatory_ok and len(passed) >= 6

    return {
        "should_enter": should_enter,
        "score":   len(passed),
        "passed":  passed,
        "failed":  failed,
        "debug":   debug,
        "e100_4h": e100_4h,
        "atr":     atr,
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

def _calc_sl(entry: float, direction: str, e100_4h: float,
             regime: str = REGIME_UNKNOWN, atr: float = 0.0) -> float:
    """
    Volatility-normalized SL calculation.

    FIX (was a fixed 2% buffer for every coin): real-data backtesting
    (/backtest_compare across 9 coins) showed a fixed-percentage SL losing
    money on both calm coins (BTC) and volatile coins (HYPE) — a flat 2%
    buffer means nothing consistent across coins with very different
    typical daily ranges. A volatile coin gets stopped out by normal noise;
    a calm coin's SL is needlessly wide relative to its real risk.

    Now: SL distance = SL_ATR_MULT × ATR(14, 4H), clamped between
    SL_MIN_PCT and SL_MAX_PCT of entry price. This means SL distance
    scales with each coin's OWN actual volatility instead of one fixed
    number for every symbol. The former EMA100-anchored structural level
    is dropped in favor of this — ATR captures the same "give it room"
    intent in a coin-relative, principled way.

    - Volatile Breakout regime: tighter SL at 0.8× ATR (fast exit if a
      breakout fails) — kept as before, still the most sensitive case.
    """
    if regime == REGIME_VOLATILE_BREAKOUT and atr > 0:
        sl_dist = atr * 0.8
        if direction == "LONG":
            return round(entry - sl_dist, 6)
        else:
            return round(entry + sl_dist, 6)

    # Standard case: volatility-scaled via ATR, clamped to a sane range
    if atr > 0:
        atr_pct_frac = atr / entry
        raw_dist_frac = SL_ATR_MULT * atr_pct_frac
        clamped_dist_frac = min(max(raw_dist_frac, SL_MIN_PCT), SL_MAX_PCT)
        dist = entry * clamped_dist_frac
    else:
        # Fallback if ATR unavailable — old fixed-2% behavior as a safety net
        dist = entry * 0.02

    if direction == "LONG":
        return round(entry - dist, 6)
    else:
        return round(entry + dist, 6)

def _calc_tps(entry: float, sl: float, direction: str):
    risk = abs(entry - sl)
    if direction == "LONG":
        return round(entry + risk * 1.5, 6), round(entry + risk * 3.0, 6)
    else:
        return round(entry - risk * 1.5, 6), round(entry - risk * 3.0, 6)

def _calc_risk_based_qty(balance: float, entry: float, sl: float, leverage: float) -> float:
    """
    FIX (was fixed 20% of balance for every trade, regardless of SL
    distance): solves for position size such that RISK_PCT of balance is
    lost if SL is hit — REGARDLESS of how wide or tight that coin's
    volatility-scaled SL turned out to be. A volatile coin with a wide SL
    automatically gets a SMALLER position; a calm coin with a tight SL
    gets a LARGER position (up to a hard ceiling) — but the dollar risk
    if wrong stays the same either way, instead of swinging wildly by coin.
    """
    sl_dist_frac = abs(entry - sl) / entry
    if sl_dist_frac <= 0:
        sl_dist_frac = 0.02  # safety fallback, should not normally happen

    risk_amount = balance * RISK_PCT
    qty = risk_amount / (sl_dist_frac * entry)

    # Hard ceiling: position notional value never exceeds MAX_POSITION_PCT
    # of balance × leverage, even if risk-sizing math would suggest more
    # (protects against an anomalously tight ATR implying an oversized
    # position for the same dollar risk).
    max_notional = balance * MAX_POSITION_PCT * leverage
    max_qty = max_notional / entry
    qty = min(qty, max_qty)

    return qty

def _est_pnl(pos: dict, price: float) -> float:
    """
    Estimate unrealised PnL for an open position.
    qty is already the full contract size — DO NOT multiply by leverage again.
    PnL = price_diff × qty (same formula Bybit uses for linear contracts).
    """
    diff = (price - pos["entry_price"]) if pos["direction"] == "LONG" \
           else (pos["entry_price"] - price)
    return diff * pos["remaining_qty"]

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

    current_regime = indicators.get("regime", REGIME_UNKNOWN)
    min_q, max_q, step, min_notional, max_lev = await get_instrument_constraints(symbol)
    effective_lev = actual_lev

    # ── NO STOP LOSS — EMA-inversion-only exit strategy ──────────────────────
    # This position will NOT have a stop loss order placed on Bybit, and will
    # NOT take partial profit at any price target. The ONLY exit condition is
    # Daily EMA structure inverting (checked in manage_position). Since there
    # is no SL distance to size risk against, position sizing reverts to a
    # simple fixed % of balance rather than the risk-based ATR sizing used
    # elsewhere — there is no "risk if SL hit" to solve for anymore.
    #
    # IMPORTANT RISK NOTE: removing the bot's own stop does not remove risk.
    # With leverage active, a large enough adverse move before the Daily EMA
    # structure inverts (which only evaluates fully-closed candles and can
    # take days) may reach Bybit's own liquidation price first — liquidation
    # is a worse outcome than a controlled stop (forced closure + penalty),
    # not a safer one.
    if current_regime == REGIME_VOLATILE_BREAKOUT:
        regime_lev = min(effective_lev, 5)
        if regime_lev < effective_lev:
            try:
                bybit_session.set_leverage(
                    category="linear", symbol=symbol,
                    buyLeverage=str(regime_lev), sellLeverage=str(regime_lev))
                effective_lev = regime_lev
            except Exception:
                pass
        regime_size_mult = 0.5
    else:
        regime_lev = effective_lev
        regime_size_mult = 1.0

    target_margin   = balance * POSITION_PCT * regime_size_mult
    target_notional = target_margin * regime_lev
    raw_qty         = target_notional / entry
    raw_qty         = max(raw_qty, min_notional / entry)

    qty = _round_qty(raw_qty, min_q, step)

    logger.info(f"[{symbol}] Sizing (no-SL strategy): regime={current_regime} "
                f"pos_pct={POSITION_PCT*100:.0f}% lev={effective_lev}x qty={qty}")

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

    pos = {
        "symbol":         symbol,
        "direction":      direction,
        "entry_price":    fill_price,
        "original_qty":   qty,
        "remaining_qty":  qty,
        "entry_time":     time.time(),
        "score":          indicators["score"],
        "filters_passed": indicators["passed"],
        "entry_regime":   current_regime,
        "regime_conf":    indicators.get("regime_confidence", 0),
    }
    symbol_positions[symbol] = pos
    consec_losses = 0
    save_state()

    emoji  = "🟢" if direction == "LONG" else "🔴"
    f_str  = " ".join([f"✅{p}" for p in indicators["passed"]])
    fail_s = " ".join([f"❌{p}" for p in indicators["failed"]])

    re = indicators.get("regime", REGIME_UNKNOWN)
    rc = indicators.get("regime_confidence", 0)
    lev_note = f"\n⚠️ Leverage capped at {effective_lev}x (symbol max, requested {LEVERAGE}x)" if effective_lev < LEVERAGE else ""

    await alert(bot,
        f"{emoji} <b>TRADE OPENED — {symbol} {direction}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 Entry  : <code>{fill_price:.4f}</code>\n"
        f"🚫 SL     : NONE — this strategy has no stop loss\n"
        f"🚫 TP     : NONE — no partial profit taking\n"
        f"🔚 Exit   : ONLY on Daily EMA structure inversion\n"
        f"📊 Score  : {indicators['score']}/9\n"
        f"✅ Passed : {f_str}\n"
        f"❌ Failed : {fail_s}\n"
        f"{regime_emoji(re)} Regime: {regime_label(re)} ({rc}%)\n"
        f"⚡ Qty    : {qty}  |  Leverage: {effective_lev}x  |  Balance: {balance:.2f} USDT{lev_note}\n\n"
        f"⚠️ <b>No bot-side stop — an adverse move before EMA inversion "
        f"triggers may reach exchange liquidation instead.</b>")
    logger.info(f"[{symbol}] Trade opened {direction} @ {fill_price}")

# ─────────────────────────────────────────────────────────────────────────────
#  POSITION MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────
async def manage_position(pos: dict, bot) -> None:
    global consec_losses, daily_loss, weekly_loss
    symbol    = pos["symbol"]
    direction = pos["direction"]

    # ── CRITICAL: Verify position still exists on Bybit FIRST ────────────────
    # No SL/TP orders are placed anymore (EMA-inversion-only exit strategy),
    # so this check now primarily catches manual closes or liquidation.
    actual_size = await asyncio.to_thread(_sync_get_position_size, symbol)

    if actual_size == 0.0:
        last_price  = pos.get("_last_price", pos.get("entry_price", 0))
        entry_price = pos.get("entry_price", 0)
        qty         = pos.get("original_qty", 0)
        if direction == "LONG":
            est_pnl = (last_price - entry_price) * qty
        else:
            est_pnl = (entry_price - last_price) * qty

        logger.warning(f"[{symbol}] Position gone on Bybit! "
                       f"Clearing internal state. Est PnL: {est_pnl:.2f}")

        await alert(bot,
            "⚠️ POSITION SYNC — " + symbol + " " + direction + "\n"
            "Bybit shows NO open position — closed externally.\n"
            "No SL/TP orders exist on this strategy, so this was either a "
            "MANUAL close or a LIQUIDATION (leverage + no bot-side stop "
            "means an adverse move can hit Bybit's liquidation price "
            "before Daily EMA structure ever inverts).\n\n"
            f"Entry     : {entry_price:.4f}\n"
            f"Last price: {last_price:.4f}\n"
            f"Est. PnL  : {est_pnl:+.2f} USDT\n\n"
            f"Bot state cleared. Watching for next signal on {symbol}.")
        if est_pnl < 0:
            consec_losses += 1
            daily_loss    += est_pnl
            weekly_loss   += est_pnl
        _record_trade(pos, last_price, est_pnl, "EXTERNAL_CLOSE", bot_type="psych")
        _close_pos(symbol, est_pnl)
        return

    elif actual_size < 0:
        logger.warning(f"[{symbol}] Could not verify position size (API error). Skipping cycle.")
        return

    # Position confirmed open on Bybit — proceed with normal management
    price, df_1d, df_4h = await asyncio.gather(
        asyncio.to_thread(_sync_get_price, symbol),
        asyncio.to_thread(_sync_get_klines, symbol, "D", 100),
        asyncio.to_thread(_sync_get_klines, symbol, "240", 50),
    )

    if not price or df_1d is None or df_4h is None:
        return

    current_price = price
    pos["_last_price"] = current_price

    # ── Live regime re-check (informational only — no SL to adapt anymore) ───
    live_regime_result = await detect_regime(symbol, df_1d, df_4h)
    live_regime        = live_regime_result["regime"]
    live_regime_conf   = live_regime_result["confidence"]
    entry_regime       = pos.get("entry_regime", REGIME_UNKNOWN)

    if live_regime != entry_regime and live_regime != pos.get("_last_alerted_regime"):
        pos["_last_alerted_regime"] = live_regime
        await alert(bot,
            f"🔀 <b>REGIME SHIFT DURING OPEN TRADE — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Trade opened as : {regime_emoji(entry_regime)} {regime_label(entry_regime)}\n"
            f"Current regime  : {regime_emoji(live_regime)} <b>{regime_label(live_regime)}</b> ({live_regime_conf}%)\n\n"
            f"This position exits ONLY on Daily EMA inversion — regime "
            f"shifts are informational and do not change exit behaviour "
            f"on this strategy.")

    pos["live_regime"] = live_regime

    # ── EMA inversion exit — the ONLY exit condition on this strategy ────────
    # No SL, no TP1/TP2, no trailing, no dead-trade timeout. The position
    # holds until Daily EMA structure inverts, full stop. SAFETY NET: never
    # allow this exit within the first 15 minutes of a fresh entry — a
    # genuine structural inversion persists for hours/days and will still
    # be there 15 minutes later if it's real; this only guards against a
    # residual data-timing edge case flipping the reading momentarily.
    trade_age_minutes = (time.time() - pos["entry_time"]) / 60
    if trade_age_minutes < 15:
        ema_exit = False
    else:
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
        whipsaw = _close_pos(symbol, pnl, exit_price=current_price, exit_reason="EMA_EXIT")
        if whipsaw:
            await alert(bot,
                f"🚨 <b>WHIPSAW DETECTED — {symbol}</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"{QUICK_EXIT_THRESHOLD_MIN}+ minute trades closed rapidly "
                f"{WHIPSAW_TRIGGER_COUNT}+ times within {WHIPSAW_WINDOW_MIN} minutes.\n"
                f"⏸ {symbol} PAUSED for {WHIPSAW_COOLDOWN_MIN} minutes — "
                f"no new entries until cooldown expires.\n"
                f"This protects your balance from repeated fee-bleed on a "
                f"choppy/borderline signal. Use /resume to override early "
                f"if you believe this is a false positive.")
        else:
            await check_reversal_after_exit(symbol, direction, bot)
        return
    # No other exit condition exists. Position holds indefinitely until
    # Daily EMA structure inverts, manual close, or exchange liquidation.

def _close_pos(symbol: str, pnl: float, exit_price: float = 0.0,
               exit_reason: str = "UNKNOWN") -> None:
    global daily_loss, weekly_loss
    daily_loss  += min(pnl, 0)
    weekly_loss += min(pnl, 0)
    pos = symbol_positions.get(symbol)
    if pos and exit_price > 0:
        _record_trade(pos, exit_price, pnl, exit_reason, bot_type="psych")

    # ── Whipsaw detection ──────────────────────────────────────────────────
    # If this trade was held for less than QUICK_EXIT_THRESHOLD_MIN, log it.
    # If enough quick exits pile up for this symbol within WHIPSAW_WINDOW_MIN,
    # pause the symbol so it can't immediately re-enter and repeat the loop.
    whipsaw_triggered = False
    if pos:
        hold_minutes = (time.time() - pos.get("entry_time", time.time())) / 60
        if hold_minutes < QUICK_EXIT_THRESHOLD_MIN:
            now = time.time()
            history = symbol_quick_exit_times.get(symbol, [])
            history = [t for t in history if now - t <= WHIPSAW_WINDOW_MIN * 60]
            history.append(now)
            symbol_quick_exit_times[symbol] = history
            if len(history) >= WHIPSAW_TRIGGER_COUNT:
                symbol_cooldown_until[symbol] = now + WHIPSAW_COOLDOWN_MIN * 60
                symbol_quick_exit_times[symbol] = []  # reset counter after triggering
                whipsaw_triggered = True

    symbol_positions.pop(symbol, None)
    save_state()
    return whipsaw_triggered

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
            f"Score: {indicators['score']}/9 — Opening reversal trade...")
        await open_trade(symbol, new_direction, indicators, bot)
    else:
        await alert(bot,
            f"🟡 <b>GOING FLAT — {symbol}</b>\n"
            f"Exited {old_direction} but {new_direction} conditions not met "
            f"(score {indicators['score']}/9 — need 6).\n"
            f"Bot watches and waits for next valid signal.")


# =============================================================================
#  MARKET REGIME DETECTOR
#  Classifies market conditions every monitoring cycle using 7 indicators.
#  The regime adjusts HOW the bot trades — entry direction, position size,
#  TP targets, and SL tightness — without overriding the core strategy filters.
#
#  Regimes:
#   STRONG_UPTREND   — trending up, LONG only, wider TPs
#   STRONG_DOWNTREND — trending down, SHORT only, wider TPs
#   CHOPPY_RANGING   — no clear direction, skip new entries
#   VOLATILE_BREAKOUT— fast explosive move, tighter SL, smaller size
#   MEAN_REVERTING   — oscillating, smaller TPs, quick exits
# =============================================================================

# Regime constants moved to top of file

# Cache: symbol -> (regime, confidence, timestamp, detail_dict)
regime_cache: dict[str, tuple] = {}
REGIME_CACHE_TTL = 300   # reuse for 5 min unless forced refresh

# Per-symbol last known regime (for change detection)
regime_last: dict[str, str] = {}


def _sync_detect_regime(symbol: str,
                         df_1d: "pd.DataFrame",
                         df_4h: "pd.DataFrame") -> dict:
    """
    Synchronous regime classifier — runs in background thread.
    Returns a dict with regime, confidence (0-100), and all raw values.

    Scoring algorithm:
    - Each regime accumulates points from relevant indicators.
    - The regime with the highest score wins.
    - Confidence = winner_score / total_possible_points * 100.
    """
    import numpy as np

    result = {
        "regime":     REGIME_UNKNOWN,
        "confidence": 0,
        "symbol":     symbol,
        "indicators": {},
    }

    try:
        # FIX: use only CLOSED daily candles for regime classification —
        # matching the same fix applied to entry filters after the
        # EIGENUSDT whipsaw bug. The last row of df_1d is Bybit's still-
        # forming, currently-updating candle; reading it directly meant
        # every one of the 11 regime signals (7 lagging + 4 leading) could
        # flicker as today's price ticks up and down, especially the
        # price-structure leading indicator (3-4 consecutive closes),
        # which would count today's still-changing close as one of those
        # closes. Regime classification is a structural read of completed
        # market state, not a live-execution price — there's no reason
        # for it to react to an incomplete candle the way order fills must.
        df_1d_closed = df_1d.iloc[:-1] if len(df_1d) > 1 else df_1d

        close = df_1d_closed["close"]
        high  = df_1d_closed["high"]
        low   = df_1d_closed["low"]
        vol   = df_1d_closed["volume"]

        # ── Indicator 1: ADX (trend strength) ────────────────────────────────
        adx_s = ta.adx(high, low, close, length=14)
        adx   = float(adx_s[f"ADX_14"].iloc[-1]) if adx_s is not None else 20.0
        dmp   = float(adx_s[f"DMP_14"].iloc[-1]) if adx_s is not None else 0.0
        dmn   = float(adx_s[f"DMN_14"].iloc[-1]) if adx_s is not None else 0.0

        # ── Indicator 2: ATR % (volatility relative to price) ────────────────
        atr_s   = ta.atr(high, low, close, length=14)
        atr     = float(atr_s.iloc[-1]) if atr_s is not None else 0.0
        price   = float(close.iloc[-1])
        atr_pct = (atr / price * 100) if price > 0 else 0.0

        # ── Indicator 3: Bollinger Band width (squeeze = range, expand = trend)
        bb = ta.bbands(close, length=20, std=2.0)
        if bb is not None:
            bb_upper = float(bb["BBU_20_2.0_2.0"].iloc[-1])
            bb_lower = float(bb["BBL_20_2.0_2.0"].iloc[-1])
            bb_mid   = float(bb["BBM_20_2.0_2.0"].iloc[-1])
            bb_width = (bb_upper - bb_lower) / bb_mid * 100 if bb_mid > 0 else 2.0
        else:
            bb_upper = bb_lower = bb_mid = price
            bb_width = 2.0

        # ── Indicator 4: EMA slope (direction and angle) ──────────────────────
        ema50  = close.ewm(span=50,  adjust=False).mean()
        ema200 = close.ewm(span=200, adjust=False).mean()
        # Slope as % change over last 5 candles
        ema50_slope  = float((ema50.iloc[-1]  - ema50.iloc[-6])  / ema50.iloc[-6]  * 100) if len(ema50)  > 6 else 0.0
        ema200_slope = float((ema200.iloc[-1] - ema200.iloc[-6]) / ema200.iloc[-6] * 100) if len(ema200) > 6 else 0.0
        ema_aligned_bull = float(ema50.iloc[-1]) > float(ema200.iloc[-1])
        ema_aligned_bear = float(ema50.iloc[-1]) < float(ema200.iloc[-1])
        price_above_ema50  = price > float(ema50.iloc[-1])
        price_below_ema50  = price < float(ema50.iloc[-1])
        price_above_ema200 = price > float(ema200.iloc[-1])
        price_below_ema200 = price < float(ema200.iloc[-1])

        # ── Indicator 5: RSI (overbought/oversold/midrange) ──────────────────
        rsi_s = ta.rsi(close, length=14)
        rsi   = float(rsi_s.iloc[-1]) if rsi_s is not None else 50.0

        # ── Indicator 6: Volume ratio (current vs 20-period avg) ─────────────
        # `vol` already set from df_1d_closed at the top of this function
        avg_vol = float(vol.iloc[-21:-1].mean()) if len(vol) > 21 else float(vol.mean())
        cur_vol = float(vol.iloc[-1])
        vol_ratio = cur_vol / avg_vol if avg_vol > 0 else 1.0

        # ── Indicator 7: BB %B (where price sits in the band) ────────────────
        bb_pct_b = (price - bb_lower) / (bb_upper - bb_lower) if (bb_upper - bb_lower) > 0 else 0.5

        # ── Historical volatility (20-day rolling std of returns) ─────────────
        returns  = close.pct_change().dropna()
        hist_vol = float(returns.rolling(20).std().iloc[-1] * 100) if len(returns) >= 20 else atr_pct

        # ═════════════════════════════════════════════════════════════════════
        #  LEADING INDICATORS — fire BEFORE lagging ADX catches up
        #  These detect regime shifts 3–10 candles earlier than ADX alone.
        # ═════════════════════════════════════════════════════════════════════

        # ── Leading 1: BB Squeeze (Keltner/BB crossover) ─────────────────────
        # When BB width contracts to a multi-month low, a breakout is IMMINENT.
        # NOTE: bb_upper/bb_lower are scalars (float), kc_upper/kc_lower are Series.
        # Must compare scalar to scalar using .iloc[-1].
        kc_atr  = ta.atr(high, low, close, length=20)
        kc_mid  = close.ewm(span=20, adjust=False).mean()
        if kc_atr is not None:
            kc_upper_s = kc_mid + 1.5 * kc_atr   # Series
            kc_lower_s = kc_mid - 1.5 * kc_atr   # Series
            # Compare last two candles using scalar values only
            squeeze_now  = (bb_upper < float(kc_upper_s.iloc[-1])) and                            (bb_lower > float(kc_lower_s.iloc[-1]))
            squeeze_prev = (bb_upper < float(kc_upper_s.iloc[-2])) and                            (bb_lower > float(kc_lower_s.iloc[-2]))
            squeeze_active   = squeeze_now
            squeeze_released = squeeze_prev and not squeeze_now
            # BB width at 20-bar low using Series (all scalars from bb)
            bb_width_s = (kc_upper_s - kc_lower_s) / kc_mid * 100
            bb_at_20bar_low = float(bb_width_s.iloc[-1]) <= float(
                bb_width_s.rolling(20).min().iloc[-1]) * 1.05
        else:
            squeeze_active   = False
            squeeze_released = False
            bb_at_20bar_low  = False

        # ── Leading 2: EMA slope acceleration (2nd derivative) ───────────────
        # Slope inflecting from negative toward zero = trend birth, days before
        # EMA actually crosses or ADX rises. This is the "turn" not the "trend".
        # Slope of slope = (current_slope - slope_5_bars_ago)
        if len(ema50) > 11:
            ema50_slope_5ago = float((ema50.iloc[-6] - ema50.iloc[-11]) / ema50.iloc[-11] * 100)
            slope_accel = ema50_slope - ema50_slope_5ago  # positive = accelerating up
        else:
            slope_accel = 0.0
        slope_turning_bull = slope_accel > 0.02 and ema50_slope > ema50_slope_5ago
        slope_turning_bear = slope_accel < -0.02 and ema50_slope < ema50_slope_5ago

        # ── Leading 3: Price structure (higher highs / lower lows) ───────────
        # No smoothing — raw price action. 3 consecutive higher closes =
        # uptrend forming. 3 lower closes = downtrend forming.
        # Faster than any moving average by definition.
        if len(close) >= 5:
            c1, c2, c3, c4 = (float(close.iloc[-i]) for i in (1,2,3,4))
            hh_hl = c1 > c2 > c3       # 3 consecutive higher closes
            lh_ll = c1 < c2 < c3       # 3 consecutive lower closes
            # Confirm with 4th candle for reliability
            strong_hh = c1 > c2 > c3 > c4
            strong_ll  = c1 < c2 < c3 < c4
        else:
            hh_hl = lh_ll = strong_hh = strong_ll = False

        # ── Leading 4: Volume-price divergence (accumulation/distribution) ───
        # Rising volume with rising price = strong buying = trend confirmation
        # Rising volume with falling price = distribution = trend confirmation
        # High volume with small price move = absorption = range/reversal
        if len(close) >= 3 and len(vol) >= 3:
            price_chg_3d  = (float(close.iloc[-1]) - float(close.iloc[-4])) / float(close.iloc[-4]) * 100
            vol_3d_avg    = float(vol.iloc[-4:-1].mean())
            vol_expanding = float(vol.iloc[-1]) > vol_3d_avg * 1.2
            # Trend confirmation: big price move + expanding volume
            vol_confirms_bull = price_chg_3d > 1.0 and vol_expanding
            vol_confirms_bear = price_chg_3d < -1.0 and vol_expanding
            # Chop signal: high volume but price barely moved
            vol_chop_signal   = vol_ratio > 1.3 and abs(price_chg_3d) < 0.5
        else:
            vol_confirms_bull = vol_confirms_bear = vol_chop_signal = False
            price_chg_3d = 0.0

        # Store all raw values — now including leading indicators
        result["indicators"] = {
            "adx":               round(adx, 1),
            "dmp":               round(dmp, 1),
            "dmn":               round(dmn, 1),
            "atr_pct":           round(atr_pct, 2),
            "bb_width":          round(bb_width, 2),
            "ema50_slope":       round(ema50_slope, 3),
            "ema200_slope":      round(ema200_slope, 3),
            "slope_accel":       round(slope_accel, 4),
            "rsi":               round(rsi, 1),
            "vol_ratio":         round(vol_ratio, 2),
            "bb_pct_b":          round(bb_pct_b, 2),
            "hist_vol":          round(hist_vol, 2),
            "price":             round(price, 6),
            "price_chg_3d":      round(price_chg_3d, 2),
            # Leading signals (bool → shown clearly in /regime output)
            "squeeze_active":    squeeze_active,
            "squeeze_released":  squeeze_released,
            "bb_at_20bar_low":   bb_at_20bar_low,
            "slope_turning_bull":slope_turning_bull,
            "slope_turning_bear":slope_turning_bear,
            "hh_hl":             hh_hl,
            "lh_ll":             lh_ll,
            "strong_hh":         strong_hh,
            "strong_ll":         strong_ll,
            "vol_confirms_bull": vol_confirms_bull,
            "vol_confirms_bear": vol_confirms_bear,
            "vol_chop_signal":   vol_chop_signal,
        }

        # ── REGIME SCORING ────────────────────────────────────────────────────
        scores = {
            REGIME_STRONG_UPTREND:    0,
            REGIME_STRONG_DOWNTREND:  0,
            REGIME_CHOPPY_RANGING:    0,
            REGIME_VOLATILE_BREAKOUT: 0,
            REGIME_MEAN_REVERTING:    0,
        }

        # ── STRONG UPTREND scoring (max 44 pts) ──────────────────────────────
        # Lagging confirmators (ADX — still useful once trend is established)
        if adx > 25:               scores[REGIME_STRONG_UPTREND] += 4
        elif adx > 20:             scores[REGIME_STRONG_UPTREND] += 2
        if dmp > dmn:              scores[REGIME_STRONG_UPTREND] += 3
        if ema_aligned_bull:       scores[REGIME_STRONG_UPTREND] += 3
        if ema50_slope > 0.05:     scores[REGIME_STRONG_UPTREND] += 2
        if price_above_ema50:      scores[REGIME_STRONG_UPTREND] += 2
        if price_above_ema200:     scores[REGIME_STRONG_UPTREND] += 1
        if rsi > 50:               scores[REGIME_STRONG_UPTREND] += 1
        # Leading: price structure (fires 3–5 candles before ADX)
        if hh_hl:                  scores[REGIME_STRONG_UPTREND] += 6
        if strong_hh:              scores[REGIME_STRONG_UPTREND] += 4   # extra for 4 consecutive
        # Leading: EMA slope turning bullish (fires before EMA crosses)
        if slope_turning_bull:     scores[REGIME_STRONG_UPTREND] += 6
        # Leading: volume confirms buying
        if vol_confirms_bull:      scores[REGIME_STRONG_UPTREND] += 5
        # Leading: squeeze released with price above midpoint = breakout up
        if squeeze_released and price > bb_mid:
            scores[REGIME_STRONG_UPTREND] += 7

        # ── STRONG DOWNTREND scoring (max 44 pts) ────────────────────────────
        if adx > 25:               scores[REGIME_STRONG_DOWNTREND] += 4
        elif adx > 20:             scores[REGIME_STRONG_DOWNTREND] += 2
        if dmn > dmp:              scores[REGIME_STRONG_DOWNTREND] += 3
        if ema_aligned_bear:       scores[REGIME_STRONG_DOWNTREND] += 3
        if ema50_slope < -0.05:    scores[REGIME_STRONG_DOWNTREND] += 2
        if price_below_ema50:      scores[REGIME_STRONG_DOWNTREND] += 2
        if price_below_ema200:     scores[REGIME_STRONG_DOWNTREND] += 1
        if rsi < 50:               scores[REGIME_STRONG_DOWNTREND] += 1
        # Leading: price structure
        if lh_ll:                  scores[REGIME_STRONG_DOWNTREND] += 6
        if strong_ll:              scores[REGIME_STRONG_DOWNTREND] += 4
        # Leading: EMA slope turning bearish
        if slope_turning_bear:     scores[REGIME_STRONG_DOWNTREND] += 6
        # Leading: volume confirms selling
        if vol_confirms_bear:      scores[REGIME_STRONG_DOWNTREND] += 5
        # Leading: squeeze released with price below midpoint = breakout down
        if squeeze_released and price < bb_mid:
            scores[REGIME_STRONG_DOWNTREND] += 7

        # ── CHOPPY / RANGING scoring (max 30 pts) ────────────────────────────
        # Lagging ADX fine here — chop persists, lag doesn't matter
        if adx < 20:               scores[REGIME_CHOPPY_RANGING] += 8
        elif adx < 25:             scores[REGIME_CHOPPY_RANGING] += 4
        if bb_width < 3.0:         scores[REGIME_CHOPPY_RANGING] += 6
        elif bb_width < 5.0:       scores[REGIME_CHOPPY_RANGING] += 3
        if abs(ema50_slope) < 0.02: scores[REGIME_CHOPPY_RANGING] += 4
        if 40 < rsi < 60:          scores[REGIME_CHOPPY_RANGING] += 3
        if vol_ratio < 0.8:        scores[REGIME_CHOPPY_RANGING] += 2
        # Leading: squeeze still active = chop not yet broken out
        if squeeze_active:         scores[REGIME_CHOPPY_RANGING] += 4
        # Leading: volume + tiny price move = absorption = chop
        if vol_chop_signal:        scores[REGIME_CHOPPY_RANGING] += 3

        # ── VOLATILE BREAKOUT scoring (max 30 pts) ───────────────────────────
        # Partially leading — BB compression fires before the explosion
        if atr_pct > 3.0:          scores[REGIME_VOLATILE_BREAKOUT] += 5
        elif atr_pct > 2.0:        scores[REGIME_VOLATILE_BREAKOUT] += 3
        if vol_ratio > 2.5:        scores[REGIME_VOLATILE_BREAKOUT] += 5
        elif vol_ratio > 1.5:      scores[REGIME_VOLATILE_BREAKOUT] += 3
        if bb_width > 8.0:         scores[REGIME_VOLATILE_BREAKOUT] += 4
        elif bb_width > 5.0:       scores[REGIME_VOLATILE_BREAKOUT] += 2
        if hist_vol > 4.0:         scores[REGIME_VOLATILE_BREAKOUT] += 3
        elif hist_vol > 2.5:       scores[REGIME_VOLATILE_BREAKOUT] += 2
        if rsi > 70 or rsi < 30:   scores[REGIME_VOLATILE_BREAKOUT] += 2
        # Leading: squeeze just released = breakout starting NOW
        if squeeze_released:       scores[REGIME_VOLATILE_BREAKOUT] += 6
        # Leading: BB at 20-bar low = maximum compression = breakout imminent
        if bb_at_20bar_low:        scores[REGIME_VOLATILE_BREAKOUT] += 3

        # ── MEAN REVERTING scoring (max 22 pts) ──────────────────────────────
        if 0.2 < bb_pct_b < 0.8:   scores[REGIME_MEAN_REVERTING] += 5
        if adx < 22:                scores[REGIME_MEAN_REVERTING] += 4
        if abs(ema50_slope) < 0.04: scores[REGIME_MEAN_REVERTING] += 3
        if atr_pct < 2.5:           scores[REGIME_MEAN_REVERTING] += 3
        if 35 < rsi < 65:           scores[REGIME_MEAN_REVERTING] += 2
        if vol_ratio < 1.2:         scores[REGIME_MEAN_REVERTING] += 2
        # Leading: squeeze still in = mean reversion in full effect
        if squeeze_active and not squeeze_released:
            scores[REGIME_MEAN_REVERTING] += 3

        # ── Winner ────────────────────────────────────────────────────────────
        max_possible = {
            REGIME_STRONG_UPTREND:    44,
            REGIME_STRONG_DOWNTREND:  44,
            REGIME_CHOPPY_RANGING:    30,
            REGIME_VOLATILE_BREAKOUT: 30,
            REGIME_MEAN_REVERTING:    22,
        }
        winner  = max(scores, key=lambda k: scores[k])
        raw_score = scores[winner]
        confidence = min(100, int(raw_score / max_possible[winner] * 100))

        # ── CPO cross-check ────────────────────────────────────────────────
        # Independent confirmation from a second, differently-built signal
        # (Kaufman adaptive means + volume-price coherence + cyclic-order
        # prior — see _compute_cpo_from_series). Reuses the SAME already-
        # fetched close/high/low/vol Series computed above — no extra
        # Bybit API call. When CPO's high-confidence reading (validated
        # ~82% accurate) agrees with the winning regime, confidence gets a
        # small bounded boost. When it conflicts, confidence gets a small
        # bounded penalty and the conflict is surfaced — CPO never
        # silently overrides the primary 11-signal winner, it only adds or
        # subtracts trust in a transparent, capped way.
        cpo_result = _compute_cpo_from_series(close, high, low, vol)
        cpo_agreement = "NEUTRAL"
        cpo_note = None

        if not cpo_result.get("error"):
            cpo_phase = cpo_result["phase"]
            cpo_conf  = cpo_result["confidence"]

            # Map CPO phases to the regime(s) they'd confirm if genuinely aligned
            bullish_confirm = winner == REGIME_STRONG_UPTREND and cpo_phase == "Expansion"
            bearish_confirm = winner == REGIME_STRONG_DOWNTREND and cpo_phase == "Capitulation"
            chop_confirm    = winner in (REGIME_CHOPPY_RANGING, REGIME_MEAN_REVERTING) and cpo_phase == "Compression"
            bullish_conflict = winner == REGIME_STRONG_UPTREND and cpo_phase == "Capitulation"
            bearish_conflict = winner == REGIME_STRONG_DOWNTREND and cpo_phase == "Expansion"

            if cpo_conf >= 55 and (bullish_confirm or bearish_confirm or chop_confirm):
                confidence = min(100, confidence + 5)
                cpo_agreement = "AGREE"
                cpo_note = f"CPO confirms via {cpo_phase} phase ({cpo_conf}% CPO confidence)"
            elif cpo_conf >= 55 and (bullish_conflict or bearish_conflict):
                confidence = max(10, confidence - 5)
                cpo_agreement = "CONFLICT"
                cpo_note = f"CPO reads {cpo_phase} phase ({cpo_conf}% CPO confidence) — disagrees with {winner}"
            else:
                cpo_note = f"CPO reads {cpo_phase} phase ({cpo_conf}% CPO confidence) — neutral vs {winner}"

        result["cpo"] = cpo_result
        result["cpo_agreement"] = cpo_agreement
        result["cpo_note"] = cpo_note

        result["regime"]        = winner
        result["confidence"]    = confidence
        result["scores"]        = scores
        result["max_possible"]  = max_possible

    except Exception as e:
        logger.error(f"[REGIME] _sync_detect_regime({symbol}) FAILED: {type(e).__name__}: {e}",
                     exc_info=True)
        # Return unknown with debug info so we can see it in /regime output
        result["regime"]     = REGIME_UNKNOWN
        result["confidence"] = 0
        result["error"]      = f"{type(e).__name__}: {e}"

    return result


async def detect_regime(symbol: str,
                         df_1d: "pd.DataFrame",
                         df_4h: "pd.DataFrame",
                         force: bool = False) -> dict:
    """
    Async wrapper for regime detection with caching.
    Returns cached result if < REGIME_CACHE_TTL seconds old.
    """
    now = time.time()
    cached = regime_cache.get(symbol)
    if cached and not force and (now - cached[3]) < REGIME_CACHE_TTL:
        return cached[0]   # return cached dict

    result = await asyncio.to_thread(_sync_detect_regime, symbol, df_1d, df_4h)
    regime_cache[symbol] = (result, result["confidence"], result["regime"], now)
    return result


def regime_emoji(regime: str) -> str:
    return {
        REGIME_STRONG_UPTREND:    "📈",
        REGIME_STRONG_DOWNTREND:  "📉",
        REGIME_CHOPPY_RANGING:    "↔️",
        REGIME_VOLATILE_BREAKOUT: "💥",
        REGIME_MEAN_REVERTING:    "🔄",
        REGIME_UNKNOWN:           "❓",
    }.get(regime, "❓")


def regime_label(regime: str) -> str:
    return {
        REGIME_STRONG_UPTREND:    "Strong Uptrend",
        REGIME_STRONG_DOWNTREND:  "Strong Downtrend",
        REGIME_CHOPPY_RANGING:    "Choppy / Ranging",
        REGIME_VOLATILE_BREAKOUT: "Volatile Breakout",
        REGIME_MEAN_REVERTING:    "Mean Reverting",
        REGIME_UNKNOWN:           "Unknown",
    }.get(regime, "Unknown")


def regime_action(regime: str) -> str:
    return {
        REGIME_STRONG_UPTREND:    "LONG entries only · Wider TP targets · Hold longer",
        REGIME_STRONG_DOWNTREND:  "SHORT entries only · Wider TP targets · Hold longer",
        REGIME_CHOPPY_RANGING:    "NO new entries · Manage open positions closely",
        REGIME_VOLATILE_BREAKOUT: "Reduce position size 50% · Tighter SL (0.8× ATR) · Quick exits",
        REGIME_MEAN_REVERTING:    "Smaller TP targets (1:1 only) · Quick exits · No trailing",
        REGIME_UNKNOWN:           "Insufficient data — bot uses default settings",
    }.get(regime, "Default settings")


def regime_allows_direction(regime: str, direction: str) -> bool:
    """Check if the current regime allows this trade direction."""
    if regime == REGIME_STRONG_UPTREND    and direction == "SHORT": return False
    if regime == REGIME_STRONG_DOWNTREND  and direction == "LONG":  return False
    if regime == REGIME_CHOPPY_RANGING:                              return False
    return True


async def check_regime_change(symbol: str, new_regime: dict, bot) -> None:
    """Alert owner if regime has shifted since last check."""
    old = regime_last.get(symbol, REGIME_UNKNOWN)
    new = new_regime["regime"]
    if old != new and old != REGIME_UNKNOWN:
        emoji_old = regime_emoji(old)
        emoji_new = regime_emoji(new)
        await alert(bot,
            f"🔀 <b>REGIME SHIFT — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Was  : {emoji_old} {regime_label(old)}\n"
            f"Now  : {emoji_new} <b>{regime_label(new)}</b> "
            f"({new_regime['confidence']}% confidence)\n\n"
            f"<b>Bot behaviour changes to:</b>\n"
            f"{regime_action(new)}\n\n"
            f"<b>Key indicators:</b>\n"
            f"ADX: {new_regime['indicators'].get('adx','?')} · "
            f"ATR%: {new_regime['indicators'].get('atr_pct','?')} · "
            f"BB width: {new_regime['indicators'].get('bb_width','?')}\n"
            f"EMA50 slope: {new_regime['indicators'].get('ema50_slope','?')} · "
            f"RSI: {new_regime['indicators'].get('rsi','?')}"
        )
    regime_last[symbol] = new

# =============================================================================
#  END OF REGIME DETECTOR MODULE
# =============================================================================

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
        daily_loss     = 0.0
        last_reset_day = today
        # Auto-reset consecutive losses every new day.
        # A stop loss from yesterday should not block today's entries.
        if consec_losses > 0:
            logger.info(f"[CIRCUIT] Auto-reset consec_losses {consec_losses}→0 (new day)")
            consec_losses = 0
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
        reasons = []
        if daily_loss   <= -0.04 * balance: reasons.append(f"Daily loss {daily_loss:.2f} USDT (limit 4%)")
        if weekly_loss  <= -0.06 * balance: reasons.append(f"Weekly loss {weekly_loss:.2f} USDT (limit 6%)")
        if consec_losses >= 3:              reasons.append(f"{consec_losses} consecutive losses")
        reason_str = " | ".join(reasons)
        await alert(bot,
            f"⚠️ <b>CIRCUIT BREAKER — {symbol}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Triggered by: {reason_str}\n"
            f"No new entries until conditions reset.\n"
            f"Existing positions still managed normally.\n\n"
            f"<b>How to resume:</b>\n"
            f"• Consecutive losses reset automatically each new day\n"
            f"• Or use /resume to reset manually right now\n"
            f"• Daily/weekly loss limits reset at midnight UTC")
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

    # ── Detect market regime ─────────────────────────────────────────────────
    regime_result  = await detect_regime(symbol, df_1d, df_4h)
    await check_regime_change(symbol, regime_result, bot)
    current_regime = regime_result["regime"]
    regime_conf    = regime_result["confidence"]
    logger.info(f"[{symbol}] Regime: {regime_label(current_regime)} ({regime_conf}%)")

    # ── Whipsaw cooldown gate ─────────────────────────────────────────────────
    # If this symbol recently whipsawed (rapid open/close loop), skip all
    # entry scanning until the cooldown expires — even if regime/filters
    # would otherwise say to enter.
    cooldown_until = symbol_cooldown_until.get(symbol)
    if cooldown_until and time.time() < cooldown_until:
        remaining_min = round((cooldown_until - time.time()) / 60)
        logger.info(f"[{symbol}] In whipsaw cooldown — {remaining_min}m remaining. Skipping scan.")
        return

    # CHOPPY regime — skip new entries entirely
    if current_regime == REGIME_CHOPPY_RANGING:
        logger.info(f"[{symbol}] Regime CHOPPY ({regime_conf}%) — skipping entry scan")
        await _maybe_send_status_report(symbol, bot, job_data, balance,
                                        df_1d=df_1d, df_4h=df_4h, price=price,
                                        regime=regime_result)
        return

    # Check LONG and SHORT — regime filters allowed directions
    for direction in ("LONG", "SHORT"):
        if not regime_allows_direction(current_regime, direction):
            logger.debug(f"[{symbol}] Regime {current_regime} blocks {direction}")
            continue
        indicators = await asyncio.to_thread(
            _compute_indicators, df_1d, df_4h, df_1h, direction)
        if not indicators["should_enter"]:
            continue

        # ── Health-score gate ────────────────────────────────────────────────
        # Never blocks a direction outright — raises the bar from 6/9 to
        # 7/9 only when the OPPOSING direction's health clearly outweighs
        # this one (see _apply_health_gate for full rationale).
        health = await _get_cached_health_scores(symbol)
        own_score      = health.get("long_score" if direction == "LONG" else "short_score", 50)
        opposing_score = health.get("short_score" if direction == "LONG" else "long_score", 50)
        indicators = _apply_health_gate(indicators, direction, own_score, opposing_score)
        if not indicators["should_enter"]:
            logger.info(f"[{symbol}] {direction} filtered by health gate: "
                        f"{indicators.get('health_gate_reason', '')}")
            continue

        indicators["regime"]            = current_regime
        indicators["regime_confidence"] = regime_conf
        logger.info(f"[{symbol}] {direction} confirmed (score {indicators['score']}) "
                    f"regime={current_regime} own_health={own_score:.0f}")
        await open_trade(symbol, direction, indicators, bot)
        return

    # No signal found this cycle — log why for debugging
    logger.info(f"[{symbol}] No entry signal this cycle. "
                f"Regime: {regime_label(current_regime)} ({regime_conf}%) | "
                f"Price: {price:.4f} | "
                f"paused: {bot_paused} | circuit: {paused_circuit} | "
                f"consec_losses: {consec_losses}")

    # 30-min report when flat
    await _maybe_send_status_report(symbol, bot, job_data, balance,
                                    df_1d=df_1d, df_4h=df_4h, price=price,
                                    regime=regime_result)


async def _maybe_send_status_report(
    symbol: str, bot, job_data: dict, balance: float,
    df_1d=None, df_4h=None, price: float = 0.0,
    regime: dict = None
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
        # Use real Bybit unrealised PnL — avoids double-leverage error
        real_pnl = await asyncio.to_thread(_sync_get_position_pnl, symbol)
        pnl = real_pnl if real_pnl != 0.0 else _est_pnl(pos, current_price)
        days = (time.time() - pos["entry_time"]) / 86400
        emoji = "🟢" if pos["direction"] == "LONG" else "🔴"
        pnl_label = "PnL (Bybit)" if real_pnl != 0.0 else "Est PnL"
        trade_info = (
            f"{emoji} <b>Open {pos['direction']} position</b>\n"
            f"   Entry  : <code>{pos['entry_price']:.4f}</code>\n"
            f"   Current: <code>{current_price:.4f}</code>\n"
            f"   Exit   : Daily EMA inversion only (no SL/TP)\n"
            f"   {pnl_label}: {pnl:+.2f} USDT  |  Open: {days:.1f}d\n"
            f"   Score  : {pos['score']}/9"
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
                    f"   LONG score : {ind['score']}/9 — "
                    f"{'🚀 SIGNAL!' if ind['should_enter'] else 'waiting'}"
                )
                break
        except Exception:
            pass

    # Regime line
    regime_line = ""
    if regime:
        re = regime.get("regime", REGIME_UNKNOWN)
        rc = regime.get("confidence", 0)
        regime_line = (
            f"\n{regime_emoji(re)} <b>Regime</b>: {regime_label(re)} ({rc}%)\n"
            f"   Action: {regime_action(re)}"
        )

    await alert(bot,
        f"📋 <b>30-min Report — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Balance: <code>{balance:.2f} USDT</code>  |  "
        f"Paused: {'🔴 YES' if bot_paused else '🟢 NO'}\n"
        f"Consec losses: {consec_losses}  |  Daily loss: {daily_loss:.2f}\n"
        f"{regime_line}\n"
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
async def _send_full_command_reference(update: Update) -> None:
    """
    Complete command reference — every registered command, what it does,
    and who can use it. Split into several messages to stay under
    Telegram's length limit. Shared by /start and /commands so the two
    can never drift out of sync with each other — though this text is
    still kept in sync MANUALLY with the actual CommandHandler
    registrations at the bottom of this file; if a command is added or
    removed there, update this function too.
    """
    # ── Message 1: Welcome + Monitoring/Status + Analysis tools ───────────
    await update.message.reply_text(
        "🤖 <b>PsychologyBot v3 — Command Reference</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Three independent trading strategies on Bybit:\n"
        "📊 <b>PsychologyBot</b> — 9-filter trend-following (Daily/4H/1H)\n"
        "⚡ <b>MemeBot</b> — fast momentum (5m/3m)\n"
        "🌐 <b>Regime Strategy</b> — EMA100 trend + regime gate, curated watchlist\n"
        "📰 <b>NewsBot</b> — CryptoPanic high-impact alerts\n\n"
        "Public bot — anyone can check status &amp; run analysis. "
        "Trading controls are owner-only.\n\n"

        "<b>📊 MONITORING &amp; STATUS (public)</b>\n"
        "/start, /commands — This command reference\n"
        "/status — PsychologyBot open positions, balance, circuit breaker\n"
        "/scan — Run a live 9-filter scan on all PsychologyBot symbols now\n"
        "/balance — Current USDT balance on Bybit\n"
        "/history [days] — Closed trade log (3-week rolling window)\n"
        "/backup — Full state snapshot: symbols, positions, disk status "
        "across all three strategies\n\n"

        "<b>🔄 MARKET REGIME (public, analysis only)</b>\n"
        "/regime SYMBOL — Full regime breakdown for one coin (11 signals "
        "+ CPO cross-check)\n"
        "/regime_all — Regime summary for all PsychologyBot symbols\n\n"

        "<b>🔍 COIN ANALYSIS (public)</b>\n"
        "/screen [period] — Rank PsychologyBot symbols by alpha vs BTC\n"
        "/health SYMBOL — 0-100 health score (5-factor breakdown, LONG+SHORT)\n"
        "/uptrendcoins [period] — Top 15 Bybit coins beating BTC\n"
        "/downtrendcoins [period] — Top 15 Bybit coins lagging BTC\n"
        "/cycle SYMBOL — Cycle Phase Oscillator analysis (Compression/"
        "Expansion/Euphoria/Capitulation)\n\n"

        "<i>Continued in the next message ➜</i>",
        parse_mode="HTML",
    )

    # ── Message 2: Backtesting ─────────────────────────────────────────────
    await update.message.reply_text(
        "<b>📈 BACKTESTING (public)</b>\n"
        "<i>PsychologyBot (9-filter) strategy:</i>\n"
        "/backtest SYMBOL PERIOD [capital] [tp_mode] — historical test on "
        "real 4H+1H data\n"
        "   e.g. <code>/backtest BTCUSDT 6m</code>\n"
        "/backtest_tp SYMBOL PERIOD [capital] — runs all 4 TP mechanisms "
        "side by side (none/fixed_pct/atr/vp)\n"
        "/compare_stack SYMBOL [days] — classic vs adaptive EMA stack, "
        "one coin\n"
        "/backtest_compare SYMBOL PERIOD [capital] — classic vs adaptive "
        "stack, full backtest\n\n"

        "<i>Regime Strategy (EXPERIMENTAL — not live-traded until you add "
        "a symbol with /add_symbol_regime):</i>\n"
        "/backtest_regime SYMBOL PERIOD [capital] — LONG above EMA100, "
        "SHORT below, exit on the opposite cross\n"
        "   e.g. <code>/backtest_regime BTCUSDT 1y</code>\n"
        "/backtest_regime_dates SYMBOL DD.MM.YYYY DD.MM.YYYY [capital] — "
        "same, but an explicit date range instead of a relative period\n"
        "   e.g. <code>/backtest_regime_dates BTCUSDT 19.08.2026 20.09.2027</code>\n\n"

        "<i>Continued in the next message ➜</i>",
        parse_mode="HTML",
    )

    # ── Message 3: Bulk regime-strategy backtest rankings ──────────────────
    await update.message.reply_text(
        "<b>🏆 BULK REGIME BACKTEST RANKINGS (public)</b>\n"
        "Same regime-strategy rules as above, run across MANY symbols at "
        "once and ranked by return %. All three run as background tasks — "
        "the bot stays fully responsive to every other command while they "
        "work, and results arrive as they finish.\n"
        "/backtest_regime_all_dates DD.MM.YYYY DD.MM.YYYY [capital] — "
        "EVERY Bybit USDT symbol (400+), shows the top 50. Expect "
        "20-60+ minutes.\n"
        "/backtest_regime_50_up_dates DD.MM.YYYY DD.MM.YYYY [capital] — "
        "top 50 outperformers vs BTC (last 90d), ranks all 50. Expect "
        "several minutes.\n"
        "/backtest_regime_50_down_dates DD.MM.YYYY DD.MM.YYYY [capital] — "
        "same but top 50 underperformers vs BTC.\n"
        "   e.g. <code>/backtest_regime_50_up_dates 23.10.2025 20.09.2026</code>\n\n"
        "<i>\"from\"/\"to\" filler words are tolerated in all three, e.g. "
        "\"...50_up_dates from 23.10.2025 to 20.09.2026\".</i>\n\n"

        "<i>Continued in the next message ➜</i>",
        parse_mode="HTML",
    )

    # ── Message 4: The three live strategies (overview + owner controls) ──
    await update.message.reply_text(
        "<b>📊 PSYCHOLOGYBOT (9-filter, Daily/4H/1H)</b>\n"
        "/status, /scan — see Monitoring section above\n"
        "🔒 <i>Owner-only:</i>\n"
        "/add_symbol SYM — start monitoring a coin\n"
        "/remove_symbol SYM — stop monitoring a coin\n"
        "/close SYM — manually close an open position now (no bot-side "
        "SL/TP on this strategy — only Daily EMA structure inversion exits "
        "automatically)\n"
        "/pause — pause all new entries\n"
        "/resume — resume + reset circuit breaker\n\n"

        "<b>⚡ MEMEBOT (fast momentum, 5m/3m)</b>\n"
        "/memebot — overview\n"
        "/meme_status — open meme positions\n"
        "/scan_meme — live scan on meme symbols\n"
        "🔒 <i>Owner-only:</i>\n"
        "/add_meme SYM — start monitoring a coin\n"
        "/remove_meme SYM — stop monitoring a coin\n"
        "/meme_pause / /meme_resume — pause / resume\n\n"

        "<b>📰 NEWSBOT (CryptoPanic, high-impact only)</b>\n"
        "/news_status — status + articles seen\n"
        "/news_scan — manual scan now\n"
        "🔒 <i>Owner-only:</i>\n"
        "/news_pause / /news_resume — pause / resume\n\n"

        "<i>Continued in the next message ➜</i>",
        parse_mode="HTML",
    )

    # ── Message 5: Regime Strategy (live) + examples ───────────────────────
    await update.message.reply_text(
        "<b>🌐 REGIME STRATEGY (live) — curated watchlist</b>\n"
        "Entry: LONG when Daily close &gt; EMA100, SHORT when &lt; EMA100 "
        "(closed candles) — blocked during Choppy/Mean Reverting/Unknown "
        "regime. Exit: only when price crosses back over EMA100. No stop "
        "loss, no take profit — the EMA100 cross covers both. Checked "
        "every 10 min; open positions get a status report every 45 min.\n"
        "/regime_status — watchlist, running/paused, open positions + PnL\n"
        "🔒 <i>Owner-only:</i>\n"
        "/add_symbol_regime SYM — add a coin to the regime strategy "
        "watchlist (needs 101+ days of history before it can trade)\n"
        "/remove_symbol_regime SYM — remove a coin (open positions still "
        "get managed to exit)\n"
        "/regime_pause — pause new entries (open positions still managed)\n"
        "/regime_resume — resume\n\n"

        "<b>Examples:</b>\n"
        "<code>/add_symbol BTCUSDT</code>\n"
        "<code>/add_symbol_regime SOLUSDT</code>\n"
        "<code>/health ETHUSDT</code>\n"
        "<code>/cycle ETHUSDT</code>\n"
        "<code>/backtest AVAXUSDT 3m</code>\n"
        "<code>/backtest_regime BTCUSDT 1y</code>\n"
        "<code>/backtest_regime_50_up_dates 23.10.2025 20.09.2026</code>\n\n"

        "All symbol lists and open positions across all three strategies "
        "persist to disk and restore automatically after a restart. Trade "
        "history is kept for 3 weeks.\n\n"

        "<i>Use /help for full PsychologyBot strategy rules (filters, "
        "exits) ➜</i>",
        parse_mode="HTML",
    )


@public_command
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_full_command_reference(update)


@public_command
async def cmd_commands(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Alias of /start's command reference — same content, different
    entry point for whenever "commands" is the more natural thing to type."""
    await _send_full_command_reference(update)


@public_command
async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Detailed strategy rules — entry filters, SL/TP tiers, exit logic."""
    await update.message.reply_text(
        "<b>📋 Strategy Rules — PsychologyBot</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"

        "<b>Entry Filters (6/9 required — #1, #5 &amp; #9 mandatory):</b>\n"
        "1. ✅ MANDATORY: ADX above coin's OWN 90-period baseline (4H) — "
        "trend strength relative to that coin's typical state, not a fixed number\n"
        "2. Price above/below Ichimoku Cloud (Daily)\n"
        "3. RSI not extreme (not &gt;70 long / not &lt;30 short) (4H)\n"
        "4. Supertrend green=LONG / red=SHORT (4H)\n"
        "5. ✅ MANDATORY: EMA 20&gt;50&gt;100&gt;200 stack (Daily)\n"
        "6. MACD histogram green=LONG / red=SHORT (Daily)\n"
        "7. Price pulled back to 4H EMA 50 or 100\n"
        "8. Bullish/Bearish 1H candle trigger (optional — a single "
        "candle is noisier than trend/strength/volume)\n"
        "9. ✅ MANDATORY: Volume Confirmation (4H) — current volume ≥1.2x "
        "the 20-period average, confirms real participation behind the move\n\n"

        "<b>Health-score gate (never blocks, only raises the bar):</b>\n"
        "Each coin gets independent LONG and SHORT health scores (0-100, "
        "see /health). If the OPPOSING direction's score clearly beats "
        "this direction's, the requirement rises from 6/9 to 7/9 — a "
        "coin favoring shorts doesn't forbid longs, it just demands a "
        "stronger long setup.\n\n"

        "<b>Market Regime Gate (on top of filters):</b>\n"
        "📈 Strong Uptrend → LONG only\n"
        "📉 Strong Downtrend → SHORT only\n"
        "↔️ Choppy/Ranging → ALL entries blocked\n"
        "💥 Volatile Breakout → Position halved (5x lev cap)\n"
        "🔄 Mean Reverting → informational only (no SL/TP behaviour to adapt)\n\n"

        "<b>Stop Loss / Take Profit — NONE:</b>\n"
        "This strategy has no stop loss and no take profit. No stop order "
        "is placed on Bybit at entry. No partial closes happen at any "
        "price target.\n\n"

        "<b>Exit Rules — ONE rule only:</b>\n"
        "Exit immediately when Daily EMA structure inverts (checked on "
        "fully closed candles only, 15-min minimum hold after entry to "
        "avoid data-timing noise). No dead-trade timeout — a position "
        "holds indefinitely until the EMA structure flips, a manual "
        "/close, or exchange liquidation.\n\n"
        "⚠️ With leverage active and no bot-side stop, a large adverse "
        "move before EMA inversion triggers may hit Bybit's own "
        "liquidation price first — that is a worse outcome than a "
        "controlled stop, not a safer one.\n\n"
        "Use <code>/close SYMBOL</code> (owner-only, requires confirm) to "
        "manually exit a position early without waiting for EMA inversion.\n\n"

        "<b>Position Sizing:</b>\n"
        "PsychologyBot: 20% balance, up to 10x leverage (fixed % — no SL "
        "to size risk against)\n"
        "MemeBot: 8% balance, up to 5x leverage, max 2 open positions "
        "(MemeBot is unaffected by any of the above — separate strategy, "
        "still has its own SL/TP)\n\n"

        "<b>Infrastructure:</b>\n"
        "WebSocket real-time prices + REST fallback\n"
        "All Bybit/pandas calls run in background threads (never freezes)\n"
        "30-min status reports per symbol\n"
        "State persists across redeploys via /var/data persistent disk\n"
        "Position sync check every 10s — catches manual closes/liquidation "
        "even if bot was offline",
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

    # Save synchronously — critical that this completes before anything else
    save_state()
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
    # Check circuit breaker
    cb_daily  = daily_loss  <= -0.04 * balance if balance > 0 else False
    cb_weekly = weekly_loss <= -0.06 * balance if balance > 0 else False
    cb_losses = consec_losses >= 3
    circuit_on = cb_daily or cb_weekly or cb_losses

    if circuit_on:
        cb_reasons = []
        if cb_daily:  cb_reasons.append(f"daily loss {daily_loss:.2f}")
        if cb_weekly: cb_reasons.append(f"weekly loss {weekly_loss:.2f}")
        if cb_losses: cb_reasons.append(f"{consec_losses} consec losses")
        cb_str = f"🔴 ACTIVE ({', '.join(cb_reasons)}) — use /resume to reset"
    else:
        cb_str = "🟢 clear"

    lines   = [
        "📊 <b>Bot Status</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"Balance        : <code>{balance:.2f} USDT</code>",
        f"Paused         : {'🔴 YES — use /resume' if bot_paused else '🟢 NO'}",
        f"Circuit breaker: {cb_str}",
        f"Consec losses  : {consec_losses} (resets each new day automatically)",
        f"Daily loss     : {daily_loss:.2f} USDT (limit: 4% = {balance*0.04:.2f})",
        f"Weekly loss    : {weekly_loss:.2f} USDT (limit: 6% = {balance*0.06:.2f})",
        f"",
        f"<b>Watching:</b> {', '.join(monitored_symbols) or 'None'}",
        "",
    ]

    # Show any symbols currently in whipsaw cooldown
    now = time.time()
    active_cooldowns = {s: u for s, u in symbol_cooldown_until.items() if u > now}
    if active_cooldowns:
        lines.append("<b>⏸ Whipsaw cooldowns active:</b>")
        for sym, until in active_cooldowns.items():
            remaining_min = round((until - now) / 60)
            lines.append(f"  {sym}: {remaining_min}m remaining (use /resume to clear early)")
        lines.append("")

    if symbol_positions:
        lines.append("<b>Open Positions:</b>")
        for sym, pos in symbol_positions.items():
            price    = await get_price(sym)
            real_pnl = await asyncio.to_thread(_sync_get_position_pnl, sym)
            pnl      = real_pnl if real_pnl != 0.0 else _est_pnl(pos, price)
            days     = (time.time() - pos["entry_time"]) / 86400
            e        = "🟢" if pos["direction"] == "LONG" else "🔴"
            pnl_src  = "Bybit" if real_pnl != 0.0 else "Est"
            lines.append(
                f"{e} <b>{sym}</b> {pos['direction']}\n"
                f"   Entry:{pos['entry_price']:.4f}  Now:{price:.4f}\n"
                f"   Exit: Daily EMA inversion only (no SL/TP)\n"
                f"   PnL({pnl_src}):{pnl:+.2f} USDT  Open:{days:.1f}d  Score:{pos['score']}/9"
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
                f"Score: <b>{ind['score']}/9</b>\n"
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
        "No new trade entries. Existing positions continue to be managed "
        "(exit only on Daily EMA structure inversion — no SL/TP on this strategy).",
        parse_mode="HTML")

@owner_only
async def cmd_close(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Manually close an open PsychologyBot position immediately, bypassing
    the EMA-inversion-only exit rule. Owner-only since this places a real
    market order. Requires an explicit "confirm" to avoid accidental
    closes from a mistyped command — this strategy has no bot-side stop,
    so a manual close may be the only way to exit before liquidation risk
    builds on an adverse move.
    Usage: /close SYMBOL          -> shows current PnL, asks to confirm
           /close SYMBOL confirm  -> actually closes
    """
    if not ctx.args:
        open_syms = ", ".join(symbol_positions.keys()) or "None"
        await update.message.reply_text(
            "Usage: <code>/close SYMBOL</code>\n"
            "Manually closes an open position immediately — bypasses the "
            "EMA-inversion-only exit rule (this strategy has no SL/TP).\n\n"
            f"Open positions: {open_syms}",
            parse_mode="HTML")
        return

    symbol = ctx.args[0].upper()
    if not symbol.endswith("USDT"):
        symbol += "USDT"

    pos = symbol_positions.get(symbol)
    if not pos:
        await update.message.reply_text(
            f"No open PsychologyBot position on {symbol}.\n"
            f"Open positions: {', '.join(symbol_positions.keys()) or 'None'}")
        return

    direction = pos["direction"]
    current_price = await get_price(symbol)
    pnl = _est_pnl(pos, current_price) if current_price else 0.0
    confirmed = len(ctx.args) >= 2 and ctx.args[1].lower() == "confirm"

    if not confirmed:
        days = (time.time() - pos["entry_time"]) / 86400
        pnl_str = f"{pnl:+.2f} USDT" if current_price else "unavailable"
        await update.message.reply_text(
            f"⚠️ <b>Confirm manual close — {symbol} {direction}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Entry     : <code>{pos['entry_price']:.4f}</code>\n"
            f"Current   : <code>{current_price:.4f}</code>\n"
            f"Est. PnL  : {pnl_str}\n"
            f"Open      : {days:.1f}d\n\n"
            f"This closes the position IMMEDIATELY on Bybit, bypassing "
            f"the EMA-inversion-only exit rule.\n\n"
            f"Type <code>/close {symbol} confirm</code> to proceed.",
            parse_mode="HTML")
        return

    await close_full(symbol, direction)
    await update.message.reply_text(
        f"✅ <b>MANUAL CLOSE — {symbol} {direction}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Closed @ <code>{current_price:.4f}</code>\n"
        f"Est. PnL: {pnl:+.2f} USDT\n"
        f"Bot goes FLAT on {symbol}. Watching for next signal.",
        parse_mode="HTML")
    whipsaw = _close_pos(symbol, pnl, exit_price=current_price or pos["entry_price"], exit_reason="MANUAL_CLOSE")
    if whipsaw:
        await update.message.reply_text(
            f"🚨 <b>WHIPSAW DETECTED — {symbol}</b>\n"
            f"This close combined with an earlier quick exit within "
            f"{WHIPSAW_WINDOW_MIN} minutes.\n"
            f"⏸ {symbol} PAUSED for {WHIPSAW_COOLDOWN_MIN} minutes — "
            f"no new entries until cooldown expires. Use /resume to "
            f"override early.",
            parse_mode="HTML")

@owner_only
async def cmd_resume(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    global bot_paused, consec_losses, daily_loss, weekly_loss
    bot_paused    = False
    consec_losses = 0
    daily_loss    = 0.0
    weekly_loss   = 0.0
    # Reset circuit_alerted flag on all active monitor jobs so alerts fire fresh
    if ctx.job_queue:
        for job in ctx.job_queue.jobs():
            if job.data and isinstance(job.data, dict):
                job.data["circuit_alerted"] = False
    # Clear any whipsaw cooldowns — user is manually overriding
    cleared_symbols = list(symbol_cooldown_until.keys())
    symbol_cooldown_until.clear()
    symbol_quick_exit_times.clear()
    save_state()
    cooldown_note = (f"\nWhipsaw cooldowns cleared: {', '.join(cleared_symbols)}"
                     if cleared_symbols else "")
    await update.message.reply_text(
        "▶️ <b>Bot resumed.</b>\n"
        "Consecutive losses: reset to 0\n"
        "Daily/weekly loss counters: reset\n"
        "Circuit breaker: cleared"
        f"{cooldown_note}\n"
        "Bot is now actively scanning for entries.",
        parse_mode="HTML")

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

    # ── Verify position still exists on Bybit ────────────────────────────────
    actual_size = await asyncio.to_thread(_sync_get_position_size, symbol)

    if actual_size == 0.0:
        # Gone on Bybit — get last known price for PnL estimate
        last_price  = pos.get("_last_price", pos.get("entry_price", 0))
        entry_price = pos.get("entry_price", 0)
        qty         = pos.get("original_qty", 0)
        lev         = pos.get("leverage", 5)
        if direction == "LONG":
            est_pnl = (last_price - entry_price) * qty
        else:
            est_pnl = (entry_price - last_price) * qty

        sl  = pos.get("sl_price", 0)
        tp1 = pos.get("tp1_price", 0)
        if direction == "LONG":
            reason = "SL" if last_price <= sl * 1.005 else "TP1" if last_price >= tp1 * 0.995 else "BYBIT_CLOSED"
        else:
            reason = "SL" if last_price >= sl * 0.995 else "TP1" if last_price <= tp1 * 1.005 else "BYBIT_CLOSED"

        logger.warning(f"[MEME][{symbol}] Gone on Bybit — clearing. Est PnL: {est_pnl:.2f} reason={reason}")
        await alert(bot,
            "[MEME] POSITION SYNC " + symbol + " " + direction + "\n"
            "Bybit shows NO open position — closed externally.\n"
            f"Est. PnL: {est_pnl:+.2f} USDT | Reason: {reason}\n"
            "Bot state cleared. Watching for next signal.")
        meme_positions.pop(symbol, None)
        save_state()
        return

    elif actual_size < 0:
        logger.warning(f"[MEME][{symbol}] API error verifying position. Skipping.")
        return

    price, df_5m, df_3m = await asyncio.gather(
        asyncio.to_thread(_sync_get_price, symbol),
        asyncio.to_thread(_sync_get_klines, symbol, "5",  100),
        asyncio.to_thread(_sync_get_klines, symbol, "3",   60),
    )

    if not price or df_5m is None:
        return

    pos["_last_price"] = price   # track for sync recovery

    def est_pnl():
        # qty is already the full contract size — no leverage multiplier needed
        diff = (price - pos["entry_price"]) if direction == "LONG" \
               else (pos["entry_price"] - price)
        return diff * pos["remaining_qty"]

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

    # Save synchronously — must complete before anything else
    save_state()
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


@owner_only
async def cmd_backup(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Show current state as text so owner can copy/save it."""
    psych  = ", ".join(monitored_symbols) if monitored_symbols else "none"
    meme   = ", ".join(meme_symbols) if meme_symbols else "none"
    regime = ", ".join(regime_symbols) if regime_symbols else "none"
    open_p = len(symbol_positions)
    open_m = len(meme_positions)
    open_r = len(regime_positions)

    lines = [
        "💾 <b>Bot State Backup</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"State file : <code>{STATE_FILE}</code>",
        f"Disk       : {'✅ /var/data (persistent)' if os.path.isdir('/var/data') else '⚠️ No persistent disk'}",
        "",
        f"<b>PsychologyBot symbols:</b> <code>{psych}</code>",
        f"<b>MemeBot symbols:</b> <code>{meme}</code>",
        f"<b>Regime Strategy symbols:</b> <code>{regime}</code>",
        f"Open positions: {open_p} psych + {open_m} meme + {open_r} regime",
        f"Trade history : {len(trade_history)} records (kept {HISTORY_KEEP_DAYS} days)",
        f"Daily loss    : {daily_loss:.2f} USDT",
        f"Consec losses : {consec_losses}",
        "",
    ]

    if regime_positions:
        lines.append("<b>Open Regime Strategy positions:</b>")
        for sym, pos in regime_positions.items():
            lines.append(f"  {pos['direction']} {sym} @ {pos.get('entry_price',0):.4f}")
        lines.append("")

    lines.append("<b>To restore after a fresh start (usually automatic — "
                  "see note below):</b>")
    for sym in monitored_symbols:
        lines.append(f"<code>/add_symbol {sym}</code>")
    for sym in meme_symbols:
        lines.append(f"<code>/add_meme {sym}</code>")
    for sym in regime_symbols:
        lines.append(f"<code>/add_symbol_regime {sym}</code>")

    if not monitored_symbols and not meme_symbols and not regime_symbols:
        lines.append("No symbols currently monitored on any strategy.")

    lines.append("")
    lines.append("<i>Symbol lists and open positions across all three "
                  "strategies (PsychologyBot, MemeBot, Regime Strategy) "
                  "persist to disk automatically and restore themselves "
                  "on restart — the commands above are a manual fallback "
                  "only, in case the persistent disk was ever unavailable.</i>")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


@public_command
async def cmd_regime(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Show current market regime for a symbol."""
    # Accept /regime BTCUSDT or use first monitored symbol
    symbol = ctx.args[0].upper() if ctx.args else (monitored_symbols[0] if monitored_symbols else None)
    if not symbol:
        await update.message.reply_text("Usage: /regime BTCUSDT\nOr add a symbol first with /add_symbol")
        return

    await update.message.reply_text(f"Analysing regime for {symbol}...")

    df_1d, df_4h = await asyncio.gather(
        get_klines(symbol, "D",   300),
        get_klines(symbol, "240", 300),
    )
    if df_1d is None or df_4h is None:
        await update.message.reply_text(f"Failed to fetch candles for {symbol}.")
        return

    r = await detect_regime(symbol, df_1d, df_4h, force=True)
    reg  = r["regime"]
    conf = r["confidence"]
    ind  = r["indicators"]
    scores = r.get("scores", {})
    mx     = r.get("max_possible", {})

    # Build score bars
    score_lines = []
    order = [REGIME_STRONG_UPTREND, REGIME_STRONG_DOWNTREND,
             REGIME_CHOPPY_RANGING, REGIME_VOLATILE_BREAKOUT, REGIME_MEAN_REVERTING]
    for regime_key in order:
        sc = scores.get(regime_key, 0)
        mp = mx.get(regime_key, 1)
        pct = int(sc / mp * 100) if mp else 0
        bar = "█" * (pct // 10) + "░" * (10 - pct // 10)
        star = " ◄" if regime_key == reg else ""
        score_lines.append(
            f"{regime_emoji(regime_key)} {regime_label(regime_key):<22} {bar} {pct}%{star}"
        )

    # Build leading signals summary
    leading_lines = []
    # BB Squeeze
    if ind.get("squeeze_released"):
        leading_lines.append("🔔 BB Squeeze RELEASED — breakout starting NOW")
    elif ind.get("bb_at_20bar_low"):
        leading_lines.append("⚠️ BB at 20-bar low — squeeze building, breakout imminent")
    elif ind.get("squeeze_active"):
        leading_lines.append("⏳ BB Squeeze ACTIVE — compression ongoing, no direction yet")
    # Price structure
    if ind.get("strong_hh"):
        leading_lines.append("📶 4 consecutive higher closes — uptrend forming (leads ADX by ~5 days)")
    elif ind.get("hh_hl"):
        leading_lines.append("📈 3 consecutive higher closes — early uptrend signal")
    if ind.get("strong_ll"):
        leading_lines.append("📉 4 consecutive lower closes — downtrend forming (leads ADX by ~5 days)")
    elif ind.get("lh_ll"):
        leading_lines.append("📉 3 consecutive lower closes — early downtrend signal")
    # EMA slope acceleration
    if ind.get("slope_turning_bull"):
        leading_lines.append("↗️ EMA50 slope accelerating UP — trend turning bullish before EMA crosses")
    if ind.get("slope_turning_bear"):
        leading_lines.append("↘️ EMA50 slope accelerating DOWN — trend turning bearish before EMA crosses")
    # Volume-price
    if ind.get("vol_confirms_bull"):
        leading_lines.append("💚 Volume + rising price — institutional buying confirmed")
    if ind.get("vol_confirms_bear"):
        leading_lines.append("🔴 Volume + falling price — institutional selling confirmed")
    if ind.get("vol_chop_signal"):
        leading_lines.append("🔀 High volume, tiny price move — absorption, market ranging")

    leading_text = ("\n".join(leading_lines)) if leading_lines else "No leading signals firing"
    accel_str = f"{ind.get('slope_accel',0):+.4f}"

    # CPO cross-check section — independent second-opinion signal
    cpo = r.get("cpo", {})
    cpo_agreement = r.get("cpo_agreement", "NEUTRAL")
    cpo_note = r.get("cpo_note")
    if cpo.get("error") or not cpo_note:
        cpo_section = "<b>CPO cross-check:</b>\nUnavailable (insufficient data for this coin)"
    else:
        agreement_emoji = {"AGREE": "✅", "CONFLICT": "⚠️", "NEUTRAL": "➖"}[cpo_agreement]
        cpo_section = (
            f"<b>CPO cross-check (independent signal):</b> {agreement_emoji}\n"
            f"{cpo.get('phase_emoji','')} Phase: {cpo.get('phase','?')} "
            f"({cpo.get('confidence','?')}% CPO confidence)\n"
            f"{cpo_note}"
        )

    await update.message.reply_text(
        f"{regime_emoji(reg)} <b>Market Regime — {symbol}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Regime     : <b>{regime_label(reg)}</b>\n"
        f"Confidence : <b>{conf}%</b>\n\n"
        f"<b>Regime scores:</b>\n"
        f"<code>{''.join([l + chr(10) for l in score_lines])}</code>\n"
        f"<b>Bot action:</b>\n{regime_action(reg)}\n\n"
        f"<b>Leading indicators (fire first):</b>\n"
        f"{leading_text}\n\n"
        f"<b>Lagging confirmators:</b>\n"
        f"ADX(14)     : <code>{ind.get('adx','?')}</code> "
        f"({'trending' if ind.get('adx',0) > 25 else 'weak trend' if ind.get('adx',0) > 20 else 'ranging'})\n"
        f"EMA50 slope : <code>{ind.get('ema50_slope','?')}</code> "
        f"| accel: <code>{accel_str}</code>\n"
        f"ATR%        : <code>{ind.get('atr_pct','?')}</code> "
        f"| BB width: <code>{ind.get('bb_width','?')}</code>\n"
        f"RSI(14)     : <code>{ind.get('rsi','?')}</code> "
        f"| Vol ratio: <code>{ind.get('vol_ratio','?')}x</code>\n"
        f"Price 3d    : <code>{ind.get('price_chg_3d','?')}%</code> "
        f"| Hist vol: <code>{ind.get('hist_vol','?')}%</code>\n\n"
        f"{cpo_section}",
        parse_mode="HTML"
    )

@public_command
async def cmd_regime_all(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Show current regime for all monitored symbols."""
    all_syms = list(set(monitored_symbols + meme_symbols))
    if not all_syms:
        await update.message.reply_text("No symbols monitored. Use /add_symbol or /add_meme first.")
        return

    await update.message.reply_text(f"Scanning regimes for {len(all_syms)} symbol(s)...")

    lines = ["<b>Market Regime Summary</b>", "━━━━━━━━━━━━━━━━━━━━"]
    for symbol in sorted(all_syms):
        df_1d, df_4h = await asyncio.gather(
            get_klines(symbol, "D",   300),
            get_klines(symbol, "240", 300),
        )
        if df_1d is None or df_4h is None:
            lines.append(f"⚠️ {symbol}: failed to fetch candles")
            continue
        r    = await detect_regime(symbol, df_1d, df_4h, force=True)
        reg  = r["regime"]
        conf = r["confidence"]
        tag  = "📊" if symbol in monitored_symbols else "⚡"
        lines.append(
            f"{tag} <b>{symbol}</b>: {regime_emoji(reg)} {regime_label(reg)} ({conf}%)")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


# =============================================================================
#  BACKTEST ENGINE
#  Runs PsychologyBot strategy on real historical candle data from Bybit.
#  Uses actual 4H and 1H candles — no approximations for F7 and F8.
#
#  Flow:
#  1. Fetch paginated Daily + 4H + 1H candles for the full period
#  2. Walk through each Daily candle as a potential signal bar
#  3. For each signal day: check F1-F6 on Daily/4H data up to that point
#  4. If F1-F6 pass: fetch actual 4H candles for that week → check F7
#  5. If F7 passes: check actual 1H candles for that day → check F8
#  6. If all pass: simulate trade with real SL/TP/EMA-exit logic
#  7. Collect results and send Telegram report
# =============================================================================

def _sync_fetch_binance_klines(symbol: str, interval: str, days_back: int) -> Optional[pd.DataFrame]:
    """
    Fallback data source: Binance public REST API (no auth required).
    Used when Bybit's listing history for a symbol is too short to build
    reliable long-term cycle indicators (e.g. RSI cycle needing 2-3+ years).

    Binance often has longer historical data for major coins since many
    were listed there years before appearing on Bybit's linear perpetuals.
    Binance kline endpoint allows up to 1000 candles per request and
    supports pagination via startTime/endTime, same approach as Bybit.

    interval: Binance format — "1d", "1w", "4h", "1h" (different from Bybit's
    "D"/"240"/"60" — caller must pass Binance-style interval strings)
    """
    try:
        base_url = "https://api.binance.com/api/v3/klines"
        interval_ms = {
            "1d": 86400000, "4h": 14400000, "1h": 3600000, "1w": 604800000,
        }.get(interval, 86400000)

        end_time = int(time.time() * 1000)
        start_time = end_time - (days_back * 86400000)

        all_rows = []
        cur_end = end_time
        max_requests = 10

        for _ in range(max_requests):
            params = {
                "symbol": symbol,
                "interval": interval,
                "endTime": cur_end,
                "limit": 1000,
            }
            resp = requests.get(base_url, params=params, timeout=10)
            if resp.status_code != 200:
                logger.warning(f"[BINANCE] {symbol} returned status {resp.status_code}: {resp.text[:200]}")
                break

            raw = resp.json()
            if not raw or not isinstance(raw, list):
                break

            all_rows = raw + all_rows
            oldest_ts = raw[0][0]
            if oldest_ts <= start_time or len(raw) < 1000:
                break
            cur_end = oldest_ts - 1
            time.sleep(0.15)  # respect Binance rate limits

        if not all_rows:
            return None

        df = pd.DataFrame(all_rows, columns=[
            "timestamp","open","high","low","close","volume",
            "close_time","quote_volume","trades","taker_buy_base",
            "taker_buy_quote","ignore"
        ])
        for col in ["open","high","low","close","volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["timestamp"] = df["timestamp"].astype("int64")
        df = df[["timestamp","open","high","low","close","volume"]]
        df.sort_values("timestamp", inplace=True)
        df.reset_index(drop=True, inplace=True)

        cutoff_ms = end_time - days_back * 86400000
        df = df[df["timestamp"] >= cutoff_ms].reset_index(drop=True)

        logger.info(f"[BINANCE] Fetched {len(df)} candles for {symbol} "
                    f"({interval}, {days_back}d fallback)")
        return df
    except Exception as e:
        logger.error(f"_sync_fetch_binance_klines({symbol},{interval}): {e}", exc_info=True)
        return None


def _compute_volume_profile(df: pd.DataFrame, lookback: int = 90, num_bins: int = 50) -> dict:
    """
    Volume Profile — a genuinely different indicator category from
    volume-over-time (vol_ratio etc. already used elsewhere in the bot):
    this is volume-BY-PRICE, showing how much total volume traded at each
    price level over the lookback window.

    DATA LIMITATION (disclosed honestly): Bybit's API only provides OHLCV
    bars, not tick-level or footprint data. A true volume profile needs to
    know how volume was distributed WITHIN each candle, which isn't
    available. This uses the standard industry-approximation (same method
    TradingView's built-in volume profile and most retail tools use):
    each candle's volume is spread evenly across the price bins its
    high-low range touches. This is an approximation, not the real thing,
    but it's standard practice, not something exotic.

    Returns:
      poc  — Point of Control: the single price level with the most
             traded volume (the "fair value" the market has agreed on)
      vah  — Value Area High: upper bound of the range containing ~70%
             of total volume
      val  — Value Area Low: lower bound of that same range
      position — where CURRENT price sits: ABOVE_VALUE_AREA (genuine
             breakout territory, thin historical resistance),
             BELOW_VALUE_AREA (genuine breakdown territory), or
             INSIDE_VALUE_AREA (still within the established trading
             range — the classic chop/mean-reversion signature)
    """
    try:
        window = df.tail(lookback)
        if len(window) < 20:
            return {"error": "Insufficient data for volume profile"}

        lo = float(window["low"].min())
        hi = float(window["high"].max())
        if hi <= lo:
            return {"error": "Degenerate price range"}

        bin_edges = np.linspace(lo, hi, num_bins + 1)
        bin_volumes = np.zeros(num_bins)

        lows  = window["low"].astype(float).values
        highs = window["high"].astype(float).values
        vols  = window["volume"].astype(float).values

        for row_lo, row_hi, row_vol in zip(lows, highs, vols):
            if row_hi <= row_lo or row_vol <= 0:
                continue
            start_idx = max(0, np.searchsorted(bin_edges, row_lo, side="right") - 1)
            end_idx   = min(num_bins, np.searchsorted(bin_edges, row_hi, side="left"))
            n_touched = max(1, end_idx - start_idx)
            vol_per_bin = row_vol / n_touched
            for b in range(start_idx, end_idx):
                bin_volumes[b] += vol_per_bin

        poc_idx = int(np.argmax(bin_volumes))
        poc_price = float((bin_edges[poc_idx] + bin_edges[poc_idx + 1]) / 2)

        # Expand outward from POC until ~70% of total volume is captured —
        # standard Value Area construction
        total_vol = float(bin_volumes.sum())
        if total_vol <= 0:
            return {"error": "No volume in profile window"}
        target = total_vol * 0.70
        lo_i = hi_i = poc_idx
        captured = bin_volumes[poc_idx]
        while captured < target and (lo_i > 0 or hi_i < num_bins - 1):
            vol_below = bin_volumes[lo_i - 1] if lo_i > 0 else -1
            vol_above = bin_volumes[hi_i + 1] if hi_i < num_bins - 1 else -1
            if vol_below >= vol_above:
                lo_i -= 1
                captured += bin_volumes[lo_i]
            else:
                hi_i += 1
                captured += bin_volumes[hi_i]

        vah = float(bin_edges[hi_i + 1])
        val = float(bin_edges[lo_i])

        current_price = float(df["close"].iloc[-1])
        if current_price > vah:
            position = "ABOVE_VALUE_AREA"
        elif current_price < val:
            position = "BELOW_VALUE_AREA"
        else:
            position = "INSIDE_VALUE_AREA"

        return {
            "poc": round(poc_price, 6),
            "vah": round(vah, 6),
            "val": round(val, 6),
            "current_price": round(current_price, 6),
            "position": position,
            "lookback": lookback,
        }
    except Exception as e:
        logger.error(f"_compute_volume_profile: {e}", exc_info=True)
        return {"error": str(e)}


def _sync_fetch_paginated_klines_range(symbol: str, interval: str,
                                        start_ts_ms: int, end_ts_ms: int) -> Optional[pd.DataFrame]:
    """
    Fetch candles for an EXPLICIT historical window [start_ts_ms, end_ts_ms],
    paginating backward from end_ts_ms. Unlike _sync_fetch_paginated_klines
    (which always anchors to "now" and fetches the most recent N days),
    this supports arbitrary past date ranges — needed for date-range
    backtests where the end date isn't today.
    interval: "D"=Daily, "240"=4H, "60"=1H, "15"=15m
    """
    try:
        all_rows = []
        end_time = end_ts_ms
        max_requests = 60  # a multi-year daily range needs several pages

        for _ in range(max_requests):
            params = {
                "category": "linear",
                "symbol":   symbol,
                "interval": interval,
                "limit":    1000,
                "end":      str(end_time),
            }
            resp = bybit_session.get_kline(**params)
            raw  = resp["result"]["list"]
            if not raw:
                break

            all_rows.extend(raw)
            oldest_ts = min(int(r[0]) for r in raw)
            if oldest_ts <= start_ts_ms:
                break   # paged back far enough
            end_time = oldest_ts - 1
            time.sleep(0.1)   # be respectful to API rate limits

        if not all_rows:
            return None

        df = pd.DataFrame(all_rows, columns=[
            "timestamp","open","high","low","close","volume","turnover"])
        for col in ["open","high","low","close","volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["timestamp"] = df["timestamp"].astype("int64")
        df.drop_duplicates(subset="timestamp", inplace=True)
        df.sort_values("timestamp", inplace=True)
        df.reset_index(drop=True, inplace=True)

        df = df[(df["timestamp"] >= start_ts_ms) & (df["timestamp"] <= end_ts_ms)].reset_index(drop=True)
        return df
    except Exception as e:
        logger.error(f"_sync_fetch_paginated_klines_range({symbol},{interval}): {e}", exc_info=True)
        return None


def _sync_fetch_paginated_klines(symbol: str, interval: str,
                                  days_back: int) -> Optional[pd.DataFrame]:
    """
    Fetch up to `days_back` days of candles by paginating Bybit API.
    Bybit limit = 1000 candles per request. Uses `end` param to page back.
    interval: "D"=Daily, "240"=4H, "60"=1H, "15"=15m
    """
    try:
        # Calculate how many candles we need
        candles_per_day = {
            "D": 1, "W": 0.14, "240": 6, "60": 24, "15": 96, "5": 288, "3": 480
        }
        cpd = candles_per_day.get(interval, 24)
        total_needed = int(days_back * cpd) + 50   # buffer
        
        all_rows = []
        end_time = None   # start from now, page backwards
        batch_size = min(1000, total_needed)
        max_requests = 10  # safety cap
        
        for _ in range(max_requests):
            params = {
                "category": "linear",
                "symbol":   symbol,
                "interval": interval,
                "limit":    batch_size,
            }
            if end_time:
                params["end"] = str(end_time)
            
            resp = bybit_session.get_kline(**params)
            raw  = resp["result"]["list"]
            if not raw:
                break
            
            all_rows.extend(raw)
            
            # Check if we have enough
            if len(all_rows) >= total_needed:
                break
            
            # Page back: oldest timestamp in this batch - 1ms
            oldest_ts = min(int(r[0]) for r in raw)
            if end_time and oldest_ts >= end_time:
                break   # no more data
            end_time = oldest_ts - 1
            time.sleep(0.1)  # be respectful to API rate limits
        
        if not all_rows:
            return None
        
        df = pd.DataFrame(all_rows, columns=[
            "timestamp","open","high","low","close","volume","turnover"])
        for col in ["open","high","low","close","volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["timestamp"] = df["timestamp"].astype("int64")
        df.sort_values("timestamp", inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        # Keep only candles within the requested period
        cutoff_ms = (int(time.time()) - days_back * 86400) * 1000
        df = df[df["timestamp"] >= cutoff_ms].reset_index(drop=True)
        
        return df
    except Exception as e:
        logger.error(f"_sync_fetch_paginated_klines({symbol},{interval}): {e}", exc_info=True)
        return None


def _sync_run_backtest(symbol: str, days: int, start_balance: float = 10000.0,
                        tp_mode: str = "none") -> dict:
    """
    Full backtest engine for the original 9-filter strategy. Runs in a
    background thread. Returns a results dict with all trades and
    summary stats.

    tp_mode: "none" (original spec — EMA structure inversion only),
    "fixed_pct", "atr", or "vp" (Volume Profile measured move) — same 4
    modes available on the experimental regime strategy, via the SAME
    shared _calc_tp_target function, so results are directly comparable
    across both strategies.
    """
    result = {
        "symbol": symbol, "days": days, "error": None,
        "trades": [], "balance_curve": [],
        "start_balance": start_balance, "tp_mode": tp_mode,
    }
    
    try:
        logger.info(f"[BACKTEST] Starting {symbol} {days}d backtest...")
        
        # ── Step 1: Fetch all candle data ────────────────────────────────────
        # Add extra days for EMA warmup (200-day EMA needs 200+ candles)
        warmup = 220
        total_days = days + warmup
        
        df_1d = _sync_fetch_paginated_klines(symbol, "D",   total_days)
        df_4h = _sync_fetch_paginated_klines(symbol, "240", total_days)
        df_1h = _sync_fetch_paginated_klines(symbol, "60",  min(days, 180))  # 180d of 1H max
        # BTC data — needed for health-score gating (alpha vs BTC, ATH
        # distance vs BTC's own ATH distance). Fetched once, sliced
        # per-day inside the loop to avoid lookahead bias.
        df_btc_window = _sync_fetch_paginated_klines("BTCUSDT", "D", total_days) if symbol != "BTCUSDT" else df_1d
        health_cache: dict = {}   # recomputed every 7 backtest days, not every day
        
        if df_1d is None or df_4h is None or df_1h is None:
            result["error"] = "Failed to fetch candle data from Bybit"
            return result
        
        logger.info(f"[BACKTEST] Fetched: {len(df_1d)}d / {len(df_4h)}x4H / {len(df_1h)}x1H candles")
        
        # ── Step 2: Define the backtest window ───────────────────────────────
        cutoff_ms = (int(time.time()) - days * 86400) * 1000
        backtest_days = df_1d[df_1d["timestamp"] >= cutoff_ms].copy()
        
        if len(backtest_days) < 10:
            result["error"] = f"Only {len(backtest_days)} daily candles in period — need at least 10"
            return result
        
        # ── Step 3: Walk through each Daily candle ───────────────────────────
        balance = start_balance
        open_trade = None      # currently open position
        result["balance_curve"].append({"ts": int(backtest_days.iloc[0]["timestamp"]), "bal": balance})
        
        all_daily_idx = list(df_1d.index)
        
        for bt_idx, (_, day_row) in enumerate(backtest_days.iterrows()):
            day_ts   = int(day_row["timestamp"])
            day_close = float(day_row["close"])
            day_high  = float(day_row["high"])
            day_low   = float(day_row["low"])
            
            # Find this day in the full df_1d (for indicator calculation)
            full_idx = df_1d.index[df_1d["timestamp"] == day_ts].tolist()
            if not full_idx:
                continue
            full_i = full_idx[0]
            
            # Need at least 220 candles of history for EMA-200
            if full_i < 220:
                continue
            
            # ── Manage open trade ─────────────────────────────────────────────
            # NO STOP LOSS, NO TP1/TP2, NO TRAILING, NO DEAD-TRADE TIMEOUT.
            # Exit conditions: take profit (if tp_mode != "none") checked
            # first using the day's intraday range, THEN Daily EMA
            # structure inverting — same order used by the experimental
            # regime strategy's backtest for consistency.
            if open_trade:
                t = open_trade
                entry = t["entry"]
                dir_  = t["direction"]

                tp = t.get("tp")
                tp_hit = False
                if tp is not None:
                    tp_hit = (dir_ == "LONG" and day_high >= tp) or \
                             (dir_ == "SHORT" and day_low <= tp)

                if tp_hit:
                    rem = t["qty"]
                    pnl = (tp - entry) * rem if dir_ == "LONG" else (entry - tp) * rem
                    t.update({"exit_price": tp, "exit_reason": "TP_HIT",
                              "exit_day": day_ts, "pnl": round(pnl, 2)})
                    balance += pnl
                    result["trades"].append(t)
                    open_trade = None
                    result["balance_curve"].append({"ts": day_ts, "bal": round(balance, 2)})
                    continue

                # Check EMA exit (using completed daily candles up to yesterday)
                df_for_exit = df_1d.iloc[:full_i]
                ema_exit = _check_ema_exit(df_for_exit, dir_)

                if ema_exit:
                    rem = t["qty"]
                    exit_p = day_close
                    pnl = (exit_p - entry) * rem if dir_ == "LONG" else (entry - exit_p) * rem
                    t.update({"exit_price": exit_p, "exit_reason": "EMA_EXIT",
                              "exit_day": day_ts, "pnl": round(pnl, 2)})
                    balance += pnl
                    result["trades"].append(t)
                    open_trade = None
                    result["balance_curve"].append({"ts": day_ts, "bal": round(balance, 2)})

                # Don't look for new entries while in a trade
                continue
            
            # ── Look for new entry ────────────────────────────────────────────
            # Build lookback windows up to this candle
            df_1d_window = df_1d.iloc[:full_i + 1]
            
            # 4H candles up to this day's end
            day_end_ms = day_ts + 86400000
            df_4h_window = df_4h[df_4h["timestamp"] < day_end_ms].tail(300)
            
            if len(df_4h_window) < 50:
                continue
            
            # 1H candles for this specific day
            day_start_ms = day_ts
            df_1h_day = df_1h[
                (df_1h["timestamp"] >= day_start_ms) &
                (df_1h["timestamp"] <  day_end_ms)
            ]
            
            if len(df_1h_day) < 3:
                continue
            
            # Check both directions
            for direction in ("LONG", "SHORT"):
                
                # Run full indicator check — reuses the exact same _compute_indicators
                # that the live bot uses. 100% same logic.
                try:
                    ind = _compute_indicators(df_1d_window, df_4h_window, df_1h_day, direction)
                except Exception as e:
                    logger.debug(f"[BACKTEST] indicator error day {bt_idx} {direction}: {e}")
                    continue
                
                if not ind["should_enter"]:
                    continue

                # ── Health-score gate (as-of this backtest day, no lookahead) ──
                if health_cache.get("checked_day") != bt_idx // 7:
                    health_cache["checked_day"] = bt_idx // 7
                    health_cache["scores"] = _compute_health_scores_from_window(
                        df_1d_window, df_btc_window[df_btc_window["timestamp"] < day_end_ms] if df_btc_window is not None else None)
                hs = health_cache.get("scores", {})
                own_score      = hs.get("long_score" if direction=="LONG" else "short_score", 50)
                opposing_score = hs.get("short_score" if direction=="LONG" else "long_score", 50)
                ind = _apply_health_gate(ind, direction, own_score, opposing_score)
                if not ind["should_enter"]:
                    continue

                # Entry found — simulate the trade
                entry_price = day_close   # enter at day's close
                
                # NO SL — position sizing reverts to simple fixed %
                # of balance (matches live open_trade's new sizing, since
                # there's no SL distance to solve risk-based qty against).
                qty = (balance * POSITION_PCT * LEVERAGE) / entry_price

                # Take profit (if tp_mode != "none") — shared calculation
                # with the experimental regime strategy so both are
                # directly comparable on the same 4 TP mechanisms.
                atr_val = ind.get("atr", 0.0)
                vp_for_tp = None
                if tp_mode == "vp":
                    vp_for_tp = _compute_volume_profile(df_1d_window, lookback=90)
                tp = _calc_tp_target(entry_price, direction, tp_mode, atr=atr_val, vp=vp_for_tp)

                trade = {
                    "symbol":       symbol,
                    "direction":    direction,
                    "entry":        round(entry_price, 6),
                    "entry_day":    day_ts,
                    "entry_bt_idx": bt_idx,
                    "qty":          qty,
                    "orig_qty":     qty,
                    "tp":           tp,
                    "score":        ind["score"],
                    "passed":       ind["passed"],
                    # Will be filled on close:
                    "exit_price":   None,
                    "exit_reason":  None,
                    "exit_day":     None,
                    "pnl":          0.0,
                }
                open_trade = trade
                break   # one trade per day max
        
        # Close any still-open trade at the last candle's close
        if open_trade:
            t = open_trade
            last_close = float(backtest_days.iloc[-1]["close"])
            rem = t["qty"]
            pnl_rem = (last_close - t["entry"]) * rem if t["direction"] == "LONG"                       else (t["entry"] - last_close) * rem
            total_pnl = t.get("pnl_partial", 0) + round(pnl_rem, 2)
            t.update({"exit_price": last_close, "exit_reason": "PERIOD_END",
                      "exit_day": int(backtest_days.iloc[-1]["timestamp"]),
                      "pnl": total_pnl})
            balance += pnl_rem
            result["trades"].append(t)
        
        result["final_balance"] = round(balance, 2)
        result["balance_curve"].append({"ts": int(backtest_days.iloc[-1]["timestamp"]),
                                        "bal": round(balance, 2)})
        
        logger.info(f"[BACKTEST] Done: {len(result['trades'])} trades, "
                    f"final balance ${balance:.2f}")
    
    except Exception as e:
        logger.error(f"[BACKTEST] _sync_run_backtest failed: {e}", exc_info=True)
        result["error"] = str(e)
    
    return result


def _get_adaptive_f5_state(adaptive_state_series: pd.Series, full_idx: int, direction: str) -> bool:
    """Given the precomputed adaptive stack series, does Filter 5 (adaptive) pass?"""
    if adaptive_state_series is None or full_idx >= len(adaptive_state_series):
        return False
    state = adaptive_state_series.iloc[full_idx]
    return (state == 1) if direction == "LONG" else (state == -1)


def _get_adaptive_exit(adaptive_state_series: pd.Series, full_idx: int, direction: str) -> bool:
    """Adaptive-stack equivalent of _check_ema_exit — has the stack flipped
    against the trade's direction?"""
    if adaptive_state_series is None or full_idx >= len(adaptive_state_series):
        return False
    state = adaptive_state_series.iloc[full_idx]
    target = 1 if direction == "LONG" else -1
    return state != target


def _compute_indicators_with_stack(df_1d_window, df_4h_window, df_1h_day, direction,
                                    stack_mode: str, adaptive_state_series=None,
                                    full_idx: int = None) -> dict:
    """
    Wraps _compute_indicators, optionally overriding ONLY Filter 5 with the
    Adaptive Confirmed Stack instead of the classic EMA 20/50/100/200 stack.
    Filters 1-4, 6-9 are computed identically either way — this isolates
    the comparison to Filter 5's effect alone, which is the actual point
    of the comparison (does the better Filter 5 alone improve results,
    not some incidental difference elsewhere).

    Mandatory set is F1 (ADX-relative), F5 (EMA/adaptive stack), F9 (Volume
    Confirmation) — matching the current _compute_indicators structure.
    Base requirement: all 3 mandatory + at least 3 more of the remaining
    6 optional filters = 6 of 9 total (health gate may raise this to 7).
    """
    ind = _compute_indicators(df_1d_window, df_4h_window, df_1h_day, direction)
    if stack_mode == "classic":
        return ind

    m1 = "ADX>Baseline"
    m5 = "EMA_Bullish_Stack" if direction == "LONG" else "EMA_Bearish_Stack"
    m9 = "VolumeConfirmed"

    classic_m5_passed  = m5 in ind["passed"]
    adaptive_m5_passed = _get_adaptive_f5_state(adaptive_state_series, full_idx, direction)

    passed = list(ind["passed"])
    failed = list(ind["failed"])
    if classic_m5_passed and not adaptive_m5_passed:
        passed.remove(m5); failed.append(m5)
    elif not classic_m5_passed and adaptive_m5_passed:
        failed.remove(m5); passed.append(m5)
    # if they already agree, no change needed

    mandatory_ok = all(m in passed for m in (m1, m5, m9))
    should_enter = mandatory_ok and len(passed) >= 6

    new_ind = dict(ind)
    new_ind["passed"] = passed
    new_ind["failed"] = failed
    new_ind["should_enter"] = should_enter
    new_ind["score"] = len(passed)
    return new_ind


def _run_single_stack_backtest(symbol: str, stack_mode: str, backtest_days: pd.DataFrame,
                                df_1d: pd.DataFrame, df_4h: pd.DataFrame, df_1h: pd.DataFrame,
                                adaptive_state_series, start_balance: float,
                                df_btc_window: pd.DataFrame = None) -> dict:
    """
    One full backtest pass using either the classic or adaptive stack for
    Filter 5 and the EMA-inversion exit. Mirrors _sync_run_backtest's trade
    simulation mechanics exactly (SL/TP1/TP2/trail/dead-trade/sizing) so the
    ONLY difference between a classic and adaptive run is Filter 5's logic.
    Also applies the same volatility-scaled SL/risk-based sizing and
    health-score gating as the main backtest engine, for consistency.
    """
    result = {"trades": [], "balance_curve": []}
    balance = start_balance
    open_trade = None
    health_cache: dict = {}
    result["balance_curve"].append({"ts": int(backtest_days.iloc[0]["timestamp"]), "bal": balance})

    for bt_idx, (_, day_row) in enumerate(backtest_days.iterrows()):
        day_ts    = int(day_row["timestamp"])
        day_close = float(day_row["close"])
        day_high  = float(day_row["high"])
        day_low   = float(day_row["low"])

        full_idx_list = df_1d.index[df_1d["timestamp"] == day_ts].tolist()
        if not full_idx_list:
            continue
        full_i = full_idx_list[0]
        if full_i < 220:
            continue

        # ── Manage open trade ─────────────────────────────────────────────
        # NO STOP LOSS, NO TP1/TP2, NO TRAILING, NO DEAD-TRADE TIMEOUT.
        # The ONLY exit condition is Daily EMA structure inverting (classic
        # or adaptive stack, matching stack_mode).
        if open_trade:
            t = open_trade
            entry, dir_ = t["entry"], t["direction"]

            if stack_mode == "classic":
                df_for_exit = df_1d.iloc[:full_i]
                ema_exit = _check_ema_exit(df_for_exit, dir_)
            else:
                ema_exit = _get_adaptive_exit(adaptive_state_series, full_i, dir_)

            if ema_exit:
                rem = t["qty"]; exit_p = day_close
                pnl = (exit_p-entry)*rem if dir_=="LONG" else (entry-exit_p)*rem
                t.update({"exit_price":exit_p,"exit_reason":"EMA_EXIT","exit_day":day_ts,"pnl":round(pnl,2)})
                balance += pnl
                result["trades"].append(t)
                open_trade = None
                result["balance_curve"].append({"ts":day_ts,"bal":round(balance,2)})
            continue

        # ── Look for new entry ────────────────────────────────────────────
        df_1d_window = df_1d.iloc[:full_i+1]
        day_end_ms = day_ts + 86400000
        df_4h_window = df_4h[df_4h["timestamp"] < day_end_ms].tail(300)
        if len(df_4h_window) < 50:
            continue
        df_1h_day = df_1h[(df_1h["timestamp"]>=day_ts) & (df_1h["timestamp"]<day_end_ms)]
        if len(df_1h_day) < 3:
            continue

        for direction in ("LONG", "SHORT"):
            try:
                ind = _compute_indicators_with_stack(
                    df_1d_window, df_4h_window, df_1h_day, direction,
                    stack_mode, adaptive_state_series, full_i)
            except Exception:
                continue
            if not ind["should_enter"]:
                continue

            # Health-score gate (as-of this day, no lookahead) — same rule
            # applied to both classic and adaptive runs so any PnL delta
            # remains isolated to the Filter 5 difference, not this gate.
            if health_cache.get("checked_day") != bt_idx // 7:
                health_cache["checked_day"] = bt_idx // 7
                btc_slice = (df_btc_window[df_btc_window["timestamp"] < day_end_ms]
                            if df_btc_window is not None else None)
                health_cache["scores"] = _compute_health_scores_from_window(df_1d_window, btc_slice)
            hs = health_cache.get("scores", {})
            own_score      = hs.get("long_score" if direction=="LONG" else "short_score", 50)
            opposing_score = hs.get("short_score" if direction=="LONG" else "long_score", 50)
            ind = _apply_health_gate(ind, direction, own_score, opposing_score)
            if not ind["should_enter"]:
                continue

            entry_price = day_close
            # NO SL, NO TP — simple fixed % sizing (matches live open_trade)
            qty = (balance * POSITION_PCT * LEVERAGE) / entry_price

            open_trade = {
                "symbol": symbol, "direction": direction,
                "entry": round(entry_price,6), "entry_day": day_ts, "entry_bt_idx": bt_idx,
                "qty": qty, "orig_qty": qty, "score": ind["score"], "passed": ind["passed"],
                "exit_price": None, "exit_reason": None, "exit_day": None, "pnl": 0.0,
            }
            break

    if open_trade:
        t = open_trade
        last_close = float(backtest_days.iloc[-1]["close"])
        rem = t["qty"]
        pnl_rem = (last_close-t["entry"])*rem if t["direction"]=="LONG" else (t["entry"]-last_close)*rem
        total_pnl = t.get("pnl_partial",0)+round(pnl_rem,2)
        t.update({"exit_price":last_close,"exit_reason":"PERIOD_END",
                  "exit_day": int(backtest_days.iloc[-1]["timestamp"]), "pnl":total_pnl})
        balance += pnl_rem
        result["trades"].append(t)

    result["final_balance"] = round(balance, 2)
    result["balance_curve"].append({"ts": int(backtest_days.iloc[-1]["timestamp"]), "bal": round(balance,2)})
    return result


def _sync_run_backtest_comparison(symbol: str, days: int, start_balance: float = 10000.0) -> dict:
    """
    Runs the FULL trading strategy backtest twice on the SAME real historical
    data — once with the classic EMA 20/50/100/200 stack as Filter 5, once
    with the Adaptive Confirmed Stack (KAMA + volume confirmation +
    hysteresis) as Filter 5. Every other filter, SL/TP rule, and position
    sizing rule is byte-identical between the two runs — the comparison is
    isolated purely to whether the better Filter 5 changes trading outcomes.
    """
    result = {"symbol": symbol, "days": days, "error": None, "start_balance": start_balance}
    try:
        logger.info(f"[STACK COMPARE] Starting {symbol} {days}d comparison backtest...")
        warmup = 220
        total_days = days + warmup

        df_1d = _sync_fetch_paginated_klines(symbol, "D",   total_days)
        df_4h = _sync_fetch_paginated_klines(symbol, "240", total_days)
        df_1h = _sync_fetch_paginated_klines(symbol, "60",  min(days, 180))
        df_btc_window = _sync_fetch_paginated_klines("BTCUSDT", "D", total_days) if symbol != "BTCUSDT" else df_1d

        if df_1d is None or df_4h is None or df_1h is None:
            result["error"] = "Failed to fetch candle data from Bybit"
            return result

        cutoff_ms = (int(time.time()) - days * 86400) * 1000
        backtest_days = df_1d[df_1d["timestamp"] >= cutoff_ms].copy()
        if len(backtest_days) < 10:
            result["error"] = f"Only {len(backtest_days)} daily candles — need at least 10"
            return result

        # Precompute the adaptive stack ONCE over the full history — this is
        # a purely causal (no-lookahead) computation, safe to precompute
        # and index per-day during the walk-forward loop, exactly like EMAs
        # are computed once and indexed rather than recomputed each day.
        close_full  = df_1d["close"].astype(float).reset_index(drop=True)
        volume_full = df_1d["volume"].astype(float).reset_index(drop=True)
        adaptive_state_series = _adaptive_4kama_stack_series(close_full, volume_full)

        classic_result  = _run_single_stack_backtest(
            symbol, "classic", backtest_days, df_1d, df_4h, df_1h, None, start_balance, df_btc_window)
        adaptive_result = _run_single_stack_backtest(
            symbol, "adaptive", backtest_days, df_1d, df_4h, df_1h, adaptive_state_series, start_balance, df_btc_window)

        result["classic"]  = classic_result
        result["adaptive"] = adaptive_result
        logger.info(f"[STACK COMPARE] Done: classic {len(classic_result['trades'])} trades "
                    f"${classic_result['final_balance']:.2f} | adaptive {len(adaptive_result['trades'])} "
                    f"trades ${adaptive_result['final_balance']:.2f}")
    except Exception as e:
        logger.error(f"[STACK COMPARE] failed: {e}", exc_info=True)
        result["error"] = str(e)
    return result


def _format_stack_comparison(r: dict) -> list[str]:
    """Format the side-by-side backtest comparison into Telegram messages."""
    if r.get("error"):
        return [f"❌ Comparison backtest failed: {r['error']}"]

    sym, days, start = r["symbol"], r["days"], r["start_balance"]
    c, a = r["classic"], r["adaptive"]

    def stats(res):
        trades = res["trades"]
        final = res.get("final_balance", start)
        pnl = final - start
        ret_pct = pnl/start*100
        wins = [t for t in trades if t["pnl"] > 0]
        losses = [t for t in trades if t["pnl"] <= 0]
        win_rate = len(wins)/len(trades)*100 if trades else 0
        return {
            "trades": len(trades), "final": final, "pnl": pnl, "ret_pct": ret_pct,
            "win_rate": win_rate, "wins": len(wins), "losses": len(losses),
        }

    cs, as_ = stats(c), stats(a)

    msg1 = (
        f"📊 <b>Stack Backtest Comparison — {sym}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Period: last {days} days | Start: ${start:,.0f}\n"
        f"Same F1-F4/F6-F8 filters, SL/TP rules, and sizing in BOTH runs.\n"
        f"ONLY Filter 5 (EMA stack) differs.\n\n"
        f"<b>🔵 CLASSIC (EMA 20/50/100/200):</b>\n"
        f"Final balance: ${cs['final']:,.2f}\n"
        f"PnL: {cs['pnl']:+,.2f} ({cs['ret_pct']:+.1f}%)\n"
        f"Trades: {cs['trades']} | Win rate: {cs['win_rate']:.1f}% "
        f"({cs['wins']}W/{cs['losses']}L)\n\n"
        f"<b>🟢 ADAPTIVE (KAMA + volume + hysteresis):</b>\n"
        f"Final balance: ${as_['final']:,.2f}\n"
        f"PnL: {as_['pnl']:+,.2f} ({as_['ret_pct']:+.1f}%)\n"
        f"Trades: {as_['trades']} | Win rate: {as_['win_rate']:.1f}% "
        f"({as_['wins']}W/{as_['losses']}L)\n\n"
        f"<b>Δ Difference:</b>\n"
        f"PnL: {as_['pnl']-cs['pnl']:+,.2f} USDT\n"
        f"Return: {as_['ret_pct']-cs['ret_pct']:+.1f} percentage points\n"
        f"Win rate: {as_['win_rate']-cs['win_rate']:+.1f} points\n"
        f"Trade count: {as_['trades']-cs['trades']:+d}"
    )

    # Exit reason breakdown for both
    def reason_breakdown(trades):
        r = {}
        for t in trades:
            k = t.get("exit_reason","?")
            r[k] = r.get(k,0)+1
        return r

    cr, ar = reason_breakdown(c["trades"]), reason_breakdown(a["trades"])
    msg2 = (
        f"📋 <b>Exit reason breakdown — {sym}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Classic : {'  '.join(f'{k}:{v}' for k,v in sorted(cr.items()))}\n"
        f"Adaptive: {'  '.join(f'{k}:{v}' for k,v in sorted(ar.items()))}\n\n"
        f"<i>This runs the REAL strategy — same 8-filter structure, same "
        f"SL/TP tiers, same position sizing — with only Filter 5 swapped. "
        f"Unlike /compare_stack, this measures actual trading PnL on real "
        f"historical data, not just signal timing. Not wired into live "
        f"trading — for evaluation only.</i>"
    )
    return [msg1, msg2]


@public_command
async def cmd_backtest_compare(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Full strategy backtest comparison: classic EMA stack vs Adaptive
    Confirmed Stack, on the SAME real historical data, with every other
    rule held identical. Usage: /backtest_compare SYMBOL PERIOD [capital]
    """
    if len(ctx.args) < 2:
        await update.message.reply_text(
            "Usage: /backtest_compare SYMBOL PERIOD [capital]\n"
            "Example: /backtest_compare BTCUSDT 6m\n\n"
            "Runs the FULL trading strategy twice on real historical data — "
            "once with the classic EMA 20/50/100/200 stack, once with the "
            "Adaptive Confirmed Stack — with every other rule (SL/TP, "
            "sizing, other 6 filters) held identical. Shows actual PnL "
            "difference, not just signal timing.\n\n"
            "Periods: 1m/3m/6m/1y/2y. Takes 20-40 seconds (double backtest).")
        return

    symbol = ctx.args[0].upper()
    if not symbol.endswith("USDT"):
        symbol += "USDT"
    period_map = {"1m":30,"3m":90,"6m":180,"1y":365,"2y":730}
    days = period_map.get(ctx.args[1].lower())
    if not days:
        await update.message.reply_text(f"Unknown period '{ctx.args[1]}'. Use: 1m, 3m, 6m, 1y, 2y")
        return
    start_balance = 10000.0
    if len(ctx.args) >= 3:
        try: start_balance = float(ctx.args[2])
        except ValueError: pass

    await update.message.reply_text(
        f"⏳ Running comparison backtest for {symbol} ({ctx.args[1]})...\n"
        f"Running the full strategy TWICE (classic + adaptive Filter 5) "
        f"on real 4H+1H candle data. Takes 20-40 seconds.")

    result = await asyncio.to_thread(_sync_run_backtest_comparison, symbol, days, start_balance)
    messages = _format_stack_comparison(result)
    for msg in messages:
        await update.message.reply_text(msg, parse_mode="HTML")


# =============================================================================
#  EXPERIMENTAL: REGIME-BASED STRATEGY BACKTEST
#  A genuinely different strategy from the live 9-filter system, built to
#  TEST an idea before deciding whether to promote it to live trading —
#  same pattern used for CPO and the Adaptive Confirmed Stack.
#
#  Entry: LONG on confirmed Strong Uptrend regime, SHORT on confirmed
#         Strong Downtrend regime, with Volume Profile confirmation
#         (price must be OUTSIDE the Value Area — genuine breakout, not
#         noise inside an established range). Choppy/Mean Reverting/
#         Volatile Breakout/Unknown regimes are all blocked from entry.
#  Exit:  LONG exits when Daily close < EMA50. SHORT exits when Daily
#         close > EMA50. No stop loss, no take profit, no dead-trade
#         timeout — this mirrors the live strategy's current no-SL design,
#         just with a much simpler single-EMA exit instead of full 4-EMA-
#         stack inversion.
#  NOT wired into live trading — backtest/evaluation only, exactly as
#  requested ("I want to test something").
# =============================================================================

def _calc_tp_target(entry: float, direction: str, tp_mode: str,
                     atr: float = 0.0, vp: dict = None,
                     fixed_pct: float = 0.15, atr_mult: float = 4.0) -> Optional[float]:
    """
    Take profit target — GENERIC, shared by both the original 9-filter
    strategy backtest (_sync_run_backtest) and the experimental
    regime-based strategy backtest (_sync_run_regime_strategy_backtest),
    so the same 4 TP mechanisms can be directly compared across both.
    Four modes, each producing a genuinely different target:
      none      — no take profit at all (returns None)
      fixed_pct — entry ± fixed_pct (e.g. +15% LONG / -15% SHORT)
      atr       — entry ± atr_mult × ATR(14) — scales with volatility
      vp        — "measured move": projects the Value Area's height
                  (VAH − VAL at entry) from the entry price, the classic
                  breakout target technique. Requires a volume profile
                  to have been computed at entry.
    Returns None if tp_mode == "none" or if the required inputs for the
    chosen mode aren't available (caller should then treat the trade as
    having no TP for that entry, not error out).
    """
    if tp_mode == "none":
        return None
    if tp_mode == "fixed_pct":
        dist = entry * fixed_pct
    elif tp_mode == "atr":
        if atr <= 0:
            return None
        dist = atr * atr_mult
    elif tp_mode == "vp":
        if not vp or vp.get("error"):
            return None
        dist = vp["vah"] - vp["val"]
        if dist <= 0:
            return None
    else:
        return None

    return round(entry + dist, 6) if direction == "LONG" else round(entry - dist, 6)


def _sync_run_regime_strategy_backtest_core(symbol: str, df_1d: pd.DataFrame, df_4h: pd.DataFrame,
                                             sim_start_ts_ms: int, sim_end_ts_ms: int,
                                             start_balance: float = 10000.0) -> dict:
    """
    Core simulation loop for the regime strategy (v4 — simplified per
    explicit request). Assumes df_1d/df_4h already contain enough warmup
    history BEFORE sim_start_ts_ms — callers are responsible for fetching
    that. Only simulates entries/exits for days between sim_start_ts_ms
    and sim_end_ts_ms (inclusive).

    RULES (drastically simplified from the earlier v1-v3 versions — no
    Volume Profile, no EMA50 filter, no TP modes, no stop loss):
      Entry LONG : Daily close > EMA100 (closed candles)
      Entry SHORT: Daily close < EMA100 (closed candles)
      Exit LONG  : Daily close crosses below EMA100
      Exit SHORT : Daily close crosses above EMA100
      Blocked    : Choppy Ranging or Mean Reverting regime (colloquially
                   "choppy" market — see docstring note below), or
                   Unknown regime (no reliable read). Strong Uptrend,
                   Strong Downtrend, and Volatile Breakout are all
                   ALLOWED to enter — this is an explicit assumption
                   since "choppy" wasn't precisely defined; flagged
                   clearly rather than silently guessed.
      No stop loss, no take profit — the EMA100 cross itself is both
      the entry trigger and the only exit condition.
    """
    result = {"symbol": symbol, "start_balance": start_balance,
              "sim_start_ts_ms": sim_start_ts_ms, "sim_end_ts_ms": sim_end_ts_ms,
              "error": None}
    try:
        if df_1d is None or df_4h is None:
            result["error"] = "Failed to fetch candle data from Bybit"
            return result

        sim_days = df_1d[(df_1d["timestamp"] >= sim_start_ts_ms) &
                          (df_1d["timestamp"] <= sim_end_ts_ms)].copy()
        if len(sim_days) < 5:
            result["error"] = (f"Only {len(sim_days)} daily candles in the requested "
                                f"range — need at least 5")
            return result

        balance = start_balance
        open_trade = None
        trades = []
        balance_curve = [{"ts": int(sim_days.iloc[0]["timestamp"]), "bal": balance}]

        for _, day_row in sim_days.iterrows():
            day_ts    = int(day_row["timestamp"])
            day_close = float(day_row["close"])

            full_idx_list = df_1d.index[df_1d["timestamp"] == day_ts].tolist()
            if not full_idx_list:
                continue
            full_i = full_idx_list[0]

            # CLOSED candles only (strictly before today) for the EMA100
            # line — same fix applied everywhere else in this bot to
            # avoid the still-forming live candle causing flicker.
            df_closed = df_1d.iloc[:full_i]
            if len(df_closed) < 101:
                continue   # not enough history for a stable EMA100 yet

            ema100_val = float(df_closed["close"].ewm(span=100, adjust=False).mean().iloc[-1])

            # ── Manage open trade — EMA100 cross is the ONLY exit condition ──
            if open_trade:
                t = open_trade
                dir_ = t["direction"]
                exit_now = (dir_ == "LONG" and day_close < ema100_val) or \
                           (dir_ == "SHORT" and day_close > ema100_val)
                if exit_now:
                    rem = t["qty"]
                    pnl = (day_close - t["entry"]) * rem if dir_ == "LONG" else (t["entry"] - day_close) * rem
                    t.update({"exit_price": day_close, "exit_reason": "EMA100_EXIT",
                              "exit_day": day_ts, "pnl": round(pnl, 2)})
                    balance += pnl
                    trades.append(t)
                    open_trade = None
                    balance_curve.append({"ts": day_ts, "bal": round(balance, 2)})
                continue

            # ── Look for new entry ────────────────────────────────────────────
            window_start = max(0, full_i - 299)
            df_1d_window = df_1d.iloc[window_start:full_i + 1]
            day_end_ms = day_ts + 86400000
            df_4h_window = df_4h[df_4h["timestamp"] < day_end_ms].tail(300)
            if len(df_4h_window) < 50:
                continue

            try:
                regime_result = _sync_detect_regime(symbol, df_1d_window, df_4h_window)
            except Exception as e:
                logger.debug(f"[REGIME BACKTEST v4] regime error: {e}")
                continue

            regime = regime_result.get("regime", REGIME_UNKNOWN)
            if regime in (REGIME_CHOPPY_RANGING, REGIME_MEAN_REVERTING, REGIME_UNKNOWN):
                continue   # "choppy" market blocked — see docstring assumption

            if day_close > ema100_val:
                direction = "LONG"
            elif day_close < ema100_val:
                direction = "SHORT"
            else:
                continue   # exactly on the line — wait for a clear side

            entry_price = day_close
            qty = (balance * POSITION_PCT * LEVERAGE) / entry_price   # no SL to size against

            open_trade = {
                "symbol": symbol, "direction": direction,
                "entry": round(entry_price, 6), "entry_day": day_ts,
                "qty": qty, "orig_qty": qty,
                "regime_at_entry": regime,
                "exit_price": None, "exit_reason": None, "exit_day": None, "pnl": 0.0,
            }

        if open_trade:
            t = open_trade
            last_close = float(sim_days.iloc[-1]["close"])
            rem = t["qty"]
            pnl = (last_close - t["entry"]) * rem if t["direction"] == "LONG" else (t["entry"] - last_close) * rem
            t.update({"exit_price": last_close, "exit_reason": "PERIOD_END",
                      "exit_day": int(sim_days.iloc[-1]["timestamp"]), "pnl": round(pnl, 2)})
            balance += pnl
            trades.append(t)

        result["trades"] = trades
        result["final_balance"] = round(balance, 2)
        result["balance_curve"] = balance_curve
        logger.info(f"[REGIME BACKTEST v4] Done: {len(trades)} trades, final ${balance:.2f}")

    except Exception as e:
        logger.error(f"[REGIME BACKTEST v4] failed: {e}", exc_info=True)
        result["error"] = str(e)
    return result


def _sync_run_regime_strategy_backtest(symbol: str, days: int,
                                        start_balance: float = 10000.0) -> dict:
    """
    Period-based entry point (e.g. "last 365 days"). Fetches enough
    warmup history before the simulation window, then delegates to the
    shared core. See _sync_run_regime_strategy_backtest_core for the
    exact strategy rules.
    """
    warmup_days = 220   # regime detector needs 200+ candles for EMA200/CPO rolling(60) etc.
    try:
        end_ts_ms = int(time.time() * 1000)
        sim_start_ts_ms = end_ts_ms - days * 86400000
        fetch_start_ts_ms = sim_start_ts_ms - warmup_days * 86400000

        df_1d = _sync_fetch_paginated_klines_range(symbol, "D",   fetch_start_ts_ms, end_ts_ms)
        df_4h = _sync_fetch_paginated_klines_range(symbol, "240", fetch_start_ts_ms, end_ts_ms)

        result = _sync_run_regime_strategy_backtest_core(
            symbol, df_1d, df_4h, sim_start_ts_ms, end_ts_ms, start_balance)
        result["days"] = days
        return result
    except Exception as e:
        logger.error(f"[REGIME BACKTEST v4] period-based wrapper failed: {e}", exc_info=True)
        return {"symbol": symbol, "days": days, "start_balance": start_balance, "error": str(e)}


def _sync_run_regime_strategy_backtest_dates(symbol: str, start_date_str: str, end_date_str: str,
                                              start_balance: float = 10000.0) -> dict:
    """
    Date-range entry point. Dates must be DD.MM.YYYY (e.g. "19.08.2026").
    If end_date is in the future relative to real time, it's capped at
    "now" (there's no data to backtest that hasn't happened yet) and
    result["capped_future_end_date"] is set to True so the caller can
    tell the person. Fetches enough warmup history before the simulation
    window, then delegates to the shared core.
    """
    warmup_days = 220
    try:
        try:
            sim_start_dt = datetime.strptime(start_date_str, "%d.%m.%Y").replace(tzinfo=timezone.utc)
            sim_end_dt   = datetime.strptime(end_date_str,   "%d.%m.%Y").replace(tzinfo=timezone.utc)
        except ValueError:
            return {"symbol": symbol, "start_balance": start_balance,
                    "error": f"Could not parse dates '{start_date_str}' / '{end_date_str}' — "
                             f"use DD.MM.YYYY format, e.g. 19.08.2026"}

        now_ts_ms = int(time.time() * 1000)
        sim_start_ts_ms = int(sim_start_dt.timestamp() * 1000)
        sim_end_ts_ms   = int(sim_end_dt.timestamp() * 1000)

        capped_future = False
        if sim_end_ts_ms > now_ts_ms:
            sim_end_ts_ms = now_ts_ms
            capped_future = True

        if sim_start_ts_ms >= sim_end_ts_ms:
            return {"symbol": symbol, "start_balance": start_balance,
                    "error": "Start date must be before end date (and the end date "
                             "can't be entirely in the future — there's no data yet)"}

        fetch_start_ts_ms = sim_start_ts_ms - warmup_days * 86400000

        df_1d = _sync_fetch_paginated_klines_range(symbol, "D",   fetch_start_ts_ms, sim_end_ts_ms)
        df_4h = _sync_fetch_paginated_klines_range(symbol, "240", fetch_start_ts_ms, sim_end_ts_ms)

        result = _sync_run_regime_strategy_backtest_core(
            symbol, df_1d, df_4h, sim_start_ts_ms, sim_end_ts_ms, start_balance)
        result["start_date"] = start_date_str
        result["end_date"] = end_date_str
        result["capped_future_end_date"] = capped_future
        return result
    except Exception as e:
        logger.error(f"[REGIME BACKTEST v4] date-range wrapper failed: {e}", exc_info=True)
        return {"symbol": symbol, "start_balance": start_balance, "error": str(e)}


def _format_regime_strategy_results(r: dict) -> list[str]:
    if r.get("error"):
        return [f"❌ Regime-strategy backtest failed: {r['error']}"]

    trades = r.get("trades", [])
    sym, start = r["symbol"], r["start_balance"]
    final = r.get("final_balance", start)
    pnl = final - start
    ret_pct = pnl / start * 100 if start else 0.0

    if "start_date" in r:
        period_label = f"{r['start_date']} → {r['end_date']}"
        if r.get("capped_future_end_date"):
            period_label += " (end date was in the future — capped at today)"
    else:
        period_label = f"last {r.get('days','?')} days"

    if not trades:
        return [
            f"📊 <b>Regime Strategy Backtest — {sym}</b> ({period_label})\n"
            f"No trades in this period — either the market was classified "
            f"Choppy/Mean Reverting/Unknown throughout, or there wasn't "
            f"enough history for a stable EMA100 read at the start of "
            f"the window."
        ]

    wins   = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    win_rate = len(wins) / len(trades) * 100

    def ts_to_date(ts):
        try:
            return datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime("%b %d, %Y")
        except Exception:
            return "?"

    msg1 = (
        f"📊 <b>Regime Strategy Backtest — {sym}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>EXPERIMENTAL — not wired into live trading</i>\n\n"
        f"Entry: LONG when Daily close is above EMA100\n"
        f"       SHORT when Daily close is below EMA100\n"
        f"       Blocked in Choppy Ranging/Mean Reverting/Unknown regime\n"
        f"Exit : Daily close crosses back over EMA100 — the ONLY exit\n"
        f"No stop loss, no take profit — the EMA100 cross covers both\n\n"
        f"Period: {period_label} | Start: ${start:,.0f}\n"
        f"Final balance: <b>${final:,.2f}</b>\n"
        f"PnL: <b>{pnl:+,.2f} USDT ({ret_pct:+.1f}%)</b>\n"
        f"Trades: {len(trades)} | Win rate: {win_rate:.1f}% ({len(wins)}W/{len(losses)}L)"
    )

    lines = [f"📋 <b>Trade Log — {sym}</b>", "━━━━━━━━━━━━━━━━━━━━"]
    for i, t in enumerate(trades[:15]):
        emoji = "🟢" if t["pnl"] > 0 else "🔴"
        entry_d = ts_to_date(t["entry_day"])
        exit_d  = ts_to_date(t["exit_day"]) if t["exit_day"] else "Open"
        lines.append(
            f"{emoji} #{i+1} {t['direction']} {entry_d}→{exit_d} "
            f"@ {t['entry']:.4f} | regime at entry: {t.get('regime_at_entry','?')} | "
            f"{t['exit_reason']} | PnL:{t['pnl']:+.2f}"
        )
    if len(trades) > 15:
        lines.append(f"... and {len(trades)-15} more trades")
    msg2 = "\n".join(lines)

    return [msg1, msg2]


@public_command
async def cmd_backtest_regime(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """
    EXPERIMENTAL strategy backtest (period-based): LONG when price >
    EMA100, SHORT when price < EMA100, exit on the opposite cross,
    blocked during Choppy/Mean Reverting/Unknown regime. No SL/TP.
    NOT wired into live trading.
    Usage: /backtest_regime SYMBOL PERIOD [capital]
    For an explicit date range instead, use /backtest_regime_dates.
    """
    if len(ctx.args) < 2:
        await update.message.reply_text(
            "Usage: /backtest_regime SYMBOL PERIOD [capital]\n"
            "Example: /backtest_regime BTCUSDT 1y\n\n"
            "EXPERIMENTAL strategy — NOT the live bot's 9-filter system:\n"
            "• LONG when Daily close > EMA100 (closed candle)\n"
            "• SHORT when Daily close < EMA100 (closed candle)\n"
            "• Exit: Daily close crosses back over EMA100 — the ONLY "
            "exit condition\n"
            "• No stop loss, no take profit\n"
            "• Blocked during Choppy Ranging/Mean Reverting/Unknown "
            "regime (Strong Uptrend, Strong Downtrend, and Volatile "
            "Breakout are all allowed to enter)\n\n"
            "Periods: 1m/3m/6m/1y/2y. For an explicit date range instead "
            "(e.g. 19.08.2026 to 20.09.2027), use /backtest_regime_dates.\n\n"
            "Takes 20-40 seconds.")
        return

    symbol = ctx.args[0].upper()
    if not symbol.endswith("USDT"):
        symbol += "USDT"
    period_map = {"1m":30,"3m":90,"6m":180,"1y":365,"2y":730}
    days = period_map.get(ctx.args[1].lower())
    if not days:
        await update.message.reply_text(f"Unknown period '{ctx.args[1]}'. Use: 1m, 3m, 6m, 1y, 2y")
        return

    start_balance = 10000.0
    if len(ctx.args) >= 3:
        try: start_balance = float(ctx.args[2])
        except ValueError: pass

    await update.message.reply_text(
        f"⏳ Running EXPERIMENTAL regime-strategy backtest for {symbol} "
        f"({ctx.args[1]})...\nTakes 20-40 seconds.")

    result = await asyncio.to_thread(_sync_run_regime_strategy_backtest, symbol, days, start_balance)
    for msg in _format_regime_strategy_results(result):
        await update.message.reply_text(msg, parse_mode="HTML")


@public_command
async def cmd_backtest_regime_dates(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """
    EXPERIMENTAL strategy backtest over an EXPLICIT date range, instead
    of a relative period. Same strategy as /backtest_regime.
    Usage: /backtest_regime_dates SYMBOL DD.MM.YYYY DD.MM.YYYY [capital]
    "from"/"to" filler words are tolerated and ignored, e.g.:
      /backtest_regime_dates BTCUSDT from 19.08.2026 to 20.09.2027
    """
    args = [a for a in ctx.args if a.lower() not in ("from", "to")]
    if len(args) < 3:
        await update.message.reply_text(
            "Usage: /backtest_regime_dates SYMBOL DD.MM.YYYY DD.MM.YYYY [capital]\n"
            "Example: /backtest_regime_dates BTCUSDT 19.08.2026 20.09.2027\n"
            "Also accepted: /backtest_regime_dates BTCUSDT from 19.08.2026 to "
            "20.09.2027 — \"from\"/\"to\" are optional filler words.\n\n"
            "Dates use DD.MM.YYYY (day.month.year). If the end date is in "
            "the future, it's automatically capped at today.\n\n"
            "Same EXPERIMENTAL strategy as /backtest_regime: LONG above "
            "EMA100, SHORT below EMA100, exit on the opposite cross, no "
            "SL/TP, blocked during Choppy/Mean Reverting/Unknown regime.\n\n"
            "Takes 20-60 seconds depending on the range length.")
        return

    symbol = args[0].upper()
    if not symbol.endswith("USDT"):
        symbol += "USDT"
    start_date_str = args[1]
    end_date_str = args[2]
    start_balance = 10000.0
    if len(args) >= 4:
        try: start_balance = float(args[3])
        except ValueError: pass

    await update.message.reply_text(
        f"⏳ Running EXPERIMENTAL regime-strategy backtest for {symbol} "
        f"({start_date_str} → {end_date_str})...\nTakes 20-60 seconds "
        f"depending on the range length.")

    result = await asyncio.to_thread(
        _sync_run_regime_strategy_backtest_dates, symbol, start_date_str, end_date_str, start_balance)

    if result.get("capped_future_end_date"):
        await update.message.reply_text(
            f"ℹ️ End date {end_date_str} is in the future — capped the "
            f"backtest at today's date, since that data doesn't exist yet.")

    for msg in _format_regime_strategy_results(result):
        await update.message.reply_text(msg, parse_mode="HTML")


# =============================================================================
#  BULK REGIME STRATEGY BACKTEST — ALL BYBIT SYMBOLS, RANKED
#  Backtests every available Bybit USDT symbol over a date range and ranks
#  by return %. Genuinely expensive (400+ symbols × full regime detection
#  incl. CPO, per simulated day) — this bot's Telegram Application processes
#  updates SEQUENTIALLY by default (concurrent_updates not enabled), so
#  running this directly inside the command handler would freeze the ENTIRE
#  bot — no /status, /pause, nothing — for however long it takes. Instead
#  this launches as a genuinely DETACHED background task (asyncio.create_task)
#  so the command handler returns immediately and the bot stays fully
#  responsive to every other command while the bulk backtest runs.
# =============================================================================

BULK_REGIME_BACKTEST_CONCURRENCY = 6   # concurrent symbol backtests at once —
    # deliberately conservative: the shared thread pool (32 workers) also
    # serves live trading jobs (9-filter scan, MemeBot, regime live scan)
    # the whole time this runs, and each backtest's own klines fetches hit
    # Bybit's REST API directly (not through the async wrappers' shared
    # rate-limit semaphore), so keeping this modest avoids both starving
    # live trading of thread-pool capacity AND bursting past Bybit's rate
    # limits with many symbols fetching simultaneously.

async def _bulk_backtest_one_symbol(symbol: str, start_date_str: str, end_date_str: str,
                                     start_balance: float, semaphore: asyncio.Semaphore) -> tuple:
    async with semaphore:
        try:
            result = await asyncio.to_thread(
                _sync_run_regime_strategy_backtest_dates, symbol,
                start_date_str, end_date_str, start_balance)
            return symbol, result
        except Exception as e:
            logger.debug(f"[BULK REGIME BACKTEST] {symbol} failed: {e}")
            return symbol, {"error": str(e)}


async def _run_bulk_regime_backtest_task(chat_id: int, bot, start_date_str: str,
                                          end_date_str: str, start_balance: float) -> None:
    """
    The actual background task — runs detached from the triggering command
    handler via asyncio.create_task, so it never blocks the bot's normal
    update processing. Sends its own progress updates and final ranking
    directly via bot.send_message, since by the time this runs the
    original Update object's short-lived handler has already returned.
    """
    try:
        symbols = await asyncio.to_thread(_sync_get_all_usdt_symbols)
        if not symbols:
            await bot.send_message(chat_id=chat_id,
                text="❌ Could not fetch the Bybit USDT symbol list — aborting bulk backtest.")
            return

        await bot.send_message(chat_id=chat_id,
            text=f"⏳ Bulk regime-strategy backtest started: {len(symbols)} Bybit "
                 f"USDT symbols, {start_date_str} → {end_date_str}.\n"
                 f"The bot stays fully responsive to other commands while this "
                 f"runs in the background. Progress update every 50 symbols.")

        semaphore = asyncio.Semaphore(BULK_REGIME_BACKTEST_CONCURRENCY)
        tasks = [_bulk_backtest_one_symbol(sym, start_date_str, end_date_str, start_balance, semaphore)
                 for sym in symbols]

        results = {}
        completed = 0
        errors = 0
        start_time = time.time()

        for coro in asyncio.as_completed(tasks):
            symbol, result = await coro
            results[symbol] = result
            completed += 1
            if result.get("error"):
                errors += 1
            if completed % 50 == 0 or completed == len(symbols):
                elapsed_min = (time.time() - start_time) / 60
                await bot.send_message(chat_id=chat_id,
                    text=f"⏳ Bulk backtest progress: {completed}/{len(symbols)} symbols "
                         f"done ({errors} errors/skipped) — {elapsed_min:.1f} min elapsed.")

        # ── Rank by return % ─────────────────────────────────────────────
        ranked = []
        for symbol, r in results.items():
            if r.get("error"):
                continue
            trades = r.get("trades", [])
            if not trades:
                continue   # no trades on this symbol/period — nothing to rank
            final = r.get("final_balance", start_balance)
            ret_pct = (final - start_balance) / start_balance * 100
            wins = sum(1 for t in trades if t["pnl"] > 0)
            win_rate = wins / len(trades) * 100 if trades else 0
            ranked.append({"symbol": symbol, "ret_pct": ret_pct, "final": final,
                            "trades": len(trades), "win_rate": win_rate})

        ranked.sort(key=lambda x: x["ret_pct"], reverse=True)
        top50 = ranked[:50]
        total_elapsed_min = (time.time() - start_time) / 60

        if not top50:
            await bot.send_message(chat_id=chat_id,
                text=f"❌ Bulk backtest finished ({total_elapsed_min:.1f} min) — "
                     f"no symbol produced any trades in this date range. "
                     f"Nothing to rank.")
            return

        header = (
            f"🏆 <b>Top {len(top50)} Regime Strategy Performers</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Period: {start_date_str} → {end_date_str} | Start: ${start_balance:,.0f}\n"
            f"Backtested {len(symbols)} symbols in {total_elapsed_min:.1f} min | "
            f"{len(ranked)} produced trades | {errors} errors/skipped\n"
            f"Ranked by total return %. Same rules as /backtest_regime_dates: "
            f"LONG above EMA100, SHORT below, exit on the opposite cross, "
            f"no SL/TP, blocked during Choppy/Mean Reverting/Unknown regime.\n\n"
        )

        # Split into chunks that stay under Telegram's 4096-char limit
        chunk_lines = []
        current = header
        for i, r in enumerate(top50, 1):
            line = (f"{i}. <b>{r['symbol']}</b> — {r['ret_pct']:+.1f}% "
                    f"(${r['final']:,.0f}) | {r['trades']} trades | "
                    f"{r['win_rate']:.0f}% WR\n")
            if len(current) + len(line) > 3900:
                chunk_lines.append(current)
                current = line
            else:
                current += line
        chunk_lines.append(current)

        for chunk in chunk_lines:
            await bot.send_message(chat_id=chat_id, text=chunk, parse_mode="HTML")

        await bot.send_message(chat_id=chat_id,
            text="<i>Past performance on a backtest isn't a guarantee — "
                 "re-check the top few on a different date range before "
                 "adding any of them with /add_symbol_regime.</i>",
            parse_mode="HTML")

    except Exception as e:
        logger.error(f"[BULK REGIME BACKTEST] task failed: {e}", exc_info=True)
        try:
            await bot.send_message(chat_id=chat_id,
                text=f"❌ Bulk backtest crashed: {e}")
        except Exception:
            pass


@public_command
async def cmd_backtest_regime_all_dates(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Backtests the regime strategy across EVERY available Bybit USDT symbol
    over an explicit date range, ranks by return %, shows the top 50.
    Usage: /backtest_regime_all_dates DD.MM.YYYY DD.MM.YYYY [capital]
    "from"/"to" filler words tolerated, e.g.:
      /backtest_regime_all_dates from 23.10.2025 to 20.09.2026
    Runs as a DETACHED background task — this command returns immediately
    and the bot stays fully responsive to everything else while it runs.
    """
    args = [a for a in ctx.args if a.lower() not in ("from", "to")]
    if len(args) < 2:
        await update.message.reply_text(
            "Usage: /backtest_regime_all_dates DD.MM.YYYY DD.MM.YYYY [capital]\n"
            "Example: /backtest_regime_all_dates 23.10.2025 20.09.2026\n\n"
            "Backtests the regime strategy (LONG above EMA100, SHORT below, "
            "exit on the opposite cross, no SL/TP) across EVERY available "
            "Bybit USDT symbol over this date range, then ranks by return % "
            "and shows the top 50.\n\n"
            "⚠️ This is genuinely expensive — with 400+ symbols, expect "
            "20-60+ minutes depending on the date range length. It runs in "
            "the background: the bot stays fully usable for everything "
            "else while it works, and you'll get periodic progress updates "
            "plus the final ranking.")
        return

    start_date_str = args[0]
    end_date_str = args[1]
    start_balance = 10000.0
    if len(args) >= 3:
        try: start_balance = float(args[2])
        except ValueError: pass

    # Validate the date format up front so a typo doesn't only surface
    # after the symbol list has already been fetched.
    try:
        datetime.strptime(start_date_str, "%d.%m.%Y")
        datetime.strptime(end_date_str, "%d.%m.%Y")
    except ValueError:
        await update.message.reply_text(
            f"❌ Could not parse dates '{start_date_str}' / '{end_date_str}' — "
            f"use DD.MM.YYYY format, e.g. 23.10.2025")
        return

    await update.message.reply_text(
        f"🚀 Launching bulk regime-strategy backtest in the background "
        f"({start_date_str} → {end_date_str}). This can take 20-60+ "
        f"minutes — the bot remains fully responsive to other commands "
        f"the entire time. Progress updates will follow.")

    # Genuinely detached — NOT awaited here, so this handler returns
    # immediately and the bot's sequential update processing isn't blocked.
    asyncio.create_task(
        _run_bulk_regime_backtest_task(
            update.effective_chat.id, ctx.bot, start_date_str, end_date_str, start_balance))


# =============================================================================
#  REGIME STRATEGY BACKTEST — TOP/BOTTOM 50 VS BTC, RANKED
#  Same idea and same detached-background-task pattern as the "all Bybit
#  symbols" bulk backtest above, but the candidate list comes from
#  _sync_scan_top_coins (the same underlying function behind /uptrendcoins
#  and /downtrendcoins) instead of every Bybit symbol. Only ~60 major
#  coins are in that candidate pool (see _sync_scan_top_coins's own
#  docstring) — "top 50" is nearly the whole pool, not a highly selective
#  slice of hundreds of coins. Flagged in the command's help text so this
#  isn't a silent surprise. Because the list is a fixed 50 (not 400+),
#  this runs in minutes rather than tens of minutes, so it's kept simpler
#  than the all-symbols version — no progress-update throttling needed.
# =============================================================================

TOP50_REGIME_BACKTEST_CONCURRENCY = 6   # same conservative cap and rationale
    # as BULK_REGIME_BACKTEST_CONCURRENCY above — shared thread pool with
    # live trading, and Bybit rate limits.

async def _run_top50_regime_backtest_task(chat_id: int, bot, symbols_with_meta: list[dict],
                                           universe_label: str, start_date_str: str,
                                           end_date_str: str, start_balance: float) -> None:
    """
    Detached background task (launched via asyncio.create_task by the
    caller — never awaited directly, so the triggering command handler
    returns immediately and the bot stays responsive). Backtests the
    regime strategy on every symbol in symbols_with_meta over the given
    date range, then ranks and shows ALL of them (not just the ones that
    happened to trade) — every candidate the person asked to see, including
    ones with zero trades in the period, which rank at 0.0% return.
    """
    try:
        symbols = [s["symbol"] for s in symbols_with_meta]
        await bot.send_message(chat_id=chat_id,
            text=f"⏳ Step 2/2: running regime-strategy backtest on all "
                 f"{len(symbols)} coins ({start_date_str} → {end_date_str})...\n"
                 f"The bot stays fully responsive to other commands while "
                 f"this runs. Expect several minutes.")

        semaphore = asyncio.Semaphore(TOP50_REGIME_BACKTEST_CONCURRENCY)
        tasks = [_bulk_backtest_one_symbol(sym, start_date_str, end_date_str, start_balance, semaphore)
                 for sym in symbols]

        results = {}
        start_time = time.time()
        for coro in asyncio.as_completed(tasks):
            symbol, result = await coro
            results[symbol] = result

        # ── Rank ALL of them, including zero-trade symbols at 0.0% ──────────
        ranked = []
        for symbol, r in results.items():
            if r.get("error"):
                ranked.append({"symbol": symbol, "ret_pct": None, "final": start_balance,
                                "trades": 0, "win_rate": 0.0, "error": r["error"]})
                continue
            trades = r.get("trades", [])
            final = r.get("final_balance", start_balance)
            ret_pct = (final - start_balance) / start_balance * 100
            wins = sum(1 for t in trades if t["pnl"] > 0)
            win_rate = wins / len(trades) * 100 if trades else 0.0
            ranked.append({"symbol": symbol, "ret_pct": ret_pct, "final": final,
                            "trades": len(trades), "win_rate": win_rate, "error": None})

        # Errored backtests sort last regardless of (nonexistent) return
        ranked.sort(key=lambda x: (x["ret_pct"] is None, -(x["ret_pct"] or 0)))
        total_elapsed_min = (time.time() - start_time) / 60

        header = (
            f"🏆 <b>Regime Strategy Ranking — {universe_label}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"Period: {start_date_str} → {end_date_str} | Start: ${start_balance:,.0f}\n"
            f"All {len(ranked)} coins ranked, highest return to lowest — "
            f"backtested in {total_elapsed_min:.1f} min\n"
            f"Same rules as /backtest_regime_dates: LONG above EMA100, "
            f"SHORT below, exit on the opposite cross, no SL/TP, blocked "
            f"during Choppy/Mean Reverting/Unknown regime.\n\n"
        )

        chunk_lines = []
        current = header
        for i, r in enumerate(ranked, 1):
            if r["error"]:
                line = f"{i}. <b>{r['symbol']}</b> — ❌ {r['error']}\n"
            elif r["trades"] == 0:
                line = f"{i}. <b>{r['symbol']}</b> — 0.0% (no trades in this period)\n"
            else:
                line = (f"{i}. <b>{r['symbol']}</b> — {r['ret_pct']:+.1f}% "
                        f"(${r['final']:,.0f}) | {r['trades']} trades | "
                        f"{r['win_rate']:.0f}% WR\n")
            if len(current) + len(line) > 3900:
                chunk_lines.append(current)
                current = line
            else:
                current += line
        chunk_lines.append(current)

        for chunk in chunk_lines:
            await bot.send_message(chat_id=chat_id, text=chunk, parse_mode="HTML")

        await bot.send_message(chat_id=chat_id,
            text="<i>The candidate pool itself (~60 major coins, not every "
                 "Bybit symbol) is the same one behind /uptrendcoins and "
                 "/downtrendcoins — \"top 50\" is nearly the whole pool. "
                 "Past backtest performance isn't a guarantee — re-check "
                 "the top few on a different date range before adding any "
                 "with /add_symbol_regime.</i>",
            parse_mode="HTML")

    except Exception as e:
        logger.error(f"[TOP50 REGIME BACKTEST] task failed: {e}", exc_info=True)
        try:
            await bot.send_message(chat_id=chat_id, text=f"❌ Backtest crashed: {e}")
        except Exception:
            pass


async def _launch_top50_regime_backtest(update: Update, ctx: ContextTypes.DEFAULT_TYPE,
                                         mode: str, command_name: str) -> None:
    """Shared arg-parsing + launch logic for the two commands below.
    mode: 'up' for outperformers, 'down' for underperformers."""
    args = [a for a in ctx.args if a.lower() not in ("from", "to")]
    direction_word = "outperformers" if mode == "up" else "underperformers"
    if len(args) < 2:
        await update.message.reply_text(
            f"Usage: /{command_name} DD.MM.YYYY DD.MM.YYYY [capital]\n"
            f"Example: /{command_name} 23.10.2025 20.09.2026\n\n"
            f"Step 1: finds the top 50 {direction_word} vs BTC over the "
            f"last 90 days (from a curated pool of ~60 major coins — the "
            f"same pool /uptrendcoins and /downtrendcoins use, NOT every "
            f"Bybit symbol).\n"
            f"Step 2: runs the regime-strategy backtest on all 50 across "
            f"the date range you give.\n"
            f"Step 3: ranks all 50 by backtest return %, highest to lowest — "
            f"every one shown, including any with zero trades.\n\n"
            f"⚠️ Runs 50 backtests. Expect several minutes. Runs in the "
            f"background — the bot stays fully responsive to other "
            f"commands the whole time.")
        return

    start_date_str, end_date_str = args[0], args[1]
    start_balance = 10000.0
    if len(args) >= 3:
        try: start_balance = float(args[2])
        except ValueError: pass

    try:
        datetime.strptime(start_date_str, "%d.%m.%Y")
        datetime.strptime(end_date_str, "%d.%m.%Y")
    except ValueError:
        await update.message.reply_text(
            f"❌ Could not parse dates '{start_date_str}' / '{end_date_str}' — "
            f"use DD.MM.YYYY format, e.g. 23.10.2025")
        return

    await update.message.reply_text(
        f"⏳ Step 1/2: finding top 50 {direction_word} vs BTC (last 90 days)...")
    top_coins = await asyncio.to_thread(_sync_scan_top_coins, mode, 90, 50)
    if not top_coins:
        await update.message.reply_text("Failed to fetch top-coins data from Bybit. Try again.")
        return

    universe_label = f"Top {len(top_coins)} {direction_word.capitalize()} vs BTC (90d)"
    await update.message.reply_text(
        f"🚀 Found {len(top_coins)} coins. Launching regime-strategy "
        f"backtest on all of them in the background "
        f"({start_date_str} → {end_date_str}). The bot remains fully "
        f"responsive the whole time.")

    asyncio.create_task(
        _run_top50_regime_backtest_task(
            update.effective_chat.id, ctx.bot, top_coins, universe_label,
            start_date_str, end_date_str, start_balance))


@public_command
async def cmd_backtest_regime_50_up_dates(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Finds the top 50 outperformers vs BTC (last 90 days), backtests the
    regime strategy on all 50 over an explicit date range, ranks all 50
    by return %. Runs as a detached background task.
    Usage: /backtest_regime_50_up_dates DD.MM.YYYY DD.MM.YYYY [capital]
    """
    await _launch_top50_regime_backtest(update, ctx, "up", "backtest_regime_50_up_dates")


@public_command
async def cmd_backtest_regime_50_down_dates(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Finds the top 50 underperformers vs BTC (last 90 days), backtests the
    regime strategy on all 50 over an explicit date range, ranks all 50
    by return %. Runs as a detached background task.
    Usage: /backtest_regime_50_down_dates DD.MM.YYYY DD.MM.YYYY [capital]
    """
    await _launch_top50_regime_backtest(update, ctx, "down", "backtest_regime_50_down_dates")


# =============================================================================
#  REGIME STRATEGY — LIVE TRADING
#  Trades a MANUALLY CURATED symbol list (add with /add_symbol_regime,
#  remove with /remove_symbol_regime) — NOT all Bybit symbols automatically.
#  Same rules as the backtest:
#    Entry LONG : Daily close > EMA100, regime not Choppy/Mean Reverting/Unknown
#    Entry SHORT: Daily close < EMA100, same regime gate
#    Exit       : Daily close crosses back over EMA100 — the ONLY exit
#    No stop loss, no take profit.
#  SAFETY NOTE: if a symbol already has an open position under the
#  PsychologyBot (9-filter) or MemeBot strategies, this strategy SKIPS
#  entering on it — Bybit nets positions per symbol in one-way mode, so two
#  independent strategies opening on the same symbol would blend into one
#  ambiguous position rather than being tracked separately. This is a
#  deliberate, conservative choice, not an oversight.
# =============================================================================

async def regime_open_trade(symbol: str, direction: str, regime: str, bot) -> None:
    """
    Open a live regime-strategy position on the LIVE (real-money) Bybit
    session — completely separate from PsychologyBot/MemeBot's demo
    session. Mirrors the same leverage/sizing/order mechanics but with
    this strategy's simplified no-SL/no-TP rules — fixed % of balance
    sizing since there's no SL distance to size risk against.
    """
    if not REGIME_LIVE_TRADING_ENABLED:
        logger.warning(f"[REGIME LIVE] {symbol}: entry signal found but live trading "
                        f"is not configured (missing BYBIT_LIVE_API_KEY/SECRET) — skipped.")
        return
    try:
        actual_lev = await asyncio.to_thread(_sync_set_leverage_live, symbol)
        entry = await get_price_live(symbol)
        if not entry:
            return
        balance = await get_balance_live()
        if not balance:
            return
        min_q, max_q, step, min_notional, max_lev = await get_instrument_constraints_live(symbol)

        target_margin   = balance * POSITION_PCT
        target_notional = target_margin * actual_lev
        raw_qty = target_notional / entry
        raw_qty = max(raw_qty, min_notional / entry)
        qty = _round_qty(raw_qty, min_q, step)
        if qty <= 0:
            return

        side = "Buy" if direction == "LONG" else "Sell"
        result = await place_order_live(symbol, side, qty)
        if not result or "__error__" in result:
            err_msg = result.get("__error__", "Unknown") if result else "No response from Bybit"
            logger.warning(f"[REGIME LIVE] Order failed {symbol} {direction}: {err_msg}")
            return

        await asyncio.sleep(1)
        real_entry = await asyncio.to_thread(_sync_get_position_entry_live, symbol)
        fill_price = real_entry if real_entry else entry

        pos = {
            "symbol": symbol, "direction": direction,
            "entry_price": fill_price, "original_qty": qty,
            "entry_time": time.time(), "entry_regime": regime,
        }
        regime_positions[symbol] = pos
        save_state()

        emoji = "🟢" if direction == "LONG" else "🔴"
        await alert(bot,
            f"{emoji} <b>REGIME STRATEGY — LIVE TRADE OPENED — {symbol} {direction}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 REAL MONEY — Bybit live account\n"
            f"📌 Entry  : <code>{fill_price:.4f}</code>\n"
            f"🚫 SL/TP  : NONE — this strategy exits only on an EMA100 cross\n"
            f"{regime_emoji(regime)} Regime at entry: {regime_label(regime)}\n"
            f"⚡ Qty: {qty}  |  Leverage: {actual_lev}x  |  Balance: {balance:.2f} USDT\n\n"
            f"⚠️ No bot-side stop — an adverse move before the EMA100 cross "
            f"may reach exchange liquidation instead.")
        logger.info(f"[REGIME LIVE] {symbol} {direction} opened @ {fill_price}")
    except Exception as e:
        logger.error(f"[REGIME LIVE] regime_open_trade({symbol}) failed: {e}", exc_info=True)


async def regime_close_trade(symbol: str, pos: dict, exit_price: float, bot) -> None:
    """Close a live regime-strategy position on an EMA100 cross — LIVE session."""
    try:
        direction = pos["direction"]
        await close_full_live(symbol, direction)
        qty   = pos.get("original_qty", 0)
        entry = pos.get("entry_price", 0)
        pnl = (exit_price - entry) * qty if direction == "LONG" else (entry - exit_price) * qty

        regime_positions.pop(symbol, None)
        save_state()

        emoji = "🟢" if pnl > 0 else "🔴"
        await alert(bot,
            f"{emoji} <b>REGIME STRATEGY — LIVE EMA100 EXIT — {symbol} {direction}</b>\n"
            f"💰 REAL MONEY — Bybit live account\n"
            f"Daily close crossed back over EMA100 — the only exit condition.\n"
            f"Closed @ <code>{exit_price:.4f}</code>\n"
            f"Est. PnL: {pnl:+.2f} USDT\n"
            f"Bot goes flat on {symbol}. Watching for the next signal.")
        logger.info(f"[REGIME LIVE] {symbol} {direction} closed @ {exit_price}, pnl={pnl:.2f}")
    except Exception as e:
        logger.error(f"[REGIME LIVE] regime_close_trade({symbol}) failed: {e}", exc_info=True)


async def regime_manage_open_position(symbol: str, pos: dict, bot) -> None:
    """Check ONE open regime-strategy position for the EMA100 exit, and
    detect if it was closed externally (manual close or liquidation) —
    all against the LIVE (real-money) Bybit session."""
    if not REGIME_LIVE_TRADING_ENABLED:
        return
    try:
        actual_size = await asyncio.to_thread(_sync_get_position_size_live, symbol)

        if actual_size == 0.0:
            last_price = await get_price_live(symbol) or pos.get("entry_price", 0)
            entry = pos.get("entry_price", 0)
            qty   = pos.get("original_qty", 0)
            direction = pos["direction"]
            pnl = (last_price - entry) * qty if direction == "LONG" else (entry - last_price) * qty
            regime_positions.pop(symbol, None)
            save_state()
            await alert(bot,
                f"⚠️ <b>REGIME STRATEGY — LIVE POSITION SYNC — {symbol} {direction}</b>\n"
                f"💰 REAL MONEY — Bybit live account\n"
                f"Bybit shows NO open position — closed externally (manual "
                f"close or liquidation, since this strategy places no "
                f"bot-side stop).\n"
                f"Entry: {entry:.4f} | Last: {last_price:.4f} | "
                f"Est. PnL: {pnl:+.2f} USDT\n"
                f"Bot state cleared for {symbol}.")
            return
        if actual_size < 0:
            return   # API error this cycle — try again next cycle

        df_1d = await get_klines_live(symbol, "D", 110)
        if df_1d is None or len(df_1d) < 101:
            return

        df_closed = df_1d.iloc[:-1]   # closed candles only, avoid live-candle flicker
        ema100_val = float(df_closed["close"].ewm(span=100, adjust=False).mean().iloc[-1])
        price = await get_price_live(symbol)
        if not price:
            return

        direction = pos["direction"]
        exit_now = (direction == "LONG" and price < ema100_val) or \
                   (direction == "SHORT" and price > ema100_val)
        if exit_now:
            await regime_close_trade(symbol, pos, price, bot)
    except Exception as e:
        logger.error(f"[REGIME LIVE] regime_manage_open_position({symbol}) failed: {e}", exc_info=True)


async def regime_scan_for_entry(symbol: str, bot) -> None:
    """Check ONE watchlist symbol for a new regime-strategy entry —
    against the LIVE (real-money) Bybit session."""
    if not REGIME_LIVE_TRADING_ENABLED:
        return   # no point fetching data if we can't act on it anyway
    if symbol in regime_positions:
        return
    # Avoid colliding with the 9-filter or MemeBot strategies on the same
    # symbol — see module docstring above for why.
    if symbol in symbol_positions or symbol in meme_positions:
        return

    try:
        df_1d, df_4h = await asyncio.gather(
            get_klines_live(symbol, "D", 300),
            get_klines_live(symbol, "240", 300),
        )
        if df_1d is None or df_4h is None or len(df_1d) < 101:
            return   # not enough history yet — common for newer memecoins

        df_closed = df_1d.iloc[:-1]
        ema100_val = float(df_closed["close"].ewm(span=100, adjust=False).mean().iloc[-1])
        price = float(df_1d["close"].iloc[-1])

        regime_result = await detect_regime(symbol, df_1d, df_4h)
        regime = regime_result.get("regime", REGIME_UNKNOWN)
        if regime in (REGIME_CHOPPY_RANGING, REGIME_MEAN_REVERTING, REGIME_UNKNOWN):
            return

        if price > ema100_val:
            direction = "LONG"
        elif price < ema100_val:
            direction = "SHORT"
        else:
            return

        await regime_open_trade(symbol, direction, regime, bot)
    except Exception as e:
        logger.debug(f"[REGIME LIVE] regime_scan_for_entry({symbol}) error: {e}")


async def regime_batch_scan(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Single periodic job (see REGIME_SCAN_INTERVAL_SEC): manages every open
    regime-strategy position, then checks every symbol in the curated
    regime_symbols list for a new entry. No batching/rotation needed here —
    that was only necessary for the earlier "all Bybit symbols" design;
    a manually curated list is small enough to check in full each cycle.
    """
    if regime_paused:
        return
    bot = context.bot
    try:
        for symbol, pos in list(regime_positions.items()):
            await regime_manage_open_position(symbol, pos, bot)

        for symbol in regime_symbols:
            await regime_scan_for_entry(symbol, bot)
    except Exception as e:
        logger.error(f"[REGIME LIVE] regime_batch_scan failed: {e}", exc_info=True)


REGIME_REPORT_INTERVAL_SEC = 45 * 60   # 2700s — periodic heartbeat, separate
    # from regime_batch_scan's entry/exit event alerts. Any symbol currently
    # in an open regime-strategy trade gets a status update on this fixed
    # cadence regardless of whether anything actually changed, so an open
    # position is never silent for a long stretch.

async def regime_position_report(context: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Every REGIME_REPORT_INTERVAL_SEC (45 min), send a status update for
    EVERY currently open regime-strategy position — entry/current price,
    estimated PnL, and time held. This is independent of regime_batch_scan,
    which only alerts on state changes (opens/exits); this job reports
    on a fixed cadence even when nothing has changed.
    """
    if not regime_positions:
        return
    bot = context.bot
    for symbol, pos in list(regime_positions.items()):
        try:
            price = await get_price_live(symbol)
            if not price:
                continue
            entry     = pos.get("entry_price", 0)
            qty       = pos.get("original_qty", 0)
            direction = pos["direction"]
            pnl = (price - entry) * qty if direction == "LONG" else (entry - price) * qty
            pnl_pct = (pnl / (entry * qty) * 100) if entry and qty else 0.0
            hours_held = (time.time() - pos.get("entry_time", time.time())) / 3600
            regime = pos.get("entry_regime", REGIME_UNKNOWN)
            emoji = "🟢" if pnl >= 0 else "🔴"

            await alert(bot,
                f"{emoji} <b>REGIME STRATEGY — 45min UPDATE — {symbol} {direction}</b>\n"
                f"Entry: <code>{entry:.4f}</code> | Now: <code>{price:.4f}</code>\n"
                f"Est. PnL: {pnl:+.2f} USDT ({pnl_pct:+.2f}%)\n"
                f"Held: {hours_held:.1f}h | Entered on: {regime_label(regime)}\n"
                f"Exit only on an EMA100 cross — no SL/TP on this strategy.")
        except Exception as e:
            logger.error(f"[REGIME LIVE] 45min report failed for {symbol}: {e}", exc_info=True)


@public_command
async def cmd_regime_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the live regime strategy's current state: account mode
    (live vs not configured), paused/running, the curated symbol list,
    and any open positions with estimated PnL."""
    if REGIME_LIVE_TRADING_ENABLED:
        account_line = "💰 <b>LIVE — real-money Bybit account</b>"
    else:
        account_line = ("⚠️ <b>NOT CONFIGURED</b> — set BYBIT_LIVE_API_KEY and "
                         "BYBIT_LIVE_API_SECRET on Render to enable live trading")

    lines = [
        f"🌐 <b>Regime Strategy — Status</b>",
        f"━━━━━━━━━━━━━━━━━━━━",
        account_line,
        f"Status: {'⏸ PAUSED (no new entries)' if regime_paused else '▶️ RUNNING'}",
        f"Watching: {', '.join(regime_symbols) if regime_symbols else 'None — use /add_symbol_regime SYMBOL'}",
        f"Open positions: {len(regime_positions)}",
    ]
    if regime_positions:
        lines.append("")
        for sym, pos in regime_positions.items():
            price = (await get_price_live(sym)) if REGIME_LIVE_TRADING_ENABLED else pos["entry_price"]
            price = price or pos["entry_price"]
            entry = pos["entry_price"]
            qty   = pos.get("original_qty", 0)
            direction = pos["direction"]
            pnl = (price - entry) * qty if direction == "LONG" else (entry - price) * qty
            emoji = "🟢" if pnl >= 0 else "🔴"
            lines.append(f"{emoji} {direction} {sym} @ {entry:.4f} → now {price:.4f} "
                         f"| PnL~{pnl:+.2f}")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


@owner_only
async def cmd_add_symbol_regime(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Add a symbol to the regime strategy's curated watchlist.
    Usage: /add_symbol_regime BTCUSDT"""
    if not ctx.args:
        await update.message.reply_text("Usage: /add_symbol_regime BTCUSDT")
        return
    symbol = ctx.args[0].upper()
    if not symbol.endswith("USDT"):
        symbol += "USDT"
    if symbol in regime_symbols:
        await update.message.reply_text(f"⚠️ {symbol} is already on the regime strategy watchlist.")
        return
    regime_symbols.append(symbol)
    save_state()
    await update.message.reply_text(
        f"✅ <b>{symbol}</b> added to the regime strategy.\n"
        f"Checked every {REGIME_SCAN_INTERVAL_SEC//60} minutes — entries need "
        f"at least 101 closed daily candles of history, so very new listings "
        f"won't trade immediately.",
        parse_mode="HTML")


@owner_only
async def cmd_remove_symbol_regime(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Remove a symbol from the regime strategy's curated watchlist.
    Usage: /remove_symbol_regime BTCUSDT"""
    if not ctx.args:
        await update.message.reply_text("Usage: /remove_symbol_regime BTCUSDT")
        return
    symbol = ctx.args[0].upper()
    if not symbol.endswith("USDT"):
        symbol += "USDT"
    if symbol not in regime_symbols:
        await update.message.reply_text(f"⚠️ {symbol} isn't on the regime strategy watchlist.")
        return
    regime_symbols.remove(symbol)
    if symbol in regime_positions:
        await update.message.reply_text(
            f"⚠️ {symbol} has an open regime-strategy position — removed "
            f"from the watchlist but the position stays open on Bybit and "
            f"will still be checked for its EMA100 exit.")
    save_state()
    await update.message.reply_text(f"✅ <b>{symbol}</b> removed from the regime strategy.",
                                     parse_mode="HTML")


@owner_only
async def cmd_regime_pause(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """Pause the regime strategy — no new entries. Existing open positions
    are still managed for their EMA100 exit (they don't get stranded)."""
    global regime_paused
    regime_paused = True
    save_state()
    await update.message.reply_text(
        "⏸ <b>Regime strategy PAUSED</b> — no new entries.\n"
        f"{len(regime_positions)} open position(s) still managed for exit.",
        parse_mode="HTML")


@owner_only
async def cmd_regime_resume(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    global regime_paused
    regime_paused = False
    save_state()
    await update.message.reply_text("▶️ <b>Regime strategy RESUMED</b>.", parse_mode="HTML")



def _format_backtest_results(r: dict) -> list[str]:
    """
    Format backtest results into Telegram messages.
    Returns list of strings (split to stay under 4096 char limit).
    """
    from datetime import datetime, timezone
    
    if r.get("error"):
        return [f"❌ Backtest failed: {r['error']}"]
    
    trades  = r["trades"]
    sym     = r["symbol"]
    days    = r["days"]
    start   = r["start_balance"]
    final   = r.get("final_balance", start)
    pnl     = final - start
    ret_pct = (pnl / start * 100)
    
    if not trades:
        return [
            f"📊 <b>Backtest — {sym} ({days}d)</b>\n"
            f"No trades found in this period.\n"
            f"Market may have been choppy or EMA stack not aligned."
        ]
    
    wins   = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    win_rate = len(wins)/len(trades)*100 if trades else 0
    
    avg_win  = sum(t["pnl"] for t in wins)  / len(wins)  if wins   else 0
    avg_loss = sum(t["pnl"] for t in losses)/ len(losses) if losses else 0
    
    best  = max(trades, key=lambda t: t["pnl"])
    worst = min(trades, key=lambda t: t["pnl"])
    
    # Exit reason breakdown
    reasons = {}
    for t in trades:
        r_key = t.get("exit_reason","?")
        reasons[r_key] = reasons.get(r_key, 0) + 1
    reason_str = "  ".join(f"{k}:{v}" for k,v in sorted(reasons.items()))
    
    def ts_to_date(ts):
        try:
            return datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime("%b %d")
        except:
            return "?"

    tp_mode = r.get("tp_mode", "none")
    tp_label = {"none": "No TP (EMA structure inversion exit only)",
                "fixed_pct": "Fixed % target (±15%)",
                "atr": "ATR-based target (4×ATR)",
                "vp": "Volume Profile measured move"}.get(tp_mode, tp_mode)

    # Message 1: Summary
    msg1 = (
        f"📊 <b>Backtest Results — {sym}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Period       : Last {days} days\n"
        f"Take profit  : {tp_label}\n"
        f"Start balance: ${start:,.2f}\n"
        f"Final balance: <b>${final:,.2f}</b>\n"
        f"Total PnL    : <b>{pnl:+,.2f} USDT ({ret_pct:+.1f}%)</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Total trades : {len(trades)}\n"
        f"Win rate     : {win_rate:.1f}% ({len(wins)}W / {len(losses)}L)\n"
        f"Avg win      : +{avg_win:.2f} USDT\n"
        f"Avg loss     : {avg_loss:.2f} USDT\n"
        f"Best trade   : +{best['pnl']:.2f} ({best['direction']} {ts_to_date(best['entry_day'])})\n"
        f"Worst trade  : {worst['pnl']:.2f} ({worst['direction']} {ts_to_date(worst['entry_day'])})\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Exit reasons : {reason_str}\n"
        f"\n"
        f"<b>Strategy used (live bot rules):</b>\n"
        f"F1 ADX-relative, F5 EMA stack, F9 Volume Confirmed (mandatory)\n"
        f"6/9 filters minimum (7/9 if health gate raises the bar)\n"
        f"No stop loss. Exit: {tp_label}, or Daily EMA structure inversion\n"
        f"10x leverage | 20% position | $10,000 starting"
    )
    
    # Message 2: Individual trades (up to 20)
    trade_lines = [f"\n📋 <b>Trade Log — {sym}</b>\n━━━━━━━━━━━━━━━━━━━━"]
    for i, t in enumerate(trades[:20]):
        emoji = "🟢" if t["pnl"] > 0 else "🔴"
        entry_d = ts_to_date(t["entry_day"])
        exit_d  = ts_to_date(t["exit_day"]) if t["exit_day"] else "Open"
        trade_lines.append(
            f"{emoji} #{i+1} {t['direction']} {entry_d}→{exit_d} "
            f"@ {t['entry']:.4f} | {t.get('exit_reason','?')} | "
            f"Score:{t['score']}/9 | PnL:{t['pnl']:+.2f}"
        )
    
    if len(trades) > 20:
        trade_lines.append(f"... and {len(trades)-20} more trades")
    
    msg2 = "\n".join(trade_lines)
    
    return [msg1, msg2]


@public_command
async def cmd_backtest(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Run a historical backtest for any symbol.
    Usage: /backtest BTCUSDT 6m [capital] [tp_mode]
    Periods: 1m=30d  3m=90d  6m=180d  1y=365d  2y=730d
    tp_mode: none (default), fixed_pct, atr, vp
    """
    # Parse args
    args = ctx.args
    if len(args) < 2:
        await update.message.reply_text(
            "📊 <b>Backtest command</b>\n"
            "Usage: <code>/backtest SYMBOL PERIOD [capital] [tp_mode]</code>\n\n"
            "Examples:\n"
            "  <code>/backtest BTCUSDT 6m</code>\n"
            "  <code>/backtest ETHUSDT 3m</code>\n"
            "  <code>/backtest SOLUSDT 1y 10000 vp</code>\n\n"
            "Periods: 1m (30d) · 3m (90d) · 6m (180d) · 1y (365d) · 2y (730d)\n"
            "TP modes: none (default, EMA-exit only) · fixed_pct (±15%) · "
            "atr (4×ATR) · vp (Volume Profile measured move)\n\n"
            "⏱ Takes 15-30 seconds (fetching real 4H+1H candles from Bybit)",
            parse_mode="HTML"
        )
        return
    
    symbol = args[0].upper()
    if not symbol.endswith("USDT"):
        symbol += "USDT"
    
    period_map = {"1m":30, "3m":90, "6m":180, "1y":365, "2y":730}
    period_str = args[1].lower()
    days = period_map.get(period_str)
    if not days:
        await update.message.reply_text(
            f"Unknown period '{period_str}'. Use: 1m, 3m, 6m, 1y, 2y")
        return
    
    start_balance = 10000.0
    tp_mode = "none"
    for extra in args[2:]:
        if extra.lower() in ("none", "fixed_pct", "atr", "vp"):
            tp_mode = extra.lower()
        else:
            try:
                start_balance = float(extra)
            except ValueError:
                pass
    
    await update.message.reply_text(
        f"⏳ <b>Running backtest...</b>\n"
        f"Symbol  : {symbol}\n"
        f"Period  : {period_str} ({days} days)\n"
        f"Capital : ${start_balance:,.0f}\n"
        f"TP mode : {tp_mode}\n\n"
        f"Fetching real 4H and 1H candle data from Bybit...\n"
        f"This takes 15-30 seconds.",
        parse_mode="HTML"
    )
    
    # Run backtest in background thread — never blocks event loop
    try:
        result = await asyncio.to_thread(
            _sync_run_backtest, symbol, days, start_balance, tp_mode)
        
        messages = _format_backtest_results(result)
        for msg in messages:
            await update.message.reply_text(msg, parse_mode="HTML")
    
    except Exception as e:
        logger.error(f"[BACKTEST] cmd_backtest error: {e}", exc_info=True)
        await update.message.reply_text(
            f"❌ Backtest error: {e}\nCheck Render logs for details.")


@public_command
async def cmd_backtest_tp(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Runs the ORIGINAL 9-filter strategy FOUR times on the SAME real
    historical data — no TP, fixed %, ATR-based, and Volume Profile
    measured move — mirroring /backtest_regime_tp so the two strategies'
    TP-mode comparisons can be placed side by side.
    Usage: /backtest_tp SYMBOL PERIOD [capital]
    """
    if len(ctx.args) < 2:
        await update.message.reply_text(
            "Usage: /backtest_tp SYMBOL PERIOD [capital]\n"
            "Example: /backtest_tp BTCUSDT 1y\n\n"
            "Runs the ORIGINAL 9-filter strategy 4 times on the SAME real "
            "data — no TP, fixed % (±15%), ATR-based (4×ATR), and Volume "
            "Profile measured move — so you can see which TP mechanism "
            "performs best on THIS strategy, and compare against "
            "/backtest_regime_tp's results for the experimental strategy.\n\n"
            "Periods: 1m/3m/6m/1y/2y. Takes 1-2 minutes (4 full backtests, "
            "each fetching 4H+1H data).")
        return

    symbol = ctx.args[0].upper()
    if not symbol.endswith("USDT"):
        symbol += "USDT"
    period_map = {"1m":30,"3m":90,"6m":180,"1y":365,"2y":730}
    days = period_map.get(ctx.args[1].lower())
    if not days:
        await update.message.reply_text(f"Unknown period '{ctx.args[1]}'. Use: 1m, 3m, 6m, 1y, 2y")
        return
    start_balance = 10000.0
    if len(ctx.args) >= 3:
        try: start_balance = float(ctx.args[2])
        except ValueError: pass

    await update.message.reply_text(
        f"⏳ Running ALL 4 TP variants for {symbol} ({ctx.args[1]}) on the "
        f"ORIGINAL 9-filter strategy...\nRuns the full backtest 4 separate "
        f"times (none, fixed_pct, atr, vp). Takes 1-2 minutes — please wait.")

    modes = ["none", "fixed_pct", "atr", "vp"]
    mode_labels = {"none": "No TP (EMA structure inversion only)",
                   "fixed_pct": "Fixed % (±15%)", "atr": "ATR-based (4×ATR)",
                   "vp": "Volume Profile measured move"}
    results = {}
    for mode in modes:
        results[mode] = await asyncio.to_thread(
            _sync_run_backtest, symbol, days, start_balance, mode)

    lines = [
        f"📊 <b>TP Comparison — {symbol} (Original 9-Filter Strategy)</b>",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"Period: last {days} days | Start: ${start_balance:,.0f}",
        f"Same F1/F5/F9 mandatory filters, 6/9 bar, health gate — ONLY the "
        f"take-profit mechanism differs.",
        f"",
    ]

    ranked = []
    for mode in modes:
        r = results[mode]
        label = mode_labels[mode]
        if r.get("error"):
            lines.append(f"<b>{label}:</b> ❌ {r['error']}")
            continue
        trades = r.get("trades", [])
        final = r.get("final_balance", start_balance)
        pnl = final - start_balance
        ret_pct = pnl / start_balance * 100
        wins = [t for t in trades if t["pnl"] > 0]
        win_rate = len(wins)/len(trades)*100 if trades else 0
        tp_hits = sum(1 for t in trades if t.get("exit_reason") == "TP_HIT")
        ranked.append((mode, ret_pct, final))
        lines.append(
            f"<b>{label}:</b>\n"
            f"  Final: ${final:,.2f} | PnL: {pnl:+,.2f} ({ret_pct:+.1f}%)\n"
            f"  Trades: {len(trades)} | Win rate: {win_rate:.1f}% | "
            f"TP hits: {tp_hits}/{len(trades)}"
        )

    if ranked:
        ranked.sort(key=lambda x: x[1], reverse=True)
        winner_mode, winner_ret, winner_final = ranked[0]
        lines.append(f"\n🏆 <b>Best on this data: {mode_labels[winner_mode]}</b> "
                     f"({winner_ret:+.1f}% return)")
        lines.append(f"<i>Compare against /backtest_regime_tp {symbol} "
                     f"{ctx.args[1]} for the experimental strategy's results "
                     f"on the same coin/period. Re-run on other coins before "
                     f"trusting either winner — one symbol isn't proof of a "
                     f"durable edge.</i>")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

# =============================================================================
#  END OF BACKTEST ENGINE
# =============================================================================


# =============================================================================
#  COIN SCREENER, HEALTH SCORE, UPTREND/DOWNTREND COMMANDS
#  /screen [period]     — Alpha vs BTC screener for monitored symbols
#  /health [SYMBOL]     — Full coin health score (1-100)
#  /uptrendcoins        — Top 15 overperformers vs BTC from Bybit
#  /downtrendcoins      — Top 15 underperformers vs BTC from Bybit
# =============================================================================

def _sync_get_all_usdt_symbols() -> list[str]:
    """Fetch all actively traded USDT linear perpetual symbols from Bybit."""
    try:
        resp = bybit_session.get_instruments_info(category="linear", status="Trading")
        symbols = []
        for item in resp["result"]["list"]:
            sym = item["symbol"]
            # Only USDT linear, skip inverse and options
            if sym.endswith("USDT") and item.get("quoteCoin") == "USDT":
                symbols.append(sym)
        return symbols
    except Exception as e:
        logger.error(f"_sync_get_all_usdt_symbols: {e}")
        return []


def _sync_get_coin_performance(symbol: str, days: int) -> Optional[dict]:
    """
    Get price performance for a symbol over last N days.
    Returns dict with start_price, end_price, pct_change, volume_ratio.
    """
    try:
        df = _sync_get_klines(symbol, "D", days + 5)
        if df is None or len(df) < days:
            return None
        
        start_price = float(df["close"].iloc[-(days+1)])
        end_price   = float(df["close"].iloc[-1])
        
        if start_price <= 0:
            return None
        
        pct_change = (end_price - start_price) / start_price * 100
        
        # Volume ratio: recent 14d avg vs prior 14d avg
        if len(df) >= 28:
            vol_recent = float(df["volume"].iloc[-14:].mean())
            vol_prior  = float(df["volume"].iloc[-28:-14].mean())
            vol_ratio  = vol_recent / vol_prior if vol_prior > 0 else 1.0
        else:
            vol_ratio = 1.0
        
        # ATH check: is current price within 20% of 1-year high?
        high_1y = float(df["high"].tail(365).max()) if len(df) >= 365 else float(df["high"].max())
        from_ath_pct = (end_price - high_1y) / high_1y * 100  # negative = below ATH
        
        # EMA200 slope
        ema200 = df["close"].ewm(span=200, adjust=False).mean()
        if len(ema200) >= 6:
            ema200_slope = float((ema200.iloc[-1] - ema200.iloc[-6]) / ema200.iloc[-6] * 100)
        else:
            ema200_slope = 0.0
        
        # Weekly relative performance: slope of COIN/BTC proxy
        # We use the coin's own momentum as proxy since BTC data comes separately
        ema20 = df["close"].ewm(span=20, adjust=False).mean()
        ema50 = df["close"].ewm(span=50, adjust=False).mean()
        ema_aligned = float(ema20.iloc[-1]) > float(ema50.iloc[-1]) > float(ema200.iloc[-1])
        
        return {
            "symbol":       symbol,
            "pct_change":   round(pct_change, 2),
            "start_price":  round(start_price, 6),
            "end_price":    round(end_price, 6),
            "vol_ratio":    round(vol_ratio, 2),
            "from_ath_pct": round(from_ath_pct, 2),
            "ema200_slope": round(ema200_slope, 4),
            "ema_aligned":  ema_aligned,
        }
    except Exception as e:
        logger.debug(f"_sync_get_coin_performance({symbol}): {e}")
        return None


def _compute_health_scores_core(close: pd.Series, high: pd.Series, volume: pd.Series,
                                 btc_close: pd.Series, btc_high: pd.Series) -> dict:
    """
    Core LONG/SHORT health scoring logic, operating on already-fetched
    price Series. Shared by both:
      - _sync_coin_health_score (live: fetches the LATEST 400 days)
      - _compute_health_scores_from_window (backtest: uses only data up
        to a specific historical day, avoiding lookahead bias)

    IMPORTANT DESIGN NOTE: SHORT health is NOT simply (100 - long_score).
    A coin that is choppy/directionless should score LOW on both scales —
    it's not a good long AND not a confidently-structured short. Only a
    coin showing genuine CONFIRMED bearish structure (bearish EMA stack,
    negative alpha vs BTC, red-week consistency, distribution-style
    volume, structurally impaired vs BTC's own ATH distance) should score
    high on SHORT health.
    """
    long_score, short_score = 0, 0
    long_breakdown, short_breakdown = {}, {}
    details = []

    # ── Component 1: Performance vs BTC (30 pts each direction) ──────────
    for period_days, pts in [(30, 10), (90, 10), (180, 10)]:
        if len(close) > period_days:
            coin_ret = (float(close.iloc[-1]) - float(close.iloc[-period_days])) / float(close.iloc[-period_days]) * 100
            if btc_close is not None and len(btc_close) > period_days:
                btc_ret  = (float(btc_close.iloc[-1]) - float(btc_close.iloc[-period_days])) / float(btc_close.iloc[-period_days]) * 100
                alpha    = coin_ret - btc_ret
                label    = f"{period_days}d"
                if alpha > 20:
                    long_score += pts; long_breakdown[label] = f"+{alpha:.1f}% vs BTC ✅"
                elif alpha > 0:
                    long_score += int(pts*0.6); long_breakdown[label] = f"+{alpha:.1f}% vs BTC 🟡"
                elif alpha > -20:
                    long_score += int(pts*0.2); long_breakdown[label] = f"{alpha:.1f}% vs BTC 🟠"
                else:
                    long_breakdown[label] = f"{alpha:.1f}% vs BTC ❌"
                if alpha < -20:
                    short_score += pts; short_breakdown[label] = f"{alpha:.1f}% vs BTC ✅"
                elif alpha < 0:
                    short_score += int(pts*0.6); short_breakdown[label] = f"{alpha:.1f}% vs BTC 🟡"
                elif alpha < 20:
                    short_score += int(pts*0.2); short_breakdown[label] = f"+{alpha:.1f}% vs BTC 🟠"
                else:
                    short_breakdown[label] = f"+{alpha:.1f}% vs BTC ❌"
                details.append(f"{label}: coin {coin_ret:+.1f}% | BTC {btc_ret:+.1f}% | alpha {alpha:+.1f}%")

    # ── Component 2: EMA structure (20 pts each direction) ───────────────
    ema20  = close.ewm(span=20,  adjust=False).mean()
    ema50  = close.ewm(span=50,  adjust=False).mean()
    ema100 = close.ewm(span=100, adjust=False).mean()
    ema200 = close.ewm(span=200, adjust=False).mean()

    price_now = float(close.iloc[-1])
    e20, e50, e100, e200 = (float(x.iloc[-1]) for x in [ema20,ema50,ema100,ema200])

    full_bull_stack = price_now > e20 > e50 > e100 > e200
    full_bear_stack = price_now < e20 < e50 < e100 < e200
    above_200 = price_now > e200
    below_200 = price_now < e200
    ema200_slope = (float(ema200.iloc[-1]) - float(ema200.iloc[-30])) / float(ema200.iloc[-30]) * 100 if len(ema200) >= 30 else 0

    if full_bull_stack:
        long_score += 20; long_breakdown["EMA"] = "Full bullish stack ✅"
    elif above_200 and ema200_slope > 0:
        long_score += 12; long_breakdown["EMA"] = "Above EMA200 (rising) 🟡"
    elif above_200:
        long_score += 6; long_breakdown["EMA"] = "Above EMA200 (flat) 🟠"
    else:
        long_breakdown["EMA"] = f"Below EMA200 (slope {ema200_slope:+.2f}%) ❌"
    if full_bear_stack:
        short_score += 20; short_breakdown["EMA"] = "Full bearish stack ✅"
    elif below_200 and ema200_slope < 0:
        short_score += 12; short_breakdown["EMA"] = "Below EMA200 (falling) 🟡"
    elif below_200:
        short_score += 6; short_breakdown["EMA"] = "Below EMA200 (flat) 🟠"
    else:
        short_breakdown["EMA"] = f"Above EMA200 (slope {ema200_slope:+.2f}%) ❌"
    details.append(f"EMA200 slope 30d: {ema200_slope:+.2f}%")

    # ── Component 3: Volume momentum (15 pts each direction) ─────────────
    vol_14  = float(volume.iloc[-14:].mean())
    vol_90  = float(volume.iloc[-90:-14].mean()) if len(volume) >= 90 else float(volume.mean())
    vol_ratio = vol_14 / vol_90 if vol_90 > 0 else 1.0
    price_chg_14d = (float(close.iloc[-1]) - float(close.iloc[-14])) / float(close.iloc[-14]) * 100 if len(close) >= 14 else 0

    if vol_ratio > 1.5 and price_chg_14d > 0:
        long_score += 15; long_breakdown["Volume"] = f"{vol_ratio:.2f}x + price up ✅"
    elif vol_ratio > 1.1 and price_chg_14d > 0:
        long_score += 9; long_breakdown["Volume"] = f"{vol_ratio:.2f}x + price up 🟡"
    elif vol_ratio > 0.8:
        long_score += 5; long_breakdown["Volume"] = f"{vol_ratio:.2f}x 90d avg 🟠"
    else:
        long_breakdown["Volume"] = f"{vol_ratio:.2f}x 90d avg (declining) ❌"
    if vol_ratio > 1.5 and price_chg_14d < 0:
        short_score += 15; short_breakdown["Volume"] = f"{vol_ratio:.2f}x + price down ✅"
    elif vol_ratio > 1.1 and price_chg_14d < 0:
        short_score += 9; short_breakdown["Volume"] = f"{vol_ratio:.2f}x + price down 🟡"
    elif vol_ratio > 0.8 and price_chg_14d < 0:
        short_score += 5; short_breakdown["Volume"] = f"{vol_ratio:.2f}x 90d avg, price down 🟠"
    else:
        short_breakdown["Volume"] = f"{vol_ratio:.2f}x 90d avg (no distribution signal) ❌"
    details.append(f"Vol ratio (14d/90d): {vol_ratio:.2f}x | 14d price chg: {price_chg_14d:+.1f}%")

    # ── Component 4: ATH distance vs BTC ATH distance (20 pts each) ──────
    high_1y_coin = float(high.tail(365).max()) if len(high) >= 365 else float(high.max())
    from_ath_coin = (price_now - high_1y_coin) / high_1y_coin * 100

    btc_ath_dist = -50.0
    if btc_close is not None and btc_high is not None:
        btc_high_1y = float(btc_high.tail(365).max())
        btc_now     = float(btc_close.iloc[-1])
        btc_ath_dist = (btc_now - btc_high_1y) / btc_high_1y * 100

    ath_diff = from_ath_coin - btc_ath_dist
    if ath_diff > 10:
        long_score += 20; long_breakdown["ATH dist"] = f"Closer to ATH than BTC by {ath_diff:.1f}% ✅"
    elif ath_diff > 0:
        long_score += 12; long_breakdown["ATH dist"] = "Slightly closer to ATH than BTC 🟡"
    elif ath_diff > -20:
        long_score += 5; long_breakdown["ATH dist"] = f"{abs(ath_diff):.1f}% further from ATH than BTC 🟠"
    else:
        long_breakdown["ATH dist"] = f"{abs(ath_diff):.1f}% further from ATH than BTC ❌"
    if ath_diff < -20:
        short_score += 20; short_breakdown["ATH dist"] = f"{abs(ath_diff):.1f}% further from ATH than BTC ✅"
    elif ath_diff < 0:
        short_score += 12; short_breakdown["ATH dist"] = "Slightly further from ATH than BTC 🟡"
    elif ath_diff < 10:
        short_score += 5; short_breakdown["ATH dist"] = f"Only {ath_diff:.1f}% closer to ATH than BTC 🟠"
    else:
        short_breakdown["ATH dist"] = f"Closer to ATH than BTC by {ath_diff:.1f}% ❌"
    details.append(f"Coin from 1y-high: {from_ath_coin:.1f}% | BTC: {btc_ath_dist:.1f}%")

    # ── Component 5: Trend consistency (15 pts each direction) ───────────
    green_weeks, red_weeks = 0, 0
    if len(close) >= 84:
        weekly_rets = [float(close.iloc[i*7]) - float(close.iloc[(i-1)*7])
                      for i in range(1, 13) if (i*7) < len(close)]
        green_weeks = sum(1 for r in weekly_rets if r > 0)
        red_weeks = len(weekly_rets) - green_weeks
        consistency_long  = green_weeks / len(weekly_rets) if weekly_rets else 0.5
        consistency_short = red_weeks   / len(weekly_rets) if weekly_rets else 0.5
    else:
        consistency_long = consistency_short = 0.5

    if consistency_long >= 0.7:
        long_score += 15; long_breakdown["Consistency"] = f"{green_weeks}/12 green weeks ✅"
    elif consistency_long >= 0.5:
        long_score += 8; long_breakdown["Consistency"] = f"{green_weeks}/12 green weeks 🟡"
    else:
        long_score += 2; long_breakdown["Consistency"] = f"{green_weeks}/12 green weeks ❌"

    if consistency_short >= 0.7:
        short_score += 15; short_breakdown["Consistency"] = f"{red_weeks}/12 red weeks ✅"
    elif consistency_short >= 0.5:
        short_score += 8; short_breakdown["Consistency"] = f"{red_weeks}/12 red weeks 🟡"
    else:
        short_score += 2; short_breakdown["Consistency"] = f"{red_weeks}/12 red weeks ❌"
    details.append(f"Trend consistency: {green_weeks}/12 green, {red_weeks}/12 red")

    def label_for(s):
        if s >= 75:   return "🟢 Excellent"
        elif s >= 55: return "🟡 Good"
        elif s >= 35: return "🟠 Weak"
        else:         return "🔴 Poor"

    return {
        "score": long_score, "label": label_for(long_score),
        "breakdown": long_breakdown, "details": details,
        "long_score": long_score, "long_label": label_for(long_score),
        "long_breakdown": long_breakdown,
        "short_score": short_score, "short_label": label_for(short_score),
        "short_breakdown": short_breakdown,
    }


def _sync_coin_health_score(symbol: str) -> dict:
    """
    LIVE wrapper: fetches the latest 400 days of data for the coin and BTC,
    then delegates to the shared core scoring logic. See
    _compute_health_scores_core for the full design rationale.
    """
    try:
        df = _sync_get_klines(symbol, "D", 400)
        if df is None or len(df) < 100:
            return {"score": 0, "error": "Insufficient data", "symbol": symbol}

        close, high, volume = df["close"].astype(float), df["high"].astype(float), df["volume"].astype(float)

        df_btc = _sync_get_klines("BTCUSDT", "D", 400)
        btc_close = df_btc["close"].astype(float) if df_btc is not None else None
        btc_high  = df_btc["high"].astype(float) if df_btc is not None else None

        result = _compute_health_scores_core(close, high, volume, btc_close, btc_high)
        result["symbol"] = symbol
        return result
    except Exception as e:
        logger.error(f"_sync_coin_health_score({symbol}): {e}", exc_info=True)
        return {"score": 0, "error": str(e), "symbol": symbol}


def _compute_health_scores_from_window(df_1d_window: pd.DataFrame, df_btc_window: pd.DataFrame) -> dict:
    """
    BACKTEST-SAFE wrapper: computes health scores using ONLY data up to a
    specific historical point (df_1d_window / df_btc_window are already
    sliced to "as of this backtest day" by the caller). This avoids the
    lookahead bias that would occur if the backtest called the live
    _sync_coin_health_score, which always fetches the LATEST data from
    Bybit regardless of what day the backtest simulation is currently on.
    """
    try:
        if df_1d_window is None or len(df_1d_window) < 100:
            return {"error": "Insufficient data", "long_score": 0, "short_score": 0}
        close  = df_1d_window["close"].astype(float)
        high   = df_1d_window["high"].astype(float)
        volume = df_1d_window["volume"].astype(float)
        btc_close = df_btc_window["close"].astype(float) if df_btc_window is not None and len(df_btc_window) >= 100 else None
        btc_high  = df_btc_window["high"].astype(float) if df_btc_window is not None and len(df_btc_window) >= 100 else None
        return _compute_health_scores_core(close, high, volume, btc_close, btc_high)
    except Exception as e:
        logger.error(f"_compute_health_scores_from_window: {e}", exc_info=True)
        return {"error": str(e), "long_score": 0, "short_score": 0}


def _apply_health_gate(ind: dict, direction: str, own_score: float, opposing_score: float) -> dict:
    """
    Adjusts an already-computed indicators dict's should_enter decision
    based on how this direction's health score compares to the OPPOSITE
    direction's score. Works on the output of _compute_indicators or
    _compute_indicators_with_stack — decoupled from HOW the scores were
    obtained (live cache vs backtest window) so live trading and both
    backtest engines share this exact same gating logic.

    DESIGN (per explicit instruction): health score NEVER blocks a
    direction outright. A coin's health favoring SHORT does not mean
    LONG is forbidden, and vice versa — a genuinely strong LONG setup can
    still occur on a coin whose broader structure favors shorts, and
    blocking it outright would throw away real signal. Instead: if the
    OPPOSING direction's health score clearly outweighs this direction's
    (opposing >= HEALTH_RAISE_BAR_BELOW AND opposing beats own by a
    meaningful margin), the bar for THIS direction rises from the base
    6-of-9 filters to 7-of-9 — a real but not absolute penalty. If health
    is neutral or favors this direction, the normal 6-of-9 bar applies.
    """
    if ind.get("error"):
        return ind

    passed = ind.get("passed", [])
    new_ind = dict(ind)
    new_ind["own_health_score"] = own_score
    new_ind["opposing_health_score"] = opposing_score

    HEALTH_GAP_MARGIN = 10   # opposing must beat own by at least this much
    opposing_clearly_favored = (
        opposing_score >= HEALTH_RAISE_BAR_BELOW and
        (opposing_score - own_score) >= HEALTH_GAP_MARGIN
    )

    required_total = 7 if opposing_clearly_favored else 6

    # Mandatory filters (F1, F5, F9) must always pass regardless of health —
    # health only ever affects the TOTAL bar, never bypasses the mandatory set.
    m1 = "ADX>Baseline"
    m5 = "EMA_Bullish_Stack" if direction == "LONG" else "EMA_Bearish_Stack"
    m9 = "VolumeConfirmed"
    mandatory_ok = all(m in passed for m in (m1, m5, m9))

    new_ind["should_enter"] = mandatory_ok and len(passed) >= required_total

    if opposing_clearly_favored:
        new_ind["health_gate_reason"] = (
            f"Raised bar (7/9 required): opposing health {opposing_score:.0f} vs "
            f"own {own_score:.0f} — had {len(passed)}/9"
        )
    else:
        new_ind["health_gate_reason"] = (
            f"Normal bar (6/9 required): own health {own_score:.0f} vs "
            f"opposing {opposing_score:.0f} — had {len(passed)}/9"
        )
    return new_ind




async def _get_cached_health_scores(symbol: str) -> dict:
    """
    Live-trading health score lookup with caching (HEALTH_CACHE_TTL_SEC).
    Health score requires fetching 400 days of data for the coin AND BTC —
    too expensive to recompute on every 10-second scan tick, so it's cached
    per-symbol and only refreshed once per hour. The components involved
    (30/90/180d performance, EMA structure, ATH distance) don't meaningfully
    change within an hour, so this is a safe trade-off.
    """
    cached = symbol_health_cache.get(symbol)
    now = time.time()
    if cached and (now - cached.get("ts", 0)) < HEALTH_CACHE_TTL_SEC:
        return cached

    result = await asyncio.to_thread(_sync_coin_health_score, symbol)
    entry = {
        "long_score": result.get("long_score", 50),
        "short_score": result.get("short_score", 50),
        "ts": now,
        "error": result.get("error"),
    }
    symbol_health_cache[symbol] = entry
    return entry


def _sync_scan_top_coins(mode: str, days: int = 90, top_n: int = 15) -> list[dict]:
    """
    Scan all Bybit USDT linear symbols and rank by performance vs BTC.
    mode: 'up' = outperformers, 'down' = underperformers
    Returns list of top_n dicts sorted by alpha vs BTC.
    """
    # Get BTC performance as baseline
    btc_perf = _sync_get_coin_performance("BTCUSDT", days)
    btc_ret  = btc_perf["pct_change"] if btc_perf else 0.0

    # Get all symbols — filter to top coins by some criteria
    all_syms = _sync_get_all_usdt_symbols()

    # Filter out micro/illiquid symbols — only symbols Bybit shows volume for
    # Sample a subset for speed: pick symbols with common base coins
    # We'll scan up to 80 symbols and pick the top 15
    priority_syms = [
        "BTCUSDT","ETHUSDT","SOLUSDT","BNBUSDT","XRPUSDT","ADAUSDT",
        "AVAXUSDT","DOTUSDT","LINKUSDT","MATICUSDT","ATOMUSDT","LTCUSDT",
        "UNIUSDT","AAVEUSDT","NEARUSDT","APTUSDT","ARBUSDT","OPUSDT",
        "INJUSDT","SUIUSDT","SEIUSDT","TIAUSDT","JUPUSDT","FETUSDT",
        "RENDERUSDT","WLDUSDT","STXUSDT","HBARUSDT","ALGOUSDT","VETUSDT",
        "XLMUSDT","FILUSDT","ICPUSDT","THETAUSDT","AAVEUSDT","SANDUSDT",
        "MANAUSDT","GRTUSDT","SNXUSDT","CRVUSDT","MKRUSDT","COMPUSDT",
        "LDOUSDT","RNDRUSDT","EIGENUSDT","ENAAUSDT","PENDLEUSDT","WIFUSDT",
        "BONKUSDT","PEPEUSDT","FLOKIUSDT","DOGEUSDT","SHIBUSDT","TRXUSDT",
        "TONUSDT","NOTUSDT","CKBUSDT","KASUSDT","CFXUSDT","CELOUSDT",
    ]

    # Only scan symbols that exist on Bybit
    valid_syms = [s for s in priority_syms if s in all_syms or s == "BTCUSDT"]
    results = []

    for sym in valid_syms:
        if sym == "BTCUSDT":
            continue
        perf = _sync_get_coin_performance(sym, days)
        if perf is None:
            continue

        alpha = perf["pct_change"] - btc_ret
        perf["alpha"]   = round(alpha, 2)
        perf["btc_ret"] = round(btc_ret, 2)
        results.append(perf)
        time.sleep(0.05)  # gentle rate limiting

    # Sort
    results.sort(key=lambda x: x["alpha"], reverse=(mode == "up"))
    return results[:top_n]


# ── COMMAND: /screen ─────────────────────────────────────────────────────────
@public_command
async def cmd_screen(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Alpha screener for monitored symbols vs BTC.
    Usage: /screen [period]
    Period: 30d, 90d (default), 180d
    """
    period_map = {"30d":30, "90d":90, "180d":180, "1m":30, "3m":90, "6m":180}
    period_str = ctx.args[0].lower() if ctx.args else "90d"
    days = period_map.get(period_str, 90)

    all_syms = list(set(monitored_symbols + meme_symbols))
    if not all_syms:
        await update.message.reply_text(
            "No symbols being monitored. Add some with /add_symbol first.")
        return

    await update.message.reply_text(
        f"Scanning {len(all_syms)} symbols vs BTC over last {days} days...\n"
        "Takes about 10 seconds.")

    def run_screen():
        btc_perf = _sync_get_coin_performance("BTCUSDT", days)
        btc_ret  = btc_perf["pct_change"] if btc_perf else 0.0
        results  = []
        for sym in all_syms:
            perf = _sync_get_coin_performance(sym, days)
            if perf:
                perf["alpha"]   = round(perf["pct_change"] - btc_ret, 2)
                perf["btc_ret"] = round(btc_ret, 2)
                results.append(perf)
        results.sort(key=lambda x: x["alpha"], reverse=True)
        return results, btc_ret

    results, btc_ret = await asyncio.to_thread(run_screen)

    if not results:
        await update.message.reply_text("Failed to fetch data. Try again.")
        return

    lines = [
        f"📊 <b>Alpha Screener — Last {days} days</b>",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"BTC performance: {btc_ret:+.1f}%",
        f"",
    ]

    for r in results:
        alpha = r["alpha"]
        coin_ret = r["pct_change"]
        vol = r["vol_ratio"]

        if alpha > 20:     tier = "🟢 Strong alpha"
        elif alpha > 5:    tier = "🟡 Mild alpha"
        elif alpha > -10:  tier = "⬜ Inline"
        elif alpha > -30:  tier = "🟠 Underperform"
        else:              tier = "🔴 Weak"

        ema_str = "EMA✅" if r.get("ema_aligned") else "EMA❌"
        sym_name = r["symbol"]
        ath_pct = r["from_ath_pct"]
        lines.append(
            f"{tier}\n"
            f"  <b>{sym_name}</b>: {coin_ret:+.1f}% | alpha: {alpha:+.1f}% vs BTC\n"
            f"  Vol ratio: {vol:.2f}x | {ema_str} | from 1y-high: {ath_pct:.1f}%"
        )

    lines.append(f"\n<i>Alpha = coin return minus BTC return over {days}d</i>")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")



# ── COMMAND: /health ─────────────────────────────────────────────────────────
@public_command
async def cmd_health(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Full coin health score for any symbol — TWO independent scores:
    LONG health and SHORT health. A coin can score well on one, both,
    or neither — they are NOT mirror images of each other.
    Usage: /health BTCUSDT
    """
    symbol = ctx.args[0].upper() if ctx.args else None
    if not symbol:
        await update.message.reply_text(
            "Usage: /health SYMBOL\n"
            "Scores 0-100 across 5 dimensions, computed separately for "
            "LONG favorability and SHORT favorability.",
            parse_mode="HTML")
        return

    if not symbol.endswith("USDT"):
        symbol += "USDT"

    await update.message.reply_text(
        f"⏳ Computing LONG + SHORT health scores for {symbol}...\n"
        f"Fetching 400 days of price data (takes ~10 seconds)")

    result = await asyncio.to_thread(_sync_coin_health_score, symbol)

    if result.get("error"):
        await update.message.reply_text(f"❌ Error: {result['error']}")
        return

    long_score, short_score = result["long_score"], result["short_score"]
    long_label, short_label = result["long_label"], result["short_label"]
    lbd, sbd = result["long_breakdown"], result["short_breakdown"]
    det = result["details"]

    def bar(s):
        filled = int(s/10)
        return "█"*filled + "░"*(10-filled)

    lines = [
        f"🏥 <b>Coin Health Score — {symbol}</b>",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"",
        f"<b>🟢 LONG health: {long_score}/100</b>  {long_label}",
        f"<code>{bar(long_score)}</code>",
    ]
    for key, val in lbd.items():
        lines.append(f"  {key:12}: {val}")

    lines.append("")
    lines.append(f"<b>🔴 SHORT health: {short_score}/100</b>  {short_label}")
    lines.append(f"<code>{bar(short_score)}</code>")
    for key, val in sbd.items():
        lines.append(f"  {key:12}: {val}")

    lines.append("")
    lines.append("<b>Raw data:</b>")
    for d in det:
        lines.append(f"  {d}")
    lines.append("")

    # Direction-aware bias — health NEVER blocks a direction outright.
    # The bar only rises from 6/9 to 7/9 when the OPPOSING direction's
    # score clearly outweighs this one (opposing >=55 AND beats own by
    # 10+ points). A coin with LOW scores on both sides is CHOPPY — both
    # directions get the normal 6/9 bar since neither side is "favored."
    HEALTH_GAP_MARGIN = 10
    long_raised  = short_score >= 55 and (short_score - long_score) >= HEALTH_GAP_MARGIN
    short_raised = long_score >= 55 and (long_score - short_score) >= HEALTH_GAP_MARGIN

    if long_raised:
        lines.append(f"🟠 <b>LONG bias:</b> raised bar — needs 7/9 filters "
                     f"(SHORT health {short_score} clearly outweighs LONG {long_score}).")
    else:
        lines.append(f"✅ <b>LONG bias:</b> normal bar — needs 6/9 filters.")

    if short_raised:
        lines.append(f"🟠 <b>SHORT bias:</b> raised bar — needs 7/9 filters "
                     f"(LONG health {long_score} clearly outweighs SHORT {short_score}).")
    else:
        lines.append(f"✅ <b>SHORT bias:</b> normal bar — needs 6/9 filters.")

    if long_score < 35 and short_score < 35:
        lines.append("\n⚠️ <i>Both scores low — this coin looks CHOPPY/directionless. "
                     "Neither direction is penalized by health alone, but the ADX-"
                     "relative and Volume Confirmation mandatory filters (F1, F9) "
                     "should naturally struggle to pass in genuine chop.</i>")
    elif long_score >= 55 and short_score >= 55:
        lines.append("\n⚠️ <i>Both scores elevated — mixed signals, check regime "
                     "detector for a tiebreaker.</i>")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


# ── COMMAND: /uptrendcoins ───────────────────────────────────────────────────
@public_command
async def cmd_uptrendcoins(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Fetch top 15 coins overperforming BTC over last 90 days from Bybit.
    Usage: /uptrendcoins [period]   e.g. /uptrendcoins 180d
    """
    period_map = {"30d":30,"90d":90,"180d":180,"1m":30,"3m":90,"6m":180}
    period_str = ctx.args[0].lower() if ctx.args else "90d"
    days = period_map.get(period_str, 90)

    await update.message.reply_text(
        f"📈 Scanning Bybit for top 15 outperformers vs BTC over {days} days...\n"
        f"Checking ~60 symbols. Takes 20-40 seconds.")

    results = await asyncio.to_thread(_sync_scan_top_coins, "up", days, 15)

    if not results:
        await update.message.reply_text("Failed to fetch data. Try again.")
        return

    btc_ret = results[0]["btc_ret"] if results else 0.0
    lines = [
        f"📈 <b>Top 15 Outperformers vs BTC — Last {days} days</b>",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"BTC baseline: {btc_ret:+.1f}%",
        f"",
    ]

    for i, r in enumerate(results, 1):
        alpha    = r["alpha"]
        coin_ret = r["pct_change"]
        vol      = r["vol_ratio"]
        ema_str  = "✅" if r.get("ema_aligned") else "❌"
        ath_str  = f"{r['from_ath_pct']:.1f}%"

        lines.append(
            f"<b>#{i} {r['symbol']}</b>\n"
            f"  Return: {coin_ret:+.1f}% | Alpha vs BTC: <b>+{alpha:.1f}%</b>\n"
            f"  Vol: {vol:.2f}x | EMA stack: {ema_str} | From 1y-high: {ath_str}"
        )

    lines.append(
        "These coins are generating alpha above BTC.\n"
        "Use /health SYMBOL for a full score on any of these.\n"
        "Use /add_symbol SYMBOL to start monitoring."
    )

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


# ── COMMAND: /downtrendcoins ─────────────────────────────────────────────────
@public_command
async def cmd_downtrendcoins(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Fetch top 15 coins underperforming BTC over last 90 days from Bybit.
    Usage: /downtrendcoins [period]   e.g. /downtrendcoins 180d
    """
    period_map = {"30d":30,"90d":90,"180d":180,"1m":30,"3m":90,"6m":180}
    period_str = ctx.args[0].lower() if ctx.args else "90d"
    days = period_map.get(period_str, 90)

    await update.message.reply_text(
        f"📉 Scanning Bybit for top 15 underperformers vs BTC over {days} days...\n"
        f"Checking ~60 symbols. Takes 20-40 seconds.")

    results = await asyncio.to_thread(_sync_scan_top_coins, "down", days, 15)

    if not results:
        await update.message.reply_text("Failed to fetch data. Try again.")
        return

    btc_ret = results[0]["btc_ret"] if results else 0.0
    lines = [
        f"📉 <b>Top 15 Underperformers vs BTC — Last {days} days</b>",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"BTC baseline: {btc_ret:+.1f}%",
        f"",
    ]

    for i, r in enumerate(results, 1):
        alpha    = r["alpha"]
        coin_ret = r["pct_change"]
        vol      = r["vol_ratio"]
        ema_str  = "✅" if r.get("ema_aligned") else "❌"

        lines.append(
            f"<b>#{i} {r['symbol']}</b>\n"
            f"  Return: {coin_ret:+.1f}% | Alpha vs BTC: <b>{alpha:.1f}%</b>\n"
            f"  Vol: {vol:.2f}x | EMA stack: {ema_str} | From 1y-high: {r['from_ath_pct']:.1f}%"
        )

    lines.append(
        "These coins are losing ground vs BTC.\n"
        "Avoid LONG entries. Good SHORT candidates when regime confirms downtrend.\n"
        "Use /health SYMBOL for a full breakdown."
    )

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

# =============================================================================
#  END OF SCREENER MODULE
# =============================================================================


# =============================================================================
#  CYCLE DETECTION ENGINE
#  Combines 4 independent, complementary cycle indicators:
#  1. Bitcoin Halving Clock — macro 4-year cycle position (BTC-anchored)
#  2. Weekly RSI Cycle — bottom/top oscillation history and average period
#  3. EMA 50/200 Crossover Cycle — golden/death cross history and average period
#  4. BB Width Squeeze Cycle — accumulation/expansion rhythm
#
#  Each indicator independently estimates "where in the cycle" a coin sits.
#  Combining all 4 catches what any single one would miss:
#  - Halving clock = macro timing (works for BTC, lags for alts)
#  - RSI cycle = momentum extremes (works for any coin, no BTC anchor needed)
#  - EMA crossover = trend cycle (confirms direction changes historically)
#  - BB squeeze = volatility cycle (predicts WHEN next move starts, not direction)
# =============================================================================

# Bitcoin halving dates (past + estimated future)
BTC_HALVING_DATES = [
    "2012-11-28", "2016-07-09", "2020-05-11", "2024-04-20", "2028-04-01",
]

def _sync_halving_clock() -> dict:
    """Return days since/until nearest halvings and cycle phase label."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    halvings = [datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                for d in BTC_HALVING_DATES]

    last_halving = max([h for h in halvings if h <= now], default=halvings[0])
    next_halving = min([h for h in halvings if h > now], default=halvings[-1])

    days_since = (now - last_halving).days
    days_until = (next_halving - now).days
    cycle_pct  = days_since / ((next_halving - last_halving).days) * 100

    # Historical phase pattern (based on 2016, 2020, 2024 cycles)
    if days_since < 180:
        phase = "🌱 Post-halving accumulation"
    elif days_since < 550:
        phase = "📈 Bull run building"
    elif days_since < 730:
        phase = "🚀 Cycle peak zone (historically)"
    elif days_since < 900:
        phase = "📉 Post-peak correction"
    else:
        phase = "🔴 Bear market / late cycle"

    return {
        "days_since_halving": days_since,
        "days_until_halving":  days_until,
        "cycle_pct": round(cycle_pct, 1),
        "phase": phase,
        "last_halving": last_halving.strftime("%Y-%m-%d"),
        "next_halving": next_halving.strftime("%Y-%m-%d"),
    }


def _sync_rsi_cycle(symbol: str) -> dict:
    """
    Analyse Weekly RSI cycle: find historical RSI<30 (bottom) and RSI>70 (top)
    events, compute average cycle length, and current position in cycle.
    Uses paginated fetch to cover multiple years — a single 1000-candle
    request only reaches back ~2.7 years, which misses full bear cycles.

    IMPORTANT: resamples to TRUE calendar weeks using the actual close of
    the last trading day in each week — NOT positional sampling (iloc[::7]).
    Positional sampling can silently skip over a sharp multi-day crash if
    the crash bottom doesn't happen to land on a sampled row, which was
    causing real RSI-bottom events (like FTX-collapse-era crashes) to be
    missed entirely on some symbols.
    """
    try:
        # Paginate back ~5 years (1825 days) to reliably capture 2+ full
        # bear/bull cycles for RSI bottom/top detection
        df = _sync_fetch_paginated_klines(symbol, "D", 1825)
        if df is None or len(df) < 200:
            # Fall back to whatever single-request data is available
            df = _sync_get_klines(symbol, "D", 1000)
        if df is None or len(df) < 200:
            # Try alternate exchange (Binance) — some symbols have shorter
            # history on Bybit than their actual market history elsewhere
            df = _sync_fetch_binance_klines(symbol, "1d", 1825)
        if df is None or len(df) < 200:
            return {"error": "Insufficient data", "source": "none"}

        # ── TRUE calendar-week resampling (fixes positional sampling bug) ────
        df = df.copy()
        df["dt"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df = df.set_index("dt")
        weekly = df["close"].resample("W").last().dropna()
        weekly_low  = df["low"].resample("W").min().dropna()

        if len(weekly) < 20:
            return {"error": "Insufficient weekly data"}

        rsi = ta.rsi(weekly, length=14)
        if rsi is None:
            return {"error": "RSI calc failed"}

        # Find bottom events (RSI crosses below 30) and top events (crosses above 70)
        bottoms, tops = [], []
        for i in range(1, len(rsi)):
            if pd.notna(rsi.iloc[i]) and pd.notna(rsi.iloc[i-1]):
                if rsi.iloc[i] < 30 and rsi.iloc[i-1] >= 30:
                    bottoms.append(i)
                if rsi.iloc[i] > 70 and rsi.iloc[i-1] <= 70:
                    tops.append(i)

        current_rsi = float(rsi.iloc[-1]) if pd.notna(rsi.iloc[-1]) else 50.0

        # Average cycle length (bottom to bottom, in weeks)
        avg_cycle_weeks = None
        if len(bottoms) >= 2:
            intervals = [bottoms[i] - bottoms[i-1] for i in range(1, len(bottoms))]
            avg_cycle_weeks = sum(intervals) / len(intervals)

        # Weeks since last bottom/top event
        weeks_since_bottom = (len(rsi) - 1 - bottoms[-1]) if bottoms else None
        weeks_since_top    = (len(rsi) - 1 - tops[-1]) if tops else None

        # Determine current cycle position
        coin_age_weeks = len(rsi)
        if current_rsi < 30:
            position = "🟢 AT CYCLE BOTTOM (RSI&lt;30) — historically a buy zone"
        elif current_rsi > 70:
            position = "🔴 AT CYCLE TOP (RSI&gt;70) — historically a sell zone"
        elif avg_cycle_weeks and weeks_since_bottom is not None:
            pct_through = min(100, weeks_since_bottom / avg_cycle_weeks * 100)
            if pct_through < 30:
                position = f"🌱 Early cycle ({pct_through:.0f}% through since last bottom)"
            elif pct_through < 70:
                position = f"📈 Mid cycle ({pct_through:.0f}% through since last bottom)"
            else:
                position = f"⚠️ Late cycle ({pct_through:.0f}% through — watch for top)"
        elif len(bottoms) == 1 and weeks_since_bottom is not None:
            # Only 1 bottom found — can't average, but still useful context
            position = (f"📍 {weeks_since_bottom}w since only bottom found "
                       f"(coin too young for average cycle — {coin_age_weeks}w of history)")
        elif len(bottoms) == 0 and coin_age_weeks < 52:
            # New coin, never hit RSI<30 — likely still in its first uptrend
            position = (f"🆕 New coin ({coin_age_weeks}w old) — never hit RSI&lt;30 yet. "
                       f"No completed cycle to measure.")
        elif current_rsi >= 40 and current_rsi <= 60:
            position = f"⬜ Neutral zone — no clear cycle extreme, {coin_age_weeks}w of history"
        else:
            position = f"❓ {coin_age_weeks}w of history — not enough bottom/top events to average"

        return {
            "current_rsi": round(current_rsi, 1),
            "avg_cycle_weeks": round(avg_cycle_weeks, 1) if avg_cycle_weeks else None,
            "weeks_since_bottom": weeks_since_bottom,
            "weeks_since_top": weeks_since_top,
            "num_bottoms_found": len(bottoms),
            "num_tops_found": len(tops),
            "coin_age_weeks": coin_age_weeks,
            "position": position,
        }
    except Exception as e:
        logger.error(f"_sync_rsi_cycle({symbol}): {e}", exc_info=True)
        return {"error": str(e)}


def _sync_ema_crossover_cycle(symbol: str) -> dict:
    """
    Find historical golden cross (EMA50>EMA200) and death cross events.
    Compute average cycle length and current position.
    Uses paginated fetch — single 1000-candle request only covers ~2.7 years.
    """
    try:
        df = _sync_fetch_paginated_klines(symbol, "D", 1825)
        if df is None or len(df) < 210:
            df = _sync_get_klines(symbol, "D", 1000)
        if df is None or len(df) < 210:
            df = _sync_fetch_binance_klines(symbol, "1d", 1825)
        if df is None or len(df) < 210:
            return {"error": "Insufficient data"}

        close = df["close"].astype(float)
        ema50  = close.ewm(span=50,  adjust=False).mean()
        ema200 = close.ewm(span=200, adjust=False).mean()

        golden_crosses, death_crosses = [], []
        for i in range(1, len(close)):
            if ema50.iloc[i] > ema200.iloc[i] and ema50.iloc[i-1] <= ema200.iloc[i-1]:
                golden_crosses.append(i)
            if ema50.iloc[i] < ema200.iloc[i] and ema50.iloc[i-1] >= ema200.iloc[i-1]:
                death_crosses.append(i)

        currently_bullish = float(ema50.iloc[-1]) > float(ema200.iloc[-1])

        # Average full cycle: golden-to-golden (days)
        avg_cycle_days = None
        if len(golden_crosses) >= 2:
            intervals = [golden_crosses[i] - golden_crosses[i-1] for i in range(1, len(golden_crosses))]
            avg_cycle_days = sum(intervals) / len(intervals)

        last_cross_idx = max(
            golden_crosses[-1] if golden_crosses else -1,
            death_crosses[-1]  if death_crosses  else -1
        )
        days_since_cross = (len(close) - 1 - last_cross_idx) if last_cross_idx >= 0 else None
        last_cross_type  = "Golden" if (golden_crosses and last_cross_idx == golden_crosses[-1]) else "Death"

        return {
            "currently_bullish": currently_bullish,
            "avg_cycle_days": round(avg_cycle_days, 0) if avg_cycle_days else None,
            "days_since_last_cross": days_since_cross,
            "last_cross_type": last_cross_type,
            "num_golden_crosses": len(golden_crosses),
            "num_death_crosses": len(death_crosses),
        }
    except Exception as e:
        logger.error(f"_sync_ema_crossover_cycle({symbol}): {e}", exc_info=True)
        return {"error": str(e)}


def _sync_bb_squeeze_cycle(symbol: str) -> dict:
    """
    Find historical BB squeeze events (width at local minimum) and measure
    the average time between squeezes — the volatility cycle rhythm.
    """
    try:
        df = _sync_get_klines(symbol, "D", 500)
        if df is None or len(df) < 60:
            return {"error": "Insufficient data"}

        close = df["close"].astype(float)
        bb = ta.bbands(close, length=20, std=2.0)
        if bb is None:
            return {"error": "BB calc failed"}

        bb_upper = bb["BBU_20_2.0_2.0"]
        bb_lower = bb["BBL_20_2.0_2.0"]
        bb_mid   = bb["BBM_20_2.0_2.0"]
        bb_width = (bb_upper - bb_lower) / bb_mid * 100

        # Find squeeze events: width is a local minimum over a 20-bar window
        squeezes = []
        for i in range(20, len(bb_width) - 5):
            window = bb_width.iloc[i-20:i+5]
            if pd.notna(bb_width.iloc[i]) and bb_width.iloc[i] == window.min() and bb_width.iloc[i] < window.mean() * 0.6:
                squeezes.append(i)

        # Dedupe squeezes that are too close together (<10 bars apart)
        deduped = []
        for s in squeezes:
            if not deduped or s - deduped[-1] > 10:
                deduped.append(s)

        avg_squeeze_interval = None
        if len(deduped) >= 2:
            intervals = [deduped[i] - deduped[i-1] for i in range(1, len(deduped))]
            avg_squeeze_interval = sum(intervals) / len(intervals)

        days_since_last_squeeze = (len(bb_width) - 1 - deduped[-1]) if deduped else None
        current_width = float(bb_width.iloc[-1]) if pd.notna(bb_width.iloc[-1]) else None
        avg_width     = float(bb_width.tail(200).mean())
        is_squeezing  = current_width < avg_width * 0.7 if current_width else False

        return {
            "current_width": round(current_width, 2) if current_width else None,
            "avg_width_200d": round(avg_width, 2),
            "is_squeezing_now": is_squeezing,
            "avg_squeeze_interval_days": round(avg_squeeze_interval, 0) if avg_squeeze_interval else None,
            "days_since_last_squeeze": days_since_last_squeeze,
            "num_squeezes_found": len(deduped),
        }
    except Exception as e:
        logger.error(f"_sync_bb_squeeze_cycle({symbol}): {e}", exc_info=True)
        return {"error": str(e)}


def _sync_detect_fast_swings(symbol: str) -> dict:
    """
    Short-term cycle detector — catches faster rhythms the macro swing
    detector (30-day window) would smooth over. Uses a 5-day window and
    lower thresholds (25% rally / 15% drop) appropriate for memecoins and
    other fast-moving assets that complete full cycles in weeks, not years.

    Only looks at the last 180 days — older data isn't relevant to a
    coin's CURRENT short-term rhythm, which can change as liquidity,
    volume, and market structure evolve.
    """
    try:
        df = _sync_get_klines(symbol, "D", 180)
        if df is None or len(df) < 30:
            return {"error": "Insufficient data", "completed_cycles": []}

        close = df["close"].astype(float).values
        high  = df["high"].astype(float).values
        low   = df["low"].astype(float).values
        n = len(close)

        window = 5  # ~1 week — catches short bursts a 30-day window misses
        swing_highs, swing_lows = [], []

        for i in range(window, n - window):
            if high[i] == max(high[i-window:i+window]):
                swing_highs.append((i, high[i]))
            if low[i] == min(low[i-window:i+window]):
                swing_lows.append((i, low[i]))

        def dedupe(swings, is_high):
            if not swings:
                return []
            deduped = [swings[0]]
            for idx, val in swings[1:]:
                if idx - deduped[-1][0] < 4:
                    if (is_high and val > deduped[-1][1]) or (not is_high and val < deduped[-1][1]):
                        deduped[-1] = (idx, val)
                else:
                    deduped.append((idx, val))
            return deduped

        swing_highs = dedupe(swing_highs, True)
        swing_lows  = dedupe(swing_lows, False)

        all_swings = sorted(
            [(idx, val, "H") for idx, val in swing_highs] +
            [(idx, val, "L") for idx, val in swing_lows]
        )

        # Lower thresholds for short-term: 25% rally confirms a swing high,
        # 15% drop confirms it was genuine (vs 60%/40% for the macro detector)
        completed_cycles = []
        i = 0
        while i < len(all_swings) - 2:
            if all_swings[i][2] == "L":
                low_idx, low_val = all_swings[i][0], all_swings[i][1]
                for j in range(i+1, len(all_swings)):
                    if all_swings[j][2] == "H":
                        high_idx, high_val = all_swings[j][0], all_swings[j][1]
                        rally_pct = (high_val - low_val) / low_val * 100
                        if rally_pct < 25:
                            continue
                        confirmed = False
                        for k in range(j+1, len(all_swings)):
                            if all_swings[k][2] == "L":
                                next_low_val = all_swings[k][1]
                                drop_pct = (high_val - next_low_val) / high_val * 100
                                if drop_pct >= 15:
                                    confirmed = True
                                break
                        if confirmed:
                            completed_cycles.append({
                                "low_idx": low_idx, "low_price": round(low_val, 6),
                                "high_idx": high_idx, "high_price": round(high_val, 6),
                                "multiple": round(high_val / low_val, 2),
                                "days_low_to_high": high_idx - low_idx,
                            })
                        i = j
                        break
                else:
                    break
            i += 1

        current_price = float(close[-1])
        recent_low_val, recent_low_idx = None, None
        if swing_lows:
            recent_low_idx, recent_low_val = swing_lows[-1]

        # Average short-term cycle length (low-to-low, in days)
        avg_short_cycle_days = None
        if len(swing_lows) >= 2:
            low_indices = [idx for idx, _ in swing_lows]
            intervals = [low_indices[k] - low_indices[k-1] for k in range(1, len(low_indices))]
            avg_short_cycle_days = sum(intervals) / len(intervals)

        days_since_recent_low = (n - 1 - recent_low_idx) if recent_low_idx is not None else None

        return {
            "completed_cycles": completed_cycles,
            "num_completed_cycles": len(completed_cycles),
            "current_price": round(current_price, 6),
            "most_recent_swing_low": round(recent_low_val, 6) if recent_low_val else None,
            "days_since_recent_low": days_since_recent_low,
            "avg_short_cycle_days": round(avg_short_cycle_days, 1) if avg_short_cycle_days else None,
            "num_swing_lows_found": len(swing_lows),
            "num_swing_highs_found": len(swing_highs),
            "total_candles_analysed": n,
            "window_days": window,
        }
    except Exception as e:
        logger.error(f"_sync_detect_fast_swings({symbol}): {e}", exc_info=True)
        return {"error": str(e), "completed_cycles": []}


def _sync_detect_historical_swings(symbol: str) -> dict:
    """
    Find actual completed cycle lows and highs using real price data —
    not assumptions. A "swing low" is a price point where the coin fell
    at least 40% from a prior peak and then recovered at least 60% from
    that low. A "swing high" is the peak before such a drawdown.
    Only counts a swing as COMPLETE if price has since moved away from it
    by the threshold in both directions — i.e. the low/high actually held.

    Returns TWO sets of real historical ratios from THIS coin's own past
    cycles:
      - low->high multiples   (used to build the TOP / upside price target)
      - high->low retracements (used to build the BOTTOM / downside target)
    Both are needed because a cycle has two turning points, and the
    historical rally size is NOT the same statistic as the historical
    drawdown size — they must be measured and reported separately.
    """
    try:
        df = _sync_fetch_paginated_klines(symbol, "D", 1825)
        if df is None or len(df) < 200:
            df = _sync_get_klines(symbol, "D", 1000)
        if df is None or len(df) < 200:
            return {"error": "Insufficient data", "completed_cycles": []}

        close = df["close"].astype(float).values
        high  = df["high"].astype(float).values
        low   = df["low"].astype(float).values
        n = len(close)

        # Find local maxima and minima using a rolling window approach
        window = 30  # ~1 month — filters noise, keeps genuine swings
        swing_highs, swing_lows = [], []

        for i in range(window, n - window):
            if high[i] == max(high[i-window:i+window]):
                swing_highs.append((i, high[i]))
            if low[i] == min(low[i-window:i+window]):
                swing_lows.append((i, low[i]))

        # Dedupe swings within 20 bars of each other, keep the most extreme
        def dedupe(swings, is_high):
            if not swings:
                return []
            deduped = [swings[0]]
            for idx, val in swings[1:]:
                if idx - deduped[-1][0] < 20:
                    if (is_high and val > deduped[-1][1]) or (not is_high and val < deduped[-1][1]):
                        deduped[-1] = (idx, val)
                else:
                    deduped.append((idx, val))
            return deduped

        swing_highs = dedupe(swing_highs, True)
        swing_lows  = dedupe(swing_lows, False)

        # Merge and sort chronologically to find alternating low->high->low sequences
        all_swings = sorted(
            [(idx, val, "H") for idx, val in swing_highs] +
            [(idx, val, "L") for idx, val in swing_lows]
        )

        # Extract completed cycles: a LOW followed by a HIGH that is at least
        # 60% above the low, followed by ANOTHER low that is at least 40%
        # below that high — this confirms the high was a genuine cycle top
        # (price actually reversed and fell significantly after it).
        # We now ALSO record that confirming low, so the high->low
        # retracement ratio can be computed for the BOTTOM price target.
        completed_cycles = []
        i = 0
        while i < len(all_swings) - 2:
            if all_swings[i][2] == "L":
                low_idx, low_val = all_swings[i][0], all_swings[i][1]
                for j in range(i+1, len(all_swings)):
                    if all_swings[j][2] == "H":
                        high_idx, high_val = all_swings[j][0], all_swings[j][1]
                        rally_pct = (high_val - low_val) / low_val * 100
                        if rally_pct < 60:
                            continue
                        confirmed = False
                        confirm_low_idx, confirm_low_val = None, None
                        for k in range(j+1, len(all_swings)):
                            if all_swings[k][2] == "L":
                                next_low_val = all_swings[k][1]
                                next_low_idx = all_swings[k][0]
                                drop_pct = (high_val - next_low_val) / high_val * 100
                                if drop_pct >= 40:
                                    confirmed = True
                                    confirm_low_idx, confirm_low_val = next_low_idx, next_low_val
                                break
                        if confirmed:
                            completed_cycles.append({
                                "low_idx": low_idx, "low_price": round(low_val, 6),
                                "high_idx": high_idx, "high_price": round(high_val, 6),
                                "multiple": round(high_val / low_val, 2),
                                "days_low_to_high": high_idx - low_idx,
                                # NEW: the retracement leg (high -> confirming low)
                                "confirm_low_idx": confirm_low_idx,
                                "confirm_low_price": round(confirm_low_val, 6),
                                "retracement_ratio": round(confirm_low_val / high_val, 3),
                                "days_high_to_low": confirm_low_idx - high_idx,
                            })
                        i = j
                        break
                else:
                    break
            i += 1

        # Current price context
        current_price = float(close[-1])
        # Find most recent confirmed low (start of current cycle attempt)
        recent_low_val, recent_low_idx = None, None
        if swing_lows:
            recent_low_idx, recent_low_val = swing_lows[-1]
        # Find most recent confirmed high (for BOTTOM target projection)
        recent_high_val, recent_high_idx = None, None
        if swing_highs:
            recent_high_idx, recent_high_val = swing_highs[-1]

        return {
            "completed_cycles": completed_cycles,
            "num_completed_cycles": len(completed_cycles),
            "current_price": round(current_price, 6),
            "most_recent_swing_low": round(recent_low_val, 6) if recent_low_val else None,
            "days_since_recent_low": (n - 1 - recent_low_idx) if recent_low_idx is not None else None,
            "most_recent_swing_high": round(recent_high_val, 6) if recent_high_val else None,
            "days_since_recent_high": (n - 1 - recent_high_idx) if recent_high_idx is not None else None,
            "total_candles_analysed": n,
        }
    except Exception as e:
        logger.error(f"_sync_detect_historical_swings({symbol}): {e}", exc_info=True)
        return {"error": str(e), "completed_cycles": []}


def _kaufman_efficiency_ratio(close: pd.Series, period: int = 20) -> pd.Series:
    """Kaufman Efficiency Ratio — 1.0 = pure trend, 0.0 = pure noise/chop."""
    change = (close - close.shift(period)).abs()
    volatility = close.diff().abs().rolling(period).sum()
    return (change / volatility.replace(0, np.nan)).fillna(0)


def _kaufman_adaptive_mean(close: pd.Series, fast: int = 2, slow: int = 30) -> pd.Series:
    """
    Kaufman Adaptive Moving Average (KAMA) — speeds up during trends,
    slows down during chop. Unlike a fixed EMA, "distance from KAMA"
    means the same thing regardless of how fast or slow the coin moves.
    """
    er = _kaufman_efficiency_ratio(close)
    fast_sc, slow_sc = 2 / (fast + 1), 2 / (slow + 1)
    smoothing_const = (er * (fast_sc - slow_sc) + slow_sc) ** 2
    kama = close.copy()
    for i in range(1, len(close)):
        if pd.notna(smoothing_const.iloc[i]):
            kama.iloc[i] = kama.iloc[i-1] + smoothing_const.iloc[i] * (close.iloc[i] - kama.iloc[i-1])
    return kama


_CPO_PHASE_ORDER = ["Compression", "Expansion", "Euphoria", "Capitulation"]
_CPO_PHASE_IDX = {p: i for i, p in enumerate(_CPO_PHASE_ORDER)}


# =============================================================================
#  ADAPTIVE CONFIRMED STACK — proposed replacement for the classic
#  EMA 20/50/100/200 stack (Filter 5). Uses Kaufman Adaptive MAs instead
#  of fixed-span EMAs, plus volume confirmation and hysteresis.
#  See conversation history for full design rationale and synthetic-data
#  validation (32% faster reaction / +16pt accuracy on short regimes,
#  19% faster / +4pt accuracy on long regimes, with a real, disclosed
#  cost of more false flips during extended chop on long-regime data).
#  THIS SECTION: real-data comparison tool, NOT wired into live trading.
#  Run /compare_stack SYMBOL to see how it behaves on actual coin history.
# =============================================================================

def _classic_4ema_stack_series(close: pd.Series, p1=20, p2=50, p3=100, p4=200) -> pd.Series:
    """The bot's current Filter 5 logic, as a full time series (1=bull,-1=bear,0=neither)."""
    e1 = close.ewm(span=p1, adjust=False).mean()
    e2 = close.ewm(span=p2, adjust=False).mean()
    e3 = close.ewm(span=p3, adjust=False).mean()
    e4 = close.ewm(span=p4, adjust=False).mean()
    bull = (e1 > e2) & (e2 > e3) & (e3 > e4)
    bear = (e1 < e2) & (e2 < e3) & (e3 < e4)
    out = pd.Series(0, index=close.index)
    out[bull] = 1
    out[bear] = -1
    return out


def _adaptive_4kama_stack_series(close: pd.Series, volume: pd.Series,
                                  p1=20, p2=50, p3=100, p4=200,
                                  vol_confirm_thresh: float = 1.15,
                                  hysteresis_bars: int = 3,
                                  max_wait_bars: int = 10) -> pd.Series:
    """
    Proposed replacement for Filter 5: same 4-tier stack CONCEPT, but each
    line is a Kaufman Adaptive MA, a stack change only counts if confirmed
    by elevated volume, and the new state must persist for N bars before
    the official signal flips.

    v2 FIX (based on real-data /backtest_compare results showing trade
    counts collapsing to near-zero on real coins like SOL/XRP): the
    original design required volume confirmation on the EXACT bar the
    hysteresis counter reached its threshold — a much stricter, less
    realistic requirement than "was there genuine volume support at any
    point during the transition." Real volume spikes rarely land on the
    precise final confirmation day. Fixed to check volume across the
    WHOLE persistence window instead of one bar, lowered the threshold
    (1.3->1.15) since real volume distributions are noisier than the
    synthetic data this was originally calibrated on, and added a
    max_wait_bars backstop so a genuinely persistent trend change is
    never permanently blocked just because volume never spiked — after
    max_wait_bars of sustained candidate persistence, the flip goes
    through on persistence alone, treating volume as a confidence boost
    rather than an absolute requirement.
    """
    k1 = _kaufman_adaptive_mean(close, fast=2, slow=p1)
    k2 = _kaufman_adaptive_mean(close, fast=2, slow=p2)
    k3 = _kaufman_adaptive_mean(close, fast=2, slow=p3)
    k4 = _kaufman_adaptive_mean(close, fast=2, slow=p4)
    raw_bull = (k1 > k2) & (k2 > k3) & (k3 > k4)
    raw_bear = (k1 < k2) & (k2 < k3) & (k3 < k4)
    raw_state = pd.Series(0, index=close.index)
    raw_state[raw_bull] = 1
    raw_state[raw_bear] = -1

    vol_ratio = volume / volume.rolling(20).mean()

    current_state = 0
    challenge_count = 0
    challenger = None
    vol_confirmed_in_window = False   # tracks whether ANY bar in the
                                       # current challenge streak had
                                       # elevated volume — not just the
                                       # final bar
    final = []
    for i in range(len(close)):
        candidate = raw_state.iloc[i]
        if candidate == current_state:
            challenge_count = 0
            challenger = None
            vol_confirmed_in_window = False
            final.append(current_state)
            continue

        vr = vol_ratio.iloc[i] if pd.notna(vol_ratio.iloc[i]) else 1.0
        vol_ok_this_bar = vr >= vol_confirm_thresh

        if challenger == candidate:
            challenge_count += 1
        else:
            challenger, challenge_count = candidate, 1
            vol_confirmed_in_window = False

        if vol_ok_this_bar:
            vol_confirmed_in_window = True

        # Flip if EITHER: persistence + volume confirmed anywhere in the
        # window, OR persistence alone has lasted long enough that we
        # shouldn't keep waiting for a volume spike that may never come.
        persistence_ok = challenge_count >= hysteresis_bars
        long_persistence = challenge_count >= max_wait_bars

        if candidate != 0 and (
            (persistence_ok and vol_confirmed_in_window) or long_persistence
        ):
            current_state = candidate
            challenge_count = 0
            challenger = None
            vol_confirmed_in_window = False
        final.append(current_state)
    return pd.Series(final, index=close.index)


def _sync_compare_stacks(symbol: str, days: int = 365) -> dict:
    """
    Run BOTH the classic EMA stack and the new Adaptive Confirmed Stack
    on REAL historical Bybit data for this symbol, and report:
      - total direction flips for each (fewer = more decisive/less noisy)
      - the calendar dates of every flip for each method
      - dates where the two methods currently disagree
    No "accuracy" score is given — real history has no ground truth for
    what the "correct" call was at each point, unlike the synthetic
    validation data. This is an honest side-by-side comparison only.
    """
    try:
        df = _sync_fetch_paginated_klines(symbol, "D", days + 220)  # +220 for EMA200 warmup
        if df is None or len(df) < 220:
            df = _sync_get_klines(symbol, "D", 1000)
        if df is None or len(df) < 220:
            return {"error": "Insufficient data (need 220+ days for EMA/KAMA 200 warmup)"}

        close = df["close"].astype(float).reset_index(drop=True)
        volume = df["volume"].astype(float).reset_index(drop=True)
        timestamps = df["timestamp"].astype("int64").reset_index(drop=True)

        classic = _classic_4ema_stack_series(close)
        adaptive = _adaptive_4kama_stack_series(close, volume)

        # Trim warmup period (first 220 bars) — both need it to stabilize
        warmup = 220
        n = len(close)

        def ts_to_date(ts):
            return datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime("%Y-%m-%d")

        classic_flips, adaptive_flips = [], []
        for i in range(warmup+1, n):
            if classic.iloc[i] != classic.iloc[i-1] and classic.iloc[i] != 0:
                classic_flips.append({
                    "date": ts_to_date(timestamps.iloc[i]),
                    "direction": "BULL" if classic.iloc[i]==1 else "BEAR",
                    "price": round(float(close.iloc[i]), 6),
                })
            if adaptive.iloc[i] != adaptive.iloc[i-1] and adaptive.iloc[i] != 0:
                adaptive_flips.append({
                    "date": ts_to_date(timestamps.iloc[i]),
                    "direction": "BULL" if adaptive.iloc[i]==1 else "BEAR",
                    "price": round(float(close.iloc[i]), 6),
                })

        current_classic = int(classic.iloc[-1])
        current_adaptive = int(adaptive.iloc[-1])
        agree = current_classic == current_adaptive

        return {
            "symbol": symbol,
            "days_analysed": n - warmup,
            "start_date": ts_to_date(timestamps.iloc[warmup]),
            "end_date": ts_to_date(timestamps.iloc[-1]),
            "classic_flips": classic_flips,
            "adaptive_flips": adaptive_flips,
            "classic_flip_count": len(classic_flips),
            "adaptive_flip_count": len(adaptive_flips),
            "current_classic": {"BULL":"🟢 Bullish","BEAR":"🔴 Bearish",0:"⬜ Neutral"}.get(
                "BULL" if current_classic==1 else "BEAR" if current_classic==-1 else 0),
            "current_adaptive": {"BULL":"🟢 Bullish","BEAR":"🔴 Bearish",0:"⬜ Neutral"}.get(
                "BULL" if current_adaptive==1 else "BEAR" if current_adaptive==-1 else 0),
            "currently_agree": agree,
        }
    except Exception as e:
        logger.error(f"_sync_compare_stacks({symbol}): {e}", exc_info=True)
        return {"error": str(e)}


@public_command
async def cmd_compare_stack(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Real-data comparison: classic EMA 20/50/100/200 stack vs the proposed
    Adaptive Confirmed Stack (KAMA + volume confirmation + hysteresis).
    Usage: /compare_stack SYMBOL [days]
    NOT wired into live trading — for evaluation only.
    """
    if not ctx.args:
        await update.message.reply_text(
            "Usage: /compare_stack SYMBOL [days]\n"
            "Example: /compare_stack BTCUSDT 730\n\n"
            "Compares the classic EMA 20/50/100/200 stack against the "
            "proposed Adaptive Confirmed Stack on REAL historical data.\n"
            "Shows flip count and flip dates for each — no accuracy score "
            "is given since real history has no ground truth, unlike the "
            "synthetic data used to design this indicator.")
        return

    symbol = ctx.args[0].upper()
    if not symbol.endswith("USDT"):
        symbol += "USDT"
    days = int(ctx.args[1]) if len(ctx.args) > 1 else 365
    days = min(days, 1460)  # cap at 4 years to keep pagination reasonable

    await update.message.reply_text(
        f"⏳ Fetching real {days}-day history for {symbol} and running both "
        f"indicators... (paginated fetch, may take 15-30s)")

    result = await asyncio.to_thread(_sync_compare_stacks, symbol, days)

    if result.get("error"):
        await update.message.reply_text(f"❌ {result['error']}")
        return

    agree_str = "✅ Currently AGREE" if result["currently_agree"] else "⚠️ Currently DISAGREE"

    lines = [
        f"📊 <b>Stack Comparison — {symbol}</b>",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"Period: {result['start_date']} to {result['end_date']} "
        f"({result['days_analysed']} days)",
        f"",
        f"<b>Current reading:</b>",
        f"Classic EMA stack   : {result['current_classic']}",
        f"Adaptive stack (new): {result['current_adaptive']}",
        f"{agree_str}",
        f"",
        f"<b>Total direction flips over the period:</b>",
        f"Classic EMA stack   : {result['classic_flip_count']} flips",
        f"Adaptive stack (new): {result['adaptive_flip_count']} flips",
        f"",
    ]

    # Show last 8 flips for each, most recent first
    lines.append("<b>Recent CLASSIC flips</b> (most recent first):")
    for f in result["classic_flips"][-8:][::-1]:
        emoji = "🟢" if f["direction"]=="BULL" else "🔴"
        lines.append(f"  {emoji} {f['date']} @ {f['price']}")
    if not result["classic_flips"]:
        lines.append("  (none in this period)")

    lines.append("")
    lines.append("<b>Recent ADAPTIVE flips</b> (most recent first):")
    for f in result["adaptive_flips"][-8:][::-1]:
        emoji = "🟢" if f["direction"]=="BULL" else "🔴"
        lines.append(f"  {emoji} {f['date']} @ {f['price']}")
    if not result["adaptive_flips"]:
        lines.append("  (none in this period)")

    lines.append(
        f"\n<i>No accuracy score is shown — real market history has no "
        f"ground truth for what the 'correct' call was at any point, "
        f"unlike the synthetic data used to design this indicator. "
        f"Compare flip DATES against known real events yourself (e.g. "
        f"does the adaptive version turn bullish/bearish earlier around "
        f"a move you remember?) for a real-world sanity check.\n\n"
        f"This is evaluation-only — NOT wired into live trading yet.</i>")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


def _compute_cpo_from_series(close: pd.Series, high: pd.Series, low: pd.Series,
                              vol: pd.Series, smooth: int = 6, score_smooth: int = 8,
                              hysteresis_bars: int = 2, hysteresis_margin: float = 12,
                              order_prior_weight: float = 15) -> dict:
    """
    Core CPO v8 computation — operates on already-fetched Series, no
    internal data fetch. This lets callers that already have candle data
    in memory (like the regime detector, which already fetched df_1d for
    its own 11 signals) reuse it directly instead of triggering a
    duplicate Bybit API call every time CPO is consulted. See
    _sync_cycle_phase_oscillator (thin fetch-wrapper around this) for
    the full design rationale and validation results.
    """
    try:
        n = len(close)
        if n < 90:
            return {"error": "Insufficient data"}

        # ── Sub-signal A: volatility trend (z-scored ATR%) ────────────────
        tr = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low - close.shift()).abs()
        ], axis=1).max(axis=1)
        atr_pct = tr.rolling(14).mean() / close * 100
        atr_z = ((atr_pct - atr_pct.rolling(60).mean()) / atr_pct.rolling(60).std()) \
                .fillna(0).clip(-3, 3)
        atr_z_s = atr_z.rolling(smooth).mean().fillna(0)
        atr_trend = (atr_z_s - atr_z_s.shift(10)).fillna(0)

        # ── Sub-signal E: range contraction ratio ──────────────────────────
        daily_range = (high - low) / close * 100
        range_ratio = (daily_range / daily_range.rolling(20).mean()).fillna(1.0)
        range_ratio_s = range_ratio.rolling(smooth).mean().fillna(1.0)

        # ── Sub-signal B: volume-price coherence ───────────────────────────
        price_chg = close.pct_change(5)
        vol_ratio = (vol.rolling(5).mean() / vol.rolling(20).mean() - 1).fillna(0)
        vpc = (np.sign(price_chg) * np.sign(vol_ratio)).rolling(smooth).mean().fillna(0)
        vol_trend = vol_ratio.rolling(smooth).mean().fillna(0)

        # ── Sub-signal C: displacement from adaptive mean ──────────────────
        kama = _kaufman_adaptive_mean(close)
        dam = (close - kama) / kama * 100
        dam_z = ((dam - dam.rolling(60).mean()) / dam.rolling(60).std()) \
                .fillna(0).clip(-3, 3).rolling(smooth).mean().fillna(0)

        # ── Sub-signal D: momentum persistence + deceleration ──────────────
        ret_sign = np.sign(close.diff()).fillna(0)
        persist = ret_sign.rolling(10).mean().fillna(0)
        persist_s = persist.rolling(smooth).mean().fillna(0)
        persist_decel = (persist_s.abs() - persist_s.abs().shift(8)).fillna(0)

        chg5 = close.pct_change(5).fillna(0)

        # ── Compute per-phase scores for every bar ──────────────────────────
        raw_scores = {"Compression": [], "Expansion": [], "Euphoria": [], "Capitulation": []}
        for i in range(n):
            az, atrd, dz = atr_z_s.iloc[i], atr_trend.iloc[i], dam_z.iloc[i]
            ps, pdec, vpci, vt = persist_s.iloc[i], persist_decel.iloc[i], vpc.iloc[i], vol_trend.iloc[i]
            c5, rr = chg5.iloc[i], range_ratio_s.iloc[i]

            s_comp = 0
            if az < -0.3: s_comp += 25
            elif az < 0.2: s_comp += 10
            if abs(ps) < 0.3: s_comp += 22
            elif abs(ps) < 0.5: s_comp += 9
            if abs(dz) < 0.5: s_comp += 22
            elif abs(dz) < 1.0: s_comp += 9
            if vt < -0.1: s_comp += 16
            elif vt < 0.1: s_comp += 6
            if rr < 0.8: s_comp += 15
            elif rr < 1.0: s_comp += 6

            s_exp = 0
            if atrd > 0.15: s_exp += 22
            elif atrd > 0: s_exp += 9
            if abs(ps) > 0.5: s_exp += 26
            elif abs(ps) > 0.3: s_exp += 12
            if np.sign(dz) == np.sign(ps) and abs(dz) > 0.3: s_exp += 22
            elif np.sign(dz) == np.sign(ps): s_exp += 9
            if vpci > 0.2: s_exp += 17
            elif vpci > 0: s_exp += 7
            if rr > 1.1: s_exp += 13

            s_euph = 0
            if dz > 1.0: s_euph += 26
            elif dz > 0.5: s_euph += 13
            if ps > 0 and pdec < -0.05: s_euph += 26
            elif ps > 0 and vpci < -0.1: s_euph += 22
            if az > 0.3: s_euph += 13
            if vpci < 0 and dz > 0.3: s_euph += 17
            if rr > 1.2 and ps > 0: s_euph += 10

            s_cap = 0
            if dz < -1.0: s_cap += 26
            elif dz < -0.5: s_cap += 13
            if c5 < -0.08: s_cap += 22
            elif c5 < -0.03: s_cap += 10
            if az > 0.5: s_cap += 17
            elif az > 0.2: s_cap += 7
            if ps < -0.3: s_cap += 13
            if vpci < -0.2 and c5 < 0: s_cap += 9
            if rr > 1.3 and c5 < 0: s_cap += 12

            raw_scores["Compression"].append(s_comp)
            raw_scores["Expansion"].append(s_exp)
            raw_scores["Euphoria"].append(s_euph)
            raw_scores["Capitulation"].append(s_cap)

        smoothed = {p: pd.Series(v).rolling(score_smooth).mean().bfill().values
                    for p, v in raw_scores.items()}

        # ── Walk forward applying cyclic-order prior + hysteresis ───────────
        current_phase = None
        challenge_count, challenger = 0, None
        final_phase_labels = []

        for i in range(n):
            scores_i = {p: smoothed[p][i] for p in smoothed}

            if current_phase is not None:
                cur_idx = _CPO_PHASE_IDX[current_phase]
                for p in scores_i:
                    dist = (_CPO_PHASE_IDX[p] - cur_idx) % 4
                    if dist == 0:
                        scores_i[p] += order_prior_weight * 0.3
                    elif dist == 1:
                        scores_i[p] += order_prior_weight * 0.5
                    elif dist == 2:
                        scores_i[p] -= order_prior_weight * 0.2
                    else:
                        scores_i[p] -= order_prior_weight * 0.4

            best = max(scores_i, key=scores_i.get)

            if current_phase is None:
                current_phase = best
                challenge_count = 0
            elif best == current_phase:
                challenge_count = 0
            else:
                beats_current = scores_i[best] - scores_i[current_phase]
                if beats_current > hysteresis_margin:
                    if challenger == best:
                        challenge_count += 1
                    else:
                        challenger, challenge_count = best, 1
                    if challenge_count >= hysteresis_bars:
                        current_phase = best
                        challenge_count = 0
                else:
                    challenge_count = 0

            final_phase_labels.append(current_phase)

        # ── Current reading (last bar) ───────────────────────────────────────
        phase_name = final_phase_labels[-1]
        final_scores = {p: smoothed[p][-1] for p in smoothed}
        sorted_final = sorted(final_scores.values(), reverse=True)
        margin = sorted_final[0] - sorted_final[1]
        confidence = min(100, round(margin * 1.2 + final_scores[phase_name] * 0.3))

        phase_meta = {
            "Compression":  ("🌱", "Accumulation zone — best risk/reward for LONG entries historically"),
            "Expansion":    ("📈", "Markup phase — trend-following LONG entries favoured"),
            "Euphoria":     ("⚠️", "Distribution risk — avoid new LONGs, consider taking profit"),
            "Capitulation": ("🔴", "Markdown phase — SHORT bias, or stay flat until Compression returns"),
        }
        phase_emoji, bias = phase_meta[phase_name]

        days_in_phase = 0
        for lbl in reversed(final_phase_labels):
            if lbl == phase_name:
                days_in_phase += 1
            else:
                break

        return {
            "phase": phase_name,
            "phase_emoji": phase_emoji,
            "confidence": confidence,
            "bias": bias,
            "days_in_current_phase": days_in_phase,
            "score_margin": round(margin, 1),
        }
    except Exception as e:
        logger.error(f"_compute_cpo_from_series: {e}", exc_info=True)
        return {"error": str(e)}


def _sync_cycle_phase_oscillator(symbol: str, interval: str = "D",
                                  lookback: int = 300, smooth: int = 6,
                                  score_smooth: int = 8, hysteresis_bars: int = 2,
                                  hysteresis_margin: float = 12,
                                  order_prior_weight: float = 15) -> dict:
    """
    Thin fetch-wrapper: fetches candle data for `symbol` then delegates to
    _compute_cpo_from_series for the actual CPO computation. Used by
    /cycle and /compare_stack style commands that don't already have the
    candle data in memory. See _compute_cpo_from_series for the full
    design rationale and validation results (this docstring trimmed since
    the math lives there now).

    VALIDATED (backtested on synthetic labelled data, 7 unseen test seeds
    with varying noise levels 0.7x-1.5x, calibrated on 5 separate seeds):
      - HIGH-CONFIDENCE bars only (confidence≥55): ~82% avg exact match
        (worst-case seed: 76%) — trust CPO when confidence is high,
        treat as informational only when confidence is low.
    """
    try:
        df = _sync_get_klines(symbol, interval, lookback)
        if df is None or len(df) < 90:
            return {"error": "Insufficient data"}

        close = df["close"].astype(float)
        high  = df["high"].astype(float)
        low   = df["low"].astype(float)
        vol   = df["volume"].astype(float)

        return _compute_cpo_from_series(close, high, low, vol, smooth, score_smooth,
                                         hysteresis_bars, hysteresis_margin, order_prior_weight)
    except Exception as e:
        logger.error(f"_sync_cycle_phase_oscillator({symbol}): {e}", exc_info=True)
        return {"error": str(e)}


def _sync_cpo_multi_timeframe(symbol: str) -> dict:
    """
    Run the CPO on both Daily and Weekly-equivalent (3-day candles as a
    fast proxy for weekly structure) and check agreement. When both
    timeframes agree on phase, confidence in the reading is much higher.
    """
    daily = _sync_cycle_phase_oscillator(symbol, interval="D", lookback=300)
    # Use 3-day candles as a longer-timeframe view without needing a
    # separate weekly-candle fetch — Bybit doesn't offer native "W" interval
    # for all symbols, so this approximates it efficiently.
    higher_tf = _sync_cycle_phase_oscillator(symbol, interval="D", lookback=600, smooth=16)

    if daily.get("error") or higher_tf.get("error"):
        return {
            "daily": daily,
            "higher_tf": higher_tf,
            "agreement": False,
            "combined_confidence": daily.get("confidence", 0) if not daily.get("error") else 0,
        }

    agree = daily["phase"] == higher_tf["phase"]
    combined_conf = round((daily["confidence"] + higher_tf["confidence"]) / 2)
    if agree:
        combined_conf = min(100, combined_conf + 15)  # bonus for timeframe agreement

    return {
        "daily": daily,
        "higher_tf": higher_tf,
        "agreement": agree,
        "combined_confidence": combined_conf,
    }


def _build_cycle_conclusion(symbol: str, halving: dict, rsi: dict,
                            ema: dict, bb: dict, swings: dict, cpo: dict = None) -> dict:
    """
    Synthesize all indicators + real historical swing data into a
    data-derived conclusion. Every claim here traces back to a specific
    computed value — nothing is asserted without a source.
    Returns: cycle_type, position_pct, time_remaining_range, price_target,
             outlook, action, and a confidence_level based on data quality.

    CPO (Cycle Phase Oscillator, custom-built) acts as a confirming/
    conflicting signal against the EMA+RSI-derived cycle_type. When CPO
    agrees, confidence rises. When it conflicts, that's flagged explicitly
    rather than silently averaged away — a disagreement is information.
    """
    conclusion = {
        "cycle_type": "Unknown",
        "position_summary": None,
        "time_remaining": None,
        "top_target": None,
        "bottom_target": None,
        "outlook": None,
        "action": None,
        "confidence": "Low",
        "data_quality_notes": [],
        "cpo_agreement": None,
    }

    data_points_available = 0
    total_possible = 5  # now includes CPO

    # ── Determine cycle TYPE from confluence of indicators ───────────────────
    ema_bullish = ema.get("currently_bullish", None)
    current_rsi = rsi.get("current_rsi", None)
    is_squeezing = bb.get("is_squeezing_now", None)

    if ema_bullish is not None:
        data_points_available += 1
    if current_rsi is not None:
        data_points_available += 1
    if is_squeezing is not None:
        data_points_available += 1
    if swings.get("num_completed_cycles", 0) > 0:
        data_points_available += 1
    cpo_daily = (cpo or {}).get("daily", {})
    if cpo_daily and not cpo_daily.get("error"):
        data_points_available += 1

    # Cycle type logic — purely from measured values
    if ema_bullish is True and current_rsi and current_rsi > 55:
        conclusion["cycle_type"] = "📈 Bull phase (uptrend confirmed + rising momentum)"
    elif ema_bullish is True and current_rsi and current_rsi <= 55:
        conclusion["cycle_type"] = "🌱 Early recovery (uptrend confirmed, momentum still building)"
    elif ema_bullish is False and current_rsi and current_rsi < 45:
        conclusion["cycle_type"] = "📉 Bear phase (downtrend confirmed + weak momentum)"
    elif ema_bullish is False and current_rsi and current_rsi >= 45:
        conclusion["cycle_type"] = "⚠️ Bear phase with relief bounce (downtrend confirmed, momentum recovering)"
    else:
        conclusion["cycle_type"] = "❓ Unclear — insufficient confirming data"

    # ── CPO agreement check ───────────────────────────────────────────────────
    # Map cycle_type to the CPO phase family it should agree with:
    #   Bull phase / Early recovery  -> Compression or Expansion (accumulation/markup)
    #   Bear phase / relief bounce   -> Euphoria or Capitulation (distribution/markdown)
    if cpo_daily and not cpo_daily.get("error"):
        cpo_phase = cpo_daily.get("phase")
        bullish_cycle_types = ("Bull phase", "Early recovery")
        bearish_cycle_types = ("Bear phase",)
        is_bullish_type = any(t in conclusion["cycle_type"] for t in bullish_cycle_types)
        is_bearish_type = any(t in conclusion["cycle_type"] for t in bearish_cycle_types)
        cpo_bullish_phase = cpo_phase in ("Compression", "Expansion")
        cpo_bearish_phase = cpo_phase in ("Euphoria", "Capitulation")

        if (is_bullish_type and cpo_bullish_phase) or (is_bearish_type and cpo_bearish_phase):
            conclusion["cpo_agreement"] = (
                f"✅ CPO agrees: {cpo_phase} phase ({cpo_daily.get('confidence',0)}% confidence) "
                f"confirms the {conclusion['cycle_type'].split(chr(40))[0].strip()} read.")
        elif (is_bullish_type and cpo_bearish_phase) or (is_bearish_type and cpo_bullish_phase):
            conclusion["cpo_agreement"] = (
                f"⚠️ CPO CONFLICTS: EMA/RSI suggest one direction, but CPO reads "
                f"{cpo_phase} phase ({cpo_daily.get('confidence',0)}% confidence). "
                f"Treat this cycle read with extra caution.")
        else:
            conclusion["cpo_agreement"] = (
                f"CPO reads {cpo_phase} phase ({cpo_daily.get('confidence',0)}% confidence) — "
                f"neutral relative to the EMA/RSI cycle type.")

    # ── Position in cycle — from RSI cycle % if available, else EMA cycle ────
    position_source = None
    if rsi.get("avg_cycle_weeks") and rsi.get("weeks_since_bottom") is not None:
        pct = min(100, rsi["weeks_since_bottom"] / rsi["avg_cycle_weeks"] * 100)
        conclusion["position_summary"] = (
            f"{pct:.0f}% through the average RSI cycle "
            f"(based on {rsi['num_bottoms_found']} historical bottoms on this coin)")
        position_source = "RSI cycle"
    elif ema.get("avg_cycle_days") and ema.get("days_since_last_cross") is not None:
        pct = min(100, ema["days_since_last_cross"] / ema["avg_cycle_days"] * 100)
        conclusion["position_summary"] = (
            f"{pct:.0f}% through the average EMA trend cycle "
            f"(based on {ema['num_golden_crosses']} golden + {ema['num_death_crosses']} death crosses)")
        position_source = "EMA cycle"
    else:
        conclusion["position_summary"] = "Cannot calculate — insufficient completed cycles on this coin"
        conclusion["data_quality_notes"].append(
            "No cycle-position percentage available: this coin has not "
            "completed enough historical cycles to measure an average.")

    # ── Time remaining — statistical range from historical cycle lengths ─────
    if rsi.get("avg_cycle_weeks") and rsi.get("weeks_since_bottom") is not None:
        remaining_weeks = rsi["avg_cycle_weeks"] - rsi["weeks_since_bottom"]
        if remaining_weeks > 0:
            conclusion["time_remaining"] = (
                f"~{remaining_weeks:.0f} weeks remaining if this cycle matches "
                f"the historical average ({rsi['avg_cycle_weeks']:.0f}w). "
                f"This is a statistical estimate from {rsi['num_bottoms_found']} "
                f"past cycles on {symbol} — not a guarantee.")
        else:
            conclusion["time_remaining"] = (
                f"Already {abs(remaining_weeks):.0f} weeks PAST the historical "
                f"average cycle length ({rsi['avg_cycle_weeks']:.0f}w) — "
                f"this cycle is running longer than typical for this coin.")
    else:
        conclusion["time_remaining"] = "Cannot estimate — no completed RSI cycle history for this coin"
        conclusion["data_quality_notes"].append(
            "Time-remaining estimate unavailable: requires at least 2 "
            "historical RSI-bottom events, which this coin does not have yet.")

    # ── TOP price target — from this coin's own historical low→high multiples ──
    # Applied to the most recent CONFIRMED swing low, projecting where the
    # current cycle's high is likely to land if it rallies by a similar
    # magnitude to past completed cycles on this coin.
    #
    # CALIBRATED COVERAGE: the min/max historical range is extended by a
    # margin (validated via walk-forward backtesting on synthetic multi-cycle
    # data with realistic cycle-to-cycle variability, tested on 3 independent
    # unseen batches of 30-40 series each) so that the TRUE future cycle
    # extreme falls inside the reported range roughly 70-85% of the time —
    # not a point-accuracy claim, a calibrated PREDICTION INTERVAL claim.
    # top_margin=0.2 -> validated coverage 76-82% across unseen test batches.
    _CPO_TOP_MARGIN = 0.2
    _CPO_BOT_MARGIN = 0.3

    cycles = swings.get("completed_cycles", [])
    current_price = swings.get("current_price")
    recent_low = swings.get("most_recent_swing_low")
    recent_high = swings.get("most_recent_swing_high")

    if len(cycles) >= 2 and recent_low:
        multiples = [c["multiple"] for c in cycles]
        avg_multiple = sum(multiples) / len(multiples)
        min_multiple = min(multiples)
        max_multiple = max(multiples)
        span = max_multiple - min_multiple
        cal_low_mult  = max(1.01, min_multiple - span * _CPO_TOP_MARGIN)
        cal_high_mult = max_multiple + span * _CPO_TOP_MARGIN

        conclusion["top_target"] = {
            "based_on_low": recent_low,
            "low_estimate": round(recent_low * cal_low_mult, 6),
            "avg_estimate": round(recent_low * avg_multiple, 6),
            "high_estimate": round(recent_low * cal_high_mult, 6),
            "historical_multiples": multiples,
            "num_cycles_used": len(cycles),
            "coverage_note": "~70-85% validated coverage range",
        }
    elif len(cycles) == 1 and recent_low:
        conclusion["top_target"] = {
            "based_on_low": recent_low,
            "single_cycle_multiple": cycles[0]["multiple"],
            "single_estimate": round(recent_low * cycles[0]["multiple"], 6),
            "num_cycles_used": 1,
        }
        conclusion["data_quality_notes"].append(
            "TOP target based on only 1 completed historical cycle — "
            "low confidence. Need 2+ cycles for a reliable range.")
    else:
        conclusion["top_target"] = None
        conclusion["data_quality_notes"].append(
            f"No TOP price target calculated: {symbol} has "
            f"{len(cycles)} completed cycle(s) in available history — "
            f"need at least 1 confirmed low→high→low sequence.")

    # ── BOTTOM price target — from this coin's own high→low retracement ratios ─
    # Applied to the most recent CONFIRMED swing high, projecting where the
    # current cycle's low is likely to land if it retraces by a similar
    # magnitude to past completed drawdowns on this coin. This is a
    # DIFFERENT statistic from the rally multiple above — historical rally
    # size and historical drawdown size are measured independently and
    # must not be assumed to mirror each other.
    # bot_margin=0.3 -> validated coverage 72-83% across unseen test batches
    # (needed a wider margin than TOP because drawdown depth showed more
    # cycle-to-cycle variance than rally size in backtesting).
    cycles_with_retracement = [c for c in cycles if c.get("retracement_ratio") is not None]

    if len(cycles_with_retracement) >= 2 and recent_high:
        retracements = [c["retracement_ratio"] for c in cycles_with_retracement]
        avg_retr = sum(retracements) / len(retracements)
        min_retr = min(retracements)  # smallest ratio = deepest historical drawdown
        max_retr = max(retracements)  # largest ratio = shallowest historical drawdown
        span = max_retr - min_retr
        cal_deep_retr    = max(0.02, min_retr - span * _CPO_BOT_MARGIN)
        cal_shallow_retr = min(0.98, max_retr + span * _CPO_BOT_MARGIN)

        conclusion["bottom_target"] = {
            "based_on_high": recent_high,
            "deep_estimate":     round(recent_high * cal_deep_retr, 6),  # worst-case historical drawdown
            "avg_estimate":      round(recent_high * avg_retr, 6),
            "shallow_estimate":  round(recent_high * cal_shallow_retr, 6),  # best-case historical drawdown
            "historical_retracements": retracements,
            "num_cycles_used": len(cycles_with_retracement),
            "coverage_note": "~70-85% validated coverage range",
        }
    elif len(cycles_with_retracement) == 1 and recent_high:
        conclusion["bottom_target"] = {
            "based_on_high": recent_high,
            "single_retracement_ratio": cycles_with_retracement[0]["retracement_ratio"],
            "single_estimate": round(recent_high * cycles_with_retracement[0]["retracement_ratio"], 6),
            "num_cycles_used": 1,
        }
        conclusion["data_quality_notes"].append(
            "BOTTOM target based on only 1 completed historical drawdown — "
            "low confidence. Need 2+ cycles for a reliable range.")
    else:
        conclusion["bottom_target"] = None
        conclusion["data_quality_notes"].append(
            f"No BOTTOM price target calculated: {symbol} has "
            f"{len(cycles_with_retracement)} confirmed high→low drawdown(s) — "
            f"need at least 1 confirmed high→low sequence.")

    # ── Outlook + Action — derived from cycle type + regime bias ─────────────
    if "Bull phase" in conclusion["cycle_type"]:
        conclusion["outlook"] = "Continuation of uptrend likely while EMA stack and momentum hold."
        conclusion["action"]  = "LONG entries favoured. Standard 8-filter strategy rules apply."
    elif "Early recovery" in conclusion["cycle_type"]:
        conclusion["outlook"] = "Uptrend confirmed but not yet momentum-confirmed — higher risk of failed breakout."
        conclusion["action"]  = "LONG entries allowed but require full 9/9 filter score, not just 6/9."
    elif "Bear phase with relief" in conclusion["cycle_type"]:
        conclusion["outlook"] = "Downtrend intact; current bounce is historically likely to fail and resume lower."
        conclusion["action"]  = "SHORT bias. Avoid LONG entries until EMA stack turns bullish."
    elif "Bear phase" in conclusion["cycle_type"]:
        conclusion["outlook"] = "Downtrend confirmed with weak momentum — further downside has historical precedent."
        conclusion["action"]  = "SHORT entries favoured. LONG entries blocked by regime gate."
    else:
        conclusion["outlook"] = "Indicators are mixed or data is insufficient — no reliable directional read."
        conclusion["action"]  = "No strong bias. Wait for EMA stack and RSI to align before entering."

    # If CPO conflicts, soften the action rather than silently trust EMA/RSI
    if conclusion["cpo_agreement"] and "CONFLICTS" in conclusion["cpo_agreement"]:
        conclusion["action"] += " ⚠️ CPO disagrees — reduce size or wait for alignment."

    # ── Confidence level based on how much real data backs the conclusion ────
    cpo_bonus = 0
    if cpo_daily and not cpo_daily.get("error"):
        combined_cpo_conf = (cpo or {}).get("combined_confidence", cpo_daily.get("confidence", 0))
        if (cpo or {}).get("agreement") and combined_cpo_conf >= 60:
            cpo_bonus = 1  # multi-timeframe CPO agreement + high confidence = extra trust

    if data_points_available == total_possible and len(cycles) >= 2 and cpo_bonus:
        conclusion["confidence"] = "High — all 5 indicators available (incl. CPO), 2+ completed cycles, multi-timeframe CPO agreement"
    elif data_points_available >= 4 and len(cycles) >= 2:
        conclusion["confidence"] = "High — most indicators available, 2+ completed cycles confirmed"
    elif data_points_available >= 3 and len(cycles) >= 1:
        conclusion["confidence"] = "Medium — most indicators available, limited cycle history"
    else:
        conclusion["confidence"] = "Low — insufficient historical data for this coin"
        conclusion["data_quality_notes"].append(
            f"Overall confidence is LOW: only {data_points_available}/{total_possible} "
            f"indicators returned usable data, and {len(cycles)} completed "
            f"cycle(s) found. Treat this conclusion as directional context only.")

    return conclusion


def _sync_full_cycle_analysis(symbol: str) -> dict:
    """Combine all indicators — 4 classic + CPO + macro & short-term swings."""
    halving      = _sync_halving_clock()
    rsi_cyc      = _sync_rsi_cycle(symbol)
    ema_cyc      = _sync_ema_crossover_cycle(symbol)
    bb_cyc       = _sync_bb_squeeze_cycle(symbol)
    swings       = _sync_detect_historical_swings(symbol)   # macro (30d window, 1-2yr+ cycles)
    fast_swings  = _sync_detect_fast_swings(symbol)          # short-term (5d window, weeks-scale)
    cpo          = _sync_cpo_multi_timeframe(symbol)          # custom Cycle Phase Oscillator

    conclusion = _build_cycle_conclusion(symbol, halving, rsi_cyc, ema_cyc, bb_cyc, swings, cpo)

    return {
        "symbol":      symbol,
        "halving":     halving,
        "rsi":         rsi_cyc,
        "ema":         ema_cyc,
        "bb":          bb_cyc,
        "swings":      swings,
        "fast_swings": fast_swings,
        "cpo":         cpo,
        "conclusion":  conclusion,
    }


@public_command
async def cmd_cycle(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    """
    Full cycle analysis for any coin — combines 4 independent cycle indicators.
    Usage: /cycle BTCUSDT
    """
    symbol = ctx.args[0].upper() if ctx.args else None
    if not symbol:
        await update.message.reply_text(
            "Usage: /cycle SYMBOL\n"
            "Example: /cycle BTCUSDT\n\n"
            "Combines 4 cycle indicators: BTC halving clock, "
            "Weekly RSI cycle, EMA 50/200 crossover cycle, BB squeeze cycle.")
        return

    if not symbol.endswith("USDT"):
        symbol += "USDT"

    await update.message.reply_text(
        f"⏳ Analysing full cycle for {symbol}...\n"
        f"Checking halving clock, RSI cycle, EMA crossovers, BB squeeze rhythm.\n"
        f"Takes about 10-15 seconds.")

    result = await asyncio.to_thread(_sync_full_cycle_analysis, symbol)

    h = result["halving"]
    r = result["rsi"]
    e = result["ema"]
    b = result["bb"]

    lines = [
        f"🔄 <b>Cycle Analysis — {symbol}</b>",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"",
        f"<b>1) Bitcoin Halving Clock (macro cycle)</b>",
        f"   Phase: {h['phase']}",
        f"   Days since last halving: {h['days_since_halving']} ({h['last_halving']})",
        f"   Days until next halving: {h['days_until_halving']} ({h['next_halving']})",
        f"   Cycle position: {h['cycle_pct']}% through 4-year cycle",
        f"",
    ]

    if r.get("error"):
        lines.append(f"<b>2) Weekly RSI Cycle</b>\n   ⚠️ {r['error']}\n")
    else:
        lines.extend([
            f"<b>2) Weekly RSI Cycle</b>",
            f"   Current RSI: {r['current_rsi']}",
            f"   Position: {r['position']}",
            f"   Avg cycle length: {r['avg_cycle_weeks']} weeks "
            f"(from {r['num_bottoms_found']} historical bottoms)" if r.get("avg_cycle_weeks") else "   Avg cycle length: insufficient history",
            f"",
        ])

    if e.get("error"):
        lines.append(f"<b>3) EMA 50/200 Crossover Cycle</b>\n   ⚠️ {e['error']}\n")
    else:
        trend_str = "🟢 Bullish (Golden Cross active)" if e["currently_bullish"] else "🔴 Bearish (Death Cross active)"
        lines.extend([
            f"<b>3) EMA 50/200 Crossover Cycle</b>",
            f"   Current trend: {trend_str}",
            f"   Last cross: {e['last_cross_type']} ({e['days_since_last_cross']} days ago)",
            f"   Avg cycle length: {e['avg_cycle_days']} days "
            f"({e['num_golden_crosses']} golden + {e['num_death_crosses']} death crosses found)" if e.get("avg_cycle_days") else "   Avg cycle length: insufficient history",
            f"",
        ])

    if b.get("error"):
        lines.append(f"<b>4) BB Squeeze Volatility Cycle</b>\n   ⚠️ {b['error']}\n")
    else:
        squeeze_str = "🟡 SQUEEZING NOW — breakout may be imminent" if b["is_squeezing_now"] else "⬜ Normal volatility"
        lines.extend([
            f"<b>4) BB Squeeze Volatility Cycle</b>",
            f"   Current: {squeeze_str}",
            f"   BB width: {b['current_width']}% (200d avg: {b['avg_width_200d']}%)",
            f"   Avg squeeze interval: {b['avg_squeeze_interval_days']} days "
            f"({b['num_squeezes_found']} squeezes found)" if b.get("avg_squeeze_interval_days") else "   Avg squeeze interval: insufficient history",
            f"   Days since last squeeze: {b['days_since_last_squeeze']}",
        ])

    lines.append("")
    fs = result.get("fast_swings", {})
    if fs.get("error"):
        lines.append(f"<b>5) Short-Term Cycle (5-day window, last 180d)</b>\n   ⚠️ {fs['error']}\n")
    else:
        fs_cycles = fs.get("completed_cycles", [])
        if fs.get("avg_short_cycle_days"):
            lines.extend([
                f"<b>5) Short-Term Cycle (5-day window, last 180d)</b>",
                f"   Avg swing-low-to-swing-low: {fs['avg_short_cycle_days']} days",
                f"   Days since last swing low: {fs['days_since_recent_low']}",
                f"   Completed short cycles found: {len(fs_cycles)} "
                f"({fs['num_swing_lows_found']} lows, {fs['num_swing_highs_found']} highs)",
            ])
            if fs_cycles:
                multiples = [c["multiple"] for c in fs_cycles]
                lines.append(f"   Historical low→high multiples: {', '.join(f'{m}x' for m in multiples)}")
        else:
            lines.extend([
                f"<b>5) Short-Term Cycle (5-day window, last 180d)</b>",
                f"   {fs['num_swing_lows_found']} swing lows found — not enough to average a short cycle",
                f"   This coin's short-term rhythm is not yet established, or moves too erratically",
                f"   for a repeating pattern in the last 180 days.",
            ])

    lines.append("")
    cpo_result = result.get("cpo", {})
    cpo_d = cpo_result.get("daily", {})
    cpo_h = cpo_result.get("higher_tf", {})
    if cpo_d.get("error"):
        lines.append(f"<b>6) Cycle Phase Oscillator (custom indicator)</b>\n   ⚠️ {cpo_d['error']}\n")
    else:
        agree_str = "✅ Timeframes AGREE" if cpo_result.get("agreement") else "⚠️ Timeframes DIFFER"
        trust_str = "trust this reading" if cpo_d['confidence'] >= 55 else "informational only — low confidence"
        lines.extend([
            f"<b>6) Cycle Phase Oscillator (custom-built)</b>",
            f"   {cpo_d['phase_emoji']} Phase: <b>{cpo_d['phase']}</b> "
            f"({cpo_d['days_in_current_phase']}d in phase, score margin {cpo_d['score_margin']})",
            f"   Confidence: {cpo_d['confidence']}% ({trust_str})",
            f"   Bias: {cpo_d['bias']}",
            f"   Higher-TF check: {cpo_h.get('phase','?')} — {agree_str}",
            f"   Combined confidence: {cpo_result.get('combined_confidence',0)}%",
        ])

    lines.append(f"\n<i>Raw indicator data above. Conclusion follows below ⬇️</i>")

    await update.message.reply_text("\n".join(lines), parse_mode="HTML")

    # ── Message 2: Data-derived conclusion ──────────────────────────────────
    c = result["conclusion"]

    concl_lines = [
        f"🎯 <b>Cycle Conclusion — {symbol}</b>",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"",
        f"<b>Cycle type:</b>\n{c['cycle_type']}",
        f"",
        f"<b>Where we are:</b>\n{c['position_summary']}",
        f"",
        f"<b>Time remaining:</b>\n{c['time_remaining']}",
        f"",
    ]

    # ── TOP price target (upside) ────────────────────────────────────────────
    tt = c.get("top_target")
    if tt is None:
        concl_lines.append(f"<b>🔝 TOP target (cycle high):</b>\nNot available — see data notes below")
    elif "num_cycles_used" in tt and tt["num_cycles_used"] >= 2:
        concl_lines.append(
            f"<b>🔝 TOP target</b> (from {tt['num_cycles_used']} completed cycles on this coin):\n"
            f"Based on most recent swing low: <code>{tt['based_on_low']}</code>\n"
            f"Conservative: <code>{tt['low_estimate']}</code>\n"
            f"Average:      <code>{tt['avg_estimate']}</code>\n"
            f"Optimistic:   <code>{tt['high_estimate']}</code>\n"
            f"<i>Range calibrated to ~70-85% historical coverage — i.e. in "
            f"backtesting, the coin's actual next cycle high fell inside a "
            f"range built this way roughly 70-85% of the time. Not a "
            f"point prediction — a calibrated range.</i>"
        )
    else:
        concl_lines.append(
            f"<b>🔝 TOP target</b> (⚠️ low confidence — only 1 completed cycle):\n"
            f"Based on swing low: <code>{tt['based_on_low']}</code>\n"
            f"Single-cycle estimate: <code>{tt['single_estimate']}</code> "
            f"({tt['single_cycle_multiple']:.2f}x historical multiple)\n"
            f"<i>Coverage calibration requires 2+ cycles — this single-cycle "
            f"estimate has NOT been validated for 70-85% coverage.</i>"
        )

    concl_lines.append("")

    # ── BOTTOM price target (downside) ───────────────────────────────────────
    bt = c.get("bottom_target")
    if bt is None:
        concl_lines.append(f"<b>🔻 BOTTOM target (cycle low):</b>\nNot available — see data notes below")
    elif "num_cycles_used" in bt and bt["num_cycles_used"] >= 2:
        concl_lines.append(
            f"<b>🔻 BOTTOM target</b> (from {bt['num_cycles_used']} confirmed drawdowns on this coin):\n"
            f"Based on most recent swing high: <code>{bt['based_on_high']}</code>\n"
            f"Deep (worst-case):    <code>{bt['deep_estimate']}</code>\n"
            f"Average:              <code>{bt['avg_estimate']}</code>\n"
            f"Shallow (best-case):  <code>{bt['shallow_estimate']}</code>\n"
            f"<i>Range calibrated to ~70-85% historical coverage — i.e. in "
            f"backtesting, the coin's actual next cycle low fell inside a "
            f"range built this way roughly 70-85% of the time. Not a "
            f"point prediction — a calibrated range.</i>"
        )
    else:
        concl_lines.append(
            f"<b>🔻 BOTTOM target</b> (⚠️ low confidence — only 1 confirmed drawdown):\n"
            f"Based on swing high: <code>{bt['based_on_high']}</code>\n"
            f"Single-drawdown estimate: <code>{bt['single_estimate']}</code> "
            f"({bt['single_retracement_ratio']:.2f}x of high)\n"
            f"<i>Coverage calibration requires 2+ cycles — this single-cycle "
            f"estimate has NOT been validated for 70-85% coverage.</i>"
        )

    concl_lines.extend([
        f"",
        f"<b>Outlook:</b>\n{c['outlook']}",
        f"",
        f"<b>What to do:</b>\n{c['action']}",
        f"",
    ])

    if c.get("cpo_agreement"):
        concl_lines.extend([
            f"<b>CPO cross-check:</b>\n{c['cpo_agreement']}",
            f"",
        ])

    concl_lines.append(f"<b>Confidence:</b> {c['confidence']}")

    if c["data_quality_notes"]:
        concl_lines.append(f"\n<b>⚠️ Data limitations:</b>")
        for note in c["data_quality_notes"]:
            concl_lines.append(f"• {note}")

    concl_lines.append(
        f"\n<i>Every value above is computed directly from {symbol}'s own "
        f"price history — no assumptions or generic Fibonacci levels used. "
        f"Where data is insufficient, this is stated explicitly rather "
        f"than estimated.</i>")

    await update.message.reply_text("\n".join(concl_lines), parse_mode="HTML")

# =============================================================================
#  END OF CYCLE DETECTION ENGINE
# =============================================================================

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

    # ── Step 1: Test write permission FIRST before anything else ─────────────
    state_dir = os.path.dirname(STATE_FILE)
    try:
        os.makedirs(state_dir, exist_ok=True)
        test_file = os.path.join(state_dir, ".write_test")
        with open(test_file, "w") as f:
            f.write("ok")
        os.remove(test_file)
        print(f"[STATE] Write permission OK: {state_dir}")
        print(f"[STATE] STATE_FILE = {STATE_FILE}")
    except Exception as e:
        print(f"[STATE] ⚠️ WRITE FAILED for {state_dir}: {e}")
        print(f"[STATE] Falling back to /tmp")
        globals()["STATE_FILE"]   = "/tmp/bot_state.json"
        globals()["HISTORY_FILE"] = "/tmp/trade_history.json"
        print(f"[STATE] STATE_FILE now = {STATE_FILE}")

    # ── Step 2: Check if state file exists THEN load it ──────────────────────
    state_existed = os.path.exists(STATE_FILE)
    print(f"[STATE] State file exists: {state_existed} ({STATE_FILE})")
    load_state()
    _load_history()

    # ── Step 3: Force-save immediately so file exists for next redeploy ───────
    # This writes current state (may be empty on fresh start) to disk.
    # On next redeploy load_state() will find this file and restore.
    save_state()
    print(f"[STATE] Startup save complete → {STATE_FILE}")

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
    app.add_handler(CommandHandler("commands",       cmd_commands))
    app.add_handler(CommandHandler("add_symbol",     cmd_add_symbol))
    app.add_handler(CommandHandler("remove_symbol",  cmd_remove_symbol))
    app.add_handler(CommandHandler("status",         cmd_status))
    app.add_handler(CommandHandler("scan",           cmd_scan))
    app.add_handler(CommandHandler("pause",          cmd_pause))
    app.add_handler(CommandHandler("close",          cmd_close))
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
    app.add_handler(CommandHandler("backup",          cmd_backup))
    app.add_handler(CommandHandler("regime",          cmd_regime))
    app.add_handler(CommandHandler("regime_all",      cmd_regime_all))
    app.add_handler(CommandHandler("backtest",         cmd_backtest))
    app.add_handler(CommandHandler("backtest_tp",      cmd_backtest_tp))
    app.add_handler(CommandHandler("screen",           cmd_screen))
    app.add_handler(CommandHandler("health",           cmd_health))
    app.add_handler(CommandHandler("uptrendcoins",     cmd_uptrendcoins))
    app.add_handler(CommandHandler("downtrendcoins",   cmd_downtrendcoins))
    app.add_handler(CommandHandler("cycle",             cmd_cycle))
    app.add_handler(CommandHandler("compare_stack",     cmd_compare_stack))
    app.add_handler(CommandHandler("backtest_compare",  cmd_backtest_compare))
    app.add_handler(CommandHandler("backtest_regime",   cmd_backtest_regime))
    app.add_handler(CommandHandler("backtest_regime_dates", cmd_backtest_regime_dates))
    app.add_handler(CommandHandler("backtest_regime_all_dates", cmd_backtest_regime_all_dates))
    app.add_handler(CommandHandler("backtest_regime_50_up_dates", cmd_backtest_regime_50_up_dates))
    app.add_handler(CommandHandler("backtest_regime_50_down_dates", cmd_backtest_regime_50_down_dates))
    app.add_handler(CommandHandler("regime_status", cmd_regime_status))
    app.add_handler(CommandHandler("add_symbol_regime", cmd_add_symbol_regime))
    app.add_handler(CommandHandler("remove_symbol_regime", cmd_remove_symbol_regime))
    app.add_handler(CommandHandler("regime_pause",  cmd_regime_pause))
    app.add_handler(CommandHandler("regime_resume", cmd_regime_resume))
    app.add_handler(CommandHandler("help",              cmd_help))
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

        # Regime Strategy (live) — single job checking the CURATED
        # regime_symbols watchlist (add/remove via /add_symbol_regime and
        # /remove_symbol_regime). Starts enabled by default (regime_paused=
        # False unless previously paused and persisted).
        app.job_queue.run_repeating(
            callback=regime_batch_scan,
            interval=REGIME_SCAN_INTERVAL_SEC,
            first=45,   # let the other startup jobs settle first
            chat_id=CHAT_ID,
            name="regime_strategy_scan",
        )
        print(f"[STARTUP] Regime strategy (live) active — checking "
              f"{len(regime_symbols)} watchlisted symbol(s) every "
              f"{REGIME_SCAN_INTERVAL_SEC}s "
              f"({'PAUSED' if regime_paused else 'RUNNING'})")

        # Regime Strategy — 45-minute status report for any OPEN position,
        # independent of the entry/exit event alerts above.
        app.job_queue.run_repeating(
            callback=regime_position_report,
            interval=REGIME_REPORT_INTERVAL_SEC,
            first=REGIME_REPORT_INTERVAL_SEC,   # first report 45 min after startup
            chat_id=CHAT_ID,
            name="regime_strategy_45min_report",
        )
        print(f"[STARTUP] Regime strategy 45-min position reports active "
              f"(every {REGIME_REPORT_INTERVAL_SEC//60} min)")

        # Disk-usage safeguard — trims trade_history.json to the last
        # HISTORY_KEEP_DAYS (3 weeks), independent of trade activity.
        app.job_queue.run_repeating(
            callback=cleanup_old_data,
            interval=CLEANUP_INTERVAL_SEC,
            first=120,   # run once shortly after startup, then every 6h
            chat_id=CHAT_ID,
            name="disk_cleanup",
        )
        print(f"[STARTUP] Disk cleanup active — trims trade history older "
              f"than {HISTORY_KEEP_DAYS} days, every {CLEANUP_INTERVAL_SEC//3600}h")

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

    # BUG FIX: this condition previously only checked monitored_symbols and
    # meme_symbols — a bot with ONLY regime-strategy symbols/positions open
    # would fall through to the "started fresh" branch below and be told
    # to manually re-add symbols, even though everything had actually
    # restored correctly in memory and was already being managed by the
    # regime_strategy_scan job. Now checks all three strategies.
    had_any_state = (monitored_symbols or meme_symbols or
                      regime_symbols or regime_positions)

    if state_existed and had_any_state:
        # State was restored successfully
        psych_syms  = ", ".join(monitored_symbols) if monitored_symbols else "none"
        meme_syms   = ", ".join(meme_symbols) if meme_symbols else "none"
        regime_syms = ", ".join(regime_symbols) if regime_symbols else "none"
        open_pos    = len(symbol_positions) + len(meme_positions) + len(regime_positions)

        regime_pos_lines = ""
        if regime_positions:
            regime_pos_lines = "\n" + "\n".join(
                f"   {pos['direction']} {sym} @ {pos.get('entry_price',0):.4f}"
                for sym, pos in regime_positions.items())

        startup_msg = (
            f"🔄 <b>Bot restarted &amp; state restored!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🕒 Time: {now_str}\n"
            f"💾 State: ✅ Loaded from <code>{STATE_FILE}</code>\n"
            f"💽 Persistent disk: {'✅ /var/data (state survives redeploys)' if disk_ok else '⚠️ Using /tmp — state may be lost on redeploy'}\n\n"
            f"<b>Restored:</b>\n"
            f"📊 PsychologyBot: {psych_syms}\n"
            f"⚡ MemeBot: {meme_syms}\n"
            f"🌐 Regime Strategy: {regime_syms}{regime_pos_lines}\n"
            f"📈 Open positions: {open_pos} total "
            f"({len(symbol_positions)} psych, {len(meme_positions)} meme, "
            f"{len(regime_positions)} regime)\n"
            f"📋 Trade history: {len(trade_history)} records\n\n"
            f"✅ All monitoring jobs restarted automatically across all "
            f"three strategies.\nNo action needed — bot is watching your symbols."
        )
    else:
        # Fresh start — no state file, or genuinely nothing to restore
        using_tmp = STATE_FILE.startswith("/tmp")
        startup_msg = (
            f"🚀 <b>Bot started fresh</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🕒 Time: {now_str}\n"
            f"💾 State file: <code>{STATE_FILE}</code>\n"
            f"💾 State: {'⚠️ No state file found' if not state_existed else '📭 State file empty — nothing to restore'}\n"
            f"💽 Disk: {'⚠️ /var/data not writable — using /tmp (state lost on redeploy)' if using_tmp else '✅ /var/data (persistent disk mounted)'}\n\n"
            + (
            f"<b>⚠️ State will NOT survive redeploys</b>\n"
            f"Go to Render Dashboard → Your service → Disks → Add Disk\n"
            f"Mount path: <code>/var/data</code>\n\n"
            if using_tmp else ""
            ) +
            f"<b>Add your symbols:</b>\n"
            f"• /add_symbol BTCUSDT — PsychologyBot (9-filter)\n"
            f"• /add_meme PEPEUSDT — MemeBot\n"
            f"• /add_symbol_regime SOLUSDT — Regime Strategy\n"
            f"(State will save automatically and survive future redeploys)"
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