
import os
import asyncio
import random
import time
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple, List, Dict
from dotenv import load_dotenv
from pybit.unified_trading import HTTP
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
#  Global state
# ────────────────────────────────────────────────
symbol_trends: dict[str, Optional[str]] = {}
monitored_symbols: list[str] = []
symbol_open_trade_side: dict[str, Optional[str]] = {}
symbol_manual_quantities: dict[str, Optional[float]] = {}
symbol_last_di_cross_candle: dict[str, Optional[str]] = {}

# NEW: Per-symbol risk management tracking
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

# ────────────────────────────────────────────────
#  Constants
# ────────────────────────────────────────────────
ATR_MULTIPLIER = 1.5        # Legacy (kept for reference)
ADX_THRESHOLD = 20           # Only trade when ADX > 20 (trending)
STOP_LOSS_ATR_MULT = 2.0    # Stop-loss at 2x ATR from entry
TRAILING_ACTIVATE_ATR = 1.5  # Activate trailing stop after 1.5x ATR profit
TRAILING_STOP_ATR = 1.0      # Trailing stop distance: 1x ATR from peak
TRAILING_CLOSE_PCT = 0.60    # Close 60% of position when trailing activates (lock profits)
POLL_INTERVAL_SECONDS = 10   # Poll every 10 seconds (faster for risk management)

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

async def get_ichimoku_and_atr(
    symbol: str,
    interval: str = "30",
    limit: int = 100,
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Returns: (conversion_line, atr, last_close, current_price)
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

        if df["close"].isna().any():
            raise ValueError("Invalid prices")

        nine_period_high = df["high"].rolling(window=9).max()
        nine_period_low = df["low"].rolling(window=9).min()
        df["tenkan_sen"] = (nine_period_high + nine_period_low) / 2
        conversion_line = float(df["tenkan_sen"].iloc[-1])

        df["prev_close"] = df["close"].shift(1)
        df["tr1"] = df["high"] - df["low"]
        df["tr2"] = (df["high"] - df["prev_close"]).abs()
        df["tr3"] = (df["low"] - df["prev_close"]).abs()
        df["tr"] = df[["tr1", "tr2", "tr3"]].max(axis=1)
        df["atr"] = df["tr"].rolling(window=14).mean()
        atr = float(df["atr"].iloc[-1])

        last_close = float(df["close"].iloc[-1])

        ticker_resp = await asyncio.to_thread(
            bybit_session.get_tickers,
            category="linear",
            symbol=symbol
        )
        ticker = ticker_resp["result"]["list"][0]
        current_price = float(ticker["lastPrice"])

        return conversion_line, atr, last_close, current_price

    except Exception as exc:
        print(f"Error in get_ichimoku_and_atr for {symbol}: {exc}")
        return None, None, None, None

async def get_all_indicators(
    symbol: str,
    interval: str = "30",
    limit: int = 100,
) -> Optional[dict]:
    """
    Returns a dict with all trading indicators:
    - conversion_line, atr, ema_20
    - plus_di, minus_di (current candle)
    - prev_plus_di, prev_minus_di (previous candle)
    - di_crossed (bool), di_cross_type ("bullish"/"bearish"/None)
    - adx (Average Directional Index)
    - current_price (live tick)
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

        if df["close"].isna().any():
            raise ValueError("Invalid prices")

        # 1. Ichimoku Conversion Line (Tenkan-sen)
        nine_period_high = df["high"].rolling(window=9).max()
        nine_period_low = df["low"].rolling(window=9).min()
        df["tenkan_sen"] = (nine_period_high + nine_period_low) / 2
        conversion_line = float(df["tenkan_sen"].iloc[-1])

        # 2. ATR (14) using Wilder's smoothing
        df["prev_close"] = df["close"].shift(1)
        df["tr1"] = df["high"] - df["low"]
        df["tr2"] = (df["high"] - df["prev_close"]).abs()
        df["tr3"] = (df["low"] - df["prev_close"]).abs()
        df["tr"] = df[["tr1", "tr2", "tr3"]].max(axis=1)
        df["atr"] = df["tr"].ewm(alpha=1/14, min_periods=14).mean()
        atr = float(df["atr"].iloc[-1])

        # 3. 20 EMA (30-minute chart)
        df["ema_20"] = df["close"].ewm(span=20, adjust=False).mean()
        ema_20 = float(df["ema_20"].iloc[-1])

        # 4. DMI: +DI / -DI (14-period, Wilder's smoothing)
        df["up_move"] = df["high"] - df["high"].shift(1)
        df["down_move"] = df["low"].shift(1) - df["low"]

        df["+dm"] = 0.0
        df["-dm"] = 0.0
        plus_dm_mask = (df["up_move"] > df["down_move"]) & (df["up_move"] > 0)
        minus_dm_mask = (df["down_move"] > df["up_move"]) & (df["down_move"] > 0)
        df.loc[plus_dm_mask, "+dm"] = df.loc[plus_dm_mask, "up_move"]
        df.loc[minus_dm_mask, "-dm"] = df.loc[minus_dm_mask, "down_move"]

        # Wilder's smoothing (EMA with alpha=1/14)
        df["+dm_smooth"] = df["+dm"].ewm(alpha=1/14, min_periods=14).mean()
        df["-dm_smooth"] = df["-dm"].ewm(alpha=1/14, min_periods=14).mean()

        # +DI and -DI
        df["+di"] = (df["+dm_smooth"] / df["atr"]) * 100
        df["-di"] = (df["-dm_smooth"] / df["atr"]) * 100

        plus_di = float(df["+di"].iloc[-1])
        minus_di = float(df["-di"].iloc[-1])
        prev_plus_di = float(df["+di"].iloc[-2])
        prev_minus_di = float(df["-di"].iloc[-2])

        # 5. ADX (Average Directional Index) - 14-period Wilder's smoothing of DX
        di_sum = df["+di"] + df["-di"]
        di_sum = di_sum.replace(0, float('nan'))  # avoid division by zero
        df["dx"] = (abs(df["+di"] - df["-di"]) / di_sum) * 100
        df["adx"] = df["dx"].ewm(alpha=1/14, min_periods=14).mean()
        adx = float(df["adx"].iloc[-1])

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
        last_close = float(df["close"].iloc[-2])  # second-to-last = last CLOSED candle

        # Current price from ticker (for display / real-time reference)
        ticker_resp = await asyncio.to_thread(
            bybit_session.get_tickers,
            category="linear",
            symbol=symbol
        )
        ticker = ticker_resp["result"]["list"][0]
        current_price = float(ticker["lastPrice"])

        return {
            "conversion_line": conversion_line,
            "atr": atr,
            "ema_20": ema_20,
            "plus_di": plus_di,
            "minus_di": minus_di,
            "prev_plus_di": prev_plus_di,
            "prev_minus_di": prev_minus_di,
            "di_crossed": di_crossed,
            "di_cross_type": di_cross_type,
            "adx": adx,
            "current_price": current_price,
            "last_close": last_close,
            "last_candle_ts": last_candle_ts,
        }

    except Exception as exc:
        print(f"Error in get_all_indicators for {symbol}: {exc}")
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

        _, _, _, current_price = await get_ichimoku_and_atr(symbol)
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


async def cancel_server_stops(symbol: str) -> bool:
    """
    Cancel all server-side TP/SL/trailing stops + any conditional trail orders.
    """
    # Cancel the conditional trail stop order first (if any)
    await cancel_trail_stop_order(symbol)
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
    plus_di = indicators["plus_di"]
    minus_di = indicators["minus_di"]
    di_crossed = indicators["di_crossed"]
    di_cross_type = indicators["di_cross_type"]
    last_candle_ts = indicators["last_candle_ts"]

    chat_id = context.job.chat_id if context.job else CHAT_ID

    # Use CLOSED candle price for all market state decisions
    if last_close > ema_20:
        current_market_state = "ABOVE 20 EMA (Bullish)"
    elif last_close < ema_20:
        current_market_state = "BELOW 20 EMA (Bearish)"
    else:
        current_market_state = "AT 20 EMA (Neutral)"

    # ──────────────────────────────────────────────
    # 1. RISK MANAGEMENT: Server-side stops + client-side backup
    #    - Server-side SL at 2x ATR (set at entry, Bybit executes instantly)
    #    - Trailing activation at 1.5x ATR profit: places conditional stop on 60%
    #    - Trail moves with price; Bybit closes 60% INSTANTLY when trail is hit
    #    - Remaining 40% has NO stop — exits only on EMA+DI crossover
    #    - Uses current_price (real-time) for all risk checks
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

        # Ensure server-side stop-loss is set (retry if not) — skip if trailing is active (SL was replaced by trail order)
        if not symbol_server_sl_set.get(symbol, False) and not symbol_partial_taken.get(symbol, False) and not symbol_trailing_active.get(symbol, False):
            if current_open_trade_side == "Buy":
                sl_price = entry_price - (STOP_LOSS_ATR_MULT * entry_atr)
            else:
                sl_price = entry_price + (STOP_LOSS_ATR_MULT * entry_atr)
            await set_server_stop_loss(symbol, current_open_trade_side, sl_price)

        # Update peak price tracking using REAL-TIME price (not last_close)
        peak = symbol_peak_price.get(symbol, entry_price)
        if current_open_trade_side == "Buy":
            peak = max(peak, current_price)
        else:  # Sell
            peak = min(peak, current_price)
        symbol_peak_price[symbol] = peak

        # ── CLIENT-SIDE BACKUP: ATR Stop-Loss Check using current_price ──
        # Server-side SL should fire first, but this is a safety net.
        # Skip this check if trailing is active (full SL was cancelled, trail order handles risk)
        # or if partial was already taken (remaining 40% has no SL, exits on EMA+DI only).
        if current_open_trade_side == "Buy" and not symbol_trailing_active.get(symbol, False) and not symbol_partial_taken.get(symbol, False):
            stop_loss_price = entry_price - (STOP_LOSS_ATR_MULT * entry_atr)
            if current_price <= stop_loss_price:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"\U0001f6d1 STOP-LOSS HIT for {symbol} (Long) [Backup check]\n"
                        f"Entry: {entry_price} | Stop: {stop_loss_price:.6g}\n"
                        f"Current Price: {current_price} | ATR: {entry_atr:.6g}\n"
                        f"Loss capped at 2x ATR. Going FLAT.\n"
                        f"(Server-side SL should have fired first)"
                    )
                )
                await close_position(symbol, side_to_close="Buy")
                symbol_trends[symbol] = None
                symbol_open_trade_side[symbol] = None
                return

        elif current_open_trade_side == "Sell" and not symbol_trailing_active.get(symbol, False) and not symbol_partial_taken.get(symbol, False):
            stop_loss_price = entry_price + (STOP_LOSS_ATR_MULT * entry_atr)
            if current_price >= stop_loss_price:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"\U0001f6d1 STOP-LOSS HIT for {symbol} (Short) [Backup check]\n"
                        f"Entry: {entry_price} | Stop: {stop_loss_price:.6g}\n"
                        f"Current Price: {current_price} | ATR: {entry_atr:.6g}\n"
                        f"Loss capped at 2x ATR. Going FLAT.\n"
                        f"(Server-side SL should have fired first)"
                    )
                )
                await close_position(symbol, side_to_close="Sell")
                symbol_trends[symbol] = None
                symbol_open_trade_side[symbol] = None
                return

        # ── Trailing Stop: Activate at 1.5x ATR profit ──
        # When activated: place a conditional stop order on 60% of position.
        # Bybit executes the 60% close INSTANTLY when trail price is hit (no slippage).
        # The remaining 40% has NO trailing stop — exits only on EMA+DI crossover.
        trailing_active = symbol_trailing_active.get(symbol, False)
        partial_taken = symbol_partial_taken.get(symbol, False)

        if current_open_trade_side == "Buy":
            unrealized_profit = peak - entry_price

            # ── First activation: place the 60% trail stop order ──
            if unrealized_profit >= TRAILING_ACTIVATE_ATR * entry_atr and not trailing_active:
                symbol_trailing_active[symbol] = True
                trailing_active = True

                # Get current position size and calculate 60% qty
                pos_qty = await get_position_size(symbol)
                if pos_qty:
                    trail_qty = pos_qty * TRAILING_CLOSE_PCT
                    trail_stop_price = peak - (TRAILING_STOP_ATR * entry_atr)

                    # Cancel the full-position SL (replaced by the conditional trail order)
                    await cancel_server_stops(symbol)

                    # Place conditional stop order for 60% at trail price
                    order_id = await place_trail_stop_order(symbol, "Buy", trail_qty, trail_stop_price)

                    if order_id:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"\U0001f4c8 TRAILING ACTIVATED for {symbol} (Long)\n"
                                f"Profit reached {TRAILING_ACTIVATE_ATR}x ATR ({unrealized_profit:.6g})\n"
                                f"Peak: {peak:.6g} | Current: {current_price}\n"
                                f"Trail stop set on {TRAILING_CLOSE_PCT*100:.0f}% at {trail_stop_price:.6g}\n"
                                f"Trail moves UP as price rises. Bybit executes INSTANTLY when hit.\n"
                                f"Remaining {(1-TRAILING_CLOSE_PCT)*100:.0f}% continues until EMA+DI exit."
                            )
                        )
                    else:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=f"\u26a0\ufe0f {symbol}: Trail activated but failed to place stop order. Will retry."
                        )

            # ── Update trail price if peak moved (trail only moves UP for longs) ──
            if trailing_active and not partial_taken:
                current_trail = symbol_trail_price.get(symbol)
                new_trail_price = peak - (TRAILING_STOP_ATR * entry_atr)

                if current_trail is not None and new_trail_price > current_trail:
                    success = await amend_trail_stop_price(symbol, new_trail_price)
                    if not success:
                        # Amend failed — order may have been filled, check below
                        pass

                # ── Detect if the 60% trail order has been filled ──
                current_qty = await get_position_size(symbol)
                trail_qty = symbol_trail_qty.get(symbol)
                if current_qty is not None and trail_qty is not None:
                    expected_remaining = current_qty  # After 60% close, ~40% remains
                    # If position has shrunk significantly, the trail order filled
                    if trail_qty > 0 and current_qty <= trail_qty * 0.15:
                        # Position is now ~0 — trail closed everything (position was too small to split)
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
                    # Position fully gone — trail order closed everything
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

                # Check if trail order is gone but position still exists = 60% was closed
                order_id = symbol_trail_order_id.get(symbol)
                if order_id and current_qty is not None:
                    # Position still exists but might be smaller — check if order filled
                    # We detect fill by: position qty is now roughly 40% of what it was
                    # Since we don't track original qty, use trail_qty: if pos_qty ≈ original - trail_qty
                    if trail_qty and current_qty < trail_qty:
                        # Position is smaller than the 60% we wanted to close = partial fill happened
                        symbol_partial_taken[symbol] = True
                        trail_price_display = symbol_trail_price.get(symbol, 0)
                        symbol_trail_order_id[symbol] = None
                        symbol_trail_qty[symbol] = None
                        symbol_server_ts_set[symbol] = False
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"\U0001f3c1 TRAILING STOP closed {TRAILING_CLOSE_PCT*100:.0f}% for {symbol} (Long)\n"
                                f"Trail price hit: {trail_price_display:.6g}\n"
                                f"Remaining {(1-TRAILING_CLOSE_PCT)*100:.0f}% continues trading.\n"
                                f"Will exit on EMA cross + DI crossover only."
                            )
                        )

        elif current_open_trade_side == "Sell":
            unrealized_profit = entry_price - peak  # peak is min for shorts

            # ── First activation: place the 60% trail stop order ──
            if unrealized_profit >= TRAILING_ACTIVATE_ATR * entry_atr and not trailing_active:
                symbol_trailing_active[symbol] = True
                trailing_active = True

                pos_qty = await get_position_size(symbol)
                if pos_qty:
                    trail_qty = pos_qty * TRAILING_CLOSE_PCT
                    trail_stop_price = peak + (TRAILING_STOP_ATR * entry_atr)

                    # Cancel the full-position SL
                    await cancel_server_stops(symbol)

                    # Place conditional stop order for 60% at trail price
                    order_id = await place_trail_stop_order(symbol, "Sell", trail_qty, trail_stop_price)

                    if order_id:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"\U0001f4c8 TRAILING ACTIVATED for {symbol} (Short)\n"
                                f"Profit reached {TRAILING_ACTIVATE_ATR}x ATR ({unrealized_profit:.6g})\n"
                                f"Trough: {peak:.6g} | Current: {current_price}\n"
                                f"Trail stop set on {TRAILING_CLOSE_PCT*100:.0f}% at {trail_stop_price:.6g}\n"
                                f"Trail moves DOWN as price falls. Bybit executes INSTANTLY when hit.\n"
                                f"Remaining {(1-TRAILING_CLOSE_PCT)*100:.0f}% continues until EMA+DI exit."
                            )
                        )
                    else:
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=f"\u26a0\ufe0f {symbol}: Trail activated but failed to place stop order. Will retry."
                        )

            # ── Update trail price if trough moved (trail only moves DOWN for shorts) ──
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

                order_id = symbol_trail_order_id.get(symbol)
                if order_id and current_qty is not None:
                    if trail_qty and current_qty < trail_qty:
                        symbol_partial_taken[symbol] = True
                        trail_price_display = symbol_trail_price.get(symbol, 0)
                        symbol_trail_order_id[symbol] = None
                        symbol_trail_qty[symbol] = None
                        symbol_server_ts_set[symbol] = False
                        await context.bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"\U0001f3c1 TRAILING STOP closed {TRAILING_CLOSE_PCT*100:.0f}% for {symbol} (Short)\n"
                                f"Trail price hit: {trail_price_display:.6g}\n"
                                f"Remaining {(1-TRAILING_CLOSE_PCT)*100:.0f}% continues trading.\n"
                                f"Will exit on EMA cross + DI crossover only."
                            )
                        )

    # ──────────────────────────────────────────────
    # 2. EXIT + POSSIBLE REVERSAL (Decoupled)
    #    Exit requires: price crosses EMA AND DI crossover.
    #    Reverse ONLY if DI + ADX confirm. Otherwise go FLAT.
    # ──────────────────────────────────────────────
    if current_open_trade_side is not None:

        # ── Long Exit: close price below 20 EMA AND DI crossover ──
        if current_open_trade_side == "Buy" and last_close < ema_20 and di_crossed:
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"\u26a0\ufe0f EXIT SIGNAL for {symbol} (Long)\n"
                    f"Last Close ({last_close}) < 20 EMA ({ema_20:.6g})\n"
                    f"DI Crossover detected ({di_cross_type})\n"
                    f"+DI: {plus_di:.2f} | -DI: {minus_di:.2f}\n"
                    f"Closing Long position..."
                )
            )

            await cancel_server_stops(symbol)  # Cancel server-side stops before closing
            closed_qty = await close_position(symbol, side_to_close="Buy")
            manual_override = None
            if symbol in symbol_manual_quantities and symbol_manual_quantities[symbol] is not None:
                manual_override = closed_qty

            # Check if we should REVERSE to Short (requires: -DI > +DI AND ADX > 20)
            if minus_di > plus_di and adx > ADX_THRESHOLD:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"\U0001f504 REVERSE to Short for {symbol}\n"
                        f"-DI ({minus_di:.2f}) > +DI ({plus_di:.2f}) | ADX: {adx:.2f} > {ADX_THRESHOLD}\n"
                        f"Opening Short..."
                    )
                )
                result = await place_trade(symbol, side="Sell", manual_qty_override=manual_override)
                if result and "error" not in result:
                    order_id = result.get("orderId", "\u2014")
                    _set_entry_risk_state(symbol, last_close, atr)
                    # Set server-side stop-loss immediately
                    sl_price = last_close + (STOP_LOSS_ATR_MULT * atr)
                    await set_server_stop_loss(symbol, "Sell", sl_price)
                    await context.bot.send_message(chat_id=chat_id, text=f"\u2705 Short order(s) placed for {symbol}\nLast Order ID: <code>{order_id}</code>\nServer-side SL: {sl_price:.6g}", parse_mode="HTML")
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
                        f"No reverse signal (need: -DI > +DI AND ADX > {ADX_THRESHOLD})\n"
                        f"+DI: {plus_di:.2f} | -DI: {minus_di:.2f} | ADX: {adx:.2f}\n"
                        f"Waiting for next entry signal..."
                    )
                )
                symbol_trends[symbol] = None
                symbol_open_trade_side[symbol] = None
            return

        # ── Short Exit: close price above 20 EMA AND DI crossover ──
        elif current_open_trade_side == "Sell" and last_close > ema_20 and di_crossed:
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"\u26a0\ufe0f EXIT SIGNAL for {symbol} (Short)\n"
                    f"Last Close ({last_close}) > 20 EMA ({ema_20:.6g})\n"
                    f"DI Crossover detected ({di_cross_type})\n"
                    f"+DI: {plus_di:.2f} | -DI: {minus_di:.2f}\n"
                    f"Closing Short position..."
                )
            )

            await cancel_server_stops(symbol)  # Cancel server-side stops before closing
            closed_qty = await close_position(symbol, side_to_close="Sell")
            manual_override = None
            if symbol in symbol_manual_quantities and symbol_manual_quantities[symbol] is not None:
                manual_override = closed_qty

            # Check if we should REVERSE to Long (requires: +DI > -DI AND ADX > 20)
            if plus_di > minus_di and adx > ADX_THRESHOLD:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"\U0001f504 REVERSE to Long for {symbol}\n"
                        f"+DI ({plus_di:.2f}) > -DI ({minus_di:.2f}) | ADX: {adx:.2f} > {ADX_THRESHOLD}\n"
                        f"Opening Long..."
                    )
                )
                result = await place_trade(symbol, side="Buy", manual_qty_override=manual_override)
                if result and "error" not in result:
                    order_id = result.get("orderId", "\u2014")
                    _set_entry_risk_state(symbol, last_close, atr)
                    # Set server-side stop-loss immediately
                    sl_price = last_close - (STOP_LOSS_ATR_MULT * atr)
                    await set_server_stop_loss(symbol, "Buy", sl_price)
                    await context.bot.send_message(chat_id=chat_id, text=f"\u2705 Long order(s) placed for {symbol}\nLast Order ID: <code>{order_id}</code>\nServer-side SL: {sl_price:.6g}", parse_mode="HTML")
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
                        f"No reverse signal (need: +DI > -DI AND ADX > {ADX_THRESHOLD})\n"
                        f"+DI: {plus_di:.2f} | -DI: {minus_di:.2f} | ADX: {adx:.2f}\n"
                        f"Waiting for next entry signal..."
                    )
                )
                symbol_trends[symbol] = None
                symbol_open_trade_side[symbol] = None
            return

    # ──────────────────────────────────────────────
    # 3. INITIAL TRADE ENTRY (no open position)
    #    Requires: ADX > 20 + EMA direction + DI alignment
    # ──────────────────────────────────────────────
    if current_open_trade_side is None:
        # ADX filter: only trade in trending markets
        if adx < ADX_THRESHOLD:
            # Market is sideways - stay flat, do not enter
            # (Only log occasionally to avoid spam)
            return

        new_trend = None
        side = None
        # Long: last_close > 20 EMA AND +DI > -DI
        if last_close > ema_20 and plus_di > minus_di:
            new_trend = "upward"
            side = "Buy"
        # Short: last_close < 20 EMA AND -DI > +DI
        elif last_close < ema_20 and minus_di > plus_di:
            new_trend = "downward"
            side = "Sell"

        if new_trend and side and (previous_trend is None or new_trend != previous_trend):
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            side_text = "Long (Buy)" if side == "Buy" else "Short (Sell)"
            msg = (
                f"{ts} \u2014 {symbol} ENTRY SIGNAL: <b>{new_trend.upper()}</b>\n"
                f"Last Close: {last_close} | 20 EMA: {ema_20:.6g}\n"
                f"+DI: {plus_di:.2f} | -DI: {minus_di:.2f} | ADX: {adx:.2f}\n"
                f"Direction: {side_text}"
            )
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")

            result = await place_trade(symbol, side=side)

            if result and "error" not in result:
                order_id = result.get("orderId", "\u2014")
                _set_entry_risk_state(symbol, last_close, atr)
                # Set server-side stop-loss immediately after entry
                if side == "Buy":
                    sl_price = last_close - (STOP_LOSS_ATR_MULT * atr)
                else:
                    sl_price = last_close + (STOP_LOSS_ATR_MULT * atr)
                await set_server_stop_loss(symbol, side, sl_price)
                await context.bot.send_message(chat_id=chat_id, text=f"\u2705 {side_text} order(s) placed for {symbol}\nLast Order ID: <code>{order_id}</code>\nServer-side SL: {sl_price:.6g}", parse_mode="HTML")
            elif result and "error" in result:
                await context.bot.send_message(chat_id=chat_id, text=f"\u274c Failed to place {side_text} for {symbol}: {result['error']}")

            symbol_trends[symbol] = new_trend
            symbol_open_trade_side[symbol] = await get_open_position_side(symbol)

    # ──────────────────────────────────────────────
    # 4. 30-Minute Status Report
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
            server_sl = symbol_server_sl_set.get(symbol, False)
            server_ts = symbol_server_ts_set.get(symbol, False)
            trail_price = symbol_trail_price.get(symbol)
            if partial_taken_flag:
                trailing_status = f'60% CLOSED by trail. 40% riding until EMA+DI exit'
            elif trailing and trail_price:
                trailing_status = f'ACTIVE - trail on 60% @ {trail_price:.6g}'
            elif trailing:
                trailing_status = 'ACTIVE (placing trail order...)'
            else:
                trailing_status = 'Not yet'
            risk_info += f"\n\u2022 Trailing Stop: {trailing_status}"
            risk_info += f"\n\u2022 Server-Side SL: {'SET' if server_sl else 'Not set'}"
            risk_info += f"\n\u2022 Server-Side TS: {'SET' if server_ts else 'Not set'}"

        report_msg = (
            f"Still monitoring {symbol}\n"
            f"\u2022 Entry: EMA + DI alignment + ADX > {ADX_THRESHOLD}\n"
            f"\u2022 Exit: Price crosses EMA + DI crossover required\n"
            f"\u2022 Reverse: Only if DI + ADX confirm, else FLAT\n"
            f"\u2022 Server-Side SL: {STOP_LOSS_ATR_MULT}x ATR | Trail on {TRAILING_CLOSE_PCT*100:.0f}% at {TRAILING_ACTIVATE_ATR}x ATR profit, {(1-TRAILING_CLOSE_PCT)*100:.0f}% rides until exit\n"
            f"\u2022 Monitoring every {POLL_INTERVAL_SECONDS}s (real-time price for risk)\n"
            f"\u2022 20 EMA: {ema_20:.6g} | +DI: {plus_di:.2f} | -DI: {minus_di:.2f} | ADX: {adx:.2f}\n"
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

            # Check ADX filter
            if adx < ADX_THRESHOLD:
                await update.message.reply_text(
                    f"\u26a0\ufe0f ADX ({adx:.2f}) < {ADX_THRESHOLD} - Market is SIDEWAYS.\n"
                    f"Instant trade skipped. Monitoring will wait for trending conditions."
                )
            else:
                # Require EMA + DI alignment
                side = None
                if last_close > ema_20 and plus_di > minus_di:
                    side = "Buy"
                elif last_close < ema_20 and minus_di > plus_di:
                    side = "Sell"

                if side is None:
                    await update.message.reply_text(
                        f"\u26a0\ufe0f No aligned signal for {symbol}.\n"
                        f"Last Close: {last_close} | EMA: {ema_20:.6g}\n"
                        f"+DI: {plus_di:.2f} | -DI: {minus_di:.2f} | ADX: {adx:.2f}\n"
                        f"Need: Price > EMA + (+DI > -DI) for Long, or Price < EMA + (-DI > +DI) for Short.\n"
                        f"Monitoring will wait for proper alignment."
                    )
                else:
                    side_text = "Long (Buy)" if side == "Buy" else "Short (Sell)"

                    await update.message.reply_text(
                        f"\U0001f4ca {symbol}: Last Close={last_close} | 20 EMA={ema_20:.6g}\n"
                        f"+DI: {plus_di:.2f} | -DI: {minus_di:.2f} | ADX: {adx:.2f}\n"
                        f"Direction: {side_text}"
                    )

                    result = await place_trade(symbol, side=side, manual_qty_override=manual_qty)

                    if result and "error" not in result:
                        order_id = result.get("orderId", "\u2014")
                        _set_entry_risk_state(symbol, last_close, atr)
                        # Set server-side stop-loss immediately
                        if side == "Buy":
                            sl_price = last_close - (STOP_LOSS_ATR_MULT * atr)
                        else:
                            sl_price = last_close + (STOP_LOSS_ATR_MULT * atr)
                        await set_server_stop_loss(symbol, side, sl_price)
                        await update.message.reply_text(
                            f"\u2705 Instant {side_text} order(s) placed for {symbol}\n"
                            f"Last Order ID: <code>{order_id}</code>\n"
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
        f"\u2022 Entry: Last Close vs 20 EMA + DI alignment + ADX > {ADX_THRESHOLD}\n"
        f"\u2022 Exit: Price crosses 20 EMA + DI crossover required\n"
        f"\u2022 Reverse only if DI + ADX confirm, else FLAT\n"
        f"\u2022 Server-Side SL: {STOP_LOSS_ATR_MULT}x ATR | Trail on {TRAILING_CLOSE_PCT*100:.0f}% at {TRAILING_ACTIVATE_ATR}x ATR profit, {(1-TRAILING_CLOSE_PCT)*100:.0f}% rides until exit\n"
        f"\u2022 Monitoring every {POLL_INTERVAL_SECONDS}s (real-time price for risk checks)\n"
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
    for state_dict in [symbol_entry_price, symbol_entry_atr, symbol_peak_price, symbol_trailing_active, symbol_partial_taken, symbol_server_sl_set, symbol_server_ts_set, symbol_trail_order_id, symbol_trail_qty, symbol_trail_price]:
        if symbol in state_dict:
            del state_dict[symbol]

    if context.job_queue:
        jobs = context.job_queue.get_jobs_by_name(f"{symbol}_monitor")
        for job in jobs:
            job.schedule_removal()

    await update.message.reply_text(f"Stopped monitoring {symbol}.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Privacy: only the owner can use this bot
    if update.effective_chat.id != CHAT_ID:
        await update.message.reply_text("\u26d4 Unauthorized. This bot is private.")
        return
    await update.message.reply_text(
        "Bot ready! (v5 - Server-Side Trailing)\n\n"
        "Commands:\n"
        "1. /start - Display this message\n"
        "2. /add_symbol <symbol> - Start monitoring with auto-sizing (20% balance)\n"
        "3. /add_symbol <symbol> <quantity> - INSTANT TRADE + Manual quantity monitoring\n"
        "4. /remove_symbol <symbol> - Stop monitoring a symbol\n\n"
        "v5 Trailing Stop Behavior:\n"
        f"\u2022 SERVER-SIDE stop-loss ({STOP_LOSS_ATR_MULT}x ATR) at entry\n"
        f"\u2022 At {TRAILING_ACTIVATE_ATR}x ATR profit: trailing stop activates on {TRAILING_CLOSE_PCT*100:.0f}%\n"
        f"\u2022 Trail moves with price ({TRAILING_STOP_ATR}x ATR distance). Bybit closes {TRAILING_CLOSE_PCT*100:.0f}% INSTANTLY when hit\n"
        f"\u2022 Remaining {(1-TRAILING_CLOSE_PCT)*100:.0f}% has NO trailing stop \u2014 exits on EMA+DI crossover only\n"
        f"\u2022 ADX > {ADX_THRESHOLD} filter (no trades in sideways markets)\n"
        "\u2022 EMA + DI alignment required for entry\n"
        "\u2022 Exit requires: EMA cross + DI crossover\n"
        f"\u2022 Real-time price for risk checks (polls every {POLL_INTERVAL_SECONDS}s)\n"
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
