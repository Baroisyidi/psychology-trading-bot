
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

# Global state
symbol_trends: dict[str, Optional[str]] = {}
monitored_symbols: list[str] = []
symbol_open_trade_side: dict[str, Optional[str]] = {}
symbol_manual_quantities: dict[str, Optional[float]] = {} # Store manual quantity override
symbol_last_di_cross_candle: dict[str, Optional[str]] = {}  # Track last DI cross candle to avoid re-triggering

# ATR Multiplier for the buffer zone
ATR_MULTIPLIER = 1.5

MOTIVATIONAL_QUOTES = [
    "“The market doesn’t reward intelligence 🧠 — it rewards discipline 🧱.\nSmart traders survive, disciplined traders compound.”",
    "“Consistency 🔁 beats luck 🎲 every single time.\nWhat you repeat daily becomes your edge.”",
    "“Your greatest edge isn’t your strategy 📊 — it’s your psychology 🧠.\nMaster your mind, and the market follows.”",
    "“Every loss is tuition 🎓.\nEvery lesson learned compounds into future strength 📈.”",
    "“Control your emotions 🎭, and you control your outcomes 🎯.\nLose emotional control, and the market decides for you.”",
    "“Trading is a marathon 🏃‍♂️, not a sprint ⚡.\nPatience is the fuel that finishes the race.”",
    "“Survive first 🛡️. Profit later 💰.\nWinners are simply those who stayed long enough to win.”",
    "“The market tests your discipline 🧪 before it rewards your skill 🏆.\nPass the test, earn the payout.”",
    "“You don’t need to predict the market 🔮 —\nYou need to react correctly ⚙️ when it reveals itself.”",
    "“Missed trades cost nothing 🚫.\nBad trades drain capital, confidence, and clarity 💥.”",
    "“Boring, repeatable execution 🔄 is where real money is made 💎.\nExcitement is expensive.”",
    "“Your job isn’t to win today 🗓️ —\nIt’s to still be profitable next year 📆.”",
    "“Emotional trading feels good short-term 😌\nand destroys accounts long-term 💣.”",
    "“The best traders think in probabilities 🎯, not predictions 🎲.\nCertainty is an illusion.”",
    "“Capital preservation 🛡️ is the first form of profit 💰.\nProtect the account, and growth follows.”",
    "“Discipline means doing the right thing 🧱\neven when every emotion tells you not to 😤.”",
    "“The market rewards patience ⏳ more than bravery 🦁.\nWaiting is a position.”",
    "“One good system ⚙️ executed flawlessly\nbeats ten impulsive ideas 💭 every time.”",
    "“If you can wait ⏸️, you can win 🏆.\nTime favors the disciplined.”",
    "“Professionals focus on process 🛠️.\nAmateurs obsess over outcomes 🎭.”"
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
        # Offload blocking API call to a thread
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

        # 1. Calculate Ichimoku Conversion Line (Tenkan-sen)
        nine_period_high = df["high"].rolling(window=9).max()
        nine_period_low = df["low"].rolling(window=9).min()
        df["tenkan_sen"] = (nine_period_high + nine_period_low) / 2
        conversion_line = float(df["tenkan_sen"].iloc[-1])

        # 2. Calculate ATR (14)
        df["prev_close"] = df["close"].shift(1)
        df["tr1"] = df["high"] - df["low"]
        df["tr2"] = (df["high"] - df["prev_close"]).abs()
        df["tr3"] = (df["low"] - df["prev_close"]).abs()
        df["tr"] = df[["tr1", "tr2", "tr3"]].max(axis=1)
        df["atr"] = df["tr"].rolling(window=14).mean()
        atr = float(df["atr"].iloc[-1])

        # 3. Get Last Candle Close (for confirmation)
        last_close = float(df["close"].iloc[-1])

        # 4. Get Current Price
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
    - current_price
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

        # Detect DI crossover on the latest candle
        prev_diff = prev_plus_di - prev_minus_di
        curr_diff = plus_di - minus_di

        di_crossed = False
        di_cross_type = None
        if prev_diff <= 0 and curr_diff > 0:
            di_crossed = True
            di_cross_type = "bullish"  # +DI crossed above -DI
        elif prev_diff >= 0 and curr_diff < 0:
            di_crossed = True
            di_cross_type = "bearish"  # -DI crossed above +DI

        last_candle_ts = str(df["timestamp"].iloc[-1])

        # 5. Current Price from ticker
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
            "current_price": current_price,
            "last_candle_ts": last_candle_ts,
        }

    except Exception as exc:
        print(f"Error in get_all_indicators for {symbol}: {exc}")
        return None

async def get_open_position_side(symbol: str) -> Optional[str]:
    try:
        # Offload blocking API call to a thread
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
        # Offload blocking API call to a thread
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
        max_qty = float(lot_filter["maxMktOrderQty"])  # Use MARKET order max (lower than limit order max)
        qty_step = float(lot_filter["qtyStep"])
        
        # Also get min notional value if available
        min_notional = float(lot_filter.get("minNotionalValue", 5.0)) # Default to 5 USDT if not specified
        
        print(f"[DEBUG] {symbol} constraints → min_qty={min_qty}, max_qty={max_qty}, qty_step={qty_step}, min_notional={min_notional}")
        
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
        # 1. Check for manual quantity override
        if symbol in symbol_manual_quantities and symbol_manual_quantities[symbol] is not None:
            manual_qty = symbol_manual_quantities[symbol]
            # Respect instrument constraints: round to step and clamp to min
            min_qty, max_qty, qty_step, _ = await get_instrument_constraints(symbol)
            return round_qty_to_step(manual_qty, qty_step, min_qty)

        # 2. Fallback to balance-based calculation (20% risk)
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

        # Get current price for qty calculation
        _, _, _, current_price = await get_ichimoku_and_atr(symbol)
        min_qty, max_qty, qty_step, min_notional = await get_instrument_constraints(symbol)

        if current_price is None or current_price <= 0:
            return min_qty

        # Calculate quantity based on 20% risk
        risk_amount_usd = total_avail_usd * 0.20
        
        # Ensure risk amount meets minimum notional requirement
        risk_amount_usd = max(risk_amount_usd, min_notional + 1.0) # Add 1 USDT buffer
        
        raw_qty = risk_amount_usd / current_price
        
        # Adjust for min_qty and qty_step
        if qty_step > 0:
            adjusted_qty = max(min_qty, (raw_qty // qty_step) * qty_step)
        else:
            adjusted_qty = max(min_qty, raw_qty)
            
        # Final check: ensure adjusted_qty * current_price >= min_notional
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
    """
    try:
        # Offload blocking API call to a thread
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
                
                # Get constraints for max_qty
                min_qty, max_qty, qty_step, _ = await get_instrument_constraints(symbol)
                
                # Split close order if necessary
                remaining_qty = total_qty
                
                while remaining_qty > 0:
                    current_batch_qty = min(remaining_qty, max_qty)
                    current_batch_qty = round_qty_to_step(current_batch_qty, qty_step, min_qty)
                    
                    # Format qty for API using instrument's qtyStep precision
                    qty_str = format_qty_str(current_batch_qty, qty_step)
                    
                    # Offload blocking API call to a thread
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
                    
                    # Short delay between batches if multiple
                    if remaining_qty > 0:
                        await asyncio.sleep(0.5)
                        
                return total_qty
        return None
    except Exception as exc:
        print(f"Position close failed for {symbol}: {exc}")
        return None

async def place_trade(symbol: str, side: str = "Buy", manual_qty_override: Optional[float] = None) -> Optional[dict]:
    """
    Places a trade, optionally using a specific manual quantity override for reversals.
    """
    if manual_qty_override:
        total_qty = manual_qty_override
    else:
        total_qty = await get_trade_qty(symbol)
        
    try:
        # Get constraints for max_qty
        min_qty, max_qty, qty_step, _ = await get_instrument_constraints(symbol)
        
        # Round total_qty to valid step (important for manual overrides)
        total_qty = round_qty_to_step(total_qty, qty_step, min_qty)
        
        remaining_qty = total_qty
        successful_orders = []
        
        print(f"[DEBUG] {symbol} place_trade → total_qty={total_qty}, max_qty={max_qty}, qty_step={qty_step}")
        
        while remaining_qty > 0:
            current_batch_qty = min(remaining_qty, max_qty)
            current_batch_qty = round_qty_to_step(current_batch_qty, qty_step, min_qty)
            
            # Format qty for API using instrument's qtyStep precision
            qty_str = format_qty_str(current_batch_qty, qty_step)
            
            print(f"[DEBUG] {symbol} batch → qty_str={qty_str}, remaining_after={remaining_qty - current_batch_qty}")
            
            # Offload blocking API call to a thread
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
                # If any batch fails, return error but include what we successfully placed
                return {"error": order.get("retMsg"), "partial": len(successful_orders) > 0}
            
            successful_orders.append(order.get("result"))
            remaining_qty -= current_batch_qty
            
            # Short delay between batches to ensure sequential execution
            if remaining_qty > 0:
                await asyncio.sleep(0.5)
        
        # Return the last successful order result (or a summary)
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
    ema_20 = indicators["ema_20"]
    atr = indicators["atr"]
    plus_di = indicators["plus_di"]
    minus_di = indicators["minus_di"]
    di_crossed = indicators["di_crossed"]
    di_cross_type = indicators["di_cross_type"]
    last_candle_ts = indicators["last_candle_ts"]

    chat_id = context.job.chat_id if context.job else CHAT_ID

    # Determine market state relative to 20 EMA
    if current_price > ema_20:
        current_market_state = "ABOVE 20 EMA (Bullish)"
    elif current_price < ema_20:
        current_market_state = "BELOW 20 EMA (Bearish)"
    else:
        current_market_state = "AT 20 EMA (Neutral)"

    # ──────────────────────────────────────────────
    # 1. Handle Reversals (+DI/-DI Cross + 20 EMA confirmation)
    # ──────────────────────────────────────────────
    if current_open_trade_side is not None:
        last_acted_ts = symbol_last_di_cross_candle.get(symbol)

        if di_crossed and last_candle_ts != last_acted_ts:
            # +DI/-DI crossover detected on a new candle we haven't acted on yet

            if current_price < ema_20 and current_open_trade_side == "Buy":
                # DI cross + price below 20 EMA → Close Long, open Short
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"⚡ REVERSAL SIGNAL for {symbol}!\n"
                        f"+DI/-DI crossover detected ({di_cross_type})\n"
                        f"+DI: {plus_di:.2f} | -DI: {minus_di:.2f}\n"
                        f"Price ({current_price}) < 20 EMA ({ema_20:.6g})\n"
                        f"Closing Long → Opening Short"
                    )
                )

                closed_qty = await close_position(symbol, side_to_close="Buy")
                manual_override = None
                if symbol in symbol_manual_quantities and symbol_manual_quantities[symbol] is not None:
                    manual_override = closed_qty

                result = await place_trade(symbol, side="Sell", manual_qty_override=manual_override)
                if result and "error" not in result:
                    order_id = result.get("orderId", "—")
                    await context.bot.send_message(chat_id=chat_id, text=f"✅ Short order(s) placed for {symbol}\nLast Order ID: <code>{order_id}</code>", parse_mode="HTML")
                elif result and "error" in result:
                    await context.bot.send_message(chat_id=chat_id, text=f"❌ Failed to place Short for {symbol}: {result['error']}")

                symbol_trends[symbol] = "downward"
                symbol_open_trade_side[symbol] = "Sell"
                symbol_last_di_cross_candle[symbol] = last_candle_ts
                return

            elif current_price > ema_20 and current_open_trade_side == "Sell":
                # DI cross + price above 20 EMA → Close Short, open Long
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"⚡ REVERSAL SIGNAL for {symbol}!\n"
                        f"+DI/-DI crossover detected ({di_cross_type})\n"
                        f"+DI: {plus_di:.2f} | -DI: {minus_di:.2f}\n"
                        f"Price ({current_price}) > 20 EMA ({ema_20:.6g})\n"
                        f"Closing Short → Opening Long"
                    )
                )

                closed_qty = await close_position(symbol, side_to_close="Sell")
                manual_override = None
                if symbol in symbol_manual_quantities and symbol_manual_quantities[symbol] is not None:
                    manual_override = closed_qty

                result = await place_trade(symbol, side="Buy", manual_qty_override=manual_override)
                if result and "error" not in result:
                    order_id = result.get("orderId", "—")
                    await context.bot.send_message(chat_id=chat_id, text=f"✅ Long order(s) placed for {symbol}\nLast Order ID: <code>{order_id}</code>", parse_mode="HTML")
                elif result and "error" in result:
                    await context.bot.send_message(chat_id=chat_id, text=f"❌ Failed to place Long for {symbol}: {result['error']}")

                symbol_trends[symbol] = "upward"
                symbol_open_trade_side[symbol] = "Buy"
                symbol_last_di_cross_candle[symbol] = last_candle_ts
                return

    # ──────────────────────────────────────────────
    # 2. Initial Trade Logic (20 EMA based)
    #    Long only if price > 20 EMA
    #    Short only if price < 20 EMA
    # ──────────────────────────────────────────────
    if current_open_trade_side is None:
        new_trend = None
        if current_price > ema_20:
            new_trend = "upward"
        elif current_price < ema_20:
            new_trend = "downward"

        # Act if it's the first run (previous_trend is None) OR if the trend has flipped
        if new_trend and (previous_trend is None or new_trend != previous_trend):
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            msg = (
                f"{ts} — {symbol} trend detected: <b>{new_trend.upper()}</b>!\n"
                f"Price: {current_price} | 20 EMA: {ema_20:.6g}\n"
                f"+DI: {plus_di:.2f} | -DI: {minus_di:.2f}"
            )
            await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")

            if new_trend == "upward":
                result = await place_trade(symbol, side="Buy")
                side_text = "Long (Buy)"
            else:
                result = await place_trade(symbol, side="Sell")
                side_text = "Short (Sell)"

            if result and "error" not in result:
                order_id = result.get("orderId", "—")
                await context.bot.send_message(chat_id=chat_id, text=f"✅ {side_text} order(s) placed for {symbol}\nLast Order ID: <code>{order_id}</code>", parse_mode="HTML")
            elif result and "error" in result:
                await context.bot.send_message(chat_id=chat_id, text=f"❌ Failed to place {side_text} for {symbol}: {result['error']}")

            symbol_trends[symbol] = new_trend
            symbol_open_trade_side[symbol] = await get_open_position_side(symbol)

    # ──────────────────────────────────────────────
    # 3. 30-Minute Status Report
    # ──────────────────────────────────────────────
    current_time = time.time()
    last_report_time = context.job.data.get("last_report_time", 0)

    if current_time - last_report_time >= 1800:
        trade_status = "No open trade"
        if current_open_trade_side:
            trade_status = f"✅ {current_open_trade_side} trade open for {symbol}"
        else:
            trade_status = f"❌ No open trade for {symbol}"

        report_msg = (
            f"Still monitoring {symbol}\n"
            f"• Entry Filter: Price vs 20 EMA (30m)\n"
            f"• Reversal Trigger: +DI/-DI crossover (30m) + 20 EMA confirmation\n"
            f"• Monitoring every 1 second\n"
            f"• 20 EMA: {ema_20:.6g} | +DI: {plus_di:.2f} | -DI: {minus_di:.2f}\n"
            f"• {symbol} state: {current_market_state}\n"
            f"• {trade_status}"
        )
        if symbol in symbol_manual_quantities and symbol_manual_quantities[symbol] is not None:
            report_msg += f"\n• Manual Quantity: {symbol_manual_quantities[symbol]}"
        else:
            report_msg += "\n• Auto-sizing: 20% Balance"

        await context.bot.send_message(chat_id=chat_id, text=report_msg)
        context.job.data["last_report_time"] = current_time

async def add_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage:\n/add_symbol <symbol> (Auto-size 20% balance)\n/add_symbol <symbol> <quantity> (Manual quantity + INSTANT TRADE)")
        return

    symbol = context.args[0].upper()
    manual_qty = None
    
    # Check for manual quantity argument
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
    if symbol not in monitored_symbols:
        monitored_symbols.append(symbol)

    chat_id = update.effective_chat.id
    
    # Handle instant trade for manual quantity
    if manual_qty:
        await update.message.reply_text(f"🚀 INSTANT TRADE for {symbol} with quantity {manual_qty}...")
        
        # Determine trend using 20 EMA
        indicators = await get_all_indicators(symbol)
        if indicators and indicators["current_price"]:
            current_price = indicators["current_price"]
            ema_20 = indicators["ema_20"]
            plus_di = indicators["plus_di"]
            minus_di = indicators["minus_di"]
            
            # Long if price > 20 EMA, Short if price < 20 EMA
            side = "Buy" if current_price > ema_20 else "Sell"
            side_text = "Long (Buy)" if side == "Buy" else "Short (Sell)"
            
            await update.message.reply_text(
                f"📊 {symbol}: Price={current_price} | 20 EMA={ema_20:.6g}\n"
                f"+DI: {plus_di:.2f} | -DI: {minus_di:.2f}\n"
                f"Direction: {side_text}"
            )
            
            result = await place_trade(symbol, side=side, manual_qty_override=manual_qty)
            
            if result and "error" not in result:
                order_id = result.get("orderId", "—")
                await update.message.reply_text(f"✅ Instant {side_text} order(s) placed for {symbol}\nLast Order ID: <code>{order_id}</code>", parse_mode="HTML")
                symbol_trends[symbol] = "upward" if side == "Buy" else "downward"
            elif result and "error" in result:
                await update.message.reply_text(f"❌ Failed to place instant {side_text} for {symbol}: {result['error']}")
        else:
            await update.message.reply_text(f"⚠️ Could not fetch indicators for {symbol}. Monitoring will take over.")

    qty_info = f"Manual Quantity: {manual_qty} (Instant Trade Executed)" if manual_qty else "Auto-sizing: 20% of available USDT balance"
    
    await update.message.reply_text(
        f"Monitoring {symbol}!\n"
        f"• {qty_info}\n"
        "• Entry: Price vs 20 EMA (30m chart)\n"
        "• Reversals: +DI/-DI crossover (30m) + Price vs 20 EMA\n"
        "• Monitoring every 1 second\n"
        "• Status report every 30 minutes"
    )

    if context.job_queue:
        # Check if already running to avoid duplicate jobs
        existing_jobs = context.job_queue.get_jobs_by_name(f"{symbol}_monitor")
        if not existing_jobs:
            context.job_queue.run_repeating(
                callback=monitor_symbol,
                interval=1,
                first=1,
                chat_id=chat_id,
                name=f"{symbol}_monitor",
                data={"symbol": symbol, "last_report_time": time.time()},
            )

async def remove_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
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

    if context.job_queue:
        jobs = context.job_queue.get_jobs_by_name(f"{symbol}_monitor")
        for job in jobs:
            job.schedule_removal()

    await update.message.reply_text(f"Stopped monitoring {symbol}.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Bot ready!\n"
        "Commands:\n"
        "1. /start - Display this message\n"
        "2. /add_symbol <symbol> - Start monitoring with auto-sizing (20% balance)\n"
        "3. /add_symbol <symbol> <quantity> - INSTANT TRADE + Manual quantity monitoring\n"
        "4. /remove_symbol <symbol> - Stop monitoring a symbol\n\n"
        "Strategy:\n"
        "• Initial Entry: Long if Price > 20 EMA (30m), Short if Price < 20 EMA (30m)\n"
        "• Reversals: Only when +DI/-DI crossover occurs (30m), then direction based on Price vs 20 EMA\n\n"
        "Examples:\n"
        "• /add_symbol BTCUSDT\n"
        "• /add_symbol ETHUSDT 0.5\n"
        "Manual trades place an order instantly and use the same quantity on reversals."
    )

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    print(f"Exception while handling update: {context.error}")
    if CHAT_ID:
        await context.bot.send_message(
            chat_id=CHAT_ID,
            text=f"⚠️ Bot encountered an error:\n<code>{context.error}</code>",
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
