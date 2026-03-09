import os
import time
import hmac
import hashlib
import logging
import asyncio
import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pybit.unified_trading import HTTP
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
import random

# Load environment variables
load_dotenv()

# Configuration
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

# Trading Parameters
TIMEFRAME = "30"  # 30 minutes
CHECK_INTERVAL = 1  # Check every 1 second
REPORT_INTERVAL = 1800  # Report every 30 minutes (1800 seconds)
ATR_PERIOD = 14
ATR_MULTIPLIER = 1.5  # As requested in v12
RISK_PERCENT = 0.20  # 20% of balance per trade
MAX_RETRIES = 3 # Maximum retries for "too large" quantity errors

# Global State
monitored_symbols = set()
symbol_trends = {}  # symbol -> "upward" or "downward"
symbol_open_trade_side = {} # symbol -> "Buy", "Sell", or None
symbol_last_report_time = {} # symbol -> timestamp of last report

# Motivational Quotes
MOTIVATIONAL_QUOTES = [
    "Trading is not about being right, it\\'s about making money when you\\'re right and losing little when you\\'re wrong.",
    "The goal of a successful trader is to make the best trades. Money is secondary.",
    "Don\\'t focus on the money; focus on the execution. If you execute well, the money will follow.",
    "Discipline is doing what needs to be done, even if you don\\'t want to do it.",
    "The market is a device for transferring money from the impatient to the patient.",
    "Your edge is in your discipline, not your indicators.",
    "A loss is only a failure if you didn\\'t follow your plan.",
    "Consistency is the hallmark of a professional trader.",
    "The best traders have no ego. They accept when they are wrong and move on.",
    "Risk management is the only way to survive in the long run.",
    "Plan your trade and trade your plan.",
    "The market doesn\\'t care about your feelings. Stay objective.",
    "Patience is the key to catching the big moves.",
    "Don\\'t overtrade. Quality over quantity.",
    "Every trade is just one of the next thousand trades. Don\\'t get attached.",
    "Learn to love the process, not just the results.",
    "The trend is your friend until the end when it bends.",
    "Successful trading is 90% psychology and 10% strategy.",
    "Keep your losses small and your winners big.",
    "Trading is a marathon, not a sprint. Stay focused."
]

# Logging setup
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Bybit Session
bybit_session = HTTP(
    testnet=True,  # Set to False for mainnet
    api_key=BYBIT_API_KEY,
    api_secret=BYBIT_API_SECRET,
    recv_window=5000,
)

# TEMPORARY DEBUG: Check time difference
try:
    resp = bybit_session.get_server_time()
    server_ms = int(resp["result"]["timeNano"])
    local_ms = int(time.time() * 1_000_000_000)
    diff = abs(local_ms - server_ms) / 1_000_000_000
    logger.info(f"Time diff: {diff:.2f} seconds")
    logger.info("Time sync looks good" if diff <= 5 else "WARNING: Possible drift/latency (>5 seconds)")
except Exception as e:
    logger.error(f"Time sync debug failed: {e}")

def get_ichimoku_conversion_line(df):
    """Calculate Ichimoku Conversion Line (Tenkan-sen): (9-period high + 9-period low) / 2"""
    nine_period_high = df['high'].rolling(window=9).max()
    nine_period_low = df['low'].rolling(window=9).min()
    return (nine_period_high + nine_period_low) / 2

def get_atr(df, period=14):
    """Calculate Average True Range (ATR)"""
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    return true_range.ewm(span=period, adjust=False).mean() # Using EMA for ATR as is common

async def get_data(symbol):
    """Fetch kline data and calculate indicators"""
    try:
        resp = await asyncio.to_thread(
            bybit_session.get_kline,
            category="linear",
            symbol=symbol,
            interval=TIMEFRAME,
            limit=100
        )
        if resp["retCode"] != 0:
            logger.error(f"Error fetching data for {symbol}: {resp['retMsg']}")
            return None

        data = resp["result"]["list"]
        df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"])
        df[["open", "high", "low", "close"]] = df[["open", "high", "low", "close"]].apply(pd.to_numeric)
        df = df.iloc[::-1].reset_index(drop=True)

        df['conversion_line'] = get_ichimoku_conversion_line(df)
        df['atr'] = get_atr(df, ATR_PERIOD)
        
        return df
    except Exception as e:
        logger.error(f"Exception in get_data for {symbol}: {e}")
        return None

async def get_instrument_constraints(symbol):
    """Fetch min/max order quantity and step for a symbol"""
    try:
        resp = await asyncio.to_thread(
            bybit_session.get_instruments_info,
            category="linear",
            symbol=symbol
        )
        if resp["retCode"] == 0:
            instr = resp["result"]["list"][0]
            lot_size = instr["lotSizeFilter"]
            return {
                "min_qty": float(lot_size["minOrderQty"]),
                "max_qty": float(lot_size["maxOrderQty"]),
                "qty_step": float(lot_size["qtyStep"]),
                "min_notional": float(lot_size.get("minNotionalValue", 5.0))
            }
        else:
            logger.error(f"Error getting instrument info for {symbol}: {resp['retMsg']}")
            return None
    except Exception as e:
        logger.error(f"Exception in get_instrument_constraints for {symbol}: {e}")
    return None

async def get_trade_qty(symbol, price, current_max_qty_limit=None):
    """Calculate trade quantity based on 20% of balance and instrument constraints.
    current_max_qty_limit can be passed from a previous error message to enforce a stricter limit.
    """
    logger.debug(f"[get_trade_qty] Starting quantity calculation for {symbol} at price {price}")
    try:
        constraints = await get_instrument_constraints(symbol)
        if not constraints:
            logger.error(f"[get_trade_qty] Failed to get instrument constraints for {symbol}. Returning None.")
            return None

        balance_resp = await asyncio.to_thread(
            bybit_session.get_wallet_balance,
            accountType="UNIFIED"
        )
        if balance_resp["retCode"] != 0:
            logger.error(f"[get_trade_qty] Failed to get wallet balance: {balance_resp['retMsg']}. Returning None.")
            return None
            
        balance = 0
        for coin in balance_resp["result"]["list"][0]["coin"]:
            if coin["coin"] == "USDT":
                balance = float(coin["walletBalance"])
                break
        
        if balance <= 0:
            logger.warning(f"[get_trade_qty] Insufficient balance for trading {symbol}. Balance: {balance}. Returning None.")
            return None

        min_qty = constraints["min_qty"]
        max_qty_from_api = constraints["max_qty"]
        qty_step = constraints["qty_step"]
        min_notional = constraints["min_notional"]

        effective_max_qty = min(max_qty_from_api, current_max_qty_limit) if current_max_qty_limit else max_qty_from_api
        logger.debug(f"[get_trade_qty] {symbol}: Effective Max Qty={effective_max_qty} (API={max_qty_from_api}, Error={current_max_qty_limit})")

        usdt_to_risk = balance * RISK_PERCENT
        logger.debug(f"[get_trade_qty] {symbol}: Balance={balance}, Risk_Percent={RISK_PERCENT}, USDT_to_Risk={usdt_to_risk}, Price={price}")
        raw_qty = usdt_to_risk / price
        logger.debug(f"[get_trade_qty] {symbol}: Raw Qty={raw_qty}")
        
        # 1. Apply effective_max_qty clamp first
        qty = min(raw_qty, effective_max_qty)
        logger.debug(f"[get_trade_qty] {symbol}: Qty after initial effective_max_qty clamp={qty}")
        
        # 2. Adjust for qty_step (round down to avoid exceeding limits)
        if qty_step == 0:
            logger.error(f"[get_trade_qty] {symbol}: qty_step is zero, cannot calculate quantity. Returning None.")
            return None

        qty = (np.floor(qty / qty_step)) * qty_step
        logger.debug(f"[get_trade_qty] {symbol}: Qty after qty_step adjustment={qty}")
        
        # 3. Ensure it meets min_notional (minimum USDT value)
        logger.debug(f"[get_trade_qty] {symbol}: Min Notional={min_notional}, Current Qty*Price={qty*price}")
        if (qty * price) < min_notional:
            logger.debug(f"[get_trade_qty] {symbol}: Qty*Price < Min Notional. Adjusting...")
            min_qty_for_notional = min_notional / price
            qty_from_notional = (np.ceil(min_qty_for_notional / qty_step)) * qty_step
            logger.debug(f"[get_trade_qty] {symbol}: Qty calculated from min_notional={qty_from_notional}")

            if qty_from_notional > qty:
                qty = min(qty_from_notional, effective_max_qty)
                logger.debug(f"[get_trade_qty] {symbol}: Qty adjusted for min_notional and re-clamped against effective_max_qty={qty}")
            else:
                logger.debug(f"[get_trade_qty] {symbol}: Current Qty {qty} already meets or exceeds min_notional requirements.")
            
        # 4. Ensure it meets min_qty
        logger.debug(f"[get_trade_qty] {symbol}: Min Qty={min_qty}")
        qty = max(qty, min_qty)
        logger.debug(f"[get_trade_qty] {symbol}: Qty after min_qty adjustment={qty}")
        
        # AGGRESSIVE FINAL CLAMP: Ensure we never exceed effective_max_qty even after adjustments
        # Use a tiny safety margin (0.999999) to avoid floating point precision issues
        qty = min(qty, effective_max_qty * 0.999999)
        logger.debug(f"[get_trade_qty] {symbol}: Qty after aggressive final clamp={qty}")
        
        # Final validation: ensure quantity is positive, meets min_qty, and does not exceed effective_max_qty
        if qty <= 0:
            logger.error(f"[get_trade_qty] {symbol}: Final calculated quantity {qty} is zero or negative. Returning None.")
            return None
        
        if qty < min_qty:
            logger.error(f"[get_trade_qty] {symbol}: Final calculated quantity {qty} is less than min_qty {min_qty}. Cannot place trade. Returning None.")
            return None

        return f"{qty:.{len(str(qty_step).split('.')[-1])}f}".rstrip('0').rstrip('.') if '.' in str(qty_step) else str(int(qty))

    except Exception as exc:
        logger.error(f"[get_trade_qty] Exception calculating qty for {symbol}: {exc}")
        return None

async def place_trade(context: ContextTypes.DEFAULT_TYPE, symbol: str, side: str, price: float, current_max_qty_limit=None):
    """Places a market order for the given symbol and side."""
    chat_id = CHAT_ID
    for attempt in range(MAX_RETRIES):
        try:
            qty_str = await get_trade_qty(symbol, price, current_max_qty_limit)
            if qty_str is None:
                await context.bot.send_message(chat_id=chat_id, text=f"❌ Failed to place {side} for {symbol}: Could not calculate valid quantity.")
                return False
            
            qty = float(qty_str)
            if qty <= 0:
                await context.bot.send_message(chat_id=chat_id, text=f"❌ Failed to place {side} for {symbol}: Calculated quantity is zero or negative ({qty_str}).")
                return False

            logger.info(f"Attempt {attempt + 1}: Placing {side} order for {qty_str} {symbol} at price {price}")
            order_resp = await asyncio.to_thread(
                bybit_session.place_order,
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Market",
                qty=qty_str,
            )

            if order_resp["retCode"] == 0:
                order_id = order_resp["result"]["orderId"]
                await context.bot.send_message(chat_id=chat_id, text=f"✅ Placed {side} order for {symbol}! Qty: {qty_str}, Order ID: {order_id}")
                symbol_open_trade_side[symbol] = side
                return True
            else:
                error_msg = order_resp["retMsg"]
                logger.error(f"Failed to place {side} for {symbol}: {error_msg}")
                await context.bot.send_message(chat_id=chat_id, text=f"❌ Failed to place {side} for {symbol}: {error_msg}")
                
                # Dynamic Error-Response Clamping and Retry
                if "exceeds maximum limit allowed" in error_msg:
                    match = re.search(r'max_qty:(\d+)', error_msg)
                    if match:
                        bybit_reported_max_qty_raw = int(match.group(1))
                        # Heuristic: Divide by 10^8 to convert from Bybit's internal large integer unit to actual contract units
                        bybit_reported_max_qty_scaled = bybit_reported_max_qty_raw / (10**8)
                        logger.warning(f"Bybit reported max_qty: {bybit_reported_max_qty_raw}, scaled to {bybit_reported_max_qty_scaled}")
                        if bybit_reported_max_qty_scaled > 0:
                            current_max_qty_limit = bybit_reported_max_qty_scaled * 0.99 # Use 99% of reported max as safety
                            logger.info(f"Retrying with stricter max_qty limit: {current_max_qty_limit}")
                            # Recursive call with stricter limit
                            return await place_trade(context, symbol, side, price, current_max_qty_limit)

                return False # If not a max_qty error or cannot parse, don't retry

        except Exception as e:
            logger.error(f"Exception placing {side} order for {symbol}: {e}")
            await context.bot.send_message(chat_id=chat_id, text=f"❌ Exception placing {side} order for {symbol}: {e}")
            return False
    return False # All retries failed

async def close_position(context: ContextTypes.DEFAULT_TYPE, symbol: str, side_to_close: str) -> bool:
    """Closes an open position for the given symbol and side."""
    chat_id = CHAT_ID
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
                qty = pos["size"]

                order = await asyncio.to_thread(
                    bybit_session.place_order,
                    category="linear",
                    symbol=symbol,
                    side=close_side,
                    orderType="Market",
                    qty=qty,
                    reduceOnly=True
                )

                if order["retCode"] == 0:
                    order_id = order["result"]["orderId"]
                    await context.bot.send_message(chat_id=chat_id, text=f"✅ Closed {side_to_close} position for {symbol}! Qty: {qty}, Order ID: {order_id}")
                    symbol_open_trade_side[symbol] = None
                    return True
                else:
                    error_msg = order["retMsg"]
                    logger.error(f"Failed to close {side_to_close} position for {symbol}: {error_msg}")
                    await context.bot.send_message(chat_id=chat_id, text=f"❌ Failed to close {side_to_close} position for {symbol}: {error_msg}")
                    return False
        return False # No position to close
    except Exception as e:
        logger.error(f"Exception closing {side_to_close} position for {symbol}: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"❌ Exception closing {side_to_close} position for {symbol}: {e}")
        return False

async def monitor_symbol(context: ContextTypes.DEFAULT_TYPE):
    """Monitors a symbol for trend changes and places/reverses trades."""
    symbol = context.job.data
    chat_id = CHAT_ID

    df = await get_data(symbol)
    if df is None or df.empty:
        logger.warning(f"No data to monitor for {symbol}.")
        return

    last_candle_close = float(df['close'].iloc[-1])
    conversion_line = float(df['conversion_line'].iloc[-1])
    atr = float(df['atr'].iloc[-1])
    current_price = float(df['close'].iloc[-1]) # Using close for current price for consistency with candle data

    upper_buffer_line = conversion_line + (atr * ATR_MULTIPLIER)
    lower_buffer_line = conversion_line - (atr * ATR_MULTIPLIER)

    current_trend = None
    if current_price > upper_buffer_line:
        current_trend = "UPWARD"
    elif current_price < lower_buffer_line:
        current_trend = "DOWNWARD"
    else:
        current_trend = "NEUTRAL"

    previous_trend = symbol_trends.get(symbol)
    open_trade_side = symbol_open_trade_side.get(symbol)

    # Initial trend detection / Trade placement for new symbols
    if previous_trend is None:
        if current_trend == "UPWARD":
            logger.info(f"{symbol} initial trend detected: UPWARD! Price: {current_price:.2f} | Conversion Line: {conversion_line:.2f}")
            await context.bot.send_message(chat_id=chat_id, text=f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — {symbol} initial trend detected: UPWARD!\nPrice: {current_price:.2f} | Conversion Line: {conversion_line:.2f}")
            await place_trade(context, symbol, "Buy", current_price)
        elif current_trend == "DOWNWARD":
            logger.info(f"{symbol} initial trend detected: DOWNWARD! Price: {current_price:.2f} | Conversion Line: {conversion_line:.2f}")
            await context.bot.send_message(chat_id=chat_id, text=f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — {symbol} initial trend detected: DOWNWARD!\nPrice: {current_price:.2f} | Conversion Line: {conversion_line:.2f}")
            await place_trade(context, symbol, "Sell", current_price)
        symbol_trends[symbol] = current_trend
        symbol_last_report_time[symbol] = time.time() # Initialize report time
        return

    # Trend reversal logic
    if open_trade_side == "Buy": # Currently in a Long trade
        if current_price < lower_buffer_line: # Price broke below lower buffer -> reverse to Short
            logger.info(f"{symbol} trend changed from UPWARD to DOWNWARD (reversal)! Price: {current_price:.2f} | Conversion Line: {conversion_line:.2f}")
            await context.bot.send_message(chat_id=chat_id, text=f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — {symbol} trend changed from UPWARD to DOWNWARD (reversal)!\nPrice: {current_price:.2f} | Conversion Line: {conversion_line:.2f}")
            if await close_position(context, symbol, "Buy"):
                await place_trade(context, symbol, "Sell", current_price)
            symbol_trends[symbol] = "DOWNWARD"

    elif open_trade_side == "Sell": # Currently in a Short trade
        if current_price > upper_buffer_line: # Price broke above upper buffer -> reverse to Long
            logger.info(f"{symbol} trend changed from DOWNWARD to UPWARD (reversal)! Price: {current_price:.2f} | Conversion Line: {conversion_line:.2f}")
            await context.bot.send_message(chat_id=chat_id, text=f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — {symbol} trend changed from DOWNWARD to UPWARD (reversal)!\nPrice: {current_price:.2f} | Conversion Line: {conversion_line:.2f}")
            if await close_position(context, symbol, "Sell"):
                await place_trade(context, symbol, "Buy", current_price)
            symbol_trends[symbol] = "UPWARD"

    # Update trend if no open trade and trend is clear
    elif open_trade_side is None and current_trend != "NEUTRAL" and current_trend != previous_trend:
        if current_trend == "UPWARD":
            logger.info(f"{symbol} trend changed to UPWARD! Price: {current_price:.2f} | Conversion Line: {conversion_line:.2f}")
            await context.bot.send_message(chat_id=chat_id, text=f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — {symbol} trend changed to UPWARD!\nPrice: {current_price:.2f} | Conversion Line: {conversion_line:.2f}")
            await place_trade(context, symbol, "Buy", current_price)
        elif current_trend == "DOWNWARD":
            logger.info(f"{symbol} trend changed to DOWNWARD! Price: {current_price:.2f} | Conversion Line: {conversion_line:.2f}")
            await context.bot.send_message(chat_id=chat_id, text=f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — {symbol} trend changed to DOWNWARD!\nPrice: {current_price:.2f} | Conversion Line: {conversion_line:.2f}")
            await place_trade(context, symbol, "Sell", current_price)
        symbol_trends[symbol] = current_trend

    # 30-minute status report
    if time.time() - symbol_last_report_time.get(symbol, 0) >= REPORT_INTERVAL:
        status_message = f"Still monitoring {symbol}\n"
        status_message += f"• Timeframe: {TIMEFRAME} minutes Ichimoku Conversion Line\n"
        status_message += f"• Filter: ATR-based Buffer Zone ({ATR_MULTIPLIER}x ATR)\n"
        status_message += f"• Monitoring every {CHECK_INTERVAL} second\n"
        status_message += f"• {symbol} current market state: {current_trend}\n"
        if open_trade_side:
            status_message += f"• ✅ Open {open_trade_side} trade for {symbol}"
        else:
            status_message += f"• ❌ No open trade for {symbol}"
        status_message += f"\n• Current Price: {current_price:.4f} | Conversion Line: {conversion_line:.4f} | ATR: {atr:.4f}"
        status_message += f"\n• Upper Buffer: {upper_buffer_line:.4f} | Lower Buffer: {lower_buffer_line:.4f}"
        await context.bot.send_message(chat_id=chat_id, text=status_message)
        symbol_last_report_time[symbol] = time.time()

    symbol_trends[symbol] = current_trend # Always update current trend

async def add_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Adds a symbol to the monitoring list."""
    chat_id = update.effective_chat.id
    symbol = context.args[0].upper() if context.args else None

    if not symbol:
        await update.message.reply_text("Please specify a symbol, e.g., /add_symbol BTCUSDT")
        return

    if symbol in monitored_symbols:
        await update.message.reply_text(f"{symbol} is already being monitored.")
        return

    monitored_symbols.add(symbol)
    symbol_trends[symbol] = None  # Initialize trend as None
    symbol_open_trade_side[symbol] = None # Initialize open trade as None
    symbol_last_report_time[symbol] = 0 # Initialize last report time to trigger immediate report

    # Schedule the monitoring job for this symbol
    context.job_queue.run_repeating(monitor_symbol, interval=CHECK_INTERVAL, first=0, data=symbol, name=f"monitor_{symbol}")

    await update.message.reply_text(
        f"Added {symbol} to monitoring!\n"
        f"• Timeframe: {TIMEFRAME} minutes Ichimoku Conversion Line\n"
        f"• Filter: ATR-based Buffer Zone ({ATR_MULTIPLIER}x ATR)\n"
        f"• Monitoring every {CHECK_INTERVAL} second\n"
        f"• Status report every {REPORT_INTERVAL // 60} minutes\n"
        f"Initial trade based on Conversion Line. Reversals use full ATR Buffer."
    )
    logger.info(f"Added {symbol} to monitoring.")

async def remove_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Removes a symbol from the monitoring list."""
    chat_id = update.effective_chat.id
    symbol = context.args[0].upper() if context.args else None

    if not symbol:
        await update.message.reply_text("Please specify a symbol to remove, e.g., /remove_symbol BTCUSDT")
        return

    if symbol not in monitored_symbols:
        await update.message.reply_text(f"{symbol} is not currently being monitored.")
        return

    monitored_symbols.remove(symbol)
    symbol_trends.pop(symbol, None)
    symbol_open_trade_side.pop(symbol, None)
    symbol_last_report_time.pop(symbol, None)

    # Remove the scheduled job for this symbol
    current_jobs = context.job_queue.get_jobs_by_name(f"monitor_{symbol}")
    for job in current_jobs:
        job.schedule_removal()

    await update.message.reply_text(f"Removed {symbol} from monitoring.")
    logger.info(f"Removed {symbol} from monitoring.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a welcome message when the command /start is issued."""
    await update.message.reply_text("Hello! I am your Bybit Ichimoku Trading Bot. Use /add_symbol <SYMBOL> to start monitoring a coin.")

async def send_motivational_quote_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    quote = random.choice(MOTIVATIONAL_QUOTES)
    await context.bot.send_message(chat_id=CHAT_ID, text=quote)

async def main_async():
    """Asynchronous main function to run the bot."""
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("add_symbol", add_symbol))
    application.add_handler(CommandHandler("remove_symbol", remove_symbol))

    # Schedule motivational quotes every 12 hours
    application.job_queue.run_repeating(send_motivational_quote_job, interval=timedelta(hours=12), first=0, name="motivational_quote")

    logger.info("Bot started. Waiting for commands...")
    await application.run_polling()

def main():
    """Start the bot."""
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
