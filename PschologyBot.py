import os
import time
import hmac
import hashlib
import logging
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pybit.unified_trading import HTTP
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
import random
import re

# Load environment variables
load_dotenv()

# Configuration
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
CHAT_ID = os.getenv("CHAT_ID")

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
    "Trading is not about being right, it\'s about making money when you\'re right and losing little when you\'re wrong.",
    "The goal of a successful trader is to make the best trades. Money is secondary.",
    "Don\'t focus on the money; focus on the execution. If you execute well, the money will follow.",
    "Discipline is doing what needs to be done, even if you don\'t want to do it.",
    "The market is a device for transferring money from the impatient to the patient.",
    "Your edge is in your discipline, not your indicators.",
    "A loss is only a failure if you didn\'t follow your plan.",
    "Consistency is the hallmark of a professional trader.",
    "The best traders have no ego. They accept when they are wrong and move on.",
    "Risk management is the only way to survive in the long run.",
    "Plan your trade and trade your plan.",
    "The market doesn\'t care about your feelings. Stay objective.",
    "Patience is the key to catching the big moves.",
    "Don\'t overtrade. Quality over quantity.",
    "Every trade is just one of the next thousand trades. Don\'t get attached.",
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
    return true_range.rolling(window=period).mean()

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
    except Exception as e:
        logger.error(f"Error getting constraints for {symbol}: {e}")
    return None

async def get_trade_qty(symbol, price, current_max_qty_limit=None):
    """Calculate trade quantity based on 20% of balance and instrument constraints.
    current_max_qty_limit can be passed from a previous error message to enforce a stricter limit.
    """
    try:
        constraints = await get_instrument_constraints(symbol)
        if not constraints:
            logger.error(f"[get_trade_qty] Failed to get instrument constraints for {symbol}")
            return None

        balance_resp = await asyncio.to_thread(
            bybit_session.get_wallet_balance,
            accountType="UNIFIED"
        )
        if balance_resp["retCode"] != 0:
            logger.error(f"[get_trade_qty] Failed to get wallet balance: {balance_resp["retMsg"]}")
            return None
            
        # Get available balance (USDT)
        balance = 0
        for coin in balance_resp["result"]["list"][0]["coin"]:
            if coin["coin"] == "USDT":
                balance = float(coin["walletBalance"])
                break
        
        if balance <= 0:
            logger.warning(f"[get_trade_qty] Insufficient balance for trading {symbol}. Balance: {balance}")
            return None

        # Extract constraints
        min_qty = constraints["min_qty"]
        max_qty_from_api = constraints["max_qty"]
        qty_step = constraints["qty_step"]
        min_notional = constraints["min_notional"]

        # Use the stricter max_qty if provided from an error message
        effective_max_qty = min(max_qty_from_api, current_max_qty_limit) if current_max_qty_limit else max_qty_from_api
        logger.debug(f"[get_trade_qty] {symbol}: Effective Max Qty={effective_max_qty} (API={max_qty_from_api}, Error={current_max_qty_limit})")

        # Calculate raw quantity based on risk
        usdt_to_risk = balance * RISK_PERCENT
        logger.debug(f"[get_trade_qty] {symbol}: Balance={balance}, Risk_Percent={RISK_PERCENT}, USDT_to_Risk={usdt_to_risk}, Price={price}")
        raw_qty = usdt_to_risk / price
        logger.debug(f"[get_trade_qty] {symbol}: Raw Qty={raw_qty}")
        
        # 1. Apply effective_max_qty clamp first
        qty = min(raw_qty, effective_max_qty)
        logger.debug(f"[get_trade_qty] {symbol}: Qty after initial effective_max_qty clamp={qty}")
        
        # 2. Adjust for qty_step (round down to avoid exceeding limits)
        # Ensure qty_step is not zero to prevent division by zero
        if qty_step == 0:
            logger.error(f"[get_trade_qty] {symbol}: qty_step is zero, cannot calculate quantity. Returning None.")
            return None

        qty = (np.floor(qty / qty_step)) * qty_step
        logger.debug(f"[get_trade_qty] {symbol}: Qty after qty_step adjustment={qty}")
        
        # 3. Ensure it meets min_notional (minimum USDT value)
        logger.debug(f"[get_trade_qty] {symbol}: Min Notional={min_notional}, Current Qty*Price={qty*price}")
        if (qty * price) < min_notional:
            logger.debug(f"[get_trade_qty] {symbol}: Qty*Price < Min Notional. Adjusting...")
            # Calculate min_qty needed to meet min_notional
            min_qty_for_notional = min_notional / price
            # Round up to the nearest step
            qty_from_notional = (np.ceil(min_qty_for_notional / qty_step)) * qty_step
            logger.debug(f"[get_trade_qty] {symbol}: Qty calculated from min_notional={qty_from_notional}")

            # If the quantity derived from min_notional is greater than our current qty (which was clamped by effective_max_qty),
            # we must use the min_notional derived quantity, but re-clamp it against effective_max_qty.
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
        
        # If calculated qty is less than min_qty, it means we cannot place a valid trade with current balance/constraints
        if qty < min_qty:
            logger.error(f"[get_trade_qty] {symbol}: Final calculated quantity {qty} is less than min_qty {min_qty}. Cannot place trade. Returning None.")
            return None

        # Convert to string with correct precision for Bybit API
        precision = 0
        if qty_step < 1:
            precision = len(str(qty_step).split(".")[1])
        
        final_qty_str = f"{qty:.{precision}f}"
        logger.debug(f"[get_trade_qty] {symbol}: Final Qty String={final_qty_str}")
        return final_qty_str

    except Exception as e:
        logger.error(f"[get_trade_qty] Exception for {symbol}: {e}")
        return None

async def get_open_trade_side(symbol):
    """Get the side of the currently open trade for a symbol"""
    try:
        resp = await asyncio.to_thread(
            bybit_session.get_positions,
            category="linear",
            symbol=symbol
        )
        if resp["retCode"] == 0 and resp["result"]["list"]:
            position = resp["result"]["list"][0]
            if float(position["size"]) > 0:
                return position["side"]
        return None
    except Exception as e:
        logger.error(f"Error getting open trade side for {symbol}: {e}")
        return None

async def close_trade(symbol, side, context):
    """Close an open trade"""
    try:
        logger.info(f"Attempting to close {side} trade for {symbol}")
        # For closing, side is opposite of open trade
        close_side = "Sell" if side == "Buy" else "Buy"
        
        # Get current position size
        resp = await asyncio.to_thread(
            bybit_session.get_positions,
            category="linear",
            symbol=symbol
        )
        position_size = 0
        if resp["retCode"] == 0 and resp["result"]["list"]:
            position_size = float(resp["result"]["list"][0]["size"])

        if position_size > 0:
            resp = await asyncio.to_thread(
                bybit_session.place_order,
                category="linear",
                symbol=symbol,
                side=close_side,
                orderType="Market",
                qty=str(position_size),
                timeInForce="GTC",
                reduceOnly=True  # Ensure it only reduces position
            )

            if resp["retCode"] == 0:
                logger.info(f"Successfully closed {side} trade for {symbol}")
                await context.bot.send_message(chat_id=CHAT_ID, text=f"✅ Closed {side} trade for {symbol}.")
                return True
            else:
                error_msg = resp.get("retMsg", "Unknown error")
                logger.error(f"Failed to close {side} trade for {symbol}: {error_msg}")
                await context.bot.send_message(chat_id=CHAT_ID, text=f"❌ Failed to close {side} trade for {symbol}: {error_msg}")
                return False
        else:
            logger.info(f"No open {side} position to close for {symbol}.")
            return True # Nothing to close

    except Exception as e:
        logger.error(f"Exception closing {side} trade for {symbol}: {e}")
        await context.bot.send_message(chat_id=CHAT_ID, text=f"❌ Exception closing {side} trade for {symbol}: {e}")
        return False

async def place_trade(symbol, side, context, df, retry_count=0, initial_qty_str=None):
    """Place a market order with dynamic quantity adjustment and retry for 'too large' errors"""
    price = float(df.iloc[-1]["close"])
    
    # Get instrument constraints to pass max_qty to get_trade_qty
    constraints = await get_instrument_constraints(symbol)
    max_qty_from_api = constraints["max_qty"] if constraints else None

    qty_str = initial_qty_str if initial_qty_str else await get_trade_qty(symbol, price, current_max_qty_limit=max_qty_from_api)

    if not qty_str:
        await context.bot.send_message(chat_id=CHAT_ID, text=f"❌ Failed to place {side} for {symbol}: Could not calculate valid quantity.")
        return False

    logger.info(f"Attempting to place {side} order for {symbol} with quantity {qty_str} (Retry: {retry_count})")
    
    try:
        resp = await asyncio.to_thread(
            bybit_session.place_order,
            category="linear",
            symbol=symbol,
            side=side,
            orderType="Market",
            qty=qty_str,
            timeInForce="GTC",
            isLeverage=1,
            orderFilter="Order"
        )

        if resp["retCode"] == 0:
            logger.info(f"Successfully placed {side} order for {symbol} with quantity {qty_str}")
            return True
        else:
            error_msg = resp.get("retMsg", "Unknown error")
            logger.error(f"[place_trade] Failed to place {side} order for {symbol}: {error_msg}")
            await context.bot.send_message(chat_id=CHAT_ID, text=f"❌ Failed to place {side} for {symbol}: {error_msg}")

            # Dynamic Error-Response Clamping and Retry
            if "exceeds maximum limit allowed" in error_msg and retry_count < MAX_RETRIES:
                logger.warning(f"[place_trade] Attempting to parse max_qty from error message for {symbol}")
                match = re.search(r'order_qty:(\d+) > max_qty:(\d+)', error_msg)
                if match:
                    bybit_reported_order_qty_raw = float(match.group(1))
                    bybit_reported_max_qty_raw = float(match.group(2))
                    
                    # Determine the scaling factor by comparing our sent qty with the reported order_qty_raw
                    sent_qty_float = float(qty_str)
                    
                    scaling_factor = 1.0
                    if sent_qty_float > 0.00000001: # Avoid division by zero
                        scaling_factor = bybit_reported_order_qty_raw / sent_qty_float
                    
                    # Convert bybit_reported_max_qty_raw to our units using the determined scaling factor
                    effective_max_qty_from_error = bybit_reported_max_qty_raw / scaling_factor
                    
                    logger.info(f"[place_trade] Bybit reported order_qty_raw: {bybit_reported_order_qty_raw}, max_qty_raw: {bybit_reported_max_qty_raw}")
                    logger.info(f"[place_trade] Estimated scaling factor: {scaling_factor}, Effective max_qty from error (in our units): {effective_max_qty_from_error}")

                    # Recalculate quantity using the stricter max_qty from the error message
                    # Use 99% of reported max_qty for safety to avoid floating point issues
                    new_qty_str = await get_trade_qty(symbol, price, current_max_qty_limit=effective_max_qty_from_error * 0.99)

                    if new_qty_str:
                        logger.warning(f"[place_trade] Retrying {side} order for {symbol} with adjusted quantity: {new_qty_str}")
                        await context.bot.send_message(chat_id=CHAT_ID, text=f"⚠️ Retrying {side} order for {symbol} with adjusted quantity: {new_qty_str}")
                        return await place_trade(symbol, side, context, df, retry_count + 1, initial_qty_str=new_qty_str)
                    else:
                        logger.error(f"[place_trade] Failed to recalculate valid quantity for retry for {symbol}")
                        await context.bot.send_message(chat_id=CHAT_ID, text=f"❌ Failed to recalculate valid quantity for retry for {symbol}.")
                else:
                    logger.error(f"[place_trade] Could not parse max_qty from error message for {symbol}")
            return False

    except Exception as e:
        logger.error(f"[place_trade] Exception for {symbol}: {e}")
        await context.bot.send_message(chat_id=CHAT_ID, text=f"❌ Exception placing {side} for {symbol}: {e}")
        return False

async def monitor_symbol(context: ContextTypes.DEFAULT_TYPE):
    """Monitor symbol for trend changes and place trades"""
    symbol = context.job.data["symbol"]
    chat_id = context.job.data["chat_id"]

    try:
        df = await get_data(symbol)
        if df is None or df.empty or df['conversion_line'].isnull().any() or df['atr'].isnull().any():
            logger.warning(f"Skipping {symbol} due to insufficient data.")
            return

        current_price = float(df.iloc[-1]['close'])
        conversion_line = float(df.iloc[-1]['conversion_line'])
        atr = float(df.iloc[-1]['atr'])

        upper_buffer_line = conversion_line + (ATR_MULTIPLIER * atr)
        lower_buffer_line = conversion_line - (ATR_MULTIPLIER * atr)

        current_trend = symbol_trends.get(symbol)
        open_trade_side = symbol_open_trade_side.get(symbol)

        new_trend = None
        market_state = "NEUTRAL"

        if current_price > conversion_line:
            new_trend = "upward"
        elif current_price < conversion_line:
            new_trend = "downward"

        # Determine market state based on buffer zones
        if current_price > upper_buffer_line:
            market_state = "BULLISH"
        elif current_price < lower_buffer_line:
            market_state = "BEARISH"
        
        # Initial trade placement or trend change detection
        if current_trend is None: # First time monitoring this symbol
            if new_trend == "upward":
                logger.info(f"{symbol} initial trend detected: UPWARD! Price: {current_price:.2f} | Conversion Line: {conversion_line:.2f}")
                await context.bot.send_message(chat_id=chat_id, text=f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — {symbol} initial trend detected: UPWARD!\nPrice: {current_price:.2f} | Conversion Line: {conversion_line:.2f}")
                if await place_trade(symbol, "Buy", context, df):
                    symbol_open_trade_side[symbol] = "Buy"
                symbol_trends[symbol] = new_trend
            elif new_trend == "downward":
                logger.info(f"{symbol} initial trend detected: DOWNWARD! Price: {current_price:.2f} | Conversion Line: {conversion_line:.2f}")
                await context.bot.send_message(chat_id=chat_id, text=f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — {symbol} initial trend detected: DOWNWARD!\nPrice: {current_price:.2f} | Conversion Line: {conversion_line:.2f}")
                if await place_trade(symbol, "Sell", context, df):
                    symbol_open_trade_side[symbol] = "Sell"
                symbol_trends[symbol] = new_trend
            else:
                logger.info(f"{symbol} initial trend detected: NEUTRAL. Price: {current_price:.2f} | Conversion Line: {conversion_line:.2f}")
                await context.bot.send_message(chat_id=chat_id, text=f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — {symbol} initial trend detected: NEUTRAL.\nPrice: {current_price:.2f} | Conversion Line: {conversion_line:.2f}")
                symbol_trends[symbol] = "neutral" # Mark as neutral to avoid re-triggering initial detection

        elif new_trend and new_trend != current_trend:
            logger.info(f"{symbol} trend changed from {current_trend} to {new_trend}! Price: {current_price:.2f} | Conversion Line: {conversion_line:.2f}")
            await context.bot.send_message(chat_id=chat_id, text=f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — {symbol} trend changed to {new_trend.upper()}!\nPrice: {current_price:.2f} | Conversion Line: {conversion_line:.2f}")
            symbol_trends[symbol] = new_trend

            # Reversal logic: close current trade and open new one
            if open_trade_side == "Buy" and new_trend == "downward":
                # Only reverse if price breaks below lower buffer line
                if current_price < lower_buffer_line:
                    logger.info(f"Reversing Long to Short for {symbol} as price broke below lower buffer.")
                    if await close_trade(symbol, "Buy", context):
                        symbol_open_trade_side[symbol] = None
                        if await place_trade(symbol, "Sell", context, df):
                            symbol_open_trade_side[symbol] = "Sell"
                else:
                    logger.info(f"Long trade for {symbol} continues. Price {current_price:.2f} is above lower buffer {lower_buffer_line:.2f}.")

            elif open_trade_side == "Sell" and new_trend == "upward":
                # Only reverse if price breaks above upper buffer line
                if current_price > upper_buffer_line:
                    logger.info(f"Reversing Short to Long for {symbol} as price broke above upper buffer.")
                    if await close_trade(symbol, "Sell", context):
                        symbol_open_trade_side[symbol] = None
                        if await place_trade(symbol, "Buy", context, df):
                            symbol_open_trade_side[symbol] = "Buy"
                else:
                    logger.info(f"Short trade for {symbol} continues. Price {current_price:.2f} is below upper buffer {upper_buffer_line:.2f}.")

            elif open_trade_side is None:
                # No open trade, place new trade based on new trend
                if new_trend == "upward":
                    if await place_trade(symbol, "Buy", context, df):
                        symbol_open_trade_side[symbol] = "Buy"
                elif new_trend == "downward":
                    if await place_trade(symbol, "Sell", context, df):
                        symbol_open_trade_side[symbol] = "Sell"

        # Report status every REPORT_INTERVAL
        last_report_time = symbol_last_report_time.get(symbol, 0)
        if (time.time() - last_report_time) >= REPORT_INTERVAL:
            status_message = f"Still monitoring {symbol}\n"
            status_message += f"• Timeframe: {TIMEFRAME} minutes Ichimoku Conversion Line\n"
            status_message += f"• Filter: ATR-based Buffer Zone ({ATR_MULTIPLIER}x ATR)\n"
            status_message += f"• Monitoring every {CHECK_INTERVAL} second\n"
            status_message += f"• {symbol} current market state: {market_state}\n"
            status_message += f"• ATR Buffer: {atr * ATR_MULTIPLIER:.4f}\n"
            
            if open_trade_side:
                status_message += f"• ✅ Open {open_trade_side} trade for {symbol}"
            else:
                status_message += f"• ❌ No open trade for {symbol}"
            
            await context.bot.send_message(chat_id=chat_id, text=status_message)
            symbol_last_report_time[symbol] = time.time()

    except Exception as e:
        logger.error(f"Exception in monitor_symbol for {symbol}: {e}")
        await context.bot.send_message(chat_id=chat_id, text=f"⚠️ Error monitoring {symbol}: {e}")

async def send_motivational_quote(context: ContextTypes.DEFAULT_TYPE):
    """Send a random motivational quote"""
    quote = random.choice(MOTIVATIONAL_QUOTES)
    await context.bot.send_message(chat_id=CHAT_ID, text=f"💡 Trading Wisdom: {quote}")

async def add_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Adds a symbol to the monitoring list."""
    if not context.args:
        await update.message.reply_text("Usage: /add_symbol <SYMBOL> (e.g., /add_symbol BTCUSDT)")
        return

    symbol = context.args[0].upper()
    if symbol not in monitored_symbols:
        monitored_symbols.add(symbol)
        symbol_trends[symbol] = None # Initialize trend as None
        symbol_open_trade_side[symbol] = None # No open trade initially
        symbol_last_report_time[symbol] = 0 # Initialize last report time

        # Schedule the monitoring job for this symbol
        context.job_queue.run_repeating(monitor_symbol, interval=CHECK_INTERVAL, first=0, data={"symbol": symbol, "chat_id": update.effective_chat.id}, name=f"monitor_{symbol}")
        
        confirmation_message = f"Added {symbol} to monitoring!\n"
        confirmation_message += f"• Timeframe: {TIMEFRAME} minutes Ichimoku Conversion Line\n"
        confirmation_message += f"• Filter: ATR-based Buffer Zone ({ATR_MULTIPLIER}x ATR)\n"
        confirmation_message += f"• Monitoring every {CHECK_INTERVAL} second\n"
        confirmation_message += f"• Status report every {REPORT_INTERVAL // 60} minutes\n"
        confirmation_message += f"Initial trade based on Conversion Line. Reversals use full ATR Buffer."
        await update.message.reply_text(confirmation_message)
        logger.info(f"Added {symbol} to monitoring.")
    else:
        await update.message.reply_text(f"{symbol} is already being monitored.")

async def remove_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Removes a symbol from the monitoring list."""
    if not context.args:
        await update.message.reply_text("Usage: /remove_symbol <SYMBOL>")
        return

    symbol = context.args[0].upper()
    if symbol in monitored_symbols:
        monitored_symbols.remove(symbol)
        symbol_trends.pop(symbol, None)
        symbol_open_trade_side.pop(symbol, None)
        symbol_last_report_time.pop(symbol, None)

        # Remove the scheduled job
        current_jobs = context.job_queue.get_jobs_by_name(f"monitor_{symbol}")
        for job in current_jobs:
            job.schedule_removal()
        
        await update.message.reply_text(f"Removed {symbol} from monitoring.")
        logger.info(f"Removed {symbol} from monitoring.")
    else:
        await update.message.reply_text(f"{symbol} is not being monitored.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Sends a welcome message when the command /start is issued."""
    await update.message.reply_text("Hello! I am your Bybit Ichimoku Trading Bot. Use /add_symbol <SYMBOL> to start monitoring a coin.")

async def main():
    """Start the bot."""
    if not TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN is not set in environment variables.")
        return

    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("add_symbol", add_symbol))
    application.add_handler(CommandHandler("remove_symbol", remove_symbol))

    # Schedule motivational quotes every 12 hours
    if application.job_queue:
        application.job_queue.run_repeating(send_motivational_quote, interval=timedelta(hours=12), first=0, name="motivational_quote")

    logger.info("Bot is starting...")
    # Using run_polling in a way that is compatible with the library's internal loop management
    await application.initialize()
    await application.start()
    await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
    
    # Keep the bot running until interrupted
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        await application.stop()
        await application.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass

