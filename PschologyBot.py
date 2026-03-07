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
    "Trading is not about being right, it's about making money when you're right and losing little when you're wrong.",
    "The goal of a successful trader is to make the best trades. Money is secondary.",
    "Don't focus on the money; focus on the execution. If you execute well, the money will follow.",
    "Discipline is doing what needs to be done, even if you don't want to do it.",
    "The market is a device for transferring money from the impatient to the patient.",
    "Your edge is in your discipline, not your indicators.",
    "A loss is only a failure if you didn't follow your plan.",
    "Consistency is the hallmark of a professional trader.",
    "The best traders have no ego. They accept when they are wrong and move on.",
    "Risk management is the only way to survive in the long run.",
    "Plan your trade and trade your plan.",
    "The market doesn't care about your feelings. Stay objective.",
    "Patience is the key to catching the big moves.",
    "Don't overtrade. Quality over quantity.",
    "Every trade is just one of the next thousand trades. Don't get attached.",
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

async def get_trade_qty(symbol, price):
    """Calculate trade quantity based on 20% of balance and instrument constraints"""
    try:
        constraints = await get_instrument_constraints(symbol)
        if not constraints:
            return None

        balance_resp = await asyncio.to_thread(
            bybit_session.get_wallet_balance,
            accountType="UNIFIED"
        )
        if balance_resp["retCode"] != 0:
            return None
            
        # Get available balance (USDT)
        balance = 0
        for coin in balance_resp["result"]["list"][0]["coin"]:
            if coin["coin"] == "USDT":
                balance = float(coin["walletBalance"])
                break
        
        if balance <= 0:
            logger.warning("Insufficient balance for trading.")
            return None

        # Calculate raw quantity based on risk
        usdt_to_risk = balance * RISK_PERCENT
        raw_qty = usdt_to_risk / price
        
        # 1. Apply absolute max_qty clamp first
        max_qty = constraints["max_qty"]
        qty = min(raw_qty, max_qty)
        
        # 2. Adjust for qty_step (round down to avoid exceeding limits)
        step = constraints["qty_step"]
        qty = (qty // step) * step
        
        # 3. Ensure it meets min_notional (minimum USDT value)
        min_notional = constraints["min_notional"]
        if (qty * price) < min_notional:
            qty = (min_notional / price)
            qty = (qty // step + 1) * step # Round up to meet minimum
            
        # 4. Final safety checks
        qty = max(qty, constraints["min_qty"])
        
        # AGGRESSIVE FINAL CLAMP: Ensure we never exceed max_qty even after adjustments
        # Use a tiny safety margin (0.999999) to avoid floating point precision issues
        qty = min(qty, max_qty * 0.999999)
        
        # Format to correct precision based on step
        precision = 0
        if step < 1:
            precision = len(str(step).split(".")[1])
        
        return f"{qty:.{precision}f}"
    except Exception as e:
        logger.error(f"Error calculating quantity for {symbol}: {e}")
        return None

async def get_open_position(symbol):
    """Check if there's an open position for the symbol"""
    try:
        resp = await asyncio.to_thread(
            bybit_session.get_positions,
            category="linear",
            symbol=symbol
        )
        if resp["retCode"] == 0 and resp["result"]["list"]:
            pos = resp["result"]["list"][0]
            size = float(pos["size"])
            if size > 0:
                return pos["side"] # "Buy" or "Sell"
    except Exception as e:
        logger.error(f"Error checking position for {symbol}: {e}")
    return None

async def close_position(symbol):
    """Close any open position for the symbol"""
    try:
        pos_side = await get_open_position(symbol)
        if pos_side:
            resp = await asyncio.to_thread(
                bybit_session.get_positions,
                category="linear",
                symbol=symbol
            )
            if resp["retCode"] == 0 and resp["result"]["list"]:
                pos = resp["result"]["list"][0]
                qty = pos["size"]
                side = "Sell" if pos_side == "Buy" else "Buy"
                
                order = await asyncio.to_thread(
                    bybit_session.place_order,
                    category="linear",
                    symbol=symbol,
                    side=side,
                    orderType="Market",
                    qty=qty,
                    reduceOnly=True,
                )
                return order
        return None
    except Exception as e:
        logger.error(f"Error closing position for {symbol}: {e}")
        return None

async def place_trade(symbol, side, retry_count=0):
    """Place a market order with dynamic error-response clamping and retry"""
    try:
        # Get current price for quantity calculation
        ticker = await asyncio.to_thread(bybit_session.get_tickers, category="linear", symbol=symbol)
        if ticker["retCode"] != 0:
            return {"error": f"Ticker error: {ticker['retMsg']}"}
        price = float(ticker["result"]["list"][0]["lastPrice"])
        
        qty = await get_trade_qty(symbol, price)
        if not qty:
            return {"error": "Could not calculate valid quantity."}
            
        order = await asyncio.to_thread(
            bybit_session.place_order,
            category="linear",
            symbol=symbol,
            side=side,
            orderType="Market",
            qty=qty,
        )
        
        if order.get("retCode") != 0:
            error_msg = order.get("retMsg", "Unknown error")
            
            # SELF-HEALING: Check for "exceeds maximum limit allowed" error
            if "exceeds maximum limit allowed" in error_msg and retry_count < MAX_RETRIES:
                logger.warning(f"Detected 'too large' error for {symbol}. Attempting to parse real max_qty and retry...")
                
                # Extract the actual max_qty from the error message using regex
                # Example: "order_qty:45663100000000 > max_qty:9510000000000"
                match = re.search(r"max_qty:(\d+)", error_msg)
                if match:
                    real_max_qty_raw = int(match.group(1))
                    
                    # Bybit error messages often use internal integer units. 
                    # We need to scale this back to the contract count.
                    # We'll use the ratio of our requested qty to the order_qty in the error.
                    qty_match = re.search(r"order_qty:(\d+)", error_msg)
                    if qty_match:
                        order_qty_raw = int(qty_match.group(1))
                        ratio = float(qty) / order_qty_raw
                        real_max_qty = real_max_qty_raw * ratio
                        
                        # Apply a small safety margin and format
                        constraints = await get_instrument_constraints(symbol)
                        step = constraints["qty_step"] if constraints else 0.1
                        precision = len(str(step).split(".")[1]) if "." in str(step) else 0
                        
                        new_qty = f"{(real_max_qty * 0.99):.{precision}f}"
                        logger.info(f"Retrying {symbol} {side} with corrected quantity: {new_qty}")
                        
                        # Recursive retry with corrected quantity
                        return await place_trade_with_fixed_qty(symbol, side, new_qty)
            
            return {"error": f"{error_msg} (ErrCode: {order.get('retCode')})"}
            
        return order["result"]
    except Exception as e:
        logger.error(f"Exception in place_trade for {symbol}: {e}")
        return {"error": str(e)}

async def place_trade_with_fixed_qty(symbol, side, qty):
    """Helper to place a trade with a specific, pre-calculated quantity"""
    try:
        order = await asyncio.to_thread(
            bybit_session.place_order,
            category="linear",
            symbol=symbol,
            side=side,
            orderType="Market",
            qty=qty,
        )
        if order.get("retCode") != 0:
            return {"error": f"{order.get('retMsg')} (ErrCode: {order.get('retCode')})"}
        return order["result"]
    except Exception as e:
        return {"error": str(e)}

async def monitor_symbol(context: ContextTypes.DEFAULT_TYPE):
    """Main monitoring loop for each symbol"""
    job = context.job
    symbol = job.data["symbol"]
    chat_id = job.data["chat_id"]

    df = await get_data(symbol)
    if df is None or len(df) < 20:
        return

    latest = df.iloc[-1]
    price = latest['close']
    conversion_line = latest['conversion_line']
    atr = latest['atr']
    
    upper_buffer = conversion_line + (ATR_MULTIPLIER * atr)
    lower_buffer = conversion_line - (ATR_MULTIPLIER * atr)

    # Check for trend change or initial trend
    current_trend = None
    if price > upper_buffer:
        current_trend = "upward"
    elif price < lower_buffer:
        current_trend = "downward"
    
    # Special case for initial trade: use conversion line directly
    if symbol not in symbol_trends:
        initial_trend = "upward" if price > conversion_line else "downward"
        symbol_trends[symbol] = initial_trend
        msg = f"<b>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — {symbol} initial trend detected: {initial_trend.upper()}!</b>\nPrice: {price} | Conversion Line: {conversion_line}"
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
        
        # Place initial trade
        if initial_trend == "upward":
            result = await place_trade(symbol, "Buy")
            if result and "orderId" in result:
                order_id = result.get("orderId", "—")
                await context.bot.send_message(chat_id=chat_id, text=f"✅ Long order placed for {symbol}\nOrder ID: <code>{order_id}</code>", parse_mode="HTML")
            elif result and "error" in result:
                await context.bot.send_message(chat_id=chat_id, text=f"❌ Failed to place Long for {symbol}: {result['error']}", parse_mode="HTML")
            symbol_open_trade_side[symbol] = "Buy"
        else:
            result = await place_trade(symbol, "Sell")
            if result and "orderId" in result:
                order_id = result.get("orderId", "—")
                await context.bot.send_message(chat_id=chat_id, text=f"✅ Short order placed for {symbol}\nOrder ID: <code>{order_id}</code>", parse_mode="HTML")
            elif result and "error" in result:
                await context.bot.send_message(chat_id=chat_id, text=f"❌ Failed to place Short for {symbol}: {result['error']}", parse_mode="HTML")
            symbol_open_trade_side[symbol] = "Sell"
        return

    # Buffer-to-Buffer Reversal Logic
    previous_trend = symbol_trends.get(symbol)
    open_side = symbol_open_trade_side.get(symbol)

    if open_side == "Buy" and price < lower_buffer:
        # Reverse Long to Short
        await close_position(symbol)
        msg = f"<b>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — {symbol} trend REVERSED to DOWNWARD!</b>\nPrice: {price} | Lower Buffer: {lower_buffer:.4f}"
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
        
        result = await place_trade(symbol, "Sell")
        if result and "orderId" in result:
            order_id = result.get("orderId", "—")
            await context.bot.send_message(chat_id=chat_id, text=f"✅ Short reversal placed for {symbol}\nOrder ID: <code>{order_id}</code>", parse_mode="HTML")
        elif result and "error" in result:
            await context.bot.send_message(chat_id=chat_id, text=f"❌ Failed to place Short reversal for {symbol}: {result['error']}", parse_mode="HTML")
        
        symbol_trends[symbol] = "downward"
        symbol_open_trade_side[symbol] = "Sell"

    elif open_side == "Sell" and price > upper_buffer:
        # Reverse Short to Long
        await close_position(symbol)
        msg = f"<b>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} — {symbol} trend REVERSED to UPWARD!</b>\nPrice: {price} | Upper Buffer: {upper_buffer:.4f}"
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
        
        result = await place_trade(symbol, "Buy")
        if result and "orderId" in result:
            order_id = result.get("orderId", "—")
            await context.bot.send_message(chat_id=chat_id, text=f"✅ Long reversal placed for {symbol}\nOrder ID: <code>{order_id}</code>", parse_mode="HTML")
        elif result and "error" in result:
            await context.bot.send_message(chat_id=chat_id, text=f"❌ Failed to place Long reversal for {symbol}: {result['error']}", parse_mode="HTML")
            
        symbol_trends[symbol] = "upward"
        symbol_open_trade_side[symbol] = "Buy"

    # Periodic Status Report (Every 30 minutes)
    now = time.time()
    last_report = symbol_last_report_time.get(symbol, 0)
    if now - last_report >= REPORT_INTERVAL:
        open_side = await get_open_position(symbol)
        symbol_open_trade_side[symbol] = open_side
        
        status_msg = (
            f"📊 <b>Status Report: {symbol}</b>\n"
            f"• Time: {datetime.now().strftime('%H:%M:%S')}\n"
            f"• Price: {price}\n"
            f"• Conversion Line: {conversion_line:.4f}\n"
            f"• ATR Buffer (1.5x): {ATR_MULTIPLIER * atr:.4f}\n"
            f"• Trend: {symbol_trends.get(symbol, 'UNKNOWN').upper()}\n"
            f"• Open Trade: {open_side if open_side else 'None'}\n"
            f"• Monitoring: Active (1s interval)"
        )
        await context.bot.send_message(chat_id=chat_id, text=status_msg, parse_mode="HTML")
        symbol_last_report_time[symbol] = now

async def send_motivational_quote(context: ContextTypes.DEFAULT_TYPE):
    """Send a random motivational quote to the chat"""
    quote = random.choice(MOTIVATIONAL_QUOTES)
    msg = f"💡 <b>Trading Discipline</b>\n\n<i>\"{quote}\"</i>"
    await context.bot.send_message(chat_id=CHAT_ID, text=msg, parse_mode="HTML")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start command handler"""
    await update.message.reply_text(
        "🚀 Bybit Ichimoku Bot v19 Started!\n"
        "Use /add_symbol <SYMBOL> to start monitoring.\n"
        "Example: /add_symbol BTCUSDT"
    )

async def add_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Add a symbol to monitor"""
    if not context.args:
        await update.message.reply_text("Please provide a symbol. Example: /add_symbol BTCUSDT")
        return

    symbol = context.args[0].upper()
    if symbol in monitored_symbols:
        await update.message.reply_text(f"{symbol} is already being monitored.")
        return

    monitored_symbols.add(symbol)
    chat_id = update.effective_chat.id
    
    # Initialize report time
    symbol_last_report_time[symbol] = time.time()

    # Add repeating job for 1-second monitoring
    context.job_queue.run_repeating(
        monitor_symbol,
        interval=CHECK_INTERVAL,
        first=1,
        name=f"{symbol}_monitor",
        data={"symbol": symbol, "chat_id": chat_id}
    )

    await update.message.reply_text(
        f"Added {symbol} to monitoring!\n"
        f"• Timeframe: 30 minutes Ichimoku Conversion Line\n"
        f"• Filter: ATR-based Buffer Zone (1.5x ATR)\n"
        f"• Monitoring every 1 second\n"
        f"• Status report every 30 minutes\n"
        f"Initial trade based on Conversion Line. Reversals use full ATR Buffer."
    )

async def remove_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Remove a symbol from monitoring"""
    if not context.args:
        await update.message.reply_text("Please provide a symbol. Example: /remove_symbol BTCUSDT")
        return

    symbol = context.args[0].upper()
    if symbol not in monitored_symbols:
        await update.message.reply_text(f"{symbol} is not being monitored.")
        return

    monitored_symbols.remove(symbol)
    if symbol in symbol_trends: del symbol_trends[symbol]
    if symbol in symbol_open_trade_side: del symbol_open_trade_side[symbol]
    if symbol in symbol_last_report_time: del symbol_last_report_time[symbol]

    # Remove the job
    current_jobs = context.job_queue.get_jobs_by_name(f"{symbol}_monitor")
    for job in current_jobs:
        job.schedule_removal()

    await update.message.reply_text(f"Stopped monitoring {symbol}.")

async def main():
    """Main entry point"""
    # Check time sync
    try:
        resp = await asyncio.to_thread(bybit_session.get_server_time)
        server_ms = int(resp["result"]["timeNano"])
        local_ms = int(time.time() * 1_000_000_000)
        diff = abs(local_ms - server_ms) / 1_000_000_000
        logger.info(f"Time diff: {diff:.2f} seconds")
    except Exception as e:
        logger.error(f"Time sync check failed: {e}")

    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    # Handlers
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("add_symbol", add_symbol))
    app.add_handler(CommandHandler("remove_symbol", remove_symbol))

    # Schedule motivational quotes every 12 hours
    app.job_queue.run_repeating(send_motivational_quote, interval=43200, first=10)

    logger.info("Bot is running...")
    await app.initialize()
    await app.start()
    await app.updater.start_polling()
    
    # Keep running
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
