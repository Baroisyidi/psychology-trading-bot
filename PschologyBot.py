import os
import time
import hmac
import hashlib
import logging
import asyncio
import pandas as pd
import numpy as np
import re
import random
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pybit.unified_trading import HTTP
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

# Load environment variables
load_dotenv()

# Configuration
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
CHAT_ID_STR = os.getenv("CHAT_ID")

# Trading Parameters
TIMEFRAME = "30"  # 30 minutes
CHECK_INTERVAL = 1  # Check every 1 second
REPORT_INTERVAL = 1800  # Report every 30 minutes
ATR_PERIOD = 14
ATR_MULTIPLIER = 1.5
RISK_PERCENT = 0.20  # 20% of balance per trade
MAX_RETRIES = 3

# Global State
monitored_symbols = set()
symbol_trends = {}
symbol_open_trade_side = {}
symbol_last_report_time = {}

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

# Bybit Session (Configured for Demo Account)
try:
    bybit_session = HTTP(
        demo=True,  # Set to True for Bybit Demo account
        api_key=BYBIT_API_KEY,
        api_secret=BYBIT_API_SECRET,
        recv_window=5000,
    )
except Exception as e:
    logger.error(f"Failed to initialize Bybit session: {e}")
    bybit_session = None

def get_ichimoku_conversion_line(df):
    nine_period_high = df['high'].rolling(window=9).max()
    nine_period_low = df['low'].rolling(window=9).min()
    return (nine_period_high + nine_period_low) / 2

def get_atr(df, period=14):
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    return true_range.ewm(span=period, adjust=False).mean()

async def get_data(symbol):
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
        return None
    except Exception as e:
        logger.error(f"Exception in get_instrument_constraints for {symbol}: {e}")
        return None

async def get_trade_qty(symbol, price, current_max_qty_limit=None):
    try:
        constraints = await get_instrument_constraints(symbol)
        if not constraints: return None

        balance_resp = await asyncio.to_thread(
            bybit_session.get_wallet_balance,
            accountType="UNIFIED"
        )
        if balance_resp["retCode"] != 0: return None
            
        balance = 0
        for coin in balance_resp["result"]["list"][0]["coin"]:
            if coin["coin"] == "USDT":
                balance = float(coin["walletBalance"])
                break
        
        if balance <= 0: return None

        min_qty = constraints["min_qty"]
        max_qty_from_api = constraints["max_qty"]
        qty_step = constraints["qty_step"]
        min_notional = constraints["min_notional"]

        effective_max_qty = min(max_qty_from_api, current_max_qty_limit) if current_max_qty_limit else max_qty_from_api
        raw_qty = (balance * RISK_PERCENT) / price
        qty = min(raw_qty, effective_max_qty)
        
        if qty_step == 0: return None
        qty = (np.floor(qty / qty_step)) * qty_step
        
        if (qty * price) < min_notional:
            qty = min((np.ceil((min_notional / price) / qty_step)) * qty_step, effective_max_qty)
            
        qty = max(qty, min_qty)
        qty = min(qty, effective_max_qty * 0.9999)
        
        if qty < min_qty: return None

        precision = len(str(qty_step).split('.')[-1]) if '.' in str(qty_step) else 0
        return f"{qty:.{precision}f}"
    except Exception as e:
        logger.error(f"Exception in get_trade_qty: {e}")
        return None

async def place_trade(context, symbol, side, price, chat_id, current_max_qty_limit=None):
    for attempt in range(MAX_RETRIES):
        try:
            qty_str = await get_trade_qty(symbol, price, current_max_qty_limit)
            if not qty_str:
                await context.bot.send_message(chat_id=chat_id, text=f"❌ Could not calculate valid quantity for {symbol}.")
                return None

            resp = await asyncio.to_thread(
                bybit_session.place_order,
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Market",
                qty=qty_str,
                timeInForce="GTC"
            )

            if resp["retCode"] == 0:
                order_id = resp["result"]["orderId"]
                await context.bot.send_message(chat_id=chat_id, text=f"✅ {side} order placed for {symbol}!\nQty: {qty_str}, Order ID: {order_id}")
                symbol_open_trade_side[symbol] = side
                return order_id
            else:
                error_msg = resp.get("retMsg", "Unknown error")
                await context.bot.send_message(chat_id=chat_id, text=f"❌ Failed to place {side} for {symbol}: {error_msg}")
                if "exceeds maximum limit allowed" in error_msg:
                    match = re.search(r'max_qty:(\d+)', error_msg)
                    if match:
                        current_max_qty_limit = (int(match.group(1)) / 10**8) * 0.99
                        continue
                return None
        except Exception as e:
            logger.error(f"Exception in place_trade: {e}")
            return None
    return None

async def close_position(context, symbol, side_to_close, chat_id):
    try:
        resp = await asyncio.to_thread(bybit_session.get_positions, category="linear", symbol=symbol)
        for pos in resp["result"]["list"]:
            if pos["side"] == side_to_close and float(pos["size"]) > 0:
                close_side = "Sell" if side_to_close == "Buy" else "Buy"
                order = await asyncio.to_thread(
                    bybit_session.place_order,
                    category="linear",
                    symbol=symbol,
                    side=close_side,
                    orderType="Market",
                    qty=pos["size"],
                    reduceOnly=True
                )
                if order["retCode"] == 0:
                    await context.bot.send_message(chat_id=chat_id, text=f"✅ Closed {side_to_close} position for {symbol}.")
                    symbol_open_trade_side[symbol] = None
                    return True
        return False
    except Exception as e:
        logger.error(f"Exception in close_position: {e}")
        return False

async def monitor_symbol(context: ContextTypes.DEFAULT_TYPE):
    symbol = context.job.data
    chat_id = int(os.getenv("CHAT_ID"))
    df = await get_data(symbol)
    if df is None or df.empty: return

    current_price = float(df['close'].iloc[-1])
    conversion_line = float(df['conversion_line'].iloc[-1])
    atr = float(df['atr'].iloc[-1])
    
    upper_buffer = conversion_line + (atr * ATR_MULTIPLIER)
    lower_buffer = conversion_line - (atr * ATR_MULTIPLIER)

    new_trend = "UPWARD" if current_price > upper_buffer else "DOWNWARD" if current_price < lower_buffer else "NEUTRAL"
    prev_trend = symbol_trends.get(symbol)
    open_side = symbol_open_trade_side.get(symbol)

    if prev_trend is None:
        if new_trend != "NEUTRAL":
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            await context.bot.send_message(chat_id=chat_id, text=f"{ts} — {symbol} initial trend: {new_trend}!\nPrice: {current_price:.2f} | Conv: {conversion_line:.2f}")
            await place_trade(context, symbol, "Buy" if new_trend == "UPWARD" else "Sell", current_price, chat_id)
        symbol_trends[symbol] = new_trend
        symbol_last_report_time[symbol] = time.time()
        return

    if open_side == "Buy" and current_price < lower_buffer:
        if await close_position(context, symbol, "Buy", chat_id):
            await place_trade(context, symbol, "Sell", current_price, chat_id)
        symbol_trends[symbol] = "DOWNWARD"
    elif open_side == "Sell" and current_price > upper_buffer:
        if await close_position(context, symbol, "Sell", chat_id):
            await place_trade(context, symbol, "Buy", current_price, chat_id)
        symbol_trends[symbol] = "UPWARD"
    elif open_side is None and new_trend != "NEUTRAL" and new_trend != prev_trend:
        await place_trade(context, symbol, "Buy" if new_trend == "UPWARD" else "Sell", current_price, chat_id)
        symbol_trends[symbol] = new_trend

    if time.time() - symbol_last_report_time.get(symbol, 0) >= REPORT_INTERVAL:
        msg = f"Monitoring {symbol}\nPrice: {current_price:.2f} | Conv: {conversion_line:.2f}\nTrend: {new_trend} | Position: {open_side or 'None'}"
        await context.bot.send_message(chat_id=chat_id, text=msg)
        symbol_last_report_time[symbol] = time.time()

async def add_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args: return
    symbol = context.args[0].upper()
    if symbol in monitored_symbols: return
    monitored_symbols.add(symbol)
    symbol_trends[symbol] = None
    symbol_open_trade_side[symbol] = None
    symbol_last_report_time[symbol] = 0
    context.job_queue.run_repeating(monitor_symbol, interval=CHECK_INTERVAL, first=0, data=symbol, name=f"monitor_{symbol}")
    await update.message.reply_text(f"Added {symbol} to monitoring.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Bot started. Use /add_symbol <SYMBOL>.")

async def send_quote(context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=int(os.getenv("CHAT_ID")), text=random.choice(MOTIVATIONAL_QUOTES))

async def main_async():
    if not TELEGRAM_TOKEN:
        logger.error("TELEGRAM_TOKEN is missing!")
        return
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("add_symbol", add_symbol))
    application.job_queue.run_repeating(send_quote, interval=timedelta(hours=12), first=0)
    logger.info("Bot is running...")
    await application.run_polling()

if __name__ == "__main__":
    try:
        asyncio.run(main_async())
    except Exception as e:
        logger.error(f"Fatal error: {e}")
