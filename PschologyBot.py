
import os
import asyncio
import random
import time
import pandas as pd
import math
import re
import logging
from datetime import datetime
from typing import Optional, Tuple, Union
from dotenv import load_dotenv
from pybit.unified_trading import HTTP
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
)

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

# Configure logging
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ────────────────────────────────────────────────
#  Bybit demo session
# ────────────────────────────────────────────────
bybit_session = HTTP(
    demo=True,
    api_key=BYBIT_API_KEY,
    api_secret=BYBIT_API_SECRET,
    recv_window=5000
)

# Global state
symbol_trends: dict[str, Optional[str]] = {}
monitored_symbols: list[str] = []
symbol_open_trade_side: dict[str, Optional[str]] = {}

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
        df.loc[:, "high"] = pd.to_numeric(df["high"], errors="coerce")
        df.loc[:, "low"] = pd.to_numeric(df["low"], errors="coerce")
        df.loc[:, "close"] = pd.to_numeric(df["close"], errors="coerce")

        if df["close"].isna().any():
            raise ValueError("Invalid prices")

        nine_period_high = df["high"].rolling(window=9).max()
        nine_period_low = df["low"].rolling(window=9).min()
        df.loc[:, "tenkan_sen"] = (nine_period_high + nine_period_low) / 2
        conversion_line = float(df["tenkan_sen"].iloc[-1])

        df.loc[:, "prev_close"] = df["close"].shift(1)
        df.loc[:, "tr1"] = df["high"] - df["low"]
        df.loc[:, "tr2"] = (df["high"] - df["prev_close"]).abs()
        df.loc[:, "tr3"] = (df["low"] - df["prev_close"]).abs()
        df.loc[:, "tr"] = df[["tr1", "tr2", "tr3"]].max(axis=1)
        df.loc[:, "atr"] = df["tr"].rolling(window=14).mean()
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
        logger.error(f"Error in get_ichimoku_and_atr for {symbol}: {exc}")
        return None, None, None, None

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
        logger.error(f"Error getting open position for {symbol}: {e}")
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
        max_qty = float(lot_filter["maxOrderQty"])
        qty_step = float(lot_filter["qtyStep"])
        
        min_notional = float(lot_filter.get("minNotionalValue", 5.0))
        
        return min_qty, max_qty, qty_step, min_notional
    
    except Exception as e:
        logger.error(f"Failed to get constraints for {symbol}: {e}")
        return 0.001, 1000000.0, 0.001, 5.0

async def get_trade_qty(symbol: str, max_qty_override: Optional[float] = None) -> str:
    """Calculates the trade quantity based on 20% risk and instrument constraints."""
    try:
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
        total_avail_usd = float(account.get("totalAvailableBalance", "0"))

        _, _, _, current_price = await get_ichimoku_and_atr(symbol)
        min_qty, max_qty, qty_step, min_notional = await get_instrument_constraints(symbol)

        # Apply override if provided (from self-healing)
        if max_qty_override is not None:
            max_qty = min(max_qty, max_qty_override)
            logger.info(f"Using max_qty override for {symbol}: {max_qty}")

        if current_price is None or current_price <= 0:
            return f"{min_qty:.6f}".rstrip("0").rstrip(".")

        risk_amount_usd = total_avail_usd * 0.20
        risk_amount_usd = max(risk_amount_usd, min_notional + 1.0)
        
        raw_qty = risk_amount_usd / current_price
        
        # 1. ABSOLUTE CAP: Ensure raw_qty does not exceed max_qty
        raw_qty = min(raw_qty, max_qty)
        
        # 2. Adjust for qty_step
        if qty_step > 0:
            adjusted_qty = (raw_qty // qty_step) * qty_step
        else:
            adjusted_qty = raw_qty
            
        # 3. Final Clamp between min and max
        adjusted_qty = max(min_qty, min(max_qty, adjusted_qty))
            
        # 4. Final check: ensure adjusted_qty * current_price >= min_notional
        if adjusted_qty * current_price < min_notional:
            needed_qty = (min_notional / current_price) + qty_step
            if qty_step > 0:
                needed_qty = (needed_qty // qty_step) * qty_step
            adjusted_qty = min(needed_qty, max_qty)

        # Determine precision based on qty_step
        if qty_step > 0:
            precision = max(0, int(-math.log10(qty_step)))
        else:
            precision = 6 if adjusted_qty < 10 else 3
            
        # Final safety clamp
        safe_max_qty = max_qty * 0.999999
        if adjusted_qty > safe_max_qty:
            adjusted_qty = (safe_max_qty // qty_step) * qty_step if qty_step > 0 else safe_max_qty
            
        qty_str = f"{adjusted_qty:.{precision}f}".rstrip("0").rstrip(".")
        return qty_str
    except Exception as exc:
        logger.error(f"Failed to calculate qty for {symbol}: {exc}")
        min_qty, _, _, _ = await get_instrument_constraints(symbol)
        return f"{min_qty:.6f}".rstrip("0").rstrip(".")

async def place_trade(symbol: str, side: str = "Buy", max_qty_override: Optional[float] = None, is_retry: bool = False) -> Optional[dict]:
    qty = await get_trade_qty(symbol, max_qty_override=max_qty_override)
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
            error_msg = order.get("retMsg", "Unknown error")
            logger.error(f"Trade order rejected for {symbol} (qty={qty}): {error_msg}")
            
            # Self-healing for quantity limit errors
            # Matches formats: "The max quantity is 1000", "max_qty:1000", "exceeds maximum limit allowed"
            if not is_retry and ("exceeds maximum limit allowed" in error_msg or "too large" in error_msg):
                logger.warning(f"Detected quantity limit error for {symbol}. Attempting self-healing...")
                
                # Try to parse the max quantity from various error formats
                # Format 1: "... The max quantity is 1000 ..."
                # Format 2: "... max_qty:1000000000000 ..."
                match = re.search(r"(?:max_qty:|max quantity is\s+)(\d+\.?\d*)", error_msg)
                if match:
                    parsed_max_qty = float(match.group(1))
                    logger.info(f"Extracted max quantity from error: {parsed_max_qty}")
                    # Retry with the new limit
                    return await place_trade(symbol, side, max_qty_override=parsed_max_qty, is_retry=True)
                else:
                    logger.error("Could not parse max quantity from error message.")

            return {"error": error_msg}
        
        return order.get("result")
    except Exception as exc:
        logger.error(f"Trade placement failed for {symbol} (qty={qty}): {exc}")
        return {"error": str(exc)}

async def close_position(symbol: str, side_to_close: str = "Buy") -> Optional[dict]:
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
                    reduceOnly=True,
                )
                if order.get("retCode") != 0:
                    logger.error(f"Close order rejected for {symbol}: {order.get('retMsg')}")
                return order
        return None
    except Exception as exc:
        logger.error(f"Position close failed for {symbol}: {exc}")
        return None

async def monitor_symbol(context: ContextTypes.DEFAULT_TYPE) -> None:
    symbol = context.job.data["symbol"]
    previous_trend = symbol_trends.get(symbol, None)
    current_open_trade_side = await get_open_position_side(symbol)
    symbol_open_trade_side[symbol] = current_open_trade_side

    conversion_line, atr, last_close, current_price = await get_ichimoku_and_atr(symbol)

    if conversion_line is None or atr is None or current_price is None:
        return

    upper_buffer = conversion_line + (ATR_MULTIPLIER * atr)
    lower_buffer = conversion_line - (ATR_MULTIPLIER * atr)

    if current_price > upper_buffer:
        current_market_state = "upward"
    elif current_price < lower_buffer:
        current_market_state = "downward"
    else:
        current_market_state = "neutral"

    chat_id = context.job.chat_id if context.job else CHAT_ID

    # 1. Handle Reversals (Buffer-to-Buffer Logic)
    if current_open_trade_side == "Buy":
        if current_price < lower_buffer:
            await context.bot.send_message(chat_id=chat_id, text=f"⚡ INSTANT REVERSAL: {symbol} broke below the LOWER ATR Buffer Zone! Closing Long, opening Short.")
            await close_position(symbol, side_to_close="Buy")
            result = await place_trade(symbol, side="Sell")
            if result and "error" not in result:
                order_id = result.get("orderId", "—")
                await context.bot.send_message(chat_id=chat_id, text=f"✅ Short order placed for {symbol}\nOrder ID: <code>{order_id}</code>", parse_mode="HTML")
            elif result and "error" in result:
                await context.bot.send_message(chat_id=chat_id, text=f"❌ Failed to place Short for {symbol}: {result['error']}")
            
            symbol_trends[symbol] = "downward"
            symbol_open_trade_side[symbol] = "Sell"
            return
    elif current_open_trade_side == "Sell":
        if current_price > upper_buffer:
            await context.bot.send_message(chat_id=chat_id, text=f"⚡ INSTANT REVERSAL: {symbol} broke above the UPPER ATR Buffer Zone! Closing Short, opening Long.")
            await close_position(symbol, side_to_close="Sell")
            result = await place_trade(symbol, side="Buy")
            if result and "error" not in result:
                order_id = result.get("orderId", "—")
                await context.bot.send_message(chat_id=chat_id, text=f"✅ Long order placed for {symbol}\nOrder ID: <code>{order_id}</code>", parse_mode="HTML")
            elif result and "error" in result:
                await context.bot.send_message(chat_id=chat_id, text=f"❌ Failed to place Long for {symbol}: {result['error']}")
            
            symbol_trends[symbol] = "upward"
            symbol_open_trade_side[symbol] = "Buy"
            return

    # 2. Initial Trade Logic
    if current_open_trade_side is None:
        new_trend = None
        if current_price > conversion_line:
            new_trend = "upward"
        elif current_price < conversion_line:
            new_trend = "downward"
        
        if new_trend and (previous_trend is None or new_trend != previous_trend):
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            msg = (
                f"{ts} — {symbol} initial trend detected: <b>{new_trend.upper()}</b>!\n"
                f"Price: {current_price:.2f} | Conversion Line: {conversion_line:.2f}"
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
                await context.bot.send_message(chat_id=chat_id, text=f"✅ {side_text} order placed for {symbol}\nOrder ID: <code>{order_id}</code>", parse_mode="HTML")
            elif result and "error" in result:
                await context.bot.send_message(chat_id=chat_id, text=f"❌ Failed to place {side_text} for {symbol}: {result['error']}")
            
            symbol_trends[symbol] = new_trend
            symbol_open_trade_side[symbol] = await get_open_position_side(symbol)

    # 3. 30-Minute Status Report Logic
    current_time = time.time()
    last_report_time = context.job.data.get("last_report_time", 0)
    
    if current_time - last_report_time >= 1800:
        trade_status = f"✅ {current_open_trade_side} trade open for {symbol}" if current_open_trade_side else f"❌ No open trade for {symbol}"
        report_msg = (
            f"Still monitoring {symbol}\n"
            f"• Timeframe: 30 minutes Ichimoku Conversion Line\n"
            f"• Filter: ATR-based Buffer Zone ({ATR_MULTIPLIER}x ATR)\n"
            f"• Monitoring every 1 second\n"
            f"• {symbol} current market state: {current_market_state.upper()}\n"
            f"• {trade_status}"
        )
        await context.bot.send_message(chat_id=chat_id, text=report_msg)
        context.job.data["last_report_time"] = current_time

async def add_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /add_symbol <symbol>")
        return

    symbol = context.args[0].upper()
    if symbol in monitored_symbols:
        await update.message.reply_text(f"{symbol} is already being monitored.")
        return

    monitored_symbols.append(symbol)
    symbol_trends[symbol] = None

    chat_id = update.effective_chat.id
    await update.message.reply_text(f"Added {symbol} to monitoring!")

    if context.job_queue:
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
    if context.job_queue:
        jobs = context.job_queue.get_jobs_by_name(f"{symbol}_monitor")
        for job in jobs:
            job.schedule_removal()
    await update.message.reply_text(f"Stopped monitoring {symbol}.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("Bot ready! Use /add_symbol <symbol> to start.")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Exception while handling update: {context.error}")
    if CHAT_ID:
        await context.bot.send_message(chat_id=CHAT_ID, text=f"⚠️ Bot error: {context.error}")

async def main():
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("add_symbol", add_symbol))
    application.add_handler(CommandHandler("remove_symbol", remove_symbol))
    application.add_error_handler(error_handler)
    
    # Schedule motivational quotes every 12 hours
    if application.job_queue:
        application.job_queue.run_repeating(send_motivational_quote, interval=43200, first=10, chat_id=CHAT_ID)

    logger.info("Bot starting...")
    await application.initialize()
    await application.start()
    await application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    asyncio.run(main())
