
import os
import asyncio
import random
import time
import pandas as pd
from datetime import datetime
from typing import Optional, Tuple
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

# ATR Multiplier for the buffer zone (0.5 means half an ATR)
ATR_MULTIPLIER = 1.0

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

async def get_instrument_constraints(symbol: str) -> Tuple[float, float]:
    try:
        # Offload blocking API call to a thread
        info = await asyncio.to_thread(
            bybit_session.get_instruments_info,
            category="linear",
            symbol=symbol
        )
        if info["retCode"] != 0:
            raise ValueError(info["retMsg"])
        
        lot_filter = info["result"]["list"][0]["lotSizeFilter"]
        min_qty = float(lot_filter["minOrderQty"])
        qty_step = float(lot_filter["qtyStep"])
        
        return min_qty, qty_step
    
    except Exception as e:
        print(f"Failed to get constraints for {symbol}: {e}")
        return 0.001, 0.001

async def get_trade_qty(symbol: str) -> str:
    try:
        # Offload blocking API call to a thread
        balance_resp = await asyncio.to_thread(
            bybit_session.get_wallet_balance,
            accountType="UNIFIED"
        )
        if balance_resp["retCode"] != 0:
            raise ValueError(f"Balance API error: {balance_resp["retMsg"]}")
        result = balance_resp["result"]
        if not result["list"]:
            raise ValueError("No account data in response")
        account = result["list"][0]
        total_avail_usd_str = account.get("totalAvailableBalance", "0")
        total_avail_usd = float(total_avail_usd_str)

        if total_avail_usd <= 0:
            min_qty, _ = await get_instrument_constraints(symbol)
            return f"{min_qty:.6f}".rstrip("0").rstrip(".")

        # Get current price for qty calculation
        _, _, _, current_price = await get_ichimoku_and_atr(symbol)
        if current_price is None or current_price <= 0:
            min_qty, _ = await get_instrument_constraints(symbol)
            return f"{min_qty:.6f}".rstrip("0").rstrip(".")

        risk_amount_usd = total_avail_usd * 0.20
        raw_qty = risk_amount_usd / current_price
        min_qty, qty_step = await get_instrument_constraints(symbol)
        if qty_step > 0:
            adjusted_qty = max(min_qty, (raw_qty // qty_step) * qty_step)
        else:
            adjusted_qty = max(min_qty, raw_qty)
        precision = 6 if adjusted_qty < 10 else 3
        qty_str = f"{adjusted_qty:.{precision}f}".rstrip("0").rstrip(".")
        return qty_str
    except Exception as exc:
        print(f"Failed to calculate qty for {symbol}: {exc}")
        min_qty, _ = await get_instrument_constraints(symbol)
        return f"{min_qty:.6f}".rstrip("0").rstrip(".")

async def close_position(symbol: str, side_to_close: str = "Buy") -> Optional[dict]:
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
                qty = pos["size"]

                # Offload blocking API call to a thread
                order = await asyncio.to_thread(
                    bybit_session.place_order,
                    category="linear",
                    symbol=symbol,
                    side=close_side,
                    orderType="Market",
                    qty=qty,
                    reduceOnly=True,
                )
                return order.get("result")

        return None
    except Exception as exc:
        print(f"Position close failed for {symbol}: {exc}")
        return None

async def place_trade(symbol: str, side: str = "Buy") -> Optional[dict]:
    qty = await get_trade_qty(symbol)
    try:
        # Offload blocking API call to a thread
        order = await asyncio.to_thread(
            bybit_session.place_order,
            category="linear",
            symbol=symbol,
            side=side,
            orderType="Market",
            qty=qty,
        )
        return order.get("result")
    except Exception as exc:
        print(f"Trade placement failed for {symbol} (qty={qty}): {exc}")
        return None

async def monitor_symbol(context: ContextTypes.DEFAULT_TYPE) -> None:
    symbol = context.job.data["symbol"]
    previous_trend = symbol_trends.get(symbol, None)
    current_open_trade_side = await get_open_position_side(symbol)
    symbol_open_trade_side[symbol] = current_open_trade_side

    conversion_line, atr, last_close, current_price = await get_ichimoku_and_atr(symbol)

    if conversion_line is None or atr is None or current_price is None:
        print(f"Skipping monitoring for {symbol}: Incomplete data (CL: {conversion_line}, ATR: {atr}, Price: {current_price})")
        return

    # Define the Buffer Zone using ATR
    upper_buffer = conversion_line + (ATR_MULTIPLIER * atr)
    lower_buffer = conversion_line - (ATR_MULTIPLIER * atr)

    # Determine the trend based on current price relative to the buffer zone
    if current_price > upper_buffer:
        current_trend = "upward"
    elif current_price < lower_buffer:
        current_trend = "downward"
    else:
        # Inside the buffer zone, we consider it "neutral" or "choppy"
        current_trend = "neutral"

    chat_id = context.job.chat_id if context.job else CHAT_ID

    # 1. Instant Trend Reversal Logic (Runs every second)
    # We only reverse if the price has clearly broken out of the buffer zone in the opposite direction
    if current_open_trade_side == "Buy" and current_trend == "downward":
        await context.bot.send_message(chat_id=chat_id, text=f"⚡ INSTANT REVERSAL: {symbol} broke below the ATR Buffer Zone! Closing Long, opening Short.")
        await close_position(symbol, side_to_close="Buy")
        result = await place_trade(symbol, side="Sell")
        if result:
            order_id = result.get("orderId", "—")
            await context.bot.send_message(chat_id=chat_id, text=f"✅ Short order placed for {symbol}\nOrder ID: <code>{order_id}</code>", parse_mode="HTML")
        symbol_trends[symbol] = current_trend
        symbol_open_trade_side[symbol] = "Sell"
        return
    elif current_open_trade_side == "Sell" and current_trend == "upward":
        await context.bot.send_message(chat_id=chat_id, text=f"⚡ INSTANT REVERSAL: {symbol} broke above the ATR Buffer Zone! Closing Short, opening Long.")
        await close_position(symbol, side_to_close="Sell")
        result = await place_trade(symbol, side="Buy")
        if result:
            order_id = result.get("orderId", "—")
            await context.bot.send_message(chat_id=chat_id, text=f"✅ Long order placed for {symbol}\nOrder ID: <code>{order_id}</code>", parse_mode="HTML")
        symbol_trends[symbol] = current_trend
        symbol_open_trade_side[symbol] = "Buy"
        return

    # 2. Initial Trend Change Logic (If no trade is open or first run for symbol)
    # Trigger if trend is clear (not neutral) AND (it's the first time we see a trend OR trend has changed)
    if current_trend != "neutral" and current_open_trade_side is None and (previous_trend is None or current_trend != previous_trend):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        msg = (
            f"{ts} — {symbol} trend confirmed <b>{current_trend.upper()}</b>!\n"
            f"Price: {current_price:.2f} | Conversion Line: {conversion_line:.2f}\n"
            f"ATR Buffer: {ATR_MULTIPLIER * atr:.2f}"
        )
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")

        if current_trend == "upward":
            result = await place_trade(symbol, side="Buy")
            side_text = "Long (Buy)"
        else:
            result = await place_trade(symbol, side="Sell")
            side_text = "Short (Sell)"

        if result:
            order_id = result.get("orderId", "—")
            await context.bot.send_message(chat_id=chat_id, text=f"✅ {side_text} order placed for {symbol}\nOrder ID: <code>{order_id}</code>", parse_mode="HTML")
        
        symbol_trends[symbol] = current_trend
        symbol_open_trade_side[symbol] = await get_open_position_side(symbol)
    
    # 3. 30-Minute Status Report Logic
    current_time = time.time()
    last_report_time = context.job.data.get("last_report_time", 0)
    
    if current_time - last_report_time >= 1800: # 1800 seconds = 30 minutes
        trade_status = "No open trade"
        if current_open_trade_side:
            trade_status = f"✅ {current_open_trade_side} trade open for {symbol}"
        else:
            trade_status = f"❌ No open trade for {symbol}"

        report_msg = (
            f"Still monitoring {symbol}\n"
            f"• Timeframe: 30 minutes Ichimoku Conversion Line\n"
            f"• Filter: ATR-based Buffer Zone ({ATR_MULTIPLIER}x ATR)\n"
            f"• Monitoring every 1 second\n"
            f"• {symbol} current trend: {current_trend.upper()}\n"
            f"• {trade_status}"
        )
        await context.bot.send_message(chat_id=chat_id, text=report_msg)
        context.job.data["last_report_time"] = current_time

async def add_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /add_symbol <symbol> e.g. /add_symbol BTCUSDT")
        return

    symbol = context.args[0].upper()
    if symbol in monitored_symbols:
        await update.message.reply_text(f"{symbol} is already being monitored.")
        return

    monitored_symbols.append(symbol)
    symbol_trends[symbol] = None # Initialize previous trend as None

    chat_id = update.effective_chat.id
    await update.message.reply_text(
        f"Added {symbol} to monitoring!\n"
        "• Timeframe: 30 minutes Ichimoku Conversion Line\n"
        f"• Filter: ATR-based Buffer Zone ({ATR_MULTIPLIER}x ATR)\n"
        "• Monitoring every 1 second\n"
        "• Status report every 30 minutes\n"
        "Trend changes → instant notification + demo trade"
    )

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
    if symbol in symbol_trends:
        del symbol_trends[symbol]

    if context.job_queue:
        jobs = context.job_queue.get_jobs_by_name(f"{symbol}_monitor")
        for job in jobs:
            job.schedule_removal()

    await update.message.reply_text(f"Stopped monitoring {symbol}.")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Bot ready!\n"
        "Use /add_symbol <symbol> to start monitoring (e.g. /add_symbol BTCUSDT)\n"
        "Use /remove_symbol <symbol> to stop monitoring.\n"
        "Supports multiple symbols. Trades use 20% of available USDT balance."
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
