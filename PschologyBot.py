import os 
import asyncio
from datetime import datetime
from typing import Optional, Tuple
import time

import pandas as pd
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

# ────────────────────────────────────────────────
#  Bybit demo session
# ────────────────────────────────────────────────
bybit_session = HTTP(
    demo=True,
    api_key=BYBIT_API_KEY,
    api_secret=BYBIT_API_SECRET,
    recv_window=60000
)

# ────────────────────────────────────────────────
#  TEMPORARY DEBUG: Check time difference
# ────────────────────────────────────────────────
try:
    resp = bybit_session.get_server_time()
    server_ms = int(resp['time'])
    local_ms = int(time.time() * 1000)
    diff = abs(local_ms - server_ms) / 1000
    print(f"Time diff: {diff:.2f} seconds")
    print("Time sync looks good" if diff <= 5 else "WARNING: Possible drift/latency (>5 seconds)")
except Exception as e:
    print(f"Debug failed: {e}")

# Global state
symbol_trends: dict[str, Optional[str]] = {}
monitored_symbols: list[str] = []

async def get_trend_and_ema(symbol: str, interval: str = "30", limit: int = 300) -> Tuple[Optional[str], Optional[float], Optional[float]]:
    try:
        resp = bybit_session.get_kline(category="linear", symbol=symbol, interval=interval, limit=limit)
        klines = resp["result"]["list"]
        if not klines:
            raise ValueError("No klines returned")

        df = pd.DataFrame(
            klines,
            columns=["timestamp", "open", "high", "low", "close", "volume", "turnover"],
        ).iloc[::-1].reset_index(drop=True)
        df["close"] = pd.to_numeric(df["close"], errors="coerce")

        if df["close"].isna().any():
            raise ValueError("Invalid close prices")

        df["ema200"] = df["close"].ewm(span=200, adjust=False).mean()
        ema200 = float(df["ema200"].iloc[-1])

        ticker = bybit_session.get_tickers(category="linear", symbol=symbol)["result"]["list"][0]
        current_price = float(ticker["lastPrice"])

        trend = "upward" if current_price > ema200 else "downward"
        return trend, current_price, ema200

    except Exception as exc:
        print(f"Error in get_trend_and_ema for {symbol}: {exc}")
        return None, None, None


async def get_instrument_constraints(symbol: str) -> Tuple[float, float]:
    try:
        info = bybit_session.get_instruments_info(category="linear", symbol=symbol)
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
        balance_resp = bybit_session.get_wallet_balance(accountType="UNIFIED")
        if balance_resp["retCode"] != 0:
            raise ValueError(f"Balance API error: {balance_resp['retMsg']}")
        
        result = balance_resp["result"]
        if not result["list"]:
            raise ValueError("No account data in response")
            
        account = result["list"][0]
        total_avail_usd_str = account.get("totalAvailableBalance", "0")
        total_avail_usd = float(total_avail_usd_str)

        if total_avail_usd <= 0:
            min_qty, _ = await get_instrument_constraints(symbol)
            return f"{min_qty:.6f}".rstrip('0').rstrip('.')

        _, current_price, _ = await get_trend_and_ema(symbol)
        if current_price is None or current_price <= 0:
            min_qty, _ = await get_instrument_constraints(symbol)
            return f"{min_qty:.6f}".rstrip('0').rstrip('.')

        risk_amount_usd = total_avail_usd * 0.20
        raw_qty = risk_amount_usd / current_price
        min_qty, qty_step = await get_instrument_constraints(symbol)
        
        if qty_step > 0:
            adjusted_qty = max(min_qty, (raw_qty // qty_step) * qty_step)
        else:
            adjusted_qty = max(min_qty, raw_qty)
            
        precision = 6 if adjusted_qty < 10 else 3
        qty_str = f"{adjusted_qty:.{precision}f}".rstrip('0').rstrip('.')
        return qty_str
    except Exception as exc:
        print(f"Failed to calculate qty for {symbol}: {exc}")
        min_qty, _ = await get_instrument_constraints(symbol)
        return f"{min_qty:.6f}".rstrip('0').rstrip('.')


async def close_position(symbol: str, side_to_close: str = "Buy") -> Optional[dict]:
    try:
        positions_resp = bybit_session.get_positions(category="linear", symbol=symbol)
        positions = positions_resp["result"]["list"]

        for pos in positions:
            if pos["side"] == side_to_close and float(pos["size"]) > 0:
                close_side = "Sell" if side_to_close == "Buy" else "Buy"
                qty = pos["size"]

                order = bybit_session.place_order(
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
        order = bybit_session.place_order(
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
    symbol = context.job.data
    previous_trend = symbol_trends.get(symbol, None)

    trend, price, ema = await get_trend_and_ema(symbol)

    if trend is None:
        if context.job:
            await context.bot.send_message(
                chat_id=context.job.chat_id,
                text=f"Error fetching Bybit data for {symbol}."
            )
        return

    chat_id = context.job.chat_id if context.job else CHAT_ID

    if trend != previous_trend:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # CHANGED: Switched to HTML parsing to prevent Markdown crash errors
        msg = f"⏱ {ts}\n{symbol} trend changed to <b>{trend.upper()}</b>!\nPrice: {price:.2f} | 200 EMA: {ema:.2f}"
        await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")

        if trend == "upward":
            await close_position(symbol, side_to_close="Sell")
            result = await place_trade(symbol, side="Buy")
            side_text = "Long (Buy)"
        else:
            await close_position(symbol, side_to_close="Buy")
            result = await place_trade(symbol, side="Sell")
            side_text = "Short (Sell)"

        if result:
            order_id = result.get("orderId", "—")
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"✅ {side_text} order placed for {symbol}\nOrder ID: <code>{order_id}</code>",
                parse_mode="HTML"
            )
        else:
            await context.bot.send_message(chat_id=chat_id, text=f"❌ Failed to place {side_text} order for {symbol}")

        symbol_trends[symbol] = trend
    else:
        print(f"[{datetime.now():%H:%M:%S}] No trend change for {symbol} — still {trend}")


async def add_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text("Usage: /add_symbol <symbol> e.g. /add_symbol BTCUSDT")
        return

    symbol = context.args[0].upper()
    if symbol in monitored_symbols:
        await update.message.reply_text(f"{symbol} is already being monitored.")
        return

    monitored_symbols.append(symbol)
    symbol_trends[symbol] = None

    chat_id = update.effective_chat.id
    await update.message.reply_text(
        f"✅ Added {symbol} to monitoring!\n"
        "• Timeframe: 30 minutes 200 EMA\n"
        "• Checks every 5 minutes\n"
        "Trend changes → notification + demo trade"
    )

    if context.job_queue:
        context.job_queue.run_repeating(
            callback=monitor_symbol,
            interval=300,
            first=5,
            chat_id=chat_id,
            name=f"{symbol}_monitor",
            data=symbol,
        )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Bot ready!\n"
        "Use /add_symbol <symbol> to start monitoring (e.g. /add_symbol BTCUSDT)\n"
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


def main():
    print("=== Render startup: Initializing ===")
    
    # 1. Build application
    application = Application.builder().token(TELEGRAM_TOKEN).build()

    # 2. Add Handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("add_symbol", add_symbol))
    application.add_error_handler(error_handler)

    # 3. Get Render specific dynamic variables
    port = int(os.environ.get("PORT", 10000))
    # RENDER_EXTERNAL_URL is natively injected by Render (e.g., https://your-app.onrender.com)
    render_url = os.environ.get("RENDER_EXTERNAL_URL", "https://psychology-trading-bot.onrender.com")
    webhook_url = f"{render_url}/webhook"

    print(f"Starting Webhook on Port: {port}")
    print(f"Webhook URL pointing to: {webhook_url}")

    # 4. Use built-in run_webhook (replaces custom aiohttp entirely)
    # This automatically initializes the bot, starts the JobQueue, and accepts incoming traffic.
    application.run_webhook(
        listen="0.0.0.0",
        port=port,
        webhook_url=webhook_url
    )

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nBot stopped by user.")
    except Exception as exc:
        print(f"Fatal startup error: {exc}")
        import traceback
        traceback.print_exc()