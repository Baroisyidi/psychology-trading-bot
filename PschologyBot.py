import os
import asyncio
import time
from datetime import datetime
from typing import Optional, Tuple

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
# Load environment variables
# ────────────────────────────────────────────────
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
CHAT_ID = int(os.getenv("CHAT_ID", "0"))

if not all([TELEGRAM_TOKEN, BYBIT_API_KEY, BYBIT_API_SECRET]):
    raise RuntimeError("Missing required environment variables")

# ────────────────────────────────────────────────
# Bybit demo session
# ────────────────────────────────────────────────
bybit = HTTP(
    demo=True,
    api_key=BYBIT_API_KEY,
    api_secret=BYBIT_API_SECRET,
    recv_window=60000
)

# ────────────────────────────────────────────────
# Global state
# ────────────────────────────────────────────────
symbol_trends: dict[str, Optional[str]] = {}
monitored_symbols: set[str] = set()

# ────────────────────────────────────────────────
# Market analysis
# ────────────────────────────────────────────────
async def get_trend_and_ema(
    symbol: str,
    interval: str = "30",
    limit: int = 300,
) -> Tuple[Optional[str], Optional[float], Optional[float]]:

    try:
        resp = bybit.get_kline(
            category="linear",
            symbol=symbol,
            interval=interval,
            limit=limit,
        )

        klines = resp["result"]["list"]
        if not klines:
            return None, None, None

        df = pd.DataFrame(
            klines,
            columns=["ts", "open", "high", "low", "close", "vol", "turnover"],
        ).iloc[::-1]

        df["close"] = pd.to_numeric(df["close"])
        df["ema200"] = df["close"].ewm(span=200, adjust=False).mean()

        ema200 = float(df["ema200"].iloc[-1])

        ticker = bybit.get_tickers(
            category="linear",
            symbol=symbol
        )["result"]["list"][0]

        price = float(ticker["lastPrice"])
        trend = "upward" if price > ema200 else "downward"

        return trend, price, ema200

    except Exception as e:
        print(f"[EMA ERROR] {symbol}: {e}")
        return None, None, None

# ────────────────────────────────────────────────
# Instrument constraints
# ────────────────────────────────────────────────
async def get_instrument_constraints(symbol: str) -> Tuple[float, float]:
    info = bybit.get_instruments_info(category="linear", symbol=symbol)
    lot = info["result"]["list"][0]["lotSizeFilter"]
    return float(lot["minOrderQty"]), float(lot["qtyStep"])

# ────────────────────────────────────────────────
# Position sizing (20% risk)
# ────────────────────────────────────────────────
async def get_trade_qty(symbol: str) -> str:
    balance = bybit.get_wallet_balance(accountType="UNIFIED")
    usd = float(balance["result"]["list"][0]["totalAvailableBalance"])

    _, price, _ = await get_trend_and_ema(symbol)
    if not price or usd <= 0:
        min_qty, _ = await get_instrument_constraints(symbol)
        return str(min_qty)

    risk_usd = usd * 0.20
    raw_qty = risk_usd / price

    min_qty, step = await get_instrument_constraints(symbol)
    qty = max(min_qty, (raw_qty // step) * step)

    return f"{qty:.6f}".rstrip("0").rstrip(".")

# ────────────────────────────────────────────────
# Trade execution
# ────────────────────────────────────────────────
async def close_position(symbol: str, side: str):
    positions = bybit.get_positions(category="linear", symbol=symbol)["result"]["list"]
    for pos in positions:
        if pos["side"] == side and float(pos["size"]) > 0:
            bybit.place_order(
                category="linear",
                symbol=symbol,
                side="Sell" if side == "Buy" else "Buy",
                orderType="Market",
                qty=pos["size"],
                reduceOnly=True
            )

async def place_trade(symbol: str, side: str):
    qty = await get_trade_qty(symbol)
    return bybit.place_order(
        category="linear",
        symbol=symbol,
        side=side,
        orderType="Market",
        qty=qty
    )

# ────────────────────────────────────────────────
# Job: monitor symbol
# ────────────────────────────────────────────────
async def monitor_symbol(context: ContextTypes.DEFAULT_TYPE):
    symbol = context.job.data
    prev_trend = symbol_trends.get(symbol)

    trend, price, ema = await get_trend_and_ema(symbol)
    if not trend:
        return

    if trend != prev_trend:
        symbol_trends[symbol] = trend
        ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

        await context.bot.send_message(
            chat_id=context.job.chat_id,
            text=(
                f"📊 *{symbol} Trend Change*\n"
                f"{ts}\n\n"
                f"Trend: *{trend.upper()}*\n"
                f"Price: `{price:.2f}`\n"
                f"EMA200: `{ema:.2f}`"
            ),
            parse_mode="Markdown"
        )

        if trend == "upward":
            await close_position(symbol, "Sell")
            await place_trade(symbol, "Buy")
        else:
            await close_position(symbol, "Buy")
            await place_trade(symbol, "Sell")

# ────────────────────────────────────────────────
# Commands
# ────────────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 Psychology Trading Bot\n\n"
        "Commands:\n"
        "/add_symbol BTCUSDT"
    )

async def add_symbol(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /add_symbol BTCUSDT")
        return

    symbol = context.args[0].upper()
    if symbol in monitored_symbols:
        await update.message.reply_text(f"{symbol} already monitored.")
        return

    monitored_symbols.add(symbol)
    symbol_trends[symbol] = None

    context.job_queue.run_repeating(
        monitor_symbol,
        interval=300,
        first=10,
        chat_id=update.effective_chat.id,
        data=symbol,
        name=f"{symbol}_monitor",
    )

    await update.message.reply_text(f"✅ Monitoring {symbol}")

# ────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────
async def main():
    print("🚀 Bot starting (Polling mode)")

    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("add_symbol", add_symbol))

    await app.initialize()
    await app.bot.delete_webhook(drop_pending_updates=True)
    await app.start()

    print("✅ Bot is live and responding")
    await app.run_polling()

if __name__ == "__main__":
    asyncio.run(main())