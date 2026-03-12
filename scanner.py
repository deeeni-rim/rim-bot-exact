import asyncio
from collections import defaultdict
from datetime import datetime

from config import (
    AUTO_LOAD_SYMBOLS,
    MANUAL_SYMBOLS,
    MAX_AUTO_SYMBOLS,
    SCAN_TIMEFRAME,
    FILTER_TIMEFRAME,
    SCAN_SLEEP_SECONDS,
)
from db import get_all_active_users
from mexc_client import get_contract_symbols, get_klines
from strategy import calc_signal

# защита от дублей
last_sent = defaultdict(dict)


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def load_symbols():
    if AUTO_LOAD_SYMBOLS:
        symbols = get_contract_symbols(MAX_AUTO_SYMBOLS)
    else:
        symbols = MANUAL_SYMBOLS

    symbols = sorted(set(symbols))
    return symbols


async def run_scanner(bot_app):
    symbols = load_symbols()
    print(f"[{now_str()}] Scanner started. Loaded {len(symbols)} crypto USDT symbols.")

    cycle_num = 0

    while True:
        cycle_num += 1
        print(f"[{now_str()}] Scan cycle #{cycle_num} started.")

        users = get_all_active_users()
        print(f"[{now_str()}] Active users: {len(users)}")

        sent_count = 0
        checked_count = 0

        for symbol in symbols:
            try:
                df_signal = get_klines(symbol, SCAN_TIMEFRAME, 200)
                df_filter = get_klines(symbol, FILTER_TIMEFRAME, 200)

                if df_signal is None or df_filter is None:
                    continue

                checked_count += 1

                for user in users:
                    signal = calc_signal(df_signal, df_filter, user)
                    if not signal:
                        continue

                    key = f"{symbol}:{user['telegram_id']}"
                    signature = (
                        signal["side"],
                        round(signal["entry"], 8),
                        round(signal["stop"], 8),
                    )

                    if last_sent.get(key) == signature:
                        continue

                    side_label = "🟢 LONG" if signal["side"] == "long" else "🔴 SHORT"

                    text = (
                        f"{side_label}\n\n"
                        f"Монета: {symbol}\n"
                        f"ТФ: {SCAN_TIMEFRAME}\n"
                        f"Вход: {signal['entry']:.8f}\n"
                        f"Стоп: {signal['stop']:.8f}\n"
                        f"Тейк: {signal['tp']:.8f}\n"
                        f"Риск: {signal['risk_pct']:.2f}%"
                    )

                    print(
                        f"[{now_str()}] SIGNAL {signal['side'].upper()} | "
                        f"{symbol} | user={user['telegram_id']} | "
                        f"entry={signal['entry']:.8f} | stop={signal['stop']:.8f}"
                    )

                    await bot_app.bot.send_message(
                        chat_id=user["telegram_id"],
                        text=text
                    )

                    print(
                        f"[{now_str()}] SENT to user={user['telegram_id']} | "
                        f"{symbol} | {signal['side'].upper()}"
                    )

                    last_sent[key] = signature
                    sent_count += 1

            except Exception as e:
                print(f"[{now_str()}] scanner error | {symbol} | {e}")

        print(
            f"[{now_str()}] Scan cycle #{cycle_num} finished. "
            f"Checked symbols: {checked_count}. Sent signals: {sent_count}."
        )

        await asyncio.sleep(SCAN_SLEEP_SECONDS)