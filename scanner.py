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
from db import get_all_active_users, get_user_symbol_state, upsert_user_symbol_state
from mexc_client import get_contract_symbols, get_klines
from strategy import process_user_symbol, state_from_db, state_to_db

last_sent = defaultdict(dict)

MAX_CONCURRENT_SYMBOLS = 5
SYMBOLS_REFRESH_EVERY_CYCLES = 10


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def load_symbols():
    if AUTO_LOAD_SYMBOLS:
        symbols = get_contract_symbols(MAX_AUTO_SYMBOLS)
    else:
        symbols = MANUAL_SYMBOLS

    return sorted(set(symbols))


async def scan_one_symbol(bot, symbol, users, semaphore):
    async with semaphore:
        try:
            try:
                df_scan = await asyncio.to_thread(get_klines, symbol, SCAN_TIMEFRAME, 150)
                df_filter = await asyncio.to_thread(get_klines, symbol, FILTER_TIMEFRAME, 120)
            except Exception as e:
                err = str(e)
                if "403" in err:
                    print(f"[{now_str()}] WAF 403 | {symbol} | backing off")
                    await asyncio.sleep(2)
                else:
                    print(f"[{now_str()}] scanner error | {symbol} | {e}")
                return 0, 0

            if df_scan is None or df_filter is None:
                return 0, 0

            sent_count = 0

            for user in users:
                trade_row = get_user_symbol_state(user["telegram_id"], symbol)
                trade_state = state_from_db(trade_row or {})

                signal = process_user_symbol(df_scan, df_filter, user, trade_state)

                upsert_user_symbol_state(
                    state_to_db(user["telegram_id"], symbol, trade_state)
                )

                if not signal:
                    continue

                key = f"{symbol}:{user['telegram_id']}"
                signature = getattr(signal, "signature", None)

                if last_sent.get(key) == signature:
                    continue

                side_label = "🟢 LONG" if signal.side == "long" else "🔴 SHORT"

                text = (
                    f"{side_label}\n\n"
                    f"Монета: {symbol}\n"
                    f"ТФ: {SCAN_TIMEFRAME}\n"
                    f"Вход: {signal.entry:.8f}\n"
                    f"Стоп: {signal.stop:.8f}\n"
                    f"Тейк: {signal.tp:.8f}\n"
                    f"Риск: {signal.risk_pct:.2f}%"
                )

                print(
                    f"[{now_str()}] SIGNAL {signal.side.upper()} | "
                    f"{symbol} | user={user['telegram_id']} | "
                    f"entry={signal.entry:.8f} | stop={signal.stop:.8f}"
                )

                try:
                    await bot.send_message(
                        chat_id=user["telegram_id"],
                        text=text
                    )
                except Exception as e:
                    print(f"[{now_str()}] send_message error | user={user['telegram_id']} | {symbol} | {e}")
                    continue

                print(
                    f"[{now_str()}] SENT to user={user['telegram_id']} | "
                    f"{symbol} | {signal.side.upper()}"
                )

                last_sent[key] = signature
                sent_count += 1

            return 1, sent_count

        except Exception as e:
            print(f"[{now_str()}] scanner error | {symbol} | {e}")
            return 0, 0


async def run_scanner(bot):
    cycle_num = 0
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_SYMBOLS)
    symbols = load_symbols()

    print(f"[{now_str()}] Scanner started. Loaded {len(symbols)} crypto USDT symbols.")

    while True:
        cycle_num += 1

        if cycle_num == 1 or cycle_num % SYMBOLS_REFRESH_EVERY_CYCLES == 0:
            symbols = load_symbols()
            print(f"[{now_str()}] Symbols refreshed. Loaded {len(symbols)} crypto USDT symbols.")

        print(f"[{now_str()}] Scan cycle #{cycle_num} started.")

        users = get_all_active_users()
        print(f"[{now_str()}] Active users: {len(users)}")

        tasks = [
            scan_one_symbol(bot, symbol, users, semaphore)
            for symbol in symbols
        ]

        results = await asyncio.gather(*tasks, return_exceptions=False)

        checked_count = sum(r[0] for r in results)
        sent_count = sum(r[1] for r in results)

        print(
            f"[{now_str()}] Scan cycle #{cycle_num} finished. "
            f"Checked symbols: {checked_count}. Sent signals: {sent_count}."
        )

        await asyncio.sleep(SCAN_SLEEP_SECONDS)