import asyncio
import time
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


MAX_CONCURRENT_SYMBOLS = 8
SYMBOLS_REFRESH_EVERY_CYCLES = 10

last_sent = {}


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def load_symbols():
    if AUTO_LOAD_SYMBOLS:
        symbols = get_contract_symbols(MAX_AUTO_SYMBOLS)
    else:
        symbols = MANUAL_SYMBOLS

    return sorted(set(symbols))


async def _get_klines_retry(symbol: str, interval: str, limit: int):
    delay = 0.8
    last_error = None

    for attempt in range(3):
        try:
            df = await asyncio.to_thread(get_klines, symbol, interval, limit)
            if df is None or len(df) == 0:
                raise RuntimeError("empty dataframe")
            return df
        except Exception as e:
            last_error = e
            err = str(e)

            if attempt < 2:
                if "403" in err:
                    print(
                        f"[{now_str()}] WAF 403 | {symbol} | {interval} | retry {attempt + 1}/3",
                        flush=True,
                    )
                else:
                    print(
                        f"[{now_str()}] get_klines retry | {symbol} | {interval} | {e}",
                        flush=True,
                    )
                await asyncio.sleep(delay)
                delay *= 2
            else:
                raise last_error


async def scan_one_symbol(symbol, users, semaphore: asyncio.Semaphore, send_queue: asyncio.Queue):
    async with semaphore:
        try:
            df_scan, df_filter = await asyncio.gather(
                _get_klines_retry(symbol, SCAN_TIMEFRAME, 150),
                _get_klines_retry(symbol, FILTER_TIMEFRAME, 120),
            )
        except Exception as e:
            print(f"[{now_str()}] scanner error | {symbol} | {e}", flush=True)
            return 0, 0

        if df_scan is None or df_filter is None:
            return 0, 0

        queued_count = 0

        for user in users:
            try:
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
                    f"[{now_str()}] SIGNAL {signal.side.upper()} | {symbol} | "
                    f"user={user['telegram_id']} | entry={signal.entry:.8f} | stop={signal.stop:.8f}",
                    flush=True,
                )

                await send_queue.put(
                    (user["telegram_id"], text, symbol, signal.side.upper())
                )

                last_sent[key] = signature
                queued_count += 1

            except Exception as e:
                print(
                    f"[{now_str()}] symbol-user error | {symbol} | user={user.get('telegram_id')} | {e}",
                    flush=True,
                )

        return 1, queued_count


async def run_scanner(bot, send_queue: asyncio.Queue):
    print("run_scanner(): entered", flush=True)

    cycle_num = 0
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_SYMBOLS)
    symbols = load_symbols()

    print(
        f"[{now_str()}] Scanner started. Loaded {len(symbols)} crypto USDT symbols.",
        flush=True,
    )

    while True:
        cycle_num += 1
        cycle_started = time.perf_counter()

        if cycle_num == 1 or cycle_num % SYMBOLS_REFRESH_EVERY_CYCLES == 0:
            symbols = load_symbols()
            print(
                f"[{now_str()}] Symbols refreshed. Loaded {len(symbols)} crypto USDT symbols.",
                flush=True,
            )

        print(f"[{now_str()}] Scan cycle #{cycle_num} started.", flush=True)
        print(f"[{now_str()}] Symbols in cycle: {len(symbols)}", flush=True)

        users = get_all_active_users()
        print(f"[{now_str()}] Active users: {len(users)}", flush=True)

        if not users:
            print(
                f"[{now_str()}] No active users. Sleeping {SCAN_SLEEP_SECONDS}s.",
                flush=True,
            )
            await asyncio.sleep(SCAN_SLEEP_SECONDS)
            continue

        tasks = [
            scan_one_symbol(symbol, users, semaphore, send_queue)
            for symbol in symbols
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        checked_count = 0
        queued_count = 0

        for r in results:
            if isinstance(r, Exception):
                print(f"[{now_str()}] gather error | {r}", flush=True)
                continue
            checked_count += r[0]
            queued_count += r[1]

        cycle_time = time.perf_counter() - cycle_started

        print(
            f"[{now_str()}] Scan cycle #{cycle_num} finished. "
            f"Checked symbols: {checked_count}. Sent signals: {queued_count}. "
            f"Cycle time: {cycle_time:.2f}s",
            flush=True,
        )

        await asyncio.sleep(SCAN_SLEEP_SECONDS)