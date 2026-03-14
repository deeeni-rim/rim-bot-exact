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
from db import (
    init_db,
    get_all_active_users,
    get_user_symbol_state,
    upsert_user_symbol_state,
    enqueue_outbound_message,
    utc_now,
)
from mexc_client import get_contract_symbols, get_klines
from strategy import (
    state_from_db,
    state_to_db,
    build_market_snapshot,
    _build_long_structure,
    _build_short_structure,
    process_user_symbol_fast,
)

MAX_CONCURRENT_SYMBOLS = 32
SYMBOLS_REFRESH_EVERY_CYCLES = 30
VALIDATION_CONCURRENCY = 30


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def load_symbols():
    if AUTO_LOAD_SYMBOLS:
        symbols = get_contract_symbols(MAX_AUTO_SYMBOLS)
    else:
        symbols = MANUAL_SYMBOLS

    return sorted(set(symbols))


async def _get_klines_retry(symbol: str, interval: str, limit: int):
    delay = 0.35

    for attempt in range(2):
        try:
            df = await asyncio.to_thread(get_klines, symbol, interval, limit)

            if df is None or len(df) == 0:
                return None

            return df

        except Exception as e:
            if attempt < 1:
                print(
                    f"[{now_str()}] get_klines retry {attempt + 1}/2 | {symbol} | {interval} | {e}",
                    flush=True,
                )
                await asyncio.sleep(delay)
                delay *= 2
            else:
                return None


async def is_valid_futures_symbol(symbol: str) -> bool:
    try:
        df_5m, df_1h = await asyncio.gather(
            _get_klines_retry(symbol, SCAN_TIMEFRAME, 60),
            _get_klines_retry(symbol, FILTER_TIMEFRAME, 30),
        )

        if df_5m is None or df_1h is None:
            return False

        if len(df_5m) < 20 or len(df_1h) < 20:
            return False

        return True
    except Exception:
        return False


async def build_valid_symbols(symbols: list[str]) -> list[str]:
    print(f"[{now_str()}] Validating futures symbols: {len(symbols)}", flush=True)

    semaphore = asyncio.Semaphore(VALIDATION_CONCURRENCY)

    async def _check(sym: str):
        async with semaphore:
            ok = await is_valid_futures_symbol(sym)
            return sym, ok

    tasks = [_check(sym) for sym in symbols]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    valid = []

    for r in results:
        if isinstance(r, Exception):
            continue
        sym, ok = r
        if ok:
            valid.append(sym)

    print(
        f"[{now_str()}] Futures validation finished. Valid symbols: {len(valid)} / {len(symbols)}",
        flush=True,
    )
    return valid


async def scan_one_symbol(symbol, users, semaphore: asyncio.Semaphore):
    async with semaphore:
        try:
            df_scan, df_filter = await asyncio.gather(
                _get_klines_retry(symbol, SCAN_TIMEFRAME, 120),
                _get_klines_retry(symbol, FILTER_TIMEFRAME, 80),
            )
        except Exception as e:
            print(f"[{now_str()}] scanner error | {symbol} | {e}", flush=True)
            return 0, 0

        if df_scan is None or df_filter is None:
            return 0, 0

        snapshot = build_market_snapshot(df_scan, df_filter)
        if snapshot is None:
            return 1, 0

        queued_count = 0

        users_by_sens = {}
        for user in users:
            sens = int(user["structure_sensitivity"])
            users_by_sens.setdefault(sens, []).append(user)

        for sens, sens_users in users_by_sens.items():
            try:
                long_l, long_h = _build_long_structure(df_scan, sens)
                short_h, short_l = _build_short_structure(df_scan, sens)
            except Exception as e:
                print(
                    f"[{now_str()}] structure error | {symbol} | sens={sens} | {e}",
                    flush=True,
                )
                continue

            for user in sens_users:
                try:
                    trade_row = get_user_symbol_state(user["telegram_id"], symbol)
                    trade_state = state_from_db(trade_row or {})

                    signal = process_user_symbol_fast(
                        snapshot=snapshot,
                        df_scan=df_scan,
                        long_l=long_l,
                        long_h=long_h,
                        short_h=short_h,
                        short_l=short_l,
                        user=user,
                        trade_state=trade_state,
                    )

                    upsert_user_symbol_state(
                        state_to_db(user["telegram_id"], symbol, trade_state)
                    )

                    if not signal:
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

                    bar_marker = str(df_scan.index[-2])

                    print(
                        f"[{now_str()}] SIGNAL {signal.side.upper()} | {symbol} | "
                        f"user={user['telegram_id']} | entry={signal.entry:.8f} | stop={signal.stop:.8f}",
                        flush=True,
                    )

                    enqueue_outbound_message(
                        telegram_id=user["telegram_id"],
                        symbol=symbol,
                        side=signal.side.upper(),
                        text=text,
                        signal_key=f"{user['telegram_id']}|{symbol}|{signal.side}|{bar_marker}",
                        created_at=utc_now(),
                    )

                    queued_count += 1

                except Exception as e:
                    print(
                        f"[{now_str()}] symbol-user error | {symbol} | user={user.get('telegram_id')} | {e}",
                        flush=True,
                    )

        return 1, queued_count


async def run_scanner():
    print("run_scanner(): entered", flush=True)

    cycle_num = 0
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_SYMBOLS)

    raw_symbols = load_symbols()
    symbols = await build_valid_symbols(raw_symbols)

    print(
        f"[{now_str()}] Scanner started. Loaded {len(symbols)} valid crypto USDT futures symbols.",
        flush=True,
    )

    while True:
        try:
            cycle_num += 1
            cycle_started = time.perf_counter()

            if cycle_num == 1 or cycle_num % SYMBOLS_REFRESH_EVERY_CYCLES == 0:
                raw_symbols = load_symbols()
                symbols = await build_valid_symbols(raw_symbols)
                print(
                    f"[{now_str()}] Symbols refreshed. Loaded {len(symbols)} valid crypto USDT futures symbols.",
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

            results = []

            for i in range(0, len(symbols), MAX_CONCURRENT_SYMBOLS):
                batch = symbols[i:i + MAX_CONCURRENT_SYMBOLS]

                tasks = [
                    scan_one_symbol(symbol, users, semaphore)
                    for symbol in batch
                ]

                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                results.extend(batch_results)

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

        except Exception as e:
            print(f"[{now_str()}] FATAL scanner loop error: {e}", flush=True)
            await asyncio.sleep(5)


async def main():
    print("scanner_only.py started", flush=True)
    init_db()
    await run_scanner()


if __name__ == "__main__":
    asyncio.run(main())