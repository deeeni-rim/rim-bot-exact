import asyncio
import hashlib
import json
import time
from datetime import datetime

import pandas as pd

from config import (
    USERS_CACHE_SECONDS,
    BAR_EVENT_BLOCK_TIMEOUT,
    SIGNAL_SHARD_INDEX,
    SIGNAL_SHARD_COUNT,
)
from db import (
    init_db,
    get_all_active_users,
    get_user_symbol_state,
    upsert_user_symbol_state,
    enqueue_outbound_message,
    utc_now,
)
from redis_state import pop_bar_event_payload, set_signal_lock, redis_client
from signal_engine import process_symbol_for_user
from bot_ui import format_signal_message

_users_cache = []
_users_cache_at = 0.0


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def stable_partition(symbol: str, shard_count: int) -> int:
    h = hashlib.md5(symbol.encode("utf-8")).hexdigest()
    return int(h, 16) % shard_count


def symbol_belongs_to_this_worker(symbol: str) -> bool:
    return stable_partition(symbol, SIGNAL_SHARD_COUNT) == SIGNAL_SHARD_INDEX


def get_users_cached():
    global _users_cache, _users_cache_at

    now_ts = time.time()
    if _users_cache and (now_ts - _users_cache_at) < USERS_CACHE_SECONDS:
        return _users_cache

    _users_cache = get_all_active_users()
    _users_cache_at = now_ts
    return _users_cache


def records_to_df(records: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    if df.empty:
        return df

    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["time"]).copy()

    for col in ["open", "high", "low", "close", "vol"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close"]).copy()
    df = df.set_index("time")
    df = df.sort_index()
    return df


def load_symbol_candles_from_redis(symbol: str):
    try:
        candles_5m_raw = redis_client.get(f"candles:{symbol}:5m")
        candles_1h_raw = redis_client.get(f"candles:{symbol}:1h")

        if not candles_5m_raw or not candles_1h_raw:
            return [], []

        candles_5m = json.loads(candles_5m_raw)
        candles_1h = json.loads(candles_1h_raw)
        return candles_5m, candles_1h

    except Exception as e:
        print(f"[{now_str()}] redis load candles error | {symbol} | {e}", flush=True)
        return [], []


def process_bar_event_sync(event: dict):
    start_ts = time.perf_counter()

    try:
        symbol = event["symbol"]
        timeframe = event["timeframe"]
        bar_marker = event["bar_marker"]

        if timeframe != "Min5":
            return

        if not symbol_belongs_to_this_worker(symbol):
            return

        candles_5m, candles_1h = load_symbol_candles_from_redis(symbol)

        if not candles_5m or not candles_1h:
            print(f"[{now_str()}] redis miss | {symbol}", flush=True)
            return

        df_scan = records_to_df(candles_5m)
        df_filter = records_to_df(candles_1h)

        if df_scan.empty or df_filter.empty:
            return

        if len(df_scan) < 10 or len(df_filter) < 20:
            return

        users = get_users_cached()
        queued_count = 0

        for user in users:
            try:
                telegram_id = user["telegram_id"]
                trade_row = get_user_symbol_state(telegram_id, symbol)

                signal, trade_state, snapshot_meta = process_symbol_for_user(
                    df_scan=df_scan,
                    df_filter=df_filter,
                    user=user,
                    trade_row=trade_row,
                )

                base = trade_row if trade_row else {}

                upsert_user_symbol_state(
                    {
                        **base,
                        "telegram_id": telegram_id,
                        "symbol": symbol,
                        "in_trade": 1 if trade_state.in_trade else 0,
                        "trade_dir": trade_state.trade_dir,
                        "entry": trade_state.entry,
                        "stop": trade_state.stop,
                        "tp": trade_state.tp,
                        "last_signature": trade_state.last_signature,
                        "last_bar_marker": trade_state.last_bar_marker,
                    }
                )

                if not signal or not snapshot_meta:
                    continue

                locked = set_signal_lock(
                    user_id=telegram_id,
                    symbol=symbol,
                    side=signal.side,
                    bar_marker=bar_marker,
                    ttl_seconds=86400,
                )
                if not locked:
                    continue

                text = format_signal_message(
                    symbol=symbol,
                    side=signal.side.upper(),
                    entry=signal.entry,
                    stop=signal.stop,
                    tp=signal.tp,
                    risk_pct=signal.risk_pct,
                    user_settings=user,
                )

                enqueue_outbound_message(
                    telegram_id=telegram_id,
                    symbol=symbol,
                    side=signal.side.upper(),
                    text=text,
                    signal_key=f"{telegram_id}|{symbol}|{signal.side}|{bar_marker}",
                    created_at=utc_now(),
                )

                queued_count += 1

                print(
                    f"[{now_str()}] REDIS SIGNAL {signal.side.upper()} | {symbol} | "
                    f"user={telegram_id} | bar={bar_marker}",
                    flush=True,
                )

            except Exception as e:
                print(
                    f"[{now_str()}] signal-worker error | {symbol} | user={user.get('telegram_id')} | {e}",
                    flush=True,
                )

        elapsed = time.perf_counter() - start_ts
        print(
            f"[{now_str()}] event processed | {symbol} | bar={bar_marker} | "
            f"queued={queued_count} | took={elapsed:.3f}s",
            flush=True,
        )

    except Exception as e:
        print(f"[{now_str()}] process_bar_event fatal | {e}", flush=True)


async def run_event(event: dict, semaphore: asyncio.Semaphore):
    async with semaphore:
        await asyncio.to_thread(process_bar_event_sync, event)


async def dispatcher_loop(semaphore: asyncio.Semaphore):
    while True:
        try:
            event = await asyncio.to_thread(
                pop_bar_event_payload,
                BAR_EVENT_BLOCK_TIMEOUT,
            )

            if not event:
                await asyncio.sleep(0.01)
                continue

            asyncio.create_task(run_event(event, semaphore))

        except Exception as e:
            print(f"[{now_str()}] dispatcher error | {e}", flush=True)
            await asyncio.sleep(0.1)


async def main():
    print("signal_engine_worker.py started", flush=True)
    init_db()

    print(
        f"[{now_str()}] signal shard | index={SIGNAL_SHARD_INDEX} "
        f"count={SIGNAL_SHARD_COUNT}",
        flush=True,
    )

    # Можно потом поднять до 30, но сначала проверим на 20
    semaphore = asyncio.Semaphore(20)

    await dispatcher_loop(semaphore)


if __name__ == "__main__":
    asyncio.run(main())