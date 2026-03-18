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
    get_states_for_symbol,
    upsert_states_batch,
    enqueue_outbound_message,
    utc_now,
)
from redis_state import pop_bar_event_payload, set_signal_lock, redis_client
from signal_engine import process_symbol_for_user
from bot_ui import format_signal_message

_users_cache = []
_users_cache_at = 0.0


def now_str():
    return datetime.now().strftime("%H:%M:%S")


def stable_partition(symbol: str, shard_count: int) -> int:
    h = hashlib.md5(symbol.encode()).hexdigest()
    return int(h, 16) % shard_count


def symbol_belongs_to_this_worker(symbol: str) -> bool:
    return stable_partition(symbol, SIGNAL_SHARD_COUNT) == SIGNAL_SHARD_INDEX


def get_users_cached():
    global _users_cache, _users_cache_at

    now = time.time()
    if _users_cache and (now - _users_cache_at) < USERS_CACHE_SECONDS:
        return _users_cache

    _users_cache = get_all_active_users()
    _users_cache_at = now
    return _users_cache


def load_candles(symbol):
    try:
        c5 = redis_client.get(f"candles:{symbol}:5m")
        c1h = redis_client.get(f"candles:{symbol}:1h")

        if not c5 or not c1h:
            return None, None

        return json.loads(c5), json.loads(c1h)
    except:
        return None, None


def to_df(data):
    df = pd.DataFrame(data)
    if df.empty:
        return df

    df["time"] = pd.to_datetime(df["time"])
    df = df.set_index("time").sort_index()
    return df


def process_bar_event_sync(event):
    start = time.perf_counter()

    symbol = event["symbol"]
    timeframe = event["timeframe"]
    bar_marker = event["bar_marker"]

    if timeframe != "Min5":
        return

    if not symbol_belongs_to_this_worker(symbol):
        return

    c5, c1h = load_candles(symbol)
    if not c5 or not c1h:
        return

    df5 = to_df(c5)
    df1h = to_df(c1h)

    if len(df5) < 10 or len(df1h) < 20:
        return

    users = get_users_cached()

    # 🚀 БАТЧ ЗАГРУЗКА
    states = get_states_for_symbol(symbol)

    updates = []
    queued = 0

    for user in users:
        uid = user["telegram_id"]
        trade_row = states.get(uid)

        signal, trade_state, snapshot = process_symbol_for_user(
            df_scan=df5,
            df_filter=df1h,
            user=user,
            trade_row=trade_row,
        )

        # собираем обновления
        updates.append({
            "telegram_id": uid,
            "symbol": symbol,
            "in_trade": int(trade_state.in_trade),
            "trade_dir": trade_state.trade_dir,
            "entry": trade_state.entry,
            "stop": trade_state.stop,
            "tp": trade_state.tp,
            "last_signature": trade_state.last_signature,
            "last_bar_marker": trade_state.last_bar_marker,
        })

        if not signal or not snapshot:
            continue

        if not set_signal_lock(uid, symbol, signal.side, bar_marker, 86400):
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
            telegram_id=uid,
            symbol=symbol,
            side=signal.side.upper(),
            text=text,
            signal_key=f"{uid}|{symbol}|{signal.side}|{bar_marker}",
            created_at=utc_now(),
        )

        queued += 1

    # 🚀 ОДИН UPSERT ВМЕСТО СОТЕН
    upsert_states_batch(updates)

    elapsed = time.perf_counter() - start

    print(
        f"[{now_str()}] {symbol} | queued={queued} | took={elapsed:.2f}s",
        flush=True,
    )


async def run_event(event, sem):
    async with sem:
        await asyncio.to_thread(process_bar_event_sync, event)


async def dispatcher(sem):
    while True:
        event = await asyncio.to_thread(pop_bar_event_payload, BAR_EVENT_BLOCK_TIMEOUT)

        if not event:
            await asyncio.sleep(0.01)
            continue

        asyncio.create_task(run_event(event, sem))


async def main():
    print("STARTED", flush=True)
    init_db()

    print(f"shard={SIGNAL_SHARD_INDEX}", flush=True)

    sem = asyncio.Semaphore(20)

    await dispatcher(sem)


if __name__ == "__main__":
    asyncio.run(main())