import asyncio
import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor
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


def load_candles(symbol: str):
    try:
        c5 = redis_client.get(f"candles:{symbol}:5m")
        c1h = redis_client.get(f"candles:{symbol}:1h")

        if not c5 or not c1h:
            return None, None

        return json.loads(c5), json.loads(c1h)
    except Exception as e:
        print(f"[{now_str()}] redis load candles error | {symbol} | {e}", flush=True)
        return None, None


def to_df(data):
    df = pd.DataFrame(data)
    if df.empty:
        return df

    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["time"]).copy()

    for col in ["open", "high", "low", "close", "vol"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close"]).copy()
    df = df.set_index("time").sort_index()
    return df


def process_bar_event_sync(event: dict):
    start = time.perf_counter()

    try:
        symbol = event["symbol"]
        timeframe = event["timeframe"]
        bar_marker = event["bar_marker"]

        if timeframe != "Min5":
            return

        if not symbol_belongs_to_this_worker(symbol):
            return

        c5, c1h = load_candles(symbol)
        if not c5 or not c1h:
            print(f"[{now_str()}] redis miss | {symbol}", flush=True)
            return

        df5 = to_df(c5)
        df1h = to_df(c1h)

        if df5.empty or df1h.empty:
            print(f"[{now_str()}] empty df | {symbol}", flush=True)
            return

        if len(df5) < 10 or len(df1h) < 20:
            print(
                f"[{now_str()}] not enough candles | {symbol} | 5m={len(df5)} 1h={len(df1h)}",
                flush=True,
            )
            return

        users = get_users_cached()
        states = get_states_for_symbol(symbol)

        def process_user(user):
            try:
                uid = user["telegram_id"]
                trade_row = states.get(uid)

                signal, trade_state, snapshot = process_symbol_for_user(
                    df_scan=df5,
                    df_filter=df1h,
                    user=user,
                    trade_row=trade_row,
                )

                update = {
                    "telegram_id": uid,
                    "symbol": symbol,
                    "in_trade": int(trade_state.in_trade),
                    "trade_dir": trade_state.trade_dir,
                    "entry": trade_state.entry,
                    "stop": trade_state.stop,
                    "tp": trade_state.tp,
                    "last_signature": trade_state.last_signature,
                    "last_bar_marker": trade_state.last_bar_marker,
                }

                queued_local = 0

                if signal and snapshot:
                    if set_signal_lock(
                        user_id=uid,
                        symbol=symbol,
                        side=signal.side,
                        bar_marker=bar_marker,
                        ttl_seconds=86400,
                    ):
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

                        queued_local = 1

                return update, queued_local

            except Exception as e:
                print(
                    f"[{now_str()}] signal-worker user error | {symbol} | user={user.get('telegram_id')} | {e}",
                    flush=True,
                )
                return None, 0

        updates = []
        queued = 0

        with ThreadPoolExecutor(max_workers=10) as executor:
            results = list(executor.map(process_user, users))

        for upd, q in results:
            if upd:
                updates.append(upd)
            queued += q

        upsert_states_batch(updates)

        elapsed = time.perf_counter() - start
        print(
            f"[{now_str()}] event processed | {symbol} | bar={bar_marker} | queued={queued} | took={elapsed:.3f}s",
            flush=True,
        )

    except Exception as e:
        print(f"[{now_str()}] process_bar_event fatal | {e}", flush=True)


async def run_event(event: dict, sem: asyncio.Semaphore):
    async with sem:
        await asyncio.to_thread(process_bar_event_sync, event)


async def dispatcher(sem: asyncio.Semaphore):
    while True:
        try:
            event = await asyncio.to_thread(
                pop_bar_event_payload,
                BAR_EVENT_BLOCK_TIMEOUT,
            )

            if not event:
                await asyncio.sleep(0.01)
                continue

            asyncio.create_task(run_event(event, sem))

        except Exception as e:
            print(f"[{now_str()}] dispatcher error | {e}", flush=True)
            await asyncio.sleep(0.1)


async def main():
    print("signal_engine_worker.py started", flush=True)
    init_db()

    print(
        f"[{now_str()}] signal shard | index={SIGNAL_SHARD_INDEX} count={SIGNAL_SHARD_COUNT}",
        flush=True,
    )

    sem = asyncio.Semaphore(20)
    await dispatcher(sem)


if __name__ == "__main__":
    asyncio.run(main())