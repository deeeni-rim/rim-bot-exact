import asyncio
import hashlib
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
from redis_state import (
    pop_bar_close_event,
    load_symbol_candles_5m,
    load_symbol_candles_1h,
    set_signal_lock,
)
from signal_engine import process_symbol_for_user

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


def records_to_df(records: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(records)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["time"]).copy()

    for col in ["open", "high", "low", "close", "vol"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close"]).copy()
    df = df.set_index("time")
    df = df.sort_index()
    return df


async def process_bar_event(event: dict):
    symbol = event["symbol"]
    timeframe = event["timeframe"]
    bar_marker = event["bar_marker"]

    if timeframe != "Min5":
        return

    if not symbol_belongs_to_this_worker(symbol):
        return

    candles_5m = load_symbol_candles_5m(symbol)
    candles_1h = load_symbol_candles_1h(symbol)

    if not candles_5m or not candles_1h:
        return

    df_scan = records_to_df(candles_5m)
    df_filter = records_to_df(candles_1h)

    if len(df_scan) < 10 or len(df_filter) < 20:
        return

    users = get_users_cached()
    queued_count = 0

    for user in users:
        try:
            trade_row = get_user_symbol_state(user["telegram_id"], symbol)

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
                    "telegram_id": user["telegram_id"],
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
                user_id=user["telegram_id"],
                symbol=symbol,
                side=signal.side,
                bar_marker=bar_marker,
                ttl_seconds=86400,
            )
            if not locked:
                continue

            side_label = "🟢 LONG" if signal.side == "long" else "🔴 SHORT"

            text = (
                f"{side_label}\n\n"
                f"Монета: {symbol}\n"
                f"ТФ: Min5\n"
                f"Вход: {signal.entry:.8f}\n"
                f"Стоп: {signal.stop:.8f}\n"
                f"Тейк: {signal.tp:.8f}\n"
                f"Риск: {signal.risk_pct:.2f}%"
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

            print(
                f"[{now_str()}] REDIS SIGNAL {signal.side.upper()} | {symbol} | "
                f"user={user['telegram_id']} | bar={bar_marker}",
                flush=True,
            )

        except Exception as e:
            print(
                f"[{now_str()}] signal-worker error | {symbol} | user={user.get('telegram_id')} | {e}",
                flush=True,
            )

    if queued_count > 0:
        print(
            f"[{now_str()}] queued from redis event | {symbol} | count={queued_count} | bar={bar_marker}",
            flush=True,
        )


async def main():
    print("signal_engine_worker.py started", flush=True)
    init_db()

    while True:
        try:
            event = pop_bar_close_event(BAR_EVENT_BLOCK_TIMEOUT)
            if not event:
                await asyncio.sleep(0.2)
                continue

            await process_bar_event(event)

        except Exception as e:
            print(f"[{now_str()}] signal-engine fatal loop error | {e}", flush=True)
            await asyncio.sleep(2)


if __name__ == "__main__":
    asyncio.run(main())