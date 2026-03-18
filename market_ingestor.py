import asyncio
import hashlib
import json
from datetime import datetime, timezone

import websockets

from config import (
    AUTO_LOAD_SYMBOLS,
    MANUAL_SYMBOLS,
    MAX_AUTO_SYMBOLS,
    MEXC_FUTURES_WS,
    INGESTOR_SHARD_INDEX,
    INGESTOR_SHARD_COUNT,
    REDIS_5M_LIMIT,
    REDIS_1H_LIMIT,
)
from mexc_client import get_contract_symbols, get_klines
from redis_state import redis_ping, push_bar_event_payload, redis_client

PING_INTERVAL_SECONDS = 15
BOOTSTRAP_5M_LIMIT = 120
BOOTSTRAP_1H_LIMIT = 80
SUBSCRIBE_BATCH_SLEEP = 0.01

memory_5m: dict[str, list[dict]] = {}
memory_1h: dict[str, list[dict]] = {}


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def stable_partition(symbol: str, shard_count: int) -> int:
    h = hashlib.md5(symbol.encode("utf-8")).hexdigest()
    return int(h, 16) % shard_count


def load_symbols() -> list[str]:
    if AUTO_LOAD_SYMBOLS:
        symbols = get_contract_symbols(MAX_AUTO_SYMBOLS)
    else:
        symbols = MANUAL_SYMBOLS
    return sorted(set(symbols))


def shard_symbols(symbols: list[str], shard_index: int, shard_count: int) -> list[str]:
    return [s for s in symbols if stable_partition(s, shard_count) == shard_index]


def df_to_records(df, limit: int) -> list[dict]:
    out = []
    for idx, row in df.tail(limit).iterrows():
        out.append(
            {
                "time": str(idx),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "vol": float(row["vol"]) if "vol" in row else 0.0,
            }
        )
    return out


def iso_from_ts(ts_seconds: int) -> str:
    return datetime.fromtimestamp(ts_seconds, tz=timezone.utc).isoformat()


async def bootstrap_symbol(symbol: str):
    try:
        if (
            symbol in memory_5m
            and len(memory_5m[symbol]) > 50
            and symbol in memory_1h
            and len(memory_1h[symbol]) > 20
        ):
            return

        df_5m, df_1h = await asyncio.gather(
            asyncio.to_thread(get_klines, symbol, "Min5", BOOTSTRAP_5M_LIMIT),
            asyncio.to_thread(get_klines, symbol, "Min60", BOOTSTRAP_1H_LIMIT),
        )

        candles_5m = (
            df_to_records(df_5m, REDIS_5M_LIMIT)
            if df_5m is not None and len(df_5m) > 0
            else []
        )
        candles_1h = (
            df_to_records(df_1h, REDIS_1H_LIMIT)
            if df_1h is not None and len(df_1h) > 0
            else []
        )

        memory_5m[symbol] = candles_5m
        memory_1h[symbol] = candles_1h

        try:
            redis_client.set(
                f"candles:{symbol}:5m",
                json.dumps(candles_5m[-50:], ensure_ascii=False, separators=(",", ":")),
                ex=300,
            )
            redis_client.set(
                f"candles:{symbol}:1h",
                json.dumps(candles_1h[-30:], ensure_ascii=False, separators=(",", ":")),
                ex=1800,
            )
        except Exception as e:
            print(f"[{now_str()}] redis bootstrap save error | {symbol} | {e}", flush=True)

        print(f"[{now_str()}] bootstrap ok | {symbol}", flush=True)

    except Exception as e:
        print(f"[{now_str()}] bootstrap error | {symbol} | {e}", flush=True)


def merge_candle(existing: list[dict], candle: dict, max_len: int):
    if not existing:
        return [candle], None

    last = existing[-1]
    last_time = str(last["time"])
    new_time = str(candle["time"])

    if new_time == last_time:
        existing[-1] = candle
        return existing[-max_len:], None

    if new_time > last_time:
        existing.append(candle)
        existing = existing[-max_len:]
        closed_bar_marker = str(existing[-2]["time"]) if len(existing) >= 2 else None
        return existing, closed_bar_marker

    return existing, None


def handle_kline_push(symbol: str, interval: str, data: dict):
    candle = {
        "time": iso_from_ts(int(data["t"])),
        "open": float(data["o"]),
        "high": float(data["h"]),
        "low": float(data["l"]),
        "close": float(data["c"]),
        "vol": float(data.get("v", data.get("q", 0.0))),
    }

    if interval == "Min5":
        existing = memory_5m.get(symbol, [])
        updated, closed_bar_marker = merge_candle(existing, candle, REDIS_5M_LIMIT)
        memory_5m[symbol] = updated

        try:
            redis_client.set(
                f"candles:{symbol}:5m",
                json.dumps(updated[-50:], ensure_ascii=False, separators=(",", ":")),
                ex=300,
            )
        except Exception as e:
            print(f"[{now_str()}] redis save 5m error | {symbol} | {e}", flush=True)

        if closed_bar_marker:
            try:
                redis_client.set(
                    f"candles:{symbol}:1h",
                    json.dumps(memory_1h.get(symbol, [])[-30:], ensure_ascii=False, separators=(",", ":")),
                    ex=1800,
                )
            except Exception as e:
                print(f"[{now_str()}] redis save 1h error | {symbol} | {e}", flush=True)

            payload = {
                "symbol": symbol,
                "timeframe": "Min5",
                "bar_marker": closed_bar_marker,
            }
            push_bar_event_payload(payload)

    elif interval == "Min60":
        existing = memory_1h.get(symbol, [])
        updated, _ = merge_candle(existing, candle, REDIS_1H_LIMIT)
        memory_1h[symbol] = updated

        try:
            redis_client.set(
                f"candles:{symbol}:1h",
                json.dumps(updated[-30:], ensure_ascii=False, separators=(",", ":")),
                ex=1800,
            )
        except Exception as e:
            print(f"[{now_str()}] redis save 1h error | {symbol} | {e}", flush=True)


async def subscribe_all(ws, symbols: list[str]):
    for symbol in symbols:
        msg_5m = {
            "method": "sub.kline",
            "param": {
                "symbol": symbol,
                "interval": "Min5",
            },
        }
        msg_1h = {
            "method": "sub.kline",
            "param": {
                "symbol": symbol,
                "interval": "Min60",
            },
        }

        await ws.send(json.dumps(msg_5m))
        await asyncio.sleep(SUBSCRIBE_BATCH_SLEEP)
        await ws.send(json.dumps(msg_1h))
        await asyncio.sleep(SUBSCRIBE_BATCH_SLEEP)

    print(f"[{now_str()}] subscriptions sent | symbols={len(symbols)}", flush=True)


async def ping_loop(ws):
    while True:
        try:
            await asyncio.sleep(PING_INTERVAL_SECONDS)
            await ws.send(json.dumps({"method": "ping"}))
        except Exception:
            return


async def ws_loop(symbols: list[str]):
    while True:
        try:
            print(f"[{now_str()}] connecting ws", flush=True)

            async with websockets.connect(
                MEXC_FUTURES_WS,
                ping_interval=None,
                close_timeout=10,
                max_size=8 * 1024 * 1024,
            ) as ws:
                print(f"[{now_str()}] ws connected", flush=True)

                await subscribe_all(ws, symbols)
                ping_task = asyncio.create_task(ping_loop(ws))

                try:
                    async for raw in ws:
                        msg = json.loads(raw)

                        channel = msg.get("channel")
                        if channel == "pong":
                            continue

                        if channel != "push.kline":
                            continue

                        data = msg.get("data") or {}
                        symbol = data.get("symbol") or msg.get("symbol")
                        interval = data.get("interval")

                        if not symbol or not interval:
                            continue

                        handle_kline_push(symbol, interval, data)

                finally:
                    ping_task.cancel()

        except Exception as e:
            print(f"[{now_str()}] ws reconnect after error | {e}", flush=True)
            await asyncio.sleep(5)


async def main():
    print("market_ingestor.py started", flush=True)
    redis_ping()
    print(f"[{now_str()}] redis ok", flush=True)

    symbols = load_symbols()
    symbols = shard_symbols(symbols, INGESTOR_SHARD_INDEX, INGESTOR_SHARD_COUNT)

    print(
        f"[{now_str()}] ingestor shard | index={INGESTOR_SHARD_INDEX} "
        f"count={INGESTOR_SHARD_COUNT} symbols={len(symbols)}",
        flush=True,
    )

    semaphore = asyncio.Semaphore(20)

    async def _boot(sym: str):
        async with semaphore:
            await bootstrap_symbol(sym)

    await asyncio.gather(*[_boot(s) for s in symbols], return_exceptions=True)
    await ws_loop(symbols)


if __name__ == "__main__":
    asyncio.run(main())