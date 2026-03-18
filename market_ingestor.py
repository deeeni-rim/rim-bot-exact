import asyncio
import json
from datetime import datetime, timezone

import requests
import websockets

import config as cfg
from redis_state import redis_client, push_bar_event_payload


MEXC_REST_BASE = cfg.MEXC_FUTURES_REST_BASE.rstrip("/")
MEXC_WS_URL = cfg.MEXC_FUTURES_WS

AUTO_LOAD_SYMBOLS = cfg.AUTO_LOAD_SYMBOLS
MANUAL_SYMBOLS = [s.upper() for s in cfg.MANUAL_SYMBOLS]
MAX_AUTO_SYMBOLS = cfg.MAX_AUTO_SYMBOLS

INGESTOR_SHARD_INDEX = cfg.INGESTOR_SHARD_INDEX
INGESTOR_SHARD_COUNT = cfg.INGESTOR_SHARD_COUNT

KEEP_5M = cfg.REDIS_5M_LIMIT
KEEP_1H = cfg.REDIS_1H_LIMIT

REDIS_CANDLE_TTL_SECONDS = getattr(cfg, "REDIS_CANDLE_TTL_SECONDS", 21600)
REDIS_EVENT_TTL_SECONDS = getattr(cfg, "REDIS_EVENT_TTL_SECONDS", 3600)
WS_PING_SECONDS = getattr(cfg, "WS_PING_SECONDS", 15)

HTTP_TIMEOUT = 20
WS_RECONNECT_DELAY = 3

BOOTSTRAP_5M_LIMIT = max(KEEP_5M + 20, 180)
BOOTSTRAP_1H_LIMIT = max(KEEP_1H + 20, 120)

closed_5m = {}
closed_1h = {}

current_5m = {}
current_1h = {}

last_written_5m = {}
last_written_1h = {}
last_emitted_bar = {}


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def to_iso_utc_from_sec(ts_sec: int) -> str:
    return datetime.fromtimestamp(int(ts_sec), tz=timezone.utc).isoformat()


def safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def stable_partition(symbol: str, shard_count: int) -> int:
    acc = 0
    for ch in symbol:
        acc = (acc * 131 + ord(ch)) % 1_000_000_007
    return acc % shard_count


def symbol_belongs_to_this_worker(symbol: str) -> bool:
    return stable_partition(symbol, INGESTOR_SHARD_COUNT) == INGESTOR_SHARD_INDEX


def compact_json(obj) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def trim_keep(arr: list[dict], keep: int):
    if len(arr) > keep:
        del arr[:-keep]


def make_candle(ts_sec: int, o, h, l, c, v):
    return {
        "time": to_iso_utc_from_sec(ts_sec),
        "open": safe_float(o),
        "high": safe_float(h),
        "low": safe_float(l),
        "close": safe_float(c),
        "vol": safe_float(v),
    }


def redis_write_if_changed(symbol: str, timeframe_key: str, payload_obj: list[dict]) -> bool:
    payload = compact_json(payload_obj)
    redis_key = f"candles:{symbol}:{timeframe_key}"

    if timeframe_key == "5m":
        prev = last_written_5m.get(symbol)
        if prev == payload:
            return False
        redis_client.set(redis_key, payload, ex=REDIS_CANDLE_TTL_SECONDS)
        last_written_5m[symbol] = payload
        return True

    if timeframe_key == "1h":
        prev = last_written_1h.get(symbol)
        if prev == payload:
            return False
        redis_client.set(redis_key, payload, ex=REDIS_CANDLE_TTL_SECONDS)
        last_written_1h[symbol] = payload
        return True

    return False


def emit_bar_close_event(symbol: str, closed_bar_time_iso: str) -> bool:
    last_bar = last_emitted_bar.get(symbol)
    if last_bar == closed_bar_time_iso:
        return False

    payload = {
        "symbol": symbol,
        "timeframe": "Min5",
        "bar_marker": closed_bar_time_iso,
        "ts": int(datetime.now(tz=timezone.utc).timestamp()),
    }

    try:
        push_bar_event_payload(payload, ttl_seconds=REDIS_EVENT_TTL_SECONDS)
    except TypeError:
        push_bar_event_payload(payload)

    last_emitted_bar[symbol] = closed_bar_time_iso
    return True


def fetch_symbols_auto() -> list[str]:
    url = f"{MEXC_REST_BASE}/api/v1/contract/detail"
    r = requests.get(url, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    payload = r.json()

    rows = payload.get("data") or []
    result = []

    for row in rows:
        symbol = (row.get("symbol") or "").upper()
        if not symbol:
            continue

        if not symbol.endswith("_USDT"):
            continue

        state = row.get("state")
        if state not in (0, "0", None):
            continue

        api_allowed = row.get("apiAllowed")
        if api_allowed is False:
            continue

        settle = (row.get("settleCoin") or row.get("quoteCoin") or "").upper()
        if settle and settle != "USDT":
            continue

        result.append(symbol)

    result = sorted(set(result))
    if MAX_AUTO_SYMBOLS > 0:
        result = result[:MAX_AUTO_SYMBOLS]
    return result


def load_symbols() -> list[str]:
    if AUTO_LOAD_SYMBOLS:
        try:
            symbols = fetch_symbols_auto()
            print(f"[{now_str()}] auto symbols loaded | count={len(symbols)}", flush=True)
            return symbols
        except Exception as e:
            print(f"[{now_str()}] auto symbols load failed | {e}", flush=True)
            print(f"[{now_str()}] fallback to manual symbols | count={len(MANUAL_SYMBOLS)}", flush=True)
            return MANUAL_SYMBOLS

    print(f"[{now_str()}] manual symbols loaded | count={len(MANUAL_SYMBOLS)}", flush=True)
    return MANUAL_SYMBOLS


def fetch_klines_rest(symbol: str, interval: str):
    url = f"{MEXC_REST_BASE}/api/v1/contract/kline/{symbol}"
    params = {"interval": interval}
    r = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    payload = r.json()

    data = payload.get("data") or {}
    times = data.get("time") or []
    opens = data.get("open") or []
    highs = data.get("high") or []
    lows = data.get("low") or []
    closes = data.get("close") or []
    vols = data.get("vol") or []

    n = min(len(times), len(opens), len(highs), len(lows), len(closes), len(vols))
    out = []
    for i in range(n):
        out.append(
            make_candle(
                ts_sec=int(times[i]),
                o=opens[i],
                h=highs[i],
                l=lows[i],
                c=closes[i],
                v=vols[i],
            )
        )
    return out


def bootstrap_symbol(symbol: str):
    try:
        arr5 = fetch_klines_rest(symbol, "Min5")[-BOOTSTRAP_5M_LIMIT:]
        arr1 = fetch_klines_rest(symbol, "Min60")[-BOOTSTRAP_1H_LIMIT:]

        if len(arr5) >= 2:
            closed_5m[symbol] = arr5[:-1][-KEEP_5M:]
            current_5m[symbol] = arr5[-1]
        elif len(arr5) == 1:
            closed_5m[symbol] = []
            current_5m[symbol] = arr5[-1]
        else:
            closed_5m[symbol] = []

        if len(arr1) >= 2:
            closed_1h[symbol] = arr1[:-1][-KEEP_1H:]
            current_1h[symbol] = arr1[-1]
        elif len(arr1) == 1:
            closed_1h[symbol] = []
            current_1h[symbol] = arr1[-1]
        else:
            closed_1h[symbol] = []

        redis_write_if_changed(symbol, "5m", closed_5m[symbol])
        redis_write_if_changed(symbol, "1h", closed_1h[symbol])

        print(
            f"[{now_str()}] bootstrap ok | {symbol} | 5m={len(closed_5m[symbol])} 1h={len(closed_1h[symbol])}",
            flush=True,
        )
    except Exception as e:
        print(f"[{now_str()}] bootstrap error | {symbol} | {e}", flush=True)


def parse_ws_message(raw: str):
    try:
        msg = json.loads(raw)
    except Exception:
        return None

    if not isinstance(msg, dict):
        return None

    channel = msg.get("channel")
    if channel != "push.kline":
        return None

    data = msg.get("data") or {}
    symbol = (data.get("symbol") or "").upper()
    interval = data.get("interval")
    t = data.get("t")

    if not symbol or not interval or t is None:
        return None

    candle = make_candle(
        ts_sec=int(t),
        o=data.get("o"),
        h=data.get("h"),
        l=data.get("l"),
        c=data.get("c"),
        v=data.get("q"),
    )

    return symbol, interval, candle


def handle_kline_update(symbol: str, interval: str, candle: dict):
    if interval == "Min5":
        current_store = current_5m
        closed_store = closed_5m
        keep = KEEP_5M
        tf_key = "5m"
    elif interval == "Min60":
        current_store = current_1h
        closed_store = closed_1h
        keep = KEEP_1H
        tf_key = "1h"
    else:
        return

    prev_current = current_store.get(symbol)

    if prev_current is None:
        current_store[symbol] = candle
        return

    if prev_current["time"] == candle["time"]:
        current_store[symbol] = candle
        return

    arr = closed_store.setdefault(symbol, [])
    arr.append(prev_current)
    trim_keep(arr, keep)

    changed = redis_write_if_changed(symbol, tf_key, arr)

    if interval == "Min5" and changed:
        emit_bar_close_event(symbol, prev_current["time"])
        print(f"[{now_str()}] 5m close | {symbol} | {prev_current['time']}", flush=True)

    if interval == "Min60" and changed:
        print(f"[{now_str()}] 1h close | {symbol} | {prev_current['time']}", flush=True)

    current_store[symbol] = candle


async def ws_ping_loop(ws):
    while True:
        await asyncio.sleep(WS_PING_SECONDS)
        try:
            await ws.send(json.dumps({"method": "ping"}))
        except Exception:
            return


def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


async def subscribe_ws(ws, symbols: list[str]):
    for batch in chunked(symbols, 40):
        for symbol in batch:
            await ws.send(json.dumps({
                "method": "sub.kline",
                "param": {"symbol": symbol, "interval": "Min5"},
            }))
            await ws.send(json.dumps({
                "method": "sub.kline",
                "param": {"symbol": symbol, "interval": "Min60"},
            }))

    print(f"[{now_str()}] subscriptions sent | symbols={len(symbols)}", flush=True)


async def ws_loop(symbols: list[str]):
    while True:
        try:
            print(f"[{now_str()}] connecting ws | {MEXC_WS_URL}", flush=True)

            async with websockets.connect(
                MEXC_WS_URL,
                ping_interval=None,
                ping_timeout=None,
                close_timeout=5,
                max_queue=None,
            ) as ws:
                print(f"[{now_str()}] ws connected", flush=True)

                ping_task = asyncio.create_task(ws_ping_loop(ws))
                try:
                    await subscribe_ws(ws, symbols)

                    async for raw in ws:
                        parsed = parse_ws_message(raw)
                        if not parsed:
                            continue

                        symbol, interval, candle = parsed

                        if not symbol_belongs_to_this_worker(symbol):
                            continue

                        handle_kline_update(symbol, interval, candle)
                finally:
                    ping_task.cancel()
                    try:
                        await ping_task
                    except Exception:
                        pass

        except Exception as e:
            print(f"[{now_str()}] ws reconnect after error | {e}", flush=True)
            await asyncio.sleep(WS_RECONNECT_DELAY)


async def main():
    print("market_ingestor.py started", flush=True)

    all_symbols = [s.upper() for s in load_symbols()]
    symbols = [s for s in all_symbols if symbol_belongs_to_this_worker(s)]

    print(
        f"[{now_str()}] ingestor shard | index={INGESTOR_SHARD_INDEX} "
        f"count={INGESTOR_SHARD_COUNT} symbols={len(symbols)}",
        flush=True,
    )

    for s in symbols:
        bootstrap_symbol(s)

    await ws_loop(symbols)


if __name__ == "__main__":
    asyncio.run(main())