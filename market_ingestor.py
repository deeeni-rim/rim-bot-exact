import asyncio
import json
import math
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests
import websockets

import config as cfg
from redis_state import redis_client, push_bar_event_payload


# -----------------------------
# CONFIG / DEFAULTS
# -----------------------------
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

BOOTSTRAP_5M_LIMIT = max(KEEP_5M + 5, 130)
BOOTSTRAP_1H_LIMIT = max(KEEP_1H + 5, 90)

WS_RECONNECT_DELAY = 3
HTTP_TIMEOUT = 20


# -----------------------------
# IN-MEMORY STATE
# -----------------------------
closed_5m = {}
closed_1h = {}

current_5m = {}
current_1h = {}

last_written_5m = {}
last_written_1h = {}
last_emitted_bar = {}


# -----------------------------
# HELPERS
# -----------------------------
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def to_iso_utc_from_sec(ts_sec: int) -> str:
    return datetime.fromtimestamp(ts_sec, tz=timezone.utc).isoformat()


def stable_partition(symbol: str, shard_count: int) -> int:
    # встроенный hash в Python нестабилен между рестартами, поэтому свой
    acc = 0
    for ch in symbol:
        acc = (acc * 131 + ord(ch)) % 1_000_000_007
    return acc % shard_count


def symbol_belongs_to_this_worker(symbol: str) -> bool:
    return stable_partition(symbol, INGESTOR_SHARD_COUNT) == INGESTOR_SHARD_INDEX


def compact_json(obj) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def trim_keep(arr: list[dict], keep: int):
    if len(arr) > keep:
        del arr[:-keep]


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
        "ts": int(time.time()),
    }

    try:
        push_bar_event_payload(payload, ttl_seconds=REDIS_EVENT_TTL_SECONDS)
    except TypeError:
        # если в твоей функции нет ttl_seconds
        push_bar_event_payload(payload)

    last_emitted_bar[symbol] = closed_bar_time_iso
    return True


def make_candle(ts_sec: int, o, h, l, c, v):
    return {
        "time": to_iso_utc_from_sec(int(ts_sec)),
        "open": safe_float(o),
        "high": safe_float(h),
        "low": safe_float(l),
        "close": safe_float(c),
        "vol": safe_float(v),
    }


# -----------------------------
# SYMBOL LOADING
# -----------------------------
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

        # берём только USDT perpetual/contract symbols
        if not symbol.endswith("_USDT"):
            continue

        # если поле apiAllowed есть и оно false — пропускаем
        if row.get("apiAllowed") is False:
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

    return MANUAL_SYMBOLS


# -----------------------------
# REST BOOTSTRAP
# -----------------------------
def fetch_klines_rest(symbol: str, interval: str):
    """
    MEXC contract kline:
    GET /api/v1/contract/kline/{symbol}?interval=Min5
    Возвращает массивы data.time/open/high/low/close/vol
    """
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
        arr5 = fetch_klines_rest(symbol, "Min5")
        arr1 = fetch_klines_rest(symbol, "Min60")

        # Чтобы работать только с закрытыми свечами:
        # последнюю свечу считаем "текущей формирующейся" и храним отдельно в памяти.
        if arr5:
            if len(arr5) >= 2:
                closed_5m[symbol] = arr5[:-1][-KEEP_5M:]
                current_5m[symbol] = arr5[-1]
            else:
                closed_5m[symbol] = []
                current_5m[symbol] = arr5[-1]
        else:
            closed_5m[symbol] = []

        if arr1:
            if len(arr1) >= 2:
                closed_1h[symbol] = arr1[:-1][-KEEP_1H:]
                current_1h[symbol] = arr1[-1]
            else:
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


# -----------------------------
# WS PARSING
# -----------------------------
def parse_ws_message(raw: str):
    try:
        msg = json.loads(raw)
    except Exception:
        return None

    if not isinstance(msg, dict):
        return None

    if msg.get("channel") != "push.kline":
        return None

    data = msg.get("data") or {}
    symbol = (data.get("symbol") or msg.get("symbol") or "").upper()
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


# -----------------------------
# CLOSED-CANDLE LOGIC
# -----------------------------
def handle_kline_update(symbol: str, interval: str, candle: dict):
    """
    В ws MEXC у kline апдейта нет явного флага 'closed' в примере доки.
    Поэтому закрытие определяем так:
    - пока приходит та же свеча (тот же time) — просто обновляем current_* в памяти
    - как только пришла свеча с новым time — предыдущая current_* считается закрытой
    """
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
        # всё ещё апдейт текущей незакрытой свечи
        current_store[symbol] = candle
        return

    # пришла новая свеча => предыдущая закрылась
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


# -----------------------------
# WS SUBSCRIBE
# -----------------------------
def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


async def subscribe_ws(ws, symbols: list[str]):
    for batch in chunked(symbols, 50):
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
            async with websockets.connect(MEXC_WS_URL, ping_interval=20, ping_timeout=20) as ws:
                print(f"[{now_str()}] ws connected", flush=True)
                await subscribe_ws(ws, symbols)

                async for raw in ws:
                    parsed = parse_ws_message(raw)
                    if not parsed:
                        continue

                    symbol, interval, candle = parsed

                    if not symbol_belongs_to_this_worker(symbol):
                        continue

                    handle_kline_update(symbol, interval, candle)

        except Exception as e:
            print(f"[{now_str()}] ws reconnect after error | {e}", flush=True)
            await asyncio.sleep(WS_RECONNECT_DELAY)


# -----------------------------
# MAIN
# -----------------------------
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