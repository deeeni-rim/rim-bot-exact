import json
import os
from typing import Any, Optional

from config import REDIS_URL

_client = None

BAR_CLOSE_QUEUE_KEY = "rim:queue:bar_close"


def _get_client():
    global _client

    if _client is not None:
        return _client

    if not REDIS_URL:
        raise RuntimeError("REDIS_URL is not set")

    try:
        import redis
    except ImportError as e:
        raise RuntimeError("redis package is not installed. Add redis to requirements.txt") from e

    _client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    return _client


def redis_ping() -> bool:
    client = _get_client()
    return bool(client.ping())


def set_json(key: str, value: Any, ex: Optional[int] = None):
    client = _get_client()
    payload = json.dumps(value, default=str)
    client.set(key, payload, ex=ex)


def get_json(key: str) -> Optional[Any]:
    client = _get_client()
    raw = client.get(key)
    if not raw:
        return None
    return json.loads(raw)


def delete_key(key: str):
    client = _get_client()
    client.delete(key)


def rkey_symbol_state(symbol: str) -> str:
    return f"rim:symbol:{symbol}:state"


def rkey_symbol_candles(symbol: str, timeframe: str) -> str:
    return f"rim:symbol:{symbol}:candles:{timeframe}"


def rkey_signal_lock(user_id: int, symbol: str, side: str, bar_marker: str) -> str:
    return f"rim:signal_lock:{user_id}:{symbol}:{side}:{bar_marker}"


def rkey_bar_event_lock(symbol: str, timeframe: str, bar_marker: str) -> str:
    return f"rim:bar_event_lock:{symbol}:{timeframe}:{bar_marker}"


def save_symbol_state(symbol: str, state: dict, ex: int = 86400):
    set_json(rkey_symbol_state(symbol), state, ex=ex)


def load_symbol_state(symbol: str) -> Optional[dict]:
    return get_json(rkey_symbol_state(symbol))


def save_symbol_candles(symbol: str, timeframe: str, candles: list, ex: int = 86400):
    set_json(rkey_symbol_candles(symbol, timeframe), candles, ex=ex)


def load_symbol_candles(symbol: str, timeframe: str) -> Optional[list]:
    return get_json(rkey_symbol_candles(symbol, timeframe))


def save_symbol_candles_5m(symbol: str, candles: list, ex: int = 86400):
    save_symbol_candles(symbol, "5m", candles, ex=ex)


def save_symbol_candles_1h(symbol: str, candles: list, ex: int = 86400):
    save_symbol_candles(symbol, "1h", candles, ex=ex)


def load_symbol_candles_5m(symbol: str) -> Optional[list]:
    return load_symbol_candles(symbol, "5m")


def load_symbol_candles_1h(symbol: str) -> Optional[list]:
    return load_symbol_candles(symbol, "1h")


def set_signal_lock(user_id: int, symbol: str, side: str, bar_marker: str, ttl_seconds: int = 86400) -> bool:
    client = _get_client()
    key = rkey_signal_lock(user_id, symbol, side, bar_marker)
    return bool(client.set(key, "1", nx=True, ex=ttl_seconds))


def publish_bar_close(symbol: str, timeframe: str, bar_marker: str, ttl_seconds: int = 86400) -> bool:
    client = _get_client()
    lock_key = rkey_bar_event_lock(symbol, timeframe, bar_marker)

    # Чтобы не публиковать одно и то же закрытие по много раз
    locked = client.set(lock_key, "1", nx=True, ex=ttl_seconds)
    if not locked:
        return False

    event = {
        "symbol": symbol,
        "timeframe": timeframe,
        "bar_marker": bar_marker,
    }
    client.rpush(BAR_CLOSE_QUEUE_KEY, json.dumps(event))
    return True


def pop_bar_close_event(timeout_seconds: int = 5) -> Optional[dict]:
    client = _get_client()
    item = client.blpop(BAR_CLOSE_QUEUE_KEY, timeout=timeout_seconds)
    if not item:
        return None

    _, raw = item
    return json.loads(raw)