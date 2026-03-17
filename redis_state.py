import json
import os
from typing import Any, Optional

REDIS_URL = os.getenv("REDIS_URL", "")

_client = None


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


def rkey_symbol_state(symbol: str) -> str:
    return f"rim:symbol:{symbol}:state"


def rkey_symbol_5m(symbol: str) -> str:
    return f"rim:symbol:{symbol}:candles:5m"


def rkey_symbol_1h(symbol: str) -> str:
    return f"rim:symbol:{symbol}:candles:1h"


def rkey_signal_lock(user_id: int, symbol: str, side: str, bar_marker: str) -> str:
    return f"rim:signal_lock:{user_id}:{symbol}:{side}:{bar_marker}"


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


def set_signal_lock(user_id: int, symbol: str, side: str, bar_marker: str, ttl_seconds: int = 86400) -> bool:
    client = _get_client()
    key = rkey_signal_lock(user_id, symbol, side, bar_marker)
    return bool(client.set(key, "1", nx=True, ex=ttl_seconds))


def save_symbol_state(symbol: str, state: dict, ex: int = 86400):
    set_json(rkey_symbol_state(symbol), state, ex=ex)


def load_symbol_state(symbol: str) -> Optional[dict]:
    return get_json(rkey_symbol_state(symbol))


def save_symbol_candles_5m(symbol: str, candles: list, ex: int = 86400):
    set_json(rkey_symbol_5m(symbol), candles, ex=ex)


def save_symbol_candles_1h(symbol: str, candles: list, ex: int = 86400):
    set_json(rkey_symbol_1h(symbol), candles, ex=ex)


def load_symbol_candles_5m(symbol: str) -> Optional[list]:
    return get_json(rkey_symbol_5m(symbol))


def load_symbol_candles_1h(symbol: str) -> Optional[list]:
    return get_json(rkey_symbol_1h(symbol))