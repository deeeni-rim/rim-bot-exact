import json
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
        raise RuntimeError("redis package is not installed") from e

    _client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    return _client


def redis_ping() -> bool:
    client = _get_client()
    return bool(client.ping())


def _dumps(value: Any) -> str:
    return json.dumps(value, default=str, separators=(",", ":"))


def _loads(raw: str | None) -> Optional[Any]:
    if not raw:
        return None
    return json.loads(raw)


def set_json(key: str, value: Any, ex: Optional[int] = None):
    client = _get_client()
    if ex is None:
        client.set(key, _dumps(value))
    else:
        client.set(key, _dumps(value), ex=ex)


def get_json(key: str) -> Optional[Any]:
    client = _get_client()
    return _loads(client.get(key))


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

    pipe = client.pipeline()
    pipe.set(lock_key, "1", nx=True, ex=ttl_seconds)
    results = pipe.execute()

    locked = bool(results[0])
    if not locked:
        return False

    event = {
        "symbol": symbol,
        "timeframe": timeframe,
        "bar_marker": bar_marker,
    }
    client.rpush(BAR_CLOSE_QUEUE_KEY, _dumps(event))
    return True


def pop_bar_close_event(timeout_seconds: int = 5) -> Optional[dict]:
    client = _get_client()
    item = client.blpop(BAR_CLOSE_QUEUE_KEY, timeout=timeout_seconds)
    if not item:
        return None
    _, raw = item
    return _loads(raw)


def save_symbol_bundle(symbol: str, state: dict, candles_5m: list | None, candles_1h: list | None, ex: int = 86400):
    """
    Один батч-запрос вместо нескольких отдельных.
    """
    client = _get_client()
    pipe = client.pipeline()

    pipe.set(rkey_symbol_state(symbol), _dumps(state), ex=ex)

    if candles_5m is not None:
        pipe.set(rkey_symbol_candles(symbol, "5m"), _dumps(candles_5m), ex=ex)

    if candles_1h is not None:
        pipe.set(rkey_symbol_candles(symbol, "1h"), _dumps(candles_1h), ex=ex)

    pipe.execute()