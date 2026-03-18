import json
from typing import Any, Optional

from config import REDIS_URL

_client = None

BAR_CLOSE_QUEUE_KEY = "rim:queue:bar_close_payload"


def _get_client():
    global _client

    if _client is not None:
        return _client

    if not REDIS_URL:
        raise RuntimeError("REDIS_URL is not set")

    import redis
    _client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    return _client


redis_client = _get_client()


def redis_ping() -> bool:
    return bool(redis_client.ping())


def _dumps(value: Any) -> str:
    return json.dumps(value, default=str, separators=(",", ":"))


def _loads(raw: str | None) -> Optional[Any]:
    if not raw:
        return None
    return json.loads(raw)


def rkey_signal_lock(user_id: int, symbol: str, side: str, bar_marker: str) -> str:
    return f"rim:signal_lock:{user_id}:{symbol}:{side}:{bar_marker}"


def set_signal_lock(
    user_id: int,
    symbol: str,
    side: str,
    bar_marker: str,
    ttl_seconds: int = 86400,
) -> bool:
    key = rkey_signal_lock(user_id, symbol, side, bar_marker)
    return bool(redis_client.set(key, "1", nx=True, ex=ttl_seconds))


def push_bar_event_payload(payload: dict):
    redis_client.rpush(BAR_CLOSE_QUEUE_KEY, _dumps(payload))


def pop_bar_event_payload(timeout_seconds: int = 5) -> Optional[dict]:
    item = redis_client.blpop(BAR_CLOSE_QUEUE_KEY, timeout=timeout_seconds)
    if not item:
        return None

    _, raw = item
    return _loads(raw)