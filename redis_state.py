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


def redis_ping() -> bool:
    client = _get_client()
    return bool(client.ping())


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
    client = _get_client()
    key = rkey_signal_lock(user_id, symbol, side, bar_marker)
    return bool(client.set(key, "1", nx=True, ex=ttl_seconds))


def push_bar_event_payload(payload: dict):
    client = _get_client()
    client.rpush(BAR_CLOSE_QUEUE_KEY, _dumps(payload))


def pop_bar_event_payload(timeout_seconds: int = 5) -> Optional[dict]:
    client = _get_client()
    item = client.blpop(BAR_CLOSE_QUEUE_KEY, timeout=timeout_seconds)
    if not item:
        return None

    _, raw = item
    return _loads(raw)