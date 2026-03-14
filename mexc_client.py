import requests
import pandas as pd
from typing import List, Optional

BASE_URL = "https://contract.mexc.com"
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
})


def _safe_get(url: str, params: dict | None = None, timeout: int = 15):
    r = SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def get_contract_symbols(limit: int = 1000) -> List[str]:
    """
    Возвращает только futures USDT-пары с MEXC contract API.
    """
    url = f"{BASE_URL}/api/v1/contract/detail"
    data = _safe_get(url)

    items = data.get("data") or []
    symbols = []

    for item in items:
        symbol = item.get("symbol")
        if not symbol:
            continue

        # Только USDT perpetual/futures пары
        if not symbol.endswith("_USDT"):
            continue

        # При желании можно фильтровать по статусу,
        # но MEXC не всегда стабильно отдает поле состояния.
        symbols.append(symbol)

    return sorted(set(symbols))[:limit]


def get_klines(symbol: str, interval: str, limit: int = 200) -> Optional[pd.DataFrame]:
    """
    Получает futures candles с MEXC.
    interval: Min1 / Min5 / Min15 / Min60 / Hour4 / Day1 ...
    """
    url = f"{BASE_URL}/api/v1/contract/kline/{symbol}"
    params = {
        "interval": interval,
        "limit": limit,
    }

    data = _safe_get(url, params=params)
    raw = data.get("data")

    if not raw:
        return None

    # Ожидаемый формат:
    # {
    #   "time": [...],
    #   "open": [...],
    #   "close": [...],
    #   "high": [...],
    #   "low": [...],
    #   "vol": [...],
    #   "amount": [...]
    # }
    times = raw.get("time") or []
    opens = raw.get("open") or []
    highs = raw.get("high") or []
    lows = raw.get("low") or []
    closes = raw.get("close") or []
    vols = raw.get("vol") or raw.get("volume") or []

    if not times or not opens or not highs or not lows or not closes:
        return None

    min_len = min(len(times), len(opens), len(highs), len(lows), len(closes))
    if vols:
        min_len = min(min_len, len(vols))

    if min_len <= 0:
        return None

    df = pd.DataFrame({
        "time": times[:min_len],
        "open": opens[:min_len],
        "high": highs[:min_len],
        "low": lows[:min_len],
        "close": closes[:min_len],
        "vol": vols[:min_len] if vols else [0.0] * min_len,
    })

    if df.empty:
        return None

    for col in ["open", "high", "low", "close", "vol"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # MEXC contract API обычно отдаёт unix timestamp в секундах
    df["time"] = pd.to_datetime(df["time"], unit="s", errors="coerce")

    df = df.dropna(subset=["time", "open", "high", "low", "close"]).reset_index(drop=True)

    if df.empty:
        return None

    df = df.set_index("time")
    return df