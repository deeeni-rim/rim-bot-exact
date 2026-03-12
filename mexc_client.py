import time
import requests
import pandas as pd

BASE_URL = "https://api.mexc.com"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "rim-bot/1.0"
})

EXCLUDE_KEYWORDS = {
    "STOCK", "XAU", "XAG", "GOLD", "SILVER",
    "WTI", "BRENT", "NASDAQ", "DJI", "SPX"
}

QUOTE_SUFFIX = "_USDT"

_KLINES_CACHE = {}
_SYMBOLS_CACHE = {
    "ts": 0,
    "data": []
}

KLINES_TTL_SEC = 20
SYMBOLS_TTL_SEC = 60 * 30


def is_crypto_usdt_symbol(symbol: str) -> bool:
    s = str(symbol).upper().strip()

    if not s.endswith(QUOTE_SUFFIX):
        return False

    if any(word in s for word in EXCLUDE_KEYWORDS):
        return False

    return True


def get_contract_symbols(max_auto_symbols: int = 0):
    now = time.time()

    if _SYMBOLS_CACHE["data"] and (now - _SYMBOLS_CACHE["ts"] < SYMBOLS_TTL_SEC):
        symbols = _SYMBOLS_CACHE["data"]
    else:
        url = f"{BASE_URL}/api/v1/contract/detail"
        r = SESSION.get(url, timeout=20)
        r.raise_for_status()
        payload = r.json()

        rows = payload.get("data", [])
        symbols = []

        for item in rows:
            symbol = str(item.get("symbol", "")).upper().strip()

            if not is_crypto_usdt_symbol(symbol):
                continue

            symbols.append(symbol)

        symbols = sorted(set(symbols))

        _SYMBOLS_CACHE["ts"] = now
        _SYMBOLS_CACHE["data"] = symbols

    if max_auto_symbols and max_auto_symbols > 0:
        return symbols[:max_auto_symbols]

    return symbols


def get_klines(symbol: str, interval: str, limit: int = 150):
    now = time.time()
    cache_key = (symbol, interval, limit)

    cached = _KLINES_CACHE.get(cache_key)
    if cached and (now - cached["ts"] < KLINES_TTL_SEC):
        return cached["df"]

    url = f"{BASE_URL}/api/v1/contract/kline/{symbol}?interval={interval}&limit={limit}"
    r = SESSION.get(url, timeout=20)
    r.raise_for_status()
    payload = r.json()

    data = payload.get("data")
    if not data:
        return None

    df = pd.DataFrame(data)

    for col in ["open", "high", "low", "close", "vol"]:
        if col in df.columns:
            df[col] = df[col].astype(float)

    _KLINES_CACHE[cache_key] = {
        "ts": now,
        "df": df
    }

    return df