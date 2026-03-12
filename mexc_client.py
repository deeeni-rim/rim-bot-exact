import requests
import pandas as pd

BASE_URL = "https://api.mexc.com"

EXCLUDE_KEYWORDS = {
    "STOCK", "XAU", "XAG", "GOLD", "SILVER",
    "WTI", "BRENT", "NASDAQ", "DJI", "SPX"
}

QUOTE_SUFFIX = "_USDT"


def is_crypto_usdt_symbol(symbol: str) -> bool:
    s = str(symbol).upper().strip()

    if not s.endswith(QUOTE_SUFFIX):
        return False

    if any(word in s for word in EXCLUDE_KEYWORDS):
        return False

    return True


def get_contract_symbols(max_auto_symbols: int = 0):
    url = f"{BASE_URL}/api/v1/contract/detail"
    r = requests.get(url, timeout=20)
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

    if max_auto_symbols and max_auto_symbols > 0:
        symbols = symbols[:max_auto_symbols]

    return symbols


def get_klines(symbol: str, interval: str, limit: int = 150):
    url = f"{BASE_URL}/api/v1/contract/kline/{symbol}?interval={interval}&limit={limit}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    payload = r.json()

    data = payload.get("data")
    if not data:
        return None

    df = pd.DataFrame(data)

    for col in ["open", "high", "low", "close", "vol"]:
        if col in df.columns:
            df[col] = df[col].astype(float)

    return df