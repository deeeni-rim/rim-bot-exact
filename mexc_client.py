import requests
import pandas as pd
from typing import List
from config import AUTO_LOAD_SYMBOLS, MANUAL_SYMBOLS, MAX_AUTO_SYMBOLS

BASE_URL = "https://api.mexc.com"
_SESSION = requests.Session()


def get_contract_symbols() -> List[str]:
    if not AUTO_LOAD_SYMBOLS:
        return MANUAL_SYMBOLS

    url = f"{BASE_URL}/api/v1/contract/detail"
    r = _SESSION.get(url, timeout=20)
    r.raise_for_status()
    payload = r.json()
    data = payload.get("data")

    if data is None:
        return MANUAL_SYMBOLS

    if isinstance(data, dict):
        contracts = [data]
    else:
        contracts = data

    symbols = []
    for c in contracts:
        symbol = c.get("symbol")
        state = c.get("state")
        pair_type = c.get("type", 1)
        quote = c.get("quoteCoin")
        hidden = c.get("isHidden", False)
        if symbol and state == 0 and pair_type == 1 and quote == "USDT" and not hidden:
            symbols.append(symbol)

    symbols = sorted(set(symbols))
    if MAX_AUTO_SYMBOLS > 0:
        symbols = symbols[:MAX_AUTO_SYMBOLS]
    return symbols or MANUAL_SYMBOLS


def get_klines(symbol: str, interval: str, limit: int = 200):
    url = f"{BASE_URL}/api/v1/contract/kline/{symbol}?interval={interval}&limit={limit}"
    r = _SESSION.get(url, timeout=20)
    r.raise_for_status()
    payload = r.json()
    data = payload.get("data")

    if not data:
        return None

    # Futures kline can come as dict of arrays or list of dicts.
    if isinstance(data, dict):
        df = pd.DataFrame(data)
    else:
        df = pd.DataFrame(data)

    # Normalize expected columns.
    rename_map = {
        "a": "amount",
        "v": "vol",
        "t": "time",
    }
    for old, new in rename_map.items():
        if old in df.columns and new not in df.columns:
            df[new] = df[old]

    required = ["open", "high", "low", "close"]
    if not all(col in df.columns for col in required):
        return None

    if "vol" not in df.columns:
        if "amount" in df.columns:
            df["vol"] = df["amount"]
        else:
            df["vol"] = 0

    for col in ["open", "high", "low", "close", "vol"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    for col in ["time", "timestamp", "ts"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)
    return df if not df.empty else None
