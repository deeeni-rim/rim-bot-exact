import os

BOT_TOKEN = os.getenv("BOT_TOKEN", "PASTE_YOUR_BOT_TOKEN_HERE")

# Основной ТФ сигналов
SCAN_TIMEFRAME = os.getenv("SCAN_TIMEFRAME", "Min5")
FILTER_TIMEFRAME = os.getenv("FILTER_TIMEFRAME", "Min60")

# Пауза между циклами старого сканера
SCAN_SLEEP_SECONDS = int(os.getenv("SCAN_SLEEP_SECONDS", "5"))

# Автозагрузка символов
AUTO_LOAD_SYMBOLS = os.getenv("AUTO_LOAD_SYMBOLS", "1") == "1"
MANUAL_SYMBOLS = [
    "BTC_USDT",
    "ETH_USDT",
]
MAX_AUTO_SYMBOLS = int(os.getenv("MAX_AUTO_SYMBOLS", "1000"))

# Redis
REDIS_URL = os.getenv("REDIS_URL", "")

# MEXC Futures
MEXC_FUTURES_REST_BASE = os.getenv("MEXC_FUTURES_REST_BASE", "https://api.mexc.com")
MEXC_FUTURES_WS = os.getenv("MEXC_FUTURES_WS", "wss://contract.mexc.com/edge")

# Sharding
INGESTOR_SHARD_INDEX = int(os.getenv("INGESTOR_SHARD_INDEX", "0"))
INGESTOR_SHARD_COUNT = int(os.getenv("INGESTOR_SHARD_COUNT", "1"))

SIGNAL_SHARD_INDEX = int(os.getenv("SIGNAL_SHARD_INDEX", "0"))
SIGNAL_SHARD_COUNT = int(os.getenv("SIGNAL_SHARD_COUNT", "1"))

# Caching / polling
USERS_CACHE_SECONDS = int(os.getenv("USERS_CACHE_SECONDS", "10"))
BAR_EVENT_BLOCK_TIMEOUT = int(os.getenv("BAR_EVENT_BLOCK_TIMEOUT", "5"))

# Candle history sizes in Redis
REDIS_5M_LIMIT = int(os.getenv("REDIS_5M_LIMIT", "140"))
REDIS_1H_LIMIT = int(os.getenv("REDIS_1H_LIMIT", "100"))

# Strategy hidden settings
EMA_LEN = 50
IMPULSE_LOOKBACK_H = 12
IMPULSE_MIN_PCT = 0.3
USE_VOL_FILTER = True
VOL_MA_LEN = 20

DEFAULT_SETTINGS = {
    "enable_long": True,
    "enable_short": True,
    "max_stop_pct": 3.0,
    "tp_rr": 1.0,
    "stop_buffer_pct": 1.0,
    "structure_sensitivity": 3,
    "signals_enabled": True,
}