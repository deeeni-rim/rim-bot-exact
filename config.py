import os

BOT_TOKEN = os.getenv("BOT_TOKEN", "")

SCAN_TIMEFRAME = os.getenv("SCAN_TIMEFRAME", "Min5")
FILTER_TIMEFRAME = "Min60"

AUTO_LOAD_SYMBOLS = True

MANUAL_SYMBOLS = [
    "BTC_USDT",
    "ETH_USDT",
]

MAX_AUTO_SYMBOLS = 1000

EMA_LEN = 50
IMPULSE_LOOKBACK_H = 12
IMPULSE_MIN_PCT = 0.3
USE_VOL_FILTER = True
VOL_MA_LEN = 20

# scanner
MAX_CONCURRENT_SYMBOLS = 25
SYMBOLS_REFRESH_EVERY_CYCLES = 10
SCAN_SLEEP_SECONDS = 2

# sender
SEND_WORKERS = 5
SEND_DELAY_SECONDS = 0.08
SEND_RETRIES = 3
SEND_TIMEOUT_SECONDS = 15
SENDER_POLL_SECONDS = 1

DEFAULT_SETTINGS = {
    "enable_long": True,
    "enable_short": True,
    "max_stop_pct": 3.0,
    "tp_rr": 1.0,
    "stop_buffer_pct": 1.0,
    "structure_sensitivity": 3,
    "signals_enabled": True,
}