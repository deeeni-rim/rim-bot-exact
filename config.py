import os

BOT_TOKEN = os.getenv("BOT_TOKEN", "PASTE_YOUR_BOT_TOKEN_HERE")

# Основной ТФ сигналов
SCAN_TIMEFRAME = os.getenv("SCAN_TIMEFRAME", "Min5")

# ТФ фильтра тренда
FILTER_TIMEFRAME = os.getenv("FILTER_TIMEFRAME", "Min60")

# Пауза между циклами
SCAN_SLEEP_SECONDS = int(os.getenv("SCAN_SLEEP_SECONDS", "5"))

# Автозагрузка всех символов
AUTO_LOAD_SYMBOLS = os.getenv("AUTO_LOAD_SYMBOLS", "1") == "1"

# Если выключишь автозагрузку
MANUAL_SYMBOLS = [
    "BTC_USDT",
    "ETH_USDT",
]

# Максимум автосимволов
MAX_AUTO_SYMBOLS = int(os.getenv("MAX_AUTO_SYMBOLS", "1000"))

# Скрытые настройки стратегии
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