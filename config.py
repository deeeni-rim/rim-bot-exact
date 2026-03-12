import os

BOT_TOKEN = os.getenv("BOT_TOKEN", "8650521394:AAEr6_w6y0T-tK68K77_bGPcoSEzqxyc5MQ")

# Основной ТФ сигналов
SCAN_TIMEFRAME = os.getenv("SCAN_TIMEFRAME", "Min5")

# ТФ фильтра тренда
FILTER_TIMEFRAME = "Min60"

# Пауза между циклами
SCAN_SLEEP_SECONDS = 20

# Автозагрузка всех символов
AUTO_LOAD_SYMBOLS = True

# Если выключишь автозагрузку
MANUAL_SYMBOLS = [
    "BTC_USDT",
    "ETH_USDT",
]

# Поставь с запасом
MAX_AUTO_SYMBOLS = 1000

# Скрытые настройки
EMA_LEN = 50
IMPULSE_LOOKBACK_H = 12
IMPULSE_MIN_PCT = 0.3
USE_VOL_FILTER = True
VOL_MA_LEN = 20

DEFAULT_SETTINGS = {
    "enable_long": 1,
    "enable_short": 1,
    "max_stop_pct": 3.0,
    "tp_rr": 1.0,
    "stop_buffer_pct": 1.0,
    "structure_sensitivity": 3,
    "is_active": 1,
}