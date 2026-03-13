import os

# После того как токен был засвечен, лучше перевыпусти его в BotFather
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

# Основной ТФ сигналов
SCAN_TIMEFRAME = os.getenv("SCAN_TIMEFRAME", "Min5")

# ТФ фильтра тренда
FILTER_TIMEFRAME = "Min60"

# Пауза между циклами
# 60 секунд здесь лишние — цикл и так долгий
SCAN_SLEEP_SECONDS = 5

# Автозагрузка всех символов
AUTO_LOAD_SYMBOLS = True

# Если выключишь автозагрузку
MANUAL_SYMBOLS = [
    "BTC_USDT",
    "ETH_USDT",
]

# Максимум символов
MAX_AUTO_SYMBOLS = 1000

# Скрытые настройки Pine
EMA_LEN = 50
IMPULSE_LOOKBACK_H = 12
IMPULSE_MIN_PCT = 0.3
USE_VOL_FILTER = True
VOL_MA_LEN = 20

# Параметры сканера
MAX_CONCURRENT_SYMBOLS = 12
SYMBOLS_REFRESH_EVERY_CYCLES = 10

# Отправка в Telegram
SEND_WORKERS = 2
SEND_DELAY_SECONDS = 0.20
SEND_RETRIES = 4
SEND_TIMEOUT_SECONDS = 20

# Дефолтные настройки пользователя
DEFAULT_SETTINGS = {
    "enable_long": True,
    "enable_short": True,
    "max_stop_pct": 3.0,
    "tp_rr": 1.0,
    "stop_buffer_pct": 1.0,
    "structure_sensitivity": 3,
    "signals_enabled": True,
}