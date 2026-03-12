RIM BOT EXACT v2

ЧТО ДОБАВЛЕНО:
1) Автозагрузка всех доступных USDT futures-контрактов MEXC
2) Сохранение состояния после перезапуска (позиции и последние сигналы)
3) Лог всех отправленных сигналов в SQLite
4) Команда /mysettings

ЛОКАЛЬНЫЙ ЗАПУСК:
1. Установи Python 3.11+
2. Открой эту папку в терминале
3. Выполни: pip install -r requirements.txt
4. Открой config.py
5. Вставь токен бота в BOT_TOKEN
6. Для первого теста можно поставить AUTO_LOAD_SYMBOLS=False и оставить 3-5 монет в MANUAL_SYMBOLS
7. Запусти: python app.py
8. Открой бота и отправь /start

ДЕПЛОЙ В ОБЛАКО:
Лучше всего как background worker на Render.
Стартовая команда: python app.py
Build command: pip install -r requirements.txt
Переменные окружения:
- BOT_TOKEN=твой токен
- SCAN_TIMEFRAME=Min5 или Min15
- MAX_AUTO_SYMBOLS=0
- SCAN_SLEEP_SECONDS=20
