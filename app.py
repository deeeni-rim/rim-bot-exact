import asyncio

from db import init_db
from bot import build_bot_app
from scanner import run_scanner


async def main():
    init_db()

    bot_app = build_bot_app()
    await bot_app.initialize()
    await bot_app.start()
    await bot_app.updater.start_polling()

    asyncio.create_task(run_scanner(bot_app))

    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    asyncio.run(main())
