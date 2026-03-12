import asyncio
from telegram.ext import Application
from db import init_db
from config import BOT_TOKEN
from scanner import run_scanner


async def main():
    init_db()

    bot_app = Application.builder().token(BOT_TOKEN).build()
    await bot_app.initialize()
    await bot_app.start()

    print("Scanner service started.")

    while True:
        try:
            await run_scanner(bot_app)
        except Exception as e:
            print(f"FATAL scanner loop error: {e}")
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(main())