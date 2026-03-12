import asyncio
from telegram import Bot
from config import BOT_TOKEN
from db import init_db
from scanner import run_scanner


async def main():
    init_db()

    bot = Bot(token=BOT_TOKEN)
    print("Scanner service started.")

    while True:
        try:
            await run_scanner(bot)
        except Exception as e:
            print(f"FATAL scanner loop error: {e}")
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(main())