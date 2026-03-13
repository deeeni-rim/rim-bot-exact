import asyncio
from telegram import Bot
from telegram.request import HTTPXRequest

from config import BOT_TOKEN
from db import init_db
from scanner import run_scanner


async def main():
    init_db()

    request = HTTPXRequest(
        connection_pool_size=50,
        pool_timeout=30.0,
        connect_timeout=20.0,
        read_timeout=30.0,
        write_timeout=30.0,
    )

    bot = Bot(token=BOT_TOKEN, request=request)

    print("scanner_only.py: before run_scanner()", flush=True)
    await run_scanner(bot)


if __name__ == "__main__":
    asyncio.run(main())