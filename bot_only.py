import asyncio
from db import init_db
from bot import build_bot_app


async def main():
    init_db()

    while True:
        try:
            bot_app = build_bot_app()

            await bot_app.initialize()
            await bot_app.start()
            await bot_app.updater.start_polling()

            print("Bot service started.")

            while True:
                await asyncio.sleep(3600)

        except Exception as e:
            print(f"FATAL bot loop error: {e}")
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(main())