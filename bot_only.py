import asyncio
from telegram.error import Conflict
from db import init_db
from bot import build_bot_app


async def run_bot():
    bot_app = build_bot_app()

    await bot_app.initialize()
    await bot_app.start()

    try:
        await bot_app.updater.start_polling(drop_pending_updates=True)
        print("Bot service started.")

        while True:
            await asyncio.sleep(3600)

    finally:
        try:
            await bot_app.updater.stop()
        except Exception as e:
            print(f"updater.stop error: {e}")

        try:
            await bot_app.stop()
        except Exception as e:
            print(f"app.stop error: {e}")

        try:
            await bot_app.shutdown()
        except Exception as e:
            print(f"app.shutdown error: {e}")


async def main():
    init_db()

    while True:
        try:
            await run_bot()
        except Conflict as e:
            print(f"Conflict detected: {e}")
            print("Sleeping 15 seconds and retrying polling...")
            await asyncio.sleep(15)
        except Exception as e:
            print(f"Bot fatal error: {e}")
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(main())