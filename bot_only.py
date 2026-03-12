import asyncio
from telegram.error import Conflict
from db import init_db
from bot import build_bot_app


async def run_bot_once():
    bot_app = build_bot_app()

    await bot_app.initialize()
    await bot_app.bot.delete_webhook(drop_pending_updates=True)
    await bot_app.start()
    await bot_app.updater.start_polling(drop_pending_updates=True)

    print("Bot service started.")

    try:
        while True:
            await asyncio.sleep(3600)
    finally:
        try:
            await bot_app.updater.stop()
        except Exception:
            pass
        try:
            await bot_app.stop()
        except Exception:
            pass
        try:
            await bot_app.shutdown()
        except Exception:
            pass


async def main():
    init_db()

    while True:
        try:
            await run_bot_once()
        except Conflict as e:
            print(f"Conflict detected: {e}. Waiting 20 seconds and retrying...")
            await asyncio.sleep(20)
        except Exception as e:
            print(f"Fatal bot error: {e}")
            await asyncio.sleep(10)


if __name__ == "__main__":
    asyncio.run(main())