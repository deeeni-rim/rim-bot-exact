import asyncio
from contextlib import suppress

from telegram import Bot
from telegram.request import HTTPXRequest

from config import BOT_TOKEN
from db import init_db
from scanner import run_scanner


async def sender_worker(bot: Bot, send_queue: asyncio.Queue):
    while True:
        chat_id, text, symbol, side = await send_queue.get()

        sent = False
        last_error = None

        for attempt in range(3):
            try:
                await bot.send_message(chat_id=chat_id, text=text)
                print(
                    f"SENT to user={chat_id} | {symbol} | {side}",
                    flush=True
                )
                sent = True
                break
            except Exception as e:
                last_error = e
                await asyncio.sleep(1.0 * (attempt + 1))

        if not sent:
            print(
                f"send_message error | user={chat_id} | {symbol} | {last_error}",
                flush=True
            )

        send_queue.task_done()
        await asyncio.sleep(0.20)


async def main():
    print("scanner_only.py: start main()", flush=True)

    init_db()
    print("scanner_only.py: init_db() done", flush=True)

    request = HTTPXRequest(
        connection_pool_size=50,
        pool_timeout=30.0,
        connect_timeout=30.0,
        read_timeout=30.0,
        write_timeout=30.0,
    )
    bot = Bot(token=BOT_TOKEN, request=request)
    print("scanner_only.py: Bot created", flush=True)

    send_queue = asyncio.Queue()
    sender_task = asyncio.create_task(sender_worker(bot, send_queue))

    try:
        while True:
            try:
                print("scanner_only.py: before run_scanner()", flush=True)
                await run_scanner(bot, send_queue)
                print("scanner_only.py: after run_scanner()", flush=True)
            except Exception as e:
                print(f"scanner_only.py: FATAL scanner loop error: {e}", flush=True)
                await asyncio.sleep(10)
    finally:
        sender_task.cancel()
        with suppress(asyncio.CancelledError):
            await sender_task


if __name__ == "__main__":
    asyncio.run(main())