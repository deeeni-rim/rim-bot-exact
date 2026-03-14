import asyncio
from datetime import datetime

from telegram import Bot
from telegram.request import HTTPXRequest

from config import (
    BOT_TOKEN,
    SEND_WORKERS,
    SEND_DELAY_SECONDS,
    SEND_RETRIES,
    SENDER_POLL_SECONDS,
)
from db import (
    init_db,
    fetch_pending_messages,
    mark_message_sent,
    mark_message_retry,
    mark_message_failed,
)


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


async def send_one(bot: Bot, item: dict):
    queue_id = item["id"]
    telegram_id = item["telegram_id"]
    text = item["text"]
    symbol = item.get("symbol")
    side = item.get("side")
    retry_count = int(item.get("retry_count", 0))

    try:
        await bot.send_message(chat_id=telegram_id, text=text)

        print(
            f"[{now_str()}] SENT to user={telegram_id} | {symbol} | {side}",
            flush=True,
        )
        mark_message_sent(queue_id)

    except Exception as e:
        err = str(e)

        if retry_count + 1 >= SEND_RETRIES:
            print(
                f"[{now_str()}] send_message failed окончательно | user={telegram_id} | {symbol} | {side} | {err}",
                flush=True,
            )
            mark_message_failed(queue_id, err)
        else:
            print(
                f"[{now_str()}] send_message retry {retry_count + 1}/{SEND_RETRIES} | user={telegram_id} | {symbol} | {err}",
                flush=True,
            )
            mark_message_retry(queue_id, err)


async def sender_worker(name: str, bot: Bot):
    while True:
        items = await asyncio.to_thread(fetch_pending_messages, 10)

        if not items:
            await asyncio.sleep(SENDER_POLL_SECONDS)
            continue

        for item in items:
            await send_one(bot, item)
            await asyncio.sleep(SEND_DELAY_SECONDS)


async def main():
    init_db()

    request = HTTPXRequest(
        connection_pool_size=200,
        pool_timeout=10.0,
        connect_timeout=10.0,
        read_timeout=15.0,
        write_timeout=15.0,
    )

    bot = Bot(token=BOT_TOKEN, request=request)

    tasks = [
        asyncio.create_task(sender_worker(f"sender-{i+1}", bot))
        for i in range(SEND_WORKERS)
    ]

    print(f"sender_only.py started with {SEND_WORKERS} workers", flush=True)
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())