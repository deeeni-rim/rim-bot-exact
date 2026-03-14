import asyncio
from datetime import datetime, timezone

from telegram import Bot
from telegram.error import TimedOut, RetryAfter, NetworkError

from config import (
    BOT_TOKEN,
    SEND_WORKERS,
    SEND_RETRY_MAX,
    SEND_RETRY_DELAYS,
    SEND_WORKER_PAUSE,
    SIGNAL_TTL_SECONDS,
)
from db import (
    init_db,
    claim_outbound_batch,
    mark_outbound_sent,
    mark_outbound_failed,
    release_outbound_for_retry,
)


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def utc_now():
    return datetime.now(timezone.utc)


def is_expired(created_at):
    if created_at is None:
        return False
    age = (utc_now() - created_at).total_seconds()
    return age > SIGNAL_TTL_SECONDS


async def send_one(bot: Bot, row: dict):
    telegram_id = row["telegram_id"]
    text = row["text"]

    await bot.send_message(
        chat_id=telegram_id,
        text=text,
        disable_web_page_preview=True,
    )


async def worker(worker_name: str, bot: Bot):
    while True:
        batch = claim_outbound_batch(worker_name, 1)

        if not batch:
            await asyncio.sleep(0.5)
            continue

        row = batch[0]
        msg_id = row["id"]
        created_at = row.get("created_at")
        attempts = int(row.get("attempts", 0))
        telegram_id = row["telegram_id"]
        symbol = row["symbol"]
        side = row["side"]

        try:
            if is_expired(created_at):
                print(
                    f"[{now_str()}] skip expired | user={telegram_id} | {symbol} | {side}",
                    flush=True,
                )
                mark_outbound_failed(msg_id, "expired")
                await asyncio.sleep(SEND_WORKER_PAUSE)
                continue

            await send_one(bot, row)

            print(
                f"[{now_str()}] SENT to user={telegram_id} | {symbol} | {side}",
                flush=True,
            )
            mark_outbound_sent(msg_id)

        except RetryAfter as e:
            delay = int(getattr(e, "retry_after", 10))
            print(
                f"[{now_str()}] send_message retry-after | user={telegram_id} | {symbol} | {delay}s",
                flush=True,
            )
            release_outbound_for_retry(msg_id, f"RetryAfter: {delay}s", delay)

        except (TimedOut, NetworkError) as e:
            if attempts + 1 >= SEND_RETRY_MAX:
                print(
                    f"[{now_str()}] send_message failed окончательно | user={telegram_id} | {symbol} | {side}",
                    flush=True,
                )
                mark_outbound_failed(msg_id, str(e))
            else:
                delay = SEND_RETRY_DELAYS[min(attempts, len(SEND_RETRY_DELAYS) - 1)]
                print(
                    f"[{now_str()}] send_message retry {attempts + 1}/{SEND_RETRY_MAX} | "
                    f"user={telegram_id} | {symbol} | {e}",
                    flush=True,
                )
                release_outbound_for_retry(msg_id, str(e), delay)

        except Exception as e:
            if attempts + 1 >= SEND_RETRY_MAX:
                print(
                    f"[{now_str()}] send_message failed окончательно | user={telegram_id} | {symbol} | {side} | {e}",
                    flush=True,
                )
                mark_outbound_failed(msg_id, str(e))
            else:
                delay = SEND_RETRY_DELAYS[min(attempts, len(SEND_RETRY_DELAYS) - 1)]
                print(
                    f"[{now_str()}] send_message retry {attempts + 1}/{SEND_RETRY_MAX} | "
                    f"user={telegram_id} | {symbol} | {e}",
                    flush=True,
                )
                release_outbound_for_retry(msg_id, str(e), delay)

        await asyncio.sleep(SEND_WORKER_PAUSE)


async def main():
    init_db()
    bot = Bot(token=BOT_TOKEN)

    print("sender_only.py started", flush=True)
    print(f"workers={SEND_WORKERS}", flush=True)

    tasks = [
        asyncio.create_task(worker(f"worker-{i+1}", bot))
        for i in range(SEND_WORKERS)
    ]

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())