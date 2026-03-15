import os
import time
import requests
from datetime import datetime, timezone

from db import (
    init_db,
    claim_outbound_batch,
    mark_outbound_sent,
    mark_outbound_retry,
    mark_outbound_failed,
)

BOT_TOKEN = os.getenv("BOT_TOKEN")
WORKER_NAME = os.getenv("WORKER_NAME", "sender-1")

SEND_BATCH = 10
SEND_SLEEP = 1

TELEGRAM_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"


def now():
    return datetime.now(timezone.utc)


def send_message(chat_id: int, text: str):
    r = requests.post(
        TELEGRAM_URL,
        json={
            "chat_id": chat_id,
            "text": text,
        },
        timeout=10,
    )

    if r.status_code != 200:
        raise Exception(f"telegram error {r.status_code}: {r.text}")

    data = r.json()

    if not data.get("ok"):
        raise Exception(data)

    return True


def process_batch():
    jobs = claim_outbound_batch(WORKER_NAME, SEND_BATCH)

    if not jobs:
        return 0

    sent = 0

    for job in jobs:
        msg_id = job["id"]
        user = job["telegram_id"]
        text = job["text"]
        symbol = job["symbol"]
        side = job["side"]

        try:
            send_message(user, text)

            mark_outbound_sent(msg_id)

            print(
                f"SENT | user={user} | {symbol} | {side}",
                flush=True,
            )

            sent += 1

        except Exception as e:

            err = str(e)

            # Telegram flood control
            if "429" in err:
                print(
                    f"RATE LIMIT | retry later | user={user}",
                    flush=True,
                )
                mark_outbound_retry(msg_id, 30)

            else:
                print(
                    f"send_message failed окончательно | user={user} | {symbol} | {side} | {err}",
                    flush=True,
                )

                mark_outbound_failed(msg_id)

    return sent


def run_sender():
    print("sender started", flush=True)

    while True:
        try:
            sent = process_batch()

            if sent == 0:
                time.sleep(SEND_SLEEP)

        except Exception as e:
            print("sender fatal error:", e, flush=True)
            time.sleep(3)


def main():
    init_db()
    run_sender()


if __name__ == "__main__":
    main()