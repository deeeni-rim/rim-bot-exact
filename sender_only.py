import json
import os
import time
from datetime import datetime, timezone

import requests

from db import (
    init_db,
    claim_outbound_batch,
    mark_outbound_sent,
    mark_outbound_retry,
    mark_outbound_failed,
)

BOT_TOKEN = os.getenv("BOT_TOKEN")
WORKER_NAME = os.getenv("WORKER_NAME", "sender-1")
SEND_BATCH = int(os.getenv("SEND_BATCH", "10"))
SEND_SLEEP = float(os.getenv("SEND_SLEEP", "1"))
HTTP_TIMEOUT = int(os.getenv("HTTP_TIMEOUT", "15"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

TELEGRAM_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def symbol_to_tv(symbol: str) -> str:
    clean = symbol.replace("_", "")
    return f"https://www.tradingview.com/chart/?symbol=MEXC:{clean}"


def symbol_to_mexc(symbol: str) -> str:
    return f"https://futures.mexc.com/exchange/{symbol}?type=linear_swap"


def build_signal_reply_markup(symbol: str) -> dict:
    return {
        "inline_keyboard": [
            [
                {"text": "📈 TradingView", "url": symbol_to_tv(symbol)},
                {"text": "💹 MEXC", "url": symbol_to_mexc(symbol)},
            ],
            [
                {"text": "⚙️ Настройки", "callback_data": "open_settings"},
            ],
        ]
    }


def send_message(chat_id: int, text: str, symbol: str) -> tuple[bool, str]:
    payload = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
        "reply_markup": json.dumps(build_signal_reply_markup(symbol), ensure_ascii=False),
    }

    try:
        resp = requests.post(
            TELEGRAM_URL,
            json=payload,
            timeout=HTTP_TIMEOUT,
        )
    except Exception as e:
        return False, f"request_error: {e}"

    try:
        data = resp.json()
    except Exception:
        data = {"ok": False, "description": resp.text}

    if resp.status_code == 200 and data.get("ok"):
        return True, ""

    return False, data.get("description", f"http_{resp.status_code}")


def process_batch():
    batch = claim_outbound_batch(WORKER_NAME, SEND_BATCH)
    if not batch:
        return 0

    sent_count = 0

    for row in batch:
        msg_id = row["id"]
        telegram_id = row["telegram_id"]
        symbol = row["symbol"]
        side = row.get("side", "")
        text = row["text"]
        attempts = row.get("attempts", 0)

        ok, err = send_message(telegram_id, text, symbol)

        if ok:
            mark_outbound_sent(msg_id)
            sent_count += 1
            print(
                f"[{now_str()}] SENT | user={telegram_id} | {symbol} | {side}",
                flush=True,
            )
            continue

        if attempts >= 3:
            mark_outbound_failed(msg_id, err)
            print(
                f"[{now_str()}] send_message failed окончательно | user={telegram_id} | "
                f"{symbol} | {side} | {err}",
                flush=True,
            )
        else:
            mark_outbound_retry(msg_id, err)
            print(
                f"[{now_str()}] send_message retry {attempts + 1}/4 | user={telegram_id} | "
                f"{symbol} | {side} | {err}",
                flush=True,
            )

    return sent_count


def main():
    print("sender_only.py started", flush=True)
    print(f"worker={WORKER_NAME}", flush=True)
    print(f"send_batch={SEND_BATCH}", flush=True)

    init_db()

    while True:
        try:
            sent = process_batch()
            if sent == 0:
                time.sleep(SEND_SLEEP)
        except Exception as e:
            print(f"[{now_str()}] sender fatal loop error | {e}", flush=True)
            time.sleep(2)


if __name__ == "__main__":
    main()