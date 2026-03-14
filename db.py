import os
from datetime import datetime
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor


DATABASE_URL = os.getenv("DATABASE_URL")


def get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    return psycopg2.connect(DATABASE_URL)


def init_db():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    telegram_id BIGINT PRIMARY KEY,
                    username TEXT,
                    enable_long BOOLEAN DEFAULT TRUE,
                    enable_short BOOLEAN DEFAULT TRUE,
                    max_stop_pct DOUBLE PRECISION DEFAULT 3.0,
                    tp_rr DOUBLE PRECISION DEFAULT 1.0,
                    stop_buffer_pct DOUBLE PRECISION DEFAULT 1.0,
                    structure_sensitivity INTEGER DEFAULT 3,
                    signals_enabled BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS user_symbol_state (
                    telegram_id BIGINT NOT NULL,
                    symbol TEXT NOT NULL,
                    in_trade INTEGER DEFAULT 0,
                    trade_dir INTEGER DEFAULT 0,
                    entry DOUBLE PRECISION,
                    stop DOUBLE PRECISION,
                    tp DOUBLE PRECISION,
                    last_signature TEXT,
                    last_bar_marker TEXT,
                    updated_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (telegram_id, symbol)
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS outbound_queue (
                    id BIGSERIAL PRIMARY KEY,
                    telegram_id BIGINT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    text TEXT NOT NULL,
                    signal_key TEXT UNIQUE,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    available_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    locked_at TIMESTAMP,
                    locked_by TEXT,
                    sent_at TIMESTAMP
                )
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_outbound_queue_status_available
                ON outbound_queue(status, available_at)
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_outbound_queue_locked_at
                ON outbound_queue(locked_at)
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_user_symbol_state_user_symbol
                ON user_symbol_state(telegram_id, symbol)
            """)

        conn.commit()


def create_user_if_not_exists(telegram_id: int, username: Optional[str]):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (telegram_id, username)
                VALUES (%s, %s)
                ON CONFLICT (telegram_id) DO NOTHING
            """, (telegram_id, username))
        conn.commit()


def get_user(telegram_id: int):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM users
                WHERE telegram_id = %s
            """, (telegram_id,))
            row = cur.fetchone()
            return dict(row) if row else None


def update_user_setting(user_id: int, key: str, value):
    allowed_fields = {
        "enable_long",
        "enable_short",
        "max_stop_pct",
        "tp_rr",
        "stop_buffer_pct",
        "structure_sensitivity",
        "signals_enabled",
        "username",
    }

    if key not in allowed_fields:
        raise ValueError(f"Unsupported setting: {key}")

    if key in {"enable_long", "enable_short", "signals_enabled"}:
        if isinstance(value, str):
            value = value.strip().lower() in {"1", "true", "yes", "on"}
        else:
            value = bool(value)

    if key == "structure_sensitivity":
        value = int(value)

    if key in {"max_stop_pct", "tp_rr", "stop_buffer_pct"}:
        value = float(value)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE users
                SET {key} = %s,
                    updated_at = NOW()
                WHERE telegram_id = %s
                """,
                (value, user_id),
            )
        conn.commit()


def get_all_active_users():
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM users
                WHERE signals_enabled = TRUE
            """)
            rows = cur.fetchall()
            return [dict(r) for r in rows]


def get_user_symbol_state(telegram_id: int, symbol: str):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM user_symbol_state
                WHERE telegram_id = %s AND symbol = %s
            """, (telegram_id, symbol))
            row = cur.fetchone()
            return dict(row) if row else None


def upsert_user_symbol_state(state: dict):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO user_symbol_state (
                    telegram_id,
                    symbol,
                    in_trade,
                    trade_dir,
                    entry,
                    stop,
                    tp,
                    last_signature,
                    last_bar_marker,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (telegram_id, symbol)
                DO UPDATE SET
                    in_trade = EXCLUDED.in_trade,
                    trade_dir = EXCLUDED.trade_dir,
                    entry = EXCLUDED.entry,
                    stop = EXCLUDED.stop,
                    tp = EXCLUDED.tp,
                    last_signature = EXCLUDED.last_signature,
                    last_bar_marker = EXCLUDED.last_bar_marker,
                    updated_at = NOW()
            """, (
                state["telegram_id"],
                state["symbol"],
                state.get("in_trade", 0),
                state.get("trade_dir", 0),
                state.get("entry"),
                state.get("stop"),
                state.get("tp"),
                state.get("last_signature"),
                state.get("last_bar_marker"),
            ))
        conn.commit()


def enqueue_outbound_message(
    telegram_id: int,
    symbol: str,
    side: str,
    text: str,
    signal_key: str,
    created_at: datetime,
):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO outbound_queue (
                    telegram_id,
                    symbol,
                    side,
                    text,
                    signal_key,
                    status,
                    attempts,
                    created_at,
                    available_at
                )
                VALUES (%s, %s, %s, %s, %s, 'pending', 0, %s, %s)
                ON CONFLICT (signal_key) DO NOTHING
            """, (
                telegram_id,
                symbol,
                side,
                text,
                signal_key,
                created_at,
                created_at,
            ))
        conn.commit()


def claim_outbound_batch(worker_name: str, limit: int):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                WITH picked AS (
                    SELECT id
                    FROM outbound_queue
                    WHERE status = 'pending'
                      AND available_at <= NOW()
                      AND (
                            locked_at IS NULL
                            OR locked_at < NOW() - INTERVAL '10 minutes'
                          )
                    ORDER BY created_at ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE outbound_queue q
                SET locked_at = NOW(),
                    locked_by = %s,
                    attempts = q.attempts + 1
                FROM picked
                WHERE q.id = picked.id
                RETURNING q.*
            """, (limit, worker_name))
            rows = cur.fetchall()
        conn.commit()
        return [dict(r) for r in rows]


def mark_outbound_sent(message_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE outbound_queue
                SET status = 'sent',
                    sent_at = NOW(),
                    locked_at = NULL,
                    locked_by = NULL
                WHERE id = %s
            """, (message_id,))
        conn.commit()


def mark_outbound_failed(message_id: int, error_text: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE outbound_queue
                SET status = 'failed',
                    last_error = %s,
                    locked_at = NULL,
                    locked_by = NULL
                WHERE id = %s
            """, (str(error_text)[:1000], message_id))
        conn.commit()


def release_outbound_for_retry(message_id: int, error_text: str, delay_seconds: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE outbound_queue
                SET status = 'pending',
                    last_error = %s,
                    available_at = NOW() + (%s || ' seconds')::interval,
                    locked_at = NULL,
                    locked_by = NULL
                WHERE id = %s
            """, (
                str(error_text)[:1000],
                str(delay_seconds),
                message_id,
            ))
        conn.commit()