import os
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
                    text TEXT NOT NULL,
                    symbol TEXT,
                    side TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    sent_at TIMESTAMP,
                    dedupe_key TEXT,
                    locked_at TIMESTAMP,
                    worker_id TEXT
                )
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_outbound_queue_status_created
                ON outbound_queue (status, created_at)
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_outbound_queue_locked_at
                ON outbound_queue (locked_at)
            """)

            cur.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_outbound_queue_dedupe
                ON outbound_queue (dedupe_key)
                WHERE dedupe_key IS NOT NULL
            """)

        conn.commit()

def create_user_if_not_exists(telegram_id: int, username: str | None):
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
                    telegram_id, symbol, in_trade, trade_dir, entry, stop, tp,
                    last_signature, last_bar_marker, updated_at
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


def enqueue_message(telegram_id: int, text: str, symbol: str | None, side: str | None, dedupe_key: str | None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO outbound_queue (
                    telegram_id, text, symbol, side, dedupe_key,
                    status, retry_count, created_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, 'pending', 0, NOW(), NOW())
                ON CONFLICT (dedupe_key) DO NOTHING
            """, (telegram_id, text, symbol, side, dedupe_key))
        conn.commit()


def claim_outbound_messages(limit: int, worker_id: str):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                WITH picked AS (
                    SELECT id
                    FROM outbound_queue
                    WHERE status IN ('pending', 'retry')
                    ORDER BY created_at ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE outbound_queue q
                SET status = 'processing',
                    locked_at = NOW(),
                    worker_id = %s,
                    updated_at = NOW()
                FROM picked
                WHERE q.id = picked.id
                RETURNING q.id, q.telegram_id, q.text, q.symbol, q.side, q.retry_count
            """, (limit, worker_id))
            rows = cur.fetchall()
            conn.commit()
            return [dict(r) for r in rows]


def mark_message_sent(queue_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE outbound_queue
                SET status = 'sent',
                    sent_at = NOW(),
                    updated_at = NOW(),
                    locked_at = NULL,
                    worker_id = NULL
                WHERE id = %s
            """, (queue_id,))
        conn.commit()


def mark_message_retry(queue_id: int, error_text: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE outbound_queue
                SET status = 'retry',
                    retry_count = retry_count + 1,
                    last_error = %s,
                    updated_at = NOW(),
                    locked_at = NULL,
                    worker_id = NULL
                WHERE id = %s
            """, (error_text[:1000], queue_id))
        conn.commit()


def mark_message_failed(queue_id: int, error_text: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE outbound_queue
                SET status = 'failed',
                    last_error = %s,
                    updated_at = NOW(),
                    locked_at = NULL,
                    worker_id = NULL
                WHERE id = %s
            """, (error_text[:1000], queue_id))
        conn.commit()


def requeue_stale_processing(max_age_seconds: int = 300):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE outbound_queue
                SET status = 'retry',
                    updated_at = NOW(),
                    locked_at = NULL,
                    worker_id = NULL,
                    last_error = COALESCE(last_error, 'stale processing requeued')
                WHERE status = 'processing'
                  AND locked_at IS NOT NULL
                  AND locked_at < NOW() - (%s || ' seconds')::interval
            """, (max_age_seconds,))
        conn.commit()


def cleanup_outbound_queue(sent_older_than_days: int = 3, failed_older_than_days: int = 7):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM outbound_queue
                WHERE status = 'sent'
                  AND sent_at IS NOT NULL
                  AND sent_at < NOW() - (%s || ' days')::interval
            """, (sent_older_than_days,))

            cur.execute("""
                DELETE FROM outbound_queue
                WHERE status = 'failed'
                  AND updated_at < NOW() - (%s || ' days')::interval
            """, (failed_older_than_days,))
        conn.commit()

def get_outbound_queue_stats():
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'pending')    AS pending_count,
                    COUNT(*) FILTER (WHERE status = 'retry')      AS retry_count,
                    COUNT(*) FILTER (WHERE status = 'processing') AS processing_count,
                    COUNT(*) FILTER (WHERE status = 'failed')     AS failed_count,
                    COUNT(*) FILTER (WHERE status = 'sent')       AS sent_count
                FROM outbound_queue
            """)
            row = cur.fetchone()
            return dict(row) if row else {
                "pending_count": 0,
                "retry_count": 0,
                "processing_count": 0,
                "failed_count": 0,
                "sent_count": 0,
            }