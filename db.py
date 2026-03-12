import os
import json
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
                    max_stop_pct DOUBLE PRECISION DEFAULT 5.0,
                    tp_rr DOUBLE PRECISION DEFAULT 1.0,
                    stop_buffer_pct DOUBLE PRECISION DEFAULT 1.0,
                    structure_sensitivity INTEGER DEFAULT 2,
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


def update_user_field(telegram_id: int, field: str, value):
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

    if field not in allowed_fields:
        raise ValueError(f"Field {field} is not allowed")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE users
                SET {field} = %s,
                    updated_at = NOW()
                WHERE telegram_id = %s
                """,
                (value, telegram_id)
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

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE users
                SET {key} = %s,
                    updated_at = NOW()
                WHERE telegram_id = %s
                """,
                (value, user_id)
            )
        conn.commit()