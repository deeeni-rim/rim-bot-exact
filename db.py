import sqlite3
from typing import Optional

DB_PATH = "users.db"


DEFAULT_SETTINGS = {
    "enable_long": 1,
    "enable_short": 1,
    "max_stop_pct": 3.0,
    "tp_rr": 1.0,
    "stop_buffer_pct": 1.0,
    "structure_sensitivity": 3,
    "is_active": 1,
}


def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            telegram_id INTEGER PRIMARY KEY,
            username TEXT,
            enable_long INTEGER DEFAULT 1,
            enable_short INTEGER DEFAULT 1,
            max_stop_pct REAL DEFAULT 3.0,
            tp_rr REAL DEFAULT 1.0,
            stop_buffer_pct REAL DEFAULT 1.0,
            structure_sensitivity INTEGER DEFAULT 3,
            is_active INTEGER DEFAULT 1
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_symbol_state (
            telegram_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            in_trade INTEGER DEFAULT 0,
            trade_dir INTEGER DEFAULT 0,
            entry REAL,
            stop REAL,
            tp REAL,
            last_signature TEXT,
            last_bar_marker TEXT,
            PRIMARY KEY (telegram_id, symbol)
        )
    """)

    conn.commit()
    conn.close()


def create_user_if_not_exists(telegram_id: int, username: Optional[str] = None):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT OR IGNORE INTO users
        (telegram_id, username, enable_long, enable_short, max_stop_pct, tp_rr, stop_buffer_pct, structure_sensitivity, is_active)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        telegram_id,
        username,
        DEFAULT_SETTINGS["enable_long"],
        DEFAULT_SETTINGS["enable_short"],
        DEFAULT_SETTINGS["max_stop_pct"],
        DEFAULT_SETTINGS["tp_rr"],
        DEFAULT_SETTINGS["stop_buffer_pct"],
        DEFAULT_SETTINGS["structure_sensitivity"],
        DEFAULT_SETTINGS["is_active"],
    ))

    if username is not None:
        cursor.execute("""
            UPDATE users
            SET username = ?
            WHERE telegram_id = ?
        """, (username, telegram_id))

    conn.commit()
    conn.close()


def get_user(telegram_id: int):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT *
        FROM users
        WHERE telegram_id = ?
    """, (telegram_id,))

    row = cursor.fetchone()
    conn.close()

    return dict(row) if row else None


def update_user_setting(telegram_id: int, field_name: str, value):
    allowed_fields = {
        "enable_long",
        "enable_short",
        "max_stop_pct",
        "tp_rr",
        "stop_buffer_pct",
        "structure_sensitivity",
        "is_active",
        "username",
    }

    if field_name not in allowed_fields:
        raise ValueError(f"Unsupported field: {field_name}")

    conn = get_conn()
    cursor = conn.cursor()

    query = f"UPDATE users SET {field_name} = ? WHERE telegram_id = ?"
    cursor.execute(query, (value, telegram_id))

    conn.commit()
    conn.close()


def get_all_active_users():
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT *
        FROM users
        WHERE is_active = 1
    """)

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def get_user_symbol_state(telegram_id: int, symbol: str):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT telegram_id, symbol, in_trade, trade_dir, entry, stop, tp, last_signature, last_bar_marker
        FROM user_symbol_state
        WHERE telegram_id = ? AND symbol = ?
    """, (telegram_id, symbol))

    row = cursor.fetchone()
    conn.close()

    return dict(row) if row else None


def upsert_user_symbol_state(data: dict):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO user_symbol_state
        (telegram_id, symbol, in_trade, trade_dir, entry, stop, tp, last_signature, last_bar_marker)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(telegram_id, symbol) DO UPDATE SET
            in_trade = excluded.in_trade,
            trade_dir = excluded.trade_dir,
            entry = excluded.entry,
            stop = excluded.stop,
            tp = excluded.tp,
            last_signature = excluded.last_signature,
            last_bar_marker = excluded.last_bar_marker
    """, (
        data["telegram_id"],
        data["symbol"],
        data.get("in_trade", 0),
        data.get("trade_dir", 0),
        data.get("entry"),
        data.get("stop"),
        data.get("tp"),
        data.get("last_signature"),
        data.get("last_bar_marker"),
    ))

    conn.commit()
    conn.close()