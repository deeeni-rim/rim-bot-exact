import sqlite3
from contextlib import closing
from config import DEFAULT_SETTINGS

DB_NAME = "users.db"


def get_conn():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                username TEXT,
                enable_long INTEGER NOT NULL,
                enable_short INTEGER NOT NULL,
                structure_sensitivity INTEGER NOT NULL,
                max_stop_pct REAL NOT NULL,
                stop_buffer_pct REAL NOT NULL,
                tp_rr REAL NOT NULL,
                is_active INTEGER NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_symbol_state (
                telegram_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                in_trade INTEGER NOT NULL DEFAULT 0,
                trade_dir INTEGER NOT NULL DEFAULT 0,
                entry REAL,
                stop REAL,
                tp REAL,
                last_signature TEXT,
                last_bar_marker TEXT,
                PRIMARY KEY (telegram_id, symbol)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS signal_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                telegram_id INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                entry REAL NOT NULL,
                stop REAL NOT NULL,
                tp REAL NOT NULL,
                risk_pct REAL NOT NULL,
                timeframe TEXT NOT NULL,
                signature TEXT NOT NULL
            )
            """
        )
        conn.commit()


def create_user_if_not_exists(telegram_id: int, username: str | None):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT telegram_id FROM users WHERE telegram_id = ?", (telegram_id,))
        row = cur.fetchone()
        if not row:
            cur.execute(
                """
                INSERT INTO users (
                    telegram_id, username, enable_long, enable_short,
                    structure_sensitivity, max_stop_pct, stop_buffer_pct,
                    tp_rr, is_active
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    telegram_id,
                    username,
                    DEFAULT_SETTINGS["enable_long"],
                    DEFAULT_SETTINGS["enable_short"],
                    DEFAULT_SETTINGS["structure_sensitivity"],
                    DEFAULT_SETTINGS["max_stop_pct"],
                    DEFAULT_SETTINGS["stop_buffer_pct"],
                    DEFAULT_SETTINGS["tp_rr"],
                    DEFAULT_SETTINGS["is_active"],
                ),
            )
            conn.commit()


def update_user_setting(telegram_id: int, field: str, value):
    allowed = {
        "enable_long",
        "enable_short",
        "structure_sensitivity",
        "max_stop_pct",
        "stop_buffer_pct",
        "tp_rr",
        "is_active",
    }
    if field not in allowed:
        raise ValueError("Недопустимое поле")

    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(f"UPDATE users SET {field} = ? WHERE telegram_id = ?", (value, telegram_id))
        conn.commit()


def get_user(telegram_id: int):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        row = cur.fetchone()
        return dict(row) if row else None


def get_all_active_users():
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE is_active = 1")
        rows = cur.fetchall()
        return [dict(r) for r in rows]


def get_user_symbol_state(telegram_id: int, symbol: str):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM user_symbol_state WHERE telegram_id = ? AND symbol = ?",
            (telegram_id, symbol),
        )
        row = cur.fetchone()
        if row:
            return dict(row)
        return {
            "telegram_id": telegram_id,
            "symbol": symbol,
            "in_trade": 0,
            "trade_dir": 0,
            "entry": None,
            "stop": None,
            "tp": None,
            "last_signature": None,
            "last_bar_marker": None,
        }


def save_user_symbol_state(state: dict):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO user_symbol_state (
                telegram_id, symbol, in_trade, trade_dir, entry, stop, tp,
                last_signature, last_bar_marker
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(telegram_id, symbol) DO UPDATE SET
                in_trade=excluded.in_trade,
                trade_dir=excluded.trade_dir,
                entry=excluded.entry,
                stop=excluded.stop,
                tp=excluded.tp,
                last_signature=excluded.last_signature,
                last_bar_marker=excluded.last_bar_marker
            """,
            (
                state["telegram_id"],
                state["symbol"],
                int(state.get("in_trade", 0)),
                int(state.get("trade_dir", 0)),
                state.get("entry"),
                state.get("stop"),
                state.get("tp"),
                state.get("last_signature"),
                str(state.get("last_bar_marker")) if state.get("last_bar_marker") is not None else None,
            ),
        )
        conn.commit()


def log_signal(telegram_id: int, symbol: str, side: str, entry: float, stop: float, tp: float, risk_pct: float, timeframe: str, signature: str):
    with closing(get_conn()) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO signal_log (
                telegram_id, symbol, side, entry, stop, tp, risk_pct, timeframe, signature
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (telegram_id, symbol, side, entry, stop, tp, risk_pct, timeframe, signature),
        )
        conn.commit()

def upsert_user_symbol_state(data: dict):
    conn = sqlite3.connect(DB_PATH)
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
        data["in_trade"],
        data["trade_dir"],
        data["entry"],
        data["stop"],
        data["tp"],
        data["last_signature"],
        data["last_bar_marker"]
    ))

    conn.commit()
    conn.close()
