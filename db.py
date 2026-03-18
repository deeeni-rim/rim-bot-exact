import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor
from datetime import datetime
from config import DATABASE_URL

conn = None


def init_db():
    global conn
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True


def utc_now():
    return datetime.utcnow()


# ✅ Кэш пользователей (быстро)
def get_all_active_users():
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT *
            FROM users
            WHERE is_active = TRUE
        """)
        return cur.fetchall()


# 🚀 НОВОЕ — БАТЧ ЗАГРУЗКА СОСТОЯНИЙ
def get_states_for_symbol(symbol: str):
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT *
            FROM user_symbol_state
            WHERE symbol = %s
        """, (symbol,))
        rows = cur.fetchall()

    # превращаем в dict: user_id -> state
    result = {}
    for row in rows:
        result[row["telegram_id"]] = row
    return result


# 🚀 НОВОЕ — БАТЧ UPSERT
def upsert_states_batch(rows: list[dict]):
    if not rows:
        return

    query = """
        INSERT INTO user_symbol_state (
            telegram_id,
            symbol,
            in_trade,
            trade_dir,
            entry,
            stop,
            tp,
            last_signature,
            last_bar_marker
        )
        VALUES (
            %(telegram_id)s,
            %(symbol)s,
            %(in_trade)s,
            %(trade_dir)s,
            %(entry)s,
            %(stop)s,
            %(tp)s,
            %(last_signature)s,
            %(last_bar_marker)s
        )
        ON CONFLICT (telegram_id, symbol)
        DO UPDATE SET
            in_trade = EXCLUDED.in_trade,
            trade_dir = EXCLUDED.trade_dir,
            entry = EXCLUDED.entry,
            stop = EXCLUDED.stop,
            tp = EXCLUDED.tp,
            last_signature = EXCLUDED.last_signature,
            last_bar_marker = EXCLUDED.last_bar_marker
    """

    with conn.cursor() as cur:
        execute_batch(cur, query, rows, page_size=200)


# очередь сообщений
def enqueue_outbound_message(
    telegram_id: int,
    symbol: str,
    side: str,
    text: str,
    signal_key: str,
    created_at
):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO outbound_queue (
                telegram_id, symbol, side, text, signal_key, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (telegram_id, symbol, side, text, signal_key, created_at))