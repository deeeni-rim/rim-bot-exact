import logging
import os
from typing import Optional

import psycopg2
import psycopg2.extras
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is not set")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)


DEFAULTS = {
    "enable_long": True,
    "enable_short": True,
    "signals_enabled": True,
    "tp_rr": 1.0,
    "max_stop_pct": 3.0,
    "stop_buffer_pct": 1.0,
    "structure_sensitivity": 2,
}


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def main_menu_keyboard():
    return ReplyKeyboardMarkup(
        [
            ["⚙️ Настройки", "📊 Статус"],
        ],
        resize_keyboard=True,
    )


def settings_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Режим", callback_data="edit_mode")],
            [InlineKeyboardButton("Макс. стоп", callback_data="edit_max_stop")],
            [InlineKeyboardButton("Тейк RR", callback_data="edit_tp_rr")],
            [InlineKeyboardButton("Буфер стопа", callback_data="edit_stop_buffer")],
            [InlineKeyboardButton("Чувствительность", callback_data="edit_sensitivity")],
            [InlineKeyboardButton("Вкл/выкл сигналы", callback_data="toggle_signals")],
            [InlineKeyboardButton("Мои настройки", callback_data="show_settings")],
        ]
    )


def ensure_users_table():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    telegram_id BIGINT PRIMARY KEY,
                    username TEXT,
                    enable_long BOOLEAN DEFAULT TRUE,
                    enable_short BOOLEAN DEFAULT TRUE,
                    signals_enabled BOOLEAN DEFAULT TRUE,
                    tp_rr DOUBLE PRECISION DEFAULT 1.0,
                    max_stop_pct DOUBLE PRECISION DEFAULT 3.0,
                    stop_buffer_pct DOUBLE PRECISION DEFAULT 1.0,
                    structure_sensitivity INTEGER DEFAULT 2,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
                """
            )
            conn.commit()

            cur.execute(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS stop_buffer_pct DOUBLE PRECISION DEFAULT 1.0;
                """
            )
            cur.execute(
                """
                ALTER TABLE users
                ADD COLUMN IF NOT EXISTS structure_sensitivity INTEGER DEFAULT 2;
                """
            )
            conn.commit()


def upsert_user(telegram_id: int, username: Optional[str]):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO users (
                    telegram_id,
                    username,
                    enable_long,
                    enable_short,
                    signals_enabled,
                    tp_rr,
                    max_stop_pct,
                    stop_buffer_pct,
                    structure_sensitivity,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (telegram_id)
                DO UPDATE SET
                    username = EXCLUDED.username,
                    updated_at = NOW();
                """,
                (
                    telegram_id,
                    username,
                    DEFAULTS["enable_long"],
                    DEFAULTS["enable_short"],
                    DEFAULTS["signals_enabled"],
                    DEFAULTS["tp_rr"],
                    DEFAULTS["max_stop_pct"],
                    DEFAULTS["stop_buffer_pct"],
                    DEFAULTS["structure_sensitivity"],
                ),
            )
            conn.commit()


def get_user_settings(telegram_id: int) -> dict:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    telegram_id,
                    username,
                    enable_long,
                    enable_short,
                    signals_enabled,
                    tp_rr,
                    max_stop_pct,
                    stop_buffer_pct,
                    structure_sensitivity
                FROM users
                WHERE telegram_id = %s
                """,
                (telegram_id,),
            )
            row = cur.fetchone()
            if row:
                data = dict(row)
                if data.get("stop_buffer_pct") is None:
                    data["stop_buffer_pct"] = DEFAULTS["stop_buffer_pct"]
                if data.get("structure_sensitivity") is None:
                    data["structure_sensitivity"] = DEFAULTS["structure_sensitivity"]
                return data

    return {
        "telegram_id": telegram_id,
        "username": None,
        **DEFAULTS,
    }


def update_user_field(telegram_id: int, field_name: str, value):
    allowed = {
        "enable_long",
        "enable_short",
        "signals_enabled",
        "tp_rr",
        "max_stop_pct",
        "stop_buffer_pct",
        "structure_sensitivity",
    }
    if field_name not in allowed:
        raise ValueError(f"field not allowed: {field_name}")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE users
                SET {field_name} = %s,
                    updated_at = NOW()
                WHERE telegram_id = %s
                """,
                (value, telegram_id),
            )
            conn.commit()


def get_mode_text(settings: dict) -> str:
    long_on = settings.get("enable_long", True)
    short_on = settings.get("enable_short", True)

    if long_on and short_on:
        return "лонг + шорт"
    if long_on and not short_on:
        return "только лонг"
    if not long_on and short_on:
        return "только шорт"
    return "всё выключено"


def format_settings_text(settings: dict) -> str:
    return (
        "Сохранено.\n\n"
        "Твои настройки:\n\n"
        f"Режим: {get_mode_text(settings)}\n"
        f"Макс. стоп: {settings.get('max_stop_pct')}%\n"
        f"Тейк RR: {settings.get('tp_rr')}R\n"
        f"Буфер стопа: {settings.get('stop_buffer_pct')}%\n"
        f"Чувствительность: {settings.get('structure_sensitivity')}\n"
        f"Сигналы: {'включены' if settings.get('signals_enabled') else 'выключены'}"
    )


async def send_settings(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    settings = get_user_settings(chat_id)
    await context.bot.send_message(
        chat_id=chat_id,
        text=format_settings_text(settings),
        reply_markup=settings_keyboard(),
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    upsert_user(user.id, user.username)

    await update.message.reply_text(
        "Бот активирован. Меню ниже.",
        reply_markup=main_menu_keyboard(),
    )
    await send_settings(user.id, context)


async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await send_settings(update.effective_user.id, context)


async def status_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    settings = get_user_settings(update.effective_user.id)
    await update.message.reply_text(
        format_settings_text(settings),
        reply_markup=main_menu_keyboard(),
    )


async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id

    settings = get_user_settings(user_id)

    if data == "open_settings":
        await query.message.reply_text(
            format_settings_text(settings),
            reply_markup=settings_keyboard(),
        )
        return

    if data == "show_settings":
        await query.message.reply_text(
            format_settings_text(settings),
            reply_markup=settings_keyboard(),
        )
        return

    if data == "toggle_signals":
        update_user_field(user_id, "signals_enabled", not settings.get("signals_enabled", True))
        new_settings = get_user_settings(user_id)
        await query.message.reply_text(
            format_settings_text(new_settings),
            reply_markup=settings_keyboard(),
        )
        return

    if data == "edit_mode":
        mode_keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("Лонг + Шорт", callback_data="mode_both")],
                [InlineKeyboardButton("Только Лонг", callback_data="mode_long_only")],
                [InlineKeyboardButton("Только Шорт", callback_data="mode_short_only")],
                [InlineKeyboardButton("Всё Выкл", callback_data="mode_off")],
            ]
        )
        await query.message.reply_text("Выбери режим:", reply_markup=mode_keyboard)
        return

    if data == "mode_both":
        update_user_field(user_id, "enable_long", True)
        update_user_field(user_id, "enable_short", True)

    elif data == "mode_long_only":
        update_user_field(user_id, "enable_long", True)
        update_user_field(user_id, "enable_short", False)

    elif data == "mode_short_only":
        update_user_field(user_id, "enable_long", False)
        update_user_field(user_id, "enable_short", True)

    elif data == "mode_off":
        update_user_field(user_id, "enable_long", False)
        update_user_field(user_id, "enable_short", False)

    elif data == "edit_tp_rr":
        context.user_data["awaiting_input"] = "tp_rr"
        await query.message.reply_text("Отправь новое значение тейка RR. Например: 1.5")
        return

    elif data == "edit_max_stop":
        context.user_data["awaiting_input"] = "max_stop_pct"
        await query.message.reply_text("Отправь новый макс. стоп %. Например: 2.8")
        return

    elif data == "edit_stop_buffer":
        context.user_data["awaiting_input"] = "stop_buffer_pct"
        await query.message.reply_text("Отправь новый буфер стопа %. Например: 1.0")
        return

    elif data == "edit_sensitivity":
        sensitivity_keyboard = InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("1", callback_data="sens_1")],
                [InlineKeyboardButton("2", callback_data="sens_2")],
                [InlineKeyboardButton("3", callback_data="sens_3")],
                [InlineKeyboardButton("4", callback_data="sens_4")],
            ]
        )
        await query.message.reply_text("Выбери чувствительность:", reply_markup=sensitivity_keyboard)
        return

    elif data.startswith("sens_"):
        level = int(data.split("_")[1])
        update_user_field(user_id, "structure_sensitivity", level)

    else:
        return

    new_settings = get_user_settings(user_id)
    await query.message.reply_text(
        format_settings_text(new_settings),
        reply_markup=settings_keyboard(),
    )


async def value_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    awaiting = context.user_data.get("awaiting_input")
    if not awaiting:
        return

    user_id = update.effective_user.id
    raw = (update.message.text or "").strip().replace(",", ".")

    try:
        value = float(raw)
    except ValueError:
        await update.message.reply_text("Не удалось распознать число. Попробуй ещё раз.")
        return

    if awaiting == "tp_rr":
        if value <= 0:
            await update.message.reply_text("TP/RR должен быть больше 0.")
            return
        update_user_field(user_id, "tp_rr", value)

    elif awaiting == "max_stop_pct":
        if value <= 0 or value > 100:
            await update.message.reply_text("Макс. стоп должен быть в диапазоне 0–100.")
            return
        update_user_field(user_id, "max_stop_pct", value)

    elif awaiting == "stop_buffer_pct":
        if value < 0 or value > 100:
            await update.message.reply_text("Буфер стопа должен быть в диапазоне 0–100.")
            return
        update_user_field(user_id, "stop_buffer_pct", value)

    context.user_data.pop("awaiting_input", None)
    await send_settings(user_id, context)


async def unknown_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip()

    if text == "⚙️ Настройки":
        await send_settings(update.effective_user.id, context)
        return

    if text == "📊 Статус":
        await status_text(update, context)
        return

    await update.message.reply_text(
        "Используй меню ниже.",
        reply_markup=main_menu_keyboard(),
    )


def main():
    ensure_users_table()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("settings", settings_command))
    app.add_handler(CallbackQueryHandler(callback_router))
    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            value_input_handler,
            block=False,
        )
    )
    app.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            unknown_text_handler,
        )
    )

    print("bot_only.py started", flush=True)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()