from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from config import BOT_TOKEN
from db import create_user_if_not_exists, update_user_setting, get_user


def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Режим", callback_data="menu_mode")],
        [InlineKeyboardButton("Макс. стоп", callback_data="menu_stop")],
        [InlineKeyboardButton("Тейк RR", callback_data="menu_tp")],
        [InlineKeyboardButton("Буфер стопа", callback_data="menu_buffer")],
        [InlineKeyboardButton("Чувствительность", callback_data="menu_sens")],
        [InlineKeyboardButton("Вкл/выкл сигналы", callback_data="menu_active")],
        [InlineKeyboardButton("Мои настройки", callback_data="menu_profile")],
    ])


def _render_profile(u: dict | None) -> str:
    u = u or {}

    enable_long = bool(u.get("enable_long", True))
    enable_short = bool(u.get("enable_short", True))
    max_stop_pct = float(u.get("max_stop_pct", 5.0))
    tp_rr = float(u.get("tp_rr", 1.0))
    stop_buffer_pct = float(u.get("stop_buffer_pct", 1.0))
    structure_sensitivity = int(u.get("structure_sensitivity", 2))
    signals_enabled = bool(u.get("signals_enabled", True))

    mode = (
        "лонг + шорт" if enable_long and enable_short
        else "только лонг" if enable_long
        else "только шорт" if enable_short
        else "выключено"
    )

    return (
        "Твои настройки:\n\n"
        f"Режим: {mode}\n"
        f"Макс. стоп: {max_stop_pct}%\n"
        f"Тейк RR: {tp_rr}R\n"
        f"Буфер: {stop_buffer_pct}%\n"
        f"Чувствительность: {structure_sensitivity}\n"
        f"Сигналы: {'включены' if signals_enabled else 'выключены'}"
    )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    create_user_if_not_exists(user.id, user.username)

    text = (
        "Бот запущен.\n\n"
        "1. Нажми кнопки ниже и выстави свои параметры.\n"
        "2. После этого сигналы будут приходить тебе в личку.\n"
        "3. Логика сигнала повторяет твой Pine-индикатор под выбранные настройки."
    )
    await update.message.reply_text(text, reply_markup=main_menu())


async def mysettings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    create_user_if_not_exists(user.id, user.username)
    u = get_user(user.id)
    await update.message.reply_text(_render_profile(u), reply_markup=main_menu())


async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    data = query.data

    if data == "menu_mode":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Только лонг", callback_data="set_mode_long")],
            [InlineKeyboardButton("Только шорт", callback_data="set_mode_short")],
            [InlineKeyboardButton("Лонг + шорт", callback_data="set_mode_both")],
            [InlineKeyboardButton("← Назад", callback_data="menu_root")],
        ])
        await query.message.edit_text("Выбери режим:", reply_markup=kb)
        return

    if data == "menu_stop":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("1%", callback_data="set_stop_1")],
            [InlineKeyboardButton("2%", callback_data="set_stop_2")],
            [InlineKeyboardButton("3%", callback_data="set_stop_3")],
            [InlineKeyboardButton("5%", callback_data="set_stop_5")],
            [InlineKeyboardButton("← Назад", callback_data="menu_root")],
        ])
        await query.message.edit_text("Выбери максимальный стоп:", reply_markup=kb)
        return

    if data == "menu_tp":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("1R", callback_data="set_tp_1")],
            [InlineKeyboardButton("1.5R", callback_data="set_tp_1.5")],
            [InlineKeyboardButton("2R", callback_data="set_tp_2")],
            [InlineKeyboardButton("3R", callback_data="set_tp_3")],
            [InlineKeyboardButton("← Назад", callback_data="menu_root")],
        ])
        await query.message.edit_text("Выбери тейк RR:", reply_markup=kb)
        return

    if data == "menu_buffer":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("0%", callback_data="set_buffer_0")],
            [InlineKeyboardButton("0.25%", callback_data="set_buffer_0.25")],
            [InlineKeyboardButton("0.5%", callback_data="set_buffer_0.5")],
            [InlineKeyboardButton("1%", callback_data="set_buffer_1")],
            [InlineKeyboardButton("← Назад", callback_data="menu_root")],
        ])
        await query.message.edit_text("Выбери буфер стопа:", reply_markup=kb)
        return

    if data == "menu_sens":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("2", callback_data="set_sens_2")],
            [InlineKeyboardButton("3", callback_data="set_sens_3")],
            [InlineKeyboardButton("4", callback_data="set_sens_4")],
            [InlineKeyboardButton("5", callback_data="set_sens_5")],
            [InlineKeyboardButton("← Назад", callback_data="menu_root")],
        ])
        await query.message.edit_text("Выбери чувствительность структуры:", reply_markup=kb)
        return

    if data == "menu_active":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Включить", callback_data="set_active_1")],
            [InlineKeyboardButton("Выключить", callback_data="set_active_0")],
            [InlineKeyboardButton("← Назад", callback_data="menu_root")],
        ])
        await query.message.edit_text("Включить или выключить личные сигналы:", reply_markup=kb)
        return

    if data == "menu_profile":
        u = get_user(user_id)
        await query.message.edit_text(_render_profile(u), reply_markup=main_menu())
        return

    if data == "menu_root":
        await query.message.edit_text("Главное меню", reply_markup=main_menu())
        return

    if data == "set_mode_long":
        update_user_setting(user_id, "enable_long", 1)
        update_user_setting(user_id, "enable_short", 0)

    elif data == "set_mode_short":
        update_user_setting(user_id, "enable_long", 0)
        update_user_setting(user_id, "enable_short", 1)

    elif data == "set_mode_both":
        update_user_setting(user_id, "enable_long", 1)
        update_user_setting(user_id, "enable_short", 1)

    elif data.startswith("set_stop_"):
        update_user_setting(user_id, "max_stop_pct", float(data.replace("set_stop_", "")))

    elif data.startswith("set_tp_"):
        update_user_setting(user_id, "tp_rr", float(data.replace("set_tp_", "")))

    elif data.startswith("set_buffer_"):
        update_user_setting(user_id, "stop_buffer_pct", float(data.replace("set_buffer_", "")))

    elif data.startswith("set_sens_"):
        update_user_setting(user_id, "structure_sensitivity", int(data.replace("set_sens_", "")))

    elif data.startswith("set_active_"):
        update_user_setting(user_id, "signals_enabled", int(data.replace("set_active_", "")))

    u = get_user(user_id)
    await query.message.edit_text("Сохранено.\n\n" + _render_profile(u), reply_markup=main_menu())


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    print(f"bot error: {context.error}", flush=True)


def build_bot_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).concurrent_updates(False).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("mysettings", mysettings))
    app.add_handler(CallbackQueryHandler(handle_buttons))
    app.add_error_handler(on_error)
    return app