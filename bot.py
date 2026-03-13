from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from telegram.error import BadRequest

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


def _safe_bool(v) -> bool:
    return bool(v) if v is not None else False


def _render_profile(u: dict) -> str:
    enable_long = _safe_bool(u.get("enable_long"))
    enable_short = _safe_bool(u.get("enable_short"))
    signals_enabled = _safe_bool(u.get("signals_enabled"))

    mode = (
        "лонг + шорт" if enable_long and enable_short
        else "только лонг" if enable_long
        else "только шорт" if enable_short
        else "выключено"
    )

    return (
        f"Твои настройки:\n\n"
        f"Режим: {mode}\n"
        f"Макс. стоп: {u.get('max_stop_pct')}%\n"
        f"Тейк RR: {u.get('tp_rr')}R\n"
        f"Буфер стопа: {u.get('stop_buffer_pct')}%\n"
        f"Чувствительность: {u.get('structure_sensitivity')}\n"
        f"Сигналы: {'включены' if signals_enabled else 'выключены'}"
    )


async def _edit_or_reply(query, text: str, reply_markup=None):
    try:
        await query.message.edit_text(text, reply_markup=reply_markup)
    except BadRequest as e:
        err = str(e).lower()
        if "message is not modified" in err:
            return
        raise


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

    if not u:
        await update.message.reply_text("Пользователь не найден. Нажми /start")
        return

    await update.message.reply_text(_render_profile(u), reply_markup=main_menu())


async def handle_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    try:
        await query.answer()
    except Exception:
        pass

    user_id = query.from_user.id
    data = query.data

    create_user_if_not_exists(user_id, query.from_user.username)

    if data == "menu_mode":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Только лонг", callback_data="set_mode_long")],
            [InlineKeyboardButton("Только шорт", callback_data="set_mode_short")],
            [InlineKeyboardButton("Лонг + шорт", callback_data="set_mode_both")],
            [InlineKeyboardButton("← Назад", callback_data="menu_root")],
        ])
        await _edit_or_reply(query, "Выбери режим:", reply_markup=kb)
        return

    if data == "menu_stop":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("1%", callback_data="set_stop_1")],
            [InlineKeyboardButton("2%", callback_data="set_stop_2")],
            [InlineKeyboardButton("3%", callback_data="set_stop_3")],
            [InlineKeyboardButton("5%", callback_data="set_stop_5")],
            [InlineKeyboardButton("← Назад", callback_data="menu_root")],
        ])
        await _edit_or_reply(query, "Выбери максимальный стоп:", reply_markup=kb)
        return

    if data == "menu_tp":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("1R", callback_data="set_tp_1")],
            [InlineKeyboardButton("1.5R", callback_data="set_tp_1.5")],
            [InlineKeyboardButton("2R", callback_data="set_tp_2")],
            [InlineKeyboardButton("3R", callback_data="set_tp_3")],
            [InlineKeyboardButton("← Назад", callback_data="menu_root")],
        ])
        await _edit_or_reply(query, "Выбери тейк RR:", reply_markup=kb)
        return

    if data == "menu_buffer":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("0%", callback_data="set_buffer_0")],
            [InlineKeyboardButton("0.25%", callback_data="set_buffer_0.25")],
            [InlineKeyboardButton("0.5%", callback_data="set_buffer_0.5")],
            [InlineKeyboardButton("1%", callback_data="set_buffer_1")],
            [InlineKeyboardButton("← Назад", callback_data="menu_root")],
        ])
        await _edit_or_reply(query, "Выбери буфер стопа:", reply_markup=kb)
        return

    if data == "menu_sens":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("2", callback_data="set_sens_2")],
            [InlineKeyboardButton("3", callback_data="set_sens_3")],
            [InlineKeyboardButton("4", callback_data="set_sens_4")],
            [InlineKeyboardButton("5", callback_data="set_sens_5")],
            [InlineKeyboardButton("← Назад", callback_data="menu_root")],
        ])
        await _edit_or_reply(query, "Выбери чувствительность структуры:", reply_markup=kb)
        return

    if data == "menu_active":
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("Включить", callback_data="set_active_1")],
            [InlineKeyboardButton("Выключить", callback_data="set_active_0")],
            [InlineKeyboardButton("← Назад", callback_data="menu_root")],
        ])
        await _edit_or_reply(query, "Включить или выключить личные сигналы:", reply_markup=kb)
        return

    if data == "menu_profile":
        u = get_user(user_id)
        if not u:
            await _edit_or_reply(query, "Пользователь не найден. Нажми /start", reply_markup=main_menu())
            return
        await _edit_or_reply(query, _render_profile(u), reply_markup=main_menu())
        return

    if data == "menu_root":
        u = get_user(user_id)
        text = "Главное меню" if not u else _render_profile(u)
        await _edit_or_reply(query, text, reply_markup=main_menu())
        return

    changed = False

    if data == "set_mode_long":
        update_user_setting(user_id, "enable_long", True)
        update_user_setting(user_id, "enable_short", False)
        changed = True

    elif data == "set_mode_short":
        update_user_setting(user_id, "enable_long", False)
        update_user_setting(user_id, "enable_short", True)
        changed = True

    elif data == "set_mode_both":
        update_user_setting(user_id, "enable_long", True)
        update_user_setting(user_id, "enable_short", True)
        changed = True

    elif data.startswith("set_stop_"):
        update_user_setting(user_id, "max_stop_pct", float(data.replace("set_stop_", "")))
        changed = True

    elif data.startswith("set_tp_"):
        update_user_setting(user_id, "tp_rr", float(data.replace("set_tp_", "")))
        changed = True

    elif data.startswith("set_buffer_"):
        update_user_setting(user_id, "stop_buffer_pct", float(data.replace("set_buffer_", "")))
        changed = True

    elif data.startswith("set_sens_"):
        update_user_setting(user_id, "structure_sensitivity", int(data.replace("set_sens_", "")))
        changed = True

    elif data == "set_active_1":
        update_user_setting(user_id, "signals_enabled", True)
        changed = True

    elif data == "set_active_0":
        update_user_setting(user_id, "signals_enabled", False)
        changed = True

    if changed:
        u = get_user(user_id)
        await _edit_or_reply(query, "Сохранено.\n\n" + _render_profile(u), reply_markup=main_menu())
        return

    await _edit_or_reply(query, "Неизвестная команда.", reply_markup=main_menu())


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    print(f"bot error: {context.error}", flush=True)


def build_bot_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).concurrent_updates(False).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("mysettings", mysettings))
    app.add_handler(CallbackQueryHandler(handle_buttons))
    app.add_error_handler(on_error)
    return app