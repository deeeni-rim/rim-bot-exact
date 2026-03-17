from datetime import datetime, timezone


def symbol_to_tv(symbol: str) -> str:
    clean = symbol.replace("_", "")
    return f"https://www.tradingview.com/chart/?symbol=MEXC:{clean}"


def symbol_to_mexc(symbol: str) -> str:
    return f"https://futures.mexc.com/exchange/{symbol}?type=linear_swap"


def format_signal_message(
    symbol: str,
    side: str,
    entry: float,
    stop: float,
    tp: float,
    risk_pct: float,
    user_settings: dict | None = None,
) -> str:
    side_upper = side.upper()
    emoji = "🟢" if side_upper == "LONG" else "🔴"

    tp_rr = None
    max_stop_pct = None

    if user_settings:
        tp_rr = user_settings.get("tp_rr")
        max_stop_pct = user_settings.get("max_stop_pct")

    lines = [
        f"{emoji} {side_upper} SIGNAL",
        "",
        f"Монета: {symbol}",
        "Таймфрейм: 5m",
        f"Вход: {entry:.8f}",
        f"Стоп: {stop:.8f}",
        f"Тейк: {tp:.8f}",
        f"Риск: {risk_pct:.2f}%",
    ]

    if tp_rr is not None or max_stop_pct is not None:
        lines.extend(["", "Пользовательские настройки:"])
        if tp_rr is not None:
            lines.append(f"TP/RR: {tp_rr}")
        if max_stop_pct is not None:
            lines.append(f"Макс. стоп: {max_stop_pct}%")

    dt = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")
    lines.extend(["", f"Время: {dt}"])

    return "\n".join(lines)