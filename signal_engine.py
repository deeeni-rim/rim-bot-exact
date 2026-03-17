from strategy_core import (
    state_from_db,
    state_to_db,
    build_market_snapshot,
    _build_long_structure,
    _build_short_structure,
    process_user_symbol_fast,
)


def process_symbol_for_user(df_scan, df_filter, user: dict, trade_row: dict | None):
    """
    Унифицированная обработка одной монеты для одного пользователя.
    Возвращает:
      signal, trade_state, snapshot_meta
    """

    trade_state = state_from_db(trade_row or {})

    snapshot = build_market_snapshot(df_scan, df_filter)
    if snapshot is None:
        return None, trade_state, None

    sens = int(user["structure_sensitivity"])

    long_l, long_h = _build_long_structure(df_scan, sens)
    short_h, short_l = _build_short_structure(df_scan, sens)

    signal = process_user_symbol_fast(
        snapshot=snapshot,
        df_scan=df_scan,
        long_l=long_l,
        long_h=long_h,
        short_h=short_h,
        short_l=short_l,
        user=user,
        trade_state=trade_state,
    )

    snapshot_meta = {
        "bar_marker": str(df_scan.index[-2]),
        "close_now": snapshot.close_now,
        "close_prev": snapshot.close_prev,
        "fh_close": snapshot.fh_close,
        "fh_ema": snapshot.fh_ema,
        "impulse_pct": snapshot.impulse_pct,
        "vol_ok": snapshot.vol_ok,
        "impulse_ok": snapshot.impulse_ok,
        "long_l": long_l,
        "long_h": long_h,
        "short_h": short_h,
        "short_l": short_l,
    }

    return signal, trade_state, snapshot_meta


def build_state_payload(symbol: str, snapshot_meta: dict) -> dict:
    """
    Подготовка state для Redis.
    """
    return {
        "symbol": symbol,
        "bar_marker": snapshot_meta["bar_marker"],
        "close_now": snapshot_meta["close_now"],
        "close_prev": snapshot_meta["close_prev"],
        "fh_close": snapshot_meta["fh_close"],
        "fh_ema": snapshot_meta["fh_ema"],
        "impulse_pct": snapshot_meta["impulse_pct"],
        "vol_ok": snapshot_meta["vol_ok"],
        "impulse_ok": snapshot_meta["impulse_ok"],
        "long_l": snapshot_meta["long_l"],
        "long_h": snapshot_meta["long_h"],
        "short_h": snapshot_meta["short_h"],
        "short_l": snapshot_meta["short_l"],
    }