import json
import math
from dataclasses import dataclass
from typing import Optional

import pandas as pd

from config import EMA_LEN, IMPULSE_LOOKBACK_H, IMPULSE_MIN_PCT, USE_VOL_FILTER, VOL_MA_LEN


@dataclass
class Signal:
    side: str
    entry: float
    stop: float
    tp: float
    risk_pct: float
    signature: tuple


@dataclass
class TradeState:
    in_trade: bool = False
    trade_dir: int = 0   # 1 long, -1 short, 0 none
    entry: Optional[float] = None
    stop: Optional[float] = None
    tp: Optional[float] = None
    last_signature: Optional[str] = None
    last_bar_marker: Optional[str] = None


def signature_to_str(sig: tuple | None) -> Optional[str]:
    return json.dumps(sig) if sig is not None else None


def state_from_db(row: dict) -> TradeState:
    return TradeState(
        in_trade=bool(row.get("in_trade", 0)),
        trade_dir=int(row.get("trade_dir", 0)),
        entry=row.get("entry"),
        stop=row.get("stop"),
        tp=row.get("tp"),
        last_signature=row.get("last_signature"),
        last_bar_marker=row.get("last_bar_marker"),
    )


def state_to_db(telegram_id: int, symbol: str, st: TradeState) -> dict:
    return {
        "telegram_id": telegram_id,
        "symbol": symbol,
        "in_trade": 1 if st.in_trade else 0,
        "trade_dir": st.trade_dir,
        "entry": st.entry,
        "stop": st.stop,
        "tp": st.tp,
        "last_signature": st.last_signature,
        "last_bar_marker": st.last_bar_marker,
    }


def _ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def _sma(series: pd.Series, length: int) -> pd.Series:
    return series.rolling(length).mean()


def _pivot_low_value(df: pd.DataFrame, idx: int, sens: int) -> Optional[float]:
    if idx - sens < 0 or idx + sens >= len(df):
        return None

    center = float(df["low"].iloc[idx])
    for j in range(1, sens + 1):
        if center >= float(df["low"].iloc[idx - j]) or center >= float(df["low"].iloc[idx + j]):
            return None

    return center


def _pivot_high_value(df: pd.DataFrame, idx: int, sens: int) -> Optional[float]:
    if idx - sens < 0 or idx + sens >= len(df):
        return None

    center = float(df["high"].iloc[idx])
    for j in range(1, sens + 1):
        if center <= float(df["high"].iloc[idx - j]) or center <= float(df["high"].iloc[idx + j]):
            return None

    return center


def _build_long_structure(df: pd.DataFrame, sens: int):
    long_l = None
    long_h = None
    long_l_bar = None
    last_confirmable = len(df) - sens - 1

    for i in range(len(df)):
        if i > last_confirmable:
            break

        pl = _pivot_low_value(df, i, sens)
        ph = _pivot_high_value(df, i, sens)

        if pl is not None:
            long_l = pl
            long_l_bar = i
            long_h = None

        if long_l is not None and ph is not None:
            ph_bar = i
            if long_h is None and ph_bar > long_l_bar:
                long_h = ph
            elif long_h is not None and ph_bar > long_l_bar and ph >= long_h:
                long_h = ph

    return long_l, long_h


def _build_short_structure(df: pd.DataFrame, sens: int):
    short_h = None
    short_l = None
    short_h_bar = None
    last_confirmable = len(df) - sens - 1

    for i in range(len(df)):
        if i > last_confirmable:
            break

        ph = _pivot_high_value(df, i, sens)
        pl = _pivot_low_value(df, i, sens)

        if ph is not None:
            short_h = ph
            short_h_bar = i
            short_l = None

        if short_h is not None and pl is not None:
            pl_bar = i
            if short_l is None and pl_bar > short_h_bar:
                short_l = pl
            elif short_l is not None and pl_bar > short_h_bar and pl <= short_l:
                short_l = pl

    return short_h, short_l


def _impulse_pct(df_1h: pd.DataFrame) -> Optional[float]:
    if len(df_1h) < IMPULSE_LOOKBACK_H:
        return None

    window = df_1h.iloc[-IMPULSE_LOOKBACK_H:]
    hi = float(window["high"].max())
    lo = float(window["low"].min())

    if lo == 0:
        return None

    return (hi - lo) / lo * 100.0


def compute_bar_signal(
    df_scan: pd.DataFrame,
    df_1h: pd.DataFrame,
    user: dict,
) -> tuple[Optional[Signal], Optional[Signal]]:
    if (
        len(df_scan) < int(user["structure_sensitivity"]) * 2 + 6
        or len(df_1h) < max(EMA_LEN, VOL_MA_LEN, IMPULSE_LOOKBACK_H) + 2
    ):
        return None, None

    sens = int(user["structure_sensitivity"])
    enable_long = bool(user["enable_long"])
    enable_short = bool(user["enable_short"])
    max_stop_pct = float(user["max_stop_pct"])
    stop_buffer_pct = float(user["stop_buffer_pct"])
    tp_rr = float(user["tp_rr"])

    # Работаем только по закрытым свечам:
    # -2 = последняя закрытая 5m/1h свеча
    # -3 = предыдущая закрытая свеча
    close_now = float(df_scan["close"].iloc[-2])
    close_prev = float(df_scan["close"].iloc[-3])

    fh_close = float(df_1h["close"].iloc[-2])
    fh_ema = float(_ema(df_1h["close"], EMA_LEN).iloc[-2])

    vol_col = "vol" if "vol" in df_1h.columns else "volume"
    fh_vol = float(df_1h[vol_col].iloc[-2]) if vol_col in df_1h.columns else 0.0
    fh_vol_ma = float(_sma(df_1h[vol_col], VOL_MA_LEN).iloc[-2]) if vol_col in df_1h.columns else 0.0

    impulse_pct = _impulse_pct(df_1h)
    if impulse_pct is None:
        return None, None

    vol_ok = (not USE_VOL_FILTER) or (fh_vol > fh_vol_ma)
    impulse_ok = impulse_pct >= IMPULSE_MIN_PCT

    long_l, long_h = _build_long_structure(df_scan, sens)
    short_h, short_l = _build_short_structure(df_scan, sens)

    raw_buy = None
    if enable_long:
        long_trend_ok = fh_close > fh_ema
        long_filter_ok = long_trend_ok and impulse_ok and vol_ok

        long_reclaim = (
            long_filter_ok
            and long_h is not None
            and long_l is not None
            and close_now > long_h
            and close_prev <= long_h
        )

        if long_reclaim:
            long_stop = long_l * (1.0 - stop_buffer_pct / 100.0)
            long_stop_dist_pct = (
                ((close_now - long_stop) / close_now) * 100.0
                if close_now != 0 else math.inf
            )

            if long_stop_dist_pct <= max_stop_pct:
                long_tp = close_now + (close_now - long_stop) * tp_rr
                raw_buy = Signal(
                    side="long",
                    entry=close_now,
                    stop=long_stop,
                    tp=long_tp,
                    risk_pct=long_stop_dist_pct,
                    signature=(
                        "long",
                        round(close_now, 10),
                        round(long_stop, 10),
                        round(long_tp, 10),
                    ),
                )

    raw_sell = None
    if enable_short:
        short_trend_ok = fh_close < fh_ema
        short_filter_ok = short_trend_ok and impulse_ok and vol_ok

        short_breakdown = (
            short_filter_ok
            and short_h is not None
            and short_l is not None
            and close_now < short_l
            and close_prev >= short_l
        )

        if short_breakdown:
            short_stop = short_h * (1.0 + stop_buffer_pct / 100.0)
            short_stop_dist_pct = (
                ((short_stop - close_now) / close_now) * 100.0
                if close_now != 0 else math.inf
            )

            if short_stop_dist_pct <= max_stop_pct:
                short_tp = close_now - (short_stop - close_now) * tp_rr
                raw_sell = Signal(
                    side="short",
                    entry=close_now,
                    stop=short_stop,
                    tp=short_tp,
                    risk_pct=short_stop_dist_pct,
                    signature=(
                        "short",
                        round(close_now, 10),
                        round(short_stop, 10),
                        round(short_tp, 10),
                    ),
                )

    return raw_buy, raw_sell


def update_trade_state_for_bar(trade_state: TradeState, df_scan: pd.DataFrame):
    if not trade_state.in_trade:
        return

    # Проверяем стоп/тейк только по последней закрытой 5m свече
    bar_high = float(df_scan["high"].iloc[-2])
    bar_low = float(df_scan["low"].iloc[-2])

    long_stop_hit = trade_state.in_trade and trade_state.trade_dir == 1 and bar_low <= trade_state.stop
    long_tp_hit = trade_state.in_trade and trade_state.trade_dir == 1 and bar_high >= trade_state.tp

    short_stop_hit = trade_state.in_trade and trade_state.trade_dir == -1 and bar_high >= trade_state.stop
    short_tp_hit = trade_state.in_trade and trade_state.trade_dir == -1 and bar_low <= trade_state.tp

    if long_stop_hit or long_tp_hit or short_stop_hit or short_tp_hit:
        trade_state.in_trade = False
        trade_state.trade_dir = 0
        trade_state.entry = None
        trade_state.stop = None
        trade_state.tp = None


def process_user_symbol(df_scan: pd.DataFrame, df_1h: pd.DataFrame, user: dict, trade_state: TradeState):
    update_trade_state_for_bar(trade_state, df_scan)

    raw_buy, raw_sell = compute_bar_signal(df_scan, df_1h, user)

    if raw_buy is not None and (not trade_state.in_trade or trade_state.trade_dir == 1):
        sig_str = signature_to_str(raw_buy.signature)
        if trade_state.last_signature == sig_str:
            return None

        trade_state.in_trade = True
        trade_state.trade_dir = 1
        trade_state.entry = raw_buy.entry
        trade_state.stop = raw_buy.stop
        trade_state.tp = raw_buy.tp
        trade_state.last_signature = sig_str
        trade_state.last_bar_marker = str(df_scan.index[-2])
        return raw_buy

    if raw_sell is not None and (not trade_state.in_trade or trade_state.trade_dir == -1):
        sig_str = signature_to_str(raw_sell.signature)
        if trade_state.last_signature == sig_str:
            return None

        trade_state.in_trade = True
        trade_state.trade_dir = -1
        trade_state.entry = raw_sell.entry
        trade_state.stop = raw_sell.stop
        trade_state.tp = raw_sell.tp
        trade_state.last_signature = sig_str
        trade_state.last_bar_marker = str(df_scan.index[-2])
        return raw_sell

    return None


def calculate_signal(df_scan, df_1h, user, trade_state):
    return process_user_symbol(df_scan, df_1h, user, trade_state)