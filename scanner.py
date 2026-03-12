import asyncio
from config import SCAN_TIMEFRAME, FILTER_TIMEFRAME, SCAN_SLEEP_SECONDS
from db import get_all_active_users, get_user_symbol_state, save_user_symbol_state, log_signal
from mexc_client import get_klines, get_contract_symbols
from strategy import process_user_symbol, state_from_db, state_to_db, signature_to_str


async def run_scanner(bot_app):
    symbols = get_contract_symbols()
    print(f"Scanner started. Loaded {len(symbols)} symbols.")

    while True:
        users = get_all_active_users()

        # refresh symbol list every full pass so new MEXC contracts appear automatically
        try:
            symbols = get_contract_symbols()
        except Exception as e:
            print("symbol refresh error", e)

        for symbol in symbols:
            try:
                df_scan = get_klines(symbol, SCAN_TIMEFRAME, 300)
                df_1h = get_klines(symbol, FILTER_TIMEFRAME, 300)
                if df_scan is None or df_1h is None:
                    continue

                # use exchange timestamp if present, else fallback to dataframe length
                bar_marker = None
                for col in ["time", "timestamp", "ts"]:
                    if col in df_scan.columns:
                        bar_marker = str(df_scan[col].iloc[-1])
                        break
                if bar_marker is None:
                    bar_marker = str(len(df_scan))

                for user in users:
                    state_row = get_user_symbol_state(user["telegram_id"], symbol)
                    if state_row.get("last_bar_marker") == bar_marker:
                        continue

                    trade_state = state_from_db(state_row)
                    signal = process_user_symbol(df_scan, df_1h, user, trade_state)

                    state_row = state_to_db(user["telegram_id"], symbol, trade_state)
                    state_row["last_bar_marker"] = bar_marker

                    if signal:
                        sig_text = signature_to_str(signal.signature)
                        # exact duplicate blocked, but new TVH/new entry will pass
                        if state_row.get("last_signature") != sig_text:
                            text = (
                                f"{'🟢 LONG' if signal.side == 'long' else '🔴 SHORT'}\n\n"
                                f"Монета: {symbol}\n"
                                f"ТФ: {SCAN_TIMEFRAME}\n"
                                f"Вход: {signal.entry:.8f}\n"
                                f"Стоп: {signal.stop:.8f}\n"
                                f"Тейк: {signal.tp:.8f}\n"
                                f"Риск: {signal.risk_pct:.2f}%"
                            )
                            await bot_app.bot.send_message(chat_id=user["telegram_id"], text=text)
                            state_row["last_signature"] = sig_text
                            log_signal(
                                telegram_id=user["telegram_id"],
                                symbol=symbol,
                                side=signal.side,
                                entry=signal.entry,
                                stop=signal.stop,
                                tp=signal.tp,
                                risk_pct=signal.risk_pct,
                                timeframe=SCAN_TIMEFRAME,
                                signature=sig_text,
                            )

                    save_user_symbol_state(state_row)

            except Exception as e:
                print("scanner error", symbol, e)

        await asyncio.sleep(SCAN_SLEEP_SECONDS)
