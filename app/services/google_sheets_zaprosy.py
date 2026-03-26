import logging
from typing import Dict
from datetime import datetime
import asyncio
from app.core.config import CASSA_SPREADSHEET_ID
from app.core.logger import logger
import gspread

# Provide an isolated gspread client specifically for Zaprosy so it doesn't fail if the global one fails
try:
    gc_zaprosy = gspread.service_account(filename='/root/my007_currency_bot/n8n-google-credentials.json')
except Exception as e:
    logger.error(f"[Zaprosy Sheets] Failed to init gspread client: {e}")
    gc_zaprosy = None

def _append_zaprosy_sync_logic(op_data: Dict, current_balance: float) -> None:
    if not gc_zaprosy:
        logger.error("[Zaprosy Sheets] gspread client not initialized.")
        return
        
    try:
        sh = gc_zaprosy.open_by_key(CASSA_SPREADSHEET_ID)
    except Exception as e:
        logger.error(f"[Zaprosy Sheets] Cannot open spreadsheet {CASSA_SPREADSHEET_ID}: {e}")
        return

    safe_name = "ЗАПРОСЫ ПО ВХОД.СУММАМ И ДОКИ"
    is_new = False
    try:
        ws = sh.worksheet(safe_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=safe_name, rows=1000, cols=8)
        is_new = True

    if is_new:
        headers = ["Дата/Время", "Описание", "Валюта", "Поступления", "Общий баланс за день", "Общий баланс за месяц"]
        ws.update(range_name="A1:F1", values=[headers])

    amt = float(op_data.get("amount", 0.0))
    ts = op_data.get("timestamp", "")
    if hasattr(ts, "strftime"): ts = ts.strftime("%d.%m.%Y %H:%M:%S")

    col_a = ws.col_values(1)
    next_row = len(col_a) + 1
    
    row = [ts, op_data.get("description", ""), op_data.get("currency", ""), amt]
    if next_row == 2:
        row.extend(["=D2", "=D2"])
    else:
        f1 = f'=IF(LEFT(A{next_row}; 10)=LEFT(A{next_row-1}; 10); E{next_row-1}+D{next_row}; D{next_row})'
        f2 = f'=IF(MID(A{next_row}; 4; 7)=MID(A{next_row-1}; 4; 7); F{next_row-1}+D{next_row}; D{next_row})'
        row.extend([f1, f2])
        
    # Append MSG_ID in 9th column. Since we have D/E/F, 7th=G 8th=H 9th=I
    row.extend(["", "", op_data.get("message_id", "")])

    ws.update(range_name=f"A{next_row}:I{next_row}", values=[row], value_input_option="USER_ENTERED")
    logger.info(f"[Zaprosy Sheets] Injected explicit formula row at row {next_row}")

def _sync_zaprosy_thread(incomes: list, message_id: int):
    for inc in incomes:
        # Provide message_id so it reaches the row mapping
        inc["message_id"] = message_id
        if "timestamp" in inc and isinstance(inc["timestamp"], str):
            try: inc["timestamp"] = datetime.fromisoformat(inc["timestamp"])
            except: pass
        _append_zaprosy_sync_logic(inc, 0.0)

async def sync_zaprosy_to_sheet(incomes: list, message_id: int, db_id: int = None):
    """Async wrapper for pushing parsed zaprosy array into specific isolated sheet."""
    try:
        await asyncio.to_thread(_execute_with_retry, _sync_zaprosy_thread, incomes, message_id)
        if db_id is not None:
            from app.db.instance import db
            db.mark_operation_synced(db_id)
    except Exception as e:
        logger.error(f"[GoogleSheetsZaprosy] Batch Zaprosy sync failed: {e}")
        if db_id is not None:
            from app.db.instance import db
            db.mark_operation_failed(db_id)
