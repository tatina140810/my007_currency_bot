import logging
from typing import Dict
from datetime import datetime
import asyncio
from app.core.config import CASSA_SPREADSHEET_ID
from app.core.logger import logger
from app.services.google_sheets import _execute_with_retry
import gspread

import os

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
CREDENTIALS_FILE = os.path.join(PROJECT_DIR, "n8n-google-credentials.json")

# Provide an isolated gspread client specifically for Zaprosy so it doesn't fail if the global one fails
try:
    gc_zaprosy = gspread.service_account(filename=CREDENTIALS_FILE)
except Exception as e:
    logger.error(f"[Zaprosy Sheets] Failed to init gspread client: {e}")
    gc_zaprosy = None

def _sync_zaprosy_thread(incomes: list, message_id: int):
    if not incomes:
        return
        
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
        ws = sh.add_worksheet(title=safe_name, rows=1000, cols=9)
        is_new = True

    if is_new:
        headers = ["Дата/Время", "Описание", "Валюта", "Поступления", "Общий баланс за день", "Общий баланс за месяц", "", "", "MSG_ID"]
        ws.update(range_name="A1:I1", values=[headers])

    # 1. Sweep old rows with this EXACT message_id (Safe now due to ArrayFormulas)
    from app.services.google_sheets import _safe_sweep_msg_id
    _safe_sweep_msg_id(ws, 9, str(message_id))

    # 2. Fetch all current data to guard against double-forwards (different msg_id, same text)
    existing_rows = ws.get_all_values()
    # Next row for appending (1-indexed in Sheets).
    # Used to generate correct formulas that reference the target row indices.
    next_row = len(existing_rows) + 1
    
    seen_sigs = set()
    for r in existing_rows[1:]:
        if len(r) < 4: continue
        d_str = str(r[0]).strip()[:10]
        desc_str = str(r[1]).strip()[:100]
        curr = str(r[2]).strip()
        try:
            amt_val = float(str(r[3]).replace(" ", "").replace(",", "."))
        except ValueError:
            amt_val = 0.0
        seen_sigs.add((d_str, curr, amt_val, desc_str))

    rows_to_insert = []
    
    for inc in incomes:
        ts = inc.get("timestamp", "")
        if isinstance(ts, str):
            try: ts = datetime.fromisoformat(ts).strftime("%d.%m.%Y %H:%M:%S")
            except Exception: pass
        elif hasattr(ts, "strftime"):
            ts = ts.strftime("%d.%m.%Y %H:%M:%S")
            
        desc = str(inc.get("description", ""))
        curr = str(inc.get("currency", ""))
        amt_f = float(inc.get("amount", 0.0))
        
        # Guard against double-forwards
        d_prefix = str(ts)[:10] if ts else ""
        desc_prefix = desc.strip()[:100]
        sig = (d_prefix, curr, amt_f, desc_prefix)
        
        if sig in seen_sigs:
            logger.info(f"[Zaprosy Sheets] DOUBLE-FORWARD BLOCKED! Exact signature found today: {sig}")
            continue
            
        seen_sigs.add(sig)
        
        # To ensure calculations never lag or break, we use explicit fast SUMIFS
        # =IF(C10=""; ""; SUMIFS(D$2:D10; C$2:C10; C10; A$2:A10; ">="&LEFT(A10;10))) ...
        row_id = next_row + len(rows_to_insert)
        
        # Day Balance: Forced coercion of dates to text, and numbers to values to bypass Google Sheets typing rules
        formula_e = f'=IF(C{row_id}=""; ""; SUMPRODUCT((C$2:C{row_id}=C{row_id}) * (LEFT(TO_TEXT(A$2:A{row_id}); 10)=LEFT(TO_TEXT(A{row_id}); 10)) * IFERROR(VALUE(SUBSTITUTE(D$2:D{row_id}; " "; "")); 0)))'
        
        # Month Balance
        formula_f = f'=IF(C{row_id}=""; ""; SUMPRODUCT((C$2:C{row_id}=C{row_id}) * (MID(TO_TEXT(A$2:A{row_id}); 4; 7)=MID(TO_TEXT(A{row_id}); 4; 7)) * IFERROR(VALUE(SUBSTITUTE(D$2:D{row_id}; " "; "")); 0)))'
        
        # Columns: Дата, Описание, Валюта, Сумма, Баланс_День, Баланс_Месяц, <empty>, <empty>, MSG_ID
        row = [ts, desc, curr, amt_f, formula_e, formula_f, "", "", str(message_id)]
        rows_to_insert.append(row)
        
    if rows_to_insert:
        ws.append_rows(rows_to_insert, value_input_option="USER_ENTERED", table_range="A1")
        logger.info(f"[Zaprosy Sheets] Appended {len(rows_to_insert)} operations.")

async def sync_zaprosy_to_sheet(incomes: list, message_id: int, db_id: int = None):
    """Async wrapper for pushing parsed zaprosy array into specific isolated sheet."""
    try:
        await asyncio.to_thread(_execute_with_retry, _sync_zaprosy_thread, incomes, message_id)
        if db_id is not None:
            from app.db.instance import db
            db.mark_operation_synced(db_id)
    except Exception as e:
        logger.error(f"[GoogleSheetsZaprosy] Batch Zaprosy sync failed: {e}")
        # Keep the operation in PENDING so reconciliation can retry later.
        # (Marking FAILED here would stop further retries.)
