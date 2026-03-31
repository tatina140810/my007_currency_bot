import gspread
import asyncio
import datetime
from collections import defaultdict
from app.core.logger import logger
from app.core.config import CASSA_SPREADSHEET_ID
from app.services.google_sheets import _get_gc, _execute_with_retry, _get_gsheets_lock

def _append_zak_operations_sync(operations: list):
    """
    Synchronous logic to append parsed ZAK operations to Google Sheets.
    """
    if not operations:
        return

    gc = _get_gc()
    sh = gc.open_by_key(CASSA_SPREADSHEET_ID)
    
    # 1. Update Detailed Sheet
    ws_detail_name = "Проценты_детально"
    try:
        ws_detail = sh.worksheet(ws_detail_name)
    except gspread.exceptions.WorksheetNotFound:
        ws_detail = sh.add_worksheet(title=ws_detail_name, rows=1000, cols=16)
        headers = [
            "Дата", "Тип операции", "Банк", "Компания", "Валюта", 
            "Сумма исходная", "Процент", "Режим комиссии", "Сумма комиссии", 
            "Чистая сумма", "Сумма с комиссией", "Комментарий / тег", 
            "Исходный текст строки", "Исходный текст сообщения", 
            "Chat_Message_ID", "Время добавления", "Дата (свод)"
        ]
        ws_detail.append_row(headers)
        ws_detail.freeze(rows=1)

    # Fetch existing IDs to avoid duplicates (Column O is Chat_Message_ID)
    existing_data = ws_detail.get_all_values()
    existing_ids = set()
    if len(existing_data) > 1:
        for row in existing_data[1:]:
            if len(row) > 14:
                existing_ids.add(row[14]) # Column O

    rows_to_append = []
    
    import hashlib
    
    for op in operations:
        chat_msg_id = f"{op['chat_id']}_{op['message_id']}"
        
        # Deduplication check - we use MD5 because Python's hash() is randomized per process
        raw_str = op.get('raw_line', '') or ''
        stable_hash = hashlib.md5(raw_str.encode('utf-8')).hexdigest()[:8]
        uniq_id = f"{chat_msg_id}_{stable_hash}"
        
        if uniq_id in existing_ids:
            continue
            
        existing_ids.add(uniq_id)
        
        now_str = datetime.datetime.now().strftime("%d.%m.%Y %H:%M:%S")
        if hasattr(op['date'], 'strftime'):
            date_str = op['date'].strftime("%d.%m.%Y %H:%M")
            date_only = op['date'].strftime("%d.%m.%Y")
        else:
            date_str = str(op['date'])
            date_only = date_str.split(" ")[0] if " " in date_str else date_str
        
        row_idx = len(existing_data) + 1 + len(rows_to_append)
        percent_str_form = op['percent_str'].replace('.', ',') if op['percent_str'] else "0%"
        
        if not op['percent_str'] or op['percent_str'] in ("0%", "0"):
            formula_fee = 0.0
            formula_net = f"=F{row_idx}"
            formula_gross = f"=F{row_idx}"
        elif op['fee_mode'] == 'extra':
            formula_fee = f"=F{row_idx}*G{row_idx}"
            formula_net = f"=F{row_idx}"
            formula_gross = f"=F{row_idx}+I{row_idx}"
        else:
            formula_fee = f"=F{row_idx}*G{row_idx}/(1+G{row_idx})"
            formula_net = f"=F{row_idx}-I{row_idx}"
            formula_gross = f"=F{row_idx}"
        
        row = [
            date_str,
            op['type'],
            op['bank'],
            op['company'].replace('=', '') if op['company'] else "",
            op['currency'].replace('=', '') if op['currency'] else "",
            float(op['amount']) if op['amount'] else 0.0,
            percent_str_form,
            op['fee_mode'],
            formula_fee,
            formula_net,
            formula_gross,
            op['comment'].replace('\n', ' / ') if op['comment'] else "",
            op['raw_line'].replace('\n', ' / ') if op['raw_line'] else "",
            op['raw_text'].replace('\n', ' / ') if op['raw_text'] else "",
            uniq_id,
            now_str,
            date_only
        ]
        rows_to_append.append(row)

    if rows_to_append:
        ws_detail.append_rows(rows_to_append, value_input_option="USER_ENTERED", table_range="A1")
        logger.info(f"[ZAK Sync] Appended {len(rows_to_append)} rows to {ws_detail_name}.")

    # 2. Update Summary Sheet
    _update_zak_summary_sync(sh, ws_detail)

def _update_zak_summary_sync(sh, ws_detail):
    """
    Rebuilds the summary sheet using Google Sheets QUERY formulas.
    Arranged horizontally to allow infinite downward historical date expansion.
    """
    ws_summary_name = "Проценты_свод"
    try:
        ws_summary = sh.worksheet(ws_summary_name)
    except gspread.exceptions.WorksheetNotFound:
        ws_summary = sh.add_worksheet(title=ws_summary_name, rows=1000, cols=20)
        
    ws_summary.clear()
    if ws_summary.col_count < 20:
        ws_summary.add_cols(20 - ws_summary.col_count)
    
    updates = [
        {"range": "A1:A2", "values": [["ИТОГИ ПО КОМПАНИЯМ"], ["=QUERY('Проценты_детально'!A:Q; \"select Q, D, E, sum(F), sum(I) where Q is not null and D is not null and D != 'Компания' group by Q, D, E label Q 'Дата', D 'Компания', E 'Валюта', sum(F) 'Сумма Пополнения/Снятия', sum(I) 'Комиссия'\"; 0)"]]},
        
        {"range": "G1:G2", "values": [["ИТОГИ ПО ВАЛЮТАМ"], ["=QUERY('Проценты_детально'!A:Q; \"select Q, E, sum(F), sum(I) where Q is not null and E is not null and E != 'Валюта' group by Q, E label Q 'Дата', E 'Валюта', sum(F) 'Сумма исходная', sum(I) 'Комиссия'\"; 0)"]]},
        
        {"range": "L1:L2", "values": [["ИТОГИ ПО ТИПАМ"], ["=QUERY('Проценты_детально'!A:Q; \"select Q, B, sum(F) where Q is not null and B is not null and B != 'Тип операции' group by Q, B label Q 'Дата', B 'Тип операции', sum(F) 'Сумма исходная'\"; 0)"]]},
        
        {"range": "P1:P2", "values": [["ИТОГИ ПО БАНКАМ"], ["=QUERY('Проценты_детально'!A:Q; \"select Q, C, sum(F) where Q is not null and C is not null and C != 'Банк' group by Q, C label Q 'Дата', C 'Банк', sum(F) 'Сумма исходная'\"; 0)"]]}
    ]
    
    ws_summary.batch_update(updates, value_input_option="USER_ENTERED")
    logger.info("[ZAK Sync] Summary updated with expanding horizontal QUERY formulas.")

async def append_zak_operations_to_sheet(operations: list, db_id: int = None):
    """Async wrapper to push Zak operations safely."""
    if not operations:
        return
    try:
        async with _get_gsheets_lock():
            await asyncio.to_thread(_execute_with_retry, _append_zak_operations_sync, operations)
            if db_id is not None:
                from app.db.instance import db
                db.mark_operation_synced(db_id)
    except Exception as e:
        logger.error(f"[GoogleSheetsZak] Error forwarding zak msg: {e}")
        if db_id is not None:
            from app.db.instance import db
            db.mark_operation_failed(db_id)
