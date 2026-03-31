import os
import asyncio
import gspread
import time
import datetime
import requests
import email.utils
import google.auth._helpers
from typing import Callable, Any
from app.core.logger import logger
from app.db.instance import db

# Project root path
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
CREDENTIALS_FILE = os.path.join(PROJECT_DIR, "n8n-google-credentials.json")

# Spreadsheet IDs
SPREADSHEET_ID = "179BoxA3jyALn8JPVzVt6f0R_9nfOqNf6xXk40C3czYA"
CLIENT_SPREADSHEET_ID = "1L-b47A03ahpuzIas1IzqfQLiqTb_1EKAngHsHSCjldY"

_gsheets_lock = None

def _get_gsheets_lock():
    global _gsheets_lock
    if _gsheets_lock is None:
        _gsheets_lock = asyncio.Lock()
    return _gsheets_lock

def _apply_time_patch():
    """Patches google-auth time to avoid 'Token expired' errors if system clock is out of sync."""
    try:
        r = requests.head('https://www.google.com', timeout=5)
        date_header = r.headers['Date']
        real_time = email.utils.parsedate_to_datetime(date_header).replace(tzinfo=None)
        local_utc = datetime.datetime.utcnow()
        delta = real_time - local_utc
        
        old_utcnow = google.auth._helpers.utcnow
        def patched_utcnow():
            return old_utcnow() + delta
        google.auth._helpers.utcnow = patched_utcnow
    except Exception as e:
        logger.error(f"[GoogleSheets] Failed to patch time: {e}")

def _execute_with_retry(func: Callable, *args, max_retries: int = 3, **kwargs) -> Any:
    """Helper to execute gspread functions with exponential backoff and time patching."""
    for attempt in range(max_retries):
        try:
            _apply_time_patch()
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"[GoogleSheets] Attempt {attempt+1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
            else:
                from app.services.alerts import send_system_alert_sync
                logger.error(f"[GoogleSheets] Persistent failure after {max_retries} attempts: {e}")
                send_system_alert_sync(f"❌ **Google Sheets Sync Failure**\nOperation failed after {max_retries} retries.\nError: {e}")
                raise

def _get_gc():
    return gspread.service_account(filename=CREDENTIALS_FILE)

# --- Public Async API ---

async def sync_conversions_to_cassa_sheet(conversions: list, db_id: int = None):
    """Async wrapper for conversions sync."""
    if not conversions: return
    try:
        async with _get_gsheets_lock():
            await asyncio.to_thread(_sync_conversions_to_cassa_thread, conversions)
            if db_id is not None:
                from app.db.instance import db
                db.mark_operation_synced(db_id)
    except Exception as e:
        logger.error(f"[GoogleSheets] Conversions sync failed: {e}")
        if db_id is not None:
            from app.db.instance import db
            db.mark_operation_failed(db_id)

async def sync_payment_list_to_cassa_sheet(parsed_data: dict, db_id: int = None):
    """Async wrapper for /back_report styled payment list syncing into Cassa."""
    if not parsed_data or not parsed_data.get("items"): return
    try:
        async with _get_gsheets_lock():
            await asyncio.to_thread(_execute_with_retry, _sync_payment_list_thread, parsed_data)
            if db_id is not None:
                from app.db.instance import db
                db.mark_operation_synced(db_id)
    except Exception as e:
        logger.error(f"[GoogleSheets] Payment list sync failed: {e}")
        if db_id is not None:
            from app.db.instance import db
            db.mark_operation_failed(db_id)

async def append_operation_to_sheet(op_data: dict):
    """Async wrapper for internal history sync."""
    try:
        async with _get_gsheets_lock():
            await asyncio.to_thread(_execute_with_retry, _append_operation_sync_logic, op_data)
    except Exception as e:
        logger.error(f"[GoogleSheets] Internal history sync failed: {e}")

async def sync_all_balances_to_sheet():
    """Async wrapper for balance matrix sync."""
    try:
        async with _get_gsheets_lock():
            await asyncio.to_thread(_execute_with_retry, _sync_balances_sync_logic)
    except Exception as e:
        logger.error(f"[GoogleSheets] Balance matrix sync failed: {e}")

async def sync_daily_income(report_date_str: str, rows_data: list):
    """Async wrapper for daily income sync."""
    try:
        async with _get_gsheets_lock():
            await asyncio.to_thread(_execute_with_retry, _sync_daily_income_sync_logic, report_date_str, rows_data)
    except Exception as e:
        logger.error(f"[GoogleSheets] Daily income sync failed: {e}")

async def append_client_operation_to_sheet(op_data: dict, current_balance: float):
    """Async wrapper for client-facing sheet sync."""
    from app.core.constants import KG_TZ
    start_date = datetime.datetime(2026, 3, 11, 6, 0, 0, tzinfo=KG_TZ)
    if datetime.datetime.now(KG_TZ) < start_date:
        return

    try:
        async with _get_gsheets_lock():
            await asyncio.to_thread(_execute_with_retry, _append_client_operation_sync_logic, op_data, current_balance)
    except Exception as e:
        logger.error(f"[GoogleSheets] Client sheet sync failed: {e}")

# --- Internal Synchronous Logic (Wrapped in threads/retries) ---

def _safe_sweep_msg_id(ws, col_index: int, target_msg_id: str):
    import logging
    try:
        col_vals = ws.col_values(col_index)
        rows_to_delete = [i + 1 for i, val in enumerate(col_vals) if val == target_msg_id and i > 0]
        
        if rows_to_delete:
            blocks = []
            start = rows_to_delete[0]
            prev = rows_to_delete[0]
            for idx in rows_to_delete[1:]:
                if idx == prev + 1:
                    prev = idx
                else:
                    blocks.append((start, prev))
                    start = idx
                    prev = idx
            blocks.append((start, prev))
            
            for start_idx, end_idx in reversed(blocks):
                try:
                    ws.delete_rows(start_index=start_idx, end_index=end_idx)
                except AttributeError:
                    for r_idx in range(end_idx, start_idx - 1, -1):
                        ws.delete_row(r_idx)
            
            logging.getLogger(__name__).info(f"[GoogleSheets] Swept {len(rows_to_delete)} old rows for msg_id {target_msg_id}")
    except Exception as e:
        logging.getLogger(__name__).warning(f"[GoogleSheets] Soft-failed sweep (likely empty col or API err): {e}")

def _sync_conversions_to_cassa_thread(conversions: list):
    from app.core.config import CASSA_SPREADSHEET_ID
    from app.core.constants import KG_TZ
    
    def logic():
        gc = _get_gc()
        sh = gc.open_by_key(CASSA_SPREADSHEET_ID)
        worksheet_name = "конвертации"
        try:
            ws = sh.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=worksheet_name, rows=1000, cols=10)
            ws.append_row(["Дата", "Клиент", "Сумма", "Валюта", "Курс", "Сумма РУБ", "Оригинал текстом", "MSG_ID"])
            ws.freeze(rows=1)
            
        now_str = datetime.datetime.now(KG_TZ).strftime("%d.%m.%Y")
        
        # Маппинг валют на русские названия для отчета
        curr_map_ru = {
            "EUR": "евро", 
            "CNY": "юань", 
            "USD": "доллар", 
            "AED": "дирхам", 
            "KZT": "тенге", 
            "KGS": "сом", 
            "RUB": "рубль", 
            "USDT": "usdt"
        }
        
        # Sweep Phase: purge old rows with that exact msg_id from Col H (index 8)
        if conversions and conversions[0].get("msg_id"):
            target_msg_id = str(conversions[0].get("msg_id"))
            _safe_sweep_msg_id(ws, 8, target_msg_id)

        # Recalculate row index correctly after potential deletions
        existing_records = ws.get_all_values()
        next_row_index = len(existing_records) + 1
        
        rows = []
        for c in conversions:
            pur_currency_ru = curr_map_ru.get(c["currency"], c["currency"])
            
            amount_val = float(c["amount"]) if c["amount"] % 1 != 0 else int(c["amount"])
            rate_val = float(c["rate"])
            
            row_formula = f"=C{next_row_index}*E{next_row_index}"
            
            rows.append([
                now_str, 
                c["client"], 
                amount_val, 
                pur_currency_ru, 
                rate_val, 
                row_formula,
                c.get("original_text", ""),
                str(c.get("msg_id", "")) # Hidden Column H mapping
            ])
            next_row_index += 1
            
        ws.append_rows(rows, value_input_option="USER_ENTERED", table_range="A1")
        logger.info(f"[GoogleSheets] Synced {len(rows)} conversions to CASSA.")

    _execute_with_retry(logic)

def _sync_payment_list_thread(parsed_data: dict):
    from app.core.config import CASSA_SPREADSHEET_ID
    
    gc = _get_gc()
    sh = gc.open_by_key(CASSA_SPREADSHEET_ID)
    worksheet_name = "Платежи"
    
    try:
        ws = sh.worksheet(worksheet_name)
        # Убедимся, что заголовки 5 и 6 колонок существуют, если пользователь их случайно удалил
        current_headers = ws.row_values(1)
        if len(current_headers) < 7 or "Сумма" not in current_headers:
            ws.update(range_name="A1:G1", values=[[
                "Отчет Back", "Компания", "Тип", 
                "Контрагент", "Валюта платежа", "Сумма", "Комиссия банка (Формула)"
            ]])
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows=1000, cols=8)
        ws.append_row([
            "Отчет Back", "Компания", "Тип", 
            "Контрагент", "Валюта платежа", "Сумма", "Комиссия банка (Формула)"
        ])
        ws.freeze(rows=1)

    date_str = parsed_data.get("date", "Unknown Date")
    items = parsed_data.get("items", [])
    target_msg_id = str(parsed_data.get("msg_id", ""))
    
    if target_msg_id and target_msg_id != "None":
        _safe_sweep_msg_id(ws, 8, target_msg_id)

    # 1. Сначала добавляем строку-разделитель с датой и привязанным msg_id
    ws.append_row([f"--- ПЛАТЕЖИ ЗА {date_str} ---", "", "", "", "", "", "", target_msg_id])
    
    # Рекурсивно вычисляем следующий ряд с учетом только что вставленного лейбла
    existing_len = len(ws.col_values(1))
    next_row_index = existing_len + 1

    # 2. Формируем строки платежей
    rows = []
    
    for item in items:
        ridx = next_row_index
        formula_commission = f'=IFERROR(IFS(REGEXMATCH(LOWER(A{ridx}); "бакай"); IFS(REGEXMATCH(UPPER(E{ridx}); "USD"); MEDIAN(150; F{ridx}*0,002; 500); REGEXMATCH(UPPER(E{ridx}); "EUR"); MEDIAN(30; F{ridx}*0,002; 150); REGEXMATCH(UPPER(E{ridx}); "CNY|ЮАНЬ"); MEDIAN(160; F{ridx}*0,002; 1100); REGEXMATCH(UPPER(E{ridx}); "AED|ДИРХАМ"); MEDIAN(120; F{ridx}*0,002; 600); REGEXMATCH(UPPER(E{ridx}); "RUB|РУБ"); MEDIAN(500; F{ridx}*0,002; 1500); TRUE; F{ridx}*0,002); REGEXMATCH(LOWER(A{ridx}); "элдик"); IFS(REGEXMATCH(UPPER(E{ridx}); "USD"); MEDIAN(50; F{ridx}*0,002; 100); REGEXMATCH(UPPER(E{ridx}); "EUR"); MEDIAN(25; F{ridx}*0,002; 100); REGEXMATCH(UPPER(E{ridx}); "CNY|ЮАНЬ"); MEDIAN(160; F{ridx}*0,002; 1100); REGEXMATCH(UPPER(E{ridx}); "RUB|РУБ"); MEDIAN(200; F{ridx}*0,002; 1000); TRUE; F{ridx}*0,002); TRUE; F{ridx}*0,002); "")'
        
        rows.append([
            item.get("bank", ""),         # Отчет Back (A)
            item.get("company", ""),      # Компания (B)
            item.get("type", ""),         # Тип (C)
            item.get("counterparty", ""), # Контрагент (D)
            item.get("currency", ""),     # Валюта платежа (E)
            item.get("sum", 0.0),         # Сумма (F)
            formula_commission,           # Комиссия банка (Formula G)
            target_msg_id,                # Msg ID (Col H)
            date_str                      # Дата (Col I) - For SUMIFS linking
        ])
        next_row_index += 1
        
    # 3. Записываем платежи одним блоком
    if rows:
        ws.append_rows(rows, value_input_option="USER_ENTERED", table_range="A1")
    
    logger.info(f"[GoogleSheets] Synced {len(rows)} payment list records to {worksheet_name}.")

def _append_operation_sync_logic(op_data: dict):
    gc = _get_gc()
    sh = gc.open_by_key(SPREADSHEET_ID)
    chat_id = op_data.get("chat_id")
    chat_info = db.get_chat(chat_id)
    chat_name = chat_info[1] if chat_info and chat_info[1] else f"Chat_{chat_id}"
    
    safe_name = chat_name[:31]
    for c in r'\/:*?[]': safe_name = safe_name.replace(c, "_")

    try:
        ws = sh.worksheet(safe_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=safe_name, rows=1000, cols=10)
        ws.append_row(["ID", "Type", "Currency", "Amount", "Description", "Timestamp"])
        ws.freeze(rows=1)

    ws.append_row([
        op_data.get("id", ""), op_data.get("type", ""), op_data.get("currency", ""),
        float(op_data.get("amount", 0.0)), op_data.get("description", ""), op_data.get("timestamp", "")
    ])
    # Не пересобираем вкладку "Balances" на каждую операцию.
    # Обновление делается батчером (sync_all_balances_to_sheet) после обработки очереди,
    # чтобы снизить нагрузку на Google Sheets.


def _sync_balances_sync_logic():
    gc = _get_gc()
    sh = gc.open_by_key(SPREADSHEET_ID)
    ws = sh.worksheet("Balances")
    ws.clear()
    ws.freeze(rows=1)
    
    header = ["Chat ID", "Chat Name", "Currency", "Balance"]
    data = [header]
    from app.core.config import CURRENCIES
    for chat in db.get_all_chats():
        bals = db.get_balances(chat[0])
        for curr in CURRENCIES:
            val = bals.get(curr, 0.0)
            if val != 0: data.append([str(chat[0]), chat[1], curr, float(val)])
    ws.append_rows(data)

def _sync_daily_income_sync_logic(report_date_str: str, rows_data: list):
    gc = _get_gc()
    sh = gc.open_by_key(SPREADSHEET_ID)
    titles = [w.title for w in sh.worksheets()]
    if "Income_Matrix" not in titles:
        sh.add_worksheet(title="Income_Matrix", rows=100, cols=20)
    ws = sh.worksheet("Income_Matrix")
    ws.clear()
    if not rows_data:
        ws.update(values=[[f"No income data for {report_date_str}"]], range_name="A1")
        return
    header = ["Client Name", "Currency", "Total Amount", "Details"]
    data = [[f"Daily Income Report for: {report_date_str}"], header]
    for r in rows_data: data.append([r[0], r[1], r[2], r[3]])
    ws.append_rows(data)

def _append_client_operation_sync_logic(op_data: dict, current_balance: float):
    gc = _get_gc()
    chat_name = op_data.get("chat_name", f"Chat_{op_data.get('chat_id')}")
    
    # 1. Skip Kursy group
    if "курсы" in chat_name.lower() or "конвертации" in chat_name.lower() or "суммы" in chat_name.lower():
        return
        
    # 2. Redirect ЗАПРОСЫ... to Cassa Spreadsheet
    from app.core.config import CASSA_SPREADSHEET_ID
    if "запросы по вход" in chat_name.lower() or "запросы" in chat_name.lower():
        sh = gc.open_by_key(CASSA_SPREADSHEET_ID)
    else:
        sh = gc.open_by_key(CLIENT_SPREADSHEET_ID)
        
    safe_name = chat_name[:31]
    for c in r'\/:*?[]': safe_name = safe_name.replace(c, "_")

    is_new = False
    try:
        ws = sh.worksheet(safe_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=safe_name, rows=1000, cols=10)
        is_new = True

    if is_new:
        headers = ["Дата/Время", "Описание", "Валюта", "Поступления", "Конвертация", "Оплаты ПП", "Выдача наличных", "Доп. расходы", "Общий баланс", "Общий баланс за месяц"]
        ws.update(range_name="A1:J1", values=[headers])

    amt = float(op_data.get("amount", 0.0))
    ts = op_data.get("timestamp", "")
    if hasattr(ts, "strftime"): ts = ts.strftime("%d.%m.%Y %H:%M:%S")
    
    col_a = ws.col_values(1)
    next_row = len(col_a) + 1
    
    row = [ts, op_data.get("description", ""), op_data.get("currency", ""), "", "", "", "", ""]
    op_type = op_data.get("type", "").lower()
    if "поступление" in op_type or "взнос" in op_type or "возврат по пп" in op_type: row[3] = amt
    elif "конвертация" in op_type or "manual buy fx" in op_type or "internal exchange" in op_type: row[4] = amt
    elif "оплата пп" in op_type: row[5] = amt
    elif "выдача наличных" in op_type: row[6] = amt
    else: row[7] = amt
    
    ws.update(range_name=f"A{next_row}:H{next_row}", values=[row], value_input_option="USER_ENTERED")
