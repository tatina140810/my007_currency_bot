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

_gsheets_lock = asyncio.Lock()

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

async def sync_conversions_to_cassa_sheet(conversions: list):
    """Async wrapper for conversions sync."""
    if not conversions: return
    try:
        await asyncio.to_thread(_sync_conversions_to_cassa_thread, conversions)
    except Exception as e:
        logger.error(f"[GoogleSheets] Conversions sync failed: {e}")

async def sync_payment_list_to_cassa_sheet(parsed_data: dict):
    """Async wrapper for /back_report styled payment list syncing into Cassa."""
    if not parsed_data or not parsed_data.get("items"): return
    try:
        await asyncio.to_thread(_execute_with_retry, _sync_payment_list_thread, parsed_data)
    except Exception as e:
        logger.error(f"[GoogleSheets] Payment list sync failed: {e}")

async def append_operation_to_sheet(op_data: dict):
    """Async wrapper for internal history sync."""
    async with _gsheets_lock:
        try:
            await asyncio.to_thread(_execute_with_retry, _append_operation_sync_logic, op_data)
        except Exception as e:
            logger.error(f"[GoogleSheets] Internal history sync failed: {e}")

async def sync_all_balances_to_sheet():
    """Async wrapper for balance matrix sync."""
    async with _gsheets_lock:
        try:
            await asyncio.to_thread(_execute_with_retry, _sync_balances_sync_logic)
        except Exception as e:
            logger.error(f"[GoogleSheets] Balance matrix sync failed: {e}")

async def sync_daily_income(report_date_str: str, rows_data: list):
    """Async wrapper for daily income sync."""
    async with _gsheets_lock:
        try:
            await asyncio.to_thread(_execute_with_retry, _sync_daily_income_sync_logic, report_date_str, rows_data)
        except Exception as e:
            logger.error(f"[GoogleSheets] Daily income sync failed: {e}")

async def append_client_operation_to_sheet(op_data: dict, current_balance: float):
    """Async wrapper for client-facing sheet sync."""
    from app.core.constants import KG_TZ
    start_date = datetime.datetime(2026, 3, 11, 6, 0, 0, tzinfo=KG_TZ)
    if datetime.datetime.now(KG_TZ) < start_date:
        return

    async with _gsheets_lock:
        try:
            await asyncio.to_thread(_execute_with_retry, _append_client_operation_sync_logic, op_data, current_balance)
        except Exception as e:
            logger.error(f"[GoogleSheets] Client sheet sync failed: {e}")

# --- Internal Synchronous Logic (Wrapped in threads/retries) ---

def _sync_conversions_to_cassa_thread(conversions: list):
    from app.core.config import CASSA_SPREADSHEET_ID
    from app.core.constants import KG_TZ
    
    def logic():
        gc = _get_gc()
        sh = gc.open_by_key(CASSA_SPREADSHEET_ID)
        worksheet_name = "конветации"
        try:
            ws = sh.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=worksheet_name, rows=1000, cols=10)
            ws.append_row(["Дата", "Клиент", "Сумма", "Валюта", "Курс", "Сумма РУБ", "Оригинал"])
            ws.freeze(rows=1)
            
        now_str = datetime.datetime.now(KG_TZ).strftime("%d.%m.%Y")
        
        existing_records = ws.get_all_values()
        next_row_index = len(existing_records) + 1
        
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
                c.get("original_text", "")
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
        if len(current_headers) < 6 or "Сумма" not in current_headers:
            ws.update(range_name="A1:F1", values=[[
                "Отчет Back", "Компания", "Тип", 
                "Контрагент", "Валюта платежа", "Сумма"
            ]])
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows=1000, cols=6)
        ws.append_row([
            "Отчет Back", "Компания", "Тип", 
            "Контрагент", "Валюта платежа", "Сумма"
        ])
        ws.freeze(rows=1)

    date_str = parsed_data.get("date", "Unknown Date")
    items = parsed_data.get("items", [])
    
    # 1. Сначала добавляем строку-разделитель с датой
    ws.append_row([f"--- ПЛАТЕЖИ ЗА {date_str} ---"])
    
    # 2. Формируем строки платежей
    rows = []
    for item in items:
        rows.append([
            item.get("bank", ""),         # Отчет Back
            item.get("company", ""),      # Компания
            item.get("type", ""),         # Тип
            item.get("counterparty", ""), # Контрагент
            item.get("currency", ""),     # Валюта платежа
            item.get("sum", 0.0)          # Сумма
        ])
        
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
    sh = gc.open_by_key(CLIENT_SPREADSHEET_ID)
    chat_name = op_data.get("chat_name", f"Chat_{op_data.get('chat_id')}")
    safe_name = chat_name[:31]
    for c in r'\/:*?[]': safe_name = safe_name.replace(c, "_")

    try:
        ws = sh.worksheet(safe_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=safe_name, rows=1000, cols=10)
        ws.append_row(["Дата/Время", "Описание", "Валюта", "Поступления", "Конвертация", "Оплаты ПП", "Выдача наличных", "Доп. расходы", "Общий баланс"])

    amt = float(op_data.get("amount", 0.0))
    ts = op_data.get("timestamp", "")
    if hasattr(ts, "strftime"): ts = ts.strftime("%d.%m.%Y %H:%M")
    
    row = [ts, op_data.get("description", ""), op_data.get("currency", ""), "", "", "", "", "", current_balance]
    op_type = op_data.get("type", "").lower()
    if "поступление" in op_type or "взнос" in op_type or "возврат по пп" in op_type: row[3] = amt
    elif "конвертация" in op_type or "manual buy fx" in op_type or "internal exchange" in op_type: row[4] = amt
    elif "оплата пп" in op_type: row[5] = amt
    elif "выдача наличных" in op_type: row[6] = amt
    else: row[7] = amt
    
    ws.append_row(row)
