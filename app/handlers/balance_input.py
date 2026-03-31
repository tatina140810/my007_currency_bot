"""
Handler for private balance input messages.

User sends in private chat to the bot:
  утро
  Рубли: 300000
  USD: 1785
  Евро: 40157
  CNY: 4471
  тенге: 7120429

or:
  вечер
  Рубли: 37200
  USD: 1483
  ...

Bot parses the values, updates the corresponding row in "отчет по остаткам" sheet
for today's date, then regenerates the Расчетный вечер / Разница formulas.
"""
import re
import asyncio
from datetime import date, datetime
from zoneinfo import ZoneInfo

from telegram import Update
from telegram.ext import ContextTypes

from app.core.logger import logger
from app.core.config import CASSA_SPREADSHEET_ID
from app.handlers.utils import is_staff

KG_TZ = ZoneInfo("Asia/Bishkek")

CURRENCY_MAP = {
    "рубли": "Рубли",
    "рублей": "Рубли",
    "рубл": "Рубли",
    "rub": "Рубли",
    "usd": "USD",
    "доллар": "USD",
    "евро": "Евро",
    "euro": "Евро",
    "eur": "Евро",
    "cny": "CNY",
    "юань": "CNY",
    "тенге": "тенге",
    "kzt": "тенге",
    "тг": "тенге",
}

REPORT_SHEET = "отчет по остаткам"
COL_MAP = {"Рубли": "B", "USD": "C", "Евро": "D", "CNY": "E", "тенге": "F"}

# Row offsets within a date block (relative to header row "Дата: DD.MM.YYYY"):
# 0 = "Дата: DD.MM.YYYY"
# 1 = currency header row
# 2 = Остаток Утро
# 3 = Входящие суммы
# 4 = Снятия
# 5 = Ком. за снятие
# 6 = Ком. за пополнение
# 7 = Конвертации
# 8 = Платежи
# 9 = Swift комиссия
# 10 = Расчетный вечер
# 11 = Фактический вечер
# 12 = Разница

ROW_OFFSET_MORNING = 2   # Остаток Утро
ROW_OFFSET_EVENING = 11  # Фактический вечер


def parse_balance_message(text: str):
    """
    Parse lines like:
        утро
        Рубли: 300000
        USD: 1785
        Евро: 40157
        CNY: 4471
        тенге: 7120429
    
    Returns: (period: 'morning'|'evening', values: dict[str, float], target_date: date)
    """
    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    
    period = None
    target_date = date.today()
    values = {}
    
    for line in lines:
        lower = line.lower()
        
        # Check for period keyword
        if any(kw in lower for kw in ["утро", "morning", "утренн"]):
            period = "morning"
            continue
        if any(kw in lower for kw in ["вечер", "evening", "вечерн"]):
            period = "evening"
            continue
        
        # Check for explicit date (dd.mm.yyyy)
        date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', line)
        if date_match:
            try:
                target_date = datetime.strptime(date_match.group(1), "%d.%m.%Y").date()
            except:
                pass
            continue
        
        # Parse currency: value lines
        # Formats: "Рубли: 300000" or "Рубли 300000" or "RUB: 300000"
        match = re.match(r'^([а-яёА-ЯЁa-zA-Z]+)[:\s]+([0-9\s,\.]+)$', line)
        if match:
            raw_curr = match.group(1).strip().lower()
            raw_val = match.group(2).strip().replace(" ", "").replace(",", ".")
            
            curr_key = CURRENCY_MAP.get(raw_curr)
            if curr_key:
                try:
                    values[curr_key] = float(raw_val)
                except ValueError:
                    pass
    
    return period, values, target_date


def _update_balance_in_sheet(period: str, values: dict, target_date: date):
    """Synchronous function to update morning or evening balance in the sheet."""
    import gspread
    import os
    
    # Project root (workspace). Avoid hardcoding /root/... which breaks on macOS.
    PROJECT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    CREDENTIALS_FILE = os.path.join(PROJECT_DIR, "n8n-google-credentials.json")
    
    from app.services.google_sheets import _apply_time_patch
    _apply_time_patch()
    
    gc = gspread.service_account(filename=CREDENTIALS_FILE)
    sh = gc.open_by_key(CASSA_SPREADSHEET_ID)
    
    try:
        ws = sh.worksheet(REPORT_SHEET)
    except Exception as e:
        logger.error(f"[BalanceHandler] Cannot open sheet '{REPORT_SHEET}': {e}")
        return False, f"Лист '{REPORT_SHEET}' не найден"
    
    d_str = target_date.strftime("%d.%m.%Y")
    header_marker = f"Дата: {d_str}"
    
    all_vals = ws.get_all_values()
    block_start = None
    for i, row in enumerate(all_vals):
        if row and row[0] and header_marker in str(row[0]):
            block_start = i + 1  # 1-indexed
            break
    
    if block_start is None:
        # Need to create the block first — run the write_cash_report script
        logger.warning(f"[BalanceHandler] No block found for {d_str}, creating...")
        import subprocess
        subprocess.run(
            ["./venv/bin/python", "scripts/write_cash_report.py", d_str],
            cwd=PROJECT_DIR
        )
        # Re-read
        all_vals = ws.get_all_values()
        for i, row in enumerate(all_vals):
            if row and row[0] and header_marker in str(row[0]):
                block_start = i + 1
                break
        if block_start is None:
            return False, f"Не удалось создать блок для {d_str}"
    
    row_offset = ROW_OFFSET_MORNING if period == "morning" else ROW_OFFSET_EVENING
    target_row = block_start + row_offset
    
    updated = []
    for curr, val in values.items():
        col_letter = COL_MAP.get(curr)
        if not col_letter:
            continue
        cell = f"{col_letter}{target_row}"
        ws.update(range_name=cell, values=[[val]])
        updated.append(f"{curr}: {val:,.2f}")
    
    period_name = "Утро" if period == "morning" else "Вечер"
    logger.info(f"[BalanceHandler] Updated {period_name} for {d_str}: {updated}")
    return True, period_name, updated, d_str


def looks_like_balance_message(text: str) -> bool:
    """Quick check if a private message looks like a balance report."""
    lower = text.lower()
    has_period = any(kw in lower for kw in ["утро", "вечер", "morning", "evening"])
    has_currency = any(kw in lower for kw in ["рубли", "usd", "евро", "cny", "тенге", "rub"])
    has_number = bool(re.search(r'\d{4,}', text))
    return has_period and has_currency and has_number


async def handle_private_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handler for private chat balance input.
    Registered in main.py in group=-1 (highest priority for private).
    """
    message = update.effective_message
    user = update.effective_user
    chat = update.effective_chat
    
    if not message or not user or not chat:
        return
    if chat.type != "private":
        return
    if user.is_bot:
        return
    if not is_staff(user.id):
        return
    
    text = (message.text or "").strip()
    if not text:
        return
    
    if not looks_like_balance_message(text):
        return  # Not a balance message, let other handlers process it
    
    logger.info(f"[BalanceHandler] Received balance msg from user {user.id}: {text[:100]}")
    
    period, values, target_date = parse_balance_message(text)
    
    if not period:
        await message.reply_text(
            "❓ Укажите период: начните с **утро** или **вечер**, затем укажите суммы.\n\n"
            "Пример:\n```\nутро\nРубли: 300000\nUSD: 1785\nЕвро: 40157\nCNY: 4471\nтенге: 7120429```",
            parse_mode="Markdown"
        )
        return
    
    if not values:
        await message.reply_text("❓ Суммы не распознаны. Формат: `Рубли: 300000`", parse_mode="Markdown")
        return
    
    # Run the sheet update in a thread
    result = await asyncio.to_thread(_update_balance_in_sheet, period, values, target_date)
    
    if not result[0]:
        await message.reply_text(f"❌ Ошибка: {result[1]}")
        return
    
    _, period_name, updated, d_str = result
    
    lines = "\n".join(f"  • {u}" for u in updated)
    await message.reply_text(
        f"✅ **Остаток {period_name} {d_str}** обновлён в отчёте:\n{lines}",
        parse_mode="Markdown"
    )
