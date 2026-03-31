"""
Daily Balance Report Engine
Implements the 11-point balance calculation spec:

BALANCE = OPENING_BALANCE + INCOME - EXPENSES ± INTERNAL_TRANSFERS

Income types: Поступление, Взнос наличными, Возврат по ПП
Expense types: Выдача наличных, Оплата ПП, Комиссия*, SWIFT
Internal transfer types: Конвертация (debit one currency, credit another)

Source of truth: operations table (unique by id, timestamp-based).
Output: grouped by (chat_id, currency) written to Google Sheet.
"""
import os, sys, sqlite3, time, logging
from datetime import date, datetime
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
from zoneinfo import ZoneInfo

sys.path.insert(0, "/root/my007_currency_bot")

from app.core.config import CASSA_SPREADSHEET_ID
from app.services.google_sheets import _apply_time_patch
import gspread

logger = logging.getLogger(__name__)

KG_TZ = ZoneInfo("Asia/Bishkek")
PROJECT_DIR = "/root/my007_currency_bot"
CREDENTIALS_FILE = os.path.join(PROJECT_DIR, "n8n-google-credentials.json")
DB_PATH = os.path.join(PROJECT_DIR, "operations.db")
REPORT_SHEET = "отчет по остаткам"

# ── Operation type classification ────────────────────────────────────────────
INCOME_TYPES = {
    "Поступление",
    "Взнос наличными",
    "Возврат по ПП",
    "Поступление (авто)",
}

EXPENSE_TYPES = {
    "Выдача наличных",
    "Оплата ПП",
    "Комиссия",
    "Комиссия 1%",
    "Комиссия 0.5%",
    "SWIFT",
    "Запрос банку",   # bank fee/request
}

TRANSFER_TYPES = {
    "Конвертация",
    "Перевод",
}


def classify(op_type: str) -> str:
    """Return 'income', 'expense', 'transfer', or 'unknown'."""
    t = op_type.strip()
    if t in INCOME_TYPES:
        return "income"
    if t in EXPENSE_TYPES:
        return "expense"
    if t in TRANSFER_TYPES:
        return "transfer"
    # Fuzzy: if name contains known keywords
    lower = t.lower()
    if any(k in lower for k in ["поступ", "взнос", "возврат"]):
        return "income"
    if any(k in lower for k in ["выдач", "оплат", "комисс", "swift", "запрос"]):
        return "expense"
    if any(k in lower for k in ["конверт", "перевод", "обмен"]):
        return "transfer"
    return "unknown"


def get_operations_for_date(
    target_date: date,
    db_path: str = DB_PATH
) -> List[dict]:
    """
    Fetch all unique operations for target_date from SQLite.
    Uses timestamp DATE (operation date), not processing date.
    Returns list of dicts. Deduplication: unique by id.
    """
    date_str = target_date.strftime("%Y-%m-%d")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""
        SELECT DISTINCT
            o.id,
            o.chat_id,
            o.operation_type,
            o.currency,
            o.amount,
            o.description,
            o.timestamp,
            ch.chat_name
        FROM operations o
        LEFT JOIN chats ch ON ch.chat_id = o.chat_id
        WHERE DATE(o.timestamp) = ?
        ORDER BY o.timestamp ASC
    """, (date_str,))
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


def get_opening_balances(
    target_date: date,
    db_path: str = DB_PATH
) -> Dict[Tuple[int, str], float]:
    """
    Get opening balances from cash_opening_balances table, or compute from
    the cumulative sum of all operations before target_date.
    Returns dict keyed by (group_id, currency) -> opening_amount
    """
    date_str = target_date.strftime("%d.%m.%Y")
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    # Try explicit opening balances first
    c.execute("""
        SELECT group_id, currency, amount
        FROM cash_opening_balances
        WHERE date = ?
    """, (date_str,))
    rows = c.fetchall()
    
    opening = {}
    if rows:
        for r in rows:
            opening[(r["group_id"], r["currency"])] = float(r["amount"])
    
    if not opening:
        # Fallback: compute from cumulative operations before this date
        day_start = target_date.strftime("%Y-%m-%d")
        c.execute("""
            SELECT
                chat_id,
                currency,
                SUM(CASE
                    WHEN operation_type IN ('Поступление','Взнос наличными','Возврат по ПП','Поступление (авто)') THEN amount
                    WHEN operation_type IN ('Выдача наличных','Оплата ПП','Комиссия','Комиссия 1%','Комиссия 0.5%','SWIFT','Запрос банку') THEN -amount
                    ELSE 0
                END) AS net
            FROM operations
            WHERE DATE(timestamp) < ?
            GROUP BY chat_id, currency
        """, (day_start,))
        for r in c.fetchall():
            opening[(r["chat_id"], r["currency"])] = float(r["net"] or 0)
    
    conn.close()
    return opening


def compute_report(target_date: date) -> Dict:
    """
    Core report computation.
    Returns a dict: {
        (chat_id, currency): {
            chat_name, currency, opening, income, expense,
            transfer_in, transfer_out, closing, operations: [...]
        }
    }
    Also returns system_totals: {currency: closing_balance}.
    """
    operations = get_operations_for_date(target_date)
    opening_bal = get_opening_balances(target_date)
    
    # Accumulate per (chat_id, currency)
    acc = defaultdict(lambda: {
        "income": 0.0, "expense": 0.0,
        "transfer_in": 0.0, "transfer_out": 0.0,
        "ops": []
    })
    
    chat_names = {}
    
    for op in operations:
        chat_id = op["chat_id"]
        currency = op["currency"] or "?"
        amount = float(op["amount"] or 0)
        op_type = op["operation_type"] or ""
        kind = classify(op_type)
        
        chat_names[chat_id] = op.get("chat_name") or str(chat_id)
        key = (chat_id, currency)
        
        acc[key]["ops"].append(op)
        
        if kind == "income":
            acc[key]["income"] += amount
        elif kind == "expense":
            acc[key]["expense"] += amount
        elif kind == "transfer":
            # Transfers: amount is negative (outgoing) → expense side
            # If amount > 0 → incoming side (transfer_in)
            # If amount < 0 → outgoing side (transfer_out)
            if amount >= 0:
                acc[key]["transfer_in"] += amount
            else:
                acc[key]["transfer_out"] += abs(amount)
        # unknown: skip or treat as unknown
    
    # Build final report rows
    report = {}
    for (chat_id, currency), data in sorted(acc.items()):
        opening = opening_bal.get((chat_id, currency), 0.0)
        income = data["income"]
        expense = data["expense"]
        transfer_in = data["transfer_in"]
        transfer_out = data["transfer_out"]
        closing = opening + income - expense + transfer_in - transfer_out
        
        # Running total (sorted by timestamp)
        running = []
        bal = opening
        for op in sorted(data["ops"], key=lambda x: x["timestamp"]):
            kind = classify(op["operation_type"])
            amt = float(op["amount"] or 0)
            if kind == "income":
                bal += amt
            elif kind == "expense":
                bal -= amt
            elif kind == "transfer":
                if amt >= 0:
                    bal += amt
                else:
                    bal -= abs(amt)
            running.append((op["timestamp"], op["operation_type"], amt, round(bal, 2)))
        
        report[(chat_id, currency)] = {
            "chat_id": chat_id,
            "chat_name": chat_names.get(chat_id, str(chat_id)),
            "currency": currency,
            "opening": round(opening, 2),
            "income": round(income, 2),
            "expense": round(expense, 2),
            "transfer_in": round(transfer_in, 2),
            "transfer_out": round(transfer_out, 2),
            "closing": round(closing, 2),
            "ops_count": len(data["ops"]),
            "running": running,
        }
    
    # System totals by currency (for cross-check)
    totals = defaultdict(float)
    for (_, currency), row in report.items():
        totals[currency] += row["closing"]
    
    return report, dict(totals)


def write_report_to_sheet(report: dict, totals: dict, target_date: date):
    """Write the computed report to the Google Sheet."""
    _apply_time_patch()
    gc = gspread.service_account(filename=CREDENTIALS_FILE)
    sh = gc.open_by_key(CASSA_SPREADSHEET_ID)
    
    # Write to a dedicated tab "Баланс по операциям"
    BALANCE_SHEET = "Баланс по операциям"
    try:
        ws = sh.worksheet(BALANCE_SHEET)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=BALANCE_SHEET, rows=2000, cols=12)
    
    ws.clear()
    time.sleep(1)
    
    d_str = target_date.strftime("%d.%m.%Y")
    
    rows = []
    rows.append([f"ОТЧЁТ ПО ОСТАТКАМ — {d_str}", "", "", "", "", "", "", ""])
    rows.append([""])
    rows.append([
        "Группа / Счёт", "Валюта",
        "Начальный остаток", "Приход (+)", "Расход (-)",
        "Перемещения +", "Перемещения -",
        "Итоговый остаток", "Кол-во операций"
    ])
    
    # Sort by chat_name then currency
    for (chat_id, currency), row in sorted(report.items(), key=lambda x: (x[1]["chat_name"], x[1]["currency"])):
        rows.append([
            row["chat_name"],
            currency,
            row["opening"],
            row["income"],
            row["expense"],
            row["transfer_in"],
            row["transfer_out"],
            row["closing"],
            row["ops_count"],
        ])
    
    rows.append([""])
    rows.append(["ИТОГО ПО ВАЛЮТАМ", "", "", "", "", "", "", "", ""])
    rows.append(["Валюта", "Итоговый остаток"])
    for curr, total in sorted(totals.items()):
        rows.append([curr, round(total, 2)])
    
    rows.append([""])
    rows.append([""])
    rows.append([f"📋 ДЕТАЛИЗАЦИЯ ОПЕРАЦИЙ — {d_str}", "", "", "", "", "", ""])
    rows.append(["Время", "Группа", "Тип операции", "Валюта", "Сумма", "Нарастающий остаток", "Описание"])
    
    # Per-account running totals
    for (chat_id, currency), row in sorted(report.items(), key=lambda x: (x[1]["chat_name"], x[1]["currency"])):
        rows.append([f"── {row['chat_name']} / {currency} ──", "", "", "", "", f"Начало: {row['opening']:,.2f}", ""])
        for ts, op_type, amt, running_bal in row["running"]:
            rows.append([
                ts[:16] if ts else "",
                row["chat_name"],
                op_type,
                currency,
                amt,
                f"{running_bal:,.2f}",
                "",
            ])
    
    ws.update(range_name="A1", values=rows, value_input_option="USER_ENTERED")
    print(f"✅ Report written to '{BALANCE_SHEET}' — {len(rows)} rows")
    return ws


def validate_report(report: dict):
    """Validate: closing = opening + income - expense ± transfers. No duplicates."""
    errors = []
    seen_ids = set()
    
    for (chat_id, currency), row in report.items():
        expected = (row["opening"] + row["income"] - row["expense"]
                    + row["transfer_in"] - row["transfer_out"])
        if abs(expected - row["closing"]) > 0.01:
            errors.append(f"BALANCE MISMATCH {chat_id}/{currency}: "
                         f"expected {expected:.2f} got {row['closing']:.2f}")
    
    return errors


def run_daily_report(target_date: date = None, write_to_sheet: bool = True) -> str:
    """Main entry point."""
    if target_date is None:
        target_date = datetime.now(KG_TZ).date()
    
    d_str = target_date.strftime("%d.%m.%Y")
    print(f"\n=== DAILY BALANCE REPORT: {d_str} ===\n")
    
    report, totals = compute_report(target_date)
    
    print(f"Groups computed: {len(report)}")
    print(f"\n{'Группа':<30} {'Валюта':<8} {'Начало':>14} {'Приход':>14} {'Расход':>14} {'Итог':>14}")
    print("-" * 100)
    
    for (chat_id, currency), row in sorted(report.items(), key=lambda x: (x[1]["chat_name"], x[1]["currency"])):
        print(f"{row['chat_name'][:28]:<30} {currency:<8} "
              f"{row['opening']:>14,.2f} {row['income']:>14,.2f} "
              f"{row['expense']:>14,.2f} {row['closing']:>14,.2f}")
    
    print("\n── TOTALS BY CURRENCY ──")
    for curr, total in sorted(totals.items()):
        print(f"  {curr}: {total:,.2f}")
    
    errors = validate_report(report)
    if errors:
        print("\n⚠️ VALIDATION ERRORS:")
        for e in errors:
            print(" ", e)
    else:
        print("\n✅ Validation passed: all balances match operations sum")
    
    if write_to_sheet:
        ws = write_report_to_sheet(report, totals, target_date)
        sheet_url = f"https://docs.google.com/spreadsheets/d/{CASSA_SPREADSHEET_ID}"
        print(f"\n📊 Sheet updated: {sheet_url}")
    
    return f"Report for {d_str}: {len(report)} account/currency pairs, errors={len(errors)}"


if __name__ == "__main__":
    from datetime import date
    import sys
    if len(sys.argv) > 1:
        try:
            d = datetime.strptime(sys.argv[1], "%d.%m.%Y").date()
        except:
            print(f"Bad date: {sys.argv[1]}, use DD.MM.YYYY")
            sys.exit(1)
    else:
        d = datetime.now(KG_TZ).date()
    
    run_daily_report(d)
