import logging
from typing import Dict, List, Any
from datetime import datetime
from collections import defaultdict

from app.db.instance import db
from app.core.config import CURRENCIES, OPERATION_TYPES

logger = logging.getLogger(__name__)

async def set_opening_balances(date_str: str, balances: Dict[str, float], group_id: int = 0):
    """Сохраняет начальные остатки"""
    for currency, amount in balances.items():
        db.set_cash_opening_balance(date_str, currency, amount, group_id)

def get_report_data(report_date, group_id: int = 0) -> Dict[str, Any]:
    """
    Собирает данные для отчета.
    """
    date_str = report_date.strftime("%Y-%m-%d")
    
    # 1. Начальный остаток
    opening = db.get_cash_opening_balances(date_str, group_id)
    if not opening:
        return None  # Signal that opening balance is missing
        
    # Нормализация
    data = {}
    for cur in CURRENCIES:
        data[cur] = {
            "opening": opening.get(cur, 0.0),
            "deposit": 0.0,
            "withdraw": 0.0,
            "exchange_in": 0.0,
            "exchange_out": 0.0,
            "closing": 0.0
        }

    # 2. Получаем операции за день
    conn = db.get_connection()
    try:
        cur = conn.cursor()
        
        # JOIN with chats to get group name
        cur.execute("""
            SELECT 
                o.operation_type, 
                o.currency, 
                o.amount, 
                o.description, 
                o.timestamp,
                c.chat_name
            FROM operations o
            LEFT JOIN chats c ON o.chat_id = c.chat_id
            WHERE date(o.timestamp) = date(?)
            ORDER BY o.timestamp ASC
        """, (date_str,))
        
        rows = cur.fetchall()
    finally:
        conn.close()
    
    exchanges_list = []
    all_operations = []
    
    for row in rows:
        op_type = row["operation_type"]
        currency = row["currency"]
        amount = float(row["amount"])
        desc = row["description"] or ""
        ts = row["timestamp"] # string or datetime? sqlite3.Row returns string usually unless parsed
        group_name = row["chat_name"] or "Unknown"
        
        # Format time
        try:
            # Timestamp format in DB is likely "YYYY-MM-DD HH:MM:SS"
            dt = datetime.strptime(str(ts), "%Y-%m-%d %H:%M:%S")
            time_str = dt.strftime("%H:%M")
        except:
            time_str = str(ts)

        # Collect for Details Sheet
        all_operations.append({
            "time": time_str,
            "group": group_name,
            "type": op_type,
            "currency": currency,
            "amount": amount,
            "desc": desc
        })

        # Фильтрация типов для Summary
        if op_type in ("Взнос наличными", "Поступление"):
            if currency in data:
                data[currency]["deposit"] += amount
                
        elif op_type in ("Выдача наличных", "Выдача"):
             if currency in data:
                data[currency]["withdraw"] += abs(amount)

        elif op_type == "Internal Exchange":
            if amount < 0:
                if currency in data:
                    data[currency]["exchange_out"] += abs(amount)
            else:
                 if currency in data:
                    data[currency]["exchange_in"] += amount

            # Заполняем exchanges_list отдельно, если нужно (или можно брать из all_operations)
            exchanges_list.append({
                "currency": currency,
                "amount": amount,
                "desc": desc,
                "time": time_str,
                "group": group_name
            })

    # 3. Closing Balance
    for cur, vals in data.items():
        vals["closing"] = vals["opening"] + vals["deposit"] - vals["withdraw"] + vals["exchange_in"] - vals["exchange_out"]

    return {
        "summary": data, 
        "exchanges": exchanges_list, # Kept for backward compatibility if needed, or use all_operations for details
        "all_operations": all_operations,
        "date": date_str
    }
