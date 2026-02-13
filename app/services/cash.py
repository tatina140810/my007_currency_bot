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
        
        cur.execute("""
            SELECT operation_type, currency, amount, description
            FROM operations
            WHERE date(timestamp) = date(?)
        """, (date_str,))
        
        rows = cur.fetchall()
    finally:
        conn.close()
    
    exchanges_list = []
    
    for row in rows:
        op_type = row["operation_type"]
        currency = row["currency"]
        amount = float(row["amount"])
        desc = row["description"] or ""
        
        # Фильтрация типов
        if op_type in ("Взнос наличными", "Поступление"):
            if currency in data:
                data[currency]["deposit"] += amount
                
        elif op_type in ("Выдача наличных", "Выдача"):
             # В базе Выдача обычно с минусом? 
             # add_operation(..., sign * amount) -> если amount > 0 в аргументах, то в базу пишется с минусом?
             # Смотрим operations.py:
             # sign = -1 if op_type in ("Выдача наличных", ...)
             # await queue_operation(..., sign * amount)
             # Значит в базе лежит отрицательное число.
             # Для отчета нам нужно абсолютное значение в колонку Withdrawals (или просто sum)
             if currency in data:
                data[currency]["withdraw"] += abs(amount)

        elif op_type == "Internal Exchange":
            # Тут сложнее. Обмен состоит из двух операций в БД?
            # Или мы будем хранить его как одну операцию?
            # Если мы используем add_operation, то это 2 строки: -100 USD, +9000 RUB.
            # Как понять что они связаны?
            # Обычно по timestamp или description.
            # "Internal exchange 100 USD to RUB rate 90" -> description="Internal Exchange to RUB rate 90" / "from USD"
            
            # Если мы хотим красивый список обменов (Sheet 2), нам нужно их связывать.
            # Пока просто агрегируем суммы.
            
            if amount < 0:
                if currency in data:
                    data[currency]["exchange_out"] += abs(amount)
            else:
                 if currency in data:
                    data[currency]["exchange_in"] += amount

            # Сохраняем для Sheet 2
            # (нужна логика связывания для Sheet 2, пока просто список)
            exchanges_list.append({
                "currency": currency,
                "amount": amount,
                "desc": desc
            })

    # 3. Closing Balance
    # Closing = Opening + Deposits + Exch(In) - Exch(Out) - Withdrawals
    # Note: Exch(Out) is positive number here representing overflow.
    # Actually Balance logic:
    # Balance = Opening + Deposits - Withdrawals + ExchTotal
    
    for cur, vals in data.items():
        # deposit is positive
        # withdraw is positive (magnitude)
        # exchange_in is positive
        # exchange_out is positive (magnitude)
        
        # closing = opening + deposit - withdraw + exchange_in - exchange_out
        vals["closing"] = vals["opening"] + vals["deposit"] - vals["withdraw"] + vals["exchange_in"] - vals["exchange_out"]

    return {
        "summary": data, 
        "exchanges": exchanges_list, 
        "date": date_str
    }
