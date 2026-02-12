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

async def get_report_data(report_date, group_id: int = 0) -> Dict[str, Any]:
    """
    Собирает данные для отчета:
    - Начальный остаток
    - Приходы (Взнос наличными)
    - Расходы (Выдача наличных)
    - Обмены (Внутренние)
    - Итоговый остаток
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
    # Нам нужны операции по ВСЕМ чатам? Или только те, что помечены как Cash?
    # "Only include cash-related operations... type='cash_deposit', type='cash_exchange', type='cash_withdraw'"
    
    # Сейчас у нас есть типы: "Взнос наличными", "Выдача наличных"
    # "Cash exchanges" - новый тип, который мы будем создавать.
    
    # Мы должны пройтись по operation_types и маппить их.
    # В базе операции привязаны к chat_id. 
    # Если отчет "общий", то нужно собирать со всех чатов? 
    # Или operations table имеет group_id? Нет, operations имеет chat_id.
    # Но cash_opening_balances имеет group_id.
    
    # Assumption: The report collects CASH operations from ALL chats (or specific logic).
    # "Cash deposits (/rep operations related to cash only)" -> implies using existing operations.
    
    # Давайте соберем операции со всех чатов за дату
    # Для этого нужен метод в db, который ищет по дату по всем чатам?
    # db.get_operations_by_date принимает chat_id.
    # Нам нужно get_all_operations_by_date(date).
    
    # Add helper in services/cash.py for now to iterate all chats, or add method to DB.
    # Adding method to DB is better performance-wise, but iterating is safer for now without touching DB schema too much.
    # However, iterating 100 chats is slow.
    # Let's use SQL query to fetch globally.
    
    # But wait, `db.get_operations_by_date` filters by chat_id.
    # I should add `get_all_operations_by_date` to Database or reuse connection here.
    
    conn = db.get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT operation_type, currency, amount, description
        FROM operations
        WHERE date(timestamp) = date(?)
    """, (date_str,))
    
    rows = cur.fetchall()
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
