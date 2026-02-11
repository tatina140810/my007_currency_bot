"""
Математические операции и конвертации
"""

def compute_conversion_to_amount(amount: float, rate: float, from_curr: str, to_curr: str) -> float:
    """Вычисляет сумму конвертации"""
    weak = {"RUB", "KGS", "KZT", "CNY"}
    strong = {"USD", "USDT", "EUR", "AED"}
    
    if rate <= 0:
        raise ValueError("Курс должен быть > 0")
    
    from_weak = from_curr in weak
    from_strong = from_curr in strong
    to_weak = to_curr in weak
    to_strong = to_curr in strong
    
    if from_strong and to_weak:
        return amount * rate
    if from_weak and to_strong:
        return amount / rate
    if from_weak and to_weak:
        return amount * rate
    if from_strong and to_strong:
        return amount * rate
    
    return amount * rate


def compute_fixed_payment_amount(buy_amount: float, rate: float) -> float:
    """Вычисление фиксированной суммы оплаты"""
    if rate <= 0:
        raise ValueError("Курс должен быть > 0")
    return buy_amount * rate

from collections import defaultdict

def aggregate_bulk_sum(items: list[dict]):
    """
    items: то, что возвращает parse_bulk_pp_payments
    Агрегируем: company (клиент) x currency -> сумма
    """
    agg = defaultdict(lambda: defaultdict(float))
    totals = defaultdict(float)

    for it in items:
        company = (it.get("company") or "").strip() or "Без клиента"
        cur = (it.get("currency") or "").strip().upper()
        amt = float(it.get("amount") or 0.0)

        agg[company][cur] += amt
        totals[cur] += amt

    return agg, totals

