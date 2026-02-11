import time
from typing import Dict
from datetime import datetime
from app.db.instance import db

# Кеширование балансов
balance_cache: Dict[int, Dict[str, float]] = {}
balance_cache_time: Dict[int, float] = {}
CACHE_TTL = 5

def get_cached_balance(chat_id: int):
    """Получает баланс с кешированием"""
    now = datetime.now().timestamp()
    if chat_id in balance_cache:
        if now - balance_cache_time.get(chat_id, 0) < CACHE_TTL:
            return balance_cache[chat_id]
    
    balances = db.get_balances(chat_id)
    balance_cache[chat_id] = balances
    balance_cache_time[chat_id] = now
    return balances

def invalidate_balance_cache(chat_id: int):
    """Инвалидирует кеш баланса"""
    balance_cache.pop(chat_id, None)
    balance_cache_time.pop(chat_id, None)
