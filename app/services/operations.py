import asyncio
import logging
from collections import defaultdict
from app.db.instance import db
from app.services.balance import invalidate_balance_cache
from app.services.parser import normalize_group_name

logger = logging.getLogger(__name__)

# Батчинг операций
operation_queue = defaultdict(list)
queue_lock = asyncio.Lock()

async def process_operation_batch():
    """Фоновая задача для обработки очереди операций"""
    global operation_queue
    while True:
        await asyncio.sleep(0.5)
        
        async with queue_lock:
            if not operation_queue:
                continue
            queue_snapshot = dict(operation_queue)

        for chat_id, operations in queue_snapshot.items():
            try:
                for op in operations:
                    db.add_operation(
                        chat_id,
                        op["type"],
                        op["currency"],
                        op["amount"],
                        op["description"],
                    )
                
                async with queue_lock:
                    # Удаляем только те, что обработали из snapshot
                    # (хотя тут просто чистим весь ключ, если он есть)
                    # operation_queue.pop(chat_id, None) 
                    # Лучше так:
                    if chat_id in operation_queue:
                         del operation_queue[chat_id]
                
                invalidate_balance_cache(chat_id)
                logger.info(f"Обработано {len(operations)} операций для чата {chat_id}")
            except Exception:
                logger.exception(f"Ошибка записи операций для чата {chat_id}")


async def queue_operation(
    chat_id: int, 
    op_type: str, 
    currency: str, 
    amount: float, 
    description: str = ""
):
    """Добавляет операцию в очередь"""
    async with queue_lock:
        operation_queue[chat_id].append({
            "type": op_type,
            "currency": currency,
            "amount": amount,
            "description": description,
        })


def resolve_target_chat_id(
    chat,
    is_private: bool,
    group_from_manual: str | None = None,
):
    """
    Определяет chat_id для записи операции.
    """
    # ЛИЧНЫЙ ЧАТ
    if is_private:
        if not group_from_manual:
            raise ValueError(
                "В личном чате нужно указать группу в квадратных скобках.\n"
                "Пример:\n[УЗ] поступили 5000 usdt"
            )

        target_chat_id = db.get_chat_id_by_name(group_from_manual)
        if not target_chat_id:
            raise ValueError(f"Группа '{group_from_manual}' не найдена")

        return target_chat_id

    # ГРУППОВОЙ ЧАТ
    return chat.id
