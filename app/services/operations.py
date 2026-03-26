import asyncio
import logging
from collections import defaultdict
from app.db.instance import db
from app.services.balance import invalidate_balance_cache
from app.services.n8n import send_to_n8n
from app.services.google_sheets import append_operation_to_sheet, sync_all_balances_to_sheet, append_client_operation_to_sheet
from app.services.parser import normalize_group_name

logger = logging.getLogger(__name__)

# Батчинг операций
operation_queue = defaultdict(list)
queue_lock = asyncio.Lock()

# Background tasks references to prevent garbage collection
_bg_tasks = set()

def _fire_and_forget(coro):
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)

async def process_operation_batch():
    """Фоновая задача для обработки очереди операций"""
    global operation_queue
    while True:
        await asyncio.sleep(0.5)
        
        # Check for maintenance mode
        if getattr(db, "maintenance_mode", False):
            # If in maintenance mode, wait and retry
            await asyncio.sleep(1.0)
            continue
        
        async with queue_lock:
            if not operation_queue:
                continue
            queue_snapshot = dict(operation_queue)

        for chat_id, operations in queue_snapshot.items():
            try:
                for op in operations:
                    # DEDUPLICATION CHECK for Bank Income
                    if op["type"] == "Поступление":
                        if db.is_duplicate_operation(
                            chat_id, 
                            op["amount"], 
                            op["currency"], 
                            op["description"]
                        ):
                            logger.warning(f"Duplicate income skipped: {op['amount']} {op['currency']} in chat {chat_id}")
                            continue

                    db.add_operation(
                        chat_id,
                        op["type"],
                        op["currency"],
                        op["amount"],
                        op["description"],
                        timestamp=op.get("timestamp")
                    )
                    
                    # Offload to n8n webhook asynchronously
                    _fire_and_forget(send_to_n8n({
                        "chat_id": chat_id,
                        "type": op["type"],
                        "currency": op["currency"],
                        "amount": op["amount"],
                        "description": op["description"],
                        "timestamp": op.get("timestamp").isoformat() if op.get("timestamp") else None
                    }))
                    
                    # Fetching the chat name safely to pass to Google Sheets
                    chat_name = f"Chat_{chat_id}"
                    chat_info = db.get_chat(chat_id)
                    if chat_info and chat_info[1]:
                        chat_name = chat_info[1]
                        
                    # Find balance
                    current_balance = db.get_balance(chat_id, op["currency"])
                    
                    # Offload to Google Sheets asynchronously (Internal History Sheet)
                    _fire_and_forget(append_operation_to_sheet({
                        "id": "",
                        "chat_id": chat_id,
                        "type": op["type"],
                        "currency": op["currency"],
                        "amount": op["amount"],
                        "description": op["description"],
                        "timestamp": op.get("timestamp").isoformat() if op.get("timestamp") else None
                    }))
                    
                    # Offload to NEW Client Google Sheet asynchronously
                    _fire_and_forget(append_client_operation_to_sheet(
                        op_data={
                            "chat_id": chat_id,
                            "chat_name": chat_name,
                            "type": op["type"],
                            "currency": op["currency"],
                            "amount": op["amount"],
                            "description": op["description"],
                            "timestamp": op.get("timestamp")
                        },
                        current_balance=current_balance
                    ))
                    
                    # Prevent Google Sheets API Rate-Limit (429) on bulk inserts
                    await asyncio.sleep(1.5)
                
                invalidate_balance_cache(chat_id)
                logger.info(f"Обработано {len(operations)} операций для чата {chat_id}")
            except Exception:
                logger.exception(f"Ошибка записи операций для чата {chat_id}")
            finally:
                async with queue_lock:
                    if chat_id in operation_queue:
                         del operation_queue[chat_id]
        
        # Sync group balances to Google Sheets ONCE after the entire batch is done
        if queue_snapshot:
            _fire_and_forget(sync_all_balances_to_sheet())


async def queue_operation(
    chat_id: int, 
    op_type: str, 
    currency: str, 
    amount: float, 
    description: str = "",
    timestamp=None
):
    """Добавляет операцию в очередь"""
    async with queue_lock:
        operation_queue[chat_id].append({
            "type": op_type,
            "currency": currency,
            "amount": amount,
            "description": description,
            "timestamp": timestamp
        })


def resolve_target_chat_id(
    chat,
    is_private: bool,
    group_from_manual: str | None = None,
):
    """
    Определяет chat_id для записи операции.
    Если группа указана - ищем ее.
    Если не указана, но это личный чат - пишем прямо в личный чат (General LEDGER).
    """
    if is_private:
        if group_from_manual:
            target_chat_id = db.get_chat_id_by_name(group_from_manual)
            if not target_chat_id:
                # АВТО-СОЗДАНИЕ ВИРТУАЛЬНОЙ ГРУППЫ
                target_chat_id = db.create_virtual_group(group_from_manual)
                logger.info(f"Создана новая виртуальная группа: '{group_from_manual}' с ID {target_chat_id}")
            return target_chat_id
        else:
            # Разрешаем запись без группы в личку
            return chat.id

    # ГРУППОВОЙ ЧАТ
    return chat.id
