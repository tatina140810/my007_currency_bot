from telegram import Update
from app.core.logger import logger

def get_chat_id(update: Update) -> int:
    """Получает ID чата"""
    return update.effective_chat.id

def get_chat_name(update: Update) -> str:
    """Получает название чата"""
    chat = update.effective_chat
    if chat.type == "private":
        return f"Личный чат с {update.effective_user.first_name}"
    return chat.title or f"Группа {chat.id}"

def is_staff(user_id: int | None) -> bool:
    """Проверяет является ли пользователь сотрудником"""
    from app.core.constants import TEAM_MEMBER_IDS
    return user_id is not None and user_id in TEAM_MEMBER_IDS

async def safe_reply(message, text: str, **kwargs):
    """
    Отправляет ответ только в системную группу или в личный чат.
    Блокирует отправку автоматических уведомлений в клиентские/рабочие группы.
    """
    chat = message.chat
    # Позволяем отправку в личку
    if chat.type == 'private':
        try:
            await message.reply_text(text, **kwargs)
        except Exception as e:
            logger.error(f"Error in safe_reply (private): {e}")
        return

    # Позволяем отправку в системную группу
    from app.core.config import ADMIN_ALERT_CHAT_ID
    try:
        admin_id = int(ADMIN_ALERT_CHAT_ID)
    except:
        admin_id = 0
        
    if chat.id == admin_id:
        try:
            await message.reply_text(text, **kwargs)
        except Exception as e:
            logger.error(f"Error in safe_reply (system group): {e}")
        return

    # Иначе глушим уведомление
    logger.info(f"🔇 Muted auto-reply for group '{chat.title}': {text[:40]}...")
