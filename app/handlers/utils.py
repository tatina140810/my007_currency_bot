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
