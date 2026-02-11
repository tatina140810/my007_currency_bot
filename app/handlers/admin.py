from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes

from app.core.logger import logger
from app.core.config import ADMIN_PASSWORD
from app.core.constants import KG_TZ
from app.db.instance import db
from app.handlers.utils import get_chat_id, get_chat_name, is_staff
from app.services.parser import parse_timestamp
from app.services.balance import invalidate_balance_cache, balance_cache, balance_cache_time

async def undo_last_operation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /del"""
    user = update.effective_user or (update.callback_query and update.callback_query.from_user)
    if not is_staff(user.id):
        if update.callback_query:
            await update.callback_query.answer("Только для сотрудников", show_alert=True)
        else:
            await update.message.reply_text("Удалять операции могут только сотрудники.", parse_mode=None)
        return

    chat_id = get_chat_id(update)
    chat_name = get_chat_name(update)
    logger.info(f"Запрос удаления операции для чата {chat_id}")

    all_ops = db.get_operations(chat_id, limit=1000)
    today_date = datetime.now(KG_TZ).date()
    todays_ops = []
    for op in all_ops:
        op_id, op_type, currency, amount, description, timestamp = op
        op_dt = parse_timestamp(timestamp)
        if op_dt.date() == today_date:
            todays_ops.append(op)

    if not todays_ops:
        text = f"За сегодня операций нет\n{chat_name}"
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(text, parse_mode=None)
        else:
            await update.message.reply_text(text, parse_mode=None)
        return

    todays_ops.sort(key=lambda o: parse_timestamp(o[5]))
    text_lines = [f"УДАЛЕНИЕ ОПЕРАЦИИ\n{chat_name}\n"]
    keyboard = []

    for op in todays_ops:
        op_id, op_type, currency, amount, description, timestamp = op
        sign = "+" if amount > 0 else ""
        ts_str = parse_timestamp(timestamp).strftime("%H:%M:%S")
        text_lines.append(f"{op_type}\n   {currency}: {sign}{amount:,.2f}\n   {ts_str}\n")
        btn_text = f"{ts_str} {currency} {sign}{amount:,.2f}"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"undo_select_{op_id}")])

    keyboard.append([InlineKeyboardButton("Отмена", callback_data="cancel_undo")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    full_text = "\n".join(text_lines)

    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(full_text, reply_markup=reply_markup, parse_mode=None)
    else:
        await update.message.reply_text(full_text, reply_markup=reply_markup, parse_mode=None)


async def undo_select_operation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Callback для выбора операции на удаление"""
    user = update.effective_user or update.callback_query.from_user
    if not is_staff(user.id):
        await update.callback_query.answer("Нет прав", show_alert=True)
        return

    query = update.callback_query
    await query.answer()
    chat_id = get_chat_id(update)
    op_id = int(query.data.replace("undo_select_", ""))
    logger.info(f"Выбрана операция {op_id} для удаления в чате {chat_id}")

    operations = db.get_operations(chat_id, limit=10000)

    op_info = None
    for op in operations:
        if op[0] == op_id:
            op_info = op
            break

    if not op_info:
        await query.message.reply_text("Операция не найдена", parse_mode=None)
        return

    op_id, op_type, currency, amount, description, timestamp = op_info
    sign = "+" if amount > 0 else ""
    ts_str = parse_timestamp(timestamp).strftime("%d.%m.%Y %H:%M:%S")

    text = f"Удаление операции\n\n{op_type}\nВалюта: {currency}\nСумма: {sign}{amount:,.2f}\nДата: {ts_str}\n"
    if description:
        text += f"Описание: {description}\n"
    text += "\nВведите пароль для удаления.\nИли /cancel для отмены."

    context.user_data["pending_undo_op_id"] = op_id
    context.user_data["pending_undo_chat_id"] = chat_id
    await query.message.reply_text(text, parse_mode=None)


async def handle_delete_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка пароля для удаления"""
    user = update.effective_user
    if not is_staff(user.id):
        return
    if "pending_undo_op_id" not in context.user_data:
        # Пароль может быть воспринят как обычный текст, если мы не ждем пароля
        # Поэтому здесь просто return, и пусть operations handler разбирается (хотя в main мы настроим group=0 для этого)
        return

    chat_id = context.user_data.get("pending_undo_chat_id", get_chat_id(update))
    op_id = context.user_data["pending_undo_op_id"]
    entered_password = update.message.text.strip()

    if entered_password != ADMIN_PASSWORD:
        await update.message.reply_text("Неверный пароль. Операция не удалена.", parse_mode=None)
        return

    logger.info(f"Пароль верный, удаляем операцию {op_id}")
    operations = db.get_operations(chat_id, limit=1000)
    op_info = None
    for op in operations:
        if op[0] == op_id:
            op_info = op
            break

    if not op_info:
        await update.message.reply_text("Операция не найдена.", parse_mode=None)
        context.user_data.pop("pending_undo_op_id", None)
        context.user_data.pop("pending_undo_chat_id", None)
        return

    op_id, op_type, currency, amount, description, timestamp = op_info
    success = db.delete_operation(chat_id, op_id)
    context.user_data.pop("pending_undo_op_id", None)
    context.user_data.pop("pending_undo_chat_id", None)

    if not success:
        await update.message.reply_text("Ошибка при удалении.", parse_mode=None)
        return
    
    # Инвалидируем баланс
    invalidate_balance_cache(chat_id)

    sign = "+" if amount > 0 else ""
    ts_str = parse_timestamp(timestamp).strftime("%d.%m.%Y %H:%M:%S")
    text = f"Операция удалена\n\n{op_type}\nВалюта: {currency}\nСумма: {sign}{amount:,.2f}\nДата: {ts_str}\n"
    if description:
        text += f"Описание: {description}\n"
    await update.message.reply_text(text, parse_mode=None)


async def cancel_undo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отмена удаления (callback)"""
    query = update.callback_query
    await query.answer()
    context.user_data.pop("pending_undo_op_id", None)
    context.user_data.pop("pending_undo_chat_id", None)
    await query.edit_message_text("Отменено", parse_mode=None)


async def cmd_chats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /chats - показывает все чаты"""
    user = update.effective_user

    if not is_staff(user.id):
        return

    chats = db.get_all_chats()
    logger.info(f"/chats raw data: {chats}")

    if not chats:
        await update.message.reply_text("Группы не найдены.")
        return

    lines = ["📋 Чаты в базе:"]

    for row in chats:
        chat_id = row[0]
        try:
            chat = await context.bot.get_chat(chat_id)
            title = chat.title or chat.username or f"ID {chat_id}"
            lines.append(f"• {title}")
        except Exception:
            lines.append(f"• ID {chat_id} (недоступен)")

    await update.message.reply_text("\n".join(lines), parse_mode=None)

async def cmd_clear_all(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Очистка базы (ТОЛЬКО STAFF + ЛИЧКА).
    Вынесено из handle_text, будет регистрироваться как команда /clear_all 
    или обрабатываться в текстовом хендлере если команда с пробелом.
    В данном случае реализуем как хендлер, который можно вызывать.
    """
    user = update.effective_user
    chat = update.effective_chat
    message = update.effective_message
    
    if not is_staff(user.id) or chat.type != "private":
        return

    db.clear_all()
    balance_cache.clear()
    balance_cache_time.clear()
    await message.reply_text("База очищена.")
