import asyncio
import logging
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
)

from app.core.config import BOT_TOKEN
from app.core.logger import logger
from app.services.operations import process_operation_batch
from app.db.instance import db

# Handlers
from app.handlers.base import start, help_command, cancel_any, error_handler
from app.handlers.reports import cmd_rep, show_balance, show_history, export_operations, cmd_sum, cmd_balances, general_button_callback
from app.handlers.operations import handle_text
from app.handlers.admin import undo_last_operation, undo_select_operation, cancel_undo, handle_delete_password, cmd_chats, cmd_clear_all

# Глобальная переменная для задачи
batch_task = None

async def log_all_messages(update: Update, context):
    """Логирование всех сообщений"""
    if update.message and update.message.text:
        text = update.message.text
        user_id = update.effective_user.id if update.effective_user else "unknown"
        chat_id = update.effective_chat.id if update.effective_chat else "unknown"

        logger.info("=" * 80)
        logger.info(f"📨 ВХОДЯЩЕЕ СООБЩЕНИЕ: '{text}' from user {user_id} in chat {chat_id}")
        logger.info("=" * 80)

def main():
    """Главная функция"""
    global batch_task
    
    logger.info("Запуск бота...")
    print("🤖 ЗАПУСК БОТА...")

    # Миграция
    db.migrate_legacy_data()


    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(60)
        .read_timeout(60)
        .write_timeout(60)
        .build()
    )

    # Универсальный логгер (group=-1)
    application.add_handler(
        MessageHandler(filters.ALL, log_all_messages),
        group=-1
    )

    # Команды
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("bal", show_balance))
    application.add_handler(CommandHandler("balance", show_balance))
    application.add_handler(CommandHandler("his", show_history))
    application.add_handler(CommandHandler("history", show_history))
    application.add_handler(CommandHandler("del", undo_last_operation))
    application.add_handler(CommandHandler("ex", export_operations)) # /ex directly
    application.add_handler(CommandHandler("export", export_operations))
    application.add_handler(CommandHandler("cancel", cancel_any))
    application.add_handler(CommandHandler("chats", cmd_chats))
    application.add_handler(CommandHandler("allbal", cmd_balances))
    application.add_handler(CommandHandler("rep", cmd_rep))
    application.add_handler(CommandHandler("sum", cmd_sum))
    application.add_handler(CommandHandler("clear", cmd_clear_all)) # /clear all handled inside? no, cmd_clear_all checks logic

    # Callback кнопки
    application.add_handler(CallbackQueryHandler(general_button_callback, pattern="^(show_balance|show_history)$"))
    application.add_handler(CallbackQueryHandler(undo_select_operation, pattern="^undo_select_"))
    application.add_handler(CallbackQueryHandler(cancel_undo, pattern="^cancel_undo$"))

    # Текстовые обработчики
    # handle_delete_password: group=0 (prioritize password check)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_delete_password), group=0)
    
    # handle_text: group=1 (general operations)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text), group=1)

    # Lifecycle hooks
    async def post_init(app: Application):
        global batch_task
        batch_task = asyncio.create_task(process_operation_batch())
        logger.info("Фоновая задача батчинга запущена")

    async def post_shutdown(app: Application):
        global batch_task
        if batch_task:
            batch_task.cancel()
            try:
                await batch_task
            except asyncio.CancelledError:
                logger.info("Фоновая задача батчинга остановлена")

    application.post_init = post_init
    application.post_shutdown = post_shutdown
    application.add_error_handler(error_handler)

    logger.info("Бот успешно запущен!")
    print("🚀 БОТ УСПЕШНО ЗАПУЩЕН")
    
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=False)

if __name__ == "__main__":
    main()
