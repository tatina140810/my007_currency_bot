import asyncio
import logging
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    filters,
    MessageReactionHandler,
)

from app.core.config import BOT_TOKEN
from app.core.logger import logger
from app.services.operations import process_operation_batch
from app.db.instance import db
from app.handlers.utils import is_staff

# Handlers
from app.handlers.base import start, help_command, cancel_any, error_handler, handle_message_reaction
from app.handlers.reports import cmd_rep, show_balance, show_history, export_operations, cmd_sum, cmd_balances, general_button_callback, cmd_back_report
from app.handlers.operations import handle_text
from app.handlers.balance_input import handle_private_balance
from app.handlers.admin import undo_last_operation, undo_select_operation, cancel_undo, handle_delete_password, cmd_chats, cmd_clear_all, cmd_fix_balances, cmd_verify_integrity, cmd_normalize_currencies, cmd_purge_db
from app.handlers.pending import handle_ai_learning_callback, handle_balance_sync_callback
from app.handlers.documents import handle_uploaded_excel
from app.services.monitoring import sla_monitor_task

# Глобальная переменная для задачи
batch_task = None
sla_task = None

async def log_all_messages(update: Update, context):
    """Логирование всех сообщений"""
    message = update.message or update.edited_message or update.channel_post or update.edited_channel_post
    if getattr(update, "callback_query", None):
        logger.info(f"Callback Query: {update.callback_query.data}")
        return
        
    if not message:
        logger.info(f"Update received but no message attached: {update.to_dict()}")
        return
        
    user_id = message.from_user.id if message.from_user else "unknown"
    chat_id = message.chat.id if message.chat else "unknown"

    logger.info("=" * 80)
    if message.text:
        text = message.text
        logger.info(f"📨 ВХОДЯЩЕЕ СООБЩЕНИЕ: '{text}' (repr: {repr(text)}) from user {user_id} in chat {chat_id}")
    elif message.photo:
        caption = message.caption or ""
        logger.info(f"📸 ВХОДЯЩЕЕ ФОТО: Caption '{caption}' from user {user_id} in chat {chat_id}")
    elif message.document:
        caption = message.caption or ""
        mime = message.document.mime_type or "unknown"
        logger.info(f"📄 ВХОДЯЩИЙ ДОКУМЕНТ: MIME={mime} Caption '{caption}' from user {user_id} in chat {chat_id}")
        logger.info(f"❓ НЕИЗВЕСТНЫЙ ТИП СООБЩЕНИЯ: {message.to_dict()} from user {user_id} in chat {chat_id}")
    logger.info("=" * 80)

    # Функция фильтрации коротких "пустых" сообщений от SLA трекинга
    def is_generic_message(txt: str) -> bool:
        if not txt:
            return True
        import re
        # Убираем знаки препинания и приводим к нижнему регистру
        clean = re.sub(r'[^\w\s]', '', txt).lower()
        words = clean.split()
        if not words: # Если остались только смайлики
            return True
        
        # Сет игнорируемых слов
        ignore_words = {"добрый", "день", "вечер", "утро", "принято", "хорошо", 
                        "спасибо", "ок", "ok", "отлично", "благодарю", "супер", 
                        "понял", "понятно", "спс", "здравствуйте", "привет", 
                        "да", "ага", "плюс", "окей"}
        
        # Проверяем, состоят ли все слова текста только из игнорируемых
        return all(w in ignore_words for w in words)

    # Запись SLA для групп/супергрупп и личных чатов
    if chat_id != "unknown" and user_id != "unknown":
        if getattr(message.chat, "type", "private") in ["group", "supergroup", "private"]:
            # Если пишет юзер, и это пустое "спасибо", таймер SLA не обновляется
            if not is_staff(user_id) and is_generic_message(message.text):
                logger.info(f"Skipping SLA timer for generic message: '{message.text}'")
            else:
                db.update_chat_sla(chat_id, is_staff(user_id))

def main():
    """Главная функция"""
    global batch_task
    
    logger.info("Запуск бота...")
    print("🤖 ЗАПУСК БОТА...")

    # --- Single Instance Lock ---
    import fcntl
    import sys
    import os
    
    global lock_file
    lock_file = open("/tmp/my007_bot.lock", "w")
    try:
        fcntl.lockf(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        print("❌ Another instance is already running (check /tmp/my007_bot.lock). Exiting.")
        sys.exit(1)
    # ----------------------------

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

    # Реакции на сообщения (обновляет SLA, если лайк поставил стафф)
    application.add_handler(MessageReactionHandler(handle_message_reaction), group=1)

    # Жесткий перехват команды /back_report (group=-3)
    application.add_handler(
        MessageHandler(filters.Regex(r"^/back_report"), cmd_back_report),
        group=-3
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
    application.add_handler(CommandHandler("fix", cmd_fix_balances))
    application.add_handler(CommandHandler("verify", cmd_verify_integrity))
    application.add_handler(CommandHandler("normalize", cmd_normalize_currencies))
    application.add_handler(CommandHandler("purge_db", cmd_purge_db))
    application.add_handler(CommandHandler("back_report", cmd_back_report))



    # Handlers from app/handlers/cash.py
    from app.handlers.cash import cash_open_handler, cmd_cash_report, cmd_set_rate, cmd_internal_exchange, manual_exchange

    # Команды Cash Report
    application.add_handler(cash_open_handler)
    application.add_handler(CommandHandler("cash_report", cmd_cash_report))
    application.add_handler(CommandHandler("set_rate", cmd_set_rate))
    application.add_handler(CommandHandler("cash_exchange", cmd_internal_exchange)) # New command
    
    # Manual Currency Exchange: [rep] ...
    application.add_handler(MessageHandler(filters.Regex(r"(?i)^\[rep\]"), manual_exchange))

    # Callback кнопки
    application.add_handler(CallbackQueryHandler(general_button_callback, pattern="^(show_balance|show_history)$"))
    application.add_handler(CallbackQueryHandler(undo_select_operation, pattern="^undo_select_"))
    application.add_handler(CallbackQueryHandler(cancel_undo, pattern="^cancel_undo$"))
    # AI Learning Callbacks
    application.add_handler(CallbackQueryHandler(handle_ai_learning_callback, pattern="^ai_learn_"))
    # Balance Sync Callbacks
    application.add_handler(CallbackQueryHandler(handle_balance_sync_callback, pattern="^sync_bal_"))

    # Fallback Command Handler (Fix for missing entities)
    async def fallback_command_handler(update: Update, context):
        text = update.message.text
        if not text.startswith("/"):
            return

        # Simple parsing
        parts = text.split()
        cmd_raw = parts[0][1:].lower() # remove /
        if "@" in cmd_raw:
            cmd_raw = cmd_raw.split("@")[0]
        
        # Args
        context.args = parts[1:]
        
        logger.info(f"Fallback Command Handler: Triggered for '{cmd_raw}'")

        command_map = {
            "start": start,
            "help": help_command,
            "bal": show_balance, "balance": show_balance,
            "his": show_history, "history": show_history,
            "del": undo_last_operation,
            "ex": export_operations, "export": export_operations,
            "chats": cmd_chats,
            "allbal": cmd_balances,
            "rep": cmd_rep,
            "sum": cmd_sum,
            "clear": cmd_clear_all,
            "fix": cmd_fix_balances,
            "verify": cmd_verify_integrity,
            "normalize": cmd_normalize_currencies,
            "cash_report": cmd_cash_report,
            "set_rate": cmd_set_rate,
            "cash_exchange": cmd_internal_exchange,
            "cancel": cancel_any,
            "back_report": cmd_back_report
        }
        
        handler_func = command_map.get(cmd_raw)
        if handler_func:
            await handler_func(update, context)

    # Register Fallback Handler in Group 0 FIRST (before handle_delete_password catches it)
    application.add_handler(MessageHandler(filters.Regex(r"^/"), fallback_command_handler), group=0)

    # Текстовые обработчики
    # handle_private_balance: group=-1 (highest priority — intercepts private balance msgs)
    application.add_handler(MessageHandler(filters.TEXT & filters.ChatType.PRIVATE & ~filters.COMMAND, handle_private_balance), group=-1)

    # handle_delete_password: group=1 (prioritize password check)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_delete_password), group=1)
    
    # handle_text: group=2 (general operations)
    application.add_handler(MessageHandler((filters.TEXT | filters.PHOTO | filters.Document.ALL) & ~filters.COMMAND, handle_text), group=2)

    # handle_uploaded_excel: group=3 (excel files for reconciliation)
    application.add_handler(MessageHandler(filters.Document.FileExtension("xlsx"), handle_uploaded_excel), group=3)

    # Lifecycle hooks
    async def post_init(app: Application):
        global batch_task, sla_task
        batch_task = asyncio.create_task(process_operation_batch())
        sla_task = asyncio.create_task(sla_monitor_task(app))
        
        from app.services.reconciliation import reconcile_pending_operations
        app.job_queue.run_repeating(reconcile_pending_operations, interval=900, first=60)

        # Final evening re-check at 23:00 KG time:
        # Re-syncs Платежи / Конвертации / pending ЗАПРОСЫ ops and recalculates
        # "отчет по остаткам" to produce the authoritative end-of-day report.
        import datetime
        from zoneinfo import ZoneInfo
        from app.services.edit_check import recheck_and_resync_all_chats
        _KG_TZ = ZoneInfo("Asia/Bishkek")
        app.job_queue.run_daily(
            recheck_and_resync_all_chats,
            time=datetime.time(23, 0, tzinfo=_KG_TZ),
            name="night_final_recheck",
        )

        logger.info("Фоновая задача батчинга, SLA мониторинг, реконсилятор (15мин) и итоговый пересчёт (23:00) запущены")


    async def post_shutdown(app: Application):
        global batch_task, sla_task
        if batch_task:
            batch_task.cancel()
            try:
                await batch_task
            except asyncio.CancelledError:
                logger.info("Фоновая задача батчинга остановлена")
                
        if sla_task:
            sla_task.cancel()
            try:
                await sla_task
            except asyncio.CancelledError:
                logger.info("SLA мониторинг остановлен")

    application.post_init = post_init
    application.post_shutdown = post_shutdown
    application.add_error_handler(error_handler)

    logger.info("Бот успешно запущен!")
    print("🚀 БОТ УСПЕШНО ЗАПУЩЕН")
    
    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=False)

if __name__ == "__main__":
    main()
