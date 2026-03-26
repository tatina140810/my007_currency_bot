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

async def cmd_fix_balances(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Принудительный пересчет балансов.
    Команда: /fix - для текущего чата
    Команда: /fix all - для всех чатов
    """
    user = update.effective_user
    chat = update.effective_chat
    
    if not is_staff(user.id):
        return

    # Check for arguments
    if context.args and context.args[0].lower() == "all":
        logger.info("Запущен пересчет балансов для ВСЕХ чатов")
        await update.message.reply_text("⏳ Пересчитываю балансы для ВСЕХ чатов (режим обслуживания включен)...")
        
        try:
            db.set_maintenance_mode(True)
            # Give batcher time to pause
            import asyncio
            await asyncio.sleep(1.0)
            
            db.recalculate_balances(None)
            balance_cache.clear()
            balance_cache_time.clear()  # Clear timestamps too
            await update.message.reply_text("✅ Все балансы успешно пересчитаны.")
        except Exception as e:
            logger.error(f"Error in cmd_fix_balances (all): {e}")
            await update.message.reply_text(f"❌ Ошибка: {e}")
        finally:
            db.set_maintenance_mode(False)
        return

    # Default: Single chat
    logger.info(f"Запущен пересчет балансов для чата {chat.id} ({chat.title})")
    await update.message.reply_text("⏳ Пересчитываю балансы...")
    
    try:
        db.recalculate_balances(chat.id)
        invalidate_balance_cache(chat.id)
        
        stats = db.get_statistics(chat.id)
        lines = []
        for curr, data in stats.items():
            lines.append(f"{curr}: {data['balance']:,.2f}")
            
        text = "✅ Балансы пересчитаны:\n" + ("\n".join(lines) if lines else "Нет данных")
        await update.message.reply_text(text)
    except Exception as e:
        logger.error(f"Error in cmd_fix_balances: {e}")
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def cmd_verify_integrity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Команда /verify
    Запускает полный аудит данных.
    """
    user = update.effective_user
    if not is_staff(user.id):
        return

    await update.message.reply_text("🔎 Запускаю финансовый аудит...")
    
    try:
        issues = db.verify_financial_integrity()
        
        if not issues:
            await update.message.reply_text("✅ Аудит пройден. Ошибок целостности не найдено.\nСуммы операций совпадают с балансами.\nЗнаки операций корректны.")
        else:
            # Split messages if too long
            header = f"⚠️ Найдено проблем: {len(issues)}\n\n"
            text_chunks = [header]
            current_chunk = header
            
            for issue in issues:
                line = issue + "\n"
                if len(current_chunk) + len(line) > 4000:
                    await update.message.reply_text(current_chunk)
                    current_chunk = ""
                current_chunk += line
                
            if current_chunk:
                await update.message.reply_text(current_chunk)
                
            await update.message.reply_text("🔧 Для исправления балансов используйте /fix all")

    except Exception as e:
        logger.error(f"Error during verify: {e}")
        await update.message.reply_text(f"❌ Ошибка аудита: {e}")

async def cmd_normalize_currencies(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Команда /normalize
    Миграция данных: приводит все валюты к каноническому виду.
    """
    user = update.effective_user
    if not is_staff(user.id):
        return

    await update.message.reply_text("🔄 Запускаю нормализацию валют...")
    
    try:
        db.set_maintenance_mode(True)
        # Give batcher time to pause
        import asyncio
        await asyncio.sleep(1.0)
        
        from app.services.parser import normalize_currency
        
        conn = db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT id, currency FROM operations")
        rows = cursor.fetchall()
        
        updated_count = 0
        for row in rows:
            op_id = row["id"]
            curr_raw = row["currency"]
            curr_norm = normalize_currency(curr_raw)
            
            if curr_raw != curr_norm:
                cursor.execute(
                    "UPDATE operations SET currency = ? WHERE id = ?",
                    (curr_norm, op_id)
                )
                updated_count += 1
        
        conn.commit()
        conn.close()
        
        if updated_count > 0:
            await update.message.reply_text(f"✅ Нормализовано {updated_count} операций.\n⏳ Пересчитываю балансы...")
            db.recalculate_balances(None)
            balance_cache.clear()
            balance_cache_time.clear()
            await update.message.reply_text("✅ Балансы обновлены.")
        else:
            await update.message.reply_text("✅ Все валюты уже в норме. Изменений нет.")

    except Exception as e:
        logger.error(f"Error during normalize: {e}")
        await update.message.reply_text(f"❌ Ошибка: {e}")
    finally:
        db.set_maintenance_mode(False)

async def cmd_purge_db(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Команда /purge_db
    Очищает старые операции из локальной БД (старше 30 дней),
    разгружая память сервера, так как история уже в Google Sheets.
    """
    user = update.effective_user
    if not is_staff(user.id):
        return

    await update.message.reply_text("🗑 Запускаю очистку старых операций (>30 дней)...")
    
    try:
        db.set_maintenance_mode(True)
        import asyncio
        await asyncio.sleep(1.0)
        
        conn = db.get_connection()
        cursor = conn.cursor()
        
        # SQLite modifiers: '-30 days'
        cursor.execute("DELETE FROM operations WHERE timestamp < datetime('now', '-30 days')")
        deleted_count = cursor.rowcount
        
        conn.commit()
        conn.close()
        
        if deleted_count > 0:
            await update.message.reply_text(f"✅ Удалено {deleted_count} старых операций из локальной базы.\nВсе данные сохранены в Google Sheets.")
        else:
            await update.message.reply_text("✅ Старых операций не найдено. База уже оптимизирована.")

    except Exception as e:
        logger.error(f"Error during purge_db: {e}")
        await update.message.reply_text(f"❌ Ошибка очистки: {e}")
    finally:
        db.set_maintenance_mode(False)
