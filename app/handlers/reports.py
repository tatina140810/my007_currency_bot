import os
import asyncio
import tempfile
from datetime import datetime, date

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes

from app.core.logger import logger
from app.core.config import REPORT_CHAT_ID, CURRENCIES
from app.core.constants import KG_TZ
from app.db.instance import db
from app.handlers.utils import get_chat_id, get_chat_name, is_staff
from app.services.export import export_to_excel, export_group_balances_to_excel, export_report_income_matrix
from app.services.google_sheets import sync_all_balances_to_sheet, sync_daily_income, SPREADSHEET_ID
from app.services.parser import parse_timestamp, parse_bulk_pp_payments, normalize_currency, parse_human_number
from app.services.math import aggregate_bulk_sum

async def cmd_sum(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Работает лучше всего, если /sum отправлять REPLY на сообщение со "Список платежей..."
    msg = update.effective_message
    if not msg:
        return

    # Берем текст либо из reply, либо из текущего сообщения
    source_text = None
    if msg.reply_to_message and msg.reply_to_message.text:
        source_text = msg.reply_to_message.text
    else:
        source_text = msg.text or ""

    clean_text = source_text
    if clean_text.strip().lower().startswith("/sum"):
        clean_text = clean_text.split("\n", 1)[1] if "\n" in clean_text else ""

    bulk_items = parse_bulk_pp_payments(clean_text)
    if not bulk_items:
        await msg.reply_text(
            "❌ Не нашла платежи в сообщении.\n"
            "Сделай так: отправь список платежей и ответь на него командой /sum",
            parse_mode=None
        )
        return

    agg, totals = aggregate_bulk_sum(bulk_items)

    currencies = sorted({cur for comp in agg for cur in agg[comp].keys()})
    companies = sorted(agg.keys())

    # Красивый текст-отчет
    lines = []
    lines.append("📊 Сумма по клиентам / валютам\n")

    header = ["Клиент"] + currencies
    lines.append(" | ".join(header))
    lines.append("-" * 40)

    for comp in companies:
        row = [comp]
        for cur in currencies:
            v = agg[comp].get(cur, 0.0)
            row.append(f"{v:,.2f}" if abs(v) > 1e-9 else "")
        lines.append(" | ".join(row))

    lines.append("\nИТОГО:")
    for cur in currencies:
        lines.append(f"{cur}: {totals.get(cur, 0.0):,.2f}")

    await msg.reply_text("\n".join(lines), parse_mode=None)

async def cmd_rep(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info("=" * 60)
    logger.info("[REP] ФУНКЦИЯ ВЫЗВАНА!")
    logger.info(f"[REP] chat={update.effective_chat.id if update.effective_chat else None}")

    if not update.message:
        return

    chat = update.effective_chat
    if not chat:
        return

    # Только личка
    if chat.type != "private":
        await update.message.reply_text("⛔ Команда работает только в личных сообщениях")
        return

    report_date = datetime.now(KG_TZ).date()
    if context.args:
        arg = " ".join(context.args).strip()
        parsed = None
        for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d.%m.%y"):
            try:
                parsed = datetime.strptime(arg, fmt).date()
                break
            except ValueError:
                continue
    
        if not parsed:
            await update.message.reply_text(
                "❌ Неверный формат даты.\nПример: /rep 05.02.2026 или /rep 2026-02-05",
                parse_mode=None
            )
            return

        report_date = parsed

    report_date_str = report_date.isoformat()
    logger.info(f"[REP] Дата отчета: {report_date_str}")

    # Pass None as chat_id to search GLOBALLY
    rows = db.get_report_income_by_date(None, report_date_str)

    if not rows:
        await update.message.reply_text(
            f"За {report_date.strftime('%d.%m.%Y')} нет подходящих поступлений (по всем чатам).",
            parse_mode=None
        )
        return

    try:
        await sync_daily_income(report_date_str, rows)
        sheet_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit#gid=0"
        
        await update.message.reply_text(
            f"✅ Отчет поступлений за {report_date.strftime('%d.%m.%Y')} успешно синхронизирован с Google Таблицей!\n\n"
            f"📊 <b><a href='{sheet_url}'>Открыть Google Таблицу</a></b>",
            parse_mode="HTML"
        )
        # Also continue generating Excel as backup if needed, but per user request, we skip it
    except Exception as e:
        logger.exception("[REP] Ошибка при создании/отправке отчета")
        await update.message.reply_text(f"❌ Ошибка /rep: {e}", parse_mode=None)


async def cmd_balances(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /allbal"""
    if not update.message:
        return
    
    user = update.effective_user
    if not is_staff(user.id):
        await update.message.reply_text("Только для сотрудников")
        return

    logger.info("[ALLBAL] Начинаем экспорт в Google Sheets...")

    try:
        await sync_all_balances_to_sheet()
        sheet_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit#gid=0"

        await update.message.reply_text(
            f"✅ Остатки успешно синхронизированы!\n\n"
            f"📊 <b><a href='{sheet_url}'>Открыть Балансы</a></b>",
            parse_mode="HTML"
        )

    except Exception as e:
        logger.exception("[ALLBAL] Ошибка")
        await update.message.reply_text(f"❌ Ошибка /allbal: {e}")


async def show_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /bal"""
    chat = update.effective_chat
    chat_id = get_chat_id(update)
    chat_name = get_chat_name(update)
    telegram_chat_name = chat.title or chat.first_name or f"Чат {chat.id}"
    db.register_chat(chat.id, telegram_chat_name, chat.type)
    logger.info(f"Баланс запрошен для чата {chat_id}")

    # Note: No caching for now, direct DB call as refactoring step 1
    balances = db.get_balances(chat_id)
    text = f"БАЛАНС\n{chat_name}\n\n"
    total_exists = False
    
    for currency in CURRENCIES:
        balance = balances.get(currency, 0.0)
        if balance != 0:
            total_exists = True
        text += f"{currency}: {balance:,.2f}\n"

    if not total_exists:
        text += "\nОпераций пока нет"

    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(text, parse_mode=None)
    else:
        await update.message.reply_text(text, parse_mode=None)


async def show_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /his"""
    chat = update.effective_chat
    chat_id = get_chat_id(update)
    chat_name = get_chat_name(update)
    telegram_chat_name = chat.title or chat.first_name or f"Чат {chat.id}"
    db.register_chat(chat.id, telegram_chat_name, chat.type)
    logger.info(f"История запрошена для чата {chat_id}")

    target_date: date
    if update.message and context.args:
        date_str = " ".join(context.args).strip()
        parsed = None
        for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(date_str, fmt)
                break
            except ValueError:
                continue
        if not parsed:
            await update.message.reply_text("Неверный формат даты.\nИспользуйте: /his 01.12.2025", parse_mode=None)
            return
        target_date = parsed.date()
    else:
        target_date = datetime.now(KG_TZ).date()

    all_ops = db.get_operations(chat_id, limit=1000)
    filtered_ops = []
    for op in all_ops:
        # op: (id, type, currency, amount, description, timestamp)
        timestamp = op[5]
        op_dt = parse_timestamp(timestamp)
        if op_dt.date() == target_date:
            filtered_ops.append(op)

    if not filtered_ops:
        text = f"История за {target_date.strftime('%d.%m.%Y')} пуста\n{chat_name}"
    else:
        filtered_ops.sort(key=lambda o: parse_timestamp(o[5]))
        text = f"ОПЕРАЦИИ ЗА {target_date.strftime('%d.%m.%Y')}\n\n"
        for op in filtered_ops:
            op_id, op_type, currency, amount, description, timestamp = op
            sign = "+" if amount > 0 else ""
            ts_str = parse_timestamp(timestamp).strftime("%H:%M:%S")
            text += f"{op_type}\n"
            text += f"   {currency}: {sign}{amount:,.2f}\n"
            if description:
                text += f"   {description}\n"
            text += f"   {ts_str}\n"

    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(text, parse_mode=None)
    else:
        await update.message.reply_text(text, parse_mode=None)


async def export_operations(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /ex - экспорт в Excel"""
    message_text = update.message.text.strip()
    chat = update.effective_chat
    
    status_msg = await update.message.reply_text("⏳ Формирую файл...", parse_mode=None)

    date_from = None
    date_to = None

    parts = message_text.split(maxsplit=1)

    if len(parts) > 1:
        arg = parts[1].strip()
        arg_lower = arg.lower()

        if arg_lower in ("сегодня", "today"):
            date_from = date_to = datetime.now(KG_TZ).date()
        else:
            parsed = None
            for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d.%m.%y"):
                try:
                    parsed = datetime.strptime(arg, fmt).date()
                    break
                except ValueError:
                    continue

            if not parsed:
                await status_msg.edit_text(
                    f"❌ Неверный формат даты: '{arg}'\n\n"
                    "Примеры:\n"
                    "/ex — за всё время\n"
                    "/ex сегодня\n"
                    "/ex 15.01.2026\n"
                    "/ex 2026-01-15",
                    parse_mode=None
                )
                return

            date_from = date_to = parsed

    if date_from:
        fname_date = date_from.strftime("%d_%m_%Y")
        filename = f"operations_{fname_date}.xlsx"
    else:
        filename = "operations_all.xlsx"

    try:
        # Full history is tracked in realtime on Google Sheets, so we just provide the link.
        sheet_url = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/edit#gid=0"
        
        msg_text = (
            f"✅ История операций ведется автоматически в Google Таблице!\n\n"
            f"🔗 <b><a href='{sheet_url}'>Открыть Историю</a></b>"
        )
        
        if date_from:
             msg_text += f"\n\n<em>Примечание: Для выборки за {date_from.strftime('%d.%m.%Y')} используйте встроенные фильтры Google Sheets.</em>"
             
        await status_msg.edit_text(msg_text, parse_mode="HTML")

    except Exception as e:
        logger.exception(f"❌ Ошибка экспорта")
        try:
            await status_msg.edit_text(
                f"❌ Ошибка при экспорте:\n{str(e)[:300]}",
                parse_mode=None
            )
        except:
            await update.message.reply_text(
                f"❌ Ошибка при экспорте:\n{str(e)[:300]}",
                parse_mode=None
            )

async def general_button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка callback кнопок"""
    query = update.callback_query
    logger.info(f"Callback: {query.data}")
    await query.answer()
    
    if query.data == "show_balance":
        await show_balance(update, context)
    elif query.data == "show_history":
        await show_history(update, context)


async def cmd_back_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /back_report"""
    from app.core.logger import logger
    logger.info("[BACK_REPORT] Command triggered")
    
    msg = update.effective_message
    if not msg:
        logger.warning("[BACK_REPORT] No effective message")
        return
        
    text_to_parse = None
    if msg.reply_to_message and msg.reply_to_message.text:
        text_to_parse = msg.reply_to_message.text
        logger.info("[BACK_REPORT] Using reply_to_message text")
    elif context.args:
        text_to_parse = msg.text.split("\n", 1)[1] if "\n" in msg.text else " ".join(context.args)
        logger.info("[BACK_REPORT] Using args text")
    else:
        from app.db.instance import db
        from app.core.config import CONVERSION_GROUP_NAME
        
        if msg.chat.type == "private":
            # For DMs, try fetching the group's last message first
            group_id = db.get_chat_id_by_name(CONVERSION_GROUP_NAME)
            if group_id:
                text_to_parse = db.get_last_back_report_text(group_id)
                logger.info(f"[BACK_REPORT] Retrieved from GROUP DB ({CONVERSION_GROUP_NAME}) - length: {len(text_to_parse) if text_to_parse else 0}")
                
        # Fallback to current chat exactly
        if not text_to_parse:
            text_to_parse = db.get_last_back_report_text(msg.chat_id)
            logger.info(f"[BACK_REPORT] Retrieved from LOCAL DB, length: {len(text_to_parse) if text_to_parse else 0}")
        
    if not text_to_parse:
        logger.warning("[BACK_REPORT] text_to_parse is empty")
        await msg.reply_text("❌ Нет данных для формирования отчета. Либо ответьте на сообщение (Reply), либо отправьте список платежей перед командой.")
        return
        
    from app.services.parser import parse_back_report_payments
    from app.services.export import export_back_report_to_excel
    import tempfile
        
    try:
        parsed = parse_back_report_payments(text_to_parse)
        logger.info(f"[BACK_REPORT] Parsed data items count: {len(parsed['items'])}")
        if not parsed["items"]:
            await msg.reply_text("❌ В сообщении не найдено платежей.")
            return
            
        filename = f"back_report_{parsed['date']}.xlsx"
        filepath = os.path.join(tempfile.gettempdir(), filename)
        logger.info(f"[BACK_REPORT] Exporting to {filepath}")
        
        export_back_report_to_excel(parsed, filepath)
        
        if os.path.exists(filepath):
            logger.info(f"[BACK_REPORT] Sending document {filepath}")
            with open(filepath, "rb") as f:
                await msg.reply_document(document=f, filename=filename)
            try:
                os.remove(filepath)
            except Exception as e:
                logger.error(f"[BACK_REPORT] Failed to remove temp file: {e}")
        else:
            logger.error("[BACK_REPORT] File does not exist after export!")
    except Exception as e:
        logger.exception(f"[BACK_REPORT] Exception during execution: {e}")
        await msg.reply_text(f"❌ Ошибка при формировании отчета: {e}")

