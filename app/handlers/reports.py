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
from app.services.parser import parse_timestamp, parse_bulk_pp_payments
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

    rows = db.get_report_income_by_date(REPORT_CHAT_ID, report_date_str)

    if not rows:
        await update.message.reply_text(
            f"За {report_date.strftime('%d.%m.%Y')} нет подходящих поступлений в чате {REPORT_CHAT_ID}.",
            parse_mode=None
        )
        return

    base_dir = os.path.join(os.getcwd(), "outputs")
    os.makedirs(base_dir, exist_ok=True)

    filename = f"report_income_{report_date_str}.xlsx"
    output_path = os.path.join(base_dir, filename)

    try:
        await asyncio.to_thread(
            export_report_income_matrix,
            rows,
            output_path,
            report_date_str
        )

        with open(output_path, "rb") as f:
            await update.message.reply_document(
                document=f,
                filename=filename,
                caption=(
                    f"📄 Отчет поступлений за {report_date.strftime('%d.%m.%Y')}\n"
                    f"Источник: чат {REPORT_CHAT_ID}"
                ),
            )

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

    logger.info("[ALLBAL] Начинаем экспорт...")

    fd, path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)

    try:
        await asyncio.to_thread(export_group_balances_to_excel, db, path)

        filename = f"остатки_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.xlsx"
        
        with open(path, "rb") as f:
            await update.message.reply_document(
                document=f,
                filename=filename,
                caption="Остатки по группам (Excel)"
            )

    except Exception as e:
        logger.exception("[ALLBAL] Ошибка")
        await update.message.reply_text(f"❌ Ошибка /allbal: {e}")

    finally:
        try:
            os.remove(path)
        except Exception:
            pass


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

    base_dir = os.path.join(os.getcwd(), "outputs")
    os.makedirs(base_dir, exist_ok=True)
    output_path = os.path.join(base_dir, filename)

    try:
        await asyncio.to_thread(
            export_to_excel,
            db,
            output_path,
            date_from,
            date_to
        )

        if not os.path.exists(output_path):
            await status_msg.edit_text("❌ Ошибка: файл не был создан", parse_mode=None)
            return

        try:
            await status_msg.delete()
        except:
            pass

        with open(output_path, "rb") as file:
            caption_text = datetime.now(KG_TZ).strftime("%d.%m.%Y %H:%M")
            if date_from:
                caption_text += f"\nОперации за {date_from.strftime('%d.%m.%Y')}"
            else:
                caption_text += f"\n Все операции"

            await update.message.reply_document(
                document=file,
                filename=filename,
                caption=caption_text,
            )

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
