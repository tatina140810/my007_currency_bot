import logging
from telegram import Update
from telegram.ext import ContextTypes

from app.db.instance import db
from app.services.ai_parser import parse_with_ai
from app.services.operations import queue_operation

logger = logging.getLogger(__name__)

async def handle_ai_learning_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handles callbacks from the Admin when the AI fails to parse a transaction 
    and the bot sends an inline keyboard to resolve it manually.
    """
    query = update.callback_query
    await query.answer()

    data = query.data
    # Expected format: ai_learn_{action}_{pending_id}
    # Actions: income, payment, withdraw, deposit, ignore
    parts = data.split('_')
    if len(parts) != 4:
        return

    action = parts[2]
    try:
        pending_id = int(parts[3])
    except ValueError:
        return

    # Delete the pending operation once clicked to prevent double-clicks
    pending_op = db.get_pending_operation(pending_id)
    if not pending_op:
        await query.edit_message_text("⚠️ Эта операция уже обработана или удалена из базы.")
        return
        
    db.delete_pending_operation(pending_id)

    chat_id = pending_op['chat_id']
    original_text = pending_op['text']
    reply_context = pending_op['reply_context']

    if action == "ignore":
        await query.edit_message_text(f"🗑 Игнорировано: <i>{original_text}</i>", parse_mode="HTML")
        return

    # Map action to standardized Operation Type
    op_type_map = {
        "income": "Поступление",
        "payment": "Оплата ПП",
        "withdraw": "Выдача наличных",
        "deposit": "Взнос наличными",
        "cnv": "Конвертация",
        "hrb": "Комиссия Харбор",
        "adj": "Заявление на корректировку",
        "fee": "Комиссия за услуги"
    }
    
    forced_op_type = op_type_map.get(action)
    if not forced_op_type:
        return

    await query.edit_message_text(f"⏳ Обучаю ИИ распознавать '{forced_op_type}' из текста: <i>{original_text}</i>...", parse_mode="HTML")

    # Ask the AI again, but this time FORCE the operation type
    # We cheat a bit by appending explicit instructions to the text
    forced_prompt = f"СТРОГО ИЗВЛЕКИ СУММУ И ВАЛЮТУ. ТИП ОПЕРАЦИИ ТОЧНО '{forced_op_type}'. ИГНОРИРУЙ ОСНОВНЫЕ ПРАВИЛА И ПРОСТО ВЕРНИ ЕЕ.\n\n" + original_text
    
    ai_parsed_list = await parse_with_ai(forced_prompt, reply_context)

    if not ai_parsed_list:
        await query.edit_message_text(f"❌ ИИ все равно не смог найти сумму и валюту в тексте: <i>{original_text}</i>", parse_mode="HTML")
        return

    # Process the first valid operation found
    ai_op = ai_parsed_list[0]
    amount = ai_op["amount"]
    currency = ai_op["currency"]
    # We use the forced type regardless of what AI returned, just in case
    
    # Save to the training examples database for future Few-Shot Learning!
    db.save_ai_training_example(
        original_text=original_text,
        reply_context=reply_context,
        op_type=forced_op_type,
        currency=currency,
        amount=amount
    )

    # Re-queue the operation to actually execute it
    # Note: we need to handle negative signs for Payments/Withdrawals
    signed_amount = amount
    if forced_op_type in ["Оплата ПП", "Выдача наличных", "Возврат клиенту"]:
        signed_amount = -amount

    await queue_operation(
        chat_id=chat_id,
        op_type=forced_op_type,
        currency=currency,
        amount=signed_amount,
        description=original_text
    )

    await query.edit_message_text(
        f"✅ <b>Успешно! ИИ обучен.</b>\n"
        f"Записано: {forced_op_type} {amount:,.2f} {currency}\n"
        f"Оригинал: <i>{original_text}</i>", 
        parse_mode="HTML"
    )

async def handle_balance_sync_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handles callbacks for Balance Synchronization (Residuals).
    Format: sync_bal_{chat_id}_{currency}_{diff}_{action}
    Actions: inc (Income), dep (Deposit), exp (Withdraw), pay (Payment), ign (Ignore)
    """
    query = update.callback_query
    await query.answer()

    data = query.data
    parts = data.split('_')
    
    # Expected: ['sync', 'bal', 'chat_id', 'currency', 'diff', 'action']
    if len(parts) != 6:
        return
        
    chat_id = int(parts[2])
    currency = parts[3]
    diff = float(parts[4])
    action = parts[5]

    if action == "ign":
        await query.edit_message_text(
            f"✅ Расхождение в {diff:,.2f} {currency} проигнорировано."
        )
        return

    # Map action to standardized Operation Type
    op_type_map = {
        "inc": "Поступление",
        "dep": "Взнос наличными",
        "exp": "Выдача наличных",
        "pay": "Оплата ПП",
        "cnv": "Конвертация",
        "hrb": "Комиссия Харбор",
        "adj": "Заявление на корректировку",
        "fee": "Комиссия за услуги"
    }
    
    forced_op_type = op_type_map.get(action)
    if not forced_op_type:
        return

    # To fix the balance, we just apply the difference (diff is reported_balance - actual_balance)
    # The diff itself might be negative if reported < actual.
    # The action specifies *type* of operation, not sign. 
    # But usually adjustments are mathematically just adding `diff` to the balance.
    # We will log the operation exactly with `diff` amount. If diff is negative, it will decrease the balance. 
    
    await queue_operation(
        chat_id=chat_id,
        op_type=forced_op_type,
        currency=currency,
        amount=diff, # Note: diff already holds the proper sign for the balance correction
        description=f"Баланс: авто-синхронизация ({forced_op_type})"
    )

    await query.edit_message_text(
        f"✅ <b>Баланс синхронизирован!</b>\n"
        f"Проведена операция: {forced_op_type} на {diff:,.2f} {currency}",
        parse_mode="HTML"
    )
