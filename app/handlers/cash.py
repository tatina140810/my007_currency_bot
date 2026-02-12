import logging
from datetime import datetime
from telegram import Update
from telegram.ext import (
    ContextTypes,
    ConversationHandler,
    CommandHandler,
    MessageHandler,
    filters,
)

from app.db.instance import db
from app.core.config import CURRENCIES
from app.handlers.utils import is_staff, get_chat_id
from app.services.cash import set_opening_balances, get_report_data
from app.services.export_cash import export_cash_report
from app.services.operations import queue_operation
from app.services.parser import parse_human_number, normalize_group_name, normalize_currency

logger = logging.getLogger(__name__)

# Состояния для ConversationHandler /cash_open
WAITING_FOR_BALANCES = 1

async def cmd_cash_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Start /cash_open conversation.
    """
    user = update.effective_user
    chat = update.effective_chat
    
    # Только в личке и только сотрудники (пока проверим is_staff если нужно, но в задании "trigger in private chat")
    if chat.type != "private":
        await update.message.reply_text("Эта команда работает только в личном чате.")
        return ConversationHandler.END

    # Check if balances already exist for today?
    # Requirement: "Opening balance can be entered only once per date... allow overwrite only via /cash_open overwrite"
    
    args = context.args
    overwrite = False
    if args and "overwrite" in args[0].lower():
        overwrite = True

    today = datetime.now().strftime("%Y-%m-%d")
    existing = db.get_cash_opening_balances(today)
    
    if existing and not overwrite:
        await update.message.reply_text(
            f"⚠️ Начальный остаток на {today} уже установлен.\n"
            "Используйте `/cash_open overwrite` для перезаписи.",
            parse_mode="Markdown"
        )
        return ConversationHandler.END

    await update.message.reply_text(
        f"📅 Установка начального остатка на {today}.\n"
        "Введите суммы по валютам (можно несколько строк):\n"
        "Пример:\n"
        "USD 1200\n"
        "EUR 300\n"
        "KGS 150000"
    )
    return WAITING_FOR_BALANCES

async def handle_opening_balances_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Handle text input for opening balances.
    Supports multiline and single-line with multiple currencies.
    Format: "USD 100 EUR 500" or "100 USD \n 500 EUR"
    """
    text = update.message.text
    today = datetime.now().strftime("%Y-%m-%d")
    
    parsed_balances = {}
    
    # Pre-processing: replace nbsp
    text = text.replace("\u00A0", " ")
    
    # Strategy 1: Regex for "Currency Amount" (e.g. "Рубли 12 000 USD 500")
    # Curr: letters/symbols, Amount: digits/spaces/dots/commas
    # We use a lookahead to stop at the next currency-like token
    import re
    
    # Regex pattern:
    # 1. Currency (group 'c')
    # 2. Spaces
    # 3. Amount (group 'a') - greedy until...
    # 4. Lookahead for (Space + Currency) OR End of String
    
    # Note: Currency regex approximation: 2+ letters or specific symbols
    curr_pattern = r"(?:[a-zA-Zа-яА-Я]{2,}|[$€¥₽])"
    
    pattern_curr_first = re.compile(
        rf"(?P<c>{curr_pattern})\s+(?P<a>[\d\s.,]+?)(?=\s+{curr_pattern}|\s*$)"
    )
    
    # Strategy 2: "Amount Currency" (e.g. "12000 Руб 500 USD")
    pattern_amount_first = re.compile(
        rf"(?P<a>[\d\s.,]+?)\s+(?P<c>{curr_pattern})(?=\s+[\d\s.,]|\s*$)"
    )

    # Try matching
    matches_cf = list(pattern_curr_first.finditer(text))
    matches_af = list(pattern_amount_first.finditer(text))
    
    # Heuristic: choose the strategy with MORE matches, or default to Currency First if ambiguous?
    # User example: "Рубли 12027 694.000 USD 181 361.67..." -> Currency First
    
    final_matches = []
    if len(matches_cf) >= len(matches_af) and len(matches_cf) > 0:
        final_matches = matches_cf
    elif len(matches_af) > 0:
        final_matches = matches_af
    else:
        # Fallback to line-by-line simple split
        pass

    processed_count = 0
    
    for m in final_matches:
        raw_curr = m.group("c")
        raw_amount = m.group("a")
        
        try:
            val = parse_human_number(raw_amount)
            curr = normalize_currency(raw_curr)
            if curr: # Only if valid currency
                parsed_balances[curr] = val
                processed_count += 1
        except:
            pass
            
    # Fallback: if regex failed completely, try line-by-line (legacy simple)
    if not parsed_balances:
        lines = text.splitlines()
        for line in lines:
            parts = line.strip().split()
            if len(parts) < 2:
                continue
            
            # Simple heuristic provided before
            try:
                # Try parts[0]=Amount
                val = parse_human_number(parts[0])
                curr = normalize_currency(parts[1])
                if curr: parsed_balances[curr] = val
            except:
                try:
                    # Try parts[0]=Currency (and assume parts[1] is amount)
                    # But parts[1] might be partial amount "12 000"?
                    # Only works for "RUB 12000" (no spaces in number)
                    val = parse_human_number(parts[1])
                    curr = normalize_currency(parts[0])
                    if curr: parsed_balances[curr] = val
                except:
                    pass

    if not parsed_balances:
        await update.message.reply_text(
            "❌ Не удалось распознать данные.\n"
            "Попробуйте формат: 'USD 100' или '100 USD' (можно списком)."
        )
        return WAITING_FOR_BALANCES # Loop

    # Save
    await set_opening_balances(today, parsed_balances)
    
    msg = f"✅ Начальный остаток на {today} сохранен:\n"
    for cur, amt in parsed_balances.items():
        msg += f"{cur}: {amt:,.2f}\n"

    # Warning if we suspect we missed something?
    # Hard to know.
    
    await update.message.reply_text(msg)
    return ConversationHandler.END

async def cancel_cash_open(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Отмена ввода начального остатка.")
    return ConversationHandler.END

async def cmd_cash_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Generate Cash Evening Report.
    """
    logger.info("[CASH_REPORT] Command triggered")
    chat = update.effective_chat
    if chat.type != "private":
         await update.message.reply_text("Только в личном чате.")
         return

    report_date = datetime.now()
    # Check if opening balance exists
    today_str = report_date.strftime("%Y-%m-%d")
    logger.info(f"[CASH_REPORT] Checking opening balance for {today_str}")
    
    existing = db.get_cash_opening_balances(today_str)
    if not existing:
        logger.info("[CASH_REPORT] No opening balance found")
        await update.message.reply_text("⚠️ Please set the opening balance first using /cash_open")
        return

    await update.message.reply_text("⏳ Генерирую отчет...")
    logger.info("[CASH_REPORT] Generating data...")
    
    try:
        data = await get_report_data(report_date)
        if not data:
             logger.error("[CASH_REPORT] get_report_data returned None")
             await update.message.reply_text("Ошибка получения данных.")
             return
             
        # Generate Excel
        import os
        filename = f"Cash_Evening_Report_{today_str}.xlsx"
        path = os.path.join("outputs", filename)
        os.makedirs("outputs", exist_ok=True)
        
        logger.info(f"[CASH_REPORT] Exporting to {path}")
        export_cash_report(data, path)
        
        logger.info("[CASH_REPORT] Sending file...")
        with open(path, "rb") as f:
            await update.message.reply_document(document=f, filename=filename, caption=f"Cash Report {today_str}")
        logger.info("[CASH_REPORT] Sent successfully")
            
    except Exception as e:
        logger.exception("Error generating cash report")
        await update.message.reply_text(f"❌ Error: {e}")

async def cmd_set_rate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /set_rate [GROUP] FROM TO RATE
    Example: /set_rate [Office] USD RUB 90
    Default group if omitted? Warning.
    """
    args = context.args
    if len(args) < 3:
        await update.message.reply_text("Usage: /set_rate [GROUP] FROM TO RATE\nExample: /set_rate [Office] USD RUB 90.5")
        return
        
    # Parse args
    # Need to handle group in brackets or just first arg?
    # Logic similar to bot.py parsing?
    
    # Try: /set_rate USD RUB 90 (no group -> default?)
    # Try: /set_rate [Group] USD RUB 90
    
    text = " ".join(args)
    # Reuse regex from bot.py if possible, or simple check
    import re
    group_match = re.search(r"\[(.*?)\]", text)
    group_name = "General" # Default?
    
    if group_match:
        group_name = group_match.group(1)
        text = text.replace(group_match.group(0), "").strip()
        
    parts = text.split()
    if len(parts) < 3:
         await update.message.reply_text("Ошибка параметров.")
         return
         
    from_curr = normalize_currency(parts[0])
    to_curr = normalize_currency(parts[1])
    try:
        rate = parse_human_number(parts[2])
    except:
        await update.message.reply_text("Неверный формат курса.")
        return
        
    # Get group_id from name (reuse logic)
    # Wait, db has get_chat_id_by_name. But internal_rates table uses group_id (integer).
    # If group is new, we might strictly need an ID.
    # Logic in bot uses chat_id as group identifier.
    
    group_id = db.get_chat_id_by_name(group_name)
    if not group_id:
        # If group doesn't exist, we can't key by ID.
        # Maybe use group_name string in table?
        # Database schema for internal_rates uses group_id INTEGER.
        # So we MUST have a registered chat for it.
        await update.message.reply_text(f"❌ Группа '{group_name}' не найдена в базе.")
        return

    db.set_internal_rate(from_curr, to_curr, rate, group_id)
    await update.message.reply_text(f"✅ Курс для {group_name}: {from_curr} -> {to_curr} = {rate}")

async def cmd_internal_exchange(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /cash_exchange [GROUP] 100 USD to RUB
    """
    args = context.args
    text = " ".join(args)
    if not text:
         await update.message.reply_text("Usage: /cash_exchange [GROUP] AMOUNT CURRENCY to TARGET_CURRENCY")
         return

    # Parse group
    import re
    group_match = re.search(r"\[(.*?)\]", text)
    group_name = "General" 
    
    if group_match:
        group_name = group_match.group(1)
        text = text.replace(group_match.group(0), "").strip()

    # Parse rest: 100 USD to RUB
    parts = text.split()
    # Expected: AMOUNT FROM to TO
    # or: FROM AMOUNT to TO ?
    # Let's support "100 USD to RUB"
    
    if "to" in parts:
        to_index = parts.index("to")
        # parts before 'to' -> amount and currency
        left = parts[:to_index]
        right = parts[to_index+1:]
        
        if len(right) < 1:
            await update.message.reply_text("Error: missing target currency")
            return
            
        target_curr_code = right[0]
        
        # Left side: 100 USD or USD 100
        amount = None
        source_curr_code = None
        
        if len(left) < 2:
             await update.message.reply_text("Error: missing amount or source currency")
             return
             
        try:
            amount = parse_human_number(left[0])
            source_curr_code = left[1]
        except:
             try:
                amount = parse_human_number(left[1])
                source_curr_code = left[0]
             except:
                pass
                
        if amount is None or not source_curr_code:
             await update.message.reply_text("Error: invalid amount or currency")
             return
             
        # Normalize
        source_curr = normalize_currency(source_curr_code)
        target_curr = normalize_currency(target_curr_code)
        
        # Get Group ID
        group_id = db.get_chat_id_by_name(group_name)
        if not group_id:
             await update.message.reply_text(f"❌ Group '{group_name}' not found.")
             return
             
        # Get Rate
        rate = db.get_internal_rate(source_curr, target_curr, group_id)
        if not rate:
             await update.message.reply_text(f"❌ Internal rate {source_curr}->{target_curr} not set for {group_name}. Use /set_rate.")
             return
             
        # Calculate
        converted_amount = amount * rate
        
        # Queue Operations
        # We need to write to the 'operations' table with chat_id = group_id
        
        desc = f"Internal Exchange {source_curr}->{target_curr} @ {rate}"
        
        # 1. OUT
        await queue_operation(group_id, "Internal Exchange", source_curr, -amount, desc)
        
        # 2. IN
        await queue_operation(group_id, "Internal Exchange", target_curr, converted_amount, desc)
        
        await update.message.reply_text(
            f"✅ Exchanged {amount} {source_curr} -> {converted_amount:,.2f} {target_curr}\n"
            f"Rate: {rate}\nGroup: {group_name}"
        )
        
    else:
        await update.message.reply_text("Syntax error. use 'to' between currencies.")
        return

# Conversation handler definition
cash_open_handler = ConversationHandler(
    entry_points=[CommandHandler("cash_open", cmd_cash_open)],
    states={
        WAITING_FOR_BALANCES: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_opening_balances_input)],
    },
    fallbacks=[CommandHandler("cancel", cancel_cash_open)],
)
