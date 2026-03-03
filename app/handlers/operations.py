from telegram import Update
from telegram.ext import ContextTypes

from app.core.logger import logger
from app.db.instance import db
from app.handlers.utils import is_staff
from app.services.parser import (
    extract_group_tag, normalize_group_name, looks_like_bank_income,
    parse_income_notification, parse_bulk_pp_payments, parse_manual_operation_line
)
from app.services.ai_parser import parse_with_ai
from app.services.operations import queue_operation, resolve_target_chat_id
from app.services.math import compute_conversion_to_amount

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):

    message = update.effective_message
    user = update.effective_user
    chat = update.effective_chat

    if not message or not user or not chat:
        return

    if user.is_bot or not message.text:
        return

    text = message.text.strip()
    is_private = chat.type == "private"
    staff = is_staff(user.id)

    # Логируем
    logger.info(
        f"MSG chat={chat.id} user={user.id} private={is_private} text='{text[:100]}'"
    )

    # 1️⃣ КОМАНДЫ (кроме /clear all) - они обрабатываются отдельно, но
    # если вдруг handler текстовый перехватил, игнорируем
    if text.startswith("/") and text.lower() != "/clear all":
        return

    # 2️⃣ РЕГИСТРАЦИЯ ЧАТА
    chat_name = chat.title or chat.first_name or f"Чат {chat.id}"
    db.register_chat(chat.id, chat_name, chat.type)

    # 3️⃣ CLEAR ALL (ТОЛЬКО STAFF + ЛИЧКА)
    # Это лучше вынести в commands, но раз уж было здесь (и это редкая админ команда)
    # можно оставить или перенести в admin.py как команду
    # Здесь оставим логику "текстовой команды", если нужно

    # 4️⃣ ИЗВЛЕЧЕНИЕ ГРУППЫ ИЗ [ГРУППА] (ТОЛЬКО В ЛИЧКЕ)
    group_name = None
    clean_text = text

    if is_private:
        # Если это спец-команда [internal_report], не извлекаем её как группу
        if not text.lower().startswith("[internal_report]"):
            group_tag, clean_text = extract_group_tag(text)
            if group_tag:
                # Нормализуем группу
                group_name = normalize_group_name(group_tag)
                logger.info(f"📋 Извлечена группа: '{group_tag}' → '{group_name}'")

    # 5️⃣ АВТО-ПОСТУПЛЕНИЯ (БАНК)
    if looks_like_bank_income(clean_text):
        logger.info(f"[AUTO_INCOME] matched: chat={chat.id}")

        income = parse_income_notification(clean_text)
        if not income:
            logger.info("[AUTO_INCOME] parse_income_notification=None")
            return

        # LOGIC CHANGE: REPORT_CHAT_ID is Global Income
        from app.core.config import REPORT_CHAT_ID
        
        target_chat_id = None
        
        if chat.id == REPORT_CHAT_ID:
            # Global Income -> Always write to REPORT_CHAT_ID
            target_chat_id = REPORT_CHAT_ID
        else:
            # Client/Group Income
            if is_private:
                if not group_name:
                    await message.reply_text(
                        "❗ В личном чате укажи группу ПЕРЕД сообщением.\n"
                        "Пример:\n[УЗ] поступили 5000 usdt"
                    )
                    return
                target_chat_id = db.get_chat_id_by_name(group_name)
                if not target_chat_id:
                    await message.reply_text(f"❌ Группа '{group_name}' не найдена")
                    return
            else:
                # Группа - пишем в текущий чат
                target_chat_id = chat.id

        # Use Forward Date if available (for forwarded bank messages), else Message Date
        # PTB v20+ uses forward_origin, older uses forward_date
        msg_date = message.date
        
        if getattr(message, "forward_origin", None):
             msg_date = message.forward_origin.date
        elif getattr(message, "forward_date", None):
             msg_date = message.forward_date

        # Use Forward Date if available (for forwarded bank messages), else Message Date
        # PTB v20+ uses forward_origin, older uses forward_date
        msg_date = message.date
        
        if getattr(message, "forward_origin", None):
             msg_date = message.forward_origin.date
        elif getattr(message, "forward_date", None):
             msg_date = message.forward_date

        # FIX: Convert to KG_TZ (Asia/Bishkek) to ensure correct date (avoid UTC prev day issue)
        from app.core.constants import KG_TZ
        msg_date = msg_date.astimezone(KG_TZ)

        await queue_operation(
            target_chat_id,
            "Поступление",
            income["currency"],
            income["amount"],
            income["description"],
            timestamp=msg_date
        )

        logger.info(
            f"[AUTO_INCOME] queued {income['amount']} {income['currency']} -> chat {target_chat_id} date={msg_date}"
        )
        return

    if staff:
        bulk = parse_bulk_pp_payments(clean_text)
        if bulk:
            for item in bulk:
                target_group = normalize_group_name(item["group"])
                target_chat_id = db.get_chat_id_by_name(target_group)
                if not target_chat_id:
                    continue

                desc = f"{item['company']} | {item['receiver']}"
                await queue_operation(
                    target_chat_id,
                    "Оплата ПП",
                    item["currency"],
                    -item["amount"],
                    desc,
                )
            await message.reply_text("✅ Bulk платежи обработаны")
            return

    # =====================================================
    # 4️⃣ РУЧНЫЕ ОПЕРАЦИИ (Regular + AI Fallback)
    # =====================================================
    if not staff:
        return

    manual = parse_manual_operation_line(clean_text)
    
    if manual:
        # Standard strict parsing flow
        try:
            target_chat_id = resolve_target_chat_id(
                chat=chat,
                is_private=is_private,
                group_from_manual=manual.get("group"),
            )
        except ValueError as e:
            # SPECIAL CASE: [internal_report] commands allow defaulting to current chat (DM)
            is_internal = False
            if manual["type"] == "Manual Buy FX":
                is_internal = True
            elif manual["type"] == "Выдача наличных" and manual.get("description") == "Выдача наличных (internal_report)":
                is_internal = True
                
            if is_internal and is_private and not manual.get("group"):
                target_chat_id = chat.id
            else:
                await message.reply_text(str(e))
                return

        op_type = manual["type"]
        amount = manual["amount"]
        currency = manual["currency"]
        desc = manual.get("description", "")
        group_name = manual.get("group")
    
    else:
        # ----------------------------------------------------------------
        # 🤖 AI PARSING FALLBACK (Phase 2: Lists & Context)
        # ----------------------------------------------------------------
        reply_context = None
        if message.reply_to_message and message.reply_to_message.text:
            reply_context = message.reply_to_message.text

        ai_parsed_list = await parse_with_ai(clean_text, reply_context)
        
        if not ai_parsed_list:
            return  # Not recognized mathematically or AI failed
            
        success_messages = []
        
        for ai_op in ai_parsed_list:
            op_type = ai_op["type"]
            currency = ai_op["currency"]
            amount = ai_op["amount"]
            desc = f"[AI] {ai_op.get('description', '')}".strip()
            group_name = ai_op.get("group")
            
            # 1. Enforce Privacy Rule for 'Оплата ПП'
            if op_type == "Оплата ПП" and not is_private:
                logger.warning(f"AI attempted to parse 'Оплата ПП' in group chat {chat.id}. Ignored for security.")
                continue # Skip processing this operation
            
            # 2. Resolve target chat
            try:
                target_chat_id = resolve_target_chat_id(
                    chat=chat,
                    is_private=is_private,
                    group_from_manual=group_name,
                )
            except ValueError as e:
                # If AI fails to determine chat context, we must skip this operation
                await message.reply_text(f"⚠️ ИИ-ошибка для {op_type}: {str(e)}")
                continue

            # 3. Queue the operation
            # Handle Internal Exchange sign rules specifically
            save_amount = amount
            if op_type == "Internal Exchange" and currency == "RUB":
                save_amount = -amount # RUB goes out when buying foreign currency
            
            # Standard Expense rules
            if op_type in ["Выдача наличных", "Оплата ПП", "Комиссия 1%", "Комиссия банка", "Конвертация"]:
                save_amount = -amount
                
            await queue_operation(
                target_chat_id,
                op_type,
                currency,
                save_amount,
                desc,
            )
            
            success_messages.append(f"Тип: `{op_type}`\nСумма: `{amount} {currency}`\nОсн: _{desc}_")

        if success_messages:
            summary = "\n---\n".join(success_messages)
            await message.reply_text(
                f"🤖 *Распознано ИИ:*\n{summary}",
                parse_mode="Markdown"
            )
        return
        # EOF AI PARSER

    # --------------------
    # MANUAL BUY FX (Internal Report)
    # --------------------
    if op_type == "Manual Buy FX":
        rate = manual["rate"]
        rub_amount = amount * rate
        
        # 1. Add Foreign Currency (+)
        await queue_operation(
            target_chat_id, 
            "Internal Exchange", 
            currency, 
            amount, 
            f"FX: Buy {currency} rate {rate}"
        )
        
        # 2. Deduct RUB (-)
        await queue_operation(
            target_chat_id, 
            "Internal Exchange", 
            "RUB", 
            -rub_amount, 
            f"FX: Buy {currency} rate {rate}"
        )
        await message.reply_text(
            f"✅ [Internal Report] Buy FX\n"
            f"+{amount:,.2f} {currency}\n"
            f"-{rub_amount:,.2f} RUB"
        )
        return

        return

    # --------------------
    # MANUAL CASH OUT (Internal Report)
    # --------------------
    if op_type == "Выдача наличных" and desc == "Выдача наличных (internal_report)":
        await queue_operation(
            target_chat_id, 
            op_type, 
            currency, 
            -amount, 
            desc
        )
        await message.reply_text(
            f"✅ [Internal Report] Cash Out\n"
            f"-{amount:,.2f} {currency}"
        )
        return

    # --------------------
    # КОНВЕРТАЦИЯ
    # --------------------
    if op_type == "Конвертация":
        rate = manual["rate"]
        to_curr = manual["to_currency"]

        if rate <= 0:
            await message.reply_text("❗ Курс должен быть больше 0", parse_mode=None)
            return

        # ✅ ФИКС = ОТКУП: фикс 140000 cny 11.4 rub
        # значит: +140000 CNY, - (140000 * 11.4) RUB
        if desc == "Фикс":
            pay_amount = round(amount * rate, 6)

            # покупаем валюту откупа
            await queue_operation(target_chat_id, "Конвертация", currency, amount, desc)

            # платим валютой оплаты
            await queue_operation(target_chat_id, "Конвертация", to_curr, -pay_amount, desc)
            return

        # -------------------------------------------------------
        # ❗ НЕ фикс: оставляем старую логику (как было у тебя)
        # -------------------------------------------------------
        to_amount = compute_conversion_to_amount(amount, rate, currency, to_curr)

        await queue_operation(target_chat_id, "Конвертация", currency, -amount, desc)
        await queue_operation(target_chat_id, "Конвертация", to_curr, to_amount, desc)
        return


    # --------------------
    # ПРОЧИЕ
    # --------------------
    sign = -1 if op_type in ("Выдача наличных", "Оплата ПП", "Комиссия") else 1

    await queue_operation(
        target_chat_id,
        op_type,
        currency,
        sign * amount,
        desc,
    )
