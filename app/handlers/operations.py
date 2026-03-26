from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes
import asyncio

from app.core.logger import logger
from app.core.config import ADMIN_ALERT_CHAT_ID
from app.db.instance import db
from app.handlers.utils import get_chat_id, get_chat_name, is_staff, safe_reply
from app.services.parser import (
    extract_group_tag, normalize_group_name, looks_like_bank_income,
    parse_multiple_income_notifications, parse_bulk_pp_payments, parse_manual_operation_line,
    parse_implicit_conversion, parse_residual_balance, is_rate_message
)
from app.services.ai_parser import parse_with_ai
from app.services.operations import queue_operation, resolve_target_chat_id
from app.services.math import compute_conversion_to_amount

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):

    is_edited = bool(update.edited_message or update.edited_channel_post)
    message = update.edited_message or update.edited_channel_post or update.effective_message
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

    # SAVE TEXT FOR BACK_REPORT
    try:
        db.save_last_back_report_text(chat.id, text)
    except Exception as e:
        logger.error(f"Failed to save back_report text: {e}")

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

    # =====================================================
    # 💥 КАСТОМНЫЙ ПАРСИНГ ДЛЯ ГРУППЫ "Курсы, конветации, суммы"
    # =====================================================
    # =====================================================
    # 💥 КАСТОМНЫЙ ПАРСИНГ ДЛЯ ГРУППЫ "Курсы, конветации, суммы"
    # =====================================================
    from app.core.config import CONVERSION_GROUP_NAME
    
    # Tolerant match against spacing/casing, plus direct ID bypass
    if (chat.title and CONVERSION_GROUP_NAME.lower().replace(" ", "") in chat.title.lower().replace(" ", "")) or chat.id == -4032081164:
        logger.info(f"[CONVERSION_GROUP] Интерцепт сообщения из: {chat.title} (Edited: {is_edited})")
        
        # 1. Автоматический парсинг списков Оплата ПП (игнорируя staff)
        bulk = parse_bulk_pp_payments(clean_text)
        if bulk:
            if not is_edited:
                for item in bulk:
                    target_group = normalize_group_name(item["group"])
                    target_cht_id = db.get_chat_id_by_name(target_group)
                    if not target_cht_id:
                        continue

                    desc = f"{item['company']} | {item['receiver']}"
                    await queue_operation(
                        target_cht_id,
                        "Оплата ПП",
                        item["currency"],
                        -item["amount"],
                        desc,
                    )
            
            # Автоматическая выгрузка списка платежей во вкладку "Платежи"
            from app.services.parser import parse_back_report_payments
            from app.services.google_sheets import sync_payment_list_to_cassa_sheet
            import asyncio
            parsed_payments = parse_back_report_payments(clean_text, msg_id=message.message_id)
            if parsed_payments and parsed_payments.get("items"):
                asyncio.create_task(sync_payment_list_to_cassa_sheet(parsed_payments))
                
        # 2. Парсинг конвертаций для Внутреннего отчета кассы
        from app.services.parser_conversions import parse_group_conversions
        from app.services.google_sheets import sync_conversions_to_cassa_sheet
        import asyncio
        conversions = None
        if not getattr(message, "reply_to_message", None):
            conversions = parse_group_conversions(clean_text, msg_id=message.message_id)
        else:
            logger.info(f"[CONVERSION_GROUP] Сообщение является ответом (тег). Пропускаем парсинг конвертаций во избежание дубликатов.")

        if conversions:
            logger.info(f"[Operations] Intercepted {len(conversions)} conversion(s) in {chat.title}.")
            # Send to Cassa "конвертации"
            import json
            db_id = db.enqueue_sync_operation(chat.id, message.message_id, "conversions", json.dumps(conversions, default=str))
            asyncio.create_task(sync_conversions_to_cassa_sheet(conversions, db_id=db_id))
            
        # Полная тишина в группе (никаких ответов или проверок ИИ)
        return

    # FOR ALL OTHER FLOWS, STOP EDITED MESSAGES FROM DUPLICATING DATA
    if is_edited:
        logger.info(f"Ignored edited message from user {user.id} in chat {chat.id} because it is not from CONVERSION_GROUP")
        return

    # =====================================================
    # 💥 КАСТОМНЫЙ ПАРСИНГ ДЛЯ ГРУППЫ "Зак"
    # =====================================================
    if chat.title and "Зак" in chat.title:
        logger.info(f"[ZAK_GROUP] Интерцепт сообщения из: {chat.title}")
        
        # Use Forward Date if available
        msg_date_zak = message.date
        if getattr(message, "forward_origin", None):
             msg_date_zak = message.forward_origin.date
        elif getattr(message, "forward_date", None):
             msg_date_zak = message.forward_date
             
        from app.core.constants import KG_TZ
        msg_date_zak = msg_date_zak.astimezone(KG_TZ)
        
        from app.services.zak_parser import parse_zak_message
        from app.services.google_sheets_zak import append_zak_operations_to_sheet
        import asyncio
        
        is_reply = bool(getattr(message, "reply_to_message", None) and message.reply_to_message.text)
        has_percent = '%' in clean_text
        
        # Если это реплай на старое сообщение и сам реплай содержит процент, склеиваем их.
        if is_reply and has_percent:
            original_lines = message.reply_to_message.text.split('\n')
            combined_lines = [f"{line} {clean_text}" for line in original_lines]
            working_text = '\n'.join(combined_lines)
        else:
            working_text = clean_text
        
        parsed_zak = parse_zak_message(working_text, chat.id, message.message_id, msg_date_zak)
        
        # Строгий фильтр расходов убран, чтобы записывались все пополнения, снятия и 'живая речь'.
        # parsed_zak = [op for op in parsed_zak if op.get("percent_value", 0) > 0]
        
        if parsed_zak:
            logger.info(f"[Operations] Intercepted Zak operation: {parsed_zak}")
            import json
            db_id = db.enqueue_sync_operation(chat.id, message.message_id, "zak", json.dumps(parsed_zak, default=str))
            asyncio.create_task(append_zak_operations_to_sheet(parsed_zak, db_id=db_id))
            
        # Полная тишина в группе
        return

    # =====================================================
    # 💥 КАСТОМНЫЙ ПАРСИНГ ДЛЯ ГРУППЫ "ЗАПРОСЫ ПО ВХОД.СУММАМ И ДОКИ"
    # =====================================================
    from app.core.config import REPORT_CHAT_ID
    if chat.id == REPORT_CHAT_ID or (chat.title and "ЗАПРОСЫ ПО ВХОД" in chat.title.upper()):
        try:
            from app.services.zaprosy_parser import looks_like_bank_income_zaprosy, parse_zaprosy_incomes
            from app.services.google_sheets_zaprosy import append_zaprosy_operation_async, sync_zaprosy_to_sheet
            import asyncio
            from app.core.constants import KG_TZ
            
            logger.info(f"[ZAPROSY_GROUP] Интерцепт сообщения из: {chat.title}")
            
            chat_title_lower = chat.title.lower() if chat.title else ""
            if "запросы по вход" in chat_title_lower or "запросы" in chat_title_lower:
                import json
                
                # 1. Back_report list logic inside Zaprosy
                if "--- ПЛАТЕЖИ" in clean_text:
                    try:
                        from app.services.parser import parse_back_report_payments
                        from app.services.google_sheets import sync_payment_list_to_cassa_sheet
                        parsed_payments = parse_back_report_payments(clean_text, msg_id=message.message_id)
                        if parsed_payments and parsed_payments.get("items"):
                            logger.info(f"[Operations] Zaprosy group intercepted payment list, pushing to Платежи: {parsed_payments}")
                            db_id = db.enqueue_sync_operation(chat.id, message.message_id, "payments", json.dumps(parsed_payments, default=str))
                            asyncio.create_task(sync_payment_list_to_cassa_sheet(parsed_payments, db_id=db_id))
                    except Exception as e:
                        logger.error(f"[Operations] Error parsing Zaprosy /back_report: {e}")
                    return

            if looks_like_bank_income_zaprosy(clean_text):
                incomes = parse_zaprosy_incomes(clean_text)
                if incomes:
                    msg_date = getattr(message, "forward_origin", None) and getattr(message.forward_origin, "date", None)
                    if not msg_date:
                        msg_date = getattr(message, "forward_date", message.date)
                    msg_date = msg_date.astimezone(KG_TZ)
                    for inc in incomes:
                        inc["timestamp"] = msg_date

                    logger.info(f"[Operations] Intercepted Zaprosy Incomes: {incomes}")
                    import json
                    db_id = db.enqueue_sync_operation(chat.id, message.message_id, "zaprosy", json.dumps(incomes, default=str))
                    from app.services.google_sheets_zaprosy import sync_zaprosy_to_sheet
                    asyncio.create_task(sync_zaprosy_to_sheet(incomes, message.message_id, db_id=db_id))
                    return
            
        except Exception as zaprosy_err:
            logger.error(f"[ZAPROSY_GROUP] Critical isolation exception protected main thread: {zaprosy_err}")
            
        # Не отвечаем и не шлем в ИИ
        return

    # 5️⃣ АВТО-ПОСТУПЛЕНИЯ (БАНК)
    if looks_like_bank_income(clean_text):
        logger.info(f"[AUTO_INCOME] matched: chat={chat.id}")

        incomes = parse_multiple_income_notifications(clean_text)
        if not incomes:
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
                    await safe_reply(message, 
                        "❗ В личном чате укажи группу ПЕРЕД сообщением.\n"
                        "Пример:\n[УЗ] поступили 5000 usdt"
                    )
                    return
                target_chat_id = db.get_chat_id_by_name(group_name)
                if not target_chat_id:
                    await safe_reply(message, f"❌ Группа '{group_name}' не найдена")
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

        # FIX: Convert to KG_TZ (Asia/Bishkek) to ensure correct date (avoid UTC prev day issue)
        from app.core.constants import KG_TZ
        msg_date = msg_date.astimezone(KG_TZ)

        for cnt, income in enumerate(incomes):
            # Вставляем искусственную задержку в миллисекунду, 
            # чтобы ID транзакции (генерация по timestamp) не совпадали при массовом запуске
            import time
            time.sleep(0.001)
            
            await queue_operation(
                target_chat_id,
                "Поступление",
                income["currency"],
                income["amount"],
                income["description"],
                timestamp=msg_date
            )

            logger.info(
                f"[AUTO_INCOME] queued receipt {cnt+1}/{len(incomes)}: {income['amount']} {income['currency']} -> chat {target_chat_id} date={msg_date}"
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
            if is_private:
                from app.services.parser import parse_back_report_payments
                from app.services.google_sheets import sync_payment_list_to_cassa_sheet
                import asyncio
                
                parsed_payments = parse_back_report_payments(clean_text)
                if parsed_payments and parsed_payments.get("items"):
                    asyncio.create_task(sync_payment_list_to_cassa_sheet(parsed_payments))
                    await safe_reply(message, "✅ Bulk платежи обработаны и выгружены в таблицу Платежи")
                else:
                    await safe_reply(message, "✅ Bulk платежи обработаны")
            return

    # =====================================================
    # 4️⃣ РУЧНЫЕ ОПЕРАЦИИ (Regular + AI Fallback)
    # =====================================================
    
    # ⚖️ RESIDUAL BALANCE INTERCEPT (Синхронизация по остаткам)
    # We want this to run BEFORE the staff check so anyone can report a balance
    residual = parse_residual_balance(clean_text)
    if residual:
        try:
            target_chat_id = resolve_target_chat_id(
                chat=chat,
                is_private=is_private,
                group_from_manual=group_name,
            )
        except ValueError as e:
            await safe_reply(message, str(e))
            return

        # Strip the residual declaration from the text so AI doesn't parse it as a transaction
        import re
        clean_text = re.sub(r"(?i)ост(?:аток)?\s*-?[\d\s.,]+\s*[a-zа-я$€¥]{0,8}|-?[\d\s.,]+\s*[a-zа-я$€¥]{0,8}\s*ост(?:аток)?", "", clean_text).strip()
        
        rep_amount = residual["amount"]
        rep_currency = residual["currency"]
        
        async def verify_residual_background():
            # Wait for any operations in this specific message to queue and save (batch runs every 0.5s)
            await asyncio.sleep(1.5)
            
            # Fetch current balance
            current_balances = db.get_balances(target_chat_id)
            actual_balance = current_balances.get(rep_currency, 0.0)
            
            diff = round(rep_amount - actual_balance, 2)
            
            if diff == 0:
                if is_private:
                    await context.bot.send_message(chat.id, f"✅ Остаток сходится. Баланс {rep_amount:,.2f} {rep_currency}.")
                return
            
            # Discrepancy detected, prepare inline keyboard for Admin
            chat_name_display = group_name if group_name else (chat.title or f"ID {target_chat_id}")
            
            if ADMIN_ALERT_CHAT_ID:
                cb_base = f"sync_bal_{target_chat_id}_{rep_currency}_{diff}"
                keyboard = [
                    [InlineKeyboardButton(f"📈 Поступление (+{diff:,.2f})", callback_data=f"{cb_base}_inc")],
                    [InlineKeyboardButton(f"📥 Взнос (+{diff:,.2f})", callback_data=f"{cb_base}_dep")],
                    [InlineKeyboardButton(f"💸 Выдача ({diff:,.2f})", callback_data=f"{cb_base}_exp")],
                    [InlineKeyboardButton(f"💳 Оплата ПП ({diff:,.2f})", callback_data=f"{cb_base}_pay")],
                    [InlineKeyboardButton(f"🔄 Конвертация ({diff:,.2f})", callback_data=f"{cb_base}_cnv")],
                    [InlineKeyboardButton(f"🏦 Комиссия Харбор ({diff:,.2f})", callback_data=f"{cb_base}_hrb")],
                    [InlineKeyboardButton(f"📝 Заявление на корректировку ({diff:,.2f})", callback_data=f"{cb_base}_adj")],
                    [InlineKeyboardButton(f"🧾 Комиссия за услуги ({diff:,.2f})", callback_data=f"{cb_base}_fee")],
                    [InlineKeyboardButton("🗑 Игнорировать", callback_data=f"{cb_base}_ign")]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)
                alert_text = (
                    f"⚠️ <b>Расхождение остатка!</b>\n"
                    f"Группа: {chat_name_display}\n"
                    f"Заявлено: {rep_amount:,.2f} {rep_currency}\n"
                    f"По таблице: {actual_balance:,.2f} {rep_currency}\n"
                    f"<b>Разница:</b> {diff:,.2f} {rep_currency}\n\n"
                    f"Куда отнести эту разницу?"
                )
                try:
                    await context.bot.send_message(
                        chat_id=ADMIN_ALERT_CHAT_ID,
                        text=alert_text,
                        reply_markup=reply_markup,
                        parse_mode="HTML"
                    )
                    if is_private:
                        await context.bot.send_message(chat.id, "⏳ Расхождение остатка отправлено админу на проверку.")
                except Exception as e:
                    logger.error(f"Failed to send balance sync alert: {e}")

        # Fire and forget the verification background task
        asyncio.create_task(verify_residual_background())
        
        # If the text was exclusively a balance check (meaning after stripping, it's empty) -> Stop processing
        if not clean_text:
            return

    if not staff:
        return

    manual = parse_manual_operation_line(clean_text)
    
    if manual:
        # Standard strict parsing flow
        try:
            target_chat_id = resolve_target_chat_id(
                chat=chat,
                is_private=is_private,
                group_from_manual=manual.get("group") or group_name,
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
                await safe_reply(message, str(e))
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

        # 🤖 IMPLICIT INTERCEPT
        implicit = parse_implicit_conversion(clean_text, reply_context)
        if implicit:
            try:
                target_chat_id = resolve_target_chat_id(
                    chat=chat,
                    is_private=is_private,
                    group_from_manual=implicit.get("group") or group_name,
                )
            except ValueError as e:
                await safe_reply(message, str(e))
                return
            
            implicit_amount = implicit["amount"]
            implicit_rate = implicit["rate"]
            implicit_currency = implicit["currency"]
            implicit_to_curr = implicit["to_currency"]
            
            pay_amount = round(implicit_amount * implicit_rate, 6)

            # покупаем валюту откупа
            await queue_operation(target_chat_id, "Конвертация", implicit_currency, implicit_amount, implicit["description"])

            # платим валютой оплаты
            await queue_operation(target_chat_id, "Конвертация", implicit_to_curr, -pay_amount, implicit["description"])
            
            if is_private:
                await safe_reply(message, 
                    f"✅ Фикс обработан\n"
                    f"+{implicit_amount:,.2f} {implicit_currency}\n"
                    f"-{pay_amount:,.2f} {implicit_to_curr}"
                )
            return

        # 🤖 ИНТЕРЦЕПТ СООБЩЕНИЙ-КУРСОВ (чтобы не уходили в AI или в "Операция не распознана")
        if is_rate_message(clean_text):
            if is_private:
                await safe_reply(message, "✅ Курс учтен (для конвертации отправьте ответным сообщением сумму)")
            return

        # 🤖 AI PARSING FALLBACK
        ai_parsed_list = await parse_with_ai(clean_text, reply_context)
        
        if not ai_parsed_list:
            # AI Failed to parse. Let's check if it even looks like a financial op (has numbers)
            from app.services.parser import is_date_or_doc_number
            if any(char.isdigit() for char in clean_text) and not is_date_or_doc_number(clean_text):
                pending_id = db.save_pending_operation(
                    chat_id=chat.id, 
                    message_id=message.message_id, 
                    text=clean_text, 
                    reply_context=reply_context
                )
                
                if pending_id and ADMIN_ALERT_CHAT_ID:
                    admin_id = ADMIN_ALERT_CHAT_ID
                    keyboard = [
                        [InlineKeyboardButton("✅ Приход", callback_data=f"ai_learn_income_{pending_id}"),
                         InlineKeyboardButton("💳 Оплата ПП", callback_data=f"ai_learn_payment_{pending_id}")],
                        [InlineKeyboardButton("💸 Выдача", callback_data=f"ai_learn_withdraw_{pending_id}"),
                         InlineKeyboardButton("💰 Взнос", callback_data=f"ai_learn_deposit_{pending_id}")],
                        [InlineKeyboardButton("🔄 Конвертация", callback_data=f"ai_learn_cnv_{pending_id}"),
                         InlineKeyboardButton("🏦 Харбор", callback_data=f"ai_learn_hrb_{pending_id}")],
                        [InlineKeyboardButton("📝 Корректировка", callback_data=f"ai_learn_adj_{pending_id}"),
                         InlineKeyboardButton("🧾 За услуги", callback_data=f"ai_learn_fee_{pending_id}")],
                        [InlineKeyboardButton("🗑 Пропустить (Не операция)", callback_data=f"ai_learn_ignore_{pending_id}")]
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    
                    try:
                        chat_name = group_name if group_name else (chat.title or "Личный")
                        alert_text = (
                            f"🤖 <b>Не распознано:</b>\n"
                            f"Чат: {chat_name}\n"
                            f"Текст: <i>{clean_text}</i>\n\n"
                            f"Пожалуйста, укажите тип операции, чтобы обучить ИИ:"
                        )
                        await context.bot.send_message(
                            chat_id=admin_id,
                            text=alert_text,
                            reply_markup=reply_markup,
                            parse_mode="HTML"
                        )
                    except Exception as e:
                        logger.error(f"Failed to send pending AI operation to {admin_id}: {e}")
            
            return  # Stop processing for this message
            
        success_messages = []
        
        for ai_op in ai_parsed_list:
            op_type = ai_op["type"]
            currency = ai_op["currency"]
            amount = ai_op["amount"]
            desc = f"[AI] {ai_op.get('description', '')}".strip()
            
            # Inherit the group_name we successfully extracted with regex above if the AI didn't find one
            extracted_ai_group = ai_op.get("group")
            final_group = extracted_ai_group if extracted_ai_group else group_name
            
            # 1. Enforce Privacy Rule for 'Оплата ПП'
            if op_type == "Оплата ПП" and not is_private:
                logger.warning(f"AI attempted to parse 'Оплата ПП' in group chat {chat.id}. Proceeding anyway as fallback fallback.")
                # Removed the 'continue' here so that it proceeds instead of skipping
            
            # 2. Resolve target chat
            try:
                target_chat_id = resolve_target_chat_id(
                    chat=chat,
                    is_private=is_private,
                    group_from_manual=final_group,
                )
            except ValueError as e:
                # If AI fails to determine chat context, we must skip this operation
                await safe_reply(message, f"⚠️ ИИ-ошибка для {op_type}: {str(e)}")
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
            if chat.type in ["group", "supergroup"]:
                logger.info("Successfully parsed AI text in group, remaining silent per request. Recorded ops.")
            else:
                await safe_reply(message, 
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
        await safe_reply(message, 
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
        if is_private:
            await safe_reply(message, 
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
            if is_private:
                await safe_reply(message, "❗ Курс должен быть больше 0", parse_mode=None)
            return

        # ✅ ФИКС = ОТКУП: фикс 140000 cny 11.4 rub
        # значит: +140000 CNY, - (140000 * 11.4) RUB
        if desc == "Фикс":
            pay_amount = round(amount * rate, 6)

            # покупаем валюту откупа
            await queue_operation(target_chat_id, "Конвертация", currency, amount, desc)

            # платим валютой оплаты
            await queue_operation(target_chat_id, "Конвертация", to_curr, -pay_amount, desc)
            
            if is_private:
                await safe_reply(message, 
                    f"✅ Фикс обработан\n"
                    f"+{amount:,.2f} {currency}\n"
                    f"-{pay_amount:,.2f} {to_curr}"
                )
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

    if is_private:
        await safe_reply(message, f"✅ {op_type} обработан: {amount:,.2f} {currency}")
