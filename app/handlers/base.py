from telegram import Update
from telegram.ext import ContextTypes
from app.core.logger import logger
from app.db.instance import db
from app.handlers.utils import get_chat_name, get_chat_id

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start"""
    user = update.effective_user
    chat = update.effective_chat
    
    if not user or not chat:
        return

    chat_name = get_chat_name(update)
    telegram_chat_name = chat.title or chat.first_name or f"Чат {chat.id}"
    db.register_chat(chat.id, telegram_chat_name, chat.type)

    base_text = f"""Добро пожаловать, {user.first_name}!

Текущий чат: {chat_name}

Команды:
/bal - Показать баланс
/his - История операций
/del - Удалить операцию (по паролю)
/ex - Экспорт в Excel
/help - Справка

Операции в чате (для сотрудников):
- Поступления: "... 1000,00 руб поступили ..."
- Взнос: "взнос наличными 5000 usd"
- Выдача: "выдача наличными 3000 usd"
- Возврат: "возврат 1000 usd"

В личном чате используй [ГРУППА]:
[УЗ] поступили 5000 usdt
"""
    await update.message.reply_text(base_text, parse_mode=None)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_name = get_chat_name(update)

    help_text = f"""📌 СПРАВКА ПО БОТУ
Чат: {chat_name}

━━━━━━━━━━━━━━━━━━
🔹 ОБЩИЕ КОМАНДЫ
/bal — Баланс текущего чата
/his — История операций (за сегодня)
/his 01.01.2025 — История за дату
/del — Удалить последнюю операцию (нужен пароль)
/help — Это меню

━━━━━━━━━━━━━━━━━━
📊 ОТЧЕТЫ
/rep — Текстовый отчет за смену
/ex — Экспорт операций в Excel (вся история)
/sum 01.01-31.01 — Сумма операций за период

━━━━━━━━━━━━━━━━━━
💼 КАССОВЫЙ ОТЧЕТ (Cash Report)
*Работает в личном чате*
/cash_open — Ввод утреннего остатка (Начало смены)
/cash_report — Скачать Excel отчет (Остаток + Приход - Расход)
/set_rate — Установить курс (для отчетов)
/cash_exchange — Внутренний обмен валют

━━━━━━━━━━━━━━━━━━
🔧 АДМИН / REPAIR
/fix — Пересчитать баланс текущего чата
/fix all — Пересчитать балансы ВСЕХ чатов (при сбоях)
/verify — Проверка целостности данных (Financial Audit)
/normalize — Исправить старые названия валют (RUB/KGS...)
/chats — Список всех групп
/clear — Очистка базы (ОПАСНО!)

━━━━━━━━━━━━━━━━━━
📝 РУЧНОЙ ВВОД (ПРИМЕРЫ)

1️⃣ Обычные операции (Банк/Нал):
• "Поступили 1000 RUB" (авто-распознавание)
• "Взнос наличными 5000 USD"
• "Выдача 2000 EUR"
• "Оплата ПП 1500 USD"

2️⃣ Ввод для Cash Report ([rep]):
*Покупка валюты (Buy FX):*
`[rep] 32000 EUR 91.8`
(Купили 32000 EUR по курсу 91.8 RUB)

*Продажа валюты (Sell FX):*
`[rep] 10000 USD 90.5`
(Купили 10000 USD за RUB)

*Выдача наличных:*
`[internal_report] наличные 5000 USD`

3️⃣ Работа с группами из лички:
`[ОФИС] взнос 100 USD`
`[УЗ] выдача 5000 KGS`
"""
    await update.message.reply_text(help_text, parse_mode="Markdown")


async def cancel_any(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /cancel"""
    if "pending_undo_op_id" in context.user_data:
        context.user_data.pop("pending_undo_op_id", None)
        context.user_data.pop("pending_undo_chat_id", None)
        await update.message.reply_text("Отменено", parse_mode=None)
        return
    await update.message.reply_text("Нечего отменять.", parse_mode=None)


async def handle_message_reaction(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сброс SLA таймера при реакции на сообщение от стаффа"""
    reaction = update.message_reaction
    if not reaction:
        return
        
    chat = reaction.chat
    user = reaction.user
    
    if not chat or not user:
        return
        
    chat_id = chat.id
    user_id = user.id
    
    from app.handlers.utils import is_staff
    
    # Если реакцию поставил стафф, обновляем SLA (как будто он ответил)
    if is_staff(user_id):
        logger.info(f"Staff {user_id} reacted to message in chat {chat_id}. Resetting SLA timer.")
        db.update_chat_sla(chat_id, is_staff=True)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ошибок"""
    logger.exception("Unhandled exception", exc_info=context.error)
