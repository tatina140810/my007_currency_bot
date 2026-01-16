# auto_reply_bot.py

import logging
from datetime import datetime, time, date
from zoneinfo import ZoneInfo

KG_TZ = ZoneInfo("Asia/Bishkek")

# ID сотрудников (не получают автоответы)
TEAM_MEMBER_IDS = {
    6965593654, 6183345984, 7442420784,
    6139834526, 6143216960, 5706367013,
    7400447742, 6493433795, 1127930513, 624793227, 7155382863,
}

# chat_id -> date (когда уже отправляли автоответ в этот чат)
last_auto_reply_dates: dict[int, date] = {}

AUTO_REPLY_TEXT = (
    "Здравствуйте!\n"
    "Благодарим за обращение в нашу компанию.\n\n"
    "Ваше сообщение получено. Мы ответим на него "
    "в ближайшее рабочее время.\n\n"
    "График работы:\n"
    "Понедельник – Пятница\n"
    "09:30 – 21:00 (время Бишкека, GMT+6)\n"
    "06:30 – 18:00 (московское время, GMT+3)\n\n"
    "Выходные дни: суббота, воскресенье и праздники.\n\n"
    "С уважением,\n"
    "Команда поддержки"
)

# Праздничное авто-сообщение (активно ТОЛЬКО 31.12.2025–11.01.2026 включительно)
NEW_YEAR_TEXT = (
    "Доброго времени суток!\n"
    "Информируем вас о графике работы в праздничный период:\n"
    "• С 31 декабря 2025 года по 11 января 2026 года (включительно) — выходные и нерабочие дни.\n"
    "• 12 января 2026 года — первый рабочий день в новом году.\n"
    "✨ От всей души благодарим вас за доверие и сотрудничество в уходящем году.\n"
    "Желаем вам спокойных и тёплых праздников, приятного отдыха, новых сил и вдохновения! "
    "Пусть наступающий год принесёт стабильность, удачные сделки и только хорошие новости.\n"
    "С уважением,\n"
    "Команда поддержки"
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def _to_kg(now: datetime) -> datetime:
    """Приводим datetime к Asia/Bishkek."""
    if now.tzinfo is None:
        return now.replace(tzinfo=KG_TZ)
    return now.astimezone(KG_TZ)


def is_new_year_holidays(now: datetime) -> bool:
    """
    Новогодние выходные: 31.12.2025–11.01.2026 (включительно).
    Только в эти даты включаем праздничный автоответ.
    """
    now = _to_kg(now)
    d = now.date()
    return date(2025, 12, 31) <= d <= date(2026, 1, 11)


def is_working_time(now: datetime) -> bool:
    """
    Проверка, рабочее ли сейчас время (по Бишкеку).
    Рабочие дни: Пн–Пт
    Время: 09:30–21:00 по Бишкеку.
    """
    now = _to_kg(now)
    weekday = now.weekday()  # 0 = Пн, 6 = Вс
    current_time = now.time()

    # Сб/Вс
    if weekday >= 5:
        return False

    # 09:30–21:00
    return time(7, 30) <= current_time < time(21, 0)


def should_send_auto_reply(chat_id: int, now: datetime) -> bool:
    """Автоответ не чаще 1 раза в день на один чат."""
    now = _to_kg(now)
    today = now.date()
    return last_auto_reply_dates.get(chat_id) != today


def mark_auto_replied(chat_id: int, now: datetime) -> None:
    """Запоминаем, что сегодня в этот чат уже отправляли автоответ."""
    now = _to_kg(now)
    last_auto_reply_dates[chat_id] = now.date()


async def maybe_auto_reply(update, context) -> bool:
    """
    Вызывай в начале handle_text / handle_photo.
    Логика:
    - 31.12.2025–11.01.2026: отправляем NEW_YEAR_TEXT в ЛЮБОЕ время суток (не чаще 1 раза в день на чат)
    - В остальные дни: отправляем обычный AUTO_REPLY_TEXT только вне рабочего времени
    """
    try:
        message = getattr(update, "effective_message", None)
        user = getattr(update, "effective_user", None)
        chat = getattr(update, "effective_chat", None)

        if not message or not user or not chat:
            return False

        # сотрудникам не отвечаем
        if user.id in TEAM_MEMBER_IDS:
            return False

        now = datetime.now(KG_TZ)

        # не чаще 1 раза в день на чат
        if not should_send_auto_reply(chat.id, now):
            return False

        # 1) Праздники: отвечаем всегда (даже днем), но только до 12 января
        if is_new_year_holidays(now):
            await message.reply_text(NEW_YEAR_TEXT)
            mark_auto_replied(chat.id, now)
            logger.info(f"🎄 NEW YEAR автоответ отправлен: chat_id={chat.id} user_id={user.id}")
            return True

        # 2) В остальные дни: только в нерабочее время
        if is_working_time(now):
            return False

        await message.reply_text(AUTO_REPLY_TEXT)
        mark_auto_replied(chat.id, now)
        logger.info(f"✅ Автоответ отправлен: chat_id={chat.id} user_id={user.id}")
        return True

    except Exception:
        logger.exception("❌ Ошибка в maybe_auto_reply")
        return False
