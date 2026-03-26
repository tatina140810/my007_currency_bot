import asyncio
import logging
from datetime import datetime, date
from telegram.ext import Application
from app.db.instance import db
from app.core.config import ADMIN_ALERT_CHAT_ID

logger = logging.getLogger(__name__)

# Защита от спама:
# {chat_id: {"last_alert": datetime, "today_count": int, "today_date": date}}
_alerted_chats: dict = {}

# Рабочие часы (KG время UTC+6). Вне этих часов SLA-алерты не шлются.
QUIET_HOUR_START = 0   # 00:00
QUIET_HOUR_END   = 9   # 09:00
MAX_ALERTS_PER_DAY = 3  # Максимум алертов в сутки на один чат
ALERT_INTERVAL_MINUTES = 15  # Минимум между двумя алертами

async def sla_monitor_task(application: Application):
    """
    Фоновая задача для проверки SLA.
    Запускается каждую минуту и проверяет зависшие запросы от клиентов.
    Тихий режим: с 00:00 до 09:00 по KG (+06:00) алерты не отправляются.
    """
    threshold_minutes = 15
    admin_id = ADMIN_ALERT_CHAT_ID

    if not admin_id:
        logger.warning("ADMIN_ALERT_CHAT_ID is empty. Nowhere to send SLA alerts.")
        return

    while True:
        try:
            await asyncio.sleep(60)

            breaches = db.get_sla_breaches(threshold_minutes)
            if not breaches:
                continue

            now = datetime.now()
            current_hour = now.hour
            today = now.date()

            # Тихий режим: не беспокоить ночью
            if QUIET_HOUR_START <= current_hour < QUIET_HOUR_END:
                continue

            for breach in breaches:
                chat_id = breach['chat_id']
                chat_name = breach['chat_name'] or f"ID {chat_id}"

                state = _alerted_chats.get(chat_id, {})

                # Сбрасываем счётчик при наступлении нового дня
                if state.get("today_date") != today:
                    state = {"last_alert": None, "today_count": 0, "today_date": today}

                # Суточный лимит
                if state.get("today_count", 0) >= MAX_ALERTS_PER_DAY:
                    continue

                # Интервал между алертами
                last_alert = state.get("last_alert")
                if last_alert:
                    diff_minutes = (now - last_alert).total_seconds() / 60
                    if diff_minutes < ALERT_INTERVAL_MINUTES:
                        continue

                message = (
                    f"🚨 <b>Внимание: SLA breached!</b>\n\n"
                    f"Группа: <b>{chat_name}</b>\n"
                    f"Клиент ждет ответа уже более {threshold_minutes} минут!\n"
                    f"Пожалуйста, проверьте чат.\n\n"
                    f"<i>(Алерт {state.get('today_count', 0) + 1}/{MAX_ALERTS_PER_DAY} за сегодня)</i>"
                )

                try:
                    await application.bot.send_message(
                        chat_id=admin_id,
                        text=message,
                        parse_mode="HTML"
                    )
                    logger.info(f"SLA alert sent to {admin_id} for chat {chat_name}")
                    state["last_alert"] = now
                    state["today_count"] = state.get("today_count", 0) + 1
                    state["today_date"] = today
                    _alerted_chats[chat_id] = state
                except Exception as e:
                    logger.error(f"Failed to send SLA alert to {admin_id}: {e}")

        except asyncio.CancelledError:
            logger.info("SLA Monitor task cancelled")
            break
        except Exception as e:
            logger.error(f"Error in sla_monitor_task: {e}")

