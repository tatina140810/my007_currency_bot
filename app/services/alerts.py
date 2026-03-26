import asyncio
from telegram import Bot
from app.core.config import BOT_TOKEN, ADMIN_ALERT_CHAT_ID
from app.core.logger import logger

async def send_system_alert(message: str):
    """
    Sends an emergency alert to the Tech Chat (ADMIN_ALERT_CHAT_ID).
    Used for reporting persistent Google Sheets sync failures or other critical issues.
    """
    try:
        bot = Bot(token=BOT_TOKEN)
        full_msg = f"⚠️ **SYSTEM ALERT** ⚠️\n\n{message}"
        await bot.send_message(chat_id=ADMIN_ALERT_CHAT_ID, text=full_msg, parse_mode="Markdown")
        logger.info(f"[Alerts] System alert sent to {ADMIN_ALERT_CHAT_ID}")
    except Exception as e:
        logger.error(f"[Alerts] Failed to send system alert: {e}")

def send_system_alert_sync(message: str):
    """Sync wrapper for use in background threads."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.run_coroutine_threadsafe(send_system_alert(message), loop)
        else:
            asyncio.run(send_system_alert(message))
    except Exception:
        # Fallback if no loop is available
        pass
