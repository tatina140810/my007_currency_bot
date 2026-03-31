"""
Конфигурационный файл
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ВАЖНО: Замените на токен вашего бота от @BotFather
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

# OpenAI API Key для ИИ-разбора текста
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# Поддерживаемые валюты
CURRENCIES = ["USD", "EUR", "RUB", "CNY", "AED", "KGS", "USDT", "KZT"]

REPORT_CHAT_ID = int(os.getenv("REPORT_CHAT_ID", "-1001922337698"))

# Чат для системных уведомлений (SLA, AI Learning)
# Temporarily disabled per user request to stop alert spam
# ADMIN_ALERT_CHAT_ID = int(os.getenv("ADMIN_ALERT_CHAT_ID", "-5093538782"))
ADMIN_ALERT_CHAT_ID = None

CONVERSION_GROUP_NAME = "Курсы, конвертации,суммы"
CASSA_SPREADSHEET_ID = "1-_LgK8ZNty16hGUyHhnJ20heGM20GbPUW-RnMyLTJO4"

# Типы операций
OPERATION_TYPES = [
    "Поступление",
    "Конвертация",
    "Оплата ПП",
    "Возврат по ПП",
    "Выдача наличных",
    "Взнос наличными",
    "SWIFT",
    "Комиссия 1%",
    "Запрос банку",
]

ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "123")

# Настройки базы данных
DATABASE_NAME = os.getenv("DATABASE_NAME", "operations.db")
DB_PATH = os.path.join(os.getcwd(), DATABASE_NAME)

# Настройки комиссий
COMMISSION_PERCENT = 0.01
BANK_REQUEST_FEE = 65.0

# Группа «Зак» → «Проценты_детально»
# ZAK_BUFFER_MESSAGES: сохранять все сырые сообщения за день в БД (для вечернего разбора).
ZAK_BUFFER_MESSAGES = os.getenv("ZAK_BUFFER_MESSAGES", "1") not in ("0", "false", "False")
# ZAK_DEFER_SHEET_TO_EVENING: не писать в лист сразу; разовая выгрузка перед fill_report (вечерний Excel / 23:00).
ZAK_DEFER_SHEET_TO_EVENING = os.getenv("ZAK_DEFER_SHEET_TO_EVENING", "0") in ("1", "true", "True")
# ZAK_EVENING_USE_AI: при вечернем flush использовать OpenAI по полному дневному транскрипту; иначе — построчный regex.
ZAK_EVENING_USE_AI = os.getenv("ZAK_EVENING_USE_AI", "1") not in ("0", "false", "False")
