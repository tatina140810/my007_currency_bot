"""
Конфигурационный файл
"""
import os

# ВАЖНО: Замените на токен вашего бота от @BotFather
BOT_TOKEN = os.getenv("BOT_TOKEN", "")

# OpenAI API Key для ИИ-разбора текста
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

# Поддерживаемые валюты
CURRENCIES = ["USD", "EUR", "RUB", "CNY", "AED", "KGS", "USDT", "KZT"]

REPORT_CHAT_ID = int(os.getenv("REPORT_CHAT_ID", "-1001922337698"))

# Чат для системных уведомлений (SLA, AI Learning)
ADMIN_ALERT_CHAT_ID = int(os.getenv("ADMIN_ALERT_CHAT_ID", "-5093538782"))

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
