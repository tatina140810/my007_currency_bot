"""
Конфигурационный файл
"""
import os

# ВАЖНО: Замените на токен вашего бота от @BotFather
BOT_TOKEN = os.getenv("BOT_TOKEN", "8555695431:AAF69crWuv8krwg95uCsjR2bYuYAv9ccwAw")

# Поддерживаемые валюты
CURRENCIES = ["USD", "EUR", "RUB", "CNY", "AED", "KGS", "USDT", "KZT"]

REPORT_CHAT_ID = int(os.getenv("REPORT_CHAT_ID", "-1001922337698"))

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
DATABASE_NAME = os.getenv("DATABASE_NAME", "currency_operations.db")
DB_PATH = os.path.join(os.getcwd(), DATABASE_NAME)

# Настройки комиссий
COMMISSION_PERCENT = 0.01
BANK_REQUEST_FEE = 65.0
