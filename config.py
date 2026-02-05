"""
Конфигурационный файл
"""

# ВАЖНО: Замените на токен вашего бота от @BotFather

BOT_TOKEN = "8555695431:AAF69crWuv8krwg95uCsjR2bYuYAv9ccwAw"

# Поддерживаемые валюты
CURRENCIES = ["USD", "EUR", "RUB", "CNY", "AED", "KGS", "USDT", "KZT",]
REPORT_CHAT_ID = -1001922337698   

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
ADMIN_PASSWORD = "123"

# Настройки базы данных
DATABASE_NAME = 'currency_operations.db'
