#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Экспорт данных из базы в CSV.

Делает 3 файла:
  • operations_YYYYMMDD_HHMMSS.csv  – все операции по всем чатам (включая SWIFT)
  • balances_YYYYMMDD_HHMMSS.csv   – балансы по всем чатам
  • statistics_YYYYMMDD_HHMMSS.csv – статистика (приход / расход / баланс) по всем чатам

Открывается в Excel / Google Sheets (разделитель ; и utf-8-sig для русских букв).
"""

import csv
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from database import Database
from config import CURRENCIES

KG_TZ = ZoneInfo("Asia/Bishkek")


# ---------- ВСПОМОГАТЕЛЬНОЕ: парсер времени с переводом в KG_TZ ----------

def parse_timestamp(ts):
    """
    Разбираем timestamp из БД и переводим в Asia/Bishkek.
    Поддерживаем форматы:
      • 'YYYY-MM-DD HH:MM:SS'
      • 'YYYY-MM-DD HH:MM'
      • 'YYYY-MM-DDTHH:MM:SS'
      • 'DD.MM.YYYY HH:MM'
      • 'DD.MM.YYYY HH:MM:SS'
    Если формат не подошёл — берём текущее время в KG_TZ.
    """
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(KG_TZ)

    if not ts:
        return datetime.now(KG_TZ)

    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%d.%m.%Y %H:%M",
        "%d.%m.%Y %H:%M:%S",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(ts, fmt)
            dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(KG_TZ)
        except ValueError:
            continue

    return datetime.now(KG_TZ)


# ---------- ЭКСПОРТ ОПЕРАЦИЙ (со SWIFT) ----------

def export_operations_to_csv(db: Database, filename: str):
    """
    Экспорт всех операций по всем чатам в один CSV.

    Колонки:
      Chat ID
      Chat Name
      Operation ID
      Type
      Currency
      Amount
      SWIFT fee (USD)
      Description
      Datetime (KG)
    """
    chats = db.get_all_chats()
    if not chats:
        print("⚠️  Нет чатов / операций для экспорта")
        return

    total_rows = 0

    with open(filename, "w", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")

        # Заголовок
        writer.writerow(
            [
                "Chat ID",
                "Chat Name",
                "Operation ID",
                "Type",
                "Currency",
                "Amount",
                "SWIFT fee (USD)",
                "Description",
                "Datetime (KG)",
            ]
        )

        for chat_id, chat_name, chat_type, first_interaction, last_interaction in chats:
            chat_name = chat_name or f"Чат {chat_id}"

            # Берём все операции по этому чату
            ops = db.get_operations(chat_id, limit=10000)
            # отсортируем по времени от старых к новым
            ops_sorted = sorted(ops, key=lambda op: parse_timestamp(op[5]))

            for op in ops_sorted:
                # Возможные варианты:
                #  - (id, type, currency, amount, description, timestamp)
                #  - (id, type, currency, amount, description, timestamp, swift_fee)
                if len(op) >= 7:
                    op_id, op_type, currency, amount, description, timestamp, swift_fee = op
                else:
                    op_id, op_type, currency, amount, description, timestamp = op
                    swift_fee = 0.0

                dt_local = parse_timestamp(timestamp)
                dt_str = dt_local.strftime("%d.%m.%Y %H:%M:%S")

                # SWIFT всегда в USD, по твоему условию
                swift_fee_usd = float(swift_fee) if swift_fee else 0.0

                writer.writerow(
                    [
                        chat_id,
                        chat_name,
                        op_id,
                        op_type,
                        currency,
                        f"{amount:.2f}",
                        f"{swift_fee_usd:.2f}",
                        description or "",
                        dt_str,
                    ]
                )
                total_rows += 1

    print(f"✅ Экспортировано операций: {total_rows}")
    print(f"📁 Файл операций: {filename}")


# ---------- ЭКСПОРТ БАЛАНСОВ ----------

def export_balances_to_csv(db: Database, filename: str):
    """
    Экспорт балансов по всем чатам.

    Колонки:
      Chat ID
      Chat Name
      Currency
      Balance
    """
    chats = db.get_all_chats()
    if not chats:
        print("⚠️  Нет чатов для экспорта балансов")
        return

    total_rows = 0

    with open(filename, "w", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")

        # Заголовок
        writer.writerow(["Chat ID", "Chat Name", "Currency", "Balance"])

        for chat_id, chat_name, chat_type, first_interaction, last_interaction in chats:
            chat_name = chat_name or f"Чат {chat_id}"
            balances = db.get_balances(chat_id)

            for curr in CURRENCIES:
                balance = balances.get(curr, 0.0)
                if balance == 0:
                    continue

                writer.writerow(
                    [
                        chat_id,
                        chat_name,
                        curr,
                        f"{balance:.2f}",
                    ]
                )
                total_rows += 1

    print(f"✅ Экспортировано балансов: {total_rows}")
    print(f"📁 Файл балансов: {filename}")


# ---------- ЭКСПОРТ СТАТИСТИКИ ----------

def export_statistics_to_csv(db: Database, filename: str):
    """
    Экспорт статистики по всем чатам.

    Колонки:
      Chat ID
      Chat Name
      Currency
      Income
      Expense
      Balance
    """
    chats = db.get_all_chats()
    if not chats:
        print("⚠️  Нет чатов для экспорта статистики")
        return

    total_rows = 0

    with open(filename, "w", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.writer(csvfile, delimiter=";")

        # Заголовок
        writer.writerow(
            [
                "Chat ID",
                "Chat Name",
                "Currency",
                "Income",
                "Expense",
                "Balance",
            ]
        )

        for chat_id, chat_name, chat_type, first_interaction, last_interaction in chats:
            chat_name = chat_name or f"Чат {chat_id}"
            stats = db.get_statistics(chat_id)

            for curr, data in stats.items():
                writer.writerow(
                    [
                        chat_id,
                        chat_name,
                        curr,
                        f"{data.get('income', 0.0):.2f}",
                        f"{data.get('expense', 0.0):.2f}",
                        f"{data.get('balance', 0.0):.2f}",
                    ]
                )
                total_rows += 1

    print(f"✅ Экспортирована статистика: {total_rows}")
    print(f"📁 Файл статистики: {filename}")


# ---------- MAIN ----------

def main():
    print("📊 Экспорт данных из базы")
    print("=" * 50)
    print()

    db = Database()

    # Имя файлов с датой/временем
    ts = datetime.now(KG_TZ).strftime("%Y%m%d_%H%M%S")

    ops_file = f"operations_{ts}.csv"
    bal_file = f"balances_{ts}.csv"
    stat_file = f"statistics_{ts}.csv"

    # Экспорт операций
    print("1️⃣  Экспорт операций...")
    export_operations_to_csv(db, ops_file)
    print()

    # Экспорт балансов
    print("2️⃣  Экспорт балансов...")
    export_balances_to_csv(db, bal_file)
    print()

    # Экспорт статистики
    print("3️⃣  Экспорт статистики...")
    export_statistics_to_csv(db, stat_file)
    print()

    print("🎉 Экспорт завершён!")
    print("Файлы можно открыть в Excel или Google Sheets.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
