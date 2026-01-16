#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тестовый скрипт для проверки работы базы данных
"""

from database import Database
from config import CURRENCIES

def test_database():
    """Тестирование функций базы данных"""
    print("🧪 Запуск тестов базы данных...\n")
    
    # Создаем экземпляр базы данных
    db = Database('test_currency_operations.db')
    
    print("✅ База данных создана")
    
    # Очищаем данные
    db.clear_all_data()
    print("✅ Данные очищены")
    
    # Тест 1: Добавление поступления
    print("\n📝 Тест 1: Добавление поступления USD")
    op_id = db.add_operation('Поступление', 'USD', 1000.0, 'Тестовое поступление')
    print(f"   Операция добавлена с ID: {op_id}")
    balance = db.get_balance('USD')
    print(f"   Баланс USD: {balance}")
    assert balance == 1000.0, "Ошибка: баланс должен быть 1000"
    print("   ✅ Тест пройден")
    
    # Тест 2: Добавление расхода
    print("\n📝 Тест 2: Добавление оплаты USD")
    db.add_operation('Оплата ПП', 'USD', -300.0, 'Тестовая оплата')
    balance = db.get_balance('USD')
    print(f"   Баланс USD после оплаты: {balance}")
    assert balance == 700.0, "Ошибка: баланс должен быть 700"
    print("   ✅ Тест пройден")
    
    # Тест 3: Конвертация
    print("\n📝 Тест 3: Конвертация USD -> RUB")
    db.add_operation('Конвертация', 'USD', -100.0, 'Конвертация в рубли')
    db.add_operation('Конвертация', 'RUB', 9500.0, 'Конвертация в рубли')
    balance_usd = db.get_balance('USD')
    balance_rub = db.get_balance('RUB')
    print(f"   Баланс USD: {balance_usd}")
    print(f"   Баланс RUB: {balance_rub}")
    assert balance_usd == 600.0, "Ошибка: баланс USD должен быть 600"
    assert balance_rub == 9500.0, "Ошибка: баланс RUB должен быть 9500"
    print("   ✅ Тест пройден")
    
    # Тест 4: Получение всех балансов
    print("\n📝 Тест 4: Получение всех балансов")
    balances = db.get_balances()
    print("   Балансы:")
    for currency, balance in balances.items():
        if balance != 0:
            print(f"      {currency}: {balance}")
    print("   ✅ Тест пройден")
    
    # Тест 5: История операций
    print("\n📝 Тест 5: Получение истории операций")
    operations = db.get_operations(limit=5)
    print(f"   Найдено операций: {len(operations)}")
    for op in operations:
        op_id, op_type, currency, amount, description, timestamp = op
        print(f"      {op_type} {currency} {amount:,.2f}")
    assert len(operations) == 4, "Ошибка: должно быть 4 операции"
    print("   ✅ Тест пройден")
    
    # Тест 6: Статистика
    print("\n📝 Тест 6: Получение статистики")
    stats = db.get_statistics()
    print("   Статистика:")
    for currency, data in stats.items():
        print(f"      {currency}:")
        print(f"         Поступления: +{data['income']:,.2f}")
        print(f"         Расходы: {data['expense']:,.2f}")
        print(f"         Баланс: {data['balance']:,.2f}")
    print("   ✅ Тест пройден")
    
    # Тест 7: Пересчет балансов
    print("\n📝 Тест 7: Пересчет балансов")
    db.recalculate_balances()
    balances_after = db.get_balances()
    print("   Балансы после пересчета:")
    for currency, balance in balances_after.items():
        if balance != 0:
            print(f"      {currency}: {balance}")
    assert balances == balances_after, "Ошибка: балансы должны совпадать"
    print("   ✅ Тест пройден")
    
    # Тест 8: Удаление операции
    print("\n📝 Тест 8: Удаление операции")
    balance_before = db.get_balance('USD')
    success = db.delete_operation(1)  # Удаляем первую операцию (поступление 1000)
    balance_after = db.get_balance('USD')
    print(f"   Баланс USD до удаления: {balance_before}")
    print(f"   Баланс USD после удаления: {balance_after}")
    assert success, "Ошибка: операция не удалена"
    assert balance_after == balance_before - 1000.0, "Ошибка: баланс должен уменьшиться на 1000"
    print("   ✅ Тест пройден")
    
    print("\n🎉 Все тесты пройдены успешно!")
    print("\n🗑️  Удаление тестовой базы данных...")
    
    import os
    if os.path.exists('test_currency_operations.db'):
        os.remove('test_currency_operations.db')
        print("✅ Тестовая база данных удалена")

if __name__ == '__main__':
    try:
        test_database()
    except AssertionError as e:
        print(f"\n❌ Тест провален: {e}")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
