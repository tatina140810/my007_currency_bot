#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Расширенные тесты базы данных
Включает проверку всех типов операций, включая "Возврат по ПП"
"""

import sys
import os
import unittest

sys.path.insert(0, '/mnt/user-data/uploads')

from database import Database


class TestDatabaseOperations(unittest.TestCase):
    """Тесты операций базы данных"""

    def setUp(self):
        """Подготовка к каждому тесту"""
        self.db = Database("test_operations.db")
        conn = self.db.get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM operations")
        cur.execute("DELETE FROM balances")
        cur.execute("DELETE FROM chats")
        conn.commit()
        conn.close()

    def tearDown(self):
        """Очистка после каждого теста"""
        if os.path.exists("test_operations.db"):
            os.remove("test_operations.db")

    def test_add_income(self):
        """Тест добавления поступления"""
        chat_id = 12345
        op_id = self.db.add_operation(chat_id, "Поступление", "USD", 1000.0, "Тест")
        
        self.assertIsNotNone(op_id)
        self.assertGreater(op_id, 0)
        
        balance = self.db.get_balance(chat_id, "USD")
        self.assertEqual(balance, 1000.0)

    def test_add_payment(self):
        """Тест оплаты ПП"""
        chat_id = 12345
        
        # Сначала добавим деньги
        self.db.add_operation(chat_id, "Поступление", "USD", 5000.0, "Начальная сумма")
        
        # Потом оплата
        self.db.add_operation(chat_id, "Оплата ПП", "USD", -1000.0, "Оплата поставщику")
        
        balance = self.db.get_balance(chat_id, "USD")
        self.assertEqual(balance, 4000.0)

    def test_return_payment(self):
        """КРИТИЧЕСКИЙ ТЕСТ: Возврат по ПП"""
        chat_id = 12345
        
        # Добавляем возврат
        op_id = self.db.add_operation(chat_id, "Возврат по ПП", "USD", 79855.0, 
                                       "79 855,00 долл - Возврат пп от 25112025")
        
        self.assertIsNotNone(op_id)
        
        # Проверяем баланс
        balance = self.db.get_balance(chat_id, "USD")
        self.assertEqual(balance, 79855.0, 
                        "Возврат по ПП должен увеличить баланс на 79855 USD")
        
        # Проверяем, что операция в БД
        operations = self.db.get_operations(chat_id, limit=10)
        self.assertEqual(len(operations), 1)
        
        op = operations[0]
        self.assertEqual(op[1], "Возврат по ПП")  # operation_type
        self.assertEqual(op[2], "USD")             # currency
        self.assertEqual(op[3], 79855.0)           # amount (положительная!)

    def test_conversion_basic(self):
        """Тест простой конвертации"""
        chat_id = 12345
        
        # Конвертация: списываем USD, зачисляем RUB
        self.db.add_operation(chat_id, "Конвертация", "USD", -100.0, "Обмен")
        self.db.add_operation(chat_id, "Конвертация", "RUB", 8950.0, "Обмен")
        
        balance_usd = self.db.get_balance(chat_id, "USD")
        balance_rub = self.db.get_balance(chat_id, "RUB")
        
        self.assertEqual(balance_usd, -100.0)
        self.assertEqual(balance_rub, 8950.0)

    def test_conversion_with_initial_balance(self):
        """Тест конвертации с начальным балансом"""
        chat_id = 12345
        
        # Начальный баланс
        self.db.add_operation(chat_id, "Поступление", "USD", 5000.0, "Начало")
        
        # Конвертация 1000 USD -> RUB
        self.db.add_operation(chat_id, "Конвертация", "USD", -1000.0, "Обмен")
        self.db.add_operation(chat_id, "Конвертация", "RUB", 89500.0, "Обмен")
        
        balance_usd = self.db.get_balance(chat_id, "USD")
        balance_rub = self.db.get_balance(chat_id, "RUB")
        
        self.assertEqual(balance_usd, 4000.0)
        self.assertEqual(balance_rub, 89500.0)

    def test_cash_withdrawal(self):
        """Тест выдачи наличных"""
        chat_id = 12345
        
        self.db.add_operation(chat_id, "Поступление", "USD", 10000.0, "Начало")
        self.db.add_operation(chat_id, "Выдача наличных", "USD", -3000.0, "Выдача")
        
        balance = self.db.get_balance(chat_id, "USD")
        self.assertEqual(balance, 7000.0)

    def test_cash_deposit(self):
        """Тест взноса наличными"""
        chat_id = 12345
        
        self.db.add_operation(chat_id, "Взнос наличными", "USD", 5000.0, "Взнос")
        
        balance = self.db.get_balance(chat_id, "USD")
        self.assertEqual(balance, 5000.0)

    def test_swift_commission(self):
        """Тест SWIFT комиссии"""
        chat_id = 12345
        
        self.db.add_operation(chat_id, "Поступление", "USD", 1000.0, "Начало")
        self.db.add_operation(chat_id, "SWIFT", "USD", -25.0, "SWIFT комиссия")
        
        balance = self.db.get_balance(chat_id, "USD")
        self.assertEqual(balance, 975.0)

    def test_bank_request(self):
        """Тест запроса банку"""
        chat_id = 12345
        
        self.db.add_operation(chat_id, "Поступление", "USD", 1000.0, "Начало")
        self.db.add_operation(chat_id, "Запрос банку", "USD", -65.0, "Запрос выписки")
        
        balance = self.db.get_balance(chat_id, "USD")
        self.assertEqual(balance, 935.0)

    def test_get_operations(self):
        """Тест получения списка операций"""
        chat_id = 12345
        
        self.db.add_operation(chat_id, "Поступление", "USD", 1000.0, "Тест 1")
        self.db.add_operation(chat_id, "Оплата ПП", "USD", -500.0, "Тест 2")
        self.db.add_operation(chat_id, "Возврат по ПП", "USD", 79855.0, "Тест 3")
        
        operations = self.db.get_operations(chat_id, limit=10)
        self.assertEqual(len(operations), 3)
        
        # Проверяем последнюю операцию (возврат)
        last_op = operations[0]  # Сортировка DESC
        self.assertEqual(last_op[1], "Возврат по ПП")
        self.assertEqual(last_op[3], 79855.0)

    def test_delete_operation(self):
        """Тест удаления операции"""
        chat_id = 12345
        
        op_id = self.db.add_operation(chat_id, "Поступление", "USD", 1000.0, "Тест")
        
        balance_before = self.db.get_balance(chat_id, "USD")
        self.assertEqual(balance_before, 1000.0)
        
        success = self.db.delete_operation(chat_id, op_id)
        self.assertTrue(success)
        
        balance_after = self.db.get_balance(chat_id, "USD")
        self.assertEqual(balance_after, 0.0)

    def test_delete_nonexistent_operation(self):
        """Тест удаления несуществующей операции"""
        chat_id = 12345
        
        success = self.db.delete_operation(chat_id, 99999)
        self.assertFalse(success)

    def test_multiple_currencies(self):
        """Тест работы с несколькими валютами"""
        chat_id = 12345
        
        self.db.add_operation(chat_id, "Поступление", "USD", 1000.0, "USD тест")
        self.db.add_operation(chat_id, "Поступление", "EUR", 500.0, "EUR тест")
        self.db.add_operation(chat_id, "Поступление", "RUB", 50000.0, "RUB тест")
        
        balances = self.db.get_balances(chat_id)
        
        self.assertEqual(balances["USD"], 1000.0)
        self.assertEqual(balances["EUR"], 500.0)
        self.assertEqual(balances["RUB"], 50000.0)

    def test_recalculate_balances(self):
        """Тест пересчета балансов"""
        chat_id = 12345
        
        self.db.add_operation(chat_id, "Поступление", "USD", 1000.0, "Тест")
        self.db.add_operation(chat_id, "Оплата ПП", "USD", -300.0, "Тест")
        
        # Принудительно сбрасываем баланс в БД
        conn = self.db.get_connection()
        cur = conn.cursor()
        cur.execute("UPDATE balances SET balance = 0.0 WHERE chat_id = ?", (chat_id,))
        conn.commit()
        conn.close()
        
        # Пересчитываем
        self.db.recalculate_balances(chat_id)
        
        balance = self.db.get_balance(chat_id, "USD")
        self.assertEqual(balance, 700.0)


class TestMultipleChatsSeparation(unittest.TestCase):
    """Тесты изоляции данных между чатами"""

    def setUp(self):
        """Подготовка к каждому тесту"""
        self.db = Database("test_operations.db")
        conn = self.db.get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM operations")
        cur.execute("DELETE FROM balances")
        cur.execute("DELETE FROM chats")
        conn.commit()
        conn.close()

    def tearDown(self):
        """Очистка после каждого теста"""
        if os.path.exists("test_operations.db"):
            os.remove("test_operations.db")

    def test_two_chats_independent_balances(self):
        """Тест независимости балансов двух чатов"""
        chat1 = 111
        chat2 = 222
        
        self.db.add_operation(chat1, "Поступление", "USD", 1000.0, "Чат 1")
        self.db.add_operation(chat2, "Поступление", "USD", 2000.0, "Чат 2")
        
        balance1 = self.db.get_balance(chat1, "USD")
        balance2 = self.db.get_balance(chat2, "USD")
        
        self.assertEqual(balance1, 1000.0)
        self.assertEqual(balance2, 2000.0)

    def test_operations_isolation(self):
        """Тест изоляции операций между чатами"""
        chat1 = 111
        chat2 = 222
        
        self.db.add_operation(chat1, "Поступление", "USD", 1000.0, "Чат 1")
        self.db.add_operation(chat2, "Поступление", "USD", 2000.0, "Чат 2")
        
        ops1 = self.db.get_operations(chat1, limit=10)
        ops2 = self.db.get_operations(chat2, limit=10)
        
        self.assertEqual(len(ops1), 1)
        self.assertEqual(len(ops2), 1)
        
        # Проверяем, что операции не смешиваются
        self.assertEqual(ops1[0][4], "Чат 1")
        self.assertEqual(ops2[0][4], "Чат 2")

    def test_return_payment_in_different_chats(self):
        """Тест возврата по ПП в разных чатах"""
        chat1 = 111
        chat2 = 222
        
        self.db.add_operation(chat1, "Возврат по ПП", "USD", 10000.0, "Возврат чат 1")
        self.db.add_operation(chat2, "Возврат по ПП", "USD", 20000.0, "Возврат чат 2")
        
        balance1 = self.db.get_balance(chat1, "USD")
        balance2 = self.db.get_balance(chat2, "USD")
        
        self.assertEqual(balance1, 10000.0)
        self.assertEqual(balance2, 20000.0)


class TestStatistics(unittest.TestCase):
    """Тесты статистики"""

    def setUp(self):
        """Подготовка к каждому тесту"""
        self.db = Database("test_operations.db")
        conn = self.db.get_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM operations")
        cur.execute("DELETE FROM balances")
        cur.execute("DELETE FROM chats")
        conn.commit()
        conn.close()

    def tearDown(self):
        """Очистка после каждого теста"""
        if os.path.exists("test_operations.db"):
            os.remove("test_operations.db")

    def test_statistics_income_expense(self):
        """Тест статистики приход/расход"""
        chat_id = 12345
        
        self.db.add_operation(chat_id, "Поступление", "USD", 5000.0, "Приход 1")
        self.db.add_operation(chat_id, "Поступление", "USD", 3000.0, "Приход 2")
        self.db.add_operation(chat_id, "Оплата ПП", "USD", -2000.0, "Расход 1")
        self.db.add_operation(chat_id, "Выдача наличных", "USD", -1000.0, "Расход 2")
        
        stats = self.db.get_statistics(chat_id)
        
        self.assertIn("USD", stats)
        self.assertEqual(stats["USD"]["income"], 8000.0)
        self.assertEqual(stats["USD"]["expense"], -3000.0)
        self.assertEqual(stats["USD"]["balance"], 5000.0)

    def test_statistics_with_return(self):
        """Тест статистики с возвратом по ПП"""
        chat_id = 12345
        
        self.db.add_operation(chat_id, "Оплата ПП", "USD", -10000.0, "Оплата")
        self.db.add_operation(chat_id, "Возврат по ПП", "USD", 79855.0, "Возврат")
        
        stats = self.db.get_statistics(chat_id)
        
        self.assertIn("USD", stats)
        self.assertEqual(stats["USD"]["income"], 79855.0)
        self.assertEqual(stats["USD"]["expense"], -10000.0)
        self.assertEqual(stats["USD"]["balance"], 69855.0)


def run_tests():
    """Запуск всех тестов БД"""
    print("🧪 Запуск расширенных тестов базы данных\n")
    print("=" * 70)
    
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestDatabaseOperations))
    suite.addTests(loader.loadTestsFromTestCase(TestMultipleChatsSeparation))
    suite.addTests(loader.loadTestsFromTestCase(TestStatistics))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print("\n" + "=" * 70)
    print(f"\n📊 РЕЗУЛЬТАТЫ:")
    print(f"   ✅ Пройдено: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"   ❌ Провалено: {len(result.failures)}")
    print(f"   ⚠️  Ошибки: {len(result.errors)}")
    
    if result.wasSuccessful():
        print("\n🎉 ВСЕ ТЕСТЫ БАЗЫ ДАННЫХ ПРОЙДЕНЫ!")
        return 0
    else:
        print("\n❌ НЕКОТОРЫЕ ТЕСТЫ ПРОВАЛЕНЫ")
        return 1


if __name__ == "__main__":
    sys.exit(run_tests())
