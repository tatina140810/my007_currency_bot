#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тесты функции конвертации валют (compute_conversion_to_amount)
"""

import sys
import unittest

sys.path.insert(0, '/mnt/user-data/uploads')


def compute_conversion_to_amount(
    amount: float, rate: float, from_curr: str, to_curr: str
) -> float:
    """Умная конвертация с учетом слабых/сильных валют"""
    weak = {"RUB", "KGS", "KZT", "CNY"}
    strong = {"USD", "USDT", "EUR", "AED"}

    if rate <= 0:
        raise ValueError("Курс должен быть > 0")

    from_weak = from_curr in weak
    from_strong = from_curr in strong
    to_weak = to_curr in weak
    to_strong = to_curr in strong

    # СИЛЬНАЯ -> СЛАБАЯ (USD -> RUB)
    if from_strong and to_weak:
        return amount * rate
    
    # СЛАБАЯ -> СИЛЬНАЯ (RUB -> USD)
    if from_weak and to_strong:
        return amount / rate
    
    # СЛАБАЯ -> СЛАБАЯ (CNY -> RUB)
    if from_weak and to_weak:
        return amount * rate
    
    # СИЛЬНАЯ -> СИЛЬНАЯ (USD -> EUR)
    if from_strong and to_strong:
        return amount * rate
    
    # По умолчанию
    return amount * rate


class TestComputeConversion(unittest.TestCase):
    """Тесты функции конвертации"""

    def test_usd_to_rub(self):
        """USD -> RUB (сильная -> слабая)"""
        # 1000 USD по курсу 89.5 = 89500 RUB
        result = compute_conversion_to_amount(1000, 89.5, "USD", "RUB")
        self.assertEqual(result, 89500.0)
        
        # 100 USD по курсу 90 = 9000 RUB
        result = compute_conversion_to_amount(100, 90, "USD", "RUB")
        self.assertEqual(result, 9000.0)

    def test_rub_to_usd(self):
        """RUB -> USD (слабая -> сильная)"""
        # 89500 RUB по курсу 89.5 = 1000 USD
        result = compute_conversion_to_amount(89500, 89.5, "RUB", "USD")
        self.assertEqual(result, 1000.0)
        
        # 9000 RUB по курсу 90 = 100 USD
        result = compute_conversion_to_amount(9000, 90, "RUB", "USD")
        self.assertEqual(result, 100.0)

    def test_usd_to_eur(self):
        """USD -> EUR (сильная -> сильная)"""
        # 1000 USD по курсу 0.92 = 920 EUR
        result = compute_conversion_to_amount(1000, 0.92, "USD", "EUR")
        self.assertEqual(result, 920.0)

    def test_eur_to_usd(self):
        """EUR -> USD (сильная -> сильная)"""
        # 920 EUR по курсу 1.09 = 1002.8 USD
        result = compute_conversion_to_amount(920, 1.09, "EUR", "USD")
        self.assertAlmostEqual(result, 1002.8, places=2)

    def test_cny_to_rub(self):
        """CNY -> RUB (слабая -> слабая)"""
        # 100 CNY по курсу 12.5 = 1250 RUB
        result = compute_conversion_to_amount(100, 12.5, "CNY", "RUB")
        self.assertEqual(result, 1250.0)
        
        # 1000 CNY по курсу 12.5 = 12500 RUB
        result = compute_conversion_to_amount(1000, 12.5, "CNY", "RUB")
        self.assertEqual(result, 12500.0)

    def test_rub_to_kgs(self):
        """RUB -> KGS (слабая -> слабая)"""
        # 1000 RUB по курсу 1.2 = 1200 KGS
        result = compute_conversion_to_amount(1000, 1.2, "RUB", "KGS")
        self.assertEqual(result, 1200.0)

    def test_kgs_to_rub(self):
        """KGS -> RUB (слабая -> слабая)"""
        # 1200 KGS по курсу 0.833 ≈ 1000 RUB
        result = compute_conversion_to_amount(1200, 0.833, "KGS", "RUB")
        self.assertAlmostEqual(result, 999.6, places=1)

    def test_usd_to_kgs(self):
        """USD -> KGS (сильная -> слабая)"""
        # 100 USD по курсу 87.5 = 8750 KGS
        result = compute_conversion_to_amount(100, 87.5, "USD", "KGS")
        self.assertEqual(result, 8750.0)

    def test_kgs_to_usd(self):
        """KGS -> USD (слабая -> сильная)"""
        # 8750 KGS по курсу 87.5 = 100 USD
        result = compute_conversion_to_amount(8750, 87.5, "KGS", "USD")
        self.assertEqual(result, 100.0)

    def test_eur_to_aed(self):
        """EUR -> AED (сильная -> сильная)"""
        # 1000 EUR по курсу 4.0 = 4000 AED
        result = compute_conversion_to_amount(1000, 4.0, "EUR", "AED")
        self.assertEqual(result, 4000.0)

    def test_usdt_to_rub(self):
        """USDT -> RUB (сильная -> слабая)"""
        # 500 USDT по курсу 89.0 = 44500 RUB
        result = compute_conversion_to_amount(500, 89.0, "USDT", "RUB")
        self.assertEqual(result, 44500.0)

    def test_rub_to_usdt(self):
        """RUB -> USDT (слабая -> сильная)"""
        # 44500 RUB по курсу 89.0 = 500 USDT
        result = compute_conversion_to_amount(44500, 89.0, "RUB", "USDT")
        self.assertEqual(result, 500.0)

    def test_invalid_rate_zero(self):
        """Курс = 0 должен вызвать ошибку"""
        with self.assertRaises(ValueError):
            compute_conversion_to_amount(1000, 0, "USD", "RUB")

    def test_invalid_rate_negative(self):
        """Отрицательный курс должен вызвать ошибку"""
        with self.assertRaises(ValueError):
            compute_conversion_to_amount(1000, -10, "USD", "RUB")

    def test_zero_amount(self):
        """Конвертация нулевой суммы"""
        result = compute_conversion_to_amount(0, 89.5, "USD", "RUB")
        self.assertEqual(result, 0.0)

    def test_fractional_amount(self):
        """Конвертация дробных сумм"""
        # 10.5 USD по курсу 89.5 = 939.75 RUB
        result = compute_conversion_to_amount(10.5, 89.5, "USD", "RUB")
        self.assertEqual(result, 939.75)

    def test_large_amounts(self):
        """Конвертация больших сумм"""
        # 1000000 USD по курсу 89.5 = 89500000 RUB
        result = compute_conversion_to_amount(1000000, 89.5, "USD", "RUB")
        self.assertEqual(result, 89500000.0)

    def test_precision(self):
        """Точность вычислений"""
        # 79855 USD по курсу 89.5 = 7147022.5 RUB
        result = compute_conversion_to_amount(79855, 89.5, "USD", "RUB")
        self.assertAlmostEqual(result, 7147022.5, places=2)


class TestConversionSymmetry(unittest.TestCase):
    """Тесты симметричности конвертации (туда-обратно)"""

    def test_usd_rub_symmetry(self):
        """USD -> RUB -> USD должно вернуть исходную сумму"""
        initial = 1000.0
        rate = 89.5
        
        # USD -> RUB
        rub = compute_conversion_to_amount(initial, rate, "USD", "RUB")
        # RUB -> USD
        usd_back = compute_conversion_to_amount(rub, rate, "RUB", "USD")
        
        self.assertAlmostEqual(usd_back, initial, places=2)

    def test_cny_rub_symmetry(self):
        """CNY -> RUB -> CNY должно вернуть исходную сумму"""
        initial = 10000.0
        rate = 12.5
        
        # CNY -> RUB
        rub = compute_conversion_to_amount(initial, rate, "CNY", "RUB")
        # RUB -> CNY (обратный курс)
        cny_back = compute_conversion_to_amount(rub, 1/rate, "RUB", "CNY")
        
        self.assertAlmostEqual(cny_back, initial, places=2)


def run_tests():
    """Запуск всех тестов конвертации"""
    print("🧪 Запуск тестов конвертации\n")
    print("=" * 70)
    
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestComputeConversion))
    suite.addTests(loader.loadTestsFromTestCase(TestConversionSymmetry))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print("\n" + "=" * 70)
    print(f"\n📊 РЕЗУЛЬТАТЫ:")
    print(f"   ✅ Пройдено: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"   ❌ Провалено: {len(result.failures)}")
    print(f"   ⚠️  Ошибки: {len(result.errors)}")
    
    if result.wasSuccessful():
        print("\n🎉 ВСЕ ТЕСТЫ КОНВЕРТАЦИИ ПРОЙДЕНЫ!")
        return 0
    else:
        print("\n❌ НЕКОТОРЫЕ ТЕСТЫ ПРОВАЛЕНЫ")
        return 1


if __name__ == "__main__":
    sys.exit(run_tests())
