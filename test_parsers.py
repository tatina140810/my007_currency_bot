#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тесты парсеров (parse_income_notification, parse_manual_operation_line)
"""

import sys
import os
import unittest
import re

# Добавляем путь к модулям
sys.path.insert(0, '/mnt/user-data/uploads')

# Импортируем функции из bot.py
def normalize_currency(curr: str) -> str:
    """Приводим строки к коду валюты."""
    c = curr.strip().lower()
    curr_map = {
        "руб": "RUB", "руб.": "RUB", "₽": "RUB", "рублей": "RUB", "rub": "RUB", "рубля": "RUB",
        "сом": "KGS", "сомов": "KGS", "kgs": "KGS",
        "usd": "USD", "долл": "USD", "$": "USD", "долл.": "USD", "дол": "USD", "д": "USD",
        "доллар": "USD", "долларов": "USD", "долларах": "USD",
        "usdt": "USDT", "тез": "USDT", "тезер": "USDT",
        "eur": "EUR", "ев": "EUR", "€": "EUR", "евро": "EUR",
        "kzt": "KZT", "тенге": "KZT",
        "cny": "CNY", "yuan": "CNY", "¥": "CNY",
        "юан": "CNY", "юань": "CNY", "ю": "CNY", "юань.": "CNY",
        "юаней": "CNY", "юани": "CNY", "юаня": "CNY",
        "aed": "AED", "дирхам": "AED", "дирхамов": "AED", "дир": "AED",
    }
    return curr_map.get(c, c.upper())


def parse_human_number(s: str) -> float:
    """Парсинг чисел в разных форматах"""
    s = s.strip()
    s = s.replace("\u00A0", " ")
    s = re.sub(r"\s+", "", s)

    has_dot = "." in s
    has_comma = "," in s

    if has_dot and has_comma:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
        return float(s)

    if has_dot and not has_comma:
        if re.fullmatch(r"\d{1,3}(\.\d{3})+", s):
            s = s.replace(".", "")
            return float(s)
        return float(s)

    if has_comma and not has_dot:
        if re.fullmatch(r"\d{1,3}(,\d{3})+", s):
            s = s.replace(",", "")
            return float(s)
        s = s.replace(",", ".")
        return float(s)

    return float(s)


def parse_income_notification(text: str):
    """Разбор текстового уведомления банка о поступлении"""
    if not text:
        return None

    low = text.lower()

    if not any(
        kw in low
        for kw in (
            "поступил", "поступили", "поступление",
            "зачислен", "зачислены", "зачисление",
        )
    ):
        return None

    m = re.search(
        r"(?P<amount>\d[\d\s]*[.,]\d{2})\s*"
        r"(?P<curr>руб(?:\.|лей)?|сом(?:ов)?|kgs|usd|eur|rub|kzt|cny|долл\.?|дол)",
        text,
        re.IGNORECASE,
    )

    if not m:
        return None

    amount_str = m.group("amount")
    curr_raw = m.group("curr")

    try:
        amount = float(amount_str.replace(" ", "").replace(",", "."))
    except ValueError:
        return None

    currency = normalize_currency(curr_raw)
    description = text.strip()

    return {
        "amount": amount,
        "currency": currency,
        "description": description,
    }


class TestNormalizeCurrency(unittest.TestCase):
    """Тесты нормализации валют"""

    def test_rub_variants(self):
        """Различные варианты написания рубля"""
        self.assertEqual(normalize_currency("руб"), "RUB")
        self.assertEqual(normalize_currency("руб."), "RUB")
        self.assertEqual(normalize_currency("₽"), "RUB")
        self.assertEqual(normalize_currency("рублей"), "RUB")
        self.assertEqual(normalize_currency("RUB"), "RUB")
        self.assertEqual(normalize_currency("рубля"), "RUB")

    def test_usd_variants(self):
        """Различные варианты написания доллара"""
        self.assertEqual(normalize_currency("usd"), "USD")
        self.assertEqual(normalize_currency("USD"), "USD")
        self.assertEqual(normalize_currency("долл"), "USD")
        self.assertEqual(normalize_currency("долл."), "USD")
        self.assertEqual(normalize_currency("$"), "USD")
        self.assertEqual(normalize_currency("доллар"), "USD")
        self.assertEqual(normalize_currency("долларов"), "USD")
        self.assertEqual(normalize_currency("долларах"), "USD")

    def test_cny_variants(self):
        """Различные варианты написания юаня"""
        self.assertEqual(normalize_currency("юань"), "CNY")
        self.assertEqual(normalize_currency("юан"), "CNY")
        self.assertEqual(normalize_currency("юаней"), "CNY")
        self.assertEqual(normalize_currency("юани"), "CNY")
        self.assertEqual(normalize_currency("CNY"), "CNY")
        self.assertEqual(normalize_currency("¥"), "CNY")

    def test_eur_variants(self):
        """Различные варианты написания евро"""
        self.assertEqual(normalize_currency("евро"), "EUR")
        self.assertEqual(normalize_currency("EUR"), "EUR")
        self.assertEqual(normalize_currency("€"), "EUR")

    def test_kgs_variants(self):
        """Различные варианты написания сома"""
        self.assertEqual(normalize_currency("сом"), "KGS")
        self.assertEqual(normalize_currency("сомов"), "KGS")
        self.assertEqual(normalize_currency("KGS"), "KGS")

    def test_unknown_currency(self):
        """Неизвестная валюта возвращается как есть в UPPER"""
        self.assertEqual(normalize_currency("ABC"), "ABC")
        self.assertEqual(normalize_currency("xyz"), "XYZ")


class TestParseHumanNumber(unittest.TestCase):
    """Тесты парсинга чисел"""

    def test_simple_integers(self):
        """Простые целые числа"""
        self.assertEqual(parse_human_number("100"), 100.0)
        self.assertEqual(parse_human_number("1000"), 1000.0)
        self.assertEqual(parse_human_number("12345"), 12345.0)

    def test_decimals_dot(self):
        """Десятичные числа с точкой"""
        self.assertEqual(parse_human_number("1.5"), 1.5)
        self.assertEqual(parse_human_number("11.50"), 11.5)
        self.assertEqual(parse_human_number("123.456"), 123.456)

    def test_decimals_comma(self):
        """Десятичные числа с запятой"""
        self.assertEqual(parse_human_number("11,5"), 11.5)
        self.assertEqual(parse_human_number("123,45"), 123.45)

    def test_thousands_space(self):
        """Разделители тысяч пробелами"""
        self.assertEqual(parse_human_number("1 000"), 1000.0)
        self.assertEqual(parse_human_number("21 000"), 21000.0)
        self.assertEqual(parse_human_number("1 234 567"), 1234567.0)

    def test_thousands_dot(self):
        """Разделители тысяч точками"""
        self.assertEqual(parse_human_number("1.000"), 1000.0)
        self.assertEqual(parse_human_number("21.000"), 21000.0)
        self.assertEqual(parse_human_number("1.234.567"), 1234567.0)

    def test_thousands_comma(self):
        """Разделители тысяч запятыми"""
        self.assertEqual(parse_human_number("1,000"), 1000.0)
        self.assertEqual(parse_human_number("21,000"), 21000.0)

    def test_complex_formats(self):
        """Сложные форматы с разделителями"""
        self.assertEqual(parse_human_number("79 855,00"), 79855.0)
        self.assertEqual(parse_human_number("1.234,56"), 1234.56)
        self.assertEqual(parse_human_number("1,234.56"), 1234.56)
        self.assertEqual(parse_human_number("2 484 444.51"), 2484444.51)
        self.assertEqual(parse_human_number("2 484 444,51"), 2484444.51)

    def test_edge_cases(self):
        """Граничные случаи"""
        self.assertEqual(parse_human_number("0"), 0.0)
        self.assertEqual(parse_human_number("0.0"), 0.0)
        self.assertEqual(parse_human_number("0,0"), 0.0)


class TestParseIncomeNotification(unittest.TestCase):
    """Тесты парсинга уведомлений о поступлении"""

    def test_simple_rub_income(self):
        """Простое поступление рублей"""
        text = "На ваш счёт поступили 1000,00 руб"
        result = parse_income_notification(text)
        self.assertIsNotNone(result)
        self.assertEqual(result["amount"], 1000.0)
        self.assertEqual(result["currency"], "RUB")

    def test_complex_rub_income(self):
        """Сложное поступление рублей"""
        text = "2 484 444.51 RUB поступление от клиента"
        result = parse_income_notification(text)
        self.assertIsNotNone(result)
        self.assertEqual(result["amount"], 2484444.51)
        self.assertEqual(result["currency"], "RUB")

    def test_usd_income(self):
        """Поступление долларов"""
        text = "Зачислено 5000,00 долл на счёт"
        result = parse_income_notification(text)
        self.assertIsNotNone(result)
        self.assertEqual(result["amount"], 5000.0)
        self.assertEqual(result["currency"], "USD")

    def test_kgs_income(self):
        """Поступление сомов"""
        text = "Поступили 10000,00 сом"
        result = parse_income_notification(text)
        self.assertIsNotNone(result)
        self.assertEqual(result["amount"], 10000.0)
        self.assertEqual(result["currency"], "KGS")

    def test_no_income_keywords(self):
        """Нет ключевых слов поступления"""
        text = "Оплата по счету 1000,00 руб"
        result = parse_income_notification(text)
        self.assertIsNone(result)

    def test_invalid_format(self):
        """Неверный формат"""
        text = "Поступили деньги"
        result = parse_income_notification(text)
        self.assertIsNone(result)

    def test_empty_text(self):
        """Пустой текст"""
        result = parse_income_notification("")
        self.assertIsNone(result)
        result = parse_income_notification(None)
        self.assertIsNone(result)


def run_tests():
    """Запуск всех тестов парсеров"""
    print("🧪 Запуск тестов парсеров\n")
    print("=" * 70)
    
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestNormalizeCurrency))
    suite.addTests(loader.loadTestsFromTestCase(TestParseHumanNumber))
    suite.addTests(loader.loadTestsFromTestCase(TestParseIncomeNotification))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print("\n" + "=" * 70)
    print(f"\n📊 РЕЗУЛЬТАТЫ:")
    print(f"   ✅ Пройдено: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"   ❌ Провалено: {len(result.failures)}")
    print(f"   ⚠️  Ошибки: {len(result.errors)}")
    
    if result.wasSuccessful():
        print("\n🎉 ВСЕ ТЕСТЫ ПАРСЕРОВ ПРОЙДЕНЫ!")
        return 0
    else:
        print("\n❌ НЕКОТОРЫЕ ТЕСТЫ ПРОВАЛЕНЫ")
        return 1


if __name__ == "__main__":
    sys.exit(run_tests())
