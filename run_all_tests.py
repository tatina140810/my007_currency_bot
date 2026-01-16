#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Запуск всех тестов проекта
"""

import sys
import os
import subprocess
from datetime import datetime

def run_test_file(test_file, description):
    """Запуск одного тестового файла"""
    print(f"\n{'='*70}")
    print(f"🧪 {description}")
    print(f"{'='*70}\n")
    
    result = subprocess.run([sys.executable, test_file], capture_output=False)
    return result.returncode == 0


def main():
    """Запуск всех тестов"""
    start_time = datetime.now()
    
    print("\n" + "🚀"*35)
    print("   КОМПЛЕКСНОЕ ТЕСТИРОВАНИЕ TELEGRAM БОТА")
    print("🚀"*35 + "\n")
    
    tests = [
        ("test_parsers.py", "ТЕСТЫ ПАРСЕРОВ"),
        ("test_conversions.py", "ТЕСТЫ КОНВЕРТАЦИИ"),
        ("test_database_advanced.py", "ТЕСТЫ БАЗЫ ДАННЫХ"),
    ]
    
    results = {}
    
    for test_file, description in tests:
        if os.path.exists(test_file):
            success = run_test_file(test_file, description)
            results[description] = success
        else:
            print(f"\n⚠️  Файл {test_file} не найден, пропускаю")
            results[description] = None
    
    # Итоговый отчет
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "="*70)
    print("📊 ИТОГОВЫЙ ОТЧЕТ")
    print("="*70 + "\n")
    
    passed = 0
    failed = 0
    skipped = 0
    
    for test_name, result in results.items():
        if result is True:
            print(f"✅ {test_name}: ПРОЙДЕНО")
            passed += 1
        elif result is False:
            print(f"❌ {test_name}: ПРОВАЛЕНО")
            failed += 1
        else:
            print(f"⚠️  {test_name}: ПРОПУЩЕНО")
            skipped += 1
    
    print(f"\n{'='*70}")
    print(f"\n📈 СТАТИСТИКА:")
    print(f"   ✅ Пройдено: {passed}")
    print(f"   ❌ Провалено: {failed}")
    print(f"   ⚠️  Пропущено: {skipped}")
    print(f"   ⏱️  Время выполнения: {duration:.2f} секунд")
    
    if failed == 0 and passed > 0:
        print("\n🎉 ВСЕ ТЕСТЫ УСПЕШНО ПРОЙДЕНЫ! 🎉")
        return 0
    elif failed > 0:
        print("\n❌ НЕКОТОРЫЕ ТЕСТЫ ПРОВАЛЕНЫ")
        return 1
    else:
        print("\n⚠️  НЕТ ТЕСТОВ ДЛЯ ЗАПУСКА")
        return 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n\n⚠️  Тестирование прервано пользователем")
        sys.exit(130)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
