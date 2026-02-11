"""
УЛУЧШЕННЫЙ ПАРСЕР SWIFT v2.0
Максимально толерантен к ошибкам OCR
"""

import re
import logging
from typing import Optional, Dict, Any
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)


def similarity(a: str, b: str) -> float:
    """Вычисляет схожесть двух строк (0.0 - 1.0)"""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def fuzzy_find_tag(text: str, target_tag: str, threshold: float = 0.7) -> list:
    """
    Нечеткий поиск XML тегов с учетом ошибок OCR
    
    Пример:
        fuzzy_find_tag(text, "IntrBkSttlmAmt") найдет:
        - IntrBkSttlmAmt
        - INteBkStt loamt
        - InteBeSttlmAmt
    """
    results = []
    
    # Ищем все возможные теги в тексте
    tag_pattern = r'<([^>]+?)>'
    for match in re.finditer(tag_pattern, text):
        tag_content = match.group(1).strip()
        
        # Извлекаем имя тега (без атрибутов)
        tag_name = tag_content.split()[0] if ' ' in tag_content else tag_content
        tag_name = tag_name.strip('/<>')
        
        # Проверяем схожесть
        if similarity(tag_name, target_tag) >= threshold:
            results.append({
                'match': match.group(0),
                'tag_name': tag_name,
                'full_content': tag_content,
                'start': match.start(),
                'end': match.end(),
                'similarity': similarity(tag_name, target_tag)
            })
    
    # Сортируем по схожести
    results.sort(key=lambda x: x['similarity'], reverse=True)
    return results


def clean_number(text: str) -> str:
    """
    Очищает число от мусора OCR
    
    Примеры:
        "15 7675. 00" → "157675.00"
        "104645,00" → "104645.00"
        "1 0 4 6 4 5" → "104645"
    """
    if not text:
        return ""
    
    # Убираем все пробелы
    text = text.replace(" ", "").replace("\u00A0", "")
    
    # Убираем мусорные символы
    text = text.replace("*", "").replace("#", "").replace("'", "")
    
    # Заменяем запятую на точку
    if "," in text:
        text = text.replace(",", ".")
    
    return text


def extract_amount_and_currency_fuzzy(text: str) -> tuple[Optional[float], Optional[str]]:
    """
    УЛУЧШЕННОЕ извлечение суммы и валюты с учетом ошибок OCR
    """
    if not text:
        return None, None
    
    logger.info("🔍 Начало извлечения суммы и валюты")
    
    # 1️⃣ НЕЧЕТКИЙ ПОИСК ТЕГОВ СУММЫ
    amount_tags = ['IntrBkSttlmAmt', 'InstdAmt', 'IntrBkStt', 'InstdA']
    
    found_currency = None  # 🔥 СОХРАНЯЕМ ВАЛЮТУ
    found_amount = None
    
    for tag in amount_tags:
        matches = fuzzy_find_tag(text, tag, threshold=0.6)
        
        for match_info in matches:
            full_content = match_info['full_content']
            logger.info(f"📌 Найден похожий тег: {full_content}")
            
            # Извлекаем валюту из атрибута Ccy
            # Примеры: Ccy="CNY", Coy="CNY*", Cey#"CNY"
            ccy_pattern = r'C[ceo][ye][^"\'=]*["\']?=?["\']?\s*([A-Z]{3})'
            ccy_match = re.search(ccy_pattern, full_content, re.IGNORECASE)
            
            if ccy_match and not found_currency:
                found_currency = ccy_match.group(1).strip().upper()
                # Убираем мусор
                found_currency = found_currency.replace("*", "").replace("#", "")[:3]
                logger.info(f"💱 Найдена валюта: {found_currency}")
            
            # Ищем сумму ПОСЛЕ этого тега
            start_pos = match_info['end']
            text_after = text[start_pos:start_pos + 200]
            
            # Паттерн для суммы: любое число с точкой или запятой
            amount_pattern = r'>\s*([\d\s.,]+?)\s*<'
            amount_match = re.search(amount_pattern, text_after)
            
            if amount_match:
                amount_str = amount_match.group(1)
                logger.info(f"💰 Найдена сумма (сырая): '{amount_str}'")
                
                # Очищаем
                clean_amount = clean_number(amount_str)
                logger.info(f"💰 Сумма после очистки: '{clean_amount}'")
                
                try:
                    amount = float(clean_amount)
                    
                    # Проверка на адекватность
                    if 1 <= amount <= 1_000_000_000:
                        found_amount = amount
                        logger.info(f"✅ Сумма OK: {amount}")
                        # 🔥 Если есть и сумма и валюта - возвращаем
                        if found_currency:
                            logger.info(f"✅ УСПЕХ: {found_amount} {found_currency}")
                            return found_amount, found_currency
                    else:
                        logger.warning(f"⚠️ Сумма вне диапазона: {amount}")
                except ValueError:
                    logger.warning(f"⚠️ Не удалось преобразовать: '{clean_amount}'")
                    continue
    
    # 🔥 Возвращаем то, что нашли (даже если не полностью)
    if found_amount or found_currency:
        logger.info(f"✅ Частичный успех: {found_amount} {found_currency}")
        return found_amount, found_currency
    
    # 2️⃣ РЕЗЕРВНЫЙ МЕТОД: простой поиск "сумма + валюта"
    # Паттерн: число с пробелами + валюта
    fallback_pattern = r'([\d\s.,]{5,20})\s*([A-Z]{3})'
    
    for match in re.finditer(fallback_pattern, text):
        amount_str = match.group(1)
        currency = match.group(2)
        
        # Проверяем, что это не мусор
        if currency not in ['EUR', 'USD', 'CNY', 'RUB', 'KGS', 'AED', 'KZT']:
            continue
        
        clean_amount = clean_number(amount_str)
        
        try:
            amount = float(clean_amount)
            if 100 <= amount <= 1_000_000_000:  # более строгий диапазон для резервного метода
                logger.info(f"✅ FALLBACK: {amount} {currency}")
                return amount, currency
        except ValueError:
            continue
    
    logger.warning("❌ Сумма не найдена")
    return None, None


def extract_uetr_fuzzy(text: str) -> Optional[str]:
    """
    Извлечение UETR с учетом ошибок OCR
    
    UETR формат: 8-4-4-4-12 символов (UUID)
    Пример: d992f572-0498-4462-ba01-01302f3deb42
    """
    if not text:
        return None
    
    logger.info("🔍 Поиск UETR")
    
    # 1️⃣ Ищем тег UETR
    uetr_tags = fuzzy_find_tag(text, 'UETR', threshold=0.8)
    
    for match_info in uetr_tags:
        logger.info(f"📌 Найден тег UETR: {match_info['match']}")
        
        # Ищем UUID после тега
        start_pos = match_info['end']
        text_after = text[start_pos:start_pos + 300]
        
        # Паттерн UUID: 8-4-4-4-12 hex символов
        uuid_pattern = r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})'
        uuid_match = re.search(uuid_pattern, text_after, re.IGNORECASE)
        
        if uuid_match:
            uetr = uuid_match.group(1).lower()
            logger.info(f"✅ UETR найден: {uetr}")
            return uetr
    
    # 2️⃣ Резервный поиск: просто UUID в тексте
    uuid_pattern = r'\b([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b'
    uuid_match = re.search(uuid_pattern, text, re.IGNORECASE)
    
    if uuid_match:
        uetr = uuid_match.group(1).lower()
        logger.info(f"✅ UETR найден (fallback): {uetr}")
        return uetr
    
    logger.warning("❌ UETR не найден")
    return None


def extract_party_fuzzy(text: str, party_type: str) -> tuple[Optional[str], Optional[str]]:
    """
    Извлечение информации о плательщике/получателе
    
    Args:
        party_type: 'Dbtr' (плательщик) или 'Cdtr' (получатель)
    
    Returns:
        (имя, счет/IBAN)
    """
    if not text:
        return None, None
    
    logger.info(f"🔍 Поиск {party_type}")
    
    # 1️⃣ Ищем тег стороны
    party_tags = fuzzy_find_tag(text, party_type, threshold=0.75)
    
    if not party_tags:
        logger.warning(f"❌ Тег {party_type} не найден")
        return None, None
    
    # Берем лучшее совпадение
    best_match = party_tags[0]
    start_pos = best_match['start']
    
    # Берем текст после тега (следующие 1000 символов)
    party_section = text[start_pos:start_pos + 1000]
    
    logger.info(f"📌 Секция {party_type} найдена")
    
    # 2️⃣ Извлекаем имя (Nm)
    name = None
    nm_tags = fuzzy_find_tag(party_section, 'Nm', threshold=0.7)
    
    if nm_tags:
        nm_match = nm_tags[0]
        # Берем текст после тега
        nm_end = nm_match['end']
        text_after_nm = party_section[nm_end:nm_end + 300]
        
        # 🔥 УЛУЧШЕННОЕ извлечение с учетом разных форматов:
        # 1. <Nm>NAME</Nm>
        # 2. <Nm>"NAME"</Nm>  
        # 3. <NmNAME</Nm> (OCR склеил)
        # 4. <Nm> NAME </Nm>
        
        name_patterns = [
            r'^["\']?\s*([^"\'<>]+?)\s*["\']?\s*<',  # основной паттерн
            r'^([^<]+)<',  # резервный
            r'"([^"]+)"',  # в кавычках
            r'([A-Z][A-Za-z\s.,"&()-]{3,100})',  # просто текст
        ]
        
        for pattern in name_patterns:
            match = re.search(pattern, text_after_nm)
            if match:
                name = match.group(1).strip()
                # Очистка
                name = re.sub(r'\s+', ' ', name)
                name = name.strip('"\'')
                if len(name) >= 3:  # минимум 3 символа
                    logger.info(f"✅ Имя: {name}")
                    break
    
    # 3️⃣ Извлекаем счет/IBAN
    account = None
    
    # Ищем IBAN (начинается с 2 букв и 2 цифр)
    iban_pattern = r'\b([A-Z]{2}\d{2}[A-Z0-9]{11,30})\b'
    iban_match = re.search(iban_pattern, party_section)
    
    if iban_match:
        account = iban_match.group(1)
        logger.info(f"✅ IBAN: {account}")
    else:
        # Ищем просто ID
        id_tags = fuzzy_find_tag(party_section, 'Id', threshold=0.8)
        if id_tags:
            id_match = id_tags[0]
            id_start = id_match['end']
            text_after_id = party_section[id_start:id_start + 200]
            
            content_match = re.search(r'>([^<]+)<', text_after_id)
            if content_match:
                account = content_match.group(1).strip()
                account = re.sub(r'\s+', '', account)
                logger.info(f"✅ Счет: {account}")
    
    return name, account


def extract_description_fuzzy(text: str) -> Optional[str]:
    """
    Извлечение назначения платежа
    
    Ищет теги: Ustrd, RmtInf, AddtlInf
    """
    if not text:
        return None
    
    logger.info("🔍 Поиск описания")
    
    # 1️⃣ Ищем Ustrd (Unstructured)
    ustrd_tags = fuzzy_find_tag(text, 'Ustrd', threshold=0.7)
    
    if ustrd_tags:
        ustrd_match = ustrd_tags[0]
        logger.info(f"📌 Найден тег Ustrd")
        
        end_pos = ustrd_match['end']
        text_after = text[end_pos:end_pos + 500]
        
        # 🔥 УЛУЧШЕННОЕ извлечение содержимого:
        desc_patterns = [
            r'^([^<>]+)<',  # до следующего тега
            r'"([^"]+)"',  # в кавычках
            r'>([^<]+)<',  # между > и <
            r'([A-Z][A-Za-z0-9\s.,()/-]{10,400})',  # просто текст
        ]
        
        for pattern in desc_patterns:
            match = re.search(pattern, text_after)
            if match:
                description = match.group(1).strip()
                description = re.sub(r'\s+', ' ', description)
                if len(description) >= 10:  # минимум 10 символов
                    logger.info(f"✅ Описание: {description[:100]}")
                    return description
    
    # 2️⃣ Ищем RmtInf
    rmtinf_tags = fuzzy_find_tag(text, 'RmtInf', threshold=0.7)
    
    if rmtinf_tags:
        rmtinf_match = rmtinf_tags[0]
        start_pos = rmtinf_match['start']
        
        # Берем весь блок RmtInf
        rmtinf_section = text[start_pos:start_pos + 1000]
        
        # Извлекаем весь текст между RmtInf тегами
        content_match = re.search(r'<RmtInf[^>]*>(.*?)</RmtInf>', rmtinf_section, re.DOTALL)
        if content_match:
            description = content_match.group(1).strip()
            # Убираем внутренние теги
            description = re.sub(r'<[^>]+>', ' ', description)
            description = re.sub(r'\s+', ' ', description)
            if len(description) >= 10:
                logger.info(f"✅ Описание (RmtInf): {description[:100]}")
                return description
    
    logger.warning("❌ Описание не найдено")
    return None


def parse_swift_text_v2(text: str, return_dict: bool = False):
    """
    ОСНОВНАЯ ФУНКЦИЯ ПАРСИНГА v2.0
    
    Возвращает словарь или форматированную строку
    """
    if not text:
        return None
    
    logger.info("=" * 80)
    logger.info("🚀 ПАРСИНГ SWIFT v2.0")
    logger.info("=" * 80)
    
    # Проверка на SWIFT-маркеры
    upper = text.upper()
    swift_markers = ["PACS", "CBPR", "FITOFIC", "ISO 20022", "UETR", "BICFI"]
    hits = sum(1 for k in swift_markers if k in upper)
    
    if hits < 2:
        logger.info("⛔️ Не SWIFT: недостаточно маркеров")
        return None
    
    logger.info(f"✅ SWIFT маркеры: {hits}/6")
    
    # Извлекаем данные
    amount, currency = extract_amount_and_currency_fuzzy(text)
    uetr = extract_uetr_fuzzy(text)
    payer_name, payer_account = extract_party_fuzzy(text, 'Dbtr')
    receiver_name, receiver_account = extract_party_fuzzy(text, 'Cdtr')
    description = extract_description_fuzzy(text)
    
    # Подсчет успешно извлеченных полей
    filled_fields = sum(bool(x) for x in [
        amount, currency, uetr, payer_name, receiver_name, description
    ])
    
    logger.info(f"📊 Извлечено полей: {filled_fields}/6")
    
    if filled_fields < 2:
        logger.warning("⚠️ Недостаточно данных")
        return None
    
    # Формируем результат
    result = {
        "amount": amount,
        "currency": currency,
        "uetr": uetr,
        "payer": payer_name,
        "payer_account": payer_account,
        "receiver": receiver_name,
        "receiver_account": receiver_account,
        "payment_for": description,
    }
    
    if return_dict:
        return result
    
    # Форматируем вывод
    lines = ["💳 SWIFT ПЛАТЁЖ"]
    
    if amount and currency:
        lines.append(f"\n💰 Сумма: {amount:,.2f} {currency}")
    
    if payer_name:
        lines.append(f"\n👤 Плательщик: {payer_name}")
        if payer_account:
            lines.append(f"   Счёт: {payer_account}")
    
    if receiver_name:
        lines.append(f"\n👥 Получатель: {receiver_name}")
        if receiver_account:
            lines.append(f"   Счёт: {receiver_account}")
    
    if description:
        desc_short = description[:150]
        if len(description) > 150:
            desc_short += "..."
        lines.append(f"\n📝 Назначение:\n{desc_short}")
    
    if uetr:
        lines.append(f"\n🔑 UETR:\n{uetr}")
    
    logger.info("=" * 80)
    logger.info("✅ ПАРСИНГ ЗАВЕРШЕН")
    logger.info("=" * 80)
    
    return "\n".join(lines)


# ============================================================
# АЛИАС ДЛЯ ОБРАТНОЙ СОВМЕСТИМОСТИ
# ============================================================

# Позволяет использовать в bot.py как:
# from swift_parser_improved import parse_swift_text
parse_swift_text = parse_swift_text_v2

