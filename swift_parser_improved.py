#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Улучшенный парсер SWIFT (pacs.008) документов
Извлекает: отправителя, получателя, UETR, сумму, валюту, описание
"""

import re
import logging
from typing import Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SwiftPayment:
    """Структура платежа SWIFT"""
    amount: float
    currency: str
    uetr: Optional[str] = None
    sender: Optional[str] = None
    receiver: Optional[str] = None
    sender_account: Optional[str] = None
    receiver_account: Optional[str] = None
    description: Optional[str] = None
    reference: Optional[str] = None
    
    def __str__(self):
        lines = []
        lines.append(f"💰 СУММА: {self.amount:,.2f} {self.currency}")
        
        if self.uetr:
            lines.append(f"🔑 UETR: {self.uetr}")
        
        if self.sender:
            lines.append(f"📤 ОТПРАВИТЕЛЬ: {self.sender}")
            if self.sender_account:
                lines.append(f"   Счёт: {self.sender_account}")
        
        if self.receiver:
            lines.append(f"📥 ПОЛУЧАТЕЛЬ: {self.receiver}")
            if self.receiver_account:
                lines.append(f"   Счёт: {self.receiver_account}")
        
        if self.reference:
            lines.append(f"📋 Ссылка: {self.reference}")
        
        if self.description:
            # Ограничиваем описание 150 символами
            desc = self.description[:150]
            if len(self.description) > 150:
                desc += "..."
            lines.append(f"📝 Описание: {desc}")
        
        return "\n".join(lines)


def clean_text(text: str) -> str:
    """Очистка текста от лишних пробелов и переносов"""
    if not text:
        return ""
    # Убираем множественные пробелы
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def extract_between_tags(text: str, tag: str) -> Optional[str]:
    """
    Извлекает содержимое между XML тегами
    Примеры:
        <UETR>abc123</UETR> → "abc123"
        <Nm>John Doe</Nm> → "John Doe"
    """
    if not text or not tag:
        return None
    
    # Пробуем разные варианты тегов (с учётом возможных пробелов)
    patterns = [
        rf'<{tag}\s*>([^<]+)</{tag}\s*>',  # <Tag>content</Tag>
        rf'<{tag}>([^<]+)<',                # <Tag>content<
        rf'{tag}\s*>\s*([^<]+)',            # Tag>content
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
        if match:
            content = match.group(1).strip()
            if content:
                return clean_text(content)
    
    return None


def fix_ocr_errors(text: str) -> str:
    """
    Исправляет типичные ошибки OCR в SWIFT документах
    """
    if not text:
        return text
    
    # Типичные замены букв
    replacements = {
        # Часто путаемые валюты
        'Ccy=BUR': 'Ccy=EUR',  # Конкретная замена
        'Ccy=BURO': 'Ccy=EUR',
        'Ccy=BUH': 'Ccy=EUR',
        'Ccy=USO': 'Ccy=USD',
        'Ccy=USP': 'Ccy=USD',
        
        # Тэги
        'BICFI>': 'BICFI>',
        'IntrBkSttlmAmt': 'IntrBkSttlmAmt',
        'InstdAmt': 'InstdAmt',
        'DBTR': 'Dbtr',
        'CDTR': 'Cdtr',
        
        # UETR (часто путают 0 и O)
        'OUETR': 'UETR',
        'UETR0': 'UETR>',
    }
    
    for old, new in replacements.items():
        text = text.replace(old, new)
    
    return text


def extract_amount_and_currency(text: str) -> tuple[Optional[float], Optional[str]]:
    """
    Извлекает сумму и валюту из SWIFT текста
    
    Ищет паттерны:
    - <IntrBkSttlmAmt Ccy="EUR">118028.80</IntrBkSttlmAmt>
    - <InstdAmt Ccy="EUR">118028.80</InstdAmt>
    - Ccy="EUR">118028.80
    - Ccy=EUR>118028.80 (без кавычек - ошибка OCR)
    - Ccy=BUR>118028.80 (B вместо E - ошибка OCR)
    """
    if not text:
        return None, None
    
    logger.info(f"Ищу сумму в тексте длиной {len(text)} символов...")
    
    # Основные паттерны для сумм в SWIFT
    patterns = [
        # <IntrBkSttlmAmt Ccy="EUR">118028.80</IntrBkSttlmAmt>
        (r'<IntrBkSttlmAmt\s+Ccy="([A-Z]{3})"\s*>(\d+(?:[.,]\d+)?)', 'IntrBk с кавычками'),
        
        # <InstdAmt Ccy="EUR">118028.80</InstdAmt>
        (r'<InstdAmt\s+Ccy="([A-Z]{3})"\s*>(\d+(?:[.,]\d+)?)', 'Instd с кавычками'),
        
        # Без кавычек (OCR ошибка): <IntrBkSttlmAmt Ccy=EUR>118028.80
        (r'<IntrBkSttlmAmt\s+Ccy=([A-Z]{3})\s*>(\d+(?:[.,]\d+)?)', 'IntrBk без кавычек'),
        (r'<InstdAmt\s+Ccy=([A-Z]{3})\s*>(\d+(?:[.,]\d+)?)', 'Instd без кавычек'),
        
        # Без тегов: IntrBkSttlmAmt Ccy=EUR>118028.80
        (r'IntrBkSttlmAmt\s+Ccy=([A-Z]{3})\s*>(\d+(?:[.,]\d+)?)', 'IntrBk без < >'),
        (r'InstdAmt\s+Ccy=([A-Z]{3})\s*>(\d+(?:[.,]\d+)?)', 'Instd без < >'),
        
        # С кавычками без тегов
        (r'IntrBkSttlmAmt\s+Ccy="([A-Z]{3})"\s*>(\d+(?:[.,]\d+)?)', 'IntrBk без < >, с кавычками'),
        (r'InstdAmt\s+Ccy="([A-Z]{3})"\s*>(\d+(?:[.,]\d+)?)', 'Instd без < >, с кавычками'),
        
        # Упрощённый паттерн
        (r'Ccy="([A-Z]{3})"\s*>(\d+(?:[.,]\d+)?)', 'Просто Ccy с кавычками'),
        (r'Ccy=([A-Z]{3})\s*>(\d+(?:[.,]\d+)?)', 'Просто Ccy без кавычек'),
    ]
    
    for pattern, description in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            logger.info(f"  Паттерн '{description}' сработал!")
            currency = match.group(1).upper()
            amount_str = match.group(2)
            
            logger.info(f"  Исходная валюта: {currency}, сумма: {amount_str}")
            
            # Исправляем типичные OCR ошибки в валютах
            currency_fixes = {
                'BUR': 'EUR',  # B вместо E
                'BURO': 'EUR',
                'BUH': 'EUR',
                'USO': 'USD',  # O вместо D
                'USP': 'USD',
            }
            original_currency = currency
            currency = currency_fixes.get(currency, currency)
            
            if currency != original_currency:
                logger.info(f"  Исправлена валюта: {original_currency} → {currency}")
            
            try:
                # Нормализуем сумму
                amount_str = amount_str.replace(' ', '').replace(',', '.')
                amount = float(amount_str)
                
                logger.info(f"  Проверка валидации: сумма={amount}, валюта={currency}")
                
                # Валидация
                valid_currencies = [
                    'EUR', 'USD', 'GBP', 'CHF', 'JPY', 'CNY', 'RUB', 
                    'KGS', 'KZT', 'AED', 'TRY', 'INR'
                ]
                
                if currency not in valid_currencies:
                    logger.warning(f"  Валюта {currency} не в списке валидных")
                    continue
                
                if not (0.01 <= amount <= 100_000_000):
                    logger.warning(f"  Сумма {amount} вне допустимого диапазона")
                    continue
                
                logger.info(f"✅ Сумма найдена: {amount} {currency}")
                return amount, currency
                    
            except ValueError as e:
                logger.warning(f"  Ошибка преобразования суммы: {e}")
                continue
    
    logger.warning("⚠️ Сумма не найдена в документе")
    return None, None


def extract_uetr(text: str) -> Optional[str]:
    """
    Извлекает UETR (Unique End-to-End Transaction Reference)
    
    Формат: 8-4-4-4-12 символов (UUID)
    Пример: 65cc99f6-e3ca-4346-8631-b75dcfd0829a
    """
    if not text:
        return None
    
    # Паттерн для UUID
    pattern = r'<UETR>([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})</UETR>'
    match = re.search(pattern, text, re.IGNORECASE)
    
    if match:
        uetr = match.group(1).lower()
        logger.info(f"✅ UETR: {uetr}")
        return uetr
    
    # Альтернативный поиск (без тегов)
    pattern2 = r'\b([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\b'
    match2 = re.search(pattern2, text, re.IGNORECASE)
    
    if match2:
        uetr = match2.group(1).lower()
        logger.info(f"✅ UETR (без тегов): {uetr}")
        return uetr
    
    logger.warning("⚠️ UETR не найден")
    return None


def extract_party_info(text: str, party_tag: str) -> tuple[Optional[str], Optional[str]]:
    """
    Извлекает информацию о стороне (отправитель/получатель)
    
    Args:
        party_tag: 'Dbtr' (отправитель) или 'Cdtr' (получатель)
    
    Returns:
        (имя, счёт/IBAN)
    """
    if not text or not party_tag:
        return None, None
    
    # Ищем секцию отправителя/получателя
    party_section = re.search(
        rf'<{party_tag}>(.+?)</{party_tag}>',
        text,
        re.IGNORECASE | re.DOTALL
    )
    
    if not party_section:
        # Альтернативный поиск (без закрывающего тега)
        party_section = re.search(
            rf'<{party_tag}>(.{{1,500}})',
            text,
            re.IGNORECASE | re.DOTALL
        )
    
    if not party_section:
        logger.warning(f"⚠️ Секция {party_tag} не найдена")
        return None, None
    
    party_text = party_section.group(1)
    
    # Извлекаем имя (Nm или Name)
    name = None
    for tag in ['Nm', 'Name']:
        name = extract_between_tags(party_text, tag)
        if name:
            break
    
    # Извлекаем счёт/IBAN
    account = None
    
    # 1. Ищем IBAN
    iban_match = re.search(r'<IBAN>([A-Z0-9]{15,34})</IBAN>', party_text, re.IGNORECASE)
    if iban_match:
        account = iban_match.group(1)
    else:
        # 2. Ищем просто IBAN в тексте
        iban_match2 = re.search(r'\b([A-Z]{2}\d{2}[A-Z0-9]{11,30})\b', party_text)
        if iban_match2:
            account = iban_match2.group(1)
    
    if not account:
        # 3. Ищем ID счёта
        account = extract_between_tags(party_text, 'Id')
    
    if name:
        logger.info(f"✅ {party_tag}: {name}" + (f" ({account})" if account else ""))
    
    return name, account


def extract_description(text: str) -> Optional[str]:
    """
    Извлекает описание платежа
    
    Ищет в тегах:
    - <Ustrd>
    - <RmtInf>
    - <AddtlInf>
    """
    if not text:
        return None
    
    # 1. Ищем Ustrd (Unstructured remittance info)
    desc = extract_between_tags(text, 'Ustrd')
    if desc:
        logger.info(f"✅ Описание (Ustrd): {desc[:50]}...")
        return desc
    
    # 2. Ищем RmtInf (Remittance Information)
    rmtinf_section = re.search(
        r'<RmtInf>(.+?)</RmtInf>',
        text,
        re.IGNORECASE | re.DOTALL
    )
    if rmtinf_section:
        desc = clean_text(rmtinf_section.group(1))
        logger.info(f"✅ Описание (RmtInf): {desc[:50]}...")
        return desc
    
    # 3. Ищем AddtlInf (Additional Information)
    desc = extract_between_tags(text, 'AddtlInf')
    if desc:
        logger.info(f"✅ Описание (AddtlInf): {desc[:50]}...")
        return desc
    
    logger.warning("⚠️ Описание не найдено")
    return None


def extract_reference(text: str) -> Optional[str]:
    """Извлекает референс платежа"""
    if not text:
        return None
    
    # Message ID
    ref = extract_between_tags(text, 'MsgId')
    if ref:
        return ref
    
    # Payment ID
    ref = extract_between_tags(text, 'PmtId')
    if ref:
        return ref
    
    # Instruction ID
    ref = extract_between_tags(text, 'InstrId')
    if ref:
        return ref
    
    return None


def parse_swift_text(text: str) -> Optional[str]:
    """
    Главная функция парсинга SWIFT документа
    
    Args:
        text: OCR текст документа
    
    Returns:
        Форматированное сообщение или None
    """
    if not text:
        logger.warning("Пустой текст для парсинга")
        return None
    
    logger.info("=" * 60)
    logger.info("🔍 ПАРСИНГ SWIFT ДОКУМЕНТА")
    logger.info("=" * 60)
    
    # Исправляем типичные ошибки OCR
    text = fix_ocr_errors(text)
    
    # Проверяем что это SWIFT
    if not any(marker in text.lower() for marker in [
        'pacs.008', 'fitoficstmr', 'cbprplus', 'bicfi', 'uetr',
        'intrbksttlmamt', 'instdamt', 'dbtr', 'cdtr'
    ]):
        logger.warning("❌ Не похоже на SWIFT документ")
        return None
    
    # Извлекаем данные
    amount, currency = extract_amount_and_currency(text)
    
    if not amount or not currency:
        logger.error("❌ Не удалось извлечь сумму и валюту")
        return None
    
    uetr = extract_uetr(text)
    sender, sender_account = extract_party_info(text, 'Dbtr')
    receiver, receiver_account = extract_party_info(text, 'Cdtr')
    description = extract_description(text)
    reference = extract_reference(text)
    
    # Создаём объект платежа
    payment = SwiftPayment(
        amount=amount,
        currency=currency,
        uetr=uetr,
        sender=sender,
        receiver=receiver,
        sender_account=sender_account,
        receiver_account=receiver_account,
        description=description,
        reference=reference,
    )
    
    logger.info("=" * 60)
    logger.info("✅ SWIFT УСПЕШНО РАСПОЗНАН")
    logger.info("=" * 60)
    
    return str(payment)


def parse_swift_pages(texts: list[str]) -> list[str]:
    """
    Парсит несколько страниц SWIFT документа
    
    Args:
        texts: список OCR текстов
    
    Returns:
        список форматированных сообщений
    """
    results = []
    
    for i, text in enumerate(texts, 1):
        logger.info(f"\n📄 Обработка страницы {i}/{len(texts)}")
        result = parse_swift_text(text)
        if result:
            results.append(result)
    
    return results


if __name__ == "__main__":
    # Тест парсера
    test_text = """
    <IntrBkSttlmAmt Ccy="EUR">118028.80</IntrBkSttlmAmt>
    <UETR>65cc99f6-e3ca-4346-8631-b75dcfd0829a</UETR>
    <Dbtr>
        <Nm>SEDEP TRADE LLC</Nm>
    </Dbtr>
    <Cdtr>
        <Nm>UAB DINAURAS</Nm>
        <IBAN>PL94109027760000001525552835</IBAN>
    </Cdtr>
    <Ustrd>PAYMENT FOR AGRICULTURAL GOODS</Ustrd>
    """
    
    logging.basicConfig(level=logging.INFO)
    result = parse_swift_text(test_text)
    if result:
        print("\n" + "=" * 60)
        print("РЕЗУЛЬТАТ:")
        print("=" * 60)
        print(result)
