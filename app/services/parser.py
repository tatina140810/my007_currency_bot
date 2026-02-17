"""
Сервис парсинга текста и команд
"""
import re
import logging
from typing import Optional, Dict, List, Tuple

from datetime import datetime, timezone
from app.core.constants import GROUP_TAG_RE, CHAT_ALIASES, KG_TZ
from app.core.logger import logger

def parse_timestamp(ts: str | datetime) -> datetime:
    """Парсит временную метку с часовым поясом"""
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


def extract_client_name(text: str) -> str:
    """
    Пример текста:
    "...-Плательщик ООО \\"АВТОЦЕНТРГАЗ-РУСАВТО\\"- ЕВРО АВТО"
    Вернет: "ЕВРО АВТО"

    Логика:
    1) берем хвост после последнего дефиса (- или —) ближе к концу
    2) чистим пробелы/переводы строк
    """
    if not text:
        return "Без клиента"

    t = " ".join(str(text).split())  # нормализация пробелов/переносов

    # хвост после последнего " - " или "—" в конце
    m = re.search(r"(?:\s*[-—]\s*)([^-—]{2,})\s*$", t)
    if m:
        name = m.group(1).strip()
        return name or "Без клиента"

    return "Без клиента"


def _norm_ws(s: str) -> str:
    if not s:
        return ""
    # неразрывные/тонкие пробелы -> обычные
    return s.replace("\u00A0", " ").replace("\u202F", " ")

def extract_group_tag(text: str) -> Tuple[Optional[str], str]:
    """
    Извлекает группу из квадратных скобок.
    
    Примеры:
        "[УЗ] поступили 5000 usdt" → ("УЗ", "поступили 5000 usdt")
        "поступили 5000 usdt" → (None, "поступили 5000 usdt")
    """
    if not text:
        return None, text

    m = GROUP_TAG_RE.match(text)
    if not m:
        return None, text

    group = m.group(1).strip()
    clean_text = m.group(2).strip()
    return group, clean_text

def normalize_group_name(name: str) -> str:
    """
    Нормализует название группы через CHAT_ALIASES.
    """
    if not name:
        return ""

    n = name.strip().lower()

    # Проверяем каноническое название и все алиасы
    for canonical, aliases in CHAT_ALIASES.items():
        if n == canonical.lower():
            return canonical
        for alias in aliases:
            if n == alias.lower():
                return canonical

    return name.strip()

def normalize_currency(curr: str) -> str:
    """Нормализует валюту (без ошибок USDT → USD)"""
    if not curr:
        return ""

    c = curr.strip().lower()
    c = c.replace(".", "").replace(",", "").strip()

    if c in ("usdt", "тез", "тезер"):
        return "USDT"

    curr_map = {
        "руб": "RUB", "₽": "RUB", "рублей": "RUB", "rub": "RUB", "рубля": "RUB", "рубли": "RUB", "rubles": "RUB",
        "сом": "KGS", "сомов": "KGS", "kgs": "KGS",
        "usd": "USD", "долл": "USD", "$": "USD", "дол": "USD",
        "доллар": "USD", "долларов": "USD", "долларах": "USD",
        "eur": "EUR", "€": "EUR", "ев": "EUR", "евро": "EUR", "euro": "EUR",
        "kzt": "KZT", "тенге": "KZT",
        "cny": "CNY", "yuan": "CNY", "¥": "CNY",
        "юан": "CNY", "юань": "CNY", "юаней": "CNY", "юани": "CNY", "юаня": "CNY",
        "aed": "AED", "дирхам": "AED", "дирхамов": "AED", "дир": "AED", "dirham": "AED", "dirhams": "AED",
    }

    return curr_map.get(c, c.upper())

def parse_human_number(s: str) -> float:
    """Парсит число из человеческого формата"""
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
        # If it looks like 1.234.567 -> thousands
        if re.fullmatch(r"\d{1,3}(\.\d{3}){2,}", s):
            s = s.replace(".", "")
            return float(s)
        # If it looks like 1.234 -> Could be 1234 or 1.234. 
        # In currency context, usually 2 decimals. 
        # But if we have explicit "1.234", it is ambiguous.
        # However, for this specific bot, user inputs "6 140,00".
        # Let's assume if it is a small number (one group of 3 digits), it is decimal.
        # Or better: check context. But here we only have string.
        # Let's default to float if simple dot.
        return float(s)
    
    if has_comma and not has_dot:
        if re.fullmatch(r"\d{1,3}(,\d{3})+", s):
            s = s.replace(",", "")
            return float(s)
        s = s.replace(",", ".")
        return float(s)
    
    return float(s)

def parse_income_notification(text: str) -> Optional[Dict]:
    if not text:
        return None

    text = _norm_ws(text)
    low = text.lower()

    if not re.search(r"\b(поступ\w*|зачисл\w*|получен\w*)\b", low):
        return None

    money_re = re.compile(
        r"(?P<amount>\d[\d\s\u00A0\u202F]*(?:[.,]\d{1,2})?)\s*"
        r"(?P<curr>"
        r"₽|r\.?|руб(?:\.|ля|лей)?|rub|RUB|"
        r"сом(?:\.|ов)?|kgs|"
        r"usdt|usd|\$|"
        r"eur|€|"
        r"kzt|"
        r"cny|юан(?:ь|я|ей)?|¥|"
        r"aed|дирх(?:ам|ама|амов)?"
        r")\b",
        re.IGNORECASE,
    )

    m = money_re.search(text)
    if not m:
        return None

    amount_str = m.group("amount")
    curr_raw = m.group("curr")

    try:
        amount = parse_human_number(amount_str)
    except Exception:
        logger.exception(f"[INCOME_PARSE] bad amount: {amount_str!r}")
        return None

    currency = normalize_currency(curr_raw)

    return {
        "amount": float(amount),
        "currency": currency,
        "description": text.strip(),
    }


def parse_manual_operation_line(text: str) -> Optional[Dict]:
    """
    Парсит РУЧНЫЕ операции.
    """
    if not text:
        return None

    t = text.lower().strip()

    # MANUAL BUY FX: [internal_report] <AMOUNT> <CURRENCY> <RATE>
    # Example: [internal_report] 69000 EUR 91.8
    m = re.search(
        r"\[internal_report\]\s+([\d.,]+)\s+([a-zа-я$€¥]{2,6})\s+([\d.,]+)",
        t,
    )
    if m:
        return {
            "type": "Manual Buy FX",
            "amount": parse_human_number(m.group(1)),
            "currency": normalize_currency(m.group(2)),
            "rate": parse_human_number(m.group(3)),
            "description": f"FX: Buy {normalize_currency(m.group(2))} rate {parse_human_number(m.group(3))}",
        }

    # CASH WITHDRAWAL: [internal_report] наличные <AMOUNT> <CURRENCY>
    # Example: [internal_report] наличные 5000 USD
    m = re.search(
        r"\[internal_report\]\s+наличные\s+([\d.,]+)\s+([a-zа-я$€¥]{2,6})",
        t,
    )
    if m:
        return {
            "type": "Выдача наличных",
            "amount": parse_human_number(m.group(1)),
            "currency": normalize_currency(m.group(2)),
            "description": "Выдача наличных (internal_report)",
        }

    # ВОЗВРАТ ПО ПП (формат: Сумма Валюта - Возврат пп ...)
    # Пример: 6 140,00 долл - Возврат пп на Бакай ...
    m = re.search(
        r"^([\d\s.,]+)\s+([a-zа-я$€¥]{2,6})\s*[-–—]\s*(возврат\s*пп.*)",
        t,
    )
    if m:
        return {
            "type": "Возврат по ПП",
            "amount": parse_human_number(m.group(1)),
            "currency": normalize_currency(m.group(2)),
            "description": m.group(3).capitalize(), # Extract description starting from "Возврат пп..."
        }

    # ПОСТУПЛЕНИЕ
    m = re.search(
        r"(поступили|поступило|пришли)\s+([\d\s.,]+)\s+([a-zа-я$€¥]{2,6})",
        t,
    )
    if m:
        return {
            "type": "Поступление",
            "amount": parse_human_number(m.group(2)),
            "currency": normalize_currency(m.group(3)),
            "description": "Поступление (ручное)",
        }

    # ВЗНОС НАЛИЧНЫМИ
    m = re.search(
        r"(взнос\s+наличными)\s+([\d\s.,]+)\s+([a-zа-я$€¥]{2,6})",
        t,
    )
    if m:
        return {
            "type": "Взнос наличными",
            "amount": parse_human_number(m.group(2)),
            "currency": normalize_currency(m.group(3)),
            "description": "Взнос наличными",
        }

    # ВЫДАЧА
    m = re.search(
        r"(выдача|выдали|выдано)\s+([\d\s.,]+)\s+([a-zа-я$€¥]{2,6})",
        t,
    )
    if m:
        return {
            "type": "Выдача наличных",
            "amount": parse_human_number(m.group(2)),
            "currency": normalize_currency(m.group(3)),
            "description": "Выдача",
        }

    # ОПЛАТА ПП
    m = re.search(
        r"(оплата\s*пп)\s+([\d\s.,]+)\s+([a-zа-я$€¥]{2,6})",
        t,
    )
    if m:
        return {
            "type": "Оплата ПП",
            "amount": parse_human_number(m.group(2)),
            "currency": normalize_currency(m.group(3)),
            "description": "Оплата ПП",
        }

    # ФИКС (КОНВЕРТАЦИЯ)
    m = re.search(
        r"фикс\s+([\d\s.,]+)\s*([a-zа-я$€¥]{1,10})\s+([\d\s.,]+)\s*([a-zа-я$€¥]{1,10})",
        t,
        re.IGNORECASE,
    )
    if m:
        return {
            "type": "Конвертация",
            "amount": parse_human_number(m.group(1)),
            "currency": normalize_currency(m.group(2)),
            "rate": parse_human_number(m.group(3)),
            "to_currency": normalize_currency(m.group(4)),
            "description": "Фикс",
        }

    # ХАРБОР КОМИССИЯ
    m = re.search(
        r"(харбор\s+комиссия)\s+([\d\s.,]+)\s+([a-zа-я$€¥]{2,6})",
        t,
    )
    if m:
        return {
            "type": "Комиссия",
            "amount": parse_human_number(m.group(2)),
            "currency": normalize_currency(m.group(3)),
            "description": "Харбор комиссия",
        }

    # ЗАПРОС БАНКУ
    m = re.search(
        r"(запрос\s+банку)\s+([\d\s.,]+)\s+([a-zа-я$€¥]{2,6})",
        t,
    )
    if m:
        return {
            "type": "Комиссия",
            "amount": parse_human_number(m.group(2)),
            "currency": normalize_currency(m.group(3)),
            "description": "Запрос банку",
        }

    return None

def parse_bulk_pp_payments(clean_text: str) -> List[Dict]:
    """Парсит bulk-списки платежей"""
    if not clean_text:
        return []

    lines = [ln.strip() for ln in clean_text.splitlines() if ln.strip()]
    items = []
    current_company = None

    company_header_re = re.compile(
        r"^[А-Яа-яA-Za-z0-9().\- ]{2,}:\s*$|^[А-Яа-яA-Za-z0-9().\- ]{2,}$"
    )

    pay_re = re.compile(
        r"^\s*(\d+)\s+(.+?)\s{2,}(.+?)\s{2,}([0-9][0-9=\-., ]*)\s+([A-Z]{3})\s*$"
    )

    def norm_group(raw: str) -> str:
        raw = (raw or "").strip()
        low = raw.lower()
        if low.startswith("денис"):
            return "Денис Биш"
        if low.startswith("уз"):
            return "УЗ"
        if low.startswith("медигрупп"):
            return "Медигрупп"
        return raw

    def parse_amount(raw: str) -> float:
        s = raw.strip().replace("=", "").replace(" ", "")
        if "-" in s and s.count("-") == 1 and s.rsplit("-", 1)[1].isdigit():
            left, right = s.rsplit("-", 1)
            s = f"{left}.{right}"
        if "," in s and "." in s:
            s = s.replace(",", "")
        else:
            s = s.replace(",", ".")
        return float(s)

    for ln in lines:
        m = pay_re.match(ln)
        if m:
            _num, left_block, receiver, amount_raw, currency = m.groups()

            group_name = norm_group(left_block)
            amount = parse_amount(amount_raw)

            items.append({
                "company": current_company or "",
                "group": group_name,
                "receiver": receiver.strip(),
                "amount": amount,
                "currency": currency,
            })
            continue

        if "список платежей" in ln.lower():
            continue

        if company_header_re.match(ln):
            current_company = ln.rstrip(":").strip()
            continue

    return items

def looks_like_bank_income(text: str) -> bool:
    t = _norm_ws(text or "").lower().strip()

    # исключаем ручные операции
    if t.startswith(("оплата", "взнос", "выдача", "фикс", "запрос")):
        return False

    # ловим поступ… / зачисл…
    income_words = bool(re.search(r"\b(поступ\w*|зачисл\w*|получен\w*)\b", t))

    bank_markers = any(k in t for k in (
        "перевод spfs", "перевод finline", "согл. п.п.", "п.п.",
        "отпр.", "отпр ", "отправ", "ooo", "ооо", "osoo",
        "mcrb", "sb", "mti", "vo", "rs", "р/с", "инн", "банк", "bank",
    ))

    has_currency = bool(re.search(
        r"(₽|\brub\b|\brub\.?\b|\brubль\w*\b|\brubлей\b|\brubля\b|"
        r"\brub\b|\brub\.?\b|\brub(?:\.|ля|лей)?\b|"
        r"\brub\b|\brub\.?\b|"
        r"\brub\b|"
        r"\brub\b|"
        r"руб|₽|RUB|usd|\$|eur|€|сом|kgs|cny|¥|kzt|aed|usdt)",
        t, re.IGNORECASE
    ))

    return (income_words and has_currency) or (bank_markers and has_currency)
