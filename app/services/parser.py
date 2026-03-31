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
        "r": "RUB", "р": "RUB",
        "сом": "KGS", "сомов": "KGS", "kgs": "KGS", "c": "KGS", "с": "KGS",
        "usd": "USD", "долл": "USD", "$": "USD", "дол": "USD",
        "доллар": "USD", "долларов": "USD", "долларах": "USD",
        "eur": "EUR", "€": "EUR", "ев": "EUR", "евро": "EUR", "euro": "EUR", "е": "EUR", "e": "EUR",
        "kzt": "KZT", "тенге": "KZT",
        "cny": "CNY", "yuan": "CNY", "¥": "CNY",
        "юан": "CNY", "юань": "CNY", "юаней": "CNY", "юани": "CNY", "юаня": "CNY",
        "ю": "CNY",
        "aed": "AED", "дирхам": "AED", "дирхамов": "AED", "дир": "AED", "dirham": "AED", "dirhams": "AED",
    }

    return curr_map.get(c, c.upper())

def parse_human_number(s: str) -> float:
    """
    Парсит число из человеческого формата. 
    Добавлена защита от некорректных строк и аномальных значений.
    """
    try:
        s = s.strip()
        s = s.replace("\u00A0", " ")
        s = re.sub(r"\s+", "", s)
        
        # Explicitly reject date formats (e.g. 12.03.2026) to prevent them being treated as sums
        if re.fullmatch(r"\d{1,2}[\./-]\d{1,2}[\./-]\d{2,4}", s):
            return 0.0
            
        has_dot = "." in s
        has_comma = "," in s
        
        if has_dot and has_comma:
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", "")
        elif has_dot and not has_comma:
            # Check for 1.234.567 pattern
            if re.fullmatch(r"\d{1,3}(\.\d{3})+", s):
                s = s.replace(".", "")
        elif has_comma and not has_dot:
            # Check for 1,234,567 pattern
            if re.fullmatch(r"\d{1,3}(,\d{3})+", s):
                s = s.replace(",", "")
            else:
                s = s.replace(",", ".")

        val = float(s)
        
        # Sane range check (prevent accidental concatenation of large strings resulting in billions)
        if val > 1_000_000_000:
            logger.warning(f"[Parser] Abnormally large value detected: {val}. Capping to 0.")
            return 0.0
            
        return val
    except Exception as e:
        logger.error(f"[Parser] Failed to parse human number: '{s}' -> {e}")
        return 0.0

def extract_currency_from_str(s: str, default: str = "RUB") -> str:
    """
    Пытается вычленить валюту из строки (например 'евро', 'рск', '$', 'usd').
    """
    low = s.lower()
    if any(x in low for x in ["usd", "$", "доллар", "бакс", "cent"]):
        return "USD"
    if any(x in low for x in ["eur", "€", "евро"]):
        return "EUR"
    if any(x in low for x in ["cny", "юан", "yuan", "rmb", "¥"]):
        return "CNY"
    if any(x in low for x in ["aed", "дирхам"]):
        return "AED"
    if any(x in low for x in ["kzt", "тенге"]):
        return "KZT"
    if any(x in low for x in ["kgs", "сом"]):
        return "KGS"
    if any(x in low for x in ["usdt", "tether"]):
        return "USDT"
    if any(x in low for x in ["rub", "₽", "руб", "рск", "деревян"]):
         return "RUB"
    return default

def parse_residual_balance(text: str) -> Optional[Dict]:
    """
    Пытается найти в тексте объявление "остатка".
    Примеры:
    - "-5021720₽ Ост 95562045₽"
    - "99.899.642руб ост"
    - "40 000евро ост рск"
    Возвращает словарь {"amount": 95562045.0, "currency": "RUB"}
    """
    if not text:
        return None
        
    low = _norm_ws(text).lower()
    
    # Поддерживаем два паттерна: `<число> <валюта> ост` И `Ост <число> <валюта>`
    match = re.search(r"ост(?:аток)?\s*(-?[\d\s.,]+)\s*([a-zа-я$€¥]{0,8})", low)
    if match:
        amount_str = match.group(1).strip()
        if any(c.isdigit() for c in amount_str):
            curr_str = match.group(2)
            amount = parse_human_number(amount_str)
            curr = extract_currency_from_str(curr_str or low)
            return {"amount": amount, "currency": curr}
        
    match_rev = re.search(r"(-?[\d\s.,]+)\s*([a-zа-я$€¥]{0,8})\s*ост(?:аток)?", low)
    if match_rev:
        amount_str = match_rev.group(1).strip()
        if any(c.isdigit() for c in amount_str):
            curr_str = match_rev.group(2)
            amount = parse_human_number(amount_str)
            curr = extract_currency_from_str(curr_str or low)
            return {"amount": amount, "currency": curr}
        
    return None

def parse_multiple_income_notifications(text: str) -> List[Dict]:
    if not text:
        return []

    text = _norm_ws(text)
    low = text.lower()

    if not re.search(r"\b(поступ\w*|зачисл\w*|получен\w*|приход\w*|пришли)\b", low):
        return []

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

    results = []
    
    # Разделяем на сегменты (каждая квитанция часто отделяется //- или \n-)
    segments = re.split(r'(?://-|\n-)', text)
    if len(segments) <= 1:
        segments = [text]
        
    for seg in segments:
        seg_low = seg.lower()
        if not re.search(r"\b(поступ\w*|зачисл\w*|получен\w*|приход\w*|пришли)\b", seg_low):
            continue
            
        m = money_re.search(seg)
        if m:
            amount_str = m.group("amount")
            curr_raw = m.group("curr")
            
            try:
                amount = parse_human_number(amount_str)
                if amount <= 0:
                    continue
                    
                currency = extract_currency_from_str(curr_raw)
                from app.core.config import CURRENCIES
                if currency not in CURRENCIES:
                    continue
                    
                desc_text = seg.strip()
                # Ограничиваем описание, но оставляем полезный контекст
                if len(desc_text) > 150:
                     desc_text = desc_text[:147] + "..."
                     
                results.append({
                    "type": "Поступление",
                    "amount": amount,
                    "currency": currency,
                    "description": f"Авто-приход (SMS/Notif): {desc_text}"
                })
            except Exception as e:
                logger.error(f"[INCOME_PARSE] unexpected error in segment: {e}")
                
    return results


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
        r"(?:.*\s)?(оплата\s*пп)\s+([\d\s.,]+)\s+([a-zа-я$€¥]{2,6})",
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
        r"фикс\s+([\d\s.,]+)\s*([a-zа-я$€¥]{1,10})\s+([\d\s.,]+)\s*([a-zа-я$€¥]{1,10})?",
        t,
        re.IGNORECASE,
    )
    if m:
        # Check if the 4th group (to_currency) exists.
        to_curr = normalize_currency(m.group(4)) if m.group(4) else "RUB"
        
        return {
            "type": "Конвертация",
            "amount": parse_human_number(m.group(1)),
            "currency": normalize_currency(m.group(2)),
            "rate": parse_human_number(m.group(3)),
            "to_currency": to_curr,
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
        r"^\s*(\d+)\s+(.+?)\s+(.+?)\s+([0-9][0-9=\-., ]*)\s+([A-Z]{3})(?:\s+.*)?$"
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

    # исключаем ручные операции и списки платежей
    if t.startswith(("оплата", "взнос", "выдача", "фикс", "запрос", "список платежей")):
        return False
    if "список платежей" in t:
        return False

    # ловим поступ… / зачисл… / приход… / пришли
    income_words = bool(re.search(r"\b(поступ\w*|зачисл\w*|получен\w*|приход\w*|пришли)\b", t))

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


def parse_back_report_payments(text: str, msg_id: Optional[int] = None) -> Dict:
    """
    Парсит список платежей для /back_report.
    Возвращает dict с датой и списком строк.

    Алгоритм (two-pass, устойчивый к):
    - Многословным полям плательщика (Денис Биш, Фин.инфра -СЗ и т.д.)
    - Хвостовым аннотациям после валюты (- еще не подписан)
    """
    if not text:
        return {"date": datetime.now(KG_TZ).strftime("%d.%m.%Y"), "items": [], "msg_id": msg_id}

    KNOWN_GROUPS = ["Денис Биш", "Медигрупп", "Трейд Шоп", "АТЕКС", "Фин.инфра -СЗ",
                    "Профлайн", "УЗ", "Шол", "Трейд"]
    KNOWN_PREFIXES = {
        "денис": "Денис Биш",
        "уз": "УЗ",
        "медигрупп": "Медигрупп",
        "шол": "Шол",
        "трейд шоп": "Трейд Шоп",
        "трейд": "Трейд",
        "атекс": "АТЕКС",
        "фин.инфра": "Фин.инфра -СЗ",
        "профлайн": "Профлайн",
    }

    CURRENCY_RE = re.compile(
        r"(?<!\w)(EUR|USD|CNY|AED|KZT|KGS|RUB|USDT)(?!\w)", re.IGNORECASE
    )
    # Matches: "1. ", "2) ", "1  ", "2  " etc. at start of line
    NUMBERED_LINE_RE = re.compile(r"^\d+(?:[.)]\s+|\s{2,})")

    def norm_group(raw: str) -> str:
        raw = (raw or "").strip()
        low = raw.lower()
        for prefix, canonical in KNOWN_PREFIXES.items():
            if low.startswith(prefix):
                return canonical
        return raw

    def parse_amount_str(s: str) -> float:
        s = s.strip().replace(" ", "").replace("=", "")
        # Convert dash-decimal: 43019-63 → 43019.63
        s = re.sub(r"-(\d+)$", r".\1", s)
        s = s.replace(",", ".")
        try:
            return float(s)
        except Exception:
            return 0.0

    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

    date_str = datetime.now(KG_TZ).strftime("%d.%m.%Y")
    bank_candidate = ""
    company_candidate = ""
    last_non_payment_lines: list = []
    items = []

    # Count naturally-numbered lines to detect parse failures later
    numbered_line_count = sum(1 for ln in lines if NUMBERED_LINE_RE.match(ln))

    for ln in lines:
        # ---- Date header ----
        if "список платежей" in ln.lower():
            m = re.search(r"(\d{2}\.\d{2}\.\d{4})", ln)
            if m:
                date_str = m.group(1)
            continue

        # ---- Is this a payment line? ----
        # Strip optional leading index ("1  ", "2. ")
        bare = NUMBERED_LINE_RE.sub("", ln).strip()

        # Find currency code position (anchor)
        curr_match = CURRENCY_RE.search(bare)
        if not curr_match:
            last_non_payment_lines.append(ln)
            continue

        # Everything before the currency code is "prefix  COUNTERPARTY  AMOUNT"
        pre = bare[: curr_match.start()].strip()
        p_currency = curr_match.group(1).upper()

        # Split prefix into tokens by 2+ spaces (columns are separated by multiple spaces)
        # Typical: "Денис Биш  GUANGDONG MEIAO HOME TECH CO.,LT  43019-63"
        multi_space_parts = re.split(r"\s{2,}", pre)

        if len(multi_space_parts) >= 3:
            # Last token = amount, middle tokens = counterparty, first = group
            group_raw = multi_space_parts[0]
            counterparty_raw = "  ".join(multi_space_parts[1:-1])
            amount_raw = multi_space_parts[-1]
        elif len(multi_space_parts) == 2:
            # Could be "GROUP  AMOUNT" (counterparty missing) or "GROUP+COUNTERPARTY  AMOUNT"
            group_raw = multi_space_parts[0]
            amount_raw = multi_space_parts[-1]
            counterparty_raw = ""
        else:
            # Single block — fall back to last word as amount
            parts = pre.rsplit(None, 1)
            if len(parts) == 2:
                group_raw, amount_raw = parts
                counterparty_raw = ""
            else:
                last_non_payment_lines.append(ln)
                continue

        amt = parse_amount_str(amount_raw)
        # If amount parse failed and counterparty is non-empty, the last counterparty word might be the amount
        if amt == 0.0 and counterparty_raw:
            ct_parts = counterparty_raw.rsplit(None, 1)
            if len(ct_parts) == 2:
                attempt = parse_amount_str(ct_parts[-1])
                if attempt > 0:
                    amt = attempt
                    counterparty_raw = ct_parts[0]

        p_type = norm_group(group_raw)
        p_counterparty = counterparty_raw.strip()

        # Update bank / company context
        num_non_payments = len(last_non_payment_lines)
        if num_non_payments >= 2:
            bank_candidate = last_non_payment_lines[-2]
            company_candidate = last_non_payment_lines[-1]
        elif num_non_payments == 1:
            company_candidate = last_non_payment_lines[0]
        last_non_payment_lines = []

        items.append({
            "bank": bank_candidate,
            "company": company_candidate,
            "type": p_type,
            "counterparty": p_counterparty,
            "currency": p_currency,
            "sum": amt,
        })

    # Sanity check: warn if we parsed fewer items than there were numbered lines
    if numbered_line_count > 0 and len(items) < numbered_line_count:
        logger.warning(
            f"[parse_back_report_payments] Parsed {len(items)} items but found "
            f"{numbered_line_count} numbered lines in text — possible missed records!"
        )

    return {"date": date_str, "items": items, "msg_id": msg_id}

def parse_implicit_conversion(text: str, reply_text: str) -> Optional[Dict]:
    """
    Парсит неявную конвертацию, когда пользователь отвечает на сообщение с курсом.
    text (текущее сообщение): сумма, которую нужно поменять (например, "7803")
    reply_text (исходное сообщение): курс (например, "82.80")
    """
    if not text or not reply_text:
        return None
        
    # Пытаемся распарсить число из исходного сообщения (курс)
    try:
        # Извлекаем первое попавшееся число из reply_text
        m_rate = re.search(r"([\d.,]+)", reply_text)
        if not m_rate:
            return None
        rate = parse_human_number(m_rate.group(1))
    except Exception:
        return None
        
    # Пытаемся распарсить число из текущего сообщения (сумма ин. валюты)
    # Текущее сообщение должно в основном состоять из цифр, возможно со знаками
    t_clean = _norm_ws(text).strip()
    if not re.match(r"^[\d\s.,]+(?:[a-zа-я]{1,5})?$", t_clean.lower()):
         # If text is not just a number (maybe with small currency suffix), ignore
         return None
         
    try:
        m_amount = re.search(r"([\d.,]+)", t_clean)
        if not m_amount:
            return None
        amount = parse_human_number(m_amount.group(1))
    except Exception:
        return None
        
    # Эвристика определения валюты по курсу
    # Юань: 10 .. 20
    # Доллар: 70 .. 92
    # Евро: 93 .. 120
    # Тенге: 0.1 .. 0.5 (редко)
    # AED: 20 .. 30
    
    currency = "RUB" # Default
    if 10.0 <= rate <= 15.0:
        currency = "CNY"
    elif 70.0 <= rate <= 92.9:
        currency = "USD"
    elif 93.0 <= rate <= 120.0:
        currency = "EUR"
    elif 23.0 <= rate <= 30.0:
        currency = "AED"
    elif 0.1 <= rate <= 5.0:
        currency = "KZT"
    else:
        # Не смогли надежно определить валюту по курсу
        logger.warning(f"Не удалось определить валюту для курса {rate}. Используем USD по умолчанию.")
        currency = "USD"
        
    # Возвращаем структуру, аналогичную "Фикс"
    return {
        "type": "Конвертация",
        "amount": amount,
        "currency": currency,
        "rate": rate,
        "to_currency": "RUB", # По умолчанию конвертируем в/из RUB
        "description": "Фикс (авто)"
    }

def is_rate_message(text: str) -> bool:
    """
    Проверяет, является ли текст просто курсом (число, опционально с валютой).
    Например: "83", "95", "11.95", "11.4 юань", "95 евро".
    """
    if not text:
        return False
        
    t = _norm_ws(text).strip().lower()
    
    # Регулярка для курса:
    # 1. Одно число (возможно десятичное).
    # 2. Опциональная короткая валюта (юань, евро, usd, rub и т.д.) после числа.
    # Больше ничего быть не должно в строке.
    
    # Только число: "83", "11.95", "11,95"
    if re.fullmatch(r"[\d.,]+", t):
        try:
            val = parse_human_number(t)
            return val > 0
        except:
            return False
            
    # Число + валюта: "11.4 юань", "95 евро", "83 usd"
    # Допускаем пробел между числом и валютой. Ограничиваем длину валюты.
    match = re.fullmatch(r"([\d.,]+)\s*([a-zа-я$€¥]{1,10})", t)
    if match:
        try:
            val = parse_human_number(match.group(1))
            if val <= 0:
                return False
            
            # Проверяем, что кусок текста похож на валюту
            curr_str = match.group(2)
            # Если после нормализации это не просто пустая строка и есть маппинг
            # Но мы можем просто довериться extract_currency_from_str
            extracted = extract_currency_from_str(curr_str)
            # Даже если defaulst "RUB", это окей. Главное что строка короткая и это одно слово.
            return True
        except:
            return False
            
    return False

def is_date_or_doc_number(text: str) -> bool:
    """Checks if the text is just a date, username, or document number to avoid AI prompts."""
    if not text:
        return False
        
    t = _norm_ws(text).strip()
    
    # Check for just a username
    if re.fullmatch(r"@[a-zA-Z0-9_]+", t):
        return True
        
    # Check for date (e.g. 17.03.2026, 17.03)
    if re.fullmatch(r"\d{1,2}[\./]\d{1,2}(?:[\./]\d{2,4})?", t):
        return True
        
    # Check for document number (e.g. № 12345, n123, doc 44)
    if re.search(r"^(?:№|n|doc|док|номер|документ)\s*[\d\-a-zA-Zа-яА-Я]+$", t.lower()):
        return True
        
    return False
