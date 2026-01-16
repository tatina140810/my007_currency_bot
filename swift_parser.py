# swift_parser.py
import logging
import re

logger = logging.getLogger(__name__)


def extract_amount_and_currency(text: str):
    """
    Поиск суммы и валюты в SWIFT/ISO20022 (pacs.008 и т.п.), устойчивый к OCR-ошибкам,
    но привязанный только к тегам *Amt (чтобы не цеплять UETR и похожие числа).
    Возвращает: (currency, amount_str) или (None, None)
    """
    logger.info("Поиск суммы и валюты в тексте")

    valid_currencies = {
        "EUR", "USD", "GBP", "RUB", "CNY", "JPY", "CHF",
        "KZT", "KGS", "AED", "TRY", "INR",
    }

    curr_fix = {
        "ETR": "EUR", "EUP": "EUR", "FUR": "EUR", "EURR": "EUR", "EUR0": "EUR",
        "USO": "USD", "US0": "USD", "USDL": "USD", "USDD": "USD", "USD0": "USD",
        "GBP0": "GBP", "GBPP": "GBP",
        "RUB0": "RUB", "RUBB": "RUB",
        "CNY0": "CNY", "CNYY": "CNY",
        "KZT0": "KZT", "KZTT": "KZT",
        "KGS0": "KGS",
    }

    xml_patterns = [
        r'<IntrBkSttlmAmt[^>]*C[co]y\s*=\s*["\']?(?P<ccy>[A-Za-z]{3})["\']?[^>]*>(?P<amt>[\d\s.,]+)<',
        r'<InstdAmt[^>]*C[co]y\s*=\s*["\']?(?P<ccy>[A-Za-z]{3})["\']?[^>]*>(?P<amt>[\d\s.,]+)<',
        r'<[^>]*Amt[^>]*C[co]y\s*=\s*["\']?(?P<ccy>[A-Za-z]{3})["\']?[^>]*>(?P<amt>[\d\s.,]+)<',
    ]

    def normalize_amount(raw_amt: str) -> str:
        amt = (raw_amt.replace("\u00A0", " ")
                     .replace(" ", "")
                     .replace(",", "."))
        if amt.count(".") > 1:
            digits = re.findall(r"\d+", amt)
            if len(digits) >= 2:
                amt = "".join(digits[:-1]) + "." + digits[-1]
        return amt

    def try_xml():
        for p in xml_patterns:
            regex = re.compile(p, re.IGNORECASE | re.DOTALL)
            for m in regex.finditer(text):
                ccy_raw = (m.group("ccy") or "").upper()
                curr = curr_fix.get(ccy_raw, ccy_raw)
                raw_amt = m.group("amt") or ""
                amt_clean = normalize_amount(raw_amt)

                try:
                    value = float(amt_clean)
                except ValueError:
                    continue

                if value > 0 and curr in valid_currencies:
                    logger.info(f"Найдена сумма (XML): {value} {curr}")
                    return curr, amt_clean
        return None, None

    curr, amt = try_xml()
    if curr and amt:
        return curr, amt

    loose_regex = re.compile(
        r"(?P<ccy>[A-Z]{3})[^0-9A-Z]{0,15}(?P<amt>[\d\s.,]{4,})",
        re.IGNORECASE | re.DOTALL
    )

    for m_amt in re.finditer(r"Amt", text, re.IGNORECASE):
        start = max(0, m_amt.start() - 80)
        end = min(len(text), m_amt.end() + 120)
        window = text[start:end]

        for m in loose_regex.finditer(window.upper()):
            ccy_raw = (m.group("ccy") or "").upper()
            curr = curr_fix.get(ccy_raw, ccy_raw)
            raw_amt = m.group("amt") or ""
            amt_clean = normalize_amount(raw_amt)

            try:
                value = float(amt_clean)
            except ValueError:
                continue

            if value > 0 and curr in valid_currencies:
                logger.info(f"Найдена сумма (loose-Amt): {value} {curr}")
                return curr, amt_clean

    logger.warning("Не удалось найти сумму/валюту")
    return None, None


def extract_uetr(text: str) -> str | None:
    """
    Устойчивый поиск UETR в OCR и XML.
    Ловит:
    - <UETR>...</UETR>
    - UETR : xxxx
    - U E T R xxxx
    - UETR-xxxx
    - UUID с пробелами вместо дефисов
    """
    if not text:
        return None

    # 1. XML
    m = re.search(
        r"<UETR>\s*([0-9a-fA-F\- ]{30,})\s*</UETR>",
        text,
        re.IGNORECASE
    )
    if m:
        raw = m.group(1)
        return _normalize_uetr(raw)

    # 2. OCR — U E T R / UETR / U-ETR
    m = re.search(
        r"\bU\s*E\s*T\s*R\b[^0-9a-fA-F]{0,10}([0-9a-fA-F\- ]{30,})",
        text,
        re.IGNORECASE
    )
    if m:
        raw = m.group(1)
        return _normalize_uetr(raw)

    return None
def _normalize_uetr(raw: str) -> str | None:
    """
    Приводит OCR-мусор к нормальному UUID:
    7a7f6f5e c3b9 41f0 9aa4 6efb52386362
    → 7a7f6f5e-c3b9-41f0-9aa4-6efb52386362
    """
    if not raw:
        return None

    s = raw.lower().strip()

    # заменяем всё, кроме hex и пробелов
    s = re.sub(r"[^0-9a-f ]", " ", s)
    parts = re.findall(r"[0-9a-f]{4,}", s)

    joined = "".join(parts)
    if len(joined) != 32:
        return None

    # UUID формат
    return f"{joined[0:8]}-{joined[8:12]}-{joined[12:16]}-{joined[16:20]}-{joined[20:32]}"



def extract_purpose(text: str) -> str | None:
    """
    Берём назначение из <Ustrd>...</Ustrd>.
    Если OCR поломал теги — пробуем выцепить кусок после слова Ustrd.
    """
    if not text:
        return None

    purposes = re.findall(r"<Ustrd>(.*?)</Ustrd>", text, re.IGNORECASE | re.DOTALL)

    if not purposes:
        m = re.search(r"\bUstrd\b[^A-Za-z0-9]{0,8}(.{10,400})", text, re.IGNORECASE | re.DOTALL)
        if m:
            chunk = m.group(1)
            chunk = re.split(r"</?[:A-Za-z]{2,15}|Printer[A-Z]+|\d{2}/\d{2}/\d{2}", chunk)[0]
            purposes = [chunk]

    if not purposes:
        return None

    cleaned = []
    for p in purposes:
        s = " ".join(p.split()).strip()
        s = s.strip("> ")
        if s:
            cleaned.append(s)

    if not cleaned:
        return None

    # Если несколько Ustrd — склеиваем
    return " | ".join(cleaned)


def extract_swift_fields(text: str) -> dict:
    """Извлекаем ключевые поля (минимум для красивой расшифровки)."""
    result = {}
    logger.info(f"Парсинг SWIFT текста: {len(text)} символов")

    # UETR
    uetr = extract_uetr(text)
    if uetr:
        result["uetr"] = uetr

    # Сумма/валюта
    curr, amt = extract_amount_and_currency(text)
    if curr and amt:
        result["currency"] = curr
        result["amount"] = amt

    # Имена
    names = re.findall(r"<Nm>([^<]+)</Nm>", text)
    valid_names = [
        n.strip() for n in names
        if "NOTPROVIDED" not in n and len(n.strip()) > 2
    ]
    if valid_names:
        result["debtor_name"] = valid_names[0]
        if len(valid_names) > 1:
            result["creditor_name"] = valid_names[1]

    # Назначение
    purpose = extract_purpose(text)
    if purpose:
        result["purpose"] = purpose

    return result


def _format_amount(amount_str: str) -> str:
    """
    Красивый вывод суммы: 73000.00 -> 73 000.00
    """
    try:
        val = float(amount_str)
        return f"{val:,.2f}".replace(",", " ")
    except Exception:
        return amount_str


def format_swift_message(fields: dict) -> str | None:
    """
    Нужный тебе формат (без лишних блоков/заголовков).
    Пример:
    Swift расшифровка
    Uetr: ...
    Сумма: 73 000.00 USD
    Плательщик: ...
    Получатель: ...
    Payment for: ...
    """
    if not fields:
        return None

    lines = ["Swift расшифровка"]

    if fields.get("uetr"):
        lines.append(f"Uetr: {fields['uetr']}")

    if fields.get("amount") and fields.get("currency"):
        lines.append(f"Сумма: {_format_amount(fields['amount'])} {fields['currency']}")

    if fields.get("debtor_name"):
        lines.append(f"Плательщик: {fields['debtor_name']}")

    if fields.get("creditor_name"):
        lines.append(f"Получатель: {fields['creditor_name']}")

    if fields.get("purpose"):
        lines.append(f"Payment for: {fields['purpose']}")

    # Если вдруг вообще ничего не нашли — не спамим чат
    if len(lines) <= 1:
        return None

    return "\n".join(lines)


def parse_swift_text(text: str) -> str | None:
    """
    Главная функция: отдаёшь ей OCR-текст или XML-текст,
    на выходе — готовое сообщение или None.
    """
    fields = extract_swift_fields(text)
    return format_swift_message(fields)
