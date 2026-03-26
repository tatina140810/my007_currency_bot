import re
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

def _norm_ws_zaprosy(s: str) -> str:
    if not s:
        return ""
    return s.replace("\u00A0", " ").replace("\u202F", " ")

def parse_human_number_zaprosy(s: str) -> float:
    try:
        s = s.strip().replace("\u00A0", " ")
        s = re.sub(r"\s+", "", s)
        if re.fullmatch(r"\d{1,2}[\./-]\d{1,2}[\./-]\d{2,4}", s):
            return 0.0
        has_dot, has_comma = "." in s, "," in s
        if has_dot and has_comma:
            if s.rfind(",") > s.rfind("."):
                s = s.replace(".", "").replace(",", ".")
            else:
                s = s.replace(",", "")
        elif has_dot and not has_comma:
            if re.fullmatch(r"\d{1,3}(\.\d{3})+", s):
                s = s.replace(".", "")
        elif has_comma and not has_dot:
            if re.fullmatch(r"\d{1,3}(,\d{3})+", s):
                s = s.replace(",", "")
            else:
                s = s.replace(",", ".")
        val = float(s)
        if val > 1_000_000_000:
            return 0.0
        return val
    except Exception:
        return 0.0

def extract_currency_from_str_zaprosy(s: str, default: str = "RUB") -> str:
    low = s.lower()
    if any(x in low for x in ["usd", "$", "доллар", "бакс", "cent"]): return "USD"
    if any(x in low for x in ["eur", "€", "евро"]): return "EUR"
    if any(x in low for x in ["cny", "юан", "yuan", "rmb", "¥"]): return "CNY"
    if any(x in low for x in ["aed", "дирхам"]): return "AED"
    if any(x in low for x in ["kzt", "тенге"]): return "KZT"
    if any(x in low for x in ["kgs", "сом"]): return "KGS"
    if any(x in low for x in ["usdt", "tether"]): return "USDT"
    if any(x in low for x in ["rub", "₽", "руб", "рск", "деревян"]): return "RUB"
    return default

def looks_like_bank_income_zaprosy(text: str) -> bool:
    t = _norm_ws_zaprosy(text or "").lower().strip()
    if t.startswith(("оплата", "взнос", "выдача", "фикс", "запрос", "список платежей")) or "список платежей" in t:
        return False
    income_words = bool(re.search(r"\b(поступ\w*|зачисл\w*|получен\w*|приход\w*|пришли)\b", t))
    bank_markers = any(k in t for k in (
        "перевод spfs", "перевод finline", "согл. п.п.", "п.п.",
        "отпр.", "отпр ", "отправ", "ooo", "ооо", "osoo",
        "mcrb", "sb", "mti", "vo", "rs", "р/с", "инн", "банк", "bank",
    ))
    has_currency = bool(re.search(
        r"(₽|\brub\b|\brub\.?\b|\brubль\w*\b|\brubлей\b|\brubля\b|руб|usd|\$|eur|€|сом|kgs|cny|¥|kzt|aed|usdt)",
        t, re.IGNORECASE
    ))
    return (income_words and has_currency) or (bank_markers and has_currency)

def parse_zaprosy_incomes(text: str) -> List[Dict]:
    if not text:
        return []
    text = _norm_ws_zaprosy(text)
    if not re.search(r"\b(поступ\w*|зачисл\w*|получен\w*|приход\w*|пришли)\b", text.lower()):
        return []
        
    money_re = re.compile(
        r"(?P<amount>\d[\d\s\u00A0\u202F]*(?:[.,]\d{1,2})?)\s*"
        r"(?P<curr>₽|r\.?|руб(?:\.|ля|лей)?|rub|RUB|сом(?:\.|ов)?|kgs|usdt|usd|\$|eur|€|kzt|cny|юан(?:ь|я|ей)?|¥|aed|дирх(?:ам|ама|амов)?)\b",
        re.IGNORECASE,
    )
    
    results = []
    segments = re.split(r'(?://-|\n-)', text)
    if len(segments) <= 1:
        segments = [text]
        
    for seg in segments:
        if not re.search(r"\b(поступ\w*|зачисл\w*|получен\w*|приход\w*|пришли)\b", seg.lower()):
            continue
        m = money_re.search(seg)
        if m:
            amount = parse_human_number_zaprosy(m.group("amount"))
            if amount <= 0: continue
            currency = extract_currency_from_str_zaprosy(m.group("curr"))
            desc_text = seg.strip()
            if len(desc_text) > 150: desc_text = desc_text[:147] + "..."
            results.append({
                "type": "Поступление",
                "amount": amount,
                "currency": currency,
                "description": f"Авто-приход (SMS/Notif): {desc_text}"
            })
    return results
