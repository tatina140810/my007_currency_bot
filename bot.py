#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram бот для учета финансовых операций
ФИНАЛЬНАЯ ВЕРСИЯ:
- Тихий режим (без лишних сообщений)
- Ультрабыстрый OCR (3-5 секунд)
- Альбомы SWIFT
- БЕЗ эмодзи в чате
- Все логи только в консоли
"""

import os
import re
import io
import time
import asyncio
import logging
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo
from collections import defaultdict
from typing import Dict

from PIL import Image, ImageOps

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from datetime import date
from database import Database
from config import BOT_TOKEN, CURRENCIES, ADMIN_PASSWORD
from excel_export import export_to_excel
from auto_reply_bot import (
    is_working_time,
    AUTO_REPLY_TEXT,
    TEAM_MEMBER_IDS,
    last_auto_reply_dates,
)
from swift_parser import parse_swift_text

KG_TZ = ZoneInfo("Asia/Bishkek")

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

db = Database()

# Батчинг операций
operation_queue = defaultdict(list)
queue_lock = asyncio.Lock()
batch_task = None

# Альбомы (media_group)
media_groups: dict[str, list[bytes]] = {}
media_group_tasks: dict[str, asyncio.Task] = {}
MEDIA_GROUP_WAIT = 1.2

# Настройки комиссий
COMMISSION_PERCENT = 0.01
BANK_REQUEST_FEE = 65.0
async def error_handler(update, context):
    logger.exception("Unhandled exception", exc_info=context.error)

async def debug_list_chats(context, db):
    chats = db.get_all_chats()  # SELECT DISTINCT chat_id FROM operations

    lines = ["Чаты в базе:"]

    for chat_id in chats:
        try:
            chat = await context.bot.get_chat(chat_id)
            title = chat.title or chat.username or "Без названия"
            lines.append(f"{chat_id} → {title}")
        except Exception as e:
            lines.append(f"{chat_id} → ❌ недоступен ({e})")

    return "\n".join(lines)


# ===== УЛЬТРАБЫСТРЫЙ OCR =====
def run_ocr_from_image_bytes(image_bytes: bytes) -> str:
    """УЛЬТРАБЫСТРЫЙ OCR с множественными попытками."""
    import pytesseract

    try:
        img = Image.open(io.BytesIO(image_bytes))
        w, h = img.size
        pixels = w * h

        logger.info(f"    OCR: {w}x{h} ({pixels/1_000_000:.1f} Мпикс)")

        img = img.convert("L")
        img = ImageOps.autocontrast(img, cutoff=2)

        # Адаптивный scale
        if pixels < 500_000:
            scale = 1.3
            logger.info(f"    OCR: Очень маленькое, upscale {scale}x")
        elif pixels > 1_200_000:
            scale = 0.85
            logger.info(f"    OCR: Большое, downscale {scale}x")
        else:
            scale = 1.0
            logger.info(f"    OCR: Нормальное, БЕЗ изменений")

        if scale != 1.0:
            new_w, new_h = int(w * scale), int(h * scale)
            img = img.resize((new_w, new_h), Image.Resampling.LANCZOS)
            logger.info(f"    OCR: Изменён размер до {new_w}x{new_h}")

        # МНОЖЕСТВЕННЫЕ ПОПЫТКИ OCR с разными режимами
        configs = [
            ("--oem 3 --psm 6", "PSM 6 (блок)"),
            ("--oem 3 --psm 4", "PSM 4 (колонка)"),
            ("--oem 3 --psm 3", "PSM 3 (авто)"),
        ]

        best_text = ""
        best_length = 0

        for config, description in configs:
            logger.info(f"    OCR: {description}...")

            try:
                text = pytesseract.image_to_string(
                    img,
                    lang="eng",
                    config=config,
                    timeout=45
                ) or ""

                logger.info(f"    OCR: {description} → {len(text)} символов")

                if len(text) > best_length:
                    best_text = text
                    best_length = len(text)

                # Если получили много текста - останавливаемся
                if len(text) > 1500:
                    logger.info(f"    OCR: Достаточно текста")
                    break

            except RuntimeError:
                logger.warning(f"    OCR: TIMEOUT на {description}")
                continue

        if not best_text:
            logger.error(f"    OCR: Все попытки провалились!")
            return ""

        logger.info(f"    OCR: Итог {len(best_text)} символов")

        best_text = best_text.replace("‹", "<").replace("›", ">")
        best_text = best_text.replace("«", "<").replace("»", ">")

        return best_text.strip()

    except Exception as e:
        logger.exception(f"    OCR: Ошибка")
        return ""


def is_staff(user_id: int | None) -> bool:
    return user_id is not None and user_id in TEAM_MEMBER_IDS


async def process_operation_batch():
    global operation_queue
    while True:
        await asyncio.sleep(0.5)
        async with queue_lock:
            if not operation_queue:
                continue
            queue_snapshot = dict(operation_queue)

        for chat_id, operations in queue_snapshot.items():
            try:
                for op in operations:
                    db.add_operation(
                        chat_id,
                        op["type"],
                        op["currency"],
                        op["amount"],
                        op["description"],
                    )
                async with queue_lock:
                    operation_queue.pop(chat_id, None)
                invalidate_balance_cache(chat_id)
                logger.info(f"Обработано {len(operations)} операций для чата {chat_id}")
            except Exception:
                logger.exception(f"Ошибка записи операций для чата {chat_id}")


async def queue_operation(
    chat_id: int, op_type: str, currency: str, amount: float, description: str = ""
):
    async with queue_lock:
        operation_queue[chat_id].append({
            "type": op_type,
            "currency": currency,
            "amount": amount,
            "description": description,
        })


# Кеширование балансов
balance_cache: Dict[int, Dict[str, float]] = {}
balance_cache_time: Dict[int, float] = {}
CACHE_TTL = 5


def get_cached_balance(chat_id: int):
    now = datetime.now().timestamp()
    if chat_id in balance_cache:
        if now - balance_cache_time.get(chat_id, 0) < CACHE_TTL:
            return balance_cache[chat_id]
    balances = db.get_balances(chat_id)
    balance_cache[chat_id] = balances
    balance_cache_time[chat_id] = now
    return balances


def invalidate_balance_cache(chat_id: int):
    balance_cache.pop(chat_id, None)
    balance_cache_time.pop(chat_id, None)


def migrate_legacy_currencies():
    try:
        conn = db.get_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE operations
            SET currency = 'CNY'
            WHERE currency IN ('ЮАНЬ', 'ЮАНЕЙ', 'ЮАНЯ', 'ЮАН');
        """)
        conn.commit()
        conn.close()
        logger.info("Миграция валют выполнена")
    except Exception as e:
        logger.error(f"Ошибка миграции валют: {e}")


# Вспомогательные функции

def get_chat_id(update: Update) -> int:
    return update.effective_chat.id


def get_chat_name(update: Update) -> str:
    chat = update.effective_chat
    if chat.type == "private":
        return f"Личный чат с {update.effective_user.first_name}"
    return chat.title or f"Группа {chat.id}"


def parse_timestamp(ts: str | datetime) -> datetime:
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


def normalize_currency(curr: str) -> str:
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

def parse_income_notification(text: str):
    if not text:
        return None
    low = text.lower()
    if not any(kw in low for kw in (
        "поступил", "поступили", "поступление",
        "зачислен", "зачислены", "зачисление",
    )):
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
    return {"amount": amount, "currency": currency, "description": text.strip()}


def compute_conversion_to_amount(amount: float, rate: float, from_curr: str, to_curr: str) -> float:
    weak = {"RUB", "KGS", "KZT", "CNY"}
    strong = {"USD", "USDT", "EUR", "AED"}
    if rate <= 0:
        raise ValueError("Курс должен быть > 0")
    from_weak = from_curr in weak
    from_strong = from_curr in strong
    to_weak = to_curr in weak
    to_strong = to_curr in strong
    if from_strong and to_weak:
        return amount * rate
    if from_weak and to_strong:
        return amount / rate
    if from_weak and to_weak:
        return amount * rate
    if from_strong and to_strong:
        return amount * rate
    return amount * rate


def parse_human_number(s: str) -> float:
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


def parse_manual_operation_line(text: str):
    if not text:
        return None
    original = text.strip()
    original = re.sub(r"\s+", " ", original).strip()
    low = original.lower()

    # Оплата ПП
    if low.startswith("оплата пп"):
        logger.info(f"   Распознаю оплату ПП...")
        m = re.match(
            r"оплата\s+пп\s+(\d[\d\s]*[.,]?\d*)\s+([A-Za-zА-Яа-я¥₽$]{1,})\s*(.*)",
            original,
            re.IGNORECASE,
        )
        if not m:
            return None
        amount_str = m.group(1)
        curr_raw = m.group(2)
        rest = m.group(3).strip()
        amount = float(amount_str.replace(" ", "").replace(",", "."))
        currency = normalize_currency(curr_raw)
        swift_amount = None
        swift_currency = "USD"
        with_commission = False
        if rest:
            if re.search(r"удержан\w*\s+комисси\w*\s+1\s*%|комисси\w*\s+1\s*%", rest, re.IGNORECASE):
                with_commission = True
            sm = re.search(r"(swift|свифт)\s+(\d[\d\s]*[.,]?\d*)(?:\s+([A-Za-zА-Яа-я]{3,}))?", rest, re.IGNORECASE)
            if sm:
                sa_str = sm.group(2)
                swift_amount = float(sa_str.replace(" ", "").replace(",", "."))
                sc_raw = sm.group(3)
                if sc_raw:
                    swift_currency = normalize_currency(sc_raw)
        return {
            "type": "Оплата ПП",
            "amount": amount,
            "currency": currency,
            "to_amount": None,
            "to_currency": None,
            "rate": None,
            "description": rest,
            "swift_amount": swift_amount,
            "swift_currency": swift_currency,
            "with_commission": with_commission,
        }

    # Возврат ПП
    if re.search(r"возврат\s+(?:пп|по\s+пп)", low):
        logger.info(f"   Распознаю возврат по ПП...")
        m = re.search(r"(\d[\d\s]*[.,]?\d*)\s+([A-Za-zА-Яа-я]{2,})", original, re.IGNORECASE)
        if m:
            amount_str = m.group(1)
            curr_raw = m.group(2)
            description = original.strip()
            try:
                amount = parse_human_number(amount_str)
            except ValueError as e:
                logger.warning(f"   Ошибка парсинга суммы: {e}")
                return None
            currency = normalize_currency(curr_raw)
            logger.info(f"   Распознан возврат: {amount} {currency}")
            return {
                "type": "Возврат по ПП",
                "amount": amount,
                "currency": currency,
                "to_amount": None,
                "to_currency": None,
                "rate": None,
                "description": description,
                "swift_amount": None,
                "swift_currency": None,
                "with_commission": False,
            }

    # Конвертация (фикс)
    if re.search(r"\bфикс\b", low):
        logger.info("   Обнаружено слово 'фикс', парсим конвертацию...")
        s = re.sub(r"\s+", " ", original).strip()
        fix_patterns = [
            r"^фикс\s+(?P<amount>[\d\s.,]+)\s+(?P<from>\S{1,6})\s+(?P<rate>[\d\s.,]+)\s+(?P<to>\S{1,6})(?P<desc>.*)$",
            r"^(?P<amount>[\d\s.,]+)\s+(?P<from>\S{1,6})\s+(?P<rate>[\d\s.,]+)\s+(?P<to>\S{1,6})\s+фикс(?P<desc>.*)$",
            r"^фикс\s+(?P<amount>[\d\s.,]+)\s+(?P<from>\S{1,6})\s+(?P<rate>[\d\s.,]+)(?P<desc>.*)$",
            r"^(?P<amount>[\d\s.,]+)\s+(?P<from>\S{1,6})\s+(?P<rate>[\d\s.,]+)\s+фикс(?P<desc>.*)$",
            r"^фикс\s+(?P<amount>[\d\s.,]+)\s+(?P<from>\S{1,6})(?P<desc>.*)$",
            r"^(?P<amount>[\d\s.,]+)\s+(?P<from>\S{1,6})\s+фикс(?P<desc>.*)$",
        ]
        m = None
        for p in fix_patterns:
            m = re.match(p, s, flags=re.IGNORECASE)
            if m:
                break
        if not m:
            logger.warning(f"   Не удалось распарсить конвертацию: '{original}'")
            return None
        amount_str = (m.group("amount") or "").strip()
        from_raw = (m.group("from") or "").strip()
        rate_str = (m.groupdict().get("rate") or "").strip()
        to_raw = (m.groupdict().get("to") or "").strip()
        desc = (m.groupdict().get("desc") or "").strip()
        try:
            amount = parse_human_number(amount_str)
        except ValueError as e:
            logger.warning(f"   Ошибка парсинга суммы: {e}")
            return None
        from_curr = normalize_currency(from_raw)
        to_curr = normalize_currency(to_raw) if to_raw else "RUB"
        rate = None
        if rate_str:
            try:
                rate = parse_human_number(rate_str)
            except ValueError as e:
                logger.warning(f"   Ошибка парсинга курса: {e}")
                return None
        desc = re.sub(r"\bфикс\b", "", desc, flags=re.IGNORECASE).strip()
        if from_curr == to_curr:
            logger.warning(f"   Обе валюты одинаковые: {from_curr}")
            return None
        logger.info(f"   Распознано: {amount} {from_curr} -> {to_curr} (курс {rate})")
        return {
            "type": "Конвертация",
            "amount": amount,
            "currency": from_curr,
            "to_amount": None,
            "to_currency": to_curr,
            "rate": rate,
            "description": desc,
            "swift_amount": None,
            "swift_currency": None,
        }

    # Взнос наличными
    if low.startswith("взнос наличными") or low.startswith("взнос "):
        logger.info(f"   Распознаю взнос наличными...")
        m = re.match(
            r"взнос(?:\s+наличными)?\s+(\d[\d\s]*[.,]?\d*)\s+([A-Za-zА-Яа-я]{3,})\s*(.*)",
            original,
            re.IGNORECASE,
        )
        if not m:
            return None
        amount_str = m.group(1)
        curr_raw = m.group(2)
        desc = m.group(3).strip()
        amount = float(amount_str.replace(" ", "").replace(",", "."))
        currency = normalize_currency(curr_raw)
        return {
            "type": "Взнос наличными",
            "amount": amount,
            "currency": currency,
            "to_amount": None,
            "to_currency": None,
            "rate": None,
            "description": desc,
            "swift_amount": None,
            "swift_currency": None,
        }

    # Выдача наличными
    if re.search(r"\bвыдача\b", low):
        logger.info("   Распознаю выдачу наличными...")
        m = re.search(
            r"\bвыдача(?:\s+наличными)?\s+(\d[\d\s.,]*\d)\s+([A-Za-zА-Яа-я¥₽$]{1,})\s*(.*)",
            original,
            re.IGNORECASE,
        )
        if not m:
            return None
        amount_str = m.group(1)
        curr_raw = m.group(2)
        desc = m.group(3).strip()
        amount = parse_human_number(amount_str)
        currency = normalize_currency(curr_raw)
        return {
            "type": "Выдача наличных",
            "amount": amount,
            "currency": currency,
            "to_amount": None,
            "to_currency": None,
            "rate": None,
            "description": desc,
            "swift_amount": None,
            "swift_currency": None,
        }

    # Запрос банку
    if low.startswith("запрос банку") or low.startswith("запрос "):
        logger.info(f"   Распознаю запрос банку...")
        desc = text.replace("запрос банку", "").replace("запрос", "").strip()
        return {
            "type": "Запрос банку",
            "amount": BANK_REQUEST_FEE,
            "currency": "USD",
            "to_amount": None,
            "to_currency": None,
            "rate": None,
            "description": desc or "Запрос банку",
            "swift_amount": None,
            "swift_currency": None,
        }

    return None

import re

def parse_bulk_pp_payments(text: str):
    """
    Парсит сообщение вида:

    ТезКадам :
    1  Дельмар  Shenzhen ...  172000= CNY
    2  УЗ  HEBEI ...  248637-50 CNY

    Умут Трейд
    1  Денис Биш  ...  19484-88 USD
    ...

    Возвращает list[dict] с keys:
      company, group, receiver, amount, currency
    где:
      company = ТезКадам / Умут Трейд / ...
      group   = УЗ / Денис / Медигрупп (для распределения по телеграм-группам/Excel)
    """

    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    items = []
    current_company = None

    # Заголовок секции (компания): строка без суммы/валюты и без "1 ..."
    # На практике у тебя это "ТезКадам :", "Умут Трейд", "Сара Трейд:", "Дея Групп (Возврат)"
    company_header_re = re.compile(r"^[А-Яа-яA-Za-z0-9().\- ]{2,}:\s*$|^[А-Яа-яA-Za-z0-9().\- ]{2,}$")

    # Строка платежа:
    # 1  УЗ  HEBEI ...  248637-50 CNY
    pay_re = re.compile(
        r"^\s*(\d+)\s+(.+?)\s{2,}(.+?)\s{2,}([0-9][0-9=\-., ]*)\s+([A-Z]{3})\s*$"
    )

    def norm_group(raw: str) -> str:
        raw = (raw or "").strip()
        low = raw.lower()
        # Алиасы под твои группы
        if low.startswith("денис"):
            return "Денис"
        if low.startswith("уз"):
            return "УЗ"
        if low.startswith("медигрупп"):
            return "Медигрупп"
        return raw

    def parse_amount(raw: str) -> float:
        # "172000=" -> "172000"
        s = raw.strip().replace("=", "")
        s = s.replace(" ", "")
        # "248637-50" -> "248637.50"
        if "-" in s and s.count("-") == 1 and s.rsplit("-", 1)[1].isdigit():
            left, right = s.rsplit("-", 1)
            s = f"{left}.{right}"
        # "12,345.67" or "12.345,67" — сделаем мягко
        # если есть и ',' и '.', считаем что ',' = тысячи → убираем ','
        if "," in s and "." in s:
            s = s.replace(",", "")
        else:
            # если только ',' — пусть будет десятичной
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
                "group": group_name,          # <-- ВАЖНО: УЗ/Денис/Медигрупп
                "receiver": receiver.strip(), # контрагент/получатель
                "amount": amount,
                "currency": currency,
            })
            continue

        # если это не платеж — возможно заголовок секции
        # чуть фильтруем, чтобы не перехватить "Список платежей..."
        if "список платежей" in ln.lower():
            continue

        # заголовок компании
        if company_header_re.match(ln):
            # убираем двоеточие на конце
            current_company = ln.rstrip(":").strip()
            continue

    return items


def extract_rate_from_text(text: str) -> float | None:
    if not text:
        return None
    m = re.search(r"1\s+[A-Za-z]{3}\s*=\s*([\d\s.,]+)\s+[A-Za-z]{3}", text, re.IGNORECASE)
    if m:
        num = m.group(1)
        try:
            return float(num.replace(" ", "").replace(",", "."))
        except ValueError:
            pass
    m = re.search(r"[Кк][Уу][Рр][Сс][^0-9]{0,10}([\d\s.,]+)", text, re.IGNORECASE)
    if m:
        num = m.group(1)
        try:
            return float(num.replace(" ", "").replace(",", "."))
        except ValueError:
            pass
    m = re.search(r"(\d[\d\s]*[.,]\d+)", text)
    if m:
        num = m.group(1)
        try:
            return float(num.replace(" ", "").replace(",", "."))
        except ValueError:
            pass
    return None

def quick_swift_check(text: str) -> bool:
    """Быстрая проверка: похоже ли на SWIFT/MX (pacs.008 и т.п.)."""
    if not text:
        return False

    t = text.lower()

    # если это MX/XML — часто есть теги
    if "<" in t and ">" in t:
        # ключевые слова/теги, которые почти всегда встречаются в pacs/Swift распечатках
        keys = (
            "pacs.008", "cbprplus", "fitoficstmr", "bizmsgidr", "msgdefidr",
            "bicfi", "uetr", "intrbksttlmamt", "instdamt", "chrgbr",
            "printer", "swift", "swiftnet", "document xmlns", "<apphdr", "<document"
        )
        return any(k in t for k in keys)

    # если OCR вытащил без < > — всё равно по словам
    keys2 = ("swiftnet", "uetr", "bicfi", "pacs.008", "cbprplus", "msgdefidr")
    return any(k in t for k in keys2)

# ===== ОБРАБОТКА ФОТО (SWIFT) =====

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тихая обработка SWIFT с поддержкой альбомов."""
    message = update.effective_message
    if not message or not message.photo:
        return

    photo = message.photo[-1]
    file = await photo.get_file()
    image_bytes = bytes(await file.download_as_bytearray())

    group_id = message.media_group_id

    if not group_id:
        await _process_swift_pages([image_bytes], message)
        return

    if group_id not in media_groups:
        media_groups[group_id] = []
    media_groups[group_id].append(image_bytes)

    old_task = media_group_tasks.get(group_id)
    if old_task and not old_task.done():
        old_task.cancel()

    async def delayed():
        try:
            await asyncio.sleep(MEDIA_GROUP_WAIT)
        except asyncio.CancelledError:
            return
        pages = media_groups.pop(group_id, [])
        media_group_tasks.pop(group_id, None)
        if pages:
            await _process_swift_pages(pages, message)

    media_group_tasks[group_id] = asyncio.create_task(delayed())


async def _process_swift_pages(pages_bytes: list[bytes], message):
    """Обработка страниц SWIFT - КАЖДАЯ СТРАНИЦА ОТДЕЛЬНО."""
    start_time = time.time()
    logger.info(f"SWIFT: страниц в пачке = {len(pages_bytes)}")

    debug_dir = "outputs"
    os.makedirs(debug_dir, exist_ok=True)

    success_count = 0

    # КАЖДАЯ СТРАНИЦА ОБРАБАТЫВАЕТСЯ ОТДЕЛЬНО
    for idx, b in enumerate(pages_bytes, 1):
        page_start = time.time()

        logger.info(f"  Страница {idx}: скачивание завершено ({len(b):,} байт)")
        logger.info(f"  Страница {idx}: запуск OCR...")

        try:
            quick_text = await asyncio.to_thread(run_ocr_from_image_bytes, b)
            logger.info(f"  Страница {idx}: OCR завершён - {len(quick_text)} символов ({time.time()-page_start:.2f}с)")
        except Exception as e:
            logger.exception(f"  Страница {idx}: Ошибка OCR")
            quick_text = ""

        # Сохраняем OCR текст
        debug_file = os.path.join(debug_dir, f"swift_ocr_page_{idx}_{int(time.time())}.txt")
        with open(debug_file, "w", encoding="utf-8") as f:
            f.write(f"=== СТРАНИЦА {idx} ({len(b):,} байт) ===\n\n")
            f.write(quick_text)
        logger.info(f"  Страница {idx}: OCR сохранён в {debug_file}")

        # Быстрая проверка
        is_swift = quick_swift_check(quick_text)
        logger.info(f"  Страница {idx}: {'похоже' if is_swift else 'НЕ похоже'} на SWIFT")

        if not is_swift:
            logger.info(f"  Страница {idx}: пропущена (не SWIFT)")
            logger.info(f"  Страница {idx}: общее время {time.time()-page_start:.2f}с")
            continue

        # ПАРСИМ КАЖДУЮ СТРАНИЦУ ОТДЕЛЬНО
        parse_start = time.time()
        logger.info(f"  Страница {idx}: начинаю парсинг...")

        swift_msg = parse_swift_text(quick_text)

        logger.info(f"  Страница {idx}: время парсинга {time.time()-parse_start:.2f}с")

        if swift_msg:
            # ОТПРАВЛЯЕМ СРАЗУ (не ждём остальные страницы)
            page_time = time.time() - page_start
            logger.info(f"  Страница {idx}: успешно распознана за {page_time:.1f}с")

            # Добавляем номер страницы если их много
            if len(pages_bytes) > 1:
                swift_msg = f"Страница {idx}/{len(pages_bytes)}\n\n{swift_msg}"

            await message.reply_text(swift_msg, parse_mode=None)
            success_count += 1
        else:
            logger.warning(f"  Страница {idx}: XML найден, но данные не извлечены")

        logger.info(f"  Страница {idx}: общее время {time.time()-page_start:.2f}с")

    total_time = time.time() - start_time

    if success_count > 0:
        logger.info(f"SWIFT: успешно распознано {success_count} из {len(pages_bytes)} страниц за {total_time:.1f}с")
    else:
        logger.info(f"SWIFT: ни одна страница не распознана (время: {total_time:.1f}с)")


_SWIFT_TAG_RE = re.compile(r"<\s*[\w:.-]+(?:\s+[^>]*)?>|</\s*[\w:.-]+\s*>")

def has_swift_xml_tags(text: str) -> bool:
    if not text:
        return False
    # простая эвристика: видим xml-теги или типичные маркеры
    if "<" in text and ">" in text and _SWIFT_TAG_RE.search(text):
        return True
    markers = ("UETR", "Dbtr", "Cdtr", "Ccy", "Amt", "IntrBkSttlmAmt", "MsgId")
    return any(m in text for m in markers)

# ===== ОБРАБОТКА ТЕКСТА =====
def looks_like_bank_income(text: str) -> bool:
    t = (text or "").lower()
    # слова про зачисление + банковные маркеры/формулировки
    has_income_words = any(k in t for k in (
        "поступил", "поступили", "поступление",
        "зачислен", "зачислены", "зачисление",
    ))
    has_bank_markers = any(k in t for k in (
        "перевод finline", "перевод spfs", "согл. п.п.", "oplata", "оплата",
        "sb", "mcrb", "vo", "inn", "р/с", "rsc", "rs", "банк", "bank",
    ))
    has_currency = any(k in t for k in ("руб", "rub", "usd", "eur", "сом", "kgs", "cny", "kzt", "aed", "¥", "€", "$", "₽"))

    # достаточно либо “поступили + валюта”, либо “банковские маркеры + валюта”
    return (has_income_words and has_currency) or (has_bank_markers and has_currency)


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    user = update.effective_user
    chat = update.effective_chat

    if not all([message, user, chat]) or user.is_bot or not message.text:
        return

    chat_name = chat.title or chat.first_name or f"Чат {chat.id}"
    chat_type = chat.type
    db.register_chat(chat.id, chat_name, chat_type)

    text = message.text.strip()

    # защита: если вдруг команда попала сюда (напр. /his@botname или странные кейсы)
    if text.startswith("/"):
        return

    logger.info(f"Получено сообщение: chat_id={chat.id} user_id={user.id} text='{text[:80]}'")

    # Если ждём подтверждения отмены — не мешаем
    if "pending_undo_op_id" in context.user_data:
        return

    staff = is_staff(user.id)

    # 0) Авто-поступление: staff ИЛИ похоже на банковское уведомление
    # (чтобы в “Гармин” и другие группы попадали приходы от клиентов/банков)
    if staff or looks_like_bank_income(text):
        income = parse_income_notification(text)
        if income:
            chat_id = get_chat_id(update)
            await queue_operation(
                chat_id, "Поступление",
                income["currency"], income["amount"],
                income["description"]
            )
            logger.info(f"Авто-Поступление: {income['amount']} {income['currency']} в чате {chat_id}")
            return

    # 1) Bulk "оплата ПП" — только для staff
    if staff:
        bulk_payments = parse_bulk_pp_payments(text)
        if bulk_payments:
            created = 0
            skipped = []
            errors = []

            for item in bulk_payments:
                try:
                    group_name = item["group"]  # УЗ/Денис/Медигрупп/и т.д.
                    target_chat_id = db.get_chat_id_by_name(group_name)

                    if not target_chat_id:
                        skipped.append(group_name)
                        logger.warning(f"Группа не найдена в БД: {group_name}")
                        continue

                    company = item.get("company", "").strip()
                    receiver = item.get("receiver", "").strip()
                    description = f"{company} | {receiver}" if company else receiver

                    await queue_operation(
                        target_chat_id,
                        "Оплата ПП",
                        item["currency"],
                        -item["amount"],
                        description
                    )

                    created += 1

                except Exception as e:
                    logger.exception(f"Bulk ПП: ошибка для item={item}: {e}")
                    errors.append(f"{item.get('group','?')}: {type(e).__name__}")

            reply = f"Распознано оплат ПП: {created}"
            if skipped:
                reply += f"\nНе найдены группы: {', '.join(sorted(set(skipped)))}"
            if errors:
                reply += f"\nОшибки: {', '.join(errors)}"

            await message.reply_text(reply, parse_mode=None)
            return

    # 2) Staff: SWIFT + ручной парсинг
    if staff:
        # SWIFT в тексте
        swift_msg = None
        try:
            if has_swift_xml_tags(text):
                swift_msg = parse_swift_text(text)
        except Exception as e:
            logger.exception(f"Ошибка SWIFT-парсинга: {e}")
            swift_msg = None

        if swift_msg:
            chat_id = get_chat_id(update)
            logger.info(f"SWIFT распознан (чат {chat_id})")
            await message.reply_text(swift_msg, parse_mode=None)
            return

        # Ручной парсинг операций
        logger.info(f"Ручной парсинг: '{text}'")
        manual = parse_manual_operation_line(text)

        if not manual:
            logger.warning(f"Не удалось распознать операцию: '{text}'")
            return

        chat_id = get_chat_id(update)
        op_type = manual["type"]
        amount = manual["amount"]
        currency = manual["currency"]
        desc = manual["description"]

        if op_type == "Оплата ПП":
            await queue_operation(chat_id, "Оплата ПП", currency, -amount, desc)
            logger.info(f"Оплата ПП: {amount} {currency} в чате {chat_id}")

            if manual.get("with_commission", False):
                commission = amount * COMMISSION_PERCENT
                await queue_operation(chat_id, "Комиссия 1%", currency, -commission, f"Комиссия за ПП: {desc}")
                logger.info(f"Комиссия 1%: {commission} {currency} в чате {chat_id}")

            if manual.get("swift_amount") and manual["swift_amount"] > 0:
                swift_curr = manual.get("swift_currency") or "USD"
                await queue_operation(chat_id, "SWIFT", swift_curr, -manual["swift_amount"], desc)
                logger.info(f"SWIFT: {manual['swift_amount']} {swift_curr} в чате {chat_id}")
            return

        if op_type == "Запрос банку":
            await queue_operation(chat_id, "Запрос банку", "USD", -BANK_REQUEST_FEE, desc)
            logger.info(f"Запрос банку: {BANK_REQUEST_FEE} USD в чате {chat_id}")
            return

        if op_type == "Конвертация":
            from_curr = currency
            to_curr = manual["to_currency"]

            # курс указан прямо
            if manual.get("rate") is not None:
                rate = manual["rate"]
                try:
                    to_amount = compute_conversion_to_amount(amount, rate, from_curr, to_curr)
                except Exception as e:
                    logger.exception(f"Ошибка конвертации: {e}")
                    return

                await queue_operation(chat_id, "Конвертация", to_curr, -to_amount, desc)
                await queue_operation(chat_id, "Конвертация", from_curr, amount, desc)
                logger.info(f"Конвертация: {amount} {from_curr} -> {to_amount} {to_curr} (курс: {rate})")
                return

            # курс берём из reply
            reply = message.reply_to_message
            reply_text = (reply.text or reply.caption) if reply else None
            if not reply_text:
                logger.warning(f"Конвертация без курса: {text}")
                return

            rate = extract_rate_from_text(reply_text)
            if not rate or rate <= 0:
                await message.reply_text(
                    f"Не удалось найти курс в сообщении:\n'{reply_text}'\n\n"
                    "Укажите курс явно: фикс 1000 usd 89.5 rub",
                    parse_mode=None
                )
                return

            try:
                to_amount = compute_conversion_to_amount(amount, rate, from_curr, to_curr)
            except Exception as e:
                logger.exception(f"Ошибка конвертации (reply): {e}")
                return

            await queue_operation(chat_id, "Конвертация", to_curr, -to_amount, desc)
            await queue_operation(chat_id, "Конвертация", from_curr, amount, desc)
            logger.info(f"Конвертация (reply): {amount} {from_curr} -> {to_amount} {to_curr} (курс: {rate})")
            return

        if op_type == "Взнос наличными":
            await queue_operation(chat_id, "Взнос наличными", currency, amount, desc)
            logger.info(f"Взнос наличными: {amount} {currency} в чате {chat_id}")
            return

        if op_type == "Выдача наличных":
            await queue_operation(chat_id, "Выдача наличных", currency, -amount, desc)
            logger.info(f"Выдача наличных: {amount} {currency} в чате {chat_id}")
            return

        if op_type == "Возврат по ПП":
            await queue_operation(chat_id, "Возврат по ПП", currency, amount, desc)
            logger.info(f"Возврат по ПП: {amount} {currency} в чате {chat_id}")
            return

        return  # staff обработан

    # 3) Не staff → автоответчик
    # if user.id not in TEAM_MEMBER_IDS:
       #  now = datetime.now(KG_TZ)
        # if not is_working_time(now):
           #  if last_auto_reply_dates.get(chat.id) != now.date():
               # try:
                    #await chat.send_message(AUTO_REPLY_TEXT)
                   # last_auto_reply_dates[chat.id] = now.date()
               # except Exception as e:
                    #logger.exception(f"Автоответ: {e}")
       # else:
           # last_auto_reply_dates.pop(chat.id, None)

    return


# ===== КОМАНДЫ =====

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    chat = update.effective_chat
    chat_name = get_chat_name(update)
    telegram_chat_name = chat.title or chat.first_name or f"Чат {chat.id}"
    db.register_chat(chat.id, telegram_chat_name, chat.type)

    base_text = f"""Добро пожаловать, {user.first_name}!

Текущий чат: {chat_name}

Команды:
/bal - Показать баланс
/his - История операций
/del - Удалить операцию (по паролю)
/ex - Экспорт в Excel
/help - Справка

Операции в чате (для сотрудников):
- Поступления: "... 1000,00 руб поступили ..."
- Оплата ПП: "оплата пп 1000 usd swift 25 описание"
- С комиссией: "оплата пп 1000 usd удержание комиссии 1% описание"
- Конвертация: "фикс 1000 usd" (курс из reply) или "фикс 1000 usd 89.5 rub"
- Взнос: "взнос наличными 5000 usd"
- Выдача: "выдача наличными 3000 usd"
- Запрос банку: "запрос банку описание" (65 USD)
"""
    await update.message.reply_text(base_text, parse_mode=None)


async def show_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    chat_id = get_chat_id(update)
    chat_name = get_chat_name(update)
    telegram_chat_name = chat.title or chat.first_name or f"Чат {chat.id}"
    db.register_chat(chat.id, telegram_chat_name, chat.type)
    logger.info(f"Баланс запрошен для чата {chat_id}")

    balances = get_cached_balance(chat_id)
    text = f"БАЛАНС\n{chat_name}\n\n"
    total_exists = False
    for currency in CURRENCIES:
        balance = balances.get(currency, 0.0)
        if balance != 0:
            total_exists = True
        text += f"{currency}: {balance:,.2f}\n"

    if not total_exists:
        text += "\nОпераций пока нет"

    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(text, parse_mode=None)
    else:
        await update.message.reply_text(text, parse_mode=None)


async def show_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    chat_id = get_chat_id(update)
    chat_name = get_chat_name(update)
    telegram_chat_name = chat.title or chat.first_name or f"Чат {chat.id}"
    db.register_chat(chat.id, telegram_chat_name, chat.type)
    logger.info(f"История запрошена для чата {chat_id}")

    target_date: date
    if update.message and context.args:
        date_str = " ".join(context.args).strip()
        parsed = None
        for fmt in ("%d.%m.%Y", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(date_str, fmt)
                break
            except ValueError:
                continue
        if not parsed:
            await update.message.reply_text("Неверный формат даты.\nИспользуйте: /his 01.12.2025", parse_mode=None)
            return
        target_date = parsed.date()
    else:
        target_date = datetime.now(KG_TZ).date()

    all_ops = db.get_operations(chat_id, limit=1000)
    filtered_ops = []
    for op in all_ops:
        op_id, op_type, currency, amount, description, timestamp = op
        op_dt = parse_timestamp(timestamp)
        if op_dt.date() == target_date:
            filtered_ops.append(op)

    if not filtered_ops:
        text = f"История за {target_date.strftime('%d.%m.%Y')} пуста\n{chat_name}"
    else:
        filtered_ops.sort(key=lambda o: parse_timestamp(o[5]))
        text = f"ОПЕРАЦИИ ЗА {target_date.strftime('%d.%m.%Y')}\n\n"
        for op in filtered_ops:
            op_id, op_type, currency, amount, description, timestamp = op
            sign = "+" if amount > 0 else ""
            ts_str = parse_timestamp(timestamp).strftime("%H:%M:%S")
            text += f"{op_type}\n"
            text += f"   {currency}: {sign}{amount:,.2f}\n"
            if description:
                text += f"   {description}\n"
            text += f"   {ts_str}\n"

    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(text, parse_mode=None)
    else:
        await update.message.reply_text(text, parse_mode=None)


async def undo_last_operation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user or (update.callback_query and update.callback_query.from_user)
    if not is_staff(user.id):
        if update.callback_query:
            await update.callback_query.answer("Только для сотрудников", show_alert=True)
        else:
            await update.message.reply_text("Удалять операции могут только сотрудники.", parse_mode=None)
        return

    chat_id = get_chat_id(update)
    chat_name = get_chat_name(update)
    logger.info(f"Запрос удаления операции для чата {chat_id}")

    all_ops = db.get_operations(chat_id, limit=1000)
    today_date = datetime.now(KG_TZ).date()
    todays_ops = []
    for op in all_ops:
        op_id, op_type, currency, amount, description, timestamp = op
        op_dt = parse_timestamp(timestamp)
        if op_dt.date() == today_date:
            todays_ops.append(op)

    if not todays_ops:
        text = f"За сегодня операций нет\n{chat_name}"
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(text, parse_mode=None)
        else:
            await update.message.reply_text(text, parse_mode=None)
        return

    todays_ops.sort(key=lambda o: parse_timestamp(o[5]))
    text_lines = [f"УДАЛЕНИЕ ОПЕРАЦИИ\n{chat_name}\n"]
    keyboard = []

    for op in todays_ops:
        op_id, op_type, currency, amount, description, timestamp = op
        sign = "+" if amount > 0 else ""
        ts_str = parse_timestamp(timestamp).strftime("%H:%M:%S")
        text_lines.append(f"{op_type}\n   {currency}: {sign}{amount:,.2f}\n   {ts_str}\n")
        btn_text = f"{ts_str} {currency} {sign}{amount:,.2f}"
        keyboard.append([InlineKeyboardButton(btn_text, callback_data=f"undo_select_{op_id}")])

    keyboard.append([InlineKeyboardButton("Отмена", callback_data="cancel_undo")])
    reply_markup = InlineKeyboardMarkup(keyboard)
    full_text = "\n".join(text_lines)

    if update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(full_text, reply_markup=reply_markup, parse_mode=None)
    else:
        await update.message.reply_text(full_text, reply_markup=reply_markup, parse_mode=None)


async def undo_select_operation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user or update.callback_query.from_user
    if not is_staff(user.id):
        await update.callback_query.answer("Нет прав", show_alert=True)
        return

    query = update.callback_query
    await query.answer()
    chat_id = get_chat_id(update)
    op_id = int(query.data.replace("undo_select_", ""))
    logger.info(f"Выбрана операция {op_id} для удаления в чате {chat_id}")

    if date_from or date_to:
        operations = db.get_operations_by_date(chat_id, date_from, date_to)
    else:
        operations = db.get_operations(chat_id, limit=10000)

    op_info = None
    for op in operations:
        if op[0] == op_id:
            op_info = op
            break

    if not op_info:
        await query.message.reply_text("Операция не найдена", parse_mode=None)
        return

    op_id, op_type, currency, amount, description, timestamp = op_info
    sign = "+" if amount > 0 else ""
    ts_str = parse_timestamp(timestamp).strftime("%d.%m.%Y %H:%M:%S")

    text = f"Удаление операции\n\n{op_type}\nВалюта: {currency}\nСумма: {sign}{amount:,.2f}\nДата: {ts_str}\n"
    if description:
        text += f"Описание: {description}\n"
    text += "\nВведите пароль для удаления.\nИли /cancel для отмены."

    context.user_data["pending_undo_op_id"] = op_id
    context.user_data["pending_undo_chat_id"] = chat_id
    await query.message.reply_text(text, parse_mode=None)


async def handle_delete_password(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not is_staff(user.id):
        return
    if "pending_undo_op_id" not in context.user_data:
        return

    chat_id = context.user_data.get("pending_undo_chat_id", get_chat_id(update))
    op_id = context.user_data["pending_undo_op_id"]
    entered_password = update.message.text.strip()

    if entered_password != ADMIN_PASSWORD:
        await update.message.reply_text("Неверный пароль. Операция не удалена.", parse_mode=None)
        return

    logger.info(f"Пароль верный, удаляем операцию {op_id}")
    operations = db.get_operations(chat_id, limit=1000)
    op_info = None
    for op in operations:
        if op[0] == op_id:
            op_info = op
            break

    if not op_info:
        await update.message.reply_text("Операция не найдена.", parse_mode=None)
        context.user_data.pop("pending_undo_op_id", None)
        context.user_data.pop("pending_undo_chat_id", None)
        return

    op_id, op_type, currency, amount, description, timestamp = op_info
    success = db.delete_operation(chat_id, op_id)
    context.user_data.pop("pending_undo_op_id", None)
    context.user_data.pop("pending_undo_chat_id", None)

    if not success:
        await update.message.reply_text("Ошибка при удалении.", parse_mode=None)
        return

    sign = "+" if amount > 0 else ""
    ts_str = parse_timestamp(timestamp).strftime("%d.%m.%Y %H:%M:%S")
    text = f"Операция удалена\n\n{op_type}\nВалюта: {currency}\nСумма: {sign}{amount:,.2f}\nДата: {ts_str}\n"
    if description:
        text += f"Описание: {description}\n"
    await update.message.reply_text(text, parse_mode=None)


async def cancel_undo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    context.user_data.pop("pending_undo_op_id", None)
    context.user_data.pop("pending_undo_chat_id", None)
    await query.edit_message_text("Отменено", parse_mode=None)


async def cancel_any(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if "pending_undo_op_id" in context.user_data:
        context.user_data.pop("pending_undo_op_id", None)
        context.user_data.pop("pending_undo_chat_id", None)
        await update.message.reply_text("Отменено", parse_mode=None)
        return
    await update.message.reply_text("Нечего отменять.", parse_mode=None)


async def export_operations(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Экспорт операций в Excel"""

    # Получаем полный текст команды
    message_text = update.message.text.strip()

    print("=" * 60)
    print(f"КОМАНДА: {message_text}")
    print(f"context.args: {context.args}")
    print("=" * 60)

    logger.info("=" * 60)
    logger.info(f"КОМАНДА: {message_text}")
    logger.info(f"context.args: {context.args}")
    logger.info("=" * 60)

    chat = update.effective_chat
    chat_id = chat.id
    telegram_chat_name = chat.title or chat.first_name or f"Чат {chat.id}"
    db.register_chat(chat.id, telegram_chat_name, chat.type)

    # Отправляем сообщение о начале экспорта
    status_msg = await update.message.reply_text("⏳ Формирую файл...", parse_mode=None)

    # ---- парсим дату из ТЕКСТА сообщения (не из context.args) ----
    date_from = None
    date_to = None

    # Убираем команду и берём остаток
    parts = message_text.split(maxsplit=1)

    if len(parts) > 1:
        arg = parts[1].strip()
        logger.info(f"✅ Найден аргумент: '{arg}'")

        arg_lower = arg.lower()

        # Проверяем "сегодня" или "today"
        if arg_lower in ("сегодня", "today"):
            date_from = date_to = datetime.now(KG_TZ).date()
            logger.info(f"✅ Экспорт за СЕГОДНЯ: {date_from}")
        else:
            # Пробуем распарсить как дату
            parsed = None
            for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d.%m.%y"):
                try:
                    parsed = datetime.strptime(arg, fmt).date()
                    logger.info(f"✅ Распознана дата: {parsed} (формат: {fmt})")
                    break
                except ValueError:
                    continue

            if not parsed:
                logger.error(f"❌ Не удалось распознать дату: '{arg}'")
                await status_msg.edit_text(
                    f"❌ Неверный формат даты: '{arg}'\n\n"
                    "Примеры:\n"
                    "/ex — за всё время\n"
                    "/ex сегодня\n"
                    "/ex 15.01.2026\n"
                    "/ex 2026-01-15",
                    parse_mode=None
                )
                return

            date_from = date_to = parsed
    else:
        logger.info("📊 Экспорт за ВСЁ ВРЕМЯ")

    # ---- имя файла ----
    if date_from:
        fname_date = date_from.strftime("%d_%m_%Y")
        filename = f"operations_{fname_date}.xlsx"
        logger.info(f"📁 Файл: {filename}")
    else:
        filename = "operations_all.xlsx"
        logger.info(f"📁 Файл: {filename}")

    base_dir = os.path.join(os.getcwd(), "outputs")
    os.makedirs(base_dir, exist_ok=True)
    output_path = os.path.join(base_dir, filename)

    logger.info(f"📂 Полный путь: {output_path}")

    # ---- экспорт ----
    try:
        logger.info(f"🔄 Запуск экспорта... (from={date_from}, to={date_to})")

        await asyncio.to_thread(
            export_to_excel,
            db,
            output_path,
            date_from,
            date_to
        )

        # Проверяем что файл создан
        if not os.path.exists(output_path):
            logger.error(f"❌ Файл не создан: {output_path}")
            await status_msg.edit_text("❌ Ошибка: файл не был создан", parse_mode=None)
            return

        file_size = os.path.getsize(output_path)
        logger.info(f"✅ Файл создан: размер {file_size} байт")

        # Удаляем статусное сообщение
        try:
            await status_msg.delete()
        except:
            pass

        # Отправляем файл
        with open(output_path, "rb") as file:
            caption_text = datetime.now(KG_TZ).strftime("%d.%m.%Y %H:%M")
            if date_from:
                caption_text += f"\n📅 Операции за {date_from.strftime('%d.%m.%Y')}"
            else:
                caption_text += f"\n📊 Все операции"

            await update.message.reply_document(
                document=file,
                filename=filename,
                caption=caption_text,
            )

        logger.info("✅ Экспорт успешно отправлен")

    except Exception as e:
        logger.exception(f"❌ Ошибка экспорта")
        try:
            await status_msg.edit_text(
                f"❌ Ошибка при экспорте:\n{str(e)[:300]}",
                parse_mode=None
            )
        except:
            await update.message.reply_text(
                f"❌ Ошибка при экспорте:\n{str(e)[:300]}",
                parse_mode=None
            )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_name = get_chat_name(update)
    help_text = f"""СПРАВКА

Текущий чат: {chat_name}

Команды:
/bal - Показать баланс
/his [дата] - История (по датам)
  Пример: /his 01.12.2025
/del - Удалить операцию (по паролю)
/ex - Экспорт в Excel
/help - Эта справка

Операции в чате (для сотрудников):

Поступления (автоматически):
"... 1000,00 руб поступили ..."

Оплата ПП:
- Без комиссии:
  оплата пп 1000 usd описание
  Списывается только 1000 USD

- С комиссией 1%:
  оплата пп 1000 usd удержание комиссии 1% описание
  Списывается 1000 + 10 (1%) = 1010 USD

- С SWIFT:
  оплата пп 1000 usd swift 25 описание
  Списывается 1000 + 25 SWIFT

- Всё вместе:
  оплата пп 1000 usd swift 25 удержание комиссии 1% описание
  Списывается 1000 + 10 (1%) + 25 SWIFT = 1035 USD

Конвертация:
- С указанием курса:
  фикс 1000 usd 89.5 rub комментарий
  Купить 1000 USD за 89500 RUB

- Курс из reply-сообщения:
  фикс 1000 usd (ответом на сообщение с курсом)
  Купить 1000 USD за RUB по курсу из сообщения

Взнос наличными:
взнос наличными 5000 usd описание

Выдача наличными:
выдача наличными 3000 usd описание

Запрос банку (автоматически 65 USD):
запрос банку описание
Списывается 65 USD

Поддерживаемые валюты:
USD, EUR, RUB, CNY, KGS, KZT, USDT, AED
"""
    await update.message.reply_text(help_text, parse_mode=None)


async def general_button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    logger.info(f"Callback: {query.data}")
    await query.answer()
    if query.data == "show_balance":
        await show_balance(update, context)
    elif query.data == "show_history":
        await show_history(update, context)


# ===== MAIN =====
async def log_all_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Логирует ВСЕ входящие сообщения для отладки"""
    if update.message and update.message.text:
        text = update.message.text
        user_id = update.effective_user.id if update.effective_user else "unknown"
        chat_id = update.effective_chat.id if update.effective_chat else "unknown"

        print("=" * 80)
        print(f"📨 ВХОДЯЩЕЕ СООБЩЕНИЕ")
        print(f"   Текст: '{text}'")
        print(f"   User ID: {user_id}")
        print(f"   Chat ID: {chat_id}")
        print(f"   Entities: {update.message.entities}")
        print("=" * 80)

        logger.info("=" * 80)
        logger.info(f"📨 ВХОДЯЩЕЕ СООБЩЕНИЕ: '{text}' from user {user_id} in chat {chat_id}")
        logger.info("=" * 80)
def main():
    global batch_task
    logger.info("Запуск бота...")
    print("🤖 ЗАПУСК БОТА...")

    migrate_legacy_currencies()

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .build()
    )

    # ✅ УНИВЕРСАЛЬНЫЙ ЛОГГЕР - ЛОВИТ ВСЁ (group=-1 = высший приоритет)
    logger.info("📝 Регистрация универсального логгера...")
    print("📝 Регистрация универсального логгера...")
    application.add_handler(
        MessageHandler(filters.ALL, log_all_messages),
        group=-1
    )

    # ✅ КОМАНДА /ex - САМАЯ ПЕРВАЯ, group=-2 (ещё выше приоритет)
    logger.info("📝 Регистрация команды /ex...")
    print("📝 Регистрация команды /ex...")

    async def export_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обёртка для гарантированного перехвата /ex"""
        print(f"🎯 ПЕРЕХВАЧЕНА КОМАНДА /ex: {update.message.text}")
        logger.info(f"🎯 ПЕРЕХВАЧЕНА КОМАНДА /ex: {update.message.text}")
        await export_operations(update, context)

    application.add_handler(
        MessageHandler(
            filters.TEXT & filters.Regex(r'^/ex'),
            export_wrapper
        ),
        group=-2  # САМЫЙ ВЫСОКИЙ ПРИОРИТЕТ
    )

    # ✅ ОСТАЛЬНЫЕ КОМАНДЫ
    logger.info("📝 Регистрация остальных команд...")
    print("📝 Регистрация остальных команд...")
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("bal", show_balance))
    application.add_handler(CommandHandler("balance", show_balance))
    application.add_handler(CommandHandler("his", show_history))
    application.add_handler(CommandHandler("history", show_history))
    application.add_handler(CommandHandler("del", undo_last_operation))
    application.add_handler(CommandHandler("export", export_wrapper))  # алиас
    application.add_handler(CommandHandler("cancel", cancel_any))

    # Callback кнопки
    logger.info("📝 Регистрация callback обработчиков...")
    print("📝 Регистрация callback обработчиков...")
    application.add_handler(CallbackQueryHandler(general_button_callback, pattern="^(show_balance|show_history)$"))
    application.add_handler(CallbackQueryHandler(undo_select_operation, pattern="^undo_select_"))
    application.add_handler(CallbackQueryHandler(cancel_undo, pattern="^cancel_undo$"))

    # Текстовые обработчики (group 0, 1 - ПОСЛЕ команд)
    logger.info("📝 Регистрация текстовых обработчиков...")
    print("📝 Регистрация текстовых обработчиков...")
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_delete_password), group=0)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text), group=1)
    application.add_handler(MessageHandler(filters.PHOTO, handle_photo), group=1)

    async def post_init(app: Application):
        global batch_task
        batch_task = asyncio.create_task(process_operation_batch())
        logger.info("Фоновая задача батчинга запущена")
        print("✅ Фоновая задача батчинга запущена")

    async def post_shutdown(app: Application):
        global batch_task
        if batch_task:
            batch_task.cancel()
            try:
                await batch_task
            except asyncio.CancelledError:
                logger.info("Фоновая задача батчинга остановлена")
                print("✅ Фоновая задача батчинга остановлена")

    application.post_init = post_init
    application.post_shutdown = post_shutdown
    application.add_error_handler(error_handler)

    logger.info("Бот успешно запущен!")
    print("\n" + "=" * 60)
    print("🚀 БОТ УСПЕШНО ЗАПУЩЕН")
    print("=" * 60)
    print("  Команды экспорта: /ex, /ex сегодня, /ex 15.01.2026")
    print("=" * 60 + "\n")

    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=False)

if __name__ == "__main__":
    main()