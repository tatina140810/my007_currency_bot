
# -*- coding: utf-8 -*-

import os
import re
import io
import time
import asyncio
import logging
import tempfile
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo
from collections import defaultdict
from typing import Dict
from config import REPORT_CHAT_ID
from report_export import export_report_income_matrix

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

from database import Database
from config import BOT_TOKEN, CURRENCIES, ADMIN_PASSWORD
from excel_export import export_to_excel
from auto_reply_bot import (
    is_working_time,
    AUTO_REPLY_TEXT,
    TEAM_MEMBER_IDS,
    last_auto_reply_dates,
)

# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================

GROUP_TAG_RE = re.compile(r"^\s*\[(.+?)\]\s*(.*)$")
KG_TZ = ZoneInfo("Asia/Bishkek")

CHAT_ALIASES = {
    "Арм": ["арм", "arm"],
    "ГРАНИТ ГРУПП": ["гранит", "гранит групп", "granit"],
    "Сан Тропе групп": ["сан тропе", "santrope", "san trope"],
    "ЕВРАЗИЯ РЕСУРС": ["евразия", "евразия ресурс", "eurasia"],
    "Локал": ["локал", "local"],
    "Соода КЖ": ["соода", "sooda", "соода кж"],
    "VR GROUP": ["vr", "vr group"],
    "ИЛР Салют групп": ["илр", "салют", "ilr"],
    "Профлайн": ["профлайн", "proflin", "profile"],
    "Руб нерез": ["руб нерез", "нерез", "rub nerez"],
    "Документы Локал": ["док локал", "документы локал"],
    "Группа КОСВЕЛЛ": ["косвелл", "kosvell"],
    "Хуагэ Москва": ["хуагэ", "huage"],
    "МИНСК": ["минск", "minsk"],
    "Бутчер": ["бутчер", "butcher"],
    "Поставки из Китая": ["китай", "поставки", "china"],
    "Трейд Шоп": ["трейд", "trade shop"],
    "Группа ВЭД ББ": ["вэд", "ved"],
    "Карина": ["карина", "karina"],
    "Аскар": ["аскар", "askar"],
    "China Ru": ["china ru", "чайна"],
    "Карвен групп": ["карвен", "karven"],
    "Брокер": ["брокер", "broker"],
    "Center Tex FI": ["center tex", "tex"],
    "Шеф": ["шеф", "chef"],
    "Нарго групп": ["нарго", "nargo"],
    "Тим": ["тим", "team"],
    "Милан - ТезКадам Бакай Банк": ["милан", "тезкадам", "bakai"],
    "Каню": ["каню", "kanyu"],
    "Автокит": ["автокит", "autokit"],
    "Вояж групп": ["вояж", "voyage"],
    "Сергей Москва": ["сергей москва", "сергей"],
    "Дельмар": ["дельмар", "delmar"],
    "Barracuda": ["barracuda", "барракуда"],
    "tatinadz": ["tatina", "татина"],
    "ТиР - FinInfra": ["тир", "fininfra"],
    "УЗ": ["уз", "uз", "uz", "у з"],
    "УФА": ["уфа", "ufa"],
    "ЭКСПО": ["экспо", "expo"],
    "Денис Биш": ["денис", "denis", "денис биш"],
    "Группа Иван": ["иван", "ivan"],
    "НРК": ["нрк", "nrk"],
    "Гармин": ["гармин", "garmin"],
    "Тамеки КЖ": ["тамеки", "tameki"],
    "Киргизия 2.0": ["киргизия", "kg 2.0"],
    "РД": ["рд", "rd"],
    "Амбер Платинум": ["амбер", "amber"],
    "Медигрупп": ["меди", "medigroup", "медигрупп"],
    "Barracuda и Adonai": ["adonai", "барракуда адонай"],
    "Сокол": ["сокол", "sokol"],
    "ИЛЬ": ["иль", "il"],
    "КЬЮБ": ["кьюб", "cube"],
    "КЕША": ["кеша", "kesha"],
    "Фин.инфра-СЗ": ["фин инфра", "fininfra sz"],
    "АБАТ СТОР": ["абат", "abat"],
    "Евро Авто": ["евро авто", "euro auto"],
    "ВОРД": ["ворд", "word"],
    "Влата": ["влата", "vlata"],
    "Али": ["али", "ali"],
    "АТЕКС": ["атекс", "atex"],
    "Грузин": ["грузин", "gruzin"],
    "Марат групп": ["марат", "marat"],
    "АКА групп": ["ака", "aka"],
    "РФ ДЕН": ["рф ден", "rf den"],
    "Сергей евро": ["сергей евро", "sergey euro"],
    "Группа Антилопа": ["антилопа", "antilope"],
    "ДЕЛЬТА": ["дельта", "delta"],
}

# ============================================================
# ЛОГИРОВАНИЕ
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("telegram.ext").setLevel(logging.WARNING)
# ✅ Подавляем таймауты при остановке
logging.getLogger("telegram.ext.Updater").setLevel(logging.CRITICAL)

logger = logging.getLogger(__name__)

# ============================================================
# ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ
# ============================================================

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

# Кеширование балансов
balance_cache: Dict[int, Dict[str, float]] = {}
balance_cache_time: Dict[int, float] = {}
CACHE_TTL = 5

# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================

def extract_group_tag(text: str) -> tuple[str | None, str]:
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
    
    Примеры:
        "уз" → "УЗ"
        "uz" → "УЗ"
        "денис" → "Денис Биш"
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

    # Если не найдено - возвращаем как есть
    return name.strip()


def is_staff(user_id: int | None) -> bool:
    """Проверяет является ли пользователь сотрудником"""
    return user_id is not None and user_id in TEAM_MEMBER_IDS


def resolve_target_chat_id(
    chat,
    is_private: bool,
    group_from_manual: str | None = None,
):
    """
    Определяет chat_id для записи операции.
    
    Логика:
    - В личном чате: ТРЕБУЕТСЯ group_from_manual (из [ГРУППА])
    - В групповом чате: используется текущий chat.id
    """
    # ЛИЧНЫЙ ЧАТ
    if is_private:
        if not group_from_manual:
            raise ValueError(
                "В личном чате нужно указать группу в квадратных скобках.\n"
                "Пример:\n[УЗ] поступили 5000 usdt"
            )

        target_chat_id = db.get_chat_id_by_name(group_from_manual)
        if not target_chat_id:
            raise ValueError(f"Группа '{group_from_manual}' не найдена")

        return target_chat_id

    # ГРУППОВОЙ ЧАТ
    return chat.id


def get_cached_balance(chat_id: int):
    """Получает баланс с кешированием"""
    now = datetime.now().timestamp()
    if chat_id in balance_cache:
        if now - balance_cache_time.get(chat_id, 0) < CACHE_TTL:
            return balance_cache[chat_id]
    
    balances = db.get_balances(chat_id)
    balance_cache[chat_id] = balances
    balance_cache_time[chat_id] = now
    return balances


def invalidate_balance_cache(chat_id: int):
    """Инвалидирует кеш баланса"""
    balance_cache.pop(chat_id, None)
    balance_cache_time.pop(chat_id, None)


def get_chat_id(update: Update) -> int:
    """Получает ID чата"""
    return update.effective_chat.id


def get_chat_name(update: Update) -> str:
    """Получает название чата"""
    chat = update.effective_chat
    if chat.type == "private":
        return f"Личный чат с {update.effective_user.first_name}"
    return chat.title or f"Группа {chat.id}"


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


def normalize_currency(curr: str) -> str:
    """Нормализует валюту (без ошибок USDT → USD)"""
    if not curr:
        return ""

    c = curr.strip().lower()

    # убираем точки, запятые и пробелы по краям
    c = c.replace(".", "").replace(",", "").strip()

    # 🔥 ВАЖНО: USDT проверяем ПЕРВЫМ
    if c in ("usdt", "тез", "тезер"):
        return "USDT"

    curr_map = {
        # RUB
        "руб": "RUB", "₽": "RUB", "рублей": "RUB", "rub": "RUB", "рубля": "RUB",

        # KGS
        "сом": "KGS", "сомов": "KGS", "kgs": "KGS",

        # USD
        "usd": "USD", "долл": "USD", "$": "USD", "дол": "USD",
        "доллар": "USD", "долларов": "USD", "долларах": "USD",

        # EUR
        "eur": "EUR", "€": "EUR", "ев": "EUR", "евро": "EUR",

        # KZT
        "kzt": "KZT", "тенге": "KZT",

        # CNY
        "cny": "CNY", "yuan": "CNY", "¥": "CNY",
        "юан": "CNY", "юань": "CNY", "юаней": "CNY", "юани": "CNY", "юаня": "CNY",

        # AED
        "aed": "AED", "дирхам": "AED", "дирхамов": "AED", "дир": "AED",
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


def migrate_legacy_currencies():
    """Миграция старых валют"""
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


# ============================================================
# БАТЧИНГ ОПЕРАЦИЙ
# ============================================================

async def process_operation_batch():
    """Фоновая задача для обработки очереди операций"""
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
    chat_id: int, 
    op_type: str, 
    currency: str, 
    amount: float, 
    description: str = ""
):
    """Добавляет операцию в очередь"""
    async with queue_lock:
        operation_queue[chat_id].append({
            "type": op_type,
            "currency": currency,
            "amount": amount,
            "description": description,
        })


# ============================================================
# ПАРСИНГ ОПЕРАЦИЙ
# ============================================================

def parse_income_notification(text: str):
    if not text:
        return None

    text = _norm_ws(text)
    low = text.lower()

    # более мягкая проверка "поступ/зачисл"
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
        logger.info("[INCOME_PARSE] no money match")
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


def parse_manual_operation_line(text: str) -> dict | None:
    """
    Парсит РУЧНЫЕ операции.
    ГРУППА определяется СНАРУЖИ через [ГРУППА].
    """
    if not text:
        return None

    t = text.lower().strip()

    # --------------------
    # ПОСТУПЛЕНИЕ (ручное)
    # --------------------
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

    # --------------------
    # ВЗНОС НАЛИЧНЫМИ
    # --------------------
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

    # --------------------
    # ВЫДАЧА
    # --------------------
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

    # --------------------
    # ОПЛАТА ПП
    # --------------------
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

    # --------------------
    # ФИКС (КОНВЕРТАЦИЯ)
    # пример: фикс 200 usd 80.4 rub
    # --------------------
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


    # --------------------
    # ХАРБОР КОМИССИЯ
    # --------------------
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

    # --------------------
    # ЗАПРОС БАНКУ (фиксированная комиссия)
    # --------------------
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


def parse_bulk_pp_payments(clean_text: str):
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


def compute_conversion_to_amount(amount: float, rate: float, from_curr: str, to_curr: str) -> float:
    """Вычисляет сумму конвертации"""
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

# ============================================================
# ОБРАБОТКА ТЕКСТА
# ============================================================
def _norm_ws(s: str) -> str:
    if not s:
        return ""
    # неразрывные/тонкие пробелы -> обычные
    return s.replace("\u00A0", " ").replace("\u202F", " ")


def looks_like_bank_income(text: str) -> bool:
    t = _norm_ws(text or "").lower().strip()

    # исключаем ручные операции
    if t.startswith(("оплата", "взнос", "выдача", "фикс", "запрос")):
        return False

    # ловим поступ… / зачисл… даже с опечатками типа "поступлии"
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

def compute_fixed_payment_amount(buy_amount: float, rate: float) -> float:
    if rate <= 0:
        raise ValueError("Курс должен быть > 0")
    return buy_amount * rate

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):

    message = update.effective_message
    user = update.effective_user
    chat = update.effective_chat

    if not message or not user or not chat:
        return

    if user.is_bot or not message.text:
        return

    text = message.text.strip()
    is_private = chat.type == "private"
    staff = is_staff(user.id)

    logger.info(
        f"MSG chat={chat.id} user={user.id} private={is_private} text='{text[:100]}'"
    )

    # 1️⃣ КОМАНДЫ (кроме /clear all)
    if text.startswith("/") and text.lower() != "/clear all":
        return

    # 2️⃣ РЕГИСТРАЦИЯ ЧАТА
    chat_name = chat.title or chat.first_name or f"Чат {chat.id}"
    db.register_chat(chat.id, chat_name, chat.type)

    # 3️⃣ CLEAR ALL (ТОЛЬКО STAFF + ЛИЧКА)
    if is_private and staff and text.lower() == "/clear all":
        db.clear_all()
        balance_cache.clear()
        balance_cache_time.clear()
        await message.reply_text("База очищена.")
        return
    # 4️⃣ ИЗВЛЕЧЕНИЕ ГРУППЫ ИЗ [ГРУППА] (ТОЛЬКО В ЛИЧКЕ)
    group_name = None
    clean_text = text

    if is_private:
        group_tag, clean_text = extract_group_tag(text)
        if group_tag:
            # Нормализуем группу
            group_name = normalize_group_name(group_tag)
            logger.info(f"📋 Извлечена группа: '{group_tag}' → '{group_name}'")

    # 5️⃣ АВТО-ПОСТУПЛЕНИЯ (БАНК)
    if looks_like_bank_income(clean_text):
        logger.info(f"[AUTO_INCOME] matched: chat={chat.id}")

        income = parse_income_notification(clean_text)
        if not income:
            logger.info("[AUTO_INCOME] parse_income_notification=None")
            return

        # Личка - группа обязательна
        if is_private:
            if not group_name:
                await message.reply_text(
                    "❗ В личном чате укажи группу ПЕРЕД сообщением.\n"
                    "Пример:\n[УЗ] поступили 5000 usdt"
                )
                return

            target_chat_id = db.get_chat_id_by_name(group_name)
            if not target_chat_id:
                await message.reply_text(f"❌ Группа '{group_name}' не найдена")
                return
        else:
            # Группа - пишем в текущий чат
            target_chat_id = chat.id

        await queue_operation(
            target_chat_id,
            "Поступление",
            income["currency"],
            income["amount"],
            income["description"],
        )

        logger.info(
            f"[AUTO_INCOME] queued {income['amount']} {income['currency']} -> chat {target_chat_id}"
        )
        return

    if staff:
        bulk = parse_bulk_pp_payments(clean_text)
        if bulk:
            for item in bulk:
                target_group = normalize_group_name(item["group"])
                target_chat_id = db.get_chat_id_by_name(target_group)
                if not target_chat_id:
                    continue

                desc = f"{item['company']} | {item['receiver']}"
                await queue_operation(
                    target_chat_id,
                    "Оплата ПП",
                    item["currency"],
                    -item["amount"],
                    desc,
                )
            await message.reply_text("✅ Bulk платежи обработаны")
            return

    # =====================================================
    # 4️⃣ РУЧНЫЕ ОПЕРАЦИИ
    # =====================================================
    if not staff:
        return

    manual = parse_manual_operation_line(clean_text)
    if not manual:
        return

    target_chat_id = resolve_target_chat_id(
        chat=chat,
        is_private=is_private,
        group_from_manual=group_name,
    )

    op_type = manual["type"]
    amount = manual["amount"]
    currency = manual["currency"]
    desc = manual.get("description", "")

    # --------------------
    # КОНВЕРТАЦИЯ
    # --------------------
    if op_type == "Конвертация":
        rate = manual["rate"]
        to_curr = manual["to_currency"]

        if rate <= 0:
            await message.reply_text("❗ Курс должен быть больше 0", parse_mode=None)
            return

        # ✅ ФИКС = ОТКУП: фикс 140000 cny 11.4 rub
        # значит: +140000 CNY, - (140000 * 11.4) RUB
        if desc == "Фикс":
            pay_amount = round(amount * rate, 6)

            # покупаем валюту откупа
            await queue_operation(target_chat_id, "Конвертация", currency, amount, desc)

            # платим валютой оплаты
            await queue_operation(target_chat_id, "Конвертация", to_curr, -pay_amount, desc)
            return

        # -------------------------------------------------------
        # ❗ НЕ фикс: оставляем старую логику (как было у тебя)
        # -------------------------------------------------------
        to_amount = compute_conversion_to_amount(amount, rate, currency, to_curr)

        await queue_operation(target_chat_id, "Конвертация", currency, -amount, desc)
        await queue_operation(target_chat_id, "Конвертация", to_curr, to_amount, desc)
        return


    # --------------------
    # ПРОЧИЕ
    # --------------------
    sign = -1 if op_type in ("Выдача наличных", "Оплата ПП", "Комиссия") else 1

    await queue_operation(
        target_chat_id,
        op_type,
        currency,
        sign * amount,
        desc,
    )

   
# ============================================================
# КОМАНДЫ
# ============================================================
async def cmd_rep(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user

    # Только личка
    if not chat or chat.type != "private":
        return

    # Если нужно — ограничь доступ только staff
    # if not is_staff(user.id):
    #     await update.message.reply_text("⛔️ Только для сотрудников", parse_mode=None)
    #     return

    # Дата отчёта: по умолчанию сегодня, можно /rep 02.02.2026
    report_date = datetime.now(KG_TZ).date()
    if context.args:
        arg = " ".join(context.args).strip()
        parsed = None
        for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d.%m.%y"):
            try:
                parsed = datetime.strptime(arg, fmt).date()
                break
            except ValueError:
                continue
        if not parsed:
            await update.message.reply_text(
                "❌ Неверный формат даты.\nПример: /rep сегодня или /rep 05.02.2026",
                parse_mode=None
            )
            return
        report_date = parsed

    report_date_str = report_date.isoformat()

    rows = db.get_report_income_by_date(REPORT_CHAT_ID, report_date_str)
    if not rows:
        await update.message.reply_text(
            f"За {report_date.strftime('%d.%m.%Y')} нет подходящих поступлений в чате {REPORT_CHAT_ID}.",
            parse_mode=None
        )
        return

    base_dir = os.path.join(os.getcwd(), "outputs")
    os.makedirs(base_dir, exist_ok=True)

    filename = f"report_income_{report_date_str}.xlsx"
    output_path = os.path.join(base_dir, filename)

    # экспорт в отдельном потоке, чтобы не блокировать event loop
    await asyncio.to_thread(export_report_income_matrix, rows, output_path, report_date_str)

    with open(output_path, "rb") as f:
        await update.message.reply_document(
            document=f,
            filename=filename,
            caption=f"📄 Отчет поступлений за {report_date.strftime('%d.%m.%Y')}\nИсточник: чат {REPORT_CHAT_ID}",
        )
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start"""
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
- Взнос: "взнос наличными 5000 usd"
- Выдача: "выдача наличными 3000 usd"
- Возврат: "возврат 1000 usd"

В личном чате используй [ГРУППА]:
[УЗ] поступили 5000 usdt
"""
    await update.message.reply_text(base_text, parse_mode=None)


async def show_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /bal"""
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
    """Команда /his"""
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

async def cmd_balances(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"[ALLBAL] called chat={update.effective_chat.id} user={update.effective_user.id if update.effective_user else None}")

    fd, path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)

    try:
        db.export_group_balances_to_excel(path)

        filename = f"остатки_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.xlsx"
        with open(path, "rb") as f:
            await update.message.reply_document(
                document=f,
                filename=filename,
                caption="Остатки по группам (Excel)"
            )

        logger.info(f"[ALLBAL] sent file {filename} size={os.path.getsize(path)}")

    except Exception as e:
        logger.exception("[ALLBAL] error")
        await update.message.reply_text(f"❌ Ошибка /allbal: {e}")

    finally:
        try:
            os.remove(path)
        except Exception:
            pass



async def undo_last_operation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /del"""
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
    """Callback для выбора операции на удаление"""
    user = update.effective_user or update.callback_query.from_user
    if not is_staff(user.id):
        await update.callback_query.answer("Нет прав", show_alert=True)
        return

    query = update.callback_query
    await query.answer()
    chat_id = get_chat_id(update)
    op_id = int(query.data.replace("undo_select_", ""))
    logger.info(f"Выбрана операция {op_id} для удаления в чате {chat_id}")

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
    """Обработка пароля для удаления"""
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
    """Отмена удаления (callback)"""
    query = update.callback_query
    await query.answer()
    context.user_data.pop("pending_undo_op_id", None)
    context.user_data.pop("pending_undo_chat_id", None)
    await query.edit_message_text("Отменено", parse_mode=None)


async def cancel_any(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /cancel"""
    if "pending_undo_op_id" in context.user_data:
        context.user_data.pop("pending_undo_op_id", None)
        context.user_data.pop("pending_undo_chat_id", None)
        await update.message.reply_text("Отменено", parse_mode=None)
        return
    await update.message.reply_text("Нечего отменять.", parse_mode=None)


async def export_operations(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /ex - экспорт в Excel"""
    message_text = update.message.text.strip()

    logger.info("=" * 60)
    logger.info(f"КОМАНДА: {message_text}")
    logger.info(f"context.args: {context.args}")
    logger.info("=" * 60)

    chat = update.effective_chat
    chat_id = chat.id
    telegram_chat_name = chat.title or chat.first_name or f"Чат {chat.id}"
    db.register_chat(chat.id, telegram_chat_name, chat.type)

    status_msg = await update.message.reply_text("⏳ Формирую файл...", parse_mode=None)

    # Парсим дату из текста
    date_from = None
    date_to = None

    parts = message_text.split(maxsplit=1)

    if len(parts) > 1:
        arg = parts[1].strip()
        logger.info(f"✅ Найден аргумент: '{arg}'")

        arg_lower = arg.lower()

        if arg_lower in ("сегодня", "today"):
            date_from = date_to = datetime.now(KG_TZ).date()
            logger.info(f"✅ Экспорт за СЕГОДНЯ: {date_from}")
        else:
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

    # Имя файла
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

    # Экспорт
    try:
        logger.info(f"🔄 Запуск экспорта... (from={date_from}, to={date_to})")

        await asyncio.to_thread(
            export_to_excel,
            db,
            output_path,
            date_from,
            date_to
        )

        if not os.path.exists(output_path):
            logger.error(f"❌ Файл не создан: {output_path}")
            await status_msg.edit_text("❌ Ошибка: файл не был создан", parse_mode=None)
            return

        file_size = os.path.getsize(output_path)
        logger.info(f"✅ Файл создан: размер {file_size} байт")

        try:
            await status_msg.delete()
        except:
            pass

        with open(output_path, "rb") as file:
            caption_text = datetime.now(KG_TZ).strftime("%d.%m.%Y %H:%M")
            if date_from:
                caption_text += f"\nОперации за {date_from.strftime('%d.%m.%Y')}"
            else:
                caption_text += f"\n Все операции"

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

    help_text = f"""📌 СПРАВКА
Текущий чат: {chat_name}

━━━━━━━━━━━━━━━━━━
✅ ОСНОВНЫЕ КОМАНДЫ
/bal — показать баланс по текущей группе
/his — история операций за сегодня
/his 01.12.2025 — история за дату (ДД.ММ.ГГГГ)
/del — удалить операцию (за сегодня, через пароль)
/ex — экспорт операций в Excel (за всё время)
/ex сегодня — экспорт за сегодня
/ex 15.01.2026 — экспорт за дату
/allbal — Excel: остатки по всем группам (только staff)
/chats — список групп, которые есть в базе (только staff)
/cancel — отмена ввода пароля при удалении

━━━━━━━━━━━━━━━━━━
✅ КАК ДОБАВЛЯТЬ ОПЕРАЦИИ (только staff)

1) Авто-поступления (банк)
Просто отправь текст банка, бот сам распознает «поступили / зачислено» и сумму.
В личке ОБЯЗАТЕЛЬНО указывать группу:
[УЗ] поступили 5000 usdt

2) Ручные операции (в личке указывать [ГРУППА])
[УЗ] поступили 5000 usdt
[УЗ] взнос наличными 1000 usd
[УЗ] выдача 2000 usd
[УЗ] оплата пп 1500 usd
[УЗ] харбор комиссия 50 usd
[УЗ] запрос банку 65 usd

3) Конвертация (фикс/откуп)
Формат:
[УЗ] фикс 140000 cny 11.4 rub
Что делает бот:
+140000 CNY
-(140000 * 11.4) RUB

━━━━━━━━━━━━━━━━━━
💱 Валюты:
USD, EUR, RUB, CNY, KGS, KZT, USDT, AED

⚠️ SWIFT/OCR распознавание по фото сейчас ОТКЛЮЧЕНО.
"""
    await update.message.reply_text(help_text, parse_mode=None)


async def cmd_chats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /chats - показывает все чаты"""
    user = update.effective_user

    if not is_staff(user.id):
        return

    chats = db.get_all_chats()
    logger.info(f"/chats raw data: {chats}")

    if not chats:
        await update.message.reply_text("Группы не найдены.")
        return

    lines = ["📋 Чаты в базе:"]

    for row in chats:
        chat_id = row[0]
        try:
            chat = await context.bot.get_chat(chat_id)
            title = chat.title or chat.username or f"ID {chat_id}"
            lines.append(f"• {title}")
        except Exception:
            lines.append(f"• ID {chat_id} (недоступен)")

    await update.message.reply_text("\n".join(lines), parse_mode=None)

def extract_client_from_bank_text(text: str) -> str:
    t = (text or "").strip().rstrip(".!,;:)'\"")
    m = CLIENT_AT_END_RE.search(t)
    return (m.group(1).upper() if m else "UNKNOWN")

async def general_button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка callback кнопок"""
    query = update.callback_query
    logger.info(f"Callback: {query.data}")
    await query.answer()
    
    if query.data == "show_balance":
        await show_balance(update, context)
    elif query.data == "show_history":
        await show_history(update, context)


async def log_all_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Логирование всех сообщений"""
    if update.message and update.message.text:
        text = update.message.text
        user_id = update.effective_user.id if update.effective_user else "unknown"
        chat_id = update.effective_chat.id if update.effective_chat else "unknown"

        logger.info("=" * 80)
        logger.info(f"📨 ВХОДЯЩЕЕ СООБЩЕНИЕ: '{text}' from user {user_id} in chat {chat_id}")
        logger.info("=" * 80)


async def error_handler(update, context):
    """Обработка ошибок"""
    logger.exception("Unhandled exception", exc_info=context.error)


# ============================================================
# MAIN
# ============================================================

def main():
    """Главная функция"""
    global batch_task
    
    logger.info("Запуск бота...")
    print("🤖 ЗАПУСК БОТА...")

    migrate_legacy_currencies()

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(60)
        .read_timeout(60)
        .write_timeout(60)
        .build()
    )

    # Универсальный логгер (group=-1)
    logger.info("📝 Регистрация универсального логгера...")
    application.add_handler(
        MessageHandler(filters.ALL, log_all_messages),
        group=-1
    )

    # Команда /ex (group=-2 - самый высокий приоритет)
    logger.info("📝 Регистрация команды /ex...")

    async def export_wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обёртка для /ex"""
        logger.info(f"🎯 ПЕРЕХВАЧЕНА КОМАНДА /ex: {update.message.text}")
        await export_operations(update, context)

    application.add_handler(
        MessageHandler(
            filters.TEXT & filters.Regex(r'^/ex'),
            export_wrapper
        ),
        group=-2
    )

    # Остальные команды
    logger.info("📝 Регистрация остальных команд...")
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("bal", show_balance))
    application.add_handler(CommandHandler("balance", show_balance))
    application.add_handler(CommandHandler("his", show_history))
    application.add_handler(CommandHandler("history", show_history))
    application.add_handler(CommandHandler("del", undo_last_operation))
    application.add_handler(CommandHandler("export", export_wrapper))
    application.add_handler(CommandHandler("cancel", cancel_any))
    application.add_handler(CommandHandler("chats", cmd_chats))
    application.add_handler(CommandHandler("rep", cmd_rep))


    # Callback кнопки
    logger.info("📝 Регистрация callback обработчиков...")
    application.add_handler(CallbackQueryHandler(general_button_callback, pattern="^(show_balance|show_history)$"))
    application.add_handler(CallbackQueryHandler(undo_select_operation, pattern="^undo_select_"))
    application.add_handler(CallbackQueryHandler(cancel_undo, pattern="^cancel_undo$"))

    # Текстовые обработчики
    logger.info("📝 Регистрация текстовых обработчиков...")
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_delete_password), group=0)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text), group=1)
    application.add_handler(MessageHandler(filters.COMMAND & filters.Regex(r"^[\s\u00A0\u200B\u200C\u200D]*[/／]allbal(?:@\w+)?(?:\s|$)"
        ),
        cmd_balances
    ),
    group=-2
    )

    async def post_init(app: Application):
        global batch_task
        batch_task = asyncio.create_task(process_operation_batch())
        logger.info("Фоновая задача батчинга запущена")

    async def post_shutdown(app: Application):
        global batch_task
        if batch_task:
            batch_task.cancel()
            try:
                await batch_task
            except asyncio.CancelledError:
                logger.info("Фоновая задача батчинга остановлена")

    application.post_init = post_init
    application.post_shutdown = post_shutdown
    application.add_error_handler(error_handler)

    logger.info("Бот успешно запущен!")
    print("\n" + "=" * 60)
    print("🚀 БОТ УСПЕШНО ЗАПУЩЕН")
    print("=" * 60)
    print("  📊 Команды экспорта: /ex, /ex сегодня, /ex 15.01.2026")
    print("=" * 60 + "\n")

    application.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=False)


if __name__ == "__main__":
    main()