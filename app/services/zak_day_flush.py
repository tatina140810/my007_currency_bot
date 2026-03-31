"""
Вечерний сброс буфера сообщений группы «Зак»: один проход по всем сырым
сообщениям за день → ИИ (или regex) → запись в «Проценты_детально».
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

from openai import AsyncOpenAI

from app.core.config import OPENAI_API_KEY, ZAK_DEFER_SHEET_TO_EVENING, ZAK_EVENING_USE_AI
from app.services.ai_retry import call_openai_with_retry
from app.services.google_sheets_zak import append_zak_operations_to_sheet
from app.services.parser import normalize_currency
from app.services.zak_parser import parse_zak_message

logger = logging.getLogger(__name__)

KG_TZ = ZoneInfo("Asia/Bishkek")


def _anchor_datetime(day_kg: str) -> datetime:
    d = date.fromisoformat(day_kg)
    return datetime(d.year, d.month, d.day, 12, 0, 0, tzinfo=KG_TZ)


def _batch_message_id(chat_id: int, day_kg: str) -> int:
    h = hashlib.md5(f"{chat_id}:{day_kg}:zak_evening".encode()).hexdigest()
    return int(h[:12], 16) % 2_000_000_000


def _parse_row_message_at(s: str | None, fallback: datetime) -> datetime:
    if not s:
        return fallback
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(KG_TZ)
    except Exception:
        return fallback


def _fee_fields(amount: float, percent_val: float, fee_mode: str) -> tuple[float, float, float]:
    if fee_mode == "included":
        gross = amount
        fee = amount * percent_val / (1 + percent_val) if percent_val else 0.0
        net_amount = amount - fee
        return fee, net_amount, gross
    fee = amount * percent_val
    net_amount = amount
    gross_amount = amount + fee
    return fee, net_amount, gross_amount


def _normalize_op(
    raw: dict[str, Any],
    chat_id: int,
    batch_msg_id: int,
    anchor: datetime,
    combined_raw_text: str,
) -> dict[str, Any] | None:
    try:
        amount = float(raw.get("amount") or 0)
    except (TypeError, ValueError):
        return None
    if amount <= 0:
        return None

    op_type = (raw.get("type") or "Unknown").strip()
    tl = op_type.lower()
    if "сняти" in tl:
        op_type = "Снятие"
    elif "пополн" in tl:
        op_type = "Пополнение"
    bank = (raw.get("bank") or "Unknown").strip().upper()
    if bank not in ("РСК", "БАКАЙ"):
        if "рск" in bank.lower():
            bank = "РСК"
        elif "бакай" in bank.lower() or "bakai" in bank.lower():
            bank = "БАКАЙ"
        else:
            bank = "Unknown"

    company = (raw.get("company") or "").strip() or "—"
    curr = raw.get("currency") or "RUB"
    curr = normalize_currency(str(curr)) if curr else "RUB"

    percent_str = (raw.get("percent_str") or "").strip()
    try:
        percent_val = float(raw.get("percent_value"))
    except (TypeError, ValueError):
        percent_val = 0.0
    if not percent_str and percent_val > 0:
        percent_str = f"{percent_val * 100:.4g}%".replace(".", ",")

    fee_mode = (raw.get("fee_mode") or "extra").strip().lower()
    if fee_mode not in ("included", "extra"):
        fee_mode = "extra"

    fee, net_amount, gross_amount = _fee_fields(amount, percent_val, fee_mode)
    raw_line = (raw.get("raw_line") or "").strip() or f"{company} — {amount} {curr}"

    return {
        "date": anchor,
        "chat_id": chat_id,
        "message_id": batch_msg_id,
        "type": op_type if op_type in ("Снятие", "Пополнение") else "Unknown",
        "bank": bank,
        "company": company,
        "currency": curr,
        "amount": amount,
        "percent_str": percent_str or "0%",
        "percent_value": percent_val,
        "fee_mode": fee_mode,
        "fee": fee,
        "net_amount": net_amount,
        "gross_amount": gross_amount,
        "comment": (raw.get("comment") or "").strip(),
        "raw_line": raw_line[:500],
        "raw_text": combined_raw_text[:2000],
    }


async def parse_zak_day_with_ai(
    day_kg: str,
    chat_id: int,
    transcript: str,
) -> list[dict[str, Any]] | None:
    if not OPENAI_API_KEY or not transcript.strip():
        return None

    client = AsyncOpenAI(api_key=OPENAI_API_KEY)
    system = """
Ты разбираешь переписку группы «Зак» (банковские пополнения/снятия с комиссией).
За один день пришло несколько сообщений; учитывай контекст целиком: заголовки «ПОПОЛНЕНИЯ/СНЯТИЯ», банк РСК/БАКАЙ, валюта, проценты, уточнения в следующих сообщениях.

Верни ТОЛЬКО JSON-объект: {"operations": [ ... ]}
Каждая операция — объект с полями:
- "type": "Пополнение" или "Снятие"
- "bank": "РСК" или "БАКАЙ" (если неясно — "Unknown")
- "company": краткое имя контрагента/строки
- "currency": ISO: RUB, USD, EUR, KGS, KZT, CNY, USDT и т.д.
- "amount": число (исходная сумма в валюте строки)
- "percent_str": строка вроде "0,1%" или "0.1%"
- "percent_value": доля, например 0.001 для 0.1%
- "fee_mode": "included" если взнос/процент включён в сумму, иначе "extra"
- "comment": кратко или ""
- "raw_line": одна характерная строка для дедупликации (уникальна на операцию)

Игнорируй чистую болтовню без сумм. Не дублируй одну и ту же операцию.
Если процент дан отдельным сообщением после строк с суммами — привяжи к правильной операции.
""".strip()

    user = f"Дата (календарный день, KG): {day_kg}\nchat_id={chat_id}\n\nСообщения:\n{transcript}"

    try:
        resp = await call_openai_with_retry(
            client=client,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            response_format={"type": "json_object"},
            is_vision=False,
        )
        if not resp:
            return None
        text = resp.choices[0].message.content or "{}"
        data = json.loads(text)
        ops = data.get("operations")
        if not isinstance(ops, list):
            return None
        return ops
    except Exception as e:
        logger.error(f"[ZAK day AI] parse failed: {e}", exc_info=True)
        return None


def _build_transcript(rows: list[dict]) -> str:
    parts = []
    for r in rows:
        mid = r.get("message_id")
        ts = r.get("message_at") or ""
        txt = (r.get("text") or "").strip()
        parts.append(f"--- message_id={mid} at={ts} ---\n{txt}")
    return "\n\n".join(parts)


def _operations_from_regex_buffer(
    chat_id: int,
    batch_msg_id: int,
    anchor: datetime,
    rows: list[dict],
) -> list[dict]:
    """Построчный разбор как в live-режиме (текст в буфере — уже как working_text)."""
    out: list[dict] = []

    for r in rows:
        text = (r.get("text") or "").strip()
        if not text:
            continue
        msg_dt = _parse_row_message_at(r.get("message_at"), anchor)
        mid = r["message_id"]
        combined = text
        # reply_to не храним отдельно в буфере как текст; при flush только основной текст

        parsed = parse_zak_message(combined, chat_id, mid, msg_dt)
        for op in parsed:
            if op.get("comment_only"):
                continue
            if op.get("amount", 0) <= 0:
                continue
            op = dict(op)
            op["message_id"] = batch_msg_id
            op["date"] = anchor
            op["raw_text"] = combined[:2000]
            out.append(op)
    return out


async def flush_zak_buffers_for_report_date(day_kg: str) -> None:
    """
    Вызывать перед fill_report_block за тот же календарный день (YYYY-MM-DD, KG).
    Работает только при ZAK_DEFER_SHEET_TO_EVENING=1.
    """
    if not ZAK_DEFER_SHEET_TO_EVENING:
        return

    from app.db.instance import db

    chat_ids = db.zak_buffer_pending_chat_ids(day_kg)
    if not chat_ids:
        logger.info(f"[ZAK flush] Нет незакрытого буфера за {day_kg}")
        return

    anchor = _anchor_datetime(day_kg)

    for chat_id in chat_ids:
        rows = db.zak_buffer_get_pending(chat_id, day_kg)
        if not rows:
            continue

        transcript = _build_transcript(rows)
        batch_msg_id = _batch_message_id(chat_id, day_kg)
        combined_raw = transcript[:8000]

        operations: list[dict] = []

        if ZAK_EVENING_USE_AI:
            raw_ops = await parse_zak_day_with_ai(day_kg, chat_id, transcript)
            if raw_ops:
                for ro in raw_ops:
                    if not isinstance(ro, dict):
                        continue
                    norm = _normalize_op(ro, chat_id, batch_msg_id, anchor, combined_raw)
                    if norm:
                        operations.append(norm)

        if not operations:
            logger.info(f"[ZAK flush] AI/пусто → regex fallback chat={chat_id} day={day_kg}")
            operations = _operations_from_regex_buffer(chat_id, batch_msg_id, anchor, rows)

        if not operations:
            logger.warning(f"[ZAK flush] Нет операций после разбора chat={chat_id} day={day_kg}")
            db.zak_buffer_mark_flushed(chat_id, day_kg)
            continue

        try:
            await append_zak_operations_to_sheet(operations, db_id=None)
            db.zak_buffer_mark_flushed(chat_id, day_kg)
            logger.info(
                f"[ZAK flush] Записано {len(operations)} операций chat={chat_id} day={day_kg}"
            )
        except Exception as e:
            logger.error(f"[ZAK flush] Ошибка записи в лист chat={chat_id}: {e}", exc_info=True)
