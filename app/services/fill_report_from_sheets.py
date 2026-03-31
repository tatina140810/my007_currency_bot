"""
Service: fill_report_from_sheets.py

Reads sums for a given date from the source Google Sheet tabs:
  - Входящие суммы  ← ЗАПРОСЫ ПО ВХОД.СУММАМ И ДОКИ  (col A=date_text, col C=currency, col D=amount)
  - Снятия          ← Проценты_детально  (col A=datetime_text, col B=type, col E=currency, col F=amount)
  - Ком. за снятие  ← Проценты_детально  (F × G/100 where B=Снятие)
  - Ком. за попол.  ← Проценты_детально  (F × G/100 where B=Пополнение)
  - Платежи         ← Платежи  (col A contains "--- ПЛАТЕЖИ ЗА DD.MM.YYYY ---", col E=currency, col F=amount)
  - Swift комиссия  ← Платежи  (same rows, col G = bank commission formula value)

Then writes those computed values into the "отчет по остаткам" block for that date.
Called automatically when evening Excel is uploaded.
"""
import logging
import time
from datetime import date, datetime
from typing import Dict

logger = logging.getLogger(__name__)

# Currency code normalizer: maps how currencies appear in each sheet → standard key
_CURR_ALIASES = {
    "rub": "RUB", "руб": "RUB",
    "usd": "USD", "доллар": "USD",
    "eur": "EUR", "euro": "EUR", "евро": "EUR", "eu": "EUR",
    "cny": "CNY", "юань": "CNY", "rmb": "CNY",
    "kzt": "KZT", "тенге": "KZT",
}
# Sheet column letters for the report (B=RUB, C=USD, D=EUR, E=CNY, F=KZT)
_COL_ORDER = ["RUB", "USD", "EUR", "CNY", "KZT"]


def _norm_curr(raw: str) -> str:
    return _CURR_ALIASES.get(raw.strip().lower(), raw.strip().upper())


def _to_float(val: str) -> float:
    """Convert a cell value to float. Strips %, spaces, commas, nbsp."""
    try:
        cleaned = (
            str(val)
            .replace(" ", "").replace(",", ".").replace("\xa0", "")
            .replace("%", "").replace("=", "").strip()
        )
        return float(cleaned) if cleaned else 0.0
    except Exception:
        return 0.0


def _read_zaprosy(ws_zap, date_str: str) -> Dict[str, float]:
    """Sum col D (amount) where col A starts with date_str, grouped by col C (currency).
    ЗАПРОСЫ layout: A=Дата/Время, B=Описание, C=Валюта, D=Поступления, E=Баланс день, F=Баланс месяц, ..., I=msg_id
    """
    rows = ws_zap.get_all_values()
    totals: Dict[str, float] = {}
    matched = 0
    for r in rows[1:]:
        if len(r) < 4:
            continue
        cell_date = str(r[0]).strip()
        # Handles "27.03.2026 15:30:00" and plain "27.03.2026"
        if not cell_date.startswith(date_str):
            continue
        curr = _norm_curr(str(r[2]).strip()) if len(r) > 2 else "?"
        amt = _to_float(r[3]) if len(r) > 3 else 0.0
        if curr and amt:
            totals[curr] = totals.get(curr, 0.0) + amt
            matched += 1
    logger.info(f"[fill_report] ЗАПРОСЫ matched {matched} rows for {date_str}: {totals}")
    return totals


def _read_protsenty(ws_prot, date_str: str):
    """
    Returns (snatiya, kom_snat, kom_pop):
      snatiya  = {curr: sum(F=Сумма исходная) where B contains Снятие/Выдача}
      kom_snat = {curr: sum(I=Сумма комиссии) where B contains Снятие/Выдача}
      kom_pop  = {curr: sum(I=Сумма комиссии) where B contains Пополнение/Взнос}

    Проценты_детально layout (per ZAK writer):
      A=Дата, B=Тип операции, C=Банк, D=Компания, E=Валюта,
      F=Сумма исходная, G=Процент, H=Режим комиссии, I=Сумма комиссии [FORMULA RESULT],
      J=Чистая сумма, K=Сумма с комиссией, ...
    """
    rows = ws_prot.get_all_values()
    snatiya: Dict[str, float] = {}
    kom_snat: Dict[str, float] = {}
    kom_pop: Dict[str, float] = {}
    matched = 0
    for r in rows[1:]:
        if len(r) < 6:
            continue
        cell_date = str(r[0]).strip()
        if not cell_date.startswith(date_str):
            continue
        op_type = str(r[1]).strip().lower()
        curr = _norm_curr(str(r[4]).strip()) if len(r) > 4 else "?"
        amount = _to_float(r[5]) if len(r) > 5 else 0.0
        # Read the COMPUTED commission from col I (index 8) — already calculated by ZAK formula
        commission_amt = _to_float(r[8]) if len(r) > 8 else 0.0

        if "снятие" in op_type or "выдач" in op_type:
            snatiya[curr] = snatiya.get(curr, 0.0) + amount
            kom_snat[curr] = kom_snat.get(curr, 0.0) + commission_amt
            matched += 1
        elif "пополнени" in op_type or "взнос" in op_type:
            kom_pop[curr] = kom_pop.get(curr, 0.0) + commission_amt
            matched += 1

    logger.info(
        f"[fill_report] Проценты_детально matched {matched} rows for {date_str}: "
        f"снятия={snatiya}, ком_снятие={kom_snat}, ком_пополнение={kom_pop}"
    )
    return snatiya, kom_snat, kom_pop


def _read_platezhi(ws_pay, date_str: str):
    """
    Платежи sheet uses date separator rows:
      "--- ПЛАТЕЖИ ЗА DD.MM.YYYY ---" in col A, msg_id in col H
    Data rows until next separator:
      A=Банк, B=Компания, C=Тип, D=Контрагент, E=Валюта, F=Сумма, G=Комиссия_банка(formula), H=msg_id, I=Дата
    Returns (platezhi, swift):
      platezhi = {curr: sum(F)}
      swift    = {curr: sum(G)}  — formula result from Google Sheets
    """
    rows = ws_pay.get_all_values()
    platezhi: Dict[str, float] = {}
    swift: Dict[str, float] = {}
    in_target_date = False
    matched = 0

    for r in rows:
        if not r:
            continue
        col_a = str(r[0]).strip()
        # Separator row detection
        if "ПЛАТЕЖИ ЗА" in col_a.upper():
            in_target_date = date_str in col_a
            logger.debug(f"[fill_report] Платежи separator: '{col_a}' → in_target={in_target_date}")
            continue
        if not in_target_date:
            continue
        if len(r) < 6:
            continue
        curr = _norm_curr(str(r[4]).strip()) if len(r) > 4 else ""
        if not curr:
            continue
        amount = _to_float(r[5]) if len(r) > 5 else 0.0
        commission = _to_float(r[6]) if len(r) > 6 else 0.0
        if amount:
            platezhi[curr] = platezhi.get(curr, 0.0) + amount
            matched += 1
        if commission:
            swift[curr] = swift.get(curr, 0.0) + commission

    logger.info(f"[fill_report] Платежи matched {matched} rows for {date_str}: {platezhi}, swift={swift}")
    return platezhi, swift


def _read_konvertatsii(ws_konv, date_str: str):
    """
    Returns deltas: {currency: delta_amount}
    For Конвертации, 'Сумма' (col C, index 2) is the TARGET currency (+).
    'Сумма РУБ' (col F, index 5) is the SOURCE currency (-) which is always RUB.
    """
    rows = ws_konv.get_all_values()
    deltas: Dict[str, float] = {}
    matched = 0
    for r in rows[1:]:
        if len(r) < 6:
            continue
        cell_date = str(r[0]).strip()
        if not cell_date.startswith(date_str):
            continue
            
        target_amt = _to_float(r[2]) if len(r) > 2 else 0.0
        target_curr_ru = str(r[3]).strip()
        target_curr = _norm_curr(target_curr_ru)

        # "Конвертации" writer puts rub_spent as a formula "=C{row}*E{row}".
        # Sometimes gspread returns the formula text instead of the evaluated value,
        # so we fall back to computing it from (target_amt * rate) when needed.
        rub_spent = _to_float(r[5]) if len(r) > 5 else 0.0
        if rub_spent == 0.0 and len(r) > 4:
            rate_guess = _to_float(r[4])  # column E = rate
            if target_amt and rate_guess:
                rub_spent = target_amt * rate_guess
        
        if target_curr and target_amt:
            deltas[target_curr] = deltas.get(target_curr, 0.0) + target_amt
            deltas["RUB"] = deltas.get("RUB", 0.0) - rub_spent
            matched += 1
            
    logger.info(f"[fill_report] Конвертации matched {matched} rows for {date_str}: {deltas}")
    return deltas

def _find_block_start(ws_report, date_str_disp: str) -> int:
    """Find row number (1-indexed) of 'Дата: DD.MM.YYYY' in column A."""
    col_a = ws_report.col_values(1)
    for i, val in enumerate(col_a):
        if date_str_disp in str(val):
            return i + 1  # 1-indexed row number
    return 0


def fill_report_block(sh, target_date: date) -> str:
    """
    Main entry point: read sums from all source tabs and write to отчет по остаткам.
    sh = gspread spreadsheet object (already opened).
    Returns status string.
    """
    date_disp = target_date.strftime("%d.%m.%Y")
    logger.info(f"[fill_report] Filling report block for {date_disp}")

    try:
        ws_zap = sh.worksheet("ЗАПРОСЫ ПО ВХОД.СУММАМ И ДОКИ")
    except Exception as e:
        logger.error(f"[fill_report] Cannot open ЗАПРОСЫ sheet: {e}")
        ws_zap = None

    try:
        ws_prot = sh.worksheet("Проценты_детально")
    except Exception as e:
        logger.error(f"[fill_report] Cannot open Проценты_детально sheet: {e}")
        ws_prot = None

    try:
        ws_pay = sh.worksheet("Платежи")
    except Exception as e:
        logger.error(f"[fill_report] Cannot open Платежи sheet: {e}")
        ws_pay = None

    try:
        ws_report = sh.worksheet("отчет по остаткам")
    except Exception as e:
        logger.error(f"[fill_report] Cannot open отчет по остаткам: {e}")
        return f"Ошибка: лист «отчет по остаткам» не найден."

    try:
        ws_konv = sh.worksheet("конвертации")
    except Exception as e:
        logger.warning(f"[fill_report] Cannot open конвертации sheet: {e}")
        ws_konv = None

    # Read source data
    income: Dict[str, float] = {}
    snatiya: Dict[str, float] = {}
    kom_snat: Dict[str, float] = {}
    kom_pop: Dict[str, float] = {}
    platezhi: Dict[str, float] = {}
    swift: Dict[str, float] = {}
    konv: Dict[str, float] = {}

    if ws_zap:
        income = _read_zaprosy(ws_zap, date_disp)
        logger.info(f"[fill_report] Income from ЗАПРОСЫ: {income}")

    if ws_prot:
        snatiya, kom_snat, kom_pop = _read_protsenty(ws_prot, date_disp)
        logger.info(f"[fill_report] Снятия: {snatiya}, Ком.снятие: {kom_snat}, Ком.попол: {kom_pop}")

    if ws_pay:
        platezhi, swift = _read_platezhi(ws_pay, date_disp)
        logger.info(f"[fill_report] Платежи: {platezhi}, Swift: {swift}")

    if ws_konv:
        konv = _read_konvertatsii(ws_konv, date_disp)
        logger.info(f"[fill_report] Конвертации: {konv}")

    # Locate the report block
    block_start = _find_block_start(ws_report, date_disp)
    if not block_start:
        logger.warning(f"[fill_report] Block for {date_disp} not found in report sheet!")
        return f"Блок за {date_disp} не найден в листе «отчет по остаткам»."

    logger.info(f"[fill_report] Found block at row {block_start}")

    # Block layout offsets (0-indexed from block_start):
    # +0: Дата header
    # +1: Column headers (Рубли, USD, ...)
    # +2: Остаток Утро
    # +3: Входящие суммы (+)   ← income
    # +4: Снятия (-)           ← snatiya
    # +5: Комиссия за снятие (-) ← kom_snat
    # +6: Комиссия за пополнение (-) ← kom_pop
    # +7: Конвертации (приход) (+)  ← (leave as-is or 0 for now)
    # +8: Платежи (расход)(-) ← platezhi
    # +9: Swift комиссия(-)   ← swift
    # +10: Расчетный вечер   ← formula row
    # +11: Фактический вечер ← balances (written separately)
    # +12: Разница           ← formula row

    R_MORNING = block_start + 2
    R_INC = block_start + 3
    R_SNAT = block_start + 4
    R_KSNAT = block_start + 5
    R_KPOP = block_start + 6
    R_KONV = block_start + 7
    R_PAY = block_start + 8
    R_SWIFT = block_start + 9
    R_CALC = block_start + 10
    R_FACT = block_start + 11
    R_RAZN = block_start + 12

    updates = []

    for i, curr in enumerate(_COL_ORDER):
        col = chr(ord('B') + i)  # B, C, D, E, F

        # Входящие суммы
        updates.append({"range": f"{col}{R_INC}", "values": [[income.get(curr, 0.0)]]})
        # Снятия
        updates.append({"range": f"{col}{R_SNAT}", "values": [[snatiya.get(curr, 0.0)]]})
        # Комиссия за снятие
        updates.append({"range": f"{col}{R_KSNAT}", "values": [[round(kom_snat.get(curr, 0.0), 2)]]})
        # Комиссия за пополнение
        updates.append({"range": f"{col}{R_KPOP}", "values": [[round(kom_pop.get(curr, 0.0), 2)]]})
        # Конвертации (приход)
        updates.append({"range": f"{col}{R_KONV}", "values": [[round(konv.get(curr, 0.0), 2)]]})
        # Платежи расход
        updates.append({"range": f"{col}{R_PAY}", "values": [[platezhi.get(curr, 0.0)]]})
        # Swift комиссия
        updates.append({"range": f"{col}{R_SWIFT}", "values": [[round(swift.get(curr, 0.0), 2)]]})

    # Re-write Расчетный вечер formula: Утро + Вход - Снят - КомСн - КомПоп + Конв - Плат - Свифт
    for i in range(len(_COL_ORDER)):
        col = chr(ord('B') + i)
        f = (f"={col}{R_MORNING}"
             f"+{col}{R_INC}"
             f"-{col}{R_SNAT}"
             f"-{col}{R_KSNAT}"
             f"-{col}{R_KPOP}"
             f"+{col}{R_KONV}"
             f"-{col}{R_PAY}"
             f"-{col}{R_SWIFT}")
        updates.append({"range": f"{col}{R_CALC}", "values": [[f]]})

    # Разница = Факт - Расчет
    for i in range(len(_COL_ORDER)):
        col = chr(ord('B') + i)
        updates.append({"range": f"{col}{R_RAZN}", "values": [[f"={col}{R_FACT}-{col}{R_CALC}"]]})

    ws_report.batch_update(updates, value_input_option="USER_ENTERED")
    logger.info(f"[fill_report] Updated {len(updates)} cells for {date_disp}")

    return (f"✅ Отчёт за {date_disp} обновлён:\n"
            f"  Поступления: {income}\n"
            f"  Снятия: {snatiya}\n"
            f"  Платежи: {platezhi}\n"
            f"  Swift: {swift}")
