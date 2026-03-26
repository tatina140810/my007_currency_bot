import json
import logging
import datetime
from decimal import Decimal
import gspread
import openpyxl

from app.db.instance import db
from app.services.google_sheets import _get_gc, _execute_with_retry, _apply_time_patch
from app.core.config import CASSA_SPREADSHEET_ID

logger = logging.getLogger(__name__)

CURRENCIES_REPORT = ["Рубли", "USD", "Евро", "CNY", "тенге"]

def parse_balance_excel(file_path_or_wb, target_sheet_name: str = None) -> dict:
    """
    Парсит загруженный файл Остатки (Excel) или открытый workbook, и возвращает totals по валютам.
    Ожидаемые колонки: Рубли (B), USD (C), Евро (D), CNY (E), тенге/дирхам (F).
    """
    if isinstance(file_path_or_wb, str):
        wb = openpyxl.load_workbook(file_path_or_wb, data_only=True)
    else:
        wb = file_path_or_wb
        
    sheet = None
    if target_sheet_name:
        # Пробуем найти лист по точному или частичному совпадению без учета регистра
        for sname in wb.sheetnames:
            if target_sheet_name.lower() in sname.lower():
                sheet = wb[sname]
                logger.info(f"Нашли лист {sname} по запросу {target_sheet_name}")
                break
                
    if not sheet:
        sheet = wb.active
        logger.info(f"Используем активный лист {sheet.title}")
    
    totals = {
        "Рубли": 0.0,
        "USD": 0.0,
        "Евро": 0.0,
        "CNY": 0.0,
        "тенге": 0.0
    }
    
    # Ищем строку с ИТОГО (в колонке A или B)
    max_row = sheet.max_row
    
    # Оптимизация: не перебирать миллион строк, если лист пустой
    if max_row > 5000:
        logger.warning(f"Лист {sheet.title} имеет {max_row} строк, ограничиваем поиск.")
        max_row = min(max_row, 5000)
    
    found_row = None
    for r in range(max_row, 0, -1):
        cell_a = sheet.cell(row=r, column=1).value
        cell_b = sheet.cell(row=r, column=2).value
        text_a = str(cell_a).lower() if cell_a else ""
        text_b = str(cell_b).lower() if cell_b else ""
        
        if "итого" in text_a or "итого" in text_b:
            found_row = r
            break

    if not found_row:
        # Если не нашли слово ИТОГО, берем самую нижнюю непустую строку, где есть цифры
        for r in range(max_row, 0, -1):
            if isinstance(sheet.cell(row=r, column=3).value, (int, float)):
                found_row = r
                break

    if found_row:
        cols_mapping = {
            2: "Рубли",
            3: "USD",
            4: "Евро",
            5: "CNY",
            6: "тенге" # Может быть дирхам, но по задаче пишем "тенге" для колонки F
        }
        for col_idx, curr_name in cols_mapping.items():
            val = sheet.cell(row=found_row, column=col_idx).value
            try:
                totals[curr_name] = float(val) if val is not None else 0.0
            except (ValueError, TypeError):
                totals[curr_name] = 0.0
                
    return totals

def _parse_date(date_str: str) -> datetime.date:
    try:
        return datetime.datetime.strptime(date_str, "%d.%m.%Y").date()
    except:
        return None

def fetch_daily_sums(target_date: datetime.date) -> dict:
    """
    Собирает суммы из листов:
    - ЗАПРОСЫ ПО ВХОД.СУММАМ И ДОКИ (приходы)
    - Платежи (расходы)
    - конветации (приходы/расходы)
    """
    _apply_time_patch()
    gc = _get_gc()
    sh = gc.open_by_key(CASSA_SPREADSHEET_ID)
    
    results = {
        "inputs": {"Рубли": 0.0, "USD": 0.0, "Евро": 0.0, "CNY": 0.0, "тенге": 0.0},
        "payments": {"Рубли": 0.0, "USD": 0.0, "Евро": 0.0, "CNY": 0.0, "тенге": 0.0},
        "swift_commissions": {"Рубли": 0.0, "USD": 0.0, "Евро": 0.0, "CNY": 0.0, "тенге": 0.0},
        "conv_income": {"Рубли": 0.0, "USD": 0.0, "Евро": 0.0, "CNY": 0.0, "тенге": 0.0},
        "conv_expense": {"Рубли": 0.0, "USD": 0.0, "Евро": 0.0, "CNY": 0.0, "тенге": 0.0},
        "zak_commissions": {"Рубли": 0.0, "USD": 0.0, "Евро": 0.0, "CNY": 0.0, "тенге": 0.0}
    }
    
    # 1. ЗАПРОСЫ ПО ВХОД.СУММАМ И ДОКИ
    try:
        ws_in = sh.worksheet("ЗАПРОСЫ ПО ВХОД.СУММАМ И ДОКИ")
        records = ws_in.get_all_values()
        for r in records[1:]:
            if len(r) >= 5:
                # Ожидается Дата (0 или 1), Валюта, Сумма. Пусть Дата в 0, Сумма в 3, Валюта в 4 (надо уточнить индексы)
                # Assuming generic scan for the date in the row
                row_str = " ".join(r)
                if target_date.strftime("%d.%m.%Y") in row_str:
                    # Попробуем найти сумму и валюту простым способом
                    # Обычно: Дата, Клиент, Банк, Сумма, Валюта
                    # Ищем подходящую валюту
                    val_curr = "Рубли"
                    val_sum = 0.0
                    for cell in r:
                        c_low = cell.lower()
                        if "usd" in c_low or "доллар" in c_low: val_curr = "USD"
                        elif "eur" in c_low or "евро" in c_low: val_curr = "Евро"
                        elif "cny" in c_low or "юань" in c_low: val_curr = "CNY"
                        elif "тенге" in c_low or "kzt" in c_low: val_curr = "тенге"
                        elif "руб" in c_low: val_curr = "Рубли"
                        
                        try:
                            # Пытаемся распарсить числа
                            clean_str = cell.replace(" ", "").replace(",", ".")
                            if clean_str.replace(".", "", 1).isdigit():
                                possible_sum = float(clean_str)
                                if possible_sum > val_sum: 
                                    val_sum = possible_sum
                        except:
                            pass
                    
                    results["inputs"][val_curr] += val_sum
    except Exception as e:
        logger.error(f"Error parsing ЗАПРОСЫ: {e}")

    # 2. Платежи
    try:
        ws_pay = sh.worksheet("Платежи")
        records = ws_pay.get_all_values()
        # Ожидаемый заголовки: "Отчет Back", "Компания", "Тип", "Контрагент", "Валюта платежа", "Сумма"
        in_target_date = False
        target_marker = f"--- ПЛАТЕЖИ ЗА {target_date.strftime('%d.%m.%Y')}"
        
        for r in records:
            row_str = " ".join(r)
            if target_marker in row_str:
                in_target_date = True
                continue
            if in_target_date and "--- ПЛАТЕЖИ ЗА" in row_str:
                in_target_date = False # Начался другой день
                
            if in_target_date and len(r) >= 6:
                curr_raw = r[4].lower()
                amount_raw = r[5].replace(" ", "").replace(",", ".")
                
                try:
                    swift_com = float(str(r[6]).replace(" ", "").replace(",", ".")) if len(r) >= 7 and str(r[6]).strip() else 0.0
                except:
                    swift_com = 0.0
                    
                try:
                    amt = float(amount_raw)
                    if "usd" in curr_raw or "доллар" in curr_raw: 
                        results["payments"]["USD"] += amt
                        results["swift_commissions"]["USD"] += swift_com
                    elif "eur" in curr_raw or "евро" in curr_raw: 
                        results["payments"]["Евро"] += amt
                        results["swift_commissions"]["Евро"] += swift_com
                    elif "cny" in curr_raw or "юань" in curr_raw: 
                        results["payments"]["CNY"] += amt
                        results["swift_commissions"]["CNY"] += swift_com
                    elif "тенге" in curr_raw or "kzt" in curr_raw: 
                        results["payments"]["тенге"] += amt
                        results["swift_commissions"]["тенге"] += swift_com
                    else: 
                        results["payments"]["Рубли"] += amt
                        results["swift_commissions"]["Рубли"] += swift_com
                except:
                    pass
    except Exception as e:
        logger.error(f"Error parsing Платежи: {e}")

    # 3. Конвертации
    try:
        ws_conv = sh.worksheet("конвертации")
        records = ws_conv.get_all_values()
        # Колонки: Дата(0), Клиент(1), Сумма(2), Валюта(3), Курс(4), Сумма РУБ(5)
        for r in records[1:]:
            if len(r) >= 6 and target_date.strftime("%d.%m.%Y") in r[0]:
                curr_raw = r[3].lower()
                amount_raw = r[2].replace(" ", "").replace(",", ".")
                rub_raw = r[5].replace(" ", "").replace(",", ".")
                try:
                    amt = float(amount_raw)
                    rub_amt = float(rub_raw)
                    
                    val_curr = None
                    if "usd" in curr_raw or "доллар" in curr_raw: val_curr = "USD"
                    elif "eur" in curr_raw or "евро" in curr_raw: val_curr = "Евро"
                    elif "cny" in curr_raw or "юань" in curr_raw: val_curr = "CNY"
                    elif "тенге" in curr_raw or "kzt" in curr_raw: val_curr = "тенге"
                    
                    if val_curr:
                        # Покупка инвалюты -> мы потратили рубли, получили инвалюту
                        results["conv_income"][val_curr] += amt
                        results["conv_expense"]["Рубли"] += rub_amt
                except:
                    pass
    except Exception as e:
        logger.error(f"Error parsing конветации: {e}")

    # 4. Процент снятия и пополнения (ZAK)
    try:
        ws_zak = sh.worksheet("Проценты_детально")
        records = ws_zak.get_all_values()
        for r in records[1:]:
            if len(r) >= 9 and target_date.strftime("%d.%m.%Y") in str(r[0]):
                curr_raw = str(r[4]).lower()
                fee_raw = str(r[8]).replace(" ", "").replace(",", ".")
                try:
                    fee = float(fee_raw)
                    if "usd" in curr_raw or "доллар" in curr_raw: results["zak_commissions"]["USD"] += fee
                    elif "eur" in curr_raw or "евро" in curr_raw: results["zak_commissions"]["Евро"] += fee
                    elif "cny" in curr_raw or "юань" in curr_raw: results["zak_commissions"]["CNY"] += fee
                    elif "тенге" in curr_raw or "kzt" in curr_raw: results["zak_commissions"]["тенге"] += fee
                    else: results["zak_commissions"]["Рубли"] += fee
                except:
                    pass
    except Exception as e:
        logger.error(f"Error parsing Проценты_детально: {e}")

    return results

def process_evening_reconciliation(date_str: str) -> str:
    """
    Основная логика вечерней сверки.
    """
    db_rec = db.get_daily_balance(date_str)
    if not db_rec or not db_rec.get("morning_data") or not db_rec.get("evening_data"):
        return "Не хватает данных (Утро или Вечер) для расчетов."
        
    morning = json.loads(db_rec["morning_data"])
    evening = json.loads(db_rec["evening_data"])
    
    target_date = datetime.datetime.strptime(date_str, "%Y-%m-%d").date()
    
    # Собираем суммы
    def fetch_job():
        return fetch_daily_sums(target_date)
        
    sums = _execute_with_retry(fetch_job)
    
    # Формируем запись в Google Sheet
    def write_job():
        _apply_time_patch()
        gc = _get_gc()
        sh = gc.open_by_key(CASSA_SPREADSHEET_ID)
        sheet_name = "отчет по остаткам"
        try:
            ws = sh.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=sheet_name, rows=1000, cols=8)
            
        # Прагматично находим последнюю строку для отступа
        existing_values = ws.col_values(1)
        last_row = 0
        for i, val in enumerate(existing_values):
            if str(val).strip():
                last_row = i + 1
                
        # Стартовая строка для нового блока
        start_row = last_row + 1 
        if last_row > 1 and str(existing_values[-1]).strip() != "":
            start_row += 1

        td_str = target_date.strftime('%d.%m.%Y')
        zap_f = f'=IFERROR(INDEX(\'ЗАПРОСЫ ПО ВХОД.СУММАМ И ДОКИ\'!E:E; MAX(FILTER(ROW(\'ЗАПРОСЫ ПО ВХОД.СУММАМ И ДОКИ\'!A:A); LEFT(\'ЗАПРОСЫ ПО ВХОД.СУММАМ И ДОКИ\'!A:A; 10)="{td_str}"))); 0)'
        
        row_3 = start_row + 2
        row_last_calc = start_row + 8
        r_calc = start_row + 9
        r_fact = start_row + 10
        # Native dynamic formulas for Conversions mapping - Do not use text wildcards (*) on Date columns!
        conv_rub_exp = f'=-SUMIFS(\'конвертации\'!F:F; \'конвертации\'!A:A; "{td_str}")'
        conv_usd_inc = f'=SUMIFS(\'конвертации\'!C:C; \'конвертации\'!A:A; "{td_str}"; \'конвертации\'!D:D; "*usd*") + SUMIFS(\'конвертации\'!C:C; \'конвертации\'!A:A; "{td_str}"; \'конвертации\'!D:D; "*доллар*")'
        conv_eur_inc = f'=SUMIFS(\'конвертации\'!C:C; \'конвертации\'!A:A; "{td_str}"; \'конвертации\'!D:D; "*eur*") + SUMIFS(\'конвертации\'!C:C; \'конвертации\'!A:A; "{td_str}"; \'конвертации\'!D:D; "*евро*")'
        conv_cny_inc = f'=SUMIFS(\'конвертации\'!C:C; \'конвертации\'!A:A; "{td_str}"; \'конвертации\'!D:D; "*cny*") + SUMIFS(\'конвертации\'!C:C; \'конвертации\'!A:A; "{td_str}"; \'конвертации\'!D:D; "*юань*")'
        conv_tenge_inc = f'=SUMIFS(\'конвертации\'!C:C; \'конвертации\'!A:A; "{td_str}"; \'конвертации\'!D:D; "*kzt*") + SUMIFS(\'конвертации\'!C:C; \'конвертации\'!A:A; "{td_str}"; \'конвертации\'!D:D; "*тенге*")'

        block = [
            [f"Дата: {td_str}", "", "", "", "", ""],
            ["", "Рубли", "USD", "Евро", "CNY", "тенге"],
            ["Остаток Утро", morning.get("Рубли",0), morning.get("USD",0), morning.get("Евро",0), morning.get("CNY",0), morning.get("тенге",0)],
            ["Входящие суммы", zap_f, sums["inputs"].get("USD",0), sums["inputs"].get("Евро",0), sums["inputs"].get("CNY",0), sums["inputs"].get("тенге",0)],
            ["Конвертации (расход)", conv_rub_exp, 0, 0, 0, 0],
            ["Конвертации (приход)", 0, conv_usd_inc, conv_eur_inc, conv_cny_inc, conv_tenge_inc],
            ["Платежи (расход)", -sums["payments"].get("Рубли",0), -sums["payments"].get("USD",0), -sums["payments"].get("Евро",0), -sums["payments"].get("CNY",0), -sums["payments"].get("тенге",0)],
            ["Swift комиссия", -sums["swift_commissions"].get("Рубли",0), -sums["swift_commissions"].get("USD",0), -sums["swift_commissions"].get("Евро",0), -sums["swift_commissions"].get("CNY",0), -sums["swift_commissions"].get("тенге",0)],
            ["Процент снятия и пополнения", -sums["zak_commissions"].get("Рубли",0), -sums["zak_commissions"].get("USD",0), -sums["zak_commissions"].get("Евро",0), -sums["zak_commissions"].get("CNY",0), -sums["zak_commissions"].get("тенге",0)],
            ["Расчетный вечер", f"=SUM(B{row_3}:B{row_last_calc})", f"=SUM(C{row_3}:C{row_last_calc})", f"=SUM(D{row_3}:D{row_last_calc})", f"=SUM(E{row_3}:E{row_last_calc})", f"=SUM(F{row_3}:F{row_last_calc})"],
            ["Фактический вечер", evening.get("Рубли",0), evening.get("USD",0), evening.get("Евро",0), evening.get("CNY",0), evening.get("тенге",0)],
            ["Разница", f"=B{r_calc}-B{r_fact}", f"=C{r_calc}-C{r_fact}", f"=D{r_calc}-D{r_fact}", f"=E{r_calc}-E{r_fact}", f"=F{r_calc}-F{r_fact}"],
            ["", "", "", "", "", ""]
        ]

        try:
            ws.update(range_name=f"A{start_row}", values=block, value_input_option="USER_ENTERED")
        except TypeError:
            ws.update(f"A{start_row}", block, value_input_option="USER_ENTERED")
            
        updated_values = ws.col_values(1)
        requests = []
        for i, val in enumerate(updated_values):
            if "Дата:" in str(val):
                requests.append({
                    "repeatCell": {
                        "range": {
                            "sheetId": ws.id, "startRowIndex": i, "endRowIndex": i + 1, "startColumnIndex": 0, "endColumnIndex": 6
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {"red": 1.0, "green": 1.0, "blue": 0.8},
                                "textFormat": {"bold": True}
                            }
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat)"
                    }
                })
        
        if requests:
            try:
                sh.batch_update({"requests": requests})
            except Exception as e:
                logger.error(f"Failed to apply highlight formatting: {e}")
                
    _execute_with_retry(write_job)
    db.save_daily_balance(date_str, processed=True)
    return "Сверка загружена в таблицу."
