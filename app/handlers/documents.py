import json
import logging
import os
import datetime
from telegram import Update
from telegram.ext import ContextTypes

from app.db.instance import db
from app.services.balance_reconciliation import parse_balance_excel, process_evening_reconciliation
from app.core.logger import logger
from app.core.constants import KG_TZ
from app.handlers.utils import safe_reply

def _extract_excel_data_sync(tmp_path: str, caption: str, file_name: str, now_kg: datetime.datetime):
    import openpyxl
    import re
    
    # Загружаем книгу один раз
    wb = openpyxl.load_workbook(tmp_path, data_only=True)
    sheet_names = wb.sheetnames
    
    target_date_obj = now_kg.date()
    target_date_str = target_date_obj.strftime("%d.%m.%y")
    
    date_pattern = re.compile(r"(\d{2})\.(\d{2})\.(\d{2})")
    
    # Если юзер прямо написал дату в подписи
    caption_date_match = re.search(r"(\d{2})[\./](\d{2})[\./](\d{2,4})", caption)
    if caption_date_match:
        d, m, y = caption_date_match.groups()
        if len(y) == 4: y = y[2:]
        target_date_str = f"{d}.{m}.{y}"
        target_date_obj = datetime.datetime.strptime(f"{d}.{m}.20{y}", "%d.%m.%Y").date()
    else:
        # Ищем самую новую дату
        found_dates = []
        for sn in sheet_names:
            m_grp = date_pattern.search(sn)
            if m_grp:
                d, month, y = m_grp.groups()
                try:
                    dt = datetime.datetime.strptime(f"{d}.{month}.20{y}", "%d.%m.%Y").date()
                    found_dates.append((dt, sn))
                except ValueError:
                    pass
        
        if found_dates:
            found_dates.sort(key=lambda x: x[0], reverse=True)
            target_date_obj = found_dates[0][0]
            target_date_str = target_date_obj.strftime("%d.%m.%y")

    is_morning = False
    is_evening = False
    
    # Сначала смотрим на подпись от пользователя
    if "утро" in caption:
        is_morning = True
    elif "вечер" in caption:
        is_evening = True
    else:
        # Смотрим, какие вкладки ЕСТЬ ДЛЯ ВЫБРАННОЙ ДАТЫ
        has_evening_tab = any(f"{target_date_str}" in sn and "вечер" in sn.lower() for sn in sheet_names)
        has_morning_tab = any(f"{target_date_str}" in sn and "утро" in sn.lower() for sn in sheet_names)
        
        if has_evening_tab and has_morning_tab:
            is_evening = True
            is_morning = True
        elif has_evening_tab:
            is_evening = True
        elif has_morning_tab:
            is_morning = True
        else:
            # Если вкладок с датой нет, пробуем угадать по имени файла
            if "утро" in file_name:
                is_morning = True
            elif "вечер" in file_name:
                is_evening = True
            else:
                if now_kg.hour < 15:
                    is_morning = True
                else:
                    is_evening = True

    db_date_str = target_date_obj.strftime("%Y-%m-%d")
    
    totals_m = None
    totals_e = None
    morning_sheet_name = None
    evening_sheet_name = None
    
    if is_morning:
        morning_sheet_name = f"{target_date_str} утро"
        totals_m = parse_balance_excel(wb, target_sheet_name=morning_sheet_name)
        
    if is_evening:
        evening_sheet_name = f"{target_date_str} вечер"
        totals_e = parse_balance_excel(wb, target_sheet_name=evening_sheet_name)
        
    wb.close()
    
    return {
        "db_date_str": db_date_str,
        "target_date_obj": target_date_obj,
        "is_morning": is_morning,
        "is_evening": is_evening,
        "totals_m": totals_m,
        "totals_e": totals_e,
        "morning_sheet_name": morning_sheet_name,
        "evening_sheet_name": evening_sheet_name
    }

async def handle_uploaded_excel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Обработчик загрузки Excel (.xlsx) файлов.
    """
    message = update.message or update.edited_message
    if not message or not message.document:
        return
        
    chat = message.chat
    chat_title = chat.title or ""
    
    # Мы обрабатываем файлы в группе "Остатки !" или в личных сообщениях (для тестов)
    if chat.type != 'private' and "остатки" not in chat_title.lower():
        # Не этот чат -> игнорим
        logger.info(f"Игнорируем документ в чате {chat_title} (type: {chat.type})")
        return

    doc = message.document
    if not doc.file_name or not doc.file_name.lower().endswith(".xlsx"):
        logger.info(f"Документ {doc.file_name} не является .xlsx")
        return

    # Скачиваем файл
    file_id = doc.file_id
    new_file = await context.bot.get_file(file_id)
    
    # Сохраняем во временную директорию
    tmp_path = f"/tmp/{file_id}_{doc.file_name}"
    await new_file.download_to_drive(custom_path=tmp_path)
    
    try:
        caption = (message.caption or "").lower()
        file_name = doc.file_name.lower()
        now_kg = datetime.datetime.now(KG_TZ)
        
        import asyncio
        data = await asyncio.to_thread(_extract_excel_data_sync, tmp_path, caption, file_name, now_kg)
        
        db_date_str = data["db_date_str"]
        target_date_obj = data["target_date_obj"]
        is_morning = data["is_morning"]
        is_evening = data["is_evening"]
        
        if is_morning and data["totals_m"]:
            totals_json = json.dumps(data["totals_m"], ensure_ascii=False)
            db.save_daily_balance(db_date_str, morning_data=totals_json)
            # Update "Остаток Утро" row in отчет по остаткам
            try:
                from app.handlers.balance_input import _update_balance_in_sheet
                await asyncio.to_thread(_update_balance_in_sheet, "morning", data["totals_m"], target_date_obj)
                logger.info(f"[Excel] Сводки morning updated for {target_date_obj}")
            except Exception as upd_err:
                logger.warning(f"[Excel] Сводки morning update failed: {upd_err}")
            if message.chat.type == "private":
                lines = "\n".join(f"  • {k}: {v:,.2f}" for k, v in (data["totals_m"] or {}).items() if v)
                await safe_reply(message,
                    f"🌅 Утренние остатки за {target_date_obj.strftime('%d.%m.%Y')} сохранены из вкладки '{data['morning_sheet_name']}':\n{lines}\n\nОбновлено в отчёте по остаткам.")

        if is_evening and data["totals_e"]:
            totals_json = json.dumps(data["totals_e"], ensure_ascii=False)
            db.save_daily_balance(db_date_str, evening_data=totals_json)
            # Update "Фактический вечер" row in отчет по остаткам
            try:
                from app.handlers.balance_input import _update_balance_in_sheet
                await asyncio.to_thread(_update_balance_in_sheet, "evening", data["totals_e"], target_date_obj)
                logger.info(f"[Excel] Сводки evening updated for {target_date_obj}")
            except Exception as upd_err:
                logger.warning(f"[Excel] Сводки evening update failed: {upd_err}")
            if message.chat.type == "private":
                lines = "\n".join(f"  • {k}: {v:,.2f}" for k, v in (data["totals_e"] or {}).items() if v)
                await safe_reply(message,
                    f"🌇 Вечерние остатки за {target_date_obj.strftime('%d.%m.%Y')} приняты из вкладки '{data['evening_sheet_name']}':\n{lines}\n\nЗапускаю сверку...")

            # ── Step 1: Flush any pending ЗАПРОСЫ/ZAK ops into sheets ────────────────────
            # This must run BEFORE fill_report_block so all income is visible in the sheet.
            try:
                from app.services.reconciliation import reconcile_pending_operations
                await reconcile_pending_operations()
                logger.info("[Excel] Pre-fill reconciler pass complete")
            except Exception as e:
                logger.error(f"[Excel] Pre-fill reconciler failed (continuing anyway): {e}")

            try:
                from app.services.zak_day_flush import flush_zak_buffers_for_report_date
                await flush_zak_buffers_for_report_date(db_date_str)
                logger.info(f"[Excel] ZAK day buffer flush for {db_date_str}")
            except Exception as zfe:
                logger.error(f"[Excel] ZAK buffer flush failed (continuing anyway): {zfe}")

            # ── Step 2: Auto-fill the report block from source sheets ────────────────────
            # Reads ЗАПРОСЫ, Проценты_детально, Платежи and writes into «отчет по остаткам»
            try:
                # Ensure the report block exists for this date.
                # Evening uploads can happen before any manual morning inputs.
                import sys, subprocess
                project_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
                write_script = os.path.join(project_dir, "scripts", "write_cash_report.py")
                date_for_script = target_date_obj.strftime("%d.%m.%Y")
                try:
                    await asyncio.to_thread(
                        subprocess.run,
                        [sys.executable, write_script, date_for_script],
                        {"cwd": project_dir, "check": True},
                    )
                except Exception as ensure_err:
                    logger.warning(f"[Excel] Failed to ensure report block via write_cash_report: {ensure_err}")

                import gspread
                from app.services.google_sheets import _apply_time_patch
                from app.services.fill_report_from_sheets import fill_report_block
                from app.core.config import CASSA_SPREADSHEET_ID

                def _fill_job():
                    _apply_time_patch()
                    credentials_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "n8n-google-credentials.json")
                    credentials_file = os.path.abspath(credentials_file)
                    gc = gspread.service_account(filename=credentials_file)
                    sh = gc.open_by_key(CASSA_SPREADSHEET_ID)
                    return fill_report_block(sh, target_date_obj)

                fill_result = await asyncio.to_thread(_fill_job)
                logger.info(f"[Excel] Draft report filled: {fill_result}")
                if message.chat.type == "private":
                    await safe_reply(message, f"📊 Черновой отчёт:\n{fill_result}")
            except Exception as fe:
                logger.error(f"[Excel] fill_report_block error: {fe}")
                if message.chat.type == "private":
                    await safe_reply(message, f"⚠️ Черновой отчёт заполнен частично: {fe}")



                
    except Exception as e:
        logger.error(f"Error handling Excel upload: {e}")
        if message.chat.type == "private":
            await safe_reply(message, f"❌ Ошибка при чтении Excel файла: {e}")
        
    finally:
        # Очистка
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except:
                pass
