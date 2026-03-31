"""
app/services/edit_check.py

Scheduled job that runs at 23:00 (KG time) to:
1. Re-sync any late/edited payment lists from CONVERSION_GROUP → "Платежи" sheet
2. Re-sync any late/edited conversions from CONVERSION_GROUP → "Конвертации" sheet
3. Re-check ЗАПРОСЫ (REPORT_CHAT_ID) — push any pending operations missed by reconciler
4. Re-run fill_report_block for today → final "отчет по остаткам"

All sync operations are idempotent (swept by msg_id or date-keyed).
No Telegram messages are sent — this is a silent background job.
"""
import asyncio
import os
from datetime import datetime, date
from zoneinfo import ZoneInfo

from app.core.logger import logger
from app.core.config import CASSA_SPREADSHEET_ID

KG_TZ = ZoneInfo("Asia/Bishkek")
CREDENTIALS_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "n8n-google-credentials.json"
)


async def recheck_and_resync_all_chats(context=None):
    """
    PTB job callback (also callable directly without context).
    Runs at 23:00 KG to produce the final evening report.

    Steps:
      1. Re-parse & re-sync Платежи from saved back_report text (CONVERSION_GROUP)
      2. Re-parse & re-sync Конвертации from saved back_report text (CONVERSION_GROUP)
      3. Trigger the 15-min reconciler one more time (picks up pending ЗАПРОСЫ ops)
      4. Re-run fill_report_block → writes final values to "отчет по остаткам"
    """
    now = datetime.now(KG_TZ)
    today = now.date()
    logger.info(f"[EditCheck] ▶ Starting final 23:00 re-check for {today.strftime('%d.%m.%Y')}")

    # ── Step 1 & 2: Re-sync Платежи + Конвертации ─────────────────────────────
    await _resync_conversion_group(today)

    # ── Step 3: Run the pending-operations reconciler (ЗАПРОСЫ et al.) ─────────
    try:
        from app.services.reconciliation import reconcile_pending_operations
        await reconcile_pending_operations()
        logger.info("[EditCheck] ✅ Reconciler pass complete")
    except Exception as e:
        logger.error(f"[EditCheck] Reconciler pass failed: {e}")

    try:
        from app.services.zak_day_flush import flush_zak_buffers_for_report_date

        await flush_zak_buffers_for_report_date(today.strftime("%Y-%m-%d"))
        logger.info("[EditCheck] ZAK day buffer flush complete")
    except Exception as e:
        logger.error(f"[EditCheck] ZAK buffer flush failed: {e}")

    # ── Step 4: Re-run fill_report_block ──────────────────────────────────────
    await _refill_report(today)

    logger.info("[EditCheck] ✅ Re-sync complete for " + today.strftime("%d.%m.%Y"))


async def _resync_conversion_group(today: date):
    """
    Reads the last saved text for CONVERSION_GROUP from the DB,
    re-parses payments and conversions, re-syncs both sheets.
    The sync functions are idempotent (they sweep by msg_id before writing).
    """
    from app.db.instance import db
    from app.core.config import CONVERSION_GROUP_NAME

    # Locate the CONVERSION_GROUP chat_id
    conv_chat_id = db.get_chat_id_by_name(CONVERSION_GROUP_NAME)
    # Fallback to hardcoded ID if name lookup fails
    if not conv_chat_id:
        conv_chat_id = -4032081164

    saved_text = db.get_last_back_report_text(conv_chat_id)
    if not saved_text:
        logger.info("[EditCheck] No saved back_report text for CONVERSION_GROUP — skipping re-parse")
        return

    # ── Платежи ───────────────────────────────────────────────────────────────
    try:
        from app.services.parser import parse_back_report_payments
        from app.services.google_sheets import sync_payment_list_to_cassa_sheet

        parsed = parse_back_report_payments(saved_text)
        today_str = today.strftime("%d.%m.%Y")

        if parsed and parsed.get("items") and parsed.get("date") == today_str:
            logger.info(f"[EditCheck] Re-syncing {len(parsed['items'])} payment items for {today_str}")
            await sync_payment_list_to_cassa_sheet(parsed)
        else:
            logger.info(
                f"[EditCheck] Payment list date='{parsed.get('date', '?')}' vs today={today_str} "
                f"— items={len(parsed.get('items', []))}. Skipping Платежи re-sync."
            )
    except Exception as e:
        logger.error(f"[EditCheck] Платежи re-sync failed: {e}")

    # ── Конвертации ───────────────────────────────────────────────────────────
    try:
        from app.services.parser_conversions import parse_group_conversions
        from app.services.google_sheets import sync_conversions_to_cassa_sheet

        conversions = parse_group_conversions(saved_text)
        if conversions:
            logger.info(f"[EditCheck] Re-syncing {len(conversions)} conversions")
            await sync_conversions_to_cassa_sheet(conversions)
        else:
            logger.info("[EditCheck] No conversions found in saved text — skipping re-sync")
    except Exception as e:
        logger.error(f"[EditCheck] Конвертации re-sync failed: {e}")


async def _refill_report(today: date):
    """
    Re-runs fill_report_block for today, updating all formula rows in
    'отчет по остаткам' based on the latest data in Платежи/ЗАПРОСЫ/Проценты_детально.
    """
    def _do_fill():
        import gspread
        import sys
        import subprocess
        from app.services.google_sheets import _apply_time_patch
        from app.services.fill_report_from_sheets import fill_report_block

        # Ensure the report block exists for this date.
        # Evening uploads can happen before manual morning inputs.
        project_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
        write_script = os.path.join(project_dir, "scripts", "write_cash_report.py")
        date_for_script = today.strftime("%d.%m.%Y")
        subprocess.run(
            [sys.executable, write_script, date_for_script],
            cwd=project_dir,
            check=True,
        )

        cred_path = os.path.abspath(CREDENTIALS_FILE)
        if not os.path.exists(cred_path):
            raise FileNotFoundError(f"Credentials not found: {cred_path}")

        _apply_time_patch()
        gc = gspread.service_account(filename=cred_path)
        sh = gc.open_by_key(CASSA_SPREADSHEET_ID)
        return fill_report_block(sh, today)

    try:
        result = await asyncio.to_thread(_do_fill)
        logger.info(f"[EditCheck] fill_report_block → {result}")
    except Exception as e:
        logger.error(f"[EditCheck] fill_report_block failed: {e}")
