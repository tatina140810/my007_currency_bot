import asyncio
import json
from collections import defaultdict
from datetime import datetime
from app.db.instance import db
from app.core.logger import logger
from app.services.google_sheets import _get_gc, _get_gsheets_lock, _apply_time_patch
from app.core.config import CASSA_SPREADSHEET_ID

def _fetch_sheet_msg_ids(ws_name: str, col_index: int) -> set:
    """Synchronously fetches the column of MSG_IDs from a target worksheet."""
    _apply_time_patch()
    gc = _get_gc()
    sh = gc.open_by_key(CASSA_SPREADSHEET_ID)
    try:
        ws = sh.worksheet(ws_name)
        col_values = ws.col_values(col_index)
        # Skip header, try to parse to int
        msg_ids = set()
        for v in col_values[1:]:
            if not v: continue
            try:
                msg_ids.add(int(v.strip()))
            except ValueError:
                pass
        return msg_ids
    except Exception as e:
        logger.error(f"[Reconciliation] Error fetching msg ids from {ws_name}: {e}")
        return set()

async def reconcile_pending_operations(context=None):
    """
    Background worker that runs every 15 minutes.
    It fetches all PENDING operations older than 1 minute, checks if their message_id
    already explicitly exists in the target Google Sheet logic columns, and triggers
    backfilling push routines if genuinely missing.
    """
    pending = db.get_pending_operations()
    if not pending:
        return
        
    logger.info(f"[Recon] Found {len(pending)} pending unsynced operations. Beginning verification loop...")
    
    # 1. Group by group_type
    by_type = defaultdict(list)
    for op in pending:
        by_type[op['group_type']].append(op)
        
    # 2. Reconcile Conversions (MSG_ID Col H -> Index 8)
    if "conversions" in by_type:
        async with _get_gsheets_lock():
            msg_ids = await asyncio.to_thread(_fetch_sheet_msg_ids, "Конвертации", 8)
        for op in by_type["conversions"]:
            if op['message_id'] in msg_ids:
                db.mark_operation_synced(op['id'])
                logger.info(f"[Recon] DB_ID:{op['id']} (conversions) marked SYNCED. Already present in sheet.")
            else:
                from app.services.google_sheets import sync_conversions_to_cassa_sheet
                payload = json.loads(op['payload_json'])
                logger.warning(f"[Recon] DB_ID:{op['id']} (conversions) verified MISSING. Pushing fallback payload.")
                await sync_conversions_to_cassa_sheet(payload, db_id=op['id'])
                
    # 3. Reconcile Payments (MSG_ID Col H -> Index 8)
    if "payments" in by_type:
        async with _get_gsheets_lock():
            msg_ids = await asyncio.to_thread(_fetch_sheet_msg_ids, "Платежи", 8)
        for op in by_type["payments"]:
            if op['message_id'] in msg_ids:
                db.mark_operation_synced(op['id'])
                logger.info(f"[Recon] DB_ID:{op['id']} (payments) marked SYNCED. Already present in sheet.")
            else:
                from app.services.google_sheets import sync_payment_list_to_cassa_sheet
                payload = json.loads(op['payload_json'])
                logger.warning(f"[Recon] DB_ID:{op['id']} (payments) verified MISSING. Pushing fallback payload.")
                await sync_payment_list_to_cassa_sheet(payload, db_id=op['id'])
                
    # 4. Reconcile Zak (MSG_ID Col O -> Index 15)
    if "zak" in by_type:
        async with _get_gsheets_lock():
            msg_ids = await asyncio.to_thread(_fetch_sheet_msg_ids, "Проценты_детально", 15)
        for op in by_type["zak"]:
            if op['message_id'] in msg_ids:
                db.mark_operation_synced(op['id'])
                logger.info(f"[Recon] DB_ID:{op['id']} (zak) marked SYNCED. Already present in sheet.")
            else:
                from app.services.google_sheets_zak import append_zak_operations_to_sheet
                payload = json.loads(op['payload_json'])
                for item in payload:
                    if 'date' in item and isinstance(item['date'], str):
                        try:
                            item['date'] = datetime.fromisoformat(item['date'])
                        except Exception: pass
                logger.warning(f"[Recon] DB_ID:{op['id']} (zak) verified MISSING. Pushing fallback payload.")
                await append_zak_operations_to_sheet(payload, db_id=op['id'])

    # 5. Reconcile Zaprosy (MSG_ID Col I -> Index 9)
    if "zaprosy" in by_type:
        # Note: Zaprosy handles 2 target sheets potentially due to date overlaps, but primarily writes to "ЗАПРОСЫ ПО ВХОД.СУММАМ И ДОКИ"
        async with _get_gsheets_lock():
            msg_ids = await asyncio.to_thread(_fetch_sheet_msg_ids, "ЗАПРОСЫ ПО ВХОД.СУММАМ И ДОКИ", 9)
        for op in by_type["zaprosy"]:
            if op['message_id'] in msg_ids:
                db.mark_operation_synced(op['id'])
                logger.info(f"[Recon] DB_ID:{op['id']} (zaprosy) marked SYNCED. Already present in sheet.")
            else:
                from app.services.google_sheets_zaprosy import sync_zaprosy_to_sheet
                payload = json.loads(op['payload_json'])
                logger.warning(f"[Recon] DB_ID:{op['id']} (zaprosy) verified MISSING. Pushing fallback payload.")
                await sync_zaprosy_to_sheet(payload, message_id=op['message_id'], db_id=op['id'])
