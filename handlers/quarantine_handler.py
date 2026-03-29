import json
from datetime import datetime, timezone
from warehouse.connection import get_cursor, execute_values
import logging
logger = logging.getLogger(__name__)


def send_to_quarantine(
    conn,
    source_file,
    entity_type,
    raw_record,
    error_type,
    error_details,
    orphan_type=None,
    raw_orphan_id=None,
    pipeline_run_id=None
):

    sql = """INSERT INTO fastfeast.quarantine (source_file, entity_type, raw_record, error_type, error_details,
                                               orphan_type, raw_orphan_id, pipeline_run_id, quarantined_at)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        

    raw_record_json = json.dumps(raw_record, default=str) #Convert the raw_record dict → JSON string
    now = datetime.now(timezone.utc)

    safe_orphan_id = str(raw_orphan_id) if raw_orphan_id is not None else None


    try:
        with get_cursor() as cur:
            cur.execute(sql, (
                source_file,
                entity_type,
                raw_record_json,
                error_type,
                error_details,
                orphan_type,
                safe_orphan_id,
                pipeline_run_id,
                now
            ))
 
        logger.debug(
            f"[QUARANTINE] 1 record saved | "
            f"entity={entity_type} | error_type={error_type} | file={source_file}"
        )
        return True

    except Exception as e:
        # get_cursor() already rolled back when the exception propagated out.
        # We catch it here only to log it and return False instead of crashing.
        logger.error(
            f"[QUARANTINE] Failed to save record: {e} | "
            f"entity={entity_type} | file={source_file}"
        )
        return False
    
def send_batch_to_quarantine(conn, failed_records):

    sql = """INSERT INTO fastfeast.quarantine (source_file, entity_type, raw_record, error_type, error_details,
                                               orphan_type, raw_orphan_id, pipeline_run_id, quarantined_at)
             VALUES %s"""
    now = datetime.now(timezone.utc)

    rows=[]

    for rec in failed_records:
        raw_orphan_id = rec.get("raw_orphan_id")
        rows.append((
            rec.get("source_file",    "unknown"),
            rec.get("entity_type",    "unknown"),
            json.dumps(rec.get("raw_record", {}), default=str),
            rec.get("error_type",     "unknown"),
            rec.get("error_details",  ""),
            rec.get("orphan_type",    None),
            str(raw_orphan_id) if raw_orphan_id is not None else None,
            rec.get("pipeline_run_id", None),
            now
            # All records share the same timestamp 
        ))
        
    try:
    
        count = execute_values(sql, rows)  # execute_values() from connection.py → sends ONE multi-row INSERT

        logger.info(f"[QUARANTINE] Batch complete — {count} records quarantined.")
        return count

    except Exception as e:
        # We catch here to run the fallback instead of crashing.
        logger.error(f"[QUARANTINE] Batch insert failed: {e}. Starting fallback...")

        # ---> ONE-BY-ONE FALLBACK <---
        # execute_values() is all-or-nothing [atomicity]: one bad row fails the entire batch.
        # The fallback calls send_to_quarantine() per record so each record has its own commit & rollback

        # Without fallback: 0 / 50 saved.
        # With fallback:   49 / 50 saved. 
        saved = 0
        for rec in failed_records:
            success = send_to_quarantine(
                conn,
                source_file=rec.get("source_file",    "unknown"),
                entity_type=rec.get("entity_type",    "unknown"),
                raw_record=rec.get("raw_record",       {}),
                error_type=rec.get("error_type",       "unknown"),
                error_details=rec.get("error_details", ""),
                orphan_type=rec.get("orphan_type"),
                raw_orphan_id=rec.get("raw_orphan_id"),
                pipeline_run_id=rec.get("pipeline_run_id")
            )
            if success:
                saved += 1
 
        logger.info(
            f"[QUARANTINE] Fallback complete — {saved}/{len(failed_records)} records saved."
        )
        return saved
