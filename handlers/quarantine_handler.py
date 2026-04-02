import json
import logging
import math
import psycopg2.extras

import pandas as pd
import numpy as np

from warehouse.connection import get_cursor
from utils.retry import db_retry
from utils.logger import get_logger_name

logger = get_logger_name(__name__)


def _sanitize_record(record: dict) -> dict:
    """
    Sanitize a record dict for JSON serialization.
    Converts pandas NaN, numpy NaN, NaT, and Inf to None.
    """
    sanitized = {}
    for key, value in record.items():
        if value is None:
            sanitized[key] = None
        elif isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                sanitized[key] = None
            else:
                sanitized[key] = value
        elif isinstance(value, np.floating):
            if np.isnan(value) or np.isinf(value):
                sanitized[key] = None
            else:
                sanitized[key] = float(value)
        elif isinstance(value, np.integer):
            sanitized[key] = int(value)
        elif isinstance(value, np.ndarray):
            sanitized[key] = value.tolist()
        elif pd.isna(value):  # Handles pandas NaT, NaN, etc.
            sanitized[key] = None
        else:
            sanitized[key] = value
    return sanitized


def _json_default(obj):
    """Handle NaN and other non-serializable values."""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
    if isinstance(obj, np.floating):
        if np.isnan(obj) or np.isinf(obj):
            return None
        return float(obj)
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if pd.isna(obj):
        return None
    return str(obj)


@db_retry
def send_to_quarantine(
    source_file: str,
    entity_type: str,
    raw_record: dict,
    error_type: str,
    error_details: str,
    orphan_type: str = None,
    raw_orphan_id: str = None,
    pipeline_run_id: int = None,
) -> bool:
    """
    Insert a single record into pipeline_audit.quarantine.
    Returns True on success, False on failure.
    """
    sql = """
        INSERT INTO pipeline_audit.quarantine
            (source_file, entity_type, raw_record, error_type, error_details,
             orphan_type, raw_orphan_id, pipeline_run_id)
        VALUES (%s, %s, %s::jsonb, %s, %s, %s, %s, %s)
    """

    safe_orphan_id = str(raw_orphan_id) if raw_orphan_id is not None else None

    # Sanitize record to handle NaN values
    sanitized_record = _sanitize_record(raw_record)

    try:
        with get_cursor() as cur:
            cur.execute(sql, (
                source_file,
                entity_type,
                json.dumps(sanitized_record, default=_json_default),
                error_type,
                error_details,
                orphan_type,
                safe_orphan_id,
                pipeline_run_id,
            ))

        logger.debug(
            "quarantine_record_saved",
            entity=entity_type,
            error_type=error_type,
            file=source_file,
        )
        return True

    except Exception as e:
        # get_cursor() already rolled back when the exception propagated out.
        # We catch it here only to log it and return False instead of crashing.
        logger.error(
            "quarantine_record_failed",
            entity=entity_type,
            file=source_file,
            error=str(e),
        )
        return False


@db_retry
def send_batch_to_quarantine(failed_records: list[dict]) -> int:
    """
    Bulk insert records into pipeline_audit.quarantine.
    Falls back to one-by-one on batch failure so partial saves are preserved.
    Returns count of successfully saved records.
    """
    if not failed_records:
        return 0

    sql = """
        INSERT INTO pipeline_audit.quarantine
            (source_file, entity_type, raw_record, error_type, error_details,
             orphan_type, raw_orphan_id, pipeline_run_id)
        VALUES %s
    """

    rows = []
    for rec in failed_records:
        raw_orphan_id = rec.get("raw_orphan_id")
        raw_record = rec.get("raw_record", {})
        # Sanitize record to handle NaN values
        sanitized_record = _sanitize_record(raw_record)
        rows.append((
            rec.get("source_file",    "unknown"),
            rec.get("entity_type",    "unknown"),
            json.dumps(sanitized_record, default=_json_default),
            rec.get("error_type",     "unknown"),
            rec.get("error_details",  ""),
            rec.get("orphan_type",    None),
            str(raw_orphan_id) if raw_orphan_id is not None else None,
            rec.get("pipeline_run_id", None),
        ))

    try:
        with get_cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                sql,
                rows,
                template="(%s, %s, %s::jsonb, %s, %s, %s, %s, %s)",
            )
            count = cur.rowcount

        logger.info("quarantine_batch_saved", count=count)
        return count

    except Exception as e:
        # We catch here to run the fallback instead of crashing.
        logger.error("quarantine_batch_failed", error=str(e), fallback=True)

        # ---> ONE-BY-ONE FALLBACK <---
        # execute_values() is all-or-nothing [atomicity]: one bad row fails the entire batch.
        # The fallback calls send_to_quarantine() per record so each record has its own commit & rollback

        # Without fallback: 0 / 50 saved.
        # With fallback:   49 / 50 saved. 

        saved = 0
        for rec in failed_records:
            success = send_to_quarantine(
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
            "quarantine_fallback_complete",
            saved=saved,
            total=len(failed_records),
        )
        return saved