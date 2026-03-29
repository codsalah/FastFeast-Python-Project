"""
pipeline/logging/audit_trail.py
────────────────────────────────
Python write layer for the warehouse schema.
Tables defined in warehouse/dwh_ddl.sql:
    - warehouse.pipeline_run_log
    - warehouse.file_tracker (handled by file_tracker.py)
    - warehouse.quarantine
    - warehouse.orphan_tracking
    - warehouse.pipeline_quality_metrics

Decisions taken:
    - run_id is an int (serial) — pipeline_run_log owns the sequence.
    - file tracking uses file_path + file_hash composite key (handled by file_tracker.py)
    - quarantine.raw_record is jsonb
    - orphan_tracking has no FK to run_log — orphans resolve async across runs.
    - All writes use @db_retry decorator for transient PG connection failures.
"""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from typing import Optional

import psycopg2.extras

from warehouse.connection import get_conn, get_cursor, get_dict_cursor
from utils.retry import db_retry
from utils.logger import get_logger_name

logger = get_logger_name(__name__)

_DDL_PATH = Path(__file__).resolve().parents[2] / "warehouse" / "dwh_ddl.sql"


def ensure_warehouse_schema() -> None:
    """Create warehouse schema and tables if they don't exist."""
    if not _DDL_PATH.exists():
        raise FileNotFoundError(f"DDL not found at {_DDL_PATH}")
    ddl = _DDL_PATH.read_text(encoding="utf-8")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
    logger.info("warehouse_schema_ready", ddl_path=str(_DDL_PATH))


# ── Run lifecycle ─────────────────────────────────────────────────────────────

@db_retry
def start_run(run_type: str) -> int:
    """
    Open a new pipeline run and return its auto-generated run_id.

    Args:
        run_type: 'batch' or 'micro_batch' or 'watcher_test'

    Returns:
        Integer run_id from pipeline_run_log.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                insert into warehouse.pipeline_run_log
                    (run_type, run_date, started_at, status)
                values (%s, current_date, now(), 'running')
                returning run_id
            """, (run_type,))
            run_id = cur.fetchone()[0]

    logger.info("run_started", run_id=run_id, run_type=run_type)
    return run_id


@db_retry
def complete_run(
    run_id: int,
    status: str,                    # 'success' | 'failed'
    total_files: int,
    successful_files: int,
    failed_files: int,
    total_records: int,
    total_loaded: int,
    total_quarantined: int,
    total_orphaned: int,
    error_message: Optional[str] = None,
) -> None:
    """
    Finalise the pipeline_run_log row with aggregate statistics.
    Call at the very end of every run, success or failure.
    """
    with get_cursor() as cur:
        cur.execute("""
            update warehouse.pipeline_run_log
            set
                status            = %s,
                completed_at      = now(),
                total_files       = %s,
                successful_files  = %s,
                failed_files      = %s,
                total_records     = %s,
                total_loaded      = %s,
                total_quarantined = %s,
                total_orphaned    = %s,
                error_message     = %s
            where run_id = %s
        """, (
            status,
            total_files, successful_files, failed_files,
            total_records, total_loaded, total_quarantined, total_orphaned,
            error_message,
            run_id,
        ))
    logger.info("run_completed", run_id=run_id, status=status)


# ── File tracker wrappers (delegate to file_tracker.py) ──────────────────────

from utils.file_tracker import (
    compute_file_hash,
    is_file_processed as _is_file_processed,
    register_file as _register_file,
    mark_file_success as _mark_file_success,
    mark_file_failed as _mark_file_failed,
)

def is_file_processed(file_path: str) -> bool:
    """
    Check if a file was already successfully processed.
    Returns True if the exact file (path + hash) was already processed.
    Returns False for non-existent files (they can't be processed yet).
    """
    # If file doesn't exist, it can't be processed
    if not os.path.exists(file_path):
        return False
    
    try:
        file_hash = compute_file_hash(file_path)
        return _is_file_processed(file_path, file_hash)
    except Exception as e:
        logger.warning("is_file_processed_failed", file=file_path, error=str(e))
        return False


def register_file(
    pipeline_run_id: int,
    file_path: str,
    file_type: str,
) -> None:
    """
    Register a file as being processed.
    Computes hash automatically.
    """
    file_hash = compute_file_hash(file_path)
    _register_file(
        run_id=pipeline_run_id,
        file_path=file_path,
        file_hash=file_hash,
        file_type=file_type,
    )


def mark_file_success(
    file_path: str,
    records_total: int,
    records_loaded: int,
    records_quarantined: int,
) -> None:
    """
    Mark a file as successfully processed.
    """
    file_hash = compute_file_hash(file_path)
    _mark_file_success(
        file_path=file_path,
        file_hash=file_hash,
        records_total=records_total,
        records_loaded=records_loaded,
        records_quarantined=records_quarantined,
    )


def mark_file_failed(file_path: str, error_message: str) -> None:
    """
    Mark a file as failed.
    """
    file_hash = compute_file_hash(file_path)
    _mark_file_failed(file_path, file_hash, error_message)


# ── Quarantine ────────────────────────────────────────────────────────────────

@db_retry
def write_quarantine_batch(
    records: list[dict],
    source_file: str,
    entity_type: str,
    error_type: str,
    pipeline_run_id: int,
    error_details: str = None,
    orphan_type: str = None,
    raw_orphan_id: str = None,
) -> None:
    """
    Bulk-insert bad records into the quarantine table.

    raw_record is stored as jsonb for structured querying.
    """
    if not records:
        return

    rows = [
        (
            source_file,
            entity_type,
            json.dumps(rec, default=str),
            error_type,
            error_details or "",
            orphan_type,
            raw_orphan_id,
            pipeline_run_id,
        )
        for rec in records
    ]

    with get_cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            insert into warehouse.quarantine
                (source_file, entity_type, raw_record,
                 error_type, error_details,
                 orphan_type, raw_orphan_id,
                 pipeline_run_id)
            values %s
            """,
            rows,
            template="(%s, %s, %s::jsonb, %s, %s, %s, %s, %s)",
        )

    logger.warning("quarantine_batch_written",
                   count=len(records),
                   source_file=source_file,
                   entity_type=entity_type,
                   error_type=error_type)


# ── Orphan tracking ───────────────────────────────────────────────────────────

@db_retry
def write_orphan_batch(
    records: list[dict],
    orphan_type: str,       # 'customer' | 'driver' | 'restaurant' | 'agent'
    fk_field: str,          # column name holding the missing FK value
) -> None:
    """
    Bulk-insert unresolved FK references into orphan_tracking.
    """
    if not records:
        return

    rows = []
    for rec in records:
        order_id = str(rec.get('order_id', rec.get('ticket_id', 'unknown')))
        raw_id = rec.get(fk_field)
        if raw_id is not None:
            rows.append((order_id, orphan_type, int(raw_id)))

    if not rows:
        return

    with get_cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            insert into warehouse.orphan_tracking
                (order_id, orphan_type, raw_id,
                 is_resolved, retry_count, detected_at)
            values %s
            """,
            [(r[0], r[1], r[2], False, 0) for r in rows],
            template="(%s, %s, %s, %s, %s, now())",
        )

    logger.warning("orphan_batch_tracked",
                   count=len(records),
                   orphan_type=orphan_type)


@db_retry
def mark_orphan_resolved(order_id: str, orphan_type: str) -> None:
    """
    Flip is_resolved=true and stamp resolved_at.
    """
    with get_cursor() as cur:
        cur.execute("""
            update warehouse.orphan_tracking
            set is_resolved = true,
                resolved_at = now()
            where order_id    = %s
              and orphan_type = %s
              and is_resolved = false
        """, (order_id, orphan_type))

    logger.info("orphan_resolved", order_id=order_id, orphan_type=orphan_type)


@db_retry
def increment_orphan_retry(order_id: str, orphan_type: str) -> None:
    """
    Increment retry_count for an orphan that was attempted but still unresolved.
    """
    with get_cursor() as cur:
        cur.execute("""
            update warehouse.orphan_tracking
            set retry_count = retry_count + 1
            where order_id    = %s
              and orphan_type = %s
              and is_resolved = false
        """, (order_id, orphan_type))


# ── Quality metrics ───────────────────────────────────────────────────────────

@db_retry
def write_quality_metrics(
    run_id: int,
    table_name: str,
    source_file: str,
    total_records: int,
    valid_records: int,
    quarantined_records: int,
    orphaned_records: int,
    duplicate_count: int,
    null_violations: int,
    processing_latency_sec: float,
    quality_details: dict = None,
) -> None:
    """
    Insert one quality-metrics row per processed file.
    """
    safe_total = total_records if total_records > 0 else 1

    duplicate_rate  = round(duplicate_count      / safe_total, 4)
    orphan_rate     = round(orphaned_records      / safe_total, 4)
    null_rate       = round(null_violations       / safe_total, 4)
    quarantine_rate = round(quarantined_records   / safe_total, 4)

    details_json = json.dumps(quality_details) if quality_details else None

    with get_cursor() as cur:
        cur.execute("""
            insert into warehouse.pipeline_quality_metrics
                (run_id, run_date, table_name, source_file,
                 total_records, valid_records,
                 quarantined_records, orphaned_records,
                 duplicate_count, null_violations,
                 duplicate_rate, orphan_rate, null_rate, quarantine_rate,
                 processing_latency_sec, quality_details)
            values
                (%s, current_date, %s, %s,
                 %s, %s,
                 %s, %s,
                 %s, %s,
                 %s, %s, %s, %s,
                 %s, %s::jsonb)
        """, (
            run_id, table_name, source_file,
            total_records, valid_records,
            quarantined_records, orphaned_records,
            duplicate_count, null_violations,
            duplicate_rate, orphan_rate, null_rate, quarantine_rate,
            processing_latency_sec,
            details_json,
        ))

    logger.info("quality_metrics_written",
                run_id=run_id,
                table=table_name,
                file=source_file,
                total=total_records,
                valid=valid_records,
                quarantine_rate=quarantine_rate)


# ── Read / summary ────────────────────────────────────────────────────────────

@db_retry
def get_run_summary(run_id: int) -> dict:
    """
    Return aggregate statistics for a completed run.
    """
    with get_dict_cursor() as cur:
        cur.execute("""
            select
                run_id,
                run_type,
                run_date,
                status,
                started_at,
                completed_at,
                total_files,
                successful_files,
                failed_files,
                total_records,
                total_loaded,
                total_quarantined,
                total_orphaned,
                error_message,
                case
                    when total_files > 0
                    then round(successful_files::numeric / total_files, 4)
                    else 0
                end as file_success_rate,
                extract(epoch from (completed_at - started_at)) as run_duration_sec
            from warehouse.pipeline_run_log
            where run_id = %s
        """, (run_id,))
        row = cur.fetchone()

    if row is None:
        logger.warning("get_run_summary_not_found", run_id=run_id)
        return {}

    return dict(row)


@db_retry
def get_quality_metrics_for_run(run_id: int) -> list[dict]:
    """
    Return all per-file quality metric rows for a given run.
    """
    with get_dict_cursor() as cur:
        cur.execute("""
            select
                metric_id,
                table_name,
                source_file,
                total_records,
                valid_records,
                quarantined_records,
                orphaned_records,
                duplicate_count,
                null_violations,
                duplicate_rate,
                orphan_rate,
                null_rate,
                quarantine_rate,
                processing_latency_sec,
                quality_details,
                recorded_at
            from warehouse.pipeline_quality_metrics
            where run_id = %s
            order by source_file
        """, (run_id,))
        rows = cur.fetchall()

    result = [dict(r) for r in rows]
    logger.debug("quality_metrics_fetched", run_id=run_id, count=len(result))
    return result