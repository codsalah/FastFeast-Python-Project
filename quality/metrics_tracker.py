"""
pipeline/logging/audit_trail.py
────────────────────────────────
Python write layer for the pipeline_audit schema.
Tables defined in warehouse/audit_ddl.sql:
    - pipeline_audit.pipeline_run_log
    - pipeline_audit.file_tracker (handled by file_tracker.py)
    - pipeline_audit.quarantine
    - pipeline_audit.orphan_tracking (writes via handlers/orphan_handler.py)
    - pipeline_audit.pipeline_quality_metrics

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
from pathlib import Path
from typing import Optional

import psycopg2.extras

from warehouse.connection import get_conn, get_cursor, get_dict_cursor
from utils.retry import db_retry
from utils.logger import get_logger_name

logger = get_logger_name(__name__)

_DDL_PATH = Path(__file__).resolve().parents[1] / "warehouse" / "audit_ddl.sql"


def ensure_audit_schema() -> None:
    """Create pipeline_audit schema and tables if they don't exist."""
    if not _DDL_PATH.exists():
        raise FileNotFoundError(f"DDL not found at {_DDL_PATH}")
    ddl = _DDL_PATH.read_text(encoding="utf-8")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
    logger.info("pipeline_audit_schema_ready", ddl_path=str(_DDL_PATH))


# ── Run lifecycle ─────────────────────────────────────────────────────────────

@db_retry
def start_run(run_type: str) -> int:
    """Open a new pipeline run and return its auto-generated run_id."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO pipeline_audit.pipeline_run_log
                    (run_type, run_date, started_at, status)
                VALUES (%s, current_date, now(), 'running')
                RETURNING run_id
            """, (run_type,))
            run_id = cur.fetchone()[0]

    logger.info("run_started", run_id=run_id, run_type=run_type)
    return run_id


@db_retry
def complete_run(
    run_id: int,
    status: str,
    total_files: int,
    successful_files: int,
    failed_files: int,
    total_records: int,
    total_loaded: int,
    total_quarantined: int,
    total_orphaned: int,
    error_message: Optional[str] = None,
) -> None:
    """Finalise the pipeline_run_log row with aggregate statistics."""
    with get_cursor() as cur:
        cur.execute("""
            UPDATE pipeline_audit.pipeline_run_log
            SET
                status = %s,
                completed_at = now(),
                total_files = %s,
                successful_files = %s,
                failed_files = %s,
                total_records = %s,
                total_loaded = %s,
                total_quarantined = %s,
                total_orphaned = %s,
                error_message = %s
            WHERE run_id = %s
        """, (
            status, total_files, successful_files, failed_files,
            total_records, total_loaded, total_quarantined, total_orphaned,
            error_message, run_id
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
    """Check if a file was already successfully processed."""
    if not os.path.exists(file_path):
        return False

    try:
        file_hash = compute_file_hash(file_path)
        return _is_file_processed(file_path, file_hash)
    except Exception as e:
        logger.warning("is_file_processed_failed", file=file_path, error=str(e))
        return False


def register_file(pipeline_run_id: int, file_path: str, file_type: str) -> None:
    """Register a file as being processed. Computes hash automatically."""
    file_hash = compute_file_hash(file_path)
    _register_file(
        run_id=pipeline_run_id,
        file_path=file_path,
        file_hash=file_hash,
        file_type=file_type,
    )


def mark_file_success(file_path: str, records_total: int, records_loaded: int, records_quarantined: int) -> None:
    """Mark a file as successfully processed."""
    file_hash = compute_file_hash(file_path)
    _mark_file_success(
        file_path=file_path,
        file_hash=file_hash,
        records_total=records_total,
        records_loaded=records_loaded,
        records_quarantined=records_quarantined,
    )


def mark_file_failed(file_path: str, error_message: str) -> None:
    """Mark a file as failed."""
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
    """Bulk-insert bad records into quarantine table."""
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
            INSERT INTO pipeline_audit.quarantine
                (source_file, entity_type, raw_record,
                 error_type, error_details,
                 orphan_type, raw_orphan_id,
                 pipeline_run_id)
            VALUES %s
            """,
            rows,
            template="(%s, %s, %s::jsonb, %s, %s, %s, %s, %s)",
        )

    logger.warning("quarantine_batch_written", count=len(records), source_file=source_file,
                   entity_type=entity_type, error_type=error_type)


@db_retry
def export_quarantine_to_file(run_id: int, output_dir: str = "quarantine_exports") -> str:
    """Fetch all quarantine records for a run and save to a JSON file."""
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    filename = f"quarantine_run_{run_id}.json"
    file_path = Path(output_dir) / filename

    with get_dict_cursor() as cur:
        cur.execute("""
            SELECT 
                quarantine_id, source_file, entity_type, raw_record, 
                error_type, error_details, quarantined_at
            FROM pipeline_audit.quarantine
            WHERE pipeline_run_id = %s
        """, (run_id,))
        rows = cur.fetchall()

    if not rows:
        return None

    # Convert to pure dicts for JSON serialization
    serialized_rows = []
    for row in rows:
        serialized_rows.append({
            "quarantine_id": row["quarantine_id"],
            "source_file": row["source_file"],
            "entity_type": row["entity_type"],
            "raw_record": row["raw_record"],  # Already a dict if using jsonb + psycopg2.extras
            "error_type": row["error_type"],
            "error_details": row["error_details"],
            "quarantined_at": row["quarantined_at"].isoformat() if row["quarantined_at"] else None
        })

    with open(file_path, "w") as f:
        json.dump(serialized_rows, f, indent=2)

    logger.info("quarantine_exported", run_id=run_id, file=str(file_path))
    return str(file_path)


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
    """Insert one quality-metrics row per processed file."""
    safe_total = total_records if total_records > 0 else 1

    duplicate_rate = round(duplicate_count / safe_total, 4)
    orphan_rate = round(orphaned_records / safe_total, 4)
    null_rate = round(null_violations / safe_total, 4)
    quarantine_rate = round(quarantined_records / safe_total, 4)

    details_json = json.dumps(quality_details) if quality_details else None

    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_audit.pipeline_quality_metrics
                (run_id, run_date, table_name, source_file,
                 total_records, valid_records,
                 quarantined_records, orphaned_records,
                 duplicate_count, null_violations,
                 duplicate_rate, orphan_rate, null_rate, quarantine_rate,
                 processing_latency_sec, quality_details)
            VALUES
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

    logger.info("quality_metrics_written", run_id=run_id, table=table_name, file=source_file,
                total=total_records, valid=valid_records, quarantine_rate=quarantine_rate)


# ── Read / summary ────────────────────────────────────────────────────────────

@db_retry
def get_run_summary(run_id: int) -> dict:
    """Return aggregate statistics for a completed run."""
    with get_dict_cursor() as cur:
        cur.execute("""
            SELECT
                run_id, run_type, run_date, status,
                started_at, completed_at,
                total_files, successful_files, failed_files,
                total_records, total_loaded, total_quarantined, total_orphaned,
                error_message,
                CASE WHEN total_files > 0
                     THEN ROUND(successful_files::numeric / total_files, 4)
                     ELSE 0 END AS file_success_rate,
                EXTRACT(EPOCH FROM (completed_at - started_at)) AS run_duration_sec
            FROM pipeline_audit.pipeline_run_log
            WHERE run_id = %s
        """, (run_id,))
        row = cur.fetchone()

    if row is None:
        logger.warning("get_run_summary_not_found", run_id=run_id)
        return {}

    return dict(row)


@db_retry
def get_quality_metrics_for_run(run_id: int) -> list[dict]:
    """Return all per-file quality metric rows for a given run."""
    with get_dict_cursor() as cur:
        cur.execute("""
            SELECT
                metric_id, table_name, source_file,
                total_records, valid_records,
                quarantined_records, orphaned_records,
                duplicate_count, null_violations,
                duplicate_rate, orphan_rate, null_rate, quarantine_rate,
                processing_latency_sec, quality_details, recorded_at
            FROM pipeline_audit.pipeline_quality_metrics
            WHERE run_id = %s
            ORDER BY source_file
        """, (run_id,))
        rows = cur.fetchall()

    result = [dict(r) for r in rows]
    logger.debug("quality_metrics_fetched", run_id=run_id, count=len(result))
    return result