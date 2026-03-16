"""
WAP (Write --> Audit --> Print)
Tables owned (all live in the audit schema (audit.*))
- audit.pipeline_run_log
- audit.file_tracking
- audit.quarantine
- audit.orphan_staging
- audit.pipeline_quality_metrics
- audit.column_quality_metrics

Decisions taken:
- run_id is an int (serial), NOT a UUID — pipeline_run_log owns the sequence.
- file tracking pk is file_path (varchar) - idempotency key is file_path alone.
    - rerun the same file path replace the row via (ON CONFLICT DO UPDATE)
- quarantine.record_data is text not jsonb - matches the schema definition.
- orphan_staging has no run_id fk - orphans arrive and resolve async across runs.   
- all writes use @db_retry decorator for transient pg connection failures.
- all 6 tables lives under audit schema. (audit.*) 
    - completely separate from the dwh schema.
    - the audit db can be queried independently of the dwh.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

import psycopg2.extras

from pipeline.utils.db import get_conn, get_cursor, get_dict_cursor
from pipeline.utils.retry import db_retry

logger = logging.getLogger(__name__)
############################################# DDL Schema #############################################

def get_audit_schema() -> str:
    return """
    create schema if not exists audit;
    
    create table if not exists audit.pipeline_run_log (
        run_id            serial      primary key,
        run_date          date        not null default current_date,
        run_type          varchar     not null,
        triggered_at      timestamp   not null default now(),
        started_at        timestamp,
        finished_at       timestamp,
        status            varchar     not null default 'running',
        total_files       int         not null default 0,
        successful_files  int         not null default 0,
        failed_files      int         not null default 0,
        total_records     int         not null default 0,
        total_loaded      int         not null default 0,
        total_quarantined int         not null default 0,
        total_orphaned    int         not null default 0,
        error_message     text
    );
    
    create table if not exists audit.file_tracking (
        file_path           varchar     primary key,
        file_type           varchar     not null default '',
        table_name          varchar     not null default '',
        status              varchar     not null default 'processing',
        records_found       int         not null default 0,
        records_loaded      int         not null default 0,
        records_quarantined int         not null default 0,
        records_orphaned    int         not null default 0,
        file_size_bytes     bigint      not null default 0,
        started_at          timestamp,
        finished_at         timestamp,
        run_id              int         references audit.pipeline_run_log(run_id)
    );
    
    create table if not exists audit.quarantine (
        quarantine_id    serial      primary key,
        source_file      varchar     not null default '',
        table_name       varchar     not null default '',
        record_id        varchar     not null default '',
        record_data      text        not null,
        rejection_reason varchar     not null,
        rejection_field  varchar     not null default '',
        rejection_value  varchar     not null default '',
        quarantined_at   timestamp   not null default now(),
        run_id           int         references audit.pipeline_run_log(run_id)
    );
    
    create table if not exists audit.orphan_staging (
        staging_id       serial      primary key,
        record_type      varchar     not null default '',
        record_id        varchar     not null default '',
        record_data      text        not null default '',
        missing_fk_type  varchar     not null default '',
        missing_fk_value varchar     not null default '',
        source_file      varchar     not null default '',
        arrived_at       timestamp   not null default now(),
        resolved_at      timestamp,
        status           varchar     not null default 'pending',
        expiry_date      date
    );
    
    create table if not exists audit.pipeline_quality_metrics (
        metric_id                serial      primary key,
        run_id                   int         references audit.pipeline_run_log(run_id),
        run_date                 date        not null default current_date,
        file_path                varchar     not null default '',
        table_name               varchar     not null default '',
        total_records            int         not null default 0,
        valid_records            int         not null default 0,
        quarantined_records      int         not null default 0,
        orphaned_records         int         not null default 0,
        duplicate_count          int         not null default 0,
        null_violations          int         not null default 0,
        type_violations          int         not null default 0,
        business_rule_violations int         not null default 0,
        orphan_count             int         not null default 0,
        duplicate_rate           numeric     not null default 0,
        orphan_rate              numeric     not null default 0,
        null_rate                numeric     not null default 0,
        integrity_rate           numeric     not null default 1,
        processing_latency_sec   numeric     not null default 0,
        recorded_at              timestamp   not null default now()
    );
    
    create table if not exists audit.column_quality_metrics (
        col_metric_id int         primary key generated always as identity,
        metric_id     int         references audit.pipeline_quality_metrics(metric_id),
        table_name    varchar     not null default '',
        column_name   varchar     not null default '',
        total_values  int         not null default 0,
        null_count    int         not null default 0,
        null_pct      numeric     not null default 0,
        invalid_count int         not null default 0,
        invalid_pct   numeric     not null default 0,
        run_date      date        not null default current_date
    );
    
    create index if not exists idx_file_tracking_run_id   on audit.file_tracking (run_id);
    create index if not exists idx_quarantine_run_id       on audit.quarantine (run_id);
    create index if not exists idx_quality_metrics_run_id  on audit.pipeline_quality_metrics (run_id);
    create index if not exists idx_orphan_staging_status   on audit.orphan_staging (status, expiry_date);

    """

def ensure_audit_schema() -> None:
    """ Ensure the audit schema is created """
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(get_audit_schema())
    logger.info("Audit Schema is ready....")



########################################### Writing Phase #############################################
@db_retry
def start_run(run_type: str) -> int:
    """
    Insert a pipeline_run_log row and return the generated run_id (int).
    
    Args:
        run_type (str): The type of the run (e.g. 'batch', 'micro_batch')

    Returns:
        The auto-generated run_id integer.
    """
    with get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
            insert into audit.pipeline_run_log 
                (run_date, run_type, triggered_at, started_at, status)
            values (current_date, %s, now(), now(), 'running')
            returning run_id
            """, (run_type,))
            run_id = cursor.fetchone()[0] # this is the auto-gen run_id

    logger.info(f"Run started with run_id: {run_id}")
    return run_id


@db_retry
def register_file(
    run_id: int, 
    file_path: str, 
    file_type: str, 
    table_name: str, 
    file_size_bytes: int = 0 ) -> None:
    """
    Register a file in file_tracking table with status 'processing'.
        - this uses (ON CONFLICT UPDATE) to handle duplicate of the same file
        - on a retry run resets it cleanly instead of failing

    Args:
        run_id:          Generated by start_run().
        file_path:       Source file path — this is the PK.
        file_type:       'csv' or 'json'.
        table_name:      Target warehouse table, e.g. 'fact_orders'.
        file_size_bytes: File size for reporting.
    """
    with get_cursor() as cur:
        cur.execute("""
            insert into audit.file_tracking
                (file_path, file_type, table_name, status, file_size_bytes, started_at, run_id)
            values (%s, %s, %s, 'processing', %s, now(), %s)
            on conflict (file_path) do update
                set
                    status = 'processing',
                    file_size_bytes = %s,
                    started_at = now(),
                    run_id = %s
            """,
            (file_path, file_type, table_name, file_size_bytes, run_id, file_size_bytes, run_id))

    logger.debug(
        "file_registered",
        extra={"run_id": run_id, "file": file_path, "table": table_name},
    )


########################################### Auditing phase  ###########################################

## Idempotency check
@db_retry
def is_file_processed(file_path: str) -> bool:
    """ 
    Return True if the file is already has status='success' in file_tracking table
    - this is called before register_file() to prevent processing the same file multiple times
        - if True, skip the file and call log_file_skipped() to log the skipped file
    
    Returns:
        True  → already loaded successfully, skip.
        False → new or previously failed, process.
    """
    with get_dict_cursor() as cur:
        cur.execute(
            """
            select 1 from audit.file_tracking
            where file_path = %s and status = 'success'
            limit 1
            """,
            (file_path,)
        )
        row = cur.fetchone()
        already_done = row is not None
        if already_done:
            logger.info(
                "file_already_processed",
                extra={"file": file_path},
            )
        return already_done


## File outcome writer
@db_retry
def mark_file_success(
        file_path: str,
        records_found: int,
        records_loaded: int,
        records_quarantined: int,
        records_orphaned: int,
    ) -> None:

    """
    Mark a file_tracking row as successfully processed
    - call this only after the warehouse upsert committed.

    Args:
        file_path: source file path (PK)
        records_found: total records found in the file
        records_loaded: total records loaded into the warehouse
        records_quarantined: total records quarantined
        records_orphaned: total records orphaned
    """
    with get_cursor() as cur:
        cur.execute("""
            update audit.file_tracking
            set
                status = 'success',
                records_found = %s,
                records_loaded = %s,
                records_quarantined = %s,
                records_orphaned = %s,
                finished_at = now()
            where file_path = %s
        """,
        (records_found, records_loaded, records_quarantined, records_orphaned, file_path))
    logger.info(
        "file_success",
        extra={
            "file": file_path,
            "records_found": records_found,
            "records_loaded": records_loaded,
            "records_quarantined": records_quarantined,
            "records_orphaned": records_orphaned,
        },
    )

@db_retry
def mark_file_failed(file_path: str, error_message: str) -> None:
    """
    Mark a file_tracking row as failed
    - call this only after the warehouse upsert failed.

    Args:
        file_path: source file path (PK)
        error_message: error message
    """
    with get_cursor() as cur:
        cur.execute("""
            update audit.file_tracking
            set
                status = 'failed',
                finished_at = now()
            where file_path = %s
        """,
        (file_path,))
    logger.info(
        "file_failed",
        extra={
            "file": file_path,
            "error_message": error_message,
        },
    )


# TODO: # Publish phase (complete_run function)
# TODO: # Quarantine phase (write_quarantine_batch function)
# TODO: # Orphan phase (write_orphan_batch function)
# TODO: # Quality metrics phase 
    # TODO: # (write_quality_metrics function
    # TODO: # write_column_quality_metrics function)
# TODO: # Complete summary phase 
    # TODO: # (get_run_summary function)
    # TODO: # get_quality_metrics_for_run function