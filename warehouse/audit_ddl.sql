-- ============================================================
-- FastFeast Pipeline Audit Schema
-- Schema: pipeline_audit
-- Contains: pipeline operational tables, quarantine, orphan tracking
-- ============================================================

CREATE SCHEMA IF NOT EXISTS pipeline_audit;
SET search_path TO pipeline_audit, public;


-- ════════════════════════════════════════════════════════════
-- PIPELINE OPERATIONAL TABLES
-- ════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS pipeline_run_log (
    run_id            serial PRIMARY KEY,
    run_type          varchar(50) NOT NULL,
    run_date          date NOT NULL,
    status            varchar(50) NOT NULL,
    started_at        timestamp NOT NULL,
    completed_at      timestamp,
    total_files       integer DEFAULT 0,
    successful_files  integer DEFAULT 0,
    failed_files      integer DEFAULT 0,
    total_records     integer DEFAULT 0,
    total_loaded      integer DEFAULT 0,
    total_quarantined integer DEFAULT 0,
    total_orphaned    integer DEFAULT 0,
    error_message     text
);

CREATE TABLE IF NOT EXISTS file_tracker (
    file_id             serial PRIMARY KEY,
    file_path           varchar(512) NOT NULL,
    file_hash           varchar(64) NOT NULL,
    file_type           varchar(50) NOT NULL,
    records_total       integer,
    records_loaded      integer,
    records_quarantined integer,
    status              varchar(50) NOT NULL,
    processed_at        timestamp NOT NULL,
    pipeline_run_id     integer REFERENCES pipeline_run_log(run_id),
    UNIQUE (file_path, file_hash)
);

CREATE TABLE IF NOT EXISTS pipeline_quality_metrics (
    metric_id               serial PRIMARY KEY,
    run_id                  integer NOT NULL REFERENCES pipeline_run_log(run_id),
    run_date                date NOT NULL,
    table_name              varchar(100) NOT NULL,
    source_file             varchar(512),
    total_records           integer,
    valid_records           integer,
    quarantined_records     integer,
    orphaned_records        integer,
    duplicate_count         integer,
    null_violations         integer,
    duplicate_rate          decimal(6,4),
    orphan_rate             decimal(6,4),
    null_rate               decimal(6,4),
    quarantine_rate         decimal(6,4),
    processing_latency_sec  decimal(10,2),
    quality_details         jsonb,
    recorded_at             timestamp NOT NULL DEFAULT now()
);


-- ════════════════════════════════════════════════════════════
-- SUPPORT TABLES — ORPHAN + QUARANTINE
-- ════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS orphan_tracking (
    tracking_id    serial PRIMARY KEY,
    order_id       varchar(256) NOT NULL,
    orphan_type    varchar(50) NOT NULL,
    raw_id         integer NOT NULL,
    is_resolved    boolean NOT NULL DEFAULT FALSE,
    retry_count    smallint NOT NULL DEFAULT 0,
    detected_at    timestamp NOT NULL,
    resolved_at    timestamp
);

CREATE TABLE IF NOT EXISTS quarantine (
    quarantine_id   serial PRIMARY KEY,
    source_file     varchar(512) NOT NULL,
    entity_type     varchar(100) NOT NULL,
    raw_record      jsonb NOT NULL,
    error_type      varchar(100) NOT NULL,
    error_details   text NOT NULL,
    orphan_type     varchar(50),
    raw_orphan_id   varchar(256),
    pipeline_run_id integer REFERENCES pipeline_run_log(run_id),
    quarantined_at  timestamp NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_file_tracker_run_id      ON pipeline_audit.file_tracker(pipeline_run_id);
CREATE INDEX IF NOT EXISTS idx_quarantine_run_id         ON pipeline_audit.quarantine(pipeline_run_id);
CREATE INDEX IF NOT EXISTS idx_orphan_tracking_order_id  ON pipeline_audit.orphan_tracking(order_id, orphan_type);
CREATE INDEX IF NOT EXISTS idx_quality_metrics_run_id    ON pipeline_audit.pipeline_quality_metrics(run_id);
