---------------------------------------------------------------------------------
-- Decisions taken:
-- Idempotent: All create statements use IF NOT EXISTS for safe reruns
-- Schema: Using pipeline_audit schema for all audit-related tables
-- PII: Record data stored raw for debugging but quarantined if invalid
---------------------------------------------------------------------------------


-- Create schema
------------ create schema if not exists pipeline_audit;
------------ set search_path to pipeline_audit;

-- orphan_staging
create table if not exists orphan_staging (
    staging_id       serial      primary key,
    record_type      varchar     not null,
    record_id        varchar     not null,
    record_data      text        not null,
    missing_fk_type  varchar     not null,
    missing_fk_value varchar     not null,
    source_file      varchar     not null,
    arrived_at       timestamptz not null default now(),
    resolved_at      timestamptz,
    signup_date      varchar,
    expiry_date      date        not null,
    status           varchar     not null default 'pending',
    created_at       timestamptz not null default now(),
    updated_at       timestamptz not null default now()
);
create index if not exists idx_orphan_staging_status on orphan_staging(status);

-- pipeline_run_log
create table if not exists pipeline_run_log (
    run_id            serial      primary key,
    run_date          timestamptz not null default now(),
    run_type          varchar     not null,
    triggered_at      timestamptz not null default now(),
    started_at        timestamptz,
    finished_at       timestamptz,
    status            varchar     not null default 'running',
    total_files       int         not null default 0,
    successful_files  int         not null default 0,
    failed_files      int         not null default 0,
    total_records     int         not null default 0,
    total_loaded      int         not null default 0,
    total_quarantined int         not null default 0,
    total_orphaned    int         not null default 0,
    error_message     text,
    created_at        timestamptz not null default now(),
    updated_at        timestamptz not null default now()
);

-- file_tracking
create table if not exists file_tracking (
    file_path           varchar     primary key,
    file_type           varchar     not null,
    table_name          varchar     not null,
    status              varchar     not null default 'processing',
    records_found       int         not null default 0,
    records_loaded      int         not null default 0,
    records_quarantined int         not null default 0,
    records_orphaned    int         not null default 0,
    file_size_bytes     bigint      not null default 0,
    started_at          timestamptz,
    finished_at         timestamptz,
    run_id              int         references pipeline_run_log(run_id),
    created_at          timestamptz not null default now(),
    updated_at          timestamptz not null default now()
);
create index if not exists idx_file_tracking_run_id on file_tracking(run_id);

-- quarantine
create table if not exists quarantine (
    quarantine_id    serial      primary key,
    source_file      varchar     not null,
    table_name       varchar     not null,
    record_id        varchar     not null,
    record_data      text        not null,
    rejection_reason varchar     not null,
    rejection_field  varchar     not null,
    rejection_value  varchar     not null,
    quarantined_at   timestamptz not null default now(),
    run_id           int         references pipeline_run_log(run_id),
    created_at       timestamptz not null default now(),
    updated_at       timestamptz not null default now()
);
create index if not exists idx_quarantine_run_id on quarantine(run_id);

-- pipeline_quality_metrics
create table if not exists pipeline_quality_metrics (
    metric_id                serial      primary key,
    run_id                   int         references pipeline_run_log(run_id),
    run_date                 date        not null default current_date,
    file_path                varchar     not null,
    table_name               varchar     not null,
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
    integrity_rate           numeric     not null default 0,
    processing_latency_sec   numeric     not null default 0,
    recorded_at              timestamptz not null default now(),
    created_at               timestamptz not null default now(),
    updated_at               timestamptz not null default now()
);
create index if not exists idx_pipeline_quality_metrics_run_id on pipeline_quality_metrics(run_id);

-- triggers to update updated_at
create or replace function update_timestamp_audit() returns trigger as $$
begin
    new.updated_at = now();
    return new;
end;
$$ language plpgsql;

create trigger trg_orphan_staging_update before update on orphan_staging for each row execute function update_timestamp_audit();
create trigger trg_pipeline_run_log_update before update on pipeline_run_log for each row execute function update_timestamp_audit();
create trigger trg_file_tracking_update before update on file_tracking for each row execute function update_timestamp_audit();
create trigger trg_quarantine_update before update on quarantine for each row execute function update_timestamp_audit();
create trigger trg_pipeline_quality_metrics_update before update on pipeline_quality_metrics for each row execute function update_timestamp_audit();
