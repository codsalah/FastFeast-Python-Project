# Quality Directory

## Definition

The `quality/` directory contains data quality tracking and reporting components for the FastFeast data pipeline. It provides comprehensive quality metrics tracking, PDF report generation, and quality threshold validation.

## What It Does

The quality layer provides:

- **Metrics Tracking**: Tracks per-file and aggregate quality metrics for each pipeline run
- **Quality Reports**: Generates PDF reports with KPIs and detailed audit information
- **Threshold Validation**: Validates quality metrics against configured thresholds
- **Audit Trail Integration**: Writes quality metrics to the pipeline_audit schema
- **Quarantine Export**: Exports quarantined records to JSON for manual review

## Why It Exists

The quality layer is essential for:

- **Data Quality Monitoring**: Providing visibility into data quality across pipeline runs
- **Operational Awareness**: Alerting stakeholders to quality issues
- **Compliance**: Ensuring data meets quality standards before consumption
- **Debugging**: Providing detailed quality information for troubleshooting
- **Continuous Improvement**: Tracking quality trends over time

## How It Works

### Core Components

#### `metrics_tracker.py` - Quality Metrics Tracking
Handles all quality metrics operations:
- Writes quality metrics to pipeline_audit schema
- Tracks per-file quality metrics (null rate, duplicate rate, orphan rate, quarantine rate)
- Calculates aggregate metrics for pipeline runs
- Exports quarantine records to JSON files
- Provides summary metrics for quality reports

**Key Functions:**
- `ensure_audit_schema()`: Creates pipeline_audit schema and tables
- `start_run(run_type)`: Starts a new pipeline run log entry
- `complete_run()`: Completes pipeline run with aggregate statistics
- `write_quality_metrics()`: Writes per-file quality metrics
- `write_quarantine_batch()`: Bulk-inserts quarantine records
- `export_quarantine_to_file()`: Exports quarantine records to JSON
- `get_run_summary()`: Returns aggregate statistics for a run
- `get_quality_metrics_for_run()`: Returns all per-file metrics for a run
- `get_run_record_totals()`: Sums record counts from quality metrics
- `get_run_file_totals()`: Returns file counts by status

**Quality Metrics Tracked:**
- total_records: Total records in file
- valid_records: Records that passed validation
- quarantined_records: Records that failed validation
- orphaned_records: Records with orphan references
- duplicate_count: Number of duplicate records
- null_violations: Number of null constraint violations
- business_rule_violations: Number of business rule violations
- duplicate_rate: Duplicate count / total records
- orphan_rate: Orphaned records / total records
- null_rate: Null violations / total records
- quarantine_rate: Quarantined records / total records
- processing_latency_sec: Time to process file
- quality_details: JSON with additional quality information

#### `quality_report.py` - Quality Report Generation
Generates PDF quality reports:
- Creates professional PDF reports with quality metrics
- Includes aggregate KPIs and per-file breakdowns
- Supports email attachment for distribution
- Provides visual summaries of quality data

**Key Functions:**
- `generate_daily_quality_report(run_id)`: Generates PDF report for a run
- `create_pdf_report()`: Creates PDF document with quality data
- `add_kpi_section()`: Adds KPI summary to report
- `add_file_metrics_section()`: Adds per-file metrics to report
- `add_quarantine_section()`: Adds quarantine summary to report

**Report Sections:**
1. **Header**: Run ID, date, status
2. **KPI Summary**: Aggregate metrics (total records, loaded, quarantined, orphaned)
3. **Quality Rates**: Null rate, duplicate rate, orphan rate, quarantine rate
4. **Per-File Metrics**: Detailed metrics for each processed file
5. **Quarantine Summary**: Count and types of quarantined records
6. **Performance**: Processing latency and duration

## Relationship with Architecture

### Position in Data Flow
```
┌─────────────────┐
│  Pipelines      │
│  (Loaders)      │
└────────┬────────┘
         │
         │ (Report metrics)
         ▼
┌─────────────────┐
│  Quality        │
│  Metrics        │
└────────┬────────┘
         │
         │ (Write to DB)
         ▼
┌─────────────────┐
│  pipeline_audit │
│  Schema         │
└────────┬────────┘
         │
         │ (Generate report)
         ▼
┌─────────────────┐
│  PDF Report     │
│  (Email)        │
└─────────────────┘
```

### Dependencies
- **warehouse/connection.py**: Database connection and query execution
- **config/settings.py**: Quality threshold configuration
- **alerting/alert_service.py**: Email notification for reports
- **utils/retry.py**: Database retry logic
- **utils/logger.py**: Structured logging

### Used By
- **loaders/**: All loaders report quality metrics via metrics_tracker
- **pipelines/batch_pipeline.py**: Generates quality reports after batch completion
- **pipelines/stream_pipeline.py**: Reports quality metrics after stream processing
- **handlers/quarantine_handler.py**: Uses quarantine export functionality

### Integration Points
1. **Loaders**: Report per-file quality metrics during loading
2. **Batch Pipeline**: Generates and emails quality reports
3. **Stream Pipeline**: Reports quality metrics (no PDF report)
4. **Audit Trail**: All quality data written to pipeline_audit schema
5. **Alerting**: PDF reports emailed to configured recipients

## Quality Metrics Schema

### pipeline_quality_metrics Table
```sql
CREATE TABLE pipeline_audit.pipeline_quality_metrics (
    metric_id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES pipeline_run_log(run_id),
    run_date DATE,
    table_name VARCHAR(100),
    source_file VARCHAR(512),
    total_records INTEGER,
    valid_records INTEGER,
    quarantined_records INTEGER,
    orphaned_records INTEGER,
    duplicate_count INTEGER,
    null_violations INTEGER,
    business_rule_violations INTEGER,
    duplicate_rate DECIMAL(6,4),
    orphan_rate DECIMAL(6,4),
    null_rate DECIMAL(6,4),
    quarantine_rate DECIMAL(6,4),
    processing_latency_sec FLOAT,
    quality_details JSONB,
    recorded_at TIMESTAMP
);
```

### quarantine Table
```sql
CREATE TABLE pipeline_audit.quarantine (
    quarantine_id SERIAL PRIMARY KEY,
    source_file VARCHAR(512),
    entity_type VARCHAR(100),
    raw_record JSONB,
    error_type VARCHAR(50),
    error_details TEXT,
    orphan_type VARCHAR(50),
    raw_orphan_id VARCHAR(256),
    pipeline_run_id INTEGER,
    quarantined_at TIMESTAMP
);
```

## Quality Thresholds

Quality thresholds are configured in `config/settings.py`:

```python
class QualityThresholdConfig(BaseSettings):
    max_null_rate: float = 0.05       # Maximum 5% null rate
    max_duplicate_rate: float = 0.01  # Maximum 1% duplicate rate
    max_orphan_rate: float = 0.05     # Maximum 5% orphan rate
    min_integrity_rate: float = 0.95  # Minimum 95% integrity rate
    min_file_success_rate: float = 0.90  # Minimum 90% file success rate
```

## Quality Report Generation

### Report Generation Flow
1. Batch pipeline completes
2. Quality metrics queried from pipeline_audit schema
3. PDF report generated with metrics
4. Report emailed to configured recipients
5. Report saved to file system (optional)

### Report Content
- **Run Information**: Run ID, date, status, duration
- **Aggregate KPIs**: Total files, records, loaded, quarantined, orphaned
- **Quality Rates**: Null rate, duplicate rate, orphan rate, quarantine rate
- **Per-File Breakdown**: Metrics for each processed file
- **Quarantine Summary**: Count by error type
- **Performance Metrics**: Processing latency, file success rate

## Quarantine Export

### Export Process
1. Query quarantine records for a run
2. Serialize to JSON format
3. Write to `quarantine_exports/quarantine_run_{run_id}.json`
4. Include in quality report summary

### Export Format
```json
[
  {
    "quarantine_id": 1,
    "source_file": "data/input/batch/2026-04-25/customers.csv",
    "entity_type": "source_customers",
    "raw_record": {...},
    "error_type": "schema_validation",
    "error_details": "customer_name cannot be null",
    "quarantined_at": "2026-04-25T12:00:00Z"
  }
]
```

## Audit Trail Integration

### Pipeline Run Log
Tracks each pipeline execution:
- run_id: Auto-generated identifier
- run_type: batch or stream
- run_date: Date of execution
- status: running, success, partial, failed, no_data
- started_at, completed_at: Timestamps
- total_files, successful_files, failed_files: File counts
- total_records, total_loaded, total_quarantined, total_orphaned: Record counts
- error_message: Error details if failed

### File Tracker
Tracks each file processed:
- file_id: Auto-generated identifier
- file_path: Path to file
- file_hash: SHA-256 hash for deduplication
- file_type: batch, stream, reference
- records_total, records_loaded, records_quarantined: Record counts
- status: processing, success, failed
- processed_at: Timestamp
- pipeline_run_id: Link to pipeline run

## Error Handling

Quality layer handles errors gracefully:
- Database errors are retried via `@db_retry`
- Report generation failures are logged but don't fail pipeline
- Export failures are logged but don't fail pipeline
- Missing data is handled with default values

## Configuration

Quality layer uses configuration from:
- **config/settings.py**: Quality thresholds, directory paths
- **Environment variables**: QUARANTINE_DIR, LOG_DIR

## Performance Considerations

- **Bulk Operations**: Quarantine records use bulk insert
- **Query Optimization**: Quality metrics queries use appropriate indexes
- **Caching**: Report generation can be cached if needed
- **Async Operations**: Email sending is asynchronous (doesn't block pipeline)

## Testing

Quality layer can be tested by:
1. Running pipeline with known quality issues
2. Verifying metrics are written to database
3. Checking quarantine export JSON
4. Validating PDF report generation
5. Testing email delivery

## Extending Quality Layer

### Adding New Quality Metrics
1. Add metric to pipeline_quality_metrics table schema
2. Update write_quality_metrics() to include new metric
3. Update calculation logic if needed
4. Add to quality report if needed
5. Update threshold configuration

### Adding New Report Sections
1. Add new section function in quality_report.py
2. Call from generate_daily_quality_report()
3. Add data query if needed
4. Update report layout

### Adding New Quarantine Types
1. Add new error_type to quarantine handler
2. Update quarantine export if needed
3. Add to quality report summary
4. Update threshold configuration if needed
