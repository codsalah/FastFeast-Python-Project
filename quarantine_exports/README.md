# Quarantine Exports Directory

## Definition

The `quarantine_exports/` directory contains exported quarantine records from pipeline runs. These JSON files contain records that failed validation and were quarantined during data loading, providing a mechanism for manual review and correction.

## What It Does

The quarantine exports directory:

- **Stores Quarantine Records**: Contains JSON files with quarantined records from each pipeline run
- **Enables Manual Review**: Allows data engineers to review and correct invalid records
- **Supports Re-import**: Corrected records can be re-imported after fixing issues
- **Provides Audit Trail**: Maintains historical record of data quality issues

## Why It Exists

The quarantine exports directory is essential for:

- **Data Quality Management**: Providing visibility into data quality issues
- **Manual Correction**: Allowing human review and correction of invalid records
- **Compliance**: Maintaining records of data that failed validation
- **Debugging**: Providing detailed information for troubleshooting data issues
- **Recovery**: Enabling recovery of valid data from quarantined records

## How It Works

### Export Process

1. **Pipeline Execution**: During pipeline runs, invalid records are detected by validators
2. **Quarantine**: Invalid records are quarantined in the database (pipeline_audit.quarantine table)
3. **Export**: After pipeline completion, quarantine records are exported to JSON
4. **File Naming**: Exported files are named `quarantine_run_{run_id}.json`
5. **Storage**: Files are stored in `quarantine_exports/` directory

### Export Trigger

Quarantine exports are triggered by:
- **Batch Pipeline**: After batch pipeline completion
- **Stream Pipeline**: After stream pipeline completion
- **Manual Export**: Can be triggered manually via quality/metrics_tracker.py

### File Format

Each quarantine export file contains an array of quarantine records:

```json
[
  {
    "quarantine_id": 1,
    "source_file": "data/input/batch/2026-04-25/customers.csv",
    "entity_type": "source_customers",
    "raw_record": {
      "customer_id": 123,
      "full_name": null,
      "email": "invalid-email",
      "phone": "123",
      "region_id": null,
      "segment_id": 1,
      "signup_date": "2020-01-01",
      "gender": "male",
      "created_at": "2020-01-01T00:00:00",
      "updated_at": "2026-04-25T12:00:00"
    },
    "error_type": "schema_validation",
    "error_details": "customer_name cannot be null",
    "orphan_type": null,
    "raw_orphan_id": null,
    "quarantined_at": "2026-04-25T12:00:00Z"
  }
]
```

### Record Structure

Each quarantine record contains:

- **quarantine_id**: Unique identifier for the quarantine record
- **source_file**: Path to the source file that contained the invalid record
- **entity_type**: Type of entity (e.g., source_customers, source_orders)
- **raw_record**: The original record data as a JSON object
- **error_type**: Type of error (schema_validation, orphan, referential_integrity, parse_error)
- **error_details**: Detailed error message
- **orphan_type**: Type of orphan (if applicable: customer, driver, restaurant)
- **raw_orphan_id**: Original orphan ID (if applicable)
- **quarantined_at**: Timestamp when the record was quarantined

## Relationship with Architecture

### Position in Data Flow
```
┌─────────────────┐
│  Pipeline       │
│  (Loaders)      │
└────────┬────────┘
         │
         │ (Invalid records)
         ▼
┌─────────────────┐
│  Quarantine     │
│  (DB Table)     │
└────────┬────────┘
         │
         │ (Export)
         ▼
┌─────────────────┐
│  quarantine_    │
│  exports/       │
│  (JSON Files)   │
└────────┬────────┘
         │
         │ (Manual review)
         ▼
┌─────────────────┐
│  Correction     │
│  & Re-import    │
└─────────────────┘
```

### Dependencies
- **quality/metrics_tracker.py**: Handles export of quarantine records
- **pipelines/batch_pipeline.py**: Triggers export after batch completion
- **pipelines/stream_pipeline.py**: Triggers export after stream completion
- **pipeline_audit.quarantine**: Database table containing quarantine records

### Used By
- **Data Engineers**: Review quarantine exports to identify and fix data issues
- **Quality Team**: Analyze quarantine patterns to improve data quality
- **Pipeline Operators**: Monitor quarantine rates for operational awareness

### Integration Points
1. **Quality Layer**: Export triggered by quality/metrics_tracker.py
2. **Batch Pipeline**: Automatic export after batch completion
3. **Stream Pipeline**: Automatic export after stream completion
4. **Manual Review**: Human review of exported JSON files

## File Naming Convention

Files are named using the pattern:
```
quarantine_run_{run_id}.json
```

Where `run_id` is the pipeline run identifier from `pipeline_audit.pipeline_run_log`.

Example:
- `quarantine_run_1.json`
- `quarantine_run_42.json`
- `quarantine_run_123.json`

## Error Types

Quarantine records can have the following error types:

### schema_validation
- Invalid data types
- Missing required fields (null violations)
- Values outside allowed ranges
- Invalid format (regex violations)
- Invalid categorical values

### orphan
- Missing dimension references
- Foreign key violations
- Non-existent parent records

### referential_integrity
- Circular references
- Self-referencing errors
- Invalid relationship constraints

### parse_error
- File parsing failures
- Malformed CSV/JSON
- Encoding issues
- Structure mismatches

## Manual Review Process

### Review Steps
1. **Identify Issue**: Review error_details to understand the problem
2. **Examine Record**: Check raw_record to see the invalid data
3. **Determine Fix**: Decide how to correct the issue
4. **Apply Fix**: Correct the source data or update the record
5. **Re-import**: Load corrected data back into the pipeline

### Common Fixes

#### Null Values
- Fill in missing required fields
- Use default values where appropriate
- Update source system to provide complete data

#### Invalid Formats
- Correct email addresses
- Fix phone number formats
- Standardize date formats
- Validate UUIDs

#### Orphan References
- Add missing dimension records
- Update foreign keys to valid references
- Wait for batch pipeline to bring in new dimensions

#### Out of Range Values
- Correct numeric ranges
- Fix rating values (1-5)
- Adjust percentages (0-100)

## Re-import Process

After correcting quarantine records:

1. **Correct Source Data**: Fix the source file or generate new data
2. **Re-run Pipeline**: Process the corrected file through the pipeline
3. **Verify**: Check that records are no longer quarantined
4. **Archive**: Move old quarantine export to archive if needed

## Configuration

Quarantine export directory is configured in `config/settings.py`:
```python
quarantine_dir: str = Field(default="data/quarantine", alias="QUARANTINE_DIR")
```

The export directory is derived from the quarantine directory:
```
{quarantine_dir}/../quarantine_exports/
```

## Security Considerations

- **Sensitive Data**: Quarantine records may contain PII (names, emails, phone numbers)
- **Access Control**: Limit access to quarantine_exports directory
- **Retention Policy**: Define retention policy for quarantine exports
- **Encryption**: Consider encrypting exports if they contain sensitive data
- **Audit Logging**: Log access to quarantine export files

## Performance Considerations

- **File Size**: Large quarantine exports can be slow to generate
- **Disk Space**: Monitor disk space usage for quarantine exports
- **Cleanup**: Implement cleanup policy for old exports
- **Compression**: Consider compressing old exports to save space

## Monitoring

Monitor quarantine exports for:
- **Export Volume**: Number of records quarantined per run
- **Error Patterns**: Common error types and sources
- **Trend Analysis**: Increasing or decreasing quarantine rates
- **Source Files**: Files with high quarantine rates

## Cleanup Strategy

Implement cleanup strategy for quarantine exports:
- **Retention Period**: Keep exports for 30-90 days
- **Archive**: Move old exports to cold storage
- **Delete**: Remove exports beyond retention period
- **Compress**: Compress exports before archiving

## Troubleshooting

### Missing Export File
- Check if pipeline completed successfully
- Verify quarantine records exist in database
- Check directory permissions
- Review pipeline logs for export errors

### Empty Export File
- No records were quarantined in the run
- Check if validation is working correctly
- Verify data quality of source files

### Corrupt Export File
- Check disk space
- Verify file permissions
- Review export process logs
- Re-run export if needed

## Extending Quarantine Exports

### Adding New Export Formats
1. Add new export function in quality/metrics_tracker.py
2. Support additional formats (CSV, Parquet)
3. Update pipeline to call new export function
4. Update documentation

### Adding Metadata to Exports
1. Add metadata fields to export function
2. Include run summary information
3. Add quality metrics to export
4. Update export format documentation
