# Loaders Directory

## Definition

The `loaders/` directory contains data loading modules that transform and load data from source files into the data warehouse. These loaders handle both dimension tables (with SCD2 versioning) and fact tables (with orphan detection and versioning).

## What It Does

The loaders provide:

- **Dimension Loading**: Loads dimension tables (customers, drivers, restaurants, agents) with SCD2 (Slowly Changing Dimension Type 2) versioning
- **Fact Loading**: Loads fact tables (orders, tickets, events) with orphan detection and versioning
- **Static Data Loading**: Loads static reference data (reasons, channels, priorities)
- **Date Dimension Loading**: Generates and loads date dimension for time-based analysis
- **Data Validation**: Validates incoming data against schema contracts
- **Quality Enforcement**: Quarantines invalid records and tracks quality metrics

## Why It Exists

The loaders are essential for:

- **Data Transformation**: Converting source data to warehouse schema
- **Change Tracking**: Maintaining history of dimension changes via SCD2
- **Data Quality**: Enforcing schema validation and business rules
- **Orphan Handling**: Detecting and tracking orphan references in fact data
- **Idempotency**: Ensuring files are not processed multiple times via hash tracking
- **Performance**: Using bulk operations and connection pooling for efficient loading

## How It Works

### Core Components

#### `base_scd2_loader.py` - Base SCD2 Loader
Abstract base class for all dimension loaders:
- Implements SCD2 logic (expire old rows, insert new versions)
- Handles schema validation and quarantine
- Provides change detection via hash comparison
- Supports SCD1 updates for non-tracked fields
- Manages same-day updates (multiple changes in same batch)

**Key Methods:**
- `load(df, batch_date, source_file, run_id)`: Main loading method
- `_detect_changes()`: Compares incoming vs active records
- `_insert_new()`: Inserts new dimension records
- `_expire_old_row()`: Expires old dimension versions
- `_apply_scd1()`: Updates non-tracked fields in-place
- `_apply_same_day_update()`: Handles multiple same-day changes

**SCD2 Logic:**
1. For each incoming record:
   - Calculate hash of tracked fields
   - Check if natural key exists with same hash
   - If exists and hash matches: Skip (no change)
   - If exists and hash differs: Expire old row, insert new version
   - If not exists: Insert new record

#### Dimension Loaders

##### `dim_customer_loader.py`
Loads customer dimension with SCD2:
- Natural key: `customer_id`
- Tracked fields: full_name, email, phone, region_id, segment_id, gender
- Source entity: `source_customers`
- PII handling: Name is masked using pepper hash

##### `dim_driver_loader.py`
Loads driver dimension with SCD2:
- Natural key: `driver_id`
- Tracked fields: driver_name, driver_phone, national_id, region_id, shift, vehicle_type, rating_avg, on_time_rate, cancel_rate, is_active
- Source entity: `source_drivers`

##### `dim_restaurant_loader.py`
Loads restaurant dimension with SCD2:
- Natural key: `restaurant_id`
- Tracked fields: restaurant_name, region_id, category_id, price_tier, rating_avg, prep_time_avg_min, is_active
- Source entity: `source_restaurants`
- Handles JSON input format

##### `dim_agent_loader.py`
Loads agent dimension with SCD2:
- Natural key: `agent_id`
- Tracked fields: agent_name, agent_email, agent_phone, team_id, skill_level, avg_handle_time_min, resolution_rate, csat_score, is_active
- Source entity: `source_agents`

##### `dim_date_loader.py`
Generates and loads date dimension:
- Natural key: `date_key` (integer representation of date)
- Generates date dimension for configurable year range
- Includes day, month, year, quarter, day_of_week, hour, time_of_day
- Marks weekends and holidays
- No SCD2 (static reference data)

##### `dim_static_loader.py`
Loads static reference data:
- Channels: Support channels (app, chat, phone, email)
- Priorities: Ticket priorities (P1-P4 with SLA thresholds)
- Reasons: Ticket reasons with categories and severity levels
- No SCD2 (static reference data, full refresh)

#### Fact Loaders

##### `fact_orders_loader.py`
Loads order fact table:
- Natural key: `order_id` (UUID)
- Resolves dimension keys (customer, driver, restaurant)
- Detects orphan references
- Supports versioning for backfill
- Stores original orphan IDs for reconciliation
- Source entity: `source_orders`

##### `fact_tickets_loader.py`
Loads ticket fact table:
- Natural key: `ticket_id` (UUID)
- Resolves dimension keys (customer, driver, restaurant, agent, reason, priority, channel)
- Tracks SLA compliance (first response, resolution)
- Calculates SLA breach flags
- Stores refund amounts
- Source entity: `source_tickets`

##### `fact_events_loader.py`
Loads ticket event fact table:
- Natural key: `event_id` (UUID)
- Tracks ticket status changes
- Links to ticket and agent
- Records event timestamps and notes
- Source entity: `source_ticket_events`

#### Utility Loaders

##### `init_data_loader.py`
Initial data loader for setup:
- Loads initial static data
- Creates dimension tables if needed
- Used during database initialization

##### `verify_loaders.py`
Loader verification utility:
- Validates loader configurations
- Checks schema contracts
- Tests loader logic
- Used for testing and validation

### Loading Process

#### Dimension Loading Flow
1. Read source file (CSV/JSON)
2. Validate against schema contract
3. Quarantine invalid records
4. For each valid record:
   - Check if natural key exists
   - Detect changes in tracked fields
   - Apply SCD2 logic (insert/update/expire)
5. Track quality metrics
6. Return load result (counts, errors)

#### Fact Loading Flow
1. Read source file (CSV/JSON)
2. Validate against schema contract
3. Resolve dimension keys (check for orphans)
4. Quarantine invalid records
5. For each valid record:
   - Resolve foreign keys to surrogate keys
   - Handle orphans (assign -1, track for reconciliation)
   - Insert fact record
6. Track quality metrics
7. Return load result (counts, errors)

### Schema Validation

All loaders use the schema registry for validation:
- **Column Existence**: Ensures all required columns are present
- **Data Types**: Validates data types match schema
- **Null Constraints**: Enforces nullability rules
- **Value Ranges**: Validates numeric ranges
- **Allowed Values**: Validates categorical values
- **Regex Patterns**: Validates string formats (email, phone, UUID)

### Quality Metrics

Loaders track and report:
- Total records in file
- Valid records (passed validation)
- Quarantined records (failed validation)
- Orphaned records (missing dimension keys)
- Duplicate records
- Null violations
- Business rule violations
- Processing latency

## Relationship with Architecture

### Position in Data Flow
```
┌─────────────────┐
│  Source Files   │
│  (CSV/JSON)     │
└────────┬────────┘
         │
         │ (Loaders)
         ▼
┌─────────────────┐
│  Validation     │
│  (Schema Check) │
└────────┬────────┘
         │
         │ (Valid/Invalid)
         ▼
┌─────────────────┐
│  Warehouse      │
│  (Dimensions/   │
│   Facts)        │
└────────┬────────┘
         │
         │ (Quarantine)
         ▼
┌─────────────────┐
│  Quarantine     │
│  Table         │
└─────────────────┘
```

### Dependencies
- **warehouse/connection.py**: Database connection and query execution
- **validators/schema_registry.py**: Schema contracts for validation
- **validators/schema_validator.py**: Validation logic
- **handlers/quarantine_handler.py**: Quarantine management
- **handlers/orphan_handler.py**: Orphan detection (fact loaders)
- **utils/retry.py**: Database retry logic
- **utils/timing.py**: Performance timing

### Used By
- **pipelines/batch_pipeline.py**: Uses dimension loaders for batch loads
- **pipelines/stream_pipeline.py**: Uses fact loaders for stream loads
- **main.py**: Uses dim_date_loader for database initialization

### Integration Points
1. **Batch Pipeline**: Loads dimensions via DimCustomerLoader, DimDriverLoader, etc.
2. **Stream Pipeline**: Loads facts via fact_orders_loader, fact_tickets_loader, etc.
3. **Quality Layer**: All loaders report quality metrics
4. **Audit Trail**: All loaders log to pipeline_audit schema

## SCD2 Implementation Details

### Dimension Table Schema Pattern
```sql
CREATE TABLE dim_customer (
    customer_key    SERIAL PRIMARY KEY,
    customer_id     INTEGER,              -- Natural key
    customer_name   VARCHAR(256),
    valid_from      DATE NOT NULL,
    valid_to        DATE,
    is_current      BOOLEAN DEFAULT TRUE
);
```

### Change Detection
- Compares hash of tracked fields
- Normalizes values before comparison (bool coercion, numeric coercion, string trimming)
- Handles same-day updates (multiple changes in same batch)
- Supports SCD1 for non-tracked fields (in-place updates)

### Version Management
- New records get `valid_from = batch_date`, `valid_to = NULL`, `is_current = TRUE`
- Expired records get `valid_to = batch_date - 1`, `is_current = FALSE`
- Current records can be queried with `WHERE is_current = TRUE`

## Orphan Handling in Fact Loaders

### Orphan Detection
1. Load dimension surrogate key maps (natural key → surrogate key)
2. For each incoming fact record:
   - Resolve customer_id → customer_key (or -1 if missing)
   - Resolve driver_id → driver_key (or -1 if missing)
   - Resolve restaurant_id → restaurant_key (or -1 if missing)
3. If surrogate is -1:
   - Store original orphan ID in `original_orphan_*` column
   - Track in orphan_tracking table

### Fact Versioning
- Facts support versioning for backfill
- `version` field tracks version number
- `is_backfilled` flag indicates backfilled records
- Unique constraint on (order_id, version)

## Performance Optimizations

### Bulk Operations
- Uses `execute_values()` for bulk inserts
- Batch size configurable via `BATCH_CHUNK_SIZE`
- Prepared statements for repeated queries

### Connection Pooling
- Uses connection pool from warehouse/connection.py
- Context managers ensure proper connection cleanup
- Retry logic for transient failures

### Change Detection Optimization
- Hash comparison for efficient change detection
- Normalization to avoid false positives
- Same-day update optimization

## Error Handling

Loaders use comprehensive error handling:
- Schema validation errors are quarantined
- Database errors are retried via `@db_retry`
- File parsing errors are logged and reported
- Never fail the entire pipeline for individual record errors

## Extending Loaders

### Adding a New Dimension Loader
1. Create new loader class extending `BaseSCD2Loader`
2. Define abstract properties: `table_name`, `natural_key`, `tracked_fields`
3. Implement `_build_insert_row()` and `_build_update_fields()`
4. Set `source_entity` for schema validation
5. Add to batch pipeline in `pipelines/batch_pipeline.py`

### Adding a New Fact Loader
1. Create new loader class (no base class required)
2. Implement schema validation
3. Implement dimension key resolution
4. Implement orphan detection
5. Add to stream pipeline in `pipelines/stream_pipeline.py`

### Adding Validation Rules
1. Add validation to schema contract in `validators/schema_registry.py`
2. Loader will automatically use new validation
3. Update quality thresholds in config if needed
