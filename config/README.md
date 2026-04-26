# Config Directory

## Definition

The `config/` directory contains configuration management for the FastFeast data pipeline, including environment variable loading, validation, and type-safe configuration settings using Pydantic.

## What It Does

The configuration layer provides:

- **Environment Variable Loading**: Loads configuration from `.env` file using Pydantic Settings
- **Type Validation**: Ensures all configuration values are of the correct type
- **Default Values**: Provides sensible defaults for optional settings
- **Validation Rules**: Enforces business rules on configuration values (e.g., port ranges, percentage ranges)
- **Nested Configuration**: Organizes related settings into logical groups (database, SLA, alerting, quality)

## Why It Exists

The configuration layer is essential for:

- **Security**: Keeps sensitive credentials (database passwords, SMTP passwords) out of code
- **Flexibility**: Allows different configurations for development, staging, and production
- **Validation**: Catches configuration errors early at startup rather than during pipeline execution
- **Type Safety**: Uses Pydantic to ensure configuration values are correctly typed
- **Maintainability**: Centralizes all configuration in one place with clear structure

## How It Works

### Core Components

#### `settings.py` - Main Configuration Module
Contains all configuration classes using Pydantic BaseSettings:
- `DatabaseConfig`: PostgreSQL connection settings
- `SLAConfig`: SLA breach thresholds
- `AlertConfig`: SMTP alerting configuration
- `QualityThresholdConfig`: Data quality gate thresholds
- `Settings`: Top-level configuration that composes all sub-configs

#### `config.yaml` - YAML Configuration
Additional configuration file for settings that are better expressed in YAML format (currently minimal usage).

### Configuration Classes

#### DatabaseConfig
PostgreSQL connection settings:
- `host`: Database host (default: localhost)
- `port`: Database port (default: 5432, validated 1-65535)
- `name`: Database name (default: fastfeast_db)
- `user`: Database user (default: fastfeast)
- `password`: Database password (default: fastfeast_pass)
- `pool_min`: Minimum connection pool size (default: 2)
- `pool_max`: Maximum connection pool size (default: 10, validated >= 1)
- `dsn`: Computed property for PostgreSQL DSN string

#### SLAConfig
Service Level Agreement thresholds:
- `response_threshold_seconds`: First response SLA threshold (default: 60)
- `resolution_threshold_seconds`: Resolution SLA threshold (default: 900)
- `breach_alert_threshold_pct`: Breach rate alert threshold (default: 0.10, validated 0-1)

#### AlertConfig
Email alerting configuration:
- `enabled`: Enable/disable alerting (default: true)
- `smtp_host`: SMTP server host (default: smtp.gmail.com)
- `smtp_port`: SMTP server port (default: 587)
- `smtp_user`: SMTP username
- `smtp_password`: SMTP password
- `sender_name`: Email sender name (default: "FastFeast Pipeline")
- `alert_recipients`: List of alert email recipients (parsed from CSV string)
- `report_recipients`: List of report email recipients (parsed from CSV string)
- `orphan_rate_threshold`: Orphan rate alert threshold (default: 0.50)
- `error_rate_threshold`: Error rate alert threshold (default: 0.10)

#### QualityThresholdConfig
Data quality gate thresholds:
- `max_null_rate`: Maximum acceptable null rate (default: 0.05)
- `max_duplicate_rate`: Maximum acceptable duplicate rate (default: 0.01)
- `max_orphan_rate`: Maximum acceptable orphan rate (default: 0.05)
- `min_integrity_rate`: Minimum acceptable integrity rate (default: 0.95)
- `min_file_success_rate`: Minimum file success rate (default: 0.90)

#### Settings
Top-level configuration:
- **Directories**: batch_input_dir, stream_input_dir, quarantine_dir, processed_dir, log_dir
- **Pipeline Behavior**: poll_interval_seconds, batch_chunk_size, max_threads, log_level
- **Watcher Scheduling**: stream_poll_seconds, batch_poll_seconds, batch_window_start_hour, batch_window_end_hour, batch_required_files
- **Warehouse Defaults**: date_dim_start_year, date_dim_end_year, db_connect_timeout_sec, db_statement_timeout_ms
- **PII**: pii_hash_pepper (required, validated to be >= 16 chars and not default placeholder)
- **Sub-configs**: db, sla, alert, quality (composed from above classes)

### Key Functions

#### `get_settings()`
Returns a cached Settings instance using `@lru_cache(maxsize=1)`. This ensures configuration is loaded only once and reused throughout the application.

#### `ensure_directories()`
Creates all required data directories if they don't exist:
- batch_input_dir
- stream_input_dir
- quarantine_dir
- processed_dir
- log_dir

### Validation Rules

#### Port Validation
Ports must be in range 1-65535.

#### Percentage Validation
Percentage values must be in range [0, 1].

#### PII Pepper Validation
PII_HASH_PEPPER must be at least 16 characters and cannot contain "CHANGE_ME" (prevents using default placeholder).

#### Hour Validation
Hour values must be in range [0, 23].

#### Pool Size Validation
pool_max must be >= 1.

### Environment Variable Loading

Configuration is loaded from `.env` file with the following precedence:
1. Environment variables (highest priority)
2. `.env` file
3. Default values (lowest priority)

Environment variable naming follows the pattern:
- Nested configs use uppercase prefixes (e.g., `POSTGRES_HOST`, `SMTP_PORT`)
- Top-level settings use uppercase names (e.g., `BATCH_INPUT_DIR`, `LOG_LEVEL`)

## Relationship with Architecture

### Architecture Diagram

```mermaid
graph TB
    subgraph "Configuration Sources"
        ENV[.env File]
        EV[Environment Variables]
        DF[Default Values]
    end

    subgraph "Configuration Layer"
        subgraph "Pydantic Settings"
            BS[BaseSettings]
            PS[pydantic_settings]
        end
        subgraph "Config Classes"
            DC[DatabaseConfig]
            SC[SLAConfig]
            AC[AlertConfig]
            QC[QualityThresholdConfig]
            TS[Top-level Settings]
        end
        subgraph "Functions"
            GS[get_settings<br/>@lru_cache]
            ED[ensure_directories]
        end
    end

    subgraph "Application Modules"
        MP[main.py]
        WC[warehouse/connection.py]
        AS[alerting/alert_service.py]
        QR[quality/quality_report.py]
        BP[pipelines/batch_pipeline.py]
        SP[pipelines/stream_pipeline.py]
        AM[All Other Modules]
    end

    subgraph "Validation"
        PV[Port Validation<br/>1-65535]
        PCTV[Percentage Validation<br/>0-1]
        PHV[PII Pepper Validation<br/>>=16 chars]
        HV[Hour Validation<br/>0-23]
        PSV[Pool Size Validation<br/>>=1]
    end

    ENV --> BS
    EV --> BS
    DF --> BS
    BS --> DC
    BS --> SC
    BS --> AC
    BS --> QC
    DC --> TS
    SC --> TS
    AC --> TS
    QC --> TS

    TS --> GS
    TS --> ED

    DC --> PV
    DC --> PSV
    SC --> PCTV
    AC --> PCTV
    QC --> PCTV
    TS --> PHV
    TS --> HV

    MP --> GS
    WC -->|db config| GS
    AS -->|alert config| GS
    QR -->|quality config| GS
    BP -->|dirs & scheduling| GS
    SP -->|dirs & polling| GS
    AM --> GS

    GS --> TS

    style ENV fill:#4ecdc4
    style TS fill:#ff6b6b
    style GS fill:#ffe66d
    style DC fill:#95e1d3
    style SC fill:#95e1d3
    style AC fill:#95e1d3
    style QC fill:#95e1d3
```

### Dependencies
- **pydantic**: For type validation and settings management
- **pydantic-settings**: For loading from environment variables

### Used By
- **main.py**: Loads settings at startup, passes to pipeline components
- **warehouse/connection.py**: Uses database configuration for connection pool
- **alerting/alert_service.py**: Uses alerting configuration for SMTP
- **quality/quality_report.py**: Uses quality thresholds for validation
- **pipelines/batch_pipeline.py**: Uses directory and scheduling configuration
- **pipelines/stream_pipeline.py**: Uses directory and polling configuration
- **All modules**: Access settings via `get_settings()`

### Integration Points
1. **Application Startup**: Configuration loaded once at startup in `main.py`
2. **Database Connection**: Connection pool initialized with database config
3. **Pipeline Execution**: All pipeline components use configuration for behavior
4. **Alerting**: Email service configured via alert config
5. **Quality Gates**: Validation thresholds from quality config

## Configuration Example

### .env File Example
```bash
# Database
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=fastfeast_db
POSTGRES_USER=fastfeast
POSTGRES_PASSWORD=fastfeast_pass
DB_POOL_MIN=2
DB_POOL_MAX=10

# Directories
BATCH_INPUT_DIR=data/input/batch
STREAM_INPUT_DIR=data/input/stream
QUARANTINE_DIR=data/quarantine
PROCESSED_DIR=data/processed
LOG_DIR=logs

# Pipeline Behavior
POLL_INTERVAL_SECONDS=30
BATCH_CHUNK_SIZE=10000
MAX_THREADS=4
LOG_LEVEL=INFO

# Watcher Scheduling
STREAM_POLL_SECONDS=30
BATCH_POLL_SECONDS=30
BATCH_WINDOW_START_HOUR=23
BATCH_WINDOW_END_HOUR=1

# SLA Thresholds
SLA_RESPONSE_THRESHOLD_SECONDS=60
SLA_RESOLUTION_THRESHOLD_SECONDS=900
SLA_BREACH_ALERT_THRESHOLD_PCT=0.10

# Alerting
ALERTING_ENABLED=true
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your-email@gmail.com
SMTP_PASSWORD=your-app-password
SENDER_NAME=FastFeast Pipeline
ALERT_RECIPIENTS=ops@example.com,data-team@example.com
REPORT_RECIPIENTS=quality@example.com
MAX_ORPHAN_RATE=0.50
MAX_ERROR_RATE=0.10

# Quality Thresholds
MAX_NULL_RATE=0.05
MAX_DUPLICATE_RATE=0.01
MAX_ORPHAN_RATE=0.05
MIN_INTEGRITY_RATE=0.95
MIN_FILE_SUCCESS_RATE=0.90

# Warehouse Defaults
DATE_DIM_START_YEAR=2020
DATE_DIM_END_YEAR=2030
DB_CONNECT_TIMEOUT_SEC=10
DB_STATEMENT_TIMEOUT_MS=240000

# PII
PII_HASH_PEPPER=<generate with: python -c "import secrets; print(secrets.token_hex(32))">
```

## Security Considerations

- **Sensitive Data**: Database passwords, SMTP passwords, and PII pepper must be kept secret
- **.env File**: Should never be committed to version control (in .gitignore)
- **PII Pepper**: Must be a strong random value (min 16 chars, recommended 64 hex chars)
- **Validation**: Configuration validation prevents insecure defaults (e.g., placeholder pepper)

## Adding New Configuration

### Adding a New Setting
1. Add the field to the appropriate config class in `settings.py`
2. Add validation using `@field_validator` if needed
3. Add the environment variable to `.env.example`
4. Update documentation

### Adding a New Config Class
1. Create a new class inheriting from `BaseSettings`
2. Add `model_config` with `env_file=".env"` and other settings
3. Add fields with appropriate types and defaults
4. Add validation as needed
5. Compose the new class into the top-level `Settings` class

## Testing Configuration

Configuration can be tested by:
1. Creating a test `.env` file with test values
2. Calling `get_settings()` and asserting expected values
3. Testing validation by providing invalid values
4. Testing default values by omitting environment variables
