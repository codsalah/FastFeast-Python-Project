# Utils Directory

## Definition

The `utils/` directory contains utility modules that provide common functionality used across the FastFeast data pipeline. These utilities handle logging, file operations, configuration loading, retry logic, timing, PII handling, and other cross-cutting concerns.

## What It Does

The utils directory provides:

- **Logging**: Structured logging with context and severity levels
- **File Operations**: File tracking, hashing, and utility functions
- **Configuration Loading**: Loading configuration from various sources
- **Retry Logic**: Automatic retry for transient failures
- **Timing**: Performance timing and measurement
- **PII Handling**: Hashing and protection of personally identifiable information
- **Date Utilities**: Date and time manipulation functions

## Why It Exists

The utils directory is essential for:

- **Code Reuse**: Providing common functionality across the codebase
- **Separation of Concerns**: Keeping utility code separate from business logic
- **Consistency**: Ensuring consistent behavior across components
- **Maintainability**: Centralizing common functionality for easier maintenance
- **Testing**: Making utility code easier to test in isolation

## How It Works

### Core Components

#### `logger.py` - Logging Configuration
Configures structured logging for the application:
- Sets up logging with structlog for structured output
- Configures log levels (DEBUG, INFO, WARNING, ERROR)
- Adds context to log messages
- Supports file and console output
- Provides helper functions for logging

**Key Functions:**
- `configure_logging(log_dir, level)`: Configures logging with directory and level
- `get_logger_name(name)`: Returns a logger instance with the given name
- `log_alert_fired()`: Specialized logging for alert events

**Log Levels:**
- DEBUG: Detailed diagnostic information
- INFO: General informational messages
- WARNING: Warning messages for potential issues
- ERROR: Error messages for failures

#### `file_tracker.py` - File Tracking
Tracks processed files to ensure idempotency:
- Computes SHA-256 hashes of files
- Tracks file processing status in database
- Prevents duplicate processing of same files
- Provides file history and audit trail

**Key Functions:**
- `compute_file_hash(file_path)`: Computes SHA-256 hash of a file
- `is_file_processed(file_path, file_hash)`: Checks if file was already processed
- `register_file(run_id, file_path, file_hash, file_type)`: Registers file for processing
- `mark_file_success(file_path, file_hash, records_total, records_loaded, records_quarantined)`: Marks file as successfully processed
- `mark_file_failed(file_path, file_hash, error_message)`: Marks file as failed

**File Status:**
- processing: File is currently being processed
- success: File was processed successfully
- failed: File processing failed

#### `file_utils.py` - File Operations
Provides common file operation utilities:
- File existence checks
- Directory creation
- File reading and writing
- Path manipulation

**Key Functions:**
- `ensure_directory(path)`: Creates directory if it doesn't exist
- `read_file(file_path)`: Reads file contents
- `write_file(file_path, content)`: Writes content to file
- `file_exists(file_path)`: Checks if file exists

#### `config_loader.py` - Configuration Loading
Loads configuration from various sources:
- YAML configuration files
- Environment variables
- Default values
- Configuration validation

**Key Functions:**
- `load_config(config_path)`: Loads configuration from YAML file
- `load_env_config()`: Loads configuration from environment variables
- `merge_configs(*configs)`: Merges multiple configuration sources

#### `retry.py` - Retry Logic
Provides automatic retry for transient failures:
- Database connection retries
- Network operation retries
- Configurable retry count and backoff
- Exponential backoff strategy

**Key Functions:**
- `@db_retry`: Decorator for database operation retries
- `retry_with_backoff()`: Generic retry with exponential backoff
- `retry_on_exception()`: Retry on specific exceptions

**Retry Strategy:**
- Default retry count: 3
- Backoff strategy: Exponential
- Retry on: Connection errors, timeouts, deadlocks

#### `timing.py` - Performance Timing
Measures and tracks performance metrics:
- Function execution time
- Block timing
- Performance logging
- Timing statistics

**Key Functions:**
- `@timed`: Decorator to time function execution
- `Timer`: Context manager for timing code blocks
- `log_timing()`: Logs timing information

**Timing Output:**
- Function name
- Execution time in seconds
- Additional context if provided

#### `PII_handler.py` - PII Protection
Handles personally identifiable information:
- Hashing of sensitive fields
- Pepper-based hashing for security
- Non-reversible hashing
- PII detection and masking

**Key Functions:**
- `hash_pii(value, pepper)`: Hashes PII value with pepper
- `mask_pii(value)`: Masks PII value for display
- `is_pii_field(field_name)`: Checks if field is PII

**Hashing Method:**
- Uses SHA-256 with pepper
- Pepper from environment variable (PII_HASH_PEPPER)
- Non-reversible (one-way hash)
- Consistent hashing for same input

#### `date_utils.py` - Date Utilities
Provides date and time manipulation functions:
- Date parsing and formatting
- Date arithmetic
- Timezone handling
- Date validation

**Key Functions:**
- `parse_date(date_string)`: Parses date string to datetime
- `format_date(datetime_obj)`: Formats datetime to string
- `add_days(date, days)`: Adds days to date
- `is_valid_date(date_string)`: Validates date string

#### `readers.py` - File Readers
Provides file reading utilities:
- CSV reading with options
- JSON reading with validation
- Error handling for malformed files
- Encoding detection

**Key Functions:**
- `read_csv(file_path, **kwargs)`: Reads CSV file with options
- `read_json(file_path)`: Reads JSON file with validation
- `detect_encoding(file_path)`: Detects file encoding

## Relationship with Architecture

### Position in Data Flow
```
┌─────────────────┐
│  All Modules    │
│  (pipelines,    │
│   loaders,      │
│   handlers,     │
│   etc.)         │
└────────┬────────┘
         │
         │ (Use)
         ▼
┌─────────────────┐
│  utils/         │
│  (Shared        │
│   Utilities)    │
└────────┬────────┘
         │
         │ (Interact with)
         ▼
┌─────────────────┐
│  File System,   │
│  Database,      │
│  Logging,       │
│  etc.           │
└─────────────────┘
```

### Dependencies
- **structlog**: Structured logging
- **hashlib**: File hashing
- **pydantic**: Configuration validation
- **psycopg2**: Database operations (for file_tracker)

### Used By
- **pipelines/**: All pipelines use logging, retry, and timing
- **loaders/**: Loaders use file tracking, retry, and PII handling
- **handlers/**: Handlers use retry and logging
- **quality/**: Quality layer uses file tracking and logging
- **validators/**: Validators use logging
- **warehouse/**: Warehouse uses logging and retry

### Integration Points
1. **Logging**: All modules use logger.py for structured logging
2. **File Tracking**: Loaders and pipelines use file_tracker.py
3. **Retry**: Database operations use retry.py
4. **Timing**: Loaders use timing.py for performance measurement
5. **PII**: Loaders use PII_handler.py for sensitive data

## Logging Configuration

### Log Levels
- **DEBUG**: Detailed diagnostic information (development)
- **INFO**: General informational messages (production default)
- **WARNING**: Warning messages for potential issues
- **ERROR**: Error messages for failures

### Log Format
Structured logs include:
- Timestamp
- Log level
- Logger name
- Message
- Context (key-value pairs)

### Log Output
- Console: Colored output for development
- File: Rotating log files for production
- Log directory: Configurable via LOG_DIR environment variable

## File Tracking

### Hash Computation
- Algorithm: SHA-256
- Purpose: Detect file changes and prevent duplicate processing
- Usage: Computed before processing, stored in database

### File Status Tracking
- **processing**: File is being processed
- **success**: File processed successfully
- **failed**: File processing failed

### Idempotency
- Files with same hash are skipped
- Prevents duplicate processing
- Ensures consistent results

## Retry Logic

### Database Retry Decorator
```python
@db_retry
def database_operation():
    # Database operation here
    pass
```

### Retry Configuration
- Default retry count: 3
- Backoff: Exponential
- Retry on: Connection errors, timeouts, deadlocks

### Retry Strategy
1. Attempt operation
2. If fails with retryable exception
3. Wait with exponential backoff
4. Retry up to max attempts
5. Raise exception if all retries fail

## PII Handling

### Hashing Method
- Algorithm: SHA-256
- Input: Value + pepper
- Pepper: From PII_HASH_PEPPER environment variable
- Output: Hexadecimal hash string

### PII Fields
Common PII fields:
- Names (customer_name, driver_name, agent_name)
- Email addresses
- Phone numbers
- National IDs
- Addresses

### Security
- Pepper must be kept secret
- Hashing is non-reversible
- Pepper should be at least 16 characters
- Generate pepper with: `python -c "import secrets; print(secrets.token_hex(32))"`

## Performance Timing

### Timed Decorator
```python
@timed
def function_to_time():
    # Function code here
    pass
```

### Timer Context Manager
```python
with Timer("operation_name"):
    # Code to time
    pass
```

### Timing Output
- Operation name
- Execution time in seconds
- Additional context if provided

## Configuration

### Environment Variables
- `LOG_DIR`: Directory for log files
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `PII_HASH_PEPPER`: Pepper for PII hashing

### Configuration Loading Priority
1. Environment variables (highest)
2. Configuration files
3. Default values (lowest)

## Best Practices

### Using Logging
- Use appropriate log levels
- Include context in log messages
- Use structured logging for complex data
- Avoid logging sensitive data (use PII masking)

### Using File Tracking
- Always compute hash before processing
- Register file before processing
- Mark success/failure after processing
- Check if file already processed

### Using Retry
- Use @db_retry for database operations
- Configure retry count appropriately
- Handle non-retryable exceptions separately
- Log retry attempts

### Using PII Handling
- Hash PII before storing
- Never log raw PII
- Use consistent pepper across application
- Generate strong pepper (64 hex chars)

## Extending Utils

### Adding New Utility
1. Create new module in utils/
2. Implement utility functions
3. Add error handling
4. Add documentation
5. Add tests
6. Update this README

### Adding New Log Context
1. Add context field to logger configuration
2. Update log formatting
3. Document new context field
4. Update examples

### Adding New Retry Strategy
1. Add new retry function in retry.py
2. Configure backoff strategy
3. Add tests
4. Document strategy
