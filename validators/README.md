# Validators Directory

## Definition

The `validators/` directory contains data validation components for the FastFeast data pipeline. These validators ensure data quality by enforcing schema contracts, detecting orphan references, and validating business rules.

## What It Does

The validators provide:

- **Schema Validation**: Validates data against schema contracts (column existence, data types, nullability)
- **Orphan Detection**: Detects orphan references in fact data (missing dimension keys)
- **Schema Registry**: Central repository of schema contracts for all entities
- **Business Rule Validation**: Enforces business rules and constraints
- **Error Reporting**: Provides detailed validation error messages

## Why It Exists

The validators are essential for:

- **Data Quality**: Ensuring only valid data enters the warehouse
- **Early Error Detection**: Catching data issues before they propagate
- **Consistency**: Enforcing consistent data structure across the pipeline
- **Documentation**: Schema contracts serve as executable documentation
- **Debugging**: Providing clear error messages for data issues

## How It Works

### Core Components

#### `schema_registry.py` - Schema Contract Registry
Central repository of schema contracts for all entities:
- Defines schema contracts for source and target entities
- Specifies column definitions with types, nullability, and constraints
- Provides validation rules (regex patterns, allowed values, ranges)
- Serves as single source of truth for data structure

**Key Classes:**
- `ColumnContract`: Immutable column specification (name, dtype, nullable, constraints)
- `SchemaContract`: Complete schema specification (entity, columns, natural key)

**Schema Contracts Include:**
- **Source Entities**: source_customers, source_drivers, source_restaurants, source_agents, source_orders, source_tickets, source_ticket_events
- **Warehouse Entities**: customers, drivers, restaurants, agents, orders, tickets, ticket_events
- **Reference Entities**: cities, regions, segments, categories, teams, reasons, channels, priorities
- **Audit Entities**: orphan_tracking, quarantine, pipeline_run_log, file_tracker, pipeline_quality_metrics

**Validation Rules:**
- **Data Types**: int, bigint, float, str, bool, datetime, date, numeric
- **Nullability**: Required vs optional fields
- **Allowed Values**: Categorical values (e.g., gender: male/female)
- **Regex Patterns**: Email, phone, UUID, Egyptian ID
- **Value Ranges**: Numeric min/max values

#### `schema_validator.py` - Schema Validation Engine
Validates data against schema contracts:
- Checks column existence and order
- Validates data types
- Enforces nullability constraints
- Validates allowed values and regex patterns
- Checks value ranges
- Provides detailed error messages

**Key Functions:**
- `validate_entity(df, entity_name)`: Validates DataFrame against schema contract
- `partition_critical_validation_rows(df, errors)`: Separates critical from logical errors
- `validate_column()`: Validates a single column against contract
- `validate_value()`: Validates a single value against column contract

**Error Levels:**
- **critical**: Schema violations (missing columns, type mismatches, null violations)
- **logical**: Business rule violations (value ranges, allowed values)

**Error Types:**
- missing_column: Required column not present
- type_mismatch: Data type doesn't match schema
- null_violation: Null value in non-nullable field
- value_out_of_range: Value outside allowed range
- invalid_value: Value not in allowed values
- regex_mismatch: Value doesn't match regex pattern

#### `orphan_detector.py` - Orphan Detection
Detects orphan references in fact data:
- Checks foreign key references against dimensions
- Identifies missing dimension keys
- Tracks orphan references for reconciliation
- Provides orphan statistics

**Key Functions:**
- `detect_orphans(df, dimension_map, foreign_key_columns)`: Detects orphan references
- `check_dimension_key()`: Checks if a dimension key exists
- `get_orphan_statistics()`: Returns orphan detection statistics

**Orphan Types:**
- customer: Missing customer dimension
- driver: Missing driver dimension
- restaurant: Missing restaurant dimension
- agent: Missing agent dimension

## Relationship with Architecture

### Position in Data Flow
```
┌─────────────────┐
│  Source Data    │
│  (CSV/JSON)     │
└────────┬────────┘
         │
         │ (Validate)
         ▼
┌─────────────────┐
│  Validators     │
│  (Schema Check, │
│   Orphan Detect)│
└────────┬────────┘
         │
         │ (Valid/Invalid)
         ▼
┌─────────────────┐
│  Loaders        │
│  (Load/Quarantine)│
└─────────────────┘
```

### Dependencies
- **pandas**: DataFrame operations for validation
- **dataclasses**: Schema contract definitions
- **typing**: Type hints for contracts
- **warehouse/connection.py**: Database queries for orphan detection

### Used By
- **loaders/base_scd2_loader.py**: Validates dimension data before loading
- **loaders/fact_orders_loader.py**: Validates order data and detects orphans
- **loaders/fact_tickets_loader.py**: Validates ticket data and detects orphans
- **loaders/fact_events_loader.py**: Validates event data
- **handlers/orphan_handler.py**: Uses orphan detection logic

### Integration Points
1. **Dimension Loaders**: Validate against source schema contracts
2. **Fact Loaders**: Validate against source schema contracts and detect orphans
3. **Quarantine Handler**: Quarantine records that fail validation
4. **Quality Layer**: Report validation metrics

## Schema Contract Structure

### ColumnContract
```python
@dataclass(frozen=True)
class ColumnContract:
    name: str
    dtype: DType  # int, bigint, float, str, bool, datetime, date, numeric
    nullable: bool = True
    allowed_values: Optional[Set[str]] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    regex: Optional[str] = None
    default_value: Optional[Any] = None
```

### SchemaContract
```python
@dataclass(frozen=True)
class SchemaContract:
    entity: str
    source_format: SourceFormat  # csv, json, parquet
    source_layer: SourceLayer  # batch, stream, audit
    natural_key: list[str]
    columns: list[ColumnContract]
```

## Validation Process

### Schema Validation Flow
1. Load schema contract for entity
2. Check DataFrame columns match contract
3. Validate each column's data type
4. Check nullability constraints
5. Validate allowed values (if specified)
6. Validate regex patterns (if specified)
7. Check value ranges (if specified)
8. Return validation errors

### Orphan Detection Flow
1. Load dimension surrogate key maps
2. For each fact record:
   - Check foreign key against dimension map
   - If missing: mark as orphan
3. Track orphan references
4. Return orphan statistics

## Standardized Regex Patterns

The schema registry includes standardized regex patterns:
- **UUID_REGEX**: `^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`
- **EMAIL_REGEX**: `^[^@\s]+@[^@\s]+\.[^@\s]+$`
- **PHONE_REGEX**: `^(?:[\+\d][\d\s\-\(\)]{6,20}|0[0-9]{9,11})$` (supports Egyptian formats)
- **EGYPTIAN_ID_REGEX**: `^\d{14}$`
- **TITLE_CASE_REGEX**: `^[A-Za-z][a-zA-Z]*(?:[ -][A-Za-z][a-zA-Z]*)*$`
- **ALPHANUMERIC_SPACE_REGEX**: `^[A-Za-z0-9 ]+$`

## Error Handling

### Validation Errors
Errors are returned as a list of ValidationError objects:
```python
ValidationError(
    row_index: int,
    column_name: str,
    level: str,  # critical or logical
    reason: str,
    expected: Any,
    actual: Any
)
```

### Error Partitioning
Errors are partitioned into:
- **Critical Errors**: Schema violations that prevent loading
- **Logical Errors**: Business rule violations that may allow loading

### Quarantine Decision
- Critical errors: Record quarantined
- Logical errors: Depends on configuration (may quarantine or load with warning)

## Schema Registry Usage

### Getting a Schema Contract
```python
from validators.schema_registry import get_contract

contract = get_contract("source_customers")
```

### Validating Data
```python
from validators.schema_validator import validate_entity

errors = validate_entity(df, "source_customers")
```

### Checking Schema
```python
contract = get_contract("source_customers")
required_columns = contract.required_columns()
dtype_map = contract.dtype_map()
```

## Adding New Schema Contracts

### Steps to Add New Contract
1. Define ColumnContract for each column
2. Define SchemaContract with columns
3. Add to REGISTRY dictionary in schema_registry.py
4. Add validation rules if needed
5. Update documentation

### Example
```python
NEW_ENTITY = SchemaContract(
    entity="new_entity",
    source_format="csv",
    source_layer="batch",
    natural_key=["entity_id"],
    columns=[
        ColumnContract("entity_id", "int", nullable=False, min_value=1),
        ColumnContract("entity_name", "str", regex=TITLE_CASE_REGEX),
        ColumnContract("status", "str", allowed_values={"active", "inactive"}),
    ]
)

REGISTRY["new_entity"] = NEW_ENTITY
REGISTRY["source_new_entity"] = SOURCE_NEW_ENTITY
```

## Validation Performance

### Optimization Strategies
- **Batch Validation**: Validate entire DataFrame at once
- **Vectorized Operations**: Use pandas vectorized operations
- **Early Exit**: Stop validation on critical errors
- **Caching**: Cache schema contracts

### Performance Considerations
- Large DataFrames: Consider chunking
- Complex Regex: Pre-compile patterns
- Allowed Values: Use set for O(1) lookup
- Value Ranges: Use vectorized comparisons

## Testing Validation

### Unit Tests
Test individual validation rules:
- Test type validation
- Test nullability validation
- Test regex validation
- Test range validation

### Integration Tests
Test validation with real data:
- Test with valid data
- Test with invalid data
- Test with mixed data
- Test error reporting

## Best Practices

### Schema Design
- Use appropriate data types
- Mark required fields as non-nullable
- Use regex for format validation
- Use allowed values for categorical data
- Use ranges for numeric validation

### Validation Strategy
- Validate early in pipeline
- Quarantine invalid data
- Log validation errors
- Report validation metrics
- Monitor validation rates

### Error Messages
- Provide clear error messages
- Include expected vs actual values
- Include row index for debugging
- Suggest fixes if possible

## Extending Validators

### Adding New Validation Rules
1. Add rule to ColumnContract
2. Implement validation in schema_validator.py
3. Add tests
4. Update documentation

### Adding New Regex Patterns
1. Add pattern constant to schema_registry.py
2. Add to appropriate ColumnContract
3. Test pattern with valid/invalid data
4. Document pattern purpose

### Adding New Error Types
1. Add error type to ValidationError
2. Implement detection logic
3. Add error handling in loaders
4. Update quarantine handling
