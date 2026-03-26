"""
Single Source of Truth for all input entity's contracts.

Each SchemaContract defines:
- Required fields and their types 
- Nullability constraints (Primary Keys are NOT nullable)
- Categorical evaluations (via allowed_values or regex)
- Numerical range boundaries 
- Standardized regex patterns for identifiers (UUIDs, PII)
- Natural keys for deduplication
- Default values for missing critical IDs (e.g., -1 for orphans)

Used by:
- schema_validator
- business_rules_validator
- deduplicator
- pipeline_audit
"""

from dataclasses import dataclass, field
from typing import Optional, Literal, Set, Any

# Strict Type Definitions 
DType = Literal["int", "bigint", "float", "str", "bool", "datetime", "date", "numeric"]
SourceFormat = Literal["csv", "json", "parquet"]
SourceLayer = Literal["batch", "stream", "audit"]

# Standardized Regex Patterns 
UUID_REGEX = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
EMAIL_REGEX = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
PHONE_REGEX = r"^(010|011|012|015)\d{8}$"
EGYPTIAN_ID_REGEX = r"^\d{14}$"
TITLE_CASE_REGEX = r"^[A-Z][a-z]+(?: [A-Z][a-z]*)*$"
ALPHANUMERIC_SPACE_REGEX = r"^[A-Za-z0-9 ]+$"


@dataclass(frozen=True)
class ColumnContract:
    """Immutable column specifications."""
    name: str
    dtype: DType
    nullable: bool = True
    allowed_values: Optional[Set[str]] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    regex: Optional[str] = None
    default_value: Optional[Any] = None


@dataclass(frozen=True)
class SchemaContract:
    """ Think of it like full expectation for a table/input file"""
    entity: str
    source_format: SourceFormat
    source_layer: SourceLayer
    natural_key: list[str]
    columns: list[ColumnContract]

    def required_columns(self) -> list[str]:
        return [c.name for c in self.columns if not c.nullable]

    def nullable_columns(self) -> list[str]:
        return [c.name for c in self.columns if c.nullable]

    def get_column(self, column_name: str) -> Optional[ColumnContract]:
        return next((c for c in self.columns if c.name == column_name), None)

    def dtype_map(self) -> dict[str, str]:
        return {c.name: c.dtype for c in self.columns}

    def categorical_columns(self) -> list[ColumnContract]:
        return [c for c in self.columns if c.allowed_values is not None or c.regex is not None]

    def numeric_range_columns(self) -> list[ColumnContract]:
        return [c for c in self.columns if c.min_value is not None or c.max_value is not None]


# ------------------------------------------------------------------------- #
# ------- Batch reference / lookup tables (static, delivered as is) ------- #
# ------------------------------------------------------------------------- #

CITIES = SchemaContract(
    entity="cities",
    source_format="json",
    source_layer="batch",
    natural_key=["city_id"],
    columns=[
        ColumnContract("city_id", "int", nullable=False, min_value=1),
        ColumnContract("city_name", "str", regex=TITLE_CASE_REGEX),
        ColumnContract("country", "str", allowed_values={"Egypt"}), # That is our business for NOW
    ],
)

REGIONS = SchemaContract(
    entity="regions",
    source_format="csv",
    source_layer="batch",
    natural_key=["region_id"],
    columns=[
        ColumnContract("region_id", "int", nullable=False, min_value=1),
        ColumnContract("region_name", "str", regex=ALPHANUMERIC_SPACE_REGEX),
        ColumnContract("city_id", "int", min_value=1),
        ColumnContract("delivery_base_fee", "float", min_value=0.0, max_value=500.0),
    ],
)

SEGMENTS = SchemaContract(
    entity="segments",
    source_format="csv",
    source_layer="batch",
    natural_key=["segment_id"],
    columns=[
        ColumnContract("segment_id", "int", nullable=False, min_value=1),
        ColumnContract("segment_name", "str", allowed_values={"Regular", "VIP"}),
        ColumnContract("discount_pct", "int", min_value=0, max_value=100),
        ColumnContract("priority_support", "bool"),
    ],
)

CATEGORIES = SchemaContract(
    entity="categories",
    source_format="csv",
    source_layer="batch",
    natural_key=["category_id"],
    columns=[
        ColumnContract("category_id", "int", nullable=False, min_value=1),
        ColumnContract("category_name", "str", regex=TITLE_CASE_REGEX),
    ],
)

TEAMS = SchemaContract(
    entity="teams",
    source_format="csv",
    source_layer="batch",
    natural_key=["team_id"],
    columns=[
        ColumnContract("team_id", "int", nullable=False, min_value=1),
        ColumnContract("team_name", "str", regex=TITLE_CASE_REGEX),
    ],
)

REASON_CATEGORIES = SchemaContract(
    entity="reason_categories",
    source_format="csv",
    source_layer="batch",
    natural_key=["reason_category_id"],
    columns=[
        ColumnContract("reason_category_id", "int", nullable=False, min_value=1),
        ColumnContract("category_name", "str", allowed_values={"Delivery", "Food", "Payment"}),
    ],
)

REASONS = SchemaContract(
    entity="reasons",
    source_format="csv",
    source_layer="batch",
    natural_key=["reason_id"],
    columns=[
        ColumnContract("reason_id", "int", nullable=False, min_value=1),
        ColumnContract("reason_name", "str"),
        ColumnContract("reason_category_id", "int", min_value=1),
        ColumnContract("severity_level", "int", min_value=1, max_value=5),
        ColumnContract("typical_refund_pct", "float", min_value=0.0, max_value=1.0),
    ],
)

CHANNELS = SchemaContract(
    entity="channels",
    source_format="csv",
    source_layer="batch",
    natural_key=["channel_id"],
    columns=[
        ColumnContract("channel_id", "int", nullable=False, min_value=1),
        ColumnContract("channel_name", "str", allowed_values={"app", "chat", "phone", "email"}),
    ],
)

PRIORITIES = SchemaContract(
    entity="priorities",
    source_format="csv",
    source_layer="batch",
    natural_key=["priority_id"],
    columns=[
        ColumnContract("priority_id", "int", nullable=False, min_value=1),
        ColumnContract("priority_code", "str", allowed_values={"P1", "P2", "P3", "P4"}),
        ColumnContract("priority_name", "str", allowed_values={"Critical", "High", "Medium", "Low"}),
        ColumnContract("sla_first_response_min", "int", min_value=1),
        ColumnContract("sla_resolution_min", "int", min_value=1),
    ],
)

# ------------------------------------------------------------------------- #
# - Batch drift tables ---------------------------------------------------- #
# ------------------------------------------------------------------------- #

CUSTOMERS = SchemaContract(
    entity="customers",
    source_format="csv",
    source_layer="batch",
    natural_key=["customer_id"],
    columns=[
        ColumnContract("customer_id", "int", nullable=False, min_value=-1), # -1 for orphan/default
        ColumnContract("full_name", "str", regex=TITLE_CASE_REGEX),
        ColumnContract("email", "str", regex=EMAIL_REGEX),
        ColumnContract("phone", "str", regex=PHONE_REGEX),
        ColumnContract("region_id", "int", min_value=1),
        ColumnContract("segment_id", "int", min_value=1),
        ColumnContract("signup_date", "datetime"),
        ColumnContract("gender", "str", allowed_values={"male", "female"}),
        ColumnContract("created_at", "datetime"),
        ColumnContract("updated_at", "datetime"),
    ],
)

RESTAURANTS = SchemaContract(
    entity="restaurants",
    source_format="json",
    source_layer="batch",
    natural_key=["restaurant_id"],
    columns=[
        ColumnContract("restaurant_id", "int", nullable=False, min_value=-1), # -1 for orphan/default
        ColumnContract("restaurant_name", "str"),
        ColumnContract("region_id", "int", min_value=1),
        ColumnContract("category_id", "int", min_value=1),
        ColumnContract("price_tier", "str", allowed_values={"Low", "Mid", "High"}),
        ColumnContract("rating_avg", "float", min_value=1.0, max_value=5.0),
        ColumnContract("prep_time_avg_min", "int", min_value=1, max_value=120),
        ColumnContract("is_active", "bool"),
        ColumnContract("created_at", "datetime"),
        ColumnContract("updated_at", "datetime"),
    ],
)

DRIVERS = SchemaContract(
    entity="drivers",
    source_format="csv",
    source_layer="batch",
    natural_key=["driver_id"],
    columns=[
        ColumnContract("driver_id", "int", nullable=False, min_value=-1), # -1 for orphan/default
        ColumnContract("driver_name", "str", regex=TITLE_CASE_REGEX),
        ColumnContract("driver_phone", "str", regex=PHONE_REGEX),
        ColumnContract("national_id", "str", regex=EGYPTIAN_ID_REGEX),
        ColumnContract("region_id", "int", min_value=1),
        ColumnContract("shift", "str", allowed_values={"morning", "evening", "night"}),
        ColumnContract("vehicle_type", "str", allowed_values={"bike", "motorbike", "car"}),
        ColumnContract("hire_date", "datetime"),
        ColumnContract("rating_avg", "float", min_value=1.0, max_value=5.0),
        ColumnContract("on_time_rate", "float", min_value=0.0, max_value=1.0),
        ColumnContract("cancel_rate", "float", min_value=0.0, max_value=1.0),
        ColumnContract("completed_deliveries", "int", min_value=0),
        ColumnContract("is_active", "bool"),
        ColumnContract("created_at", "datetime"),
        ColumnContract("updated_at", "datetime"),
    ],
)

AGENTS = SchemaContract(
    entity="agents",
    source_format="csv",
    source_layer="batch",
    natural_key=["agent_id"],
    columns=[
        ColumnContract("agent_id", "int", nullable=False, min_value=1),
        ColumnContract("agent_name", "str", regex=TITLE_CASE_REGEX),
        ColumnContract("agent_email", "str", regex=EMAIL_REGEX),
        ColumnContract("agent_phone", "str", regex=PHONE_REGEX),
        ColumnContract("team_id", "int", min_value=1),
        ColumnContract("skill_level", "str", allowed_values={"Junior", "Mid", "Senior", "Lead"}),
        ColumnContract("hire_date", "datetime"),
        ColumnContract("avg_handle_time_min", "int", min_value=1, max_value=120),
        ColumnContract("resolution_rate", "float", min_value=0.0, max_value=1.0),
        ColumnContract("csat_score", "float", min_value=1.0, max_value=5.0),
        ColumnContract("is_active", "bool"),
        ColumnContract("created_at", "datetime"),
        ColumnContract("updated_at", "datetime"),
    ],
)

# ------------------------------------------------------------------------- #
# ------------------------ Mini Batch (Stream) Data ----------------------- #
# ------------------------------------------------------------------------- #

ORDERS = SchemaContract(
    entity="orders",
    source_format="json",
    source_layer="stream",
    natural_key=["order_id"],
    columns=[
        ColumnContract("order_id", "str", nullable=False, regex=UUID_REGEX),
        ColumnContract("customer_id", "int", min_value=-1, default_value=-1),
        ColumnContract("restaurant_id", "int", min_value=-1, default_value=-1),
        ColumnContract("driver_id", "int", min_value=-1, default_value=-1),
        ColumnContract("region_id", "int", min_value=1),
        ColumnContract("order_amount", "float", min_value=0.0, max_value=20000.0),
        ColumnContract("delivery_fee", "float", min_value=0.0, max_value=1000.0),
        ColumnContract("discount_amount", "float", min_value=0.0),
        ColumnContract("total_amount", "float", min_value=0.0, max_value=21000.0),
        ColumnContract("order_status", "str", allowed_values={"Placed", "Preparing", "PickedUp", "Delivered", "Cancelled", "Refunded"}),
        ColumnContract("payment_method", "str", allowed_values={"card", "cash", "wallet"}),
        ColumnContract("order_created_at", "datetime"),
        ColumnContract("delivered_at", "datetime"),
    ],
)

TICKETS = SchemaContract(
    entity="tickets",
    source_format="csv",
    source_layer="stream",
    natural_key=["ticket_id"],
    columns=[
        ColumnContract("ticket_id", "str", nullable=False, regex=UUID_REGEX),
        ColumnContract("order_id", "str", nullable=False, regex=UUID_REGEX),
        ColumnContract("customer_id", "int", min_value=-1, default_value=-1),
        ColumnContract("driver_id", "int", min_value=-1, default_value=-1),
        ColumnContract("restaurant_id", "int", min_value=-1, default_value=-1),
        ColumnContract("agent_id", "int", min_value=1),
        ColumnContract("reason_id", "int", min_value=1),
        ColumnContract("priority_id", "int", min_value=1),
        ColumnContract("channel_id", "int", min_value=1),
        ColumnContract("status", "str", allowed_values={"Resolved", "Closed", "Reopened", "Open", "InProgress"}),
        ColumnContract("refund_amount", "float", min_value=0.0),
        ColumnContract("created_at", "datetime"),
        ColumnContract("first_response_at", "datetime"),
        ColumnContract("resolved_at", "datetime"),
        ColumnContract("sla_first_due_at", "datetime"),
        ColumnContract("sla_resolve_due_at", "datetime"),
    ],
)

TICKET_EVENTS = SchemaContract(
    entity="ticket_events",
    source_format="json",
    source_layer="stream",
    natural_key=["event_id"],
    columns=[
        ColumnContract("event_id", "str", nullable=False, regex=UUID_REGEX),
        ColumnContract("ticket_id", "str", nullable=False, regex=UUID_REGEX),
        ColumnContract("agent_id", "int", min_value=1),
        ColumnContract("event_ts", "datetime"),
        ColumnContract("old_status", "str", allowed_values={"Open", "InProgress", "Resolved", "Closed", "Reopened"}),
        ColumnContract("new_status", "str", allowed_values={"Open", "InProgress", "Resolved", "Closed", "Reopened"}),
        ColumnContract("notes", "str"),
    ],
)

# ------------------------------------------------------------------------- #
# ------------------------- Audit Schema Contracts ------------------------ #
# ------------------------------------------------------------------------- #

ORPHAN_TRACKING = SchemaContract(
    entity="orphan_tracking",
    source_format="parquet",
    source_layer="audit",
    natural_key=["tracking_id"],
    columns=[
        ColumnContract("tracking_id", "int", nullable=False, min_value=1),
        ColumnContract("order_id", "str", nullable=False),
        ColumnContract("orphan_type", "str", nullable=False, allowed_values={"customer", "driver", "restaurant"}),
        ColumnContract("raw_id", "int", nullable=False),
        ColumnContract("is_resolved", "bool", nullable=False, default_value=False),
        ColumnContract("retry_count", "int", nullable=False, default_value=0, max_value=3),
        ColumnContract("detected_at", "datetime", nullable=False),
        ColumnContract("resolved_at", "datetime", nullable=True),
    ],
)

QUARANTINE = SchemaContract(
    entity="quarantine",
    source_format="parquet",
    source_layer="audit",
    natural_key=["quarantine_id"],
    columns=[
        ColumnContract("quarantine_id", "int", nullable=False, min_value=1),
        ColumnContract("source_file", "str", nullable=False),
        ColumnContract("entity_type", "str", nullable=False),
        ColumnContract("raw_record", "str", nullable=False),  # jsonb stored as raw str
        ColumnContract("error_type", "str", nullable=False, allowed_values={"schema_validation", "orphan", "parse_error", "referential_integrity"}),
        ColumnContract("error_details", "str", nullable=False),
        ColumnContract("orphan_type", "str", nullable=True),
        ColumnContract("raw_orphan_id", "str", nullable=True),
        ColumnContract("pipeline_run_id", "int", nullable=True),
        ColumnContract("quarantined_at", "datetime", nullable=False),
    ],
)

PIPELINE_RUN_LOG = SchemaContract(
    entity="pipeline_run_log",
    source_format="parquet",
    source_layer="audit",
    natural_key=["run_id"],
    columns=[
        ColumnContract("run_id", "int", nullable=False, min_value=1),
        ColumnContract("run_type", "str", nullable=False, allowed_values={"batch", "stream", "reconciliation"}),
        ColumnContract("run_date", "date", nullable=False),
        ColumnContract("status", "str", nullable=False, allowed_values={"running", "success", "partial", "failed"}),
        ColumnContract("started_at", "datetime", nullable=False),
        ColumnContract("completed_at", "datetime", nullable=True),
        ColumnContract("total_files", "int", nullable=True, default_value=0),
        ColumnContract("successful_files", "int", nullable=True, default_value=0),
        ColumnContract("failed_files", "int", nullable=True, default_value=0),
        ColumnContract("total_records", "int", nullable=True, default_value=0),
        ColumnContract("total_loaded", "int", nullable=True, default_value=0),
        ColumnContract("total_quarantined", "int", nullable=True, default_value=0),
        ColumnContract("total_orphaned", "int", nullable=True, default_value=0),
        ColumnContract("error_message", "str", nullable=True),
    ],
)

FILE_TRACKER = SchemaContract(
    entity="file_tracker",
    source_format="parquet",
    source_layer="audit",
    natural_key=["file_id"],
    columns=[
        ColumnContract("file_id", "int", nullable=False, min_value=1),
        ColumnContract("file_path", "str", nullable=False),
        ColumnContract("file_hash", "str", nullable=False),
        ColumnContract("file_type", "str", nullable=False, allowed_values={"batch", "stream"}),
        ColumnContract("records_total", "int", nullable=True),
        ColumnContract("records_loaded", "int", nullable=True),
        ColumnContract("records_quarantined", "int", nullable=True),
        ColumnContract("status", "str", nullable=False, allowed_values={"success", "partial", "failed"}),
        ColumnContract("processed_at", "datetime", nullable=False),
        ColumnContract("pipeline_run_id", "int", nullable=True),
    ],
)

QUALITY_METRICS = SchemaContract(
    entity="pipeline_quality_metrics",
    source_format="parquet",
    source_layer="audit",
    natural_key=["metric_id"],
    columns=[
        ColumnContract("metric_id", "int", nullable=False, min_value=1),
        ColumnContract("run_id", "int", nullable=False),
        ColumnContract("run_date", "date", nullable=False),
        ColumnContract("table_name", "str", nullable=False),
        ColumnContract("source_file", "str", nullable=True),
        ColumnContract("total_records", "int", nullable=True),
        ColumnContract("valid_records", "int", nullable=True),
        ColumnContract("quarantined_records", "int", nullable=True),
        ColumnContract("orphaned_records", "int", nullable=True),
        ColumnContract("duplicate_count", "int", nullable=True),
        ColumnContract("null_violations", "int", nullable=True),
        ColumnContract("duplicate_rate", "float", nullable=True),
        ColumnContract("orphan_rate", "float", nullable=True),
        ColumnContract("null_rate", "float", nullable=True),
        ColumnContract("quarantine_rate", "float", nullable=True),
        ColumnContract("processing_latency_sec", "float", nullable=True),
        ColumnContract("quality_details", "str", nullable=True),  # jsonb stored as raw str
        ColumnContract("recorded_at", "datetime", nullable=False),
    ],
)


REGISTRY: dict[str, SchemaContract] = {
    # Lookup tables
    "cities": CITIES,
    "regions": REGIONS,
    "segments": SEGMENTS,
    "categories": CATEGORIES,
    "teams": TEAMS,
    "reason_categories": REASON_CATEGORIES,
    "reasons": REASONS,
    "channels": CHANNELS,
    "priorities": PRIORITIES,
    # Entity tables
    "customers": CUSTOMERS,
    "restaurants": RESTAURANTS,
    "drivers": DRIVERS,
    "agents": AGENTS,
    # Stream transactions
    "orders": ORDERS,
    "tickets": TICKETS,
    "ticket_events": TICKET_EVENTS,
    # Operational Audit tables
    "orphan_tracking": ORPHAN_TRACKING,
    "quarantine": QUARANTINE,
    "pipeline_run_log": PIPELINE_RUN_LOG,
    "file_tracker": FILE_TRACKER,
    "pipeline_quality_metrics": QUALITY_METRICS,
}


def get_contract(entity: str) -> SchemaContract:
    if entity not in REGISTRY:
        registered = sorted(REGISTRY.keys())
        raise KeyError(
            f"No schema contract registered for entity '{entity}'. "
            f"Registered entities: {registered}"
        )
    return REGISTRY[entity]


def list_entities(source_layer: Optional[SourceLayer] = None) -> list[str]:
    if source_layer is None:
        return sorted(REGISTRY.keys())
    return sorted(
        name for name, contract in REGISTRY.items()
        if contract.source_layer == source_layer
    )