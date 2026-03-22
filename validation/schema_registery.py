"""
Single Sourec of truth for all input entity's contract.

Each SchemaContract defines:
- required fields and their types
- nullable columns
- categorical columns
- numeric range constraints
- natural key for dedups 

Used by:
- schema_validator
- business_rules_validator
- deduplicator (if applicable)
- etc....
"""

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ColumnContract:
    """ Think of it like column's expectations """
    name: str
    dtype: str
    nullable: bool = False
    allowed_values: Optional[set] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    regex: Optional[str] = None


@dataclass
class SchemaContract:
    """ Think of it like full expectation for a table/input file"""
    entity: str
    source_format: str
    source_layer: str
    natural_key: str
    columns: list[ColumnContract]

    def __init__(self, entity: str, source_format: str, source_layer: str, natural_key: str, columns: list[ColumnContract]):
        self.entity = entity
        self.source_format = source_format
        self.source_layer = source_layer
        self.natural_key = natural_key
        self.columns = columns
    
    def required_columns(self):
        return [c.name for c in self.columns if not c.nullable]
    
    def nullable_columns(self):
        return [c.name for c in self.columns if c.nullable]
    
    # Next: return one column at a time or default to None  
    def get_column(self, column_name: str):
        return next((c for c in self.columns if c.name == column_name), None)

    # Return {column_name: dtype} 
    def dtype_map(self):
        return {c.name: c.dtype for c in self.columns}
    
    def categorical_columns(self) -> list[ColumnContract]:
        return [c for c in self.columns if c.allowed_values is not None]
    
    def numeric_range(self) -> list[ColumnContract]:
        return [c for c in self.columns if c.min_value is not None or c.max_value is not None]


## Assumptions of SchemaContract based on input data
# - Note: Any subjective value should be tuned and validated against business rules
# - Note: Most of these constraints against data will be validated again! 


# ------------------------------------------------------------------------- #
# ------- Batch reference / lookup tables (static, delivered as is) ------- #
# ------------------------------------------------------------------------- #

CITIES = SchemaContract(
    entity="cities",
    source_format="json",
    source_layer="batch",
    natural_key=["city_id"],
    columns=[
        ColumnContract(name="city_id", dtype="int", nullable=False),
        ColumnContract(name="city_name", dtype="str", nullable=False),
        ColumnContract(name="country", dtype="str", nullable=False),
    ],
)


REGIONS = SchemaContract(
    entity="regions",
    source_format="csv",
    source_layer="batch",
    natural_key=["region_id"],
    columns=[
        ColumnContract("region_id",         "int",   nullable=False),
        ColumnContract("region_name",       "str",   nullable=False),
        ColumnContract("city_id",           "int",   nullable=False),
        ColumnContract("delivery_base_fee", "float", nullable=False, min_value=0.0, max_value=100.0),
    ],
)
 
SEGMENTS = SchemaContract(
    entity="segments",
    source_format="csv",
    source_layer="batch",
    natural_key=["segment_id"],
    columns=[
        ColumnContract("segment_id",       "int",  nullable=False),
        ColumnContract("segment_name",     "str",  nullable=False, allowed_values={"Regular", "VIP"}),
        ColumnContract("discount_pct",     "int",  nullable=False, min_value=0,   max_value=100),
        ColumnContract("priority_support", "bool", nullable=False),
    ],
)
 
CATEGORIES = SchemaContract(
    entity="categories",
    source_format="csv",
    source_layer="batch",
    natural_key=["category_id"],
    columns=[
        ColumnContract("category_id",   "int", nullable=False),
        ColumnContract("category_name", "str", nullable=False),
    ],
)
 
TEAMS = SchemaContract(
    entity="teams",
    source_format="csv",
    source_layer="batch",
    natural_key=["team_id"],
    columns=[
        ColumnContract("team_id",   "int", nullable=False),
        ColumnContract("team_name", "str", nullable=False),
    ],
)
 
REASON_CATEGORIES = SchemaContract(
    entity="reason_categories",
    source_format="csv",
    source_layer="batch",
    natural_key=["reason_category_id"],
    columns=[
        ColumnContract("reason_category_id", "int", nullable=False),
        ColumnContract("category_name",      "str", nullable=False,
                       allowed_values={"Delivery", "Food", "Payment"}),
    ],
)
 
REASONS = SchemaContract(
    entity="reasons",
    source_format="csv",
    source_layer="batch",
    natural_key=["reason_id"],
    columns=[
        ColumnContract("reason_id",          "int",   nullable=False),
        ColumnContract("reason_name",        "str",   nullable=False),
        ColumnContract("reason_category_id", "int",   nullable=False),
        ColumnContract("severity_level",     "int",   nullable=False, min_value=1, max_value=5),
        ColumnContract("typical_refund_pct", "float", nullable=False, min_value=0.0, max_value=1.0),
    ],
)
 
CHANNELS = SchemaContract(
    entity="channels",
    source_format="csv",
    source_layer="batch",
    natural_key=["channel_id"],
    columns=[
        ColumnContract("channel_id",   "int", nullable=False),
        ColumnContract("channel_name", "str", nullable=False,
                       allowed_values={"app", "chat", "phone", "email"}),
    ],
)
 
PRIORITIES = SchemaContract(
    entity="priorities",
    source_format="csv",
    source_layer="batch",
    natural_key=["priority_id"],
    columns=[
        ColumnContract("priority_id",              "int", nullable=False),
        ColumnContract("priority_code",            "str", nullable=False,
                       allowed_values={"P1", "P2", "P3", "P4"}),
        ColumnContract("priority_name",            "str", nullable=False,
                       allowed_values={"Critical", "High", "Medium", "Low"}),
        ColumnContract("sla_first_response_min",   "int", nullable=False, min_value=1),
        ColumnContract("sla_resolution_min",       "int", nullable=False, min_value=1),
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
        ColumnContract("customer_id",  "int",      nullable=False),  # PII — masked before OLAP load
        ColumnContract("full_name",    "str",      nullable=True),   # ~5% null in source
        ColumnContract("email",        "str",      nullable=True, 
                       regex=r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),        # validated, not excluded until now
        ColumnContract("phone",        "str",      nullable=True,
                       regex=r"^(010|011|012|015)\d{8}$"),
        ColumnContract("region_id",    "int",      nullable=True),   # ~5% null
        ColumnContract("segment_id",   "int",      nullable=False),
        ColumnContract("signup_date",  "datetime", nullable=False),
        ColumnContract("gender",       "str",      nullable=True,
                       allowed_values={"male", "female"}),
        ColumnContract("created_at",   "datetime", nullable=False),
        ColumnContract("updated_at",   "datetime", nullable=False),
    ],
)
 
RESTAURANTS = SchemaContract(
    entity="restaurants",
    source_format="json",
    source_layer="batch",
    natural_key=["restaurant_id"],
    columns=[
        ColumnContract("restaurant_id",      "int",   nullable=False),
        ColumnContract("restaurant_name",    "str",   nullable=True),   # ~3% null
        ColumnContract("region_id",          "int",   nullable=False),
        ColumnContract("category_id",        "int",   nullable=True),   # ~3% null 
        ColumnContract("price_tier",         "str",   nullable=False,
                       allowed_values={"Low", "Mid", "High"}),
        ColumnContract("rating_avg",         "float", nullable=True, min_value=1.0, max_value=5.0),  # invalid values exist in source
        ColumnContract("prep_time_avg_min",  "int",   nullable=True, min_value=1, max_value=120),
        ColumnContract("is_active",          "bool",  nullable=False),
        ColumnContract("created_at",         "datetime", nullable=False),
        ColumnContract("updated_at",         "datetime", nullable=False),
    ],
)
 
DRIVERS = SchemaContract(
    entity="drivers",
    source_format="csv",
    source_layer="batch",
    natural_key=["driver_id"],
    columns=[
        ColumnContract("driver_id",            "int",   nullable=False),
        ColumnContract("driver_name",          "str",   nullable=True),   # ~4% null
        ColumnContract("driver_phone",         "str",   nullable=True, 
                       regex=r"^(010|011|012|015)\d{8}$"), # Phone e.g. 010********, 011, 012, 015
        ColumnContract("national_id",          "str",   nullable=True),   # PII — excluded at OLAP layer
        ColumnContract("region_id",            "int",   nullable=False),
        ColumnContract("shift",                "str",   nullable=False,
                       allowed_values={"morning", "evening", "night"}),
        ColumnContract("vehicle_type",         "str",   nullable=False,
                       allowed_values={"bike", "motorbike", "car"}),
        ColumnContract("hire_date",            "datetime", nullable=False),
        ColumnContract("rating_avg",           "float", nullable=False, min_value=1.0, max_value=5.0),
        ColumnContract("on_time_rate",         "float", nullable=True, min_value=0.0, max_value=1.0),
        ColumnContract("cancel_rate",          "float", nullable=False, min_value=0.0, max_value=1.0),
        ColumnContract("completed_deliveries", "int",   nullable=False, min_value=0),
        ColumnContract("is_active",            "bool",  nullable=False),
        ColumnContract("created_at",           "datetime", nullable=False),
        ColumnContract("updated_at",           "datetime", nullable=False),
    ],
)
 
AGENTS = SchemaContract(
    entity="agents",
    source_format="csv",
    source_layer="batch",
    natural_key=["agent_id"],
    columns=[
        ColumnContract("agent_id",             "int",   nullable=False),
        ColumnContract("agent_name",           "str",   nullable=False), # PII — masked before OLAP load
        ColumnContract("agent_email",          "str",   nullable=True,
                       regex=r"^[^@\s]+@[^@\s]+\.[^@\s]+$"),
        ColumnContract("agent_phone",          "str",   nullable=True,
                       regex=r"^(010|011|012|015)\d{8}$"),
        ColumnContract("team_id",              "int",   nullable=True),   # ~3% null
        ColumnContract("skill_level",          "str",   nullable=False,
                       allowed_values={"Junior", "Mid", "Senior", "Lead"}),
        ColumnContract("hire_date",            "datetime", nullable=False),
        ColumnContract("avg_handle_time_min",  "int",   nullable=False, min_value=1, max_value=120),
        ColumnContract("resolution_rate",      "float", nullable=False, min_value=0.0, max_value=1.0),
        ColumnContract("csat_score",           "float", nullable=False, min_value=1.0, max_value=5.0),
        ColumnContract("is_active",            "bool",  nullable=False),
        ColumnContract("created_at",           "datetime", nullable=False),
        ColumnContract("updated_at",           "datetime", nullable=False),
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
        ColumnContract("order_id",        "str",      nullable=False),   # UUID
        ColumnContract("customer_id",     "int",      nullable=False),   # FK → customers; orphans expected
        ColumnContract("restaurant_id",   "int",      nullable=False),   # FK → restaurants; orphans expected
        ColumnContract("driver_id",       "int",      nullable=False),   # FK → drivers; orphans expected
        ColumnContract("region_id",       "int",      nullable=False),
        ColumnContract("order_amount",    "float",    nullable=True,
                       min_value=0.0, max_value=10000.0),
        ColumnContract("delivery_fee",    "float",    nullable=True,
                       min_value=0.0, max_value=500.0),
        ColumnContract("discount_amount", "float",    nullable=True,
                       min_value=0.0, max_value=500.0),
        ColumnContract("total_amount",    "float",    nullable=True,
                       min_value=0.0, max_value=10000.0),
        ColumnContract("order_status",    "str",      nullable=False,
                       allowed_values={"Delivered", "Cancelled", "Refunded"}),
        ColumnContract("payment_method",  "str",      nullable=True,
                       allowed_values={"card", "cash", "wallet"}),
        ColumnContract("order_created_at","datetime", nullable=False),
        ColumnContract("delivered_at",    "datetime", nullable=True),   # null if not Delivered
    ],
)
 
TICKETS = SchemaContract(
    entity="tickets",
    source_format="csv",
    source_layer="stream",
    natural_key=["ticket_id"],
    columns=[
        ColumnContract("ticket_id",          "str",      nullable=False),   # UUID
        ColumnContract("order_id",           "str",      nullable=False),   # FK → orders
        ColumnContract("customer_id",        "int",      nullable=False),
        ColumnContract("driver_id",          "int",      nullable=True),
        ColumnContract("restaurant_id",      "int",      nullable=True),
        ColumnContract("agent_id",           "int",      nullable=False),
        ColumnContract("reason_id",          "int",      nullable=False),
        ColumnContract("priority_id",        "int",      nullable=False),
        ColumnContract("channel_id",         "int",      nullable=False),
        ColumnContract("status",             "str",      nullable=False,
                       allowed_values={"Resolved", "Closed", "Reopened", "Open", "InProgress"}),
        ColumnContract("refund_amount",      "float",    nullable=True,
                       min_value=0.0, max_value=10000.0),
        ColumnContract("created_at",         "datetime", nullable=False),
        ColumnContract("first_response_at",  "datetime", nullable=True),
        ColumnContract("resolved_at",        "datetime", nullable=True),
        ColumnContract("sla_first_due_at",   "datetime", nullable=False),
        ColumnContract("sla_resolve_due_at", "datetime", nullable=False),
    ],
)
 
TICKET_EVENTS = SchemaContract(
    entity="ticket_events",
    source_format="json",
    source_layer="stream",
    natural_key=["event_id"],
    columns=[
        ColumnContract("event_id",   "str", nullable=False),   # UUID
        ColumnContract("ticket_id",  "str", nullable=False),   # FK → tickets
        ColumnContract("agent_id",   "int", nullable=False),
        ColumnContract("event_ts",   "datetime", nullable=False),
        ColumnContract("old_status", "str", nullable=True,     # null on first event (Open)
                       allowed_values={"Open", "InProgress", "Resolved", "Closed", "Reopened"}),
        ColumnContract("new_status", "str", nullable=False,
                       allowed_values={"Open", "InProgress", "Resolved", "Closed", "Reopened"}),
        ColumnContract("notes",      "str", nullable=True),
    ],
)



REGISTRY: dict[str, SchemaContract] = {
    # Lookup tables
    "cities":             CITIES,
    "regions":            REGIONS,
    "segments":           SEGMENTS,
    "categories":         CATEGORIES,
    "teams":              TEAMS,
    "reason_categories":  REASON_CATEGORIES,
    "reasons":            REASONS,
    "channels":           CHANNELS,
    "priorities":         PRIORITIES,
    # Entity tables (can drift)
    "customers":          CUSTOMERS,
    "restaurants":        RESTAURANTS,
    "drivers":            DRIVERS,
    "agents":             AGENTS,
    # Stream transactions
    "orders":             ORDERS,
    "tickets":            TICKETS,
    "ticket_events":      TICKET_EVENTS,
}
 
 
def get_contract(entity: str) -> SchemaContract:
    """
    Fetch the schema contract for an entity.
    Raises KeyError with a clear message if the entity is not registered.
 
    Usage:
        contract = get_contract("orders")
        required = contract.required_columns()
        dtype_map = contract.dtype_map()
    """
    if entity not in REGISTRY:
        registered = sorted(REGISTRY.keys())
        raise KeyError(
            f"No schema contract registered for entity '{entity}'. "
            f"Registered entities: {registered}"
        )
    return REGISTRY[entity]
 
 
def list_entities(source_layer: Optional[str] = None) -> list[str]:
    """
    List all registered entity names.
    Optionally filter by source_layer: 'batch' | 'stream'.
 
    Usage:
        batch_entities = list_entities("batch")
        all_entities   = list_entities()
    """
    if source_layer is None:
        return sorted(REGISTRY.keys())
    return sorted(
        name for name, contract in REGISTRY.items()
        if contract.source_layer == source_layer
)