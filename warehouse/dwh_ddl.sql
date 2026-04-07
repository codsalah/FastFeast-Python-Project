-- ============================================================
-- FastFeast Data Warehouse — STAR SCHEMA
-- Schema: warehouse
-- Contains: dimension tables, fact tables only
-- ============================================================

-- Create schema if needed
CREATE SCHEMA IF NOT EXISTS warehouse;
SET search_path TO warehouse, public;


-- ════════════════════════════════════════════════════════════
-- DIMENSION TABLES
-- ════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS dim_date (
    date_key     integer PRIMARY KEY, 
    full_date    date NOT NULL,
    day          integer NOT NULL,
    month        integer NOT NULL,
    year         integer NOT NULL,
    quarter      integer NOT NULL,
    day_of_week  varchar(20) NOT NULL,
    hour         integer NOT NULL,
    time_of_day  varchar(20) NOT NULL,
    is_weekend   boolean NOT NULL,
    is_holiday   boolean NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key          integer PRIMARY KEY, -- -1 = Unknown member for orphans
    customer_id           integer,              -- Natural key from source. NULL for Unknown.
    customer_name_masked  varchar(256),--
    gender                varchar(20),
    segment_name          varchar(100),
    region_name           varchar(100),
    city_name             varchar(100),
    signup_date           date,
    valid_from            date NOT NULL,--
    valid_to              date,--
    is_current            boolean NOT NULL DEFAULT TRUE--
);

CREATE TABLE IF NOT EXISTS dim_driver (
    driver_key    integer PRIMARY KEY, -- -1 = Unknown member for orphans.
    driver_id     integer,
    driver_name   varchar(256),
    vehicle_type  varchar(50),
    shift         varchar(50),
    region_name   varchar(100),
    city_name     varchar(100),
    is_active     boolean,
    valid_from    date NOT NULL,
    valid_to      date,
    is_current    boolean NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_restaurant (
    restaurant_key   integer PRIMARY KEY,-- -1 = Unknown member for orphans.
    restaurant_id    integer,
    restaurant_name  varchar(256),
    category_name    varchar(100),
    price_tier       varchar(50),
    region_name      varchar(100),
    city_name        varchar(100),
    rating_avg       decimal(3,2),
    is_active        boolean,
    valid_from       date NOT NULL,
    valid_to         date,
    is_current       boolean NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_agent (
    agent_key      integer PRIMARY KEY,--no Unknown member for orphans.
    agent_id       integer,
    agent_name     varchar(256),
    skill_level    varchar(50),
    team_name      varchar(100),
    is_active      boolean,
    valid_from     date NOT NULL,
    valid_to       date,
    is_current     boolean NOT NULL DEFAULT TRUE
);

-- ════════════════════════════════════════════════════════════
-- STATIC DIMENSION TABLES (Use natural keys as primary keys)
-- ════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS dim_reason (
    reason_id                integer PRIMARY KEY,  -- Natural key is the PK
    reason_name              varchar(256),
    reason_category_name     varchar(100),
    severity_level           smallint,
    typical_refund_pct       decimal(5,4)
);

CREATE TABLE IF NOT EXISTS dim_channel (
    channel_id     integer PRIMARY KEY,  -- Natural key is the PK
    channel_name   varchar(50)
);

CREATE TABLE IF NOT EXISTS dim_priority (
    -- Static lookup: priority levels rarely change; no SCD2 versioning needed.
    priority_id              integer PRIMARY KEY,
    priority_code            varchar(10),
    priority_name            varchar(50),
    sla_first_response_min   integer,
    sla_resolution_min       integer
);

-- ════════════════════════════════════════════════════════════
-- SCD2 SURROGATE KEY DEFAULTS (idempotent)
-- Ensures loaders can INSERT without explicitly providing *_key.
-- Keeps ability to reserve -1 for Unknown members.
-- ════════════════════════════════════════════════════════════

CREATE SEQUENCE IF NOT EXISTS dim_customer_key_seq;
ALTER SEQUENCE dim_customer_key_seq OWNED BY dim_customer.customer_key;
ALTER TABLE dim_customer ALTER COLUMN customer_key SET DEFAULT nextval('dim_customer_key_seq');

CREATE SEQUENCE IF NOT EXISTS dim_driver_key_seq;
ALTER SEQUENCE dim_driver_key_seq OWNED BY dim_driver.driver_key;
ALTER TABLE dim_driver ALTER COLUMN driver_key SET DEFAULT nextval('dim_driver_key_seq');

CREATE SEQUENCE IF NOT EXISTS dim_restaurant_key_seq;
ALTER SEQUENCE dim_restaurant_key_seq OWNED BY dim_restaurant.restaurant_key;
ALTER TABLE dim_restaurant ALTER COLUMN restaurant_key SET DEFAULT nextval('dim_restaurant_key_seq');

CREATE SEQUENCE IF NOT EXISTS dim_agent_key_seq;
ALTER SEQUENCE dim_agent_key_seq OWNED BY dim_agent.agent_key;
ALTER TABLE dim_agent ALTER COLUMN agent_key SET DEFAULT nextval('dim_agent_key_seq');


-- ════════════════════════════════════════════════════════════
-- FACT TABLES
-- ════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS fact_orders (
    order_key                    serial PRIMARY KEY,
    order_id                     varchar(256) NOT NULL,
    customer_key                 integer NOT NULL REFERENCES dim_customer(customer_key),
    driver_key                   integer NOT NULL REFERENCES dim_driver(driver_key),
    restaurant_key               integer NOT NULL REFERENCES dim_restaurant(restaurant_key),
    region_id                    integer NOT NULL,
    date_key                     integer NOT NULL REFERENCES dim_date(date_key),
    order_amount                 decimal(10,2),
    delivery_fee                 decimal(10,2),
    discount_amount              decimal(10,2),
    total_amount                 decimal(10,2),
    order_status                 varchar(50) NOT NULL,
    payment_method               varchar(50),
    order_created_at             timestamp NOT NULL,-- we should make it consistent with date dimension
    delivered_at                 timestamp,
    original_orphan_customer_id  integer,
    original_orphan_driver_id    integer,
    --we should add original_orphan_restaurent_id for orphan handling 
    original_orphan_restaurant_id integer,
    version                      smallint NOT NULL DEFAULT 1,
    is_backfilled                boolean NOT NULL DEFAULT FALSE,
    UNIQUE(order_id, version)
);

CREATE TABLE IF NOT EXISTS fact_tickets (
    ticket_key                   serial PRIMARY KEY,
    ticket_id                    varchar(256) NOT NULL,
    order_key                    integer NOT NULL REFERENCES fact_orders(order_key),
    order_id                     varchar(256) NOT NULL, 
    customer_key                 integer NOT NULL REFERENCES dim_customer(customer_key),
    driver_key                   integer NOT NULL REFERENCES dim_driver(driver_key),
    restaurant_key               integer NOT NULL REFERENCES dim_restaurant(restaurant_key),
    agent_key                    integer NOT NULL REFERENCES dim_agent(agent_key),
    reason_id                    integer NOT NULL REFERENCES dim_reason(reason_id),
    priority_id                  integer NOT NULL REFERENCES dim_priority(priority_id),
    channel_id                   integer NOT NULL REFERENCES dim_channel(channel_id),
    date_key                     integer NOT NULL REFERENCES dim_date(date_key),
    status                       varchar(50) NOT NULL,
    refund_amount                decimal(10,2),
    sla_first_response_breached  boolean NOT NULL DEFAULT false,
    sla_resolution_breached      boolean NOT NULL DEFAULT false,
    first_response_minutes       decimal(8,2),
    resolution_minutes           decimal(8,2),
    created_at                   timestamp NOT NULL,
    first_response_at            timestamp,
    resolved_at                  timestamp,
    sla_first_due_at             timestamp NOT NULL,
    sla_resolve_due_at           timestamp NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_tickets_ticket_id ON fact_tickets (ticket_id);

CREATE TABLE IF NOT EXISTS fact_ticket_events (
    event_key       serial PRIMARY KEY,
    event_id        varchar(256) NOT NULL,
    ticket_key      integer NOT NULL REFERENCES fact_tickets(ticket_key),
    agent_key       integer NOT NULL REFERENCES dim_agent(agent_key),
    date_key        integer NOT NULL REFERENCES dim_date(date_key),
    old_status      varchar(50),
    new_status      varchar(50) NOT NULL,
    event_ts        timestamp NOT NULL,
    notes           text NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_ticket_events_event_id ON fact_ticket_events (event_id);


-- ════════════════════════════════════════════════════════════
-- VIEWS
-- ════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW orders_clean AS
SELECT * FROM fact_orders f1
WHERE version = (
  SELECT MAX(f2.version) 
  FROM fact_orders f2
  WHERE f2.order_id = f1.order_id
);
