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
    customer_name_masked  varchar(256),
    gender                varchar(20),
    segment_name          varchar(100),
    region_name           varchar(100),
    city_name             varchar(100),
    signup_date           date,
    valid_from            date NOT NULL,
    valid_to              date,
    is_current            boolean NOT NULL DEFAULT TRUE
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
    restaurant_key   integer PRIMARY KEY,
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
    agent_key      integer PRIMARY KEY,
    agent_id       integer,
    agent_name     varchar(256),
    skill_level    varchar(50),
    team_name      varchar(100),
    is_active      boolean,
    valid_from     date NOT NULL,
    valid_to       date,
    is_current     boolean NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_reason (
    reason_key             integer PRIMARY KEY,
    reason_id              integer,
    reason_name            varchar(256),
    reason_category_name   varchar(100),
    severity_level         smallint,
    typical_refund_pct     decimal(5,4)
);

CREATE TABLE IF NOT EXISTS dim_channel (
    channel_key    integer PRIMARY KEY,
    channel_id     integer,
    channel_name   varchar(50)
);

CREATE TABLE IF NOT EXISTS dim_priority (
    priority_key             integer PRIMARY KEY,
    priority_id              integer,
    priority_code            varchar(10),
    priority_name            varchar(50),
    sla_first_response_min   integer NOT NULL,
    sla_resolution_min       integer NOT NULL
);


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
    order_created_at             timestamp NOT NULL,
    delivered_at                 timestamp,
    original_orphan_customer_id  integer,
    original_orphan_driver_id    integer,
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
    driver_key                   integer REFERENCES dim_driver(driver_key),
    restaurant_key               integer REFERENCES dim_restaurant(restaurant_key),
    agent_key                    integer NOT NULL REFERENCES dim_agent(agent_key),
    reason_key                   integer NOT NULL REFERENCES dim_reason(reason_key),
    priority_key                 integer NOT NULL REFERENCES dim_priority(priority_key),
    channel_key                  integer NOT NULL REFERENCES dim_channel(channel_key),
    date_key                     integer NOT NULL REFERENCES dim_date(date_key),
    status                       varchar(50) NOT NULL,
    refund_amount                decimal(10,2),
    sla_first_response_breached  boolean,
    sla_resolution_breached      boolean,
    first_response_minutes       decimal(8,2),
    resolution_minutes           decimal(8,2),
    created_at                   timestamp NOT NULL,
    first_response_at            timestamp,
    resolved_at                  timestamp,
    sla_first_due_at             timestamp,
    sla_resolve_due_at           timestamp
);

CREATE TABLE IF NOT EXISTS fact_ticket_events (
    event_key       serial PRIMARY KEY,
    event_id        varchar(256) NOT NULL,
    ticket_key      integer NOT NULL REFERENCES fact_tickets(ticket_key),
    agent_key       integer NOT NULL REFERENCES dim_agent(agent_key),
    date_key        integer NOT NULL REFERENCES dim_date(date_key),
    old_status      varchar(50),
    new_status      varchar(50) NOT NULL,
    event_timestamp timestamp NOT NULL,
    notes           text NOT NULL
);


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
