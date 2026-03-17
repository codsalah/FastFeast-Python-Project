---------------------------------------------------------------------------------
-- Decisions taken:
-- Idempotent: All create statements use IF NOT EXISTS for safe reruns
-- Deduplication: Fact tables have unique index on source_hash to prevent duplicates
-- Orphans: Stored as-is; validation/flagging not implemented yet
-- PII: Stored raw; masking not applied in DDL
---------------------------------------------------------------------------------


-- Create schema
------------ create schema if not exists fastfeast;
------------ set search_path to fastfeast;

-- lookup dimensions
create table if not exists dim_cities (
    city_id int primary key,
    city_name varchar(256) not null,
    country varchar(256) not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
create index if not exists idx_dim_cities_name on dim_cities(city_name);

create table if not exists dim_regions (
    region_id int primary key,
    region_name varchar(256) not null,
    city_id int not null references dim_cities(city_id),
    delivery_base_fee decimal(10,2) not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
create index if not exists idx_dim_regions_city_id on dim_regions(city_id);

create table if not exists dim_segments (
    segment_id int primary key,
    segment_name varchar(256) not null,
    discount_pct int not null,
    priority_support boolean not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists dim_categories (
    category_id int primary key,
    category_name varchar(256) not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists dim_teams (
    team_id int primary key,
    team_name varchar(256) not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists dim_reason_categories (
    reason_category_id int primary key,
    category_name varchar(256) not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists dim_reasons (
    reason_id int primary key,
    reason_name varchar(512) not null,
    reason_category_id int not null references dim_reason_categories(reason_category_id),
    severity_level int not null,
    typical_refund_pct decimal(5,2) not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
create index if not exists idx_dim_reasons_category_id on dim_reasons(reason_category_id);

create table if not exists dim_channels (
    channel_id int primary key,
    channel_name varchar(256) not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists dim_priorities (
    priority_id int primary key,
    priority_code varchar(10) not null unique,
    priority_name varchar(256) not null,
    sla_first_response_min int not null,
    sla_resolution_min int not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
create index if not exists idx_dim_priorities_code on dim_priorities(priority_code);

-- entity dimensions
create table if not exists dim_customers (
    customer_id int primary key,
    segment_id int not null references dim_segments(segment_id),
    account_created_at timestamptz not null,
    is_active boolean not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
create index if not exists idx_dim_customers_segment_id on dim_customers(segment_id);
create index if not exists idx_dim_customers_is_active on dim_customers(is_active);

create table if not exists dim_restaurants (
    restaurant_id int primary key,
    category_id int not null references dim_categories(category_id),
    region_id int not null references dim_regions(region_id),
    rating_avg decimal(3,2) not null,
    prep_time_avg_min int,
    is_active boolean not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
create index if not exists idx_dim_restaurants_category_id on dim_restaurants(category_id);
create index if not exists idx_dim_restaurants_region_id on dim_restaurants(region_id);
create index if not exists idx_dim_restaurants_is_active on dim_restaurants(is_active);

create table if not exists dim_drivers (
    driver_id int primary key,
    region_id int not null references dim_regions(region_id),
    shift varchar(50) not null,
    vehicle_type varchar(50) not null,
    hire_date timestamptz not null,
    rating_avg decimal(3,2) not null,
    on_time_rate decimal(5,3),
    cancel_rate decimal(5,3) not null,
    completed_deliveries int not null,
    is_active boolean not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
create index if not exists idx_dim_drivers_region_id on dim_drivers(region_id);
create index if not exists idx_dim_drivers_is_active on dim_drivers(is_active);

create table if not exists dim_agents (
    agent_id int primary key,
    team_id int references dim_teams(team_id),
    skill_level varchar(50) not null,
    hire_date timestamptz not null,
    avg_handle_time_min int not null,
    resolution_rate decimal(5,3) not null,
    csat_score decimal(3,2) not null,
    is_active boolean not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
create index if not exists idx_dim_agents_team_id on dim_agents(team_id);
create index if not exists idx_dim_agents_is_active on dim_agents(is_active);

-- fact tables
create table if not exists fact_orders (
    order_id varchar(256) primary key,
    customer_id int not null,
    restaurant_id int not null,
    driver_id int not null,
    region_id int not null references dim_regions(region_id),
    order_amount decimal(10,2),
    delivery_fee decimal(10,2),
    discount_amount decimal(10,2),
    total_amount decimal(10,2),
    order_status varchar(50) not null,
    payment_method varchar(50),
    order_created_at timestamptz not null,
    delivered_at timestamptz,
    source_hash varchar(64),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
create unique index if not exists idx_fact_orders_source_hash on fact_orders(source_hash) where source_hash is not null;
create index if not exists idx_fact_orders_customer_id on fact_orders(customer_id);
create index if not exists idx_fact_orders_restaurant_id on fact_orders(restaurant_id);
create index if not exists idx_fact_orders_driver_id on fact_orders(driver_id);
create index if not exists idx_fact_orders_region_id on fact_orders(region_id);
create index if not exists idx_fact_orders_order_created_at on fact_orders(order_created_at desc);
create index if not exists idx_fact_orders_order_status on fact_orders(order_status);

create table if not exists fact_tickets (
    ticket_id varchar(256) primary key,
    order_id varchar(256) not null references fact_orders(order_id),
    customer_id int not null,
    driver_id int,
    restaurant_id int,
    agent_id int not null references dim_agents(agent_id),
    reason_id int not null references dim_reasons(reason_id),
    priority_id int not null references dim_priorities(priority_id),
    channel_id int not null references dim_channels(channel_id),
    status varchar(50) not null,
    refund_amount decimal(10,2),
    created_at timestamptz not null,
    first_response_at timestamptz,
    resolved_at timestamptz,
    sla_first_due_at timestamptz not null,
    sla_resolve_due_at timestamptz not null,
    source_hash varchar(64),
    updated_at timestamptz not null default now()
);
create unique index if not exists idx_fact_tickets_source_hash on fact_tickets(source_hash) where source_hash is not null;
create index if not exists idx_fact_tickets_order_id on fact_tickets(order_id);
create index if not exists idx_fact_tickets_customer_id on fact_tickets(customer_id);
create index if not exists idx_fact_tickets_driver_id on fact_tickets(driver_id);
create index if not exists idx_fact_tickets_restaurant_id on fact_tickets(restaurant_id);
create index if not exists idx_fact_tickets_agent_id on fact_tickets(agent_id);
create index if not exists idx_fact_tickets_reason_id on fact_tickets(reason_id);
create index if not exists idx_fact_tickets_priority_id on fact_tickets(priority_id);
create index if not exists idx_fact_tickets_channel_id on fact_tickets(channel_id);
create index if not exists idx_fact_tickets_created_at on fact_tickets(created_at desc);
create index if not exists idx_fact_tickets_status on fact_tickets(status);
create index if not exists idx_fact_tickets_sla_first_due on fact_tickets(sla_first_due_at);
create index if not exists idx_fact_tickets_sla_resolve_due on fact_tickets(sla_resolve_due_at);

create table if not exists fact_ticket_events (
    event_id varchar(256) primary key,
    ticket_id varchar(256) not null references fact_tickets(ticket_id),
    agent_id int not null references dim_agents(agent_id),
    event_ts timestamptz not null,
    old_status varchar(50),
    new_status varchar(50) not null,
    notes text,
    source_hash varchar(64),
    created_at timestamptz not null default now()
);
create unique index if not exists idx_fact_ticket_events_source_hash on fact_ticket_events(source_hash) where source_hash is not null;
create index if not exists idx_fact_ticket_events_ticket_id on fact_ticket_events(ticket_id);
create index if not exists idx_fact_ticket_events_agent_id on fact_ticket_events(agent_id);
create index if not exists idx_fact_ticket_events_event_ts on fact_ticket_events(event_ts desc);

-- triggers to update updated_at
create or replace function update_timestamp_dw() returns trigger as $$
begin
    new.updated_at = now();
    return new;
end;
$$ language plpgsql;

create trigger trg_dim_cities_update before update on dim_cities for each row execute function update_timestamp_dw();
create trigger trg_dim_regions_update before update on dim_regions for each row execute function update_timestamp_dw();
create trigger trg_dim_segments_update before update on dim_segments for each row execute function update_timestamp_dw();
create trigger trg_dim_categories_update before update on dim_categories for each row execute function update_timestamp_dw();
create trigger trg_dim_teams_update before update on dim_teams for each row execute function update_timestamp_dw();
create trigger trg_dim_reason_categories_update before update on dim_reason_categories for each row execute function update_timestamp_dw();
create trigger trg_dim_reasons_update before update on dim_reasons for each row execute function update_timestamp_dw();
create trigger trg_dim_channels_update before update on dim_channels for each row execute function update_timestamp_dw();
create trigger trg_dim_priorities_update before update on dim_priorities for each row execute function update_timestamp_dw();
create trigger trg_dim_customers_update before update on dim_customers for each row execute function update_timestamp_dw();
create trigger trg_dim_restaurants_update before update on dim_restaurants for each row execute function update_timestamp_dw();
create trigger trg_dim_drivers_update before update on dim_drivers for each row execute function update_timestamp_dw();
create trigger trg_dim_agents_update before update on dim_agents for each row execute function update_timestamp_dw();
create trigger trg_fact_orders_update before update on fact_orders for each row execute function update_timestamp_dw();
create trigger trg_fact_tickets_update before update on fact_tickets for each row execute function update_timestamp_dw();
