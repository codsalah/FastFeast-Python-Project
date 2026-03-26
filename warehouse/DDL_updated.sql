CREATE TABLE "dim_date" (
  "date_key" integer PRIMARY KEY,
  "full_date" date NOT NULL,
  "day" integer NOT NULL,
  "month" integer NOT NULL,
  "year" integer NOT NULL,
  "quarter" integer NOT NULL,
  "day_of_week" varchar NOT NULL,
  "hour" integer NOT NULL,
  "time_of_day" varchar NOT NULL,
  "is_weekend" boolean NOT NULL,
  "is_holiday" boolean NOT NULL DEFAULT false
);

CREATE TABLE "dim_customer" (
  "customer_key" serial PRIMARY KEY,
  "customer_id" integer,
  "customer_name_masked" varchar,
  "gender" varchar,
  "segment_name" varchar,
  "region_name" varchar,
  "city_name" varchar,
  "signup_date" date,
  "valid_from" date NOT NULL,
  "valid_to" date,
  "is_current" boolean NOT NULL DEFAULT true
);

CREATE TABLE "dim_driver" (
  "driver_key" serial PRIMARY KEY,
  "driver_id" integer,
  "driver_name" varchar,
  "vehicle_type" varchar,
  "shift" varchar,
  "region_name" varchar,
  "city_name" varchar,
  "is_active" boolean,
  "valid_from" date NOT NULL,
  "valid_to" date,
  "is_current" boolean NOT NULL DEFAULT true
);

CREATE TABLE "dim_restaurant" (
  "restaurant_key" serial PRIMARY KEY,
  "restaurant_id" integer,
  "restaurant_name" varchar,
  "category_name" varchar,
  "price_tier" varchar,
  "region_name" varchar,
  "city_name" varchar,
  "rating_avg" decimal(3,2),
  "is_active" boolean,
  "valid_from" date NOT NULL,
  "valid_to" date,
  "is_current" boolean NOT NULL DEFAULT true
);

CREATE TABLE "dim_agent" (
  "agent_key" serial PRIMARY KEY,
  "agent_id" integer,
  "agent_name" varchar,
  "skill_level" varchar,
  "team_name" varchar,
  "is_active" boolean,
  "valid_from" date NOT NULL,
  "valid_to" date,
  "is_current" boolean NOT NULL DEFAULT true
);

CREATE TABLE "dim_reason" (
  "reason_key" serial PRIMARY KEY,
  "reason_id" integer,
  "reason_name" varchar,
  "reason_category_name" varchar,
  "severity_level" smallint,
  "typical_refund_pct" decimal(5,4)
);

CREATE TABLE "dim_channel" (
  "channel_key" serial PRIMARY KEY,
  "channel_id" integer,
  "channel_name" varchar
);

CREATE TABLE "dim_priority" (
  "priority_key" serial PRIMARY KEY,
  "priority_id" integer,
  "priority_code" varchar,
  "priority_name" varchar,
  "sla_first_response_min" integer NOT NULL,
  "sla_resolution_min" integer NOT NULL
);

CREATE TABLE "fact_orders" (
  "order_key" serial PRIMARY KEY,
  "order_id" varchar NOT NULL,
  "customer_key" integer NOT NULL,
  "driver_key" integer NOT NULL,
  "restaurant_key" integer NOT NULL,
  "date_key" integer NOT NULL,
  "order_amount" decimal(10,2),
  "delivery_fee" decimal(10,2),
  "discount_amount" decimal(10,2),
  "total_amount" decimal(10,2),
  "order_status" varchar NOT NULL,
  "payment_method" varchar,
  "order_created_at" timestamp NOT NULL,
  "delivered_at" timestamp,
  "original_orphan_customer_id" integer,
  "original_orphan_driver_id" integer,
  "version" smallint NOT NULL DEFAULT 1,
  "is_backfilled" boolean NOT NULL DEFAULT false
);

CREATE TABLE "fact_tickets" (
  "ticket_key" serial PRIMARY KEY,
  "ticket_id" varchar NOT NULL,
  "order_key" integer NOT NULL,
  "order_id" varchar NOT NULL,
  "customer_key" integer NOT NULL,
  "driver_key" integer,
  "restaurant_key" integer,
  "agent_key" integer NOT NULL,
  "reason_key" integer NOT NULL,
  "priority_key" integer NOT NULL,
  "channel_key" integer NOT NULL,
  "date_key" integer NOT NULL,
  "status" varchar NOT NULL,
  "refund_amount" decimal(10,2),
  "sla_first_response_breached" boolean,
  "sla_resolution_breached" boolean,
  "first_response_minutes" decimal(8,2),
  "resolution_minutes" decimal(8,2),
  "created_at" timestamp NOT NULL,
  "first_response_at" timestamp,
  "resolved_at" timestamp,
  "sla_first_due_at" timestamp,
  "sla_resolve_due_at" timestamp
);

CREATE TABLE "fact_ticket_events" (
  "event_key" serial PRIMARY KEY,
  "event_id" varchar NOT NULL,
  "ticket_key" integer NOT NULL,
  "agent_key" integer NOT NULL,
  "date_key" integer NOT NULL,
  "old_status" varchar,
  "new_status" varchar NOT NULL,
  "event_timestamp" timestamp NOT NULL,
  "notes" text NOT NULL
);

CREATE TABLE "orphan_tracking" (
  "tracking_id" serial PRIMARY KEY,
  "order_id" varchar NOT NULL,
  "orphan_type" varchar NOT NULL,
  "raw_id" integer NOT NULL,
  "is_resolved" boolean NOT NULL DEFAULT false,
  "retry_count" smallint NOT NULL DEFAULT 0,
  "detected_at" timestamp NOT NULL,
  "resolved_at" timestamp
);

CREATE TABLE "quarantine" (
  "quarantine_id" serial PRIMARY KEY,
  "source_file" varchar NOT NULL,
  "entity_type" varchar NOT NULL,
  "raw_record" jsonb NOT NULL,
  "error_type" varchar NOT NULL,
  "error_details" text NOT NULL,
  "orphan_type" varchar,
  "raw_orphan_id" varchar,
  "pipeline_run_id" integer,
  "quarantined_at" timestamp NOT NULL DEFAULT (now())
);

CREATE TABLE "pipeline_run_log" (
  "run_id" serial PRIMARY KEY,
  "run_type" varchar NOT NULL,
  "run_date" date NOT NULL,
  "status" varchar NOT NULL,
  "started_at" timestamp NOT NULL,
  "completed_at" timestamp,
  "total_files" integer DEFAULT 0,
  "successful_files" integer DEFAULT 0,
  "failed_files" integer DEFAULT 0,
  "total_records" integer DEFAULT 0,
  "total_loaded" integer DEFAULT 0,
  "total_quarantined" integer DEFAULT 0,
  "total_orphaned" integer DEFAULT 0,
  "error_message" text
);

CREATE TABLE "file_tracker" (
  "file_id" serial PRIMARY KEY,
  "file_path" varchar NOT NULL,
  "file_hash" varchar NOT NULL,
  "file_type" varchar NOT NULL,
  "records_total" integer,
  "records_loaded" integer,
  "records_quarantined" integer,
  "status" varchar NOT NULL,
  "processed_at" timestamp NOT NULL,
  "pipeline_run_id" integer
);

CREATE TABLE "pipeline_quality_metrics" (
  "metric_id" serial PRIMARY KEY,
  "run_id" integer NOT NULL,
  "run_date" date NOT NULL,
  "table_name" varchar NOT NULL,
  "source_file" varchar,
  "total_records" integer,
  "valid_records" integer,
  "quarantined_records" integer,
  "orphaned_records" integer,
  "duplicate_count" integer,
  "null_violations" integer,
  "duplicate_rate" decimal(6,4),
  "orphan_rate" decimal(6,4),
  "null_rate" decimal(6,4),
  "quarantine_rate" decimal(6,4),
  "processing_latency_sec" decimal(10,2),
  "quality_details" jsonb,
  "recorded_at" timestamp NOT NULL
);

-- ==========================================
-- UNIQUE CONSTRAINTS
-- ==========================================
CREATE UNIQUE INDEX ON "file_tracker" ("file_path", "file_hash");

-- ==========================================
-- COMMENTS
-- ==========================================
COMMENT ON TABLE "dim_date" IS 'SCD Type 0 — generated once by pipeline. Never changes.';
COMMENT ON COLUMN "dim_date"."date_key" IS 'YYYYMMDD e.g. 20260220';
COMMENT ON COLUMN "dim_date"."day_of_week" IS 'Monday, Tuesday, etc.';
COMMENT ON COLUMN "dim_date"."hour" IS '0 to 23';
COMMENT ON COLUMN "dim_date"."time_of_day" IS 'morning 6-11, afternoon 12-17, evening 18-21, night 22-5';
COMMENT ON TABLE "dim_customer" IS 'SCD Type 2. Tracked: segment_name, region_name. PII: email/phone excluded entirely.';
COMMENT ON COLUMN "dim_customer"."customer_key" IS '-1 = Unknown member for orphans. Pre-loaded in seed.sql.';
COMMENT ON COLUMN "dim_customer"."customer_id" IS 'Natural key from source. NULL for Unknown.';
COMMENT ON COLUMN "dim_customer"."customer_name_masked" IS 'PII masked: first letter + *** e.g. A. ***';
COMMENT ON COLUMN "dim_customer"."segment_name" IS 'Regular or VIP. Merged from segments.csv.';
COMMENT ON COLUMN "dim_customer"."region_name" IS 'Merged from regions.csv. Denormalized.';
COMMENT ON COLUMN "dim_customer"."city_name" IS 'Merged from cities.csv. Denormalized.';
COMMENT ON COLUMN "dim_customer"."valid_from" IS 'SCD Type 2 — start of this version';
COMMENT ON COLUMN "dim_customer"."valid_to" IS 'SCD Type 2 — NULL means current active record';
COMMENT ON TABLE "dim_driver" IS 'SCD Type 2. Tracked: is_active, region_name. PII excluded: driver_phone, national_id.';
COMMENT ON COLUMN "dim_driver"."driver_key" IS '-1 = Unknown member for orphans.';
COMMENT ON COLUMN "dim_driver"."driver_id" IS 'Natural key. NULL for Unknown.';
COMMENT ON COLUMN "dim_driver"."driver_name" IS 'Full name stored. Acceptable — drivers are known to customers. Review against privacy regs in production.';
COMMENT ON COLUMN "dim_driver"."vehicle_type" IS 'bike, motorbike, car';
COMMENT ON COLUMN "dim_driver"."shift" IS 'morning, evening, night';
COMMENT ON COLUMN "dim_driver"."region_name" IS 'Merged from regions.csv.';
COMMENT ON COLUMN "dim_driver"."city_name" IS 'Merged from cities.csv.';
COMMENT ON TABLE "dim_restaurant" IS 'SCD Type 2 for is_active. SCD Type 1 for rating_avg.';
COMMENT ON COLUMN "dim_restaurant"."restaurant_id" IS 'Natural key';
COMMENT ON COLUMN "dim_restaurant"."category_name" IS 'Merged from categories.csv.';
COMMENT ON COLUMN "dim_restaurant"."price_tier" IS 'Low, Mid, High';
COMMENT ON COLUMN "dim_restaurant"."region_name" IS 'Merged from regions.csv.';
COMMENT ON COLUMN "dim_restaurant"."city_name" IS 'Merged from cities.csv.';
COMMENT ON COLUMN "dim_restaurant"."rating_avg" IS 'SCD Type 1 — overwritten daily';
COMMENT ON COLUMN "dim_restaurant"."is_active" IS 'SCD Type 2 tracked';
COMMENT ON TABLE "dim_agent" IS 'SCD Type 2. Tracked: team_name, skill_level, is_active. PII excluded: agent_email, agent_phone.';
COMMENT ON COLUMN "dim_agent"."agent_id" IS 'Natural key';
COMMENT ON COLUMN "dim_agent"."agent_name" IS 'PII masked: A. ***';
COMMENT ON COLUMN "dim_agent"."skill_level" IS 'Junior, Mid, Senior, Lead';
COMMENT ON COLUMN "dim_agent"."team_name" IS 'Merged from teams.csv.';
COMMENT ON TABLE "dim_reason" IS 'SCD Type 1. Rarely changes. ~10 rows.';
COMMENT ON COLUMN "dim_reason"."reason_id" IS 'Natural key';
COMMENT ON COLUMN "dim_reason"."reason_category_name" IS 'Delivery, Food, Payment — merged from reason_categories.csv';
COMMENT ON COLUMN "dim_reason"."typical_refund_pct" IS 'Stored as 0.5000 = 50%. decimal(5,4) matches source precision.';
COMMENT ON TABLE "dim_channel" IS 'SCD Type 0. Never changes. 4 rows.';
COMMENT ON COLUMN "dim_channel"."channel_id" IS 'Natural key';
COMMENT ON COLUMN "dim_channel"."channel_name" IS 'app, chat, phone, email';
COMMENT ON COLUMN "dim_priority"."priority_id" IS 'Natural key';
COMMENT ON COLUMN "dim_priority"."priority_code" IS 'P1, P2, P3, P4';
COMMENT ON COLUMN "dim_priority"."priority_name" IS 'Critical, High, Medium, Low';
COMMENT ON TABLE "fact_orders" IS 'Grain: one row per order per version. UNIQUE on (order_id, version). Analysts use orders_clean VIEW.';
COMMENT ON COLUMN "fact_orders"."order_id" IS 'UUID from source';
COMMENT ON COLUMN "fact_orders"."customer_key" IS '-1 if orphan';
COMMENT ON COLUMN "fact_orders"."driver_key" IS '-1 if orphan';
COMMENT ON COLUMN "fact_orders"."order_amount" IS 'No default — null triggers validation error';
COMMENT ON COLUMN "fact_orders"."order_status" IS 'Delivered, Cancelled, Refunded';
COMMENT ON COLUMN "fact_orders"."payment_method" IS 'card, cash, wallet';
COMMENT ON COLUMN "fact_orders"."delivered_at" IS 'NULL if not Delivered';
COMMENT ON COLUMN "fact_orders"."original_orphan_customer_id" IS 'Raw orphan ID stored for reconciliation';
COMMENT ON COLUMN "fact_orders"."original_orphan_driver_id" IS 'Raw orphan ID stored for reconciliation';
COMMENT ON COLUMN "fact_orders"."version" IS '1=original, 2=backfilled';
COMMENT ON TABLE "fact_tickets" IS 'Grain: one row per ticket. SLA flags pre-calculated at insert time. SLA due timestamps kept for auditability.';
COMMENT ON COLUMN "fact_tickets"."ticket_id" IS 'UUID from source';
COMMENT ON COLUMN "fact_tickets"."order_id" IS 'UUID from source';
COMMENT ON COLUMN "fact_tickets"."customer_key" IS 'Inherited from order. -1 if orphan. Stays -1 even after backfill.';
COMMENT ON COLUMN "fact_tickets"."status" IS 'Open, InProgress, Resolved, Closed, Reopened';
COMMENT ON COLUMN "fact_tickets"."sla_first_response_breached" IS 'Pre-calculated: first_response_at > sla_first_due_at';
COMMENT ON COLUMN "fact_tickets"."sla_resolution_breached" IS 'Pre-calculated: resolved_at > sla_resolve_due_at';
COMMENT ON COLUMN "fact_tickets"."first_response_minutes" IS 'Minutes from created to first response';
COMMENT ON COLUMN "fact_tickets"."resolution_minutes" IS 'Minutes from created to resolution';
COMMENT ON COLUMN "fact_tickets"."sla_first_due_at" IS 'Audit: SLA deadline for first response. Used to compute sla_first_response_breached.';
COMMENT ON COLUMN "fact_tickets"."sla_resolve_due_at" IS 'Audit: SLA deadline for resolution. Used to compute sla_resolution_breached.';
COMMENT ON TABLE "fact_ticket_events" IS 'Grain: one row per status change. Links to tickets, NOT directly to orders.';
COMMENT ON COLUMN "fact_ticket_events"."event_id" IS 'UUID from source';
COMMENT ON COLUMN "fact_ticket_events"."old_status" IS 'NULL on first event (Open)';
COMMENT ON COLUMN "fact_ticket_events"."new_status" IS 'Open, InProgress, Resolved, Closed, Reopened';
COMMENT ON TABLE "orphan_tracking" IS 'One row per orphan type per order. Backfill triggers only when ALL rows for an order resolved.';
COMMENT ON COLUMN "orphan_tracking"."order_id" IS 'Natural key string — no FK (order may not exist yet)';
COMMENT ON COLUMN "orphan_tracking"."orphan_type" IS 'customer, driver, restaurant';
COMMENT ON COLUMN "orphan_tracking"."raw_id" IS 'The orphan ID from source data';
COMMENT ON COLUMN "orphan_tracking"."retry_count" IS 'Max = retry_max_days from config (3)';
COMMENT ON TABLE "quarantine" IS 'Stores all failed/rejected records from the pipeline for later review and reprocessing.';
COMMENT ON COLUMN "quarantine"."source_file" IS 'File that contained the failed record';
COMMENT ON COLUMN "quarantine"."entity_type" IS 'e.g. order, ticket, customer, driver';
COMMENT ON COLUMN "quarantine"."raw_record" IS 'The full original record that failed';
COMMENT ON COLUMN "quarantine"."error_type" IS 'schema_validation | orphan | parse_error | referential_integrity';
COMMENT ON COLUMN "quarantine"."error_details" IS 'Full error message for debugging';
COMMENT ON COLUMN "quarantine"."orphan_type" IS 'e.g. missing_order, missing_driver — null if not an orphan failure';
COMMENT ON COLUMN "quarantine"."raw_orphan_id" IS 'The unresolved ID that caused orphan — varchar to support UUIDs';
COMMENT ON TABLE "pipeline_run_log" IS 'One row per pipeline execution. Parent for file_tracker and pipeline_quality_metrics.';
COMMENT ON COLUMN "pipeline_run_log"."run_type" IS 'batch, stream, reconciliation';
COMMENT ON COLUMN "pipeline_run_log"."status" IS 'running, success, partial, failed';
COMMENT ON TABLE "file_tracker" IS 'Idempotency Layer 1. Hash checked before any parsing begins.';
COMMENT ON COLUMN "file_tracker"."file_hash" IS 'SHA-256. Computed BEFORE parsing.';
COMMENT ON COLUMN "file_tracker"."file_type" IS 'batch or stream';
COMMENT ON COLUMN "file_tracker"."status" IS 'success, partial, failed';
COMMENT ON TABLE "pipeline_quality_metrics" IS 'One row per entity per run. quality_details JSONB stores per-column null rates — same info as a separate table, half the code.';
COMMENT ON COLUMN "pipeline_quality_metrics"."table_name" IS 'e.g. fact_orders, dim_customer';
COMMENT ON COLUMN "pipeline_quality_metrics"."quality_details" IS 'Per-column metrics as JSON. Replaces column_quality_metrics table.';

-- ==========================================
-- FOREIGN KEYS
-- ==========================================
ALTER TABLE "fact_orders" ADD FOREIGN KEY ("customer_key") REFERENCES "dim_customer" ("customer_key") DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "fact_orders" ADD FOREIGN KEY ("driver_key") REFERENCES "dim_driver" ("driver_key") DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "fact_orders" ADD FOREIGN KEY ("restaurant_key") REFERENCES "dim_restaurant" ("restaurant_key") DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "fact_orders" ADD FOREIGN KEY ("date_key") REFERENCES "dim_date" ("date_key") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "fact_tickets" ADD FOREIGN KEY ("order_key") REFERENCES "fact_orders" ("order_key") DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "fact_tickets" ADD FOREIGN KEY ("customer_key") REFERENCES "dim_customer" ("customer_key") DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "fact_tickets" ADD FOREIGN KEY ("driver_key") REFERENCES "dim_driver" ("driver_key") DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "fact_tickets" ADD FOREIGN KEY ("restaurant_key") REFERENCES "dim_restaurant" ("restaurant_key") DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "fact_tickets" ADD FOREIGN KEY ("agent_key") REFERENCES "dim_agent" ("agent_key") DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "fact_tickets" ADD FOREIGN KEY ("reason_key") REFERENCES "dim_reason" ("reason_key") DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "fact_tickets" ADD FOREIGN KEY ("priority_key") REFERENCES "dim_priority" ("priority_key") DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "fact_tickets" ADD FOREIGN KEY ("channel_key") REFERENCES "dim_channel" ("channel_key") DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "fact_tickets" ADD FOREIGN KEY ("date_key") REFERENCES "dim_date" ("date_key") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "fact_ticket_events" ADD FOREIGN KEY ("ticket_key") REFERENCES "fact_tickets" ("ticket_key") DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "fact_ticket_events" ADD FOREIGN KEY ("agent_key") REFERENCES "dim_agent" ("agent_key") DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "fact_ticket_events" ADD FOREIGN KEY ("date_key") REFERENCES "dim_date" ("date_key") DEFERRABLE INITIALLY IMMEDIATE;

ALTER TABLE "quarantine" ADD FOREIGN KEY ("pipeline_run_id") REFERENCES "pipeline_run_log" ("run_id") DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "file_tracker" ADD FOREIGN KEY ("pipeline_run_id") REFERENCES "pipeline_run_log" ("run_id") DEFERRABLE INITIALLY IMMEDIATE;
ALTER TABLE "pipeline_quality_metrics" ADD FOREIGN KEY ("run_id") REFERENCES "pipeline_run_log" ("run_id") DEFERRABLE INITIALLY IMMEDIATE;

-- ==========================================
-- PERFORMANCE INDEXES (Foreign Keys)
-- ==========================================
-- fact_orders indexes
CREATE INDEX idx_fact_orders_customer_key ON "fact_orders"("customer_key");
CREATE INDEX idx_fact_orders_driver_key ON "fact_orders"("driver_key");
CREATE INDEX idx_fact_orders_restaurant_key ON "fact_orders"("restaurant_key");
CREATE INDEX idx_fact_orders_date_key ON "fact_orders"("date_key");

-- fact_tickets indexes
CREATE INDEX idx_fact_tickets_order_key ON "fact_tickets"("order_key");
CREATE INDEX idx_fact_tickets_customer_key ON "fact_tickets"("customer_key");
CREATE INDEX idx_fact_tickets_driver_key ON "fact_tickets"("driver_key");
CREATE INDEX idx_fact_tickets_restaurant_key ON "fact_tickets"("restaurant_key");
CREATE INDEX idx_fact_tickets_agent_key ON "fact_tickets"("agent_key");
CREATE INDEX idx_fact_tickets_reason_key ON "fact_tickets"("reason_key");
CREATE INDEX idx_fact_tickets_priority_key ON "fact_tickets"("priority_key");
CREATE INDEX idx_fact_tickets_channel_key ON "fact_tickets"("channel_key");
CREATE INDEX idx_fact_tickets_date_key ON "fact_tickets"("date_key");

-- fact_ticket_events indexes
CREATE INDEX idx_fact_ticket_events_ticket_key ON "fact_ticket_events"("ticket_key");
CREATE INDEX idx_fact_ticket_events_agent_key ON "fact_ticket_events"("agent_key");
CREATE INDEX idx_fact_ticket_events_date_key ON "fact_ticket_events"("date_key");

-- Pipeline metadata indexes
CREATE INDEX idx_quarantine_pipeline_run_id ON "quarantine"("pipeline_run_id");
CREATE INDEX idx_file_tracker_pipeline_run_id ON "file_tracker"("pipeline_run_id");
CREATE INDEX idx_pipeline_quality_metrics_run_id ON "pipeline_quality_metrics"("run_id");