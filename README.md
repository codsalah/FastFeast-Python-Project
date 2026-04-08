# FastFeast — Food Delivery Data Pipeline

> A production-grade **OLTP → OLAP** pipeline for a simulated Egyptian food delivery platform.  
> Ingests daily dimension snapshots and hourly transaction streams, loads a PostgreSQL star-schema warehouse, and serves a Streamlit analytics dashboard.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Quick Start](#quick-start)
3. [Pipeline Concepts](#pipeline-concepts)
   - [Batch vs. Stream](#batch-vs-stream)
   - [SCD2 — Slowly Changing Dimensions](#scd2--slowly-changing-dimensions)
   - [Orphan Lifecycle](#orphan-lifecycle)
   - [Validation Stages](#validation-stages)
   - [Idempotency](#idempotency)
4. [Warehouse Schema](#warehouse-schema)
5. [CLI Reference](#cli-reference)
6. [Input File Reference](#input-file-reference)
7. [Configuration](#configuration)
8. [Project Structure](#project-structure)
9. [Data Generators](#data-generators)
10. [Testing](#testing)
11. [Troubleshooting](#troubleshooting)

---

## Architecture Overview

```
╔══════════════════════════════════════════════════════════════════════╗
║  SOURCES (simulated OLTP exports)                                    ║
║  Batch (daily)               Stream (hourly)                         ║
║  customers / drivers /       orders / tickets /                      ║
║  restaurants / agents        ticket events                           ║
╚══════════════╤═══════════════════════════╤═════════════════════════╝
               │                           │
               ▼                           ▼
      ┌─────────────────┐        ┌──────────────────────┐
      │  Schema          │        │  Schema               │
      │  Validator       │        │  Validator            │
      │  (38 contracts)  │        │  (3 schemas)          │
      └────────┬────────┘        └──────────┬───────────┘
               │ quarantine bad             │ quarantine bad
               ▼                           ▼
      ┌─────────────────┐        ┌──────────────────────┐
      │  SCD2 Loaders   │        │  Fact Loaders         │
      │  detect changes │        │  orphan FK → -1       │
      │  version history│        │  surrogate resolve    │
      └────────┬────────┘        │  SLA flag calculate   │
               │                 └──────────┬───────────┘
               └────────────┬──────────────┘
                            │
                            ▼
                  ┌─────────────────────┐
                  │  Reconciliation Job  │◄── auto-runs after batch
                  │  resolve orphans     │
                  │  insert v2 fact rows │
                  └──────────┬──────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │  PostgreSQL          │
                  │  Warehouse           │
                  │  warehouse.*         │
                  │  pipeline_audit.*    │
                  └──────────┬──────────┘
                             │
                             ▼
                  ┌─────────────────────┐
                  │  Streamlit           │
                  │  Dashboard           │
                  │  localhost:8501      │
                  └─────────────────────┘
```

**Key design principles:**
- The pipeline **never halts** on bad data — corrupt or unresolvable records are quarantined, not dropped silently.
- Execution is **idempotent** — running the same file twice produces the same result (no duplicates).
- Orphaned facts are **loaded immediately** with a placeholder FK (`-1`) and corrected automatically once the dimension arrives.

---

## Quick Start

**Prerequisites:** Python 3.11+, Docker + Docker Compose

```bash
# 1. Clone and install
git clone <repo-url> && cd FastFeast-Python-Project
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Configure secrets
cp .env.example .env
# Set POSTGRES_PASSWORD and generate a PII pepper:
python -c "import secrets; print(secrets.token_hex(32))"
# Paste the output as PII_HASH_PEPPER in .env

# 3. Start PostgreSQL
docker-compose up -d

# 4. Initialise the warehouse (DDL + seed Unknown-member rows)
python main.py init-db --with-seed

# 5. Generate synthetic data and run the pipeline for one day
python data_generators/generate_master_data.py
python data_generators/generate_batch_data.py --date 2026-04-10
python main.py batch --date 2026-04-10

python data_generators/generate_stream_data.py --date 2026-04-10 --hour 12
python main.py stream --date 2026-04-10 --hour 12

# 6. Verify everything passed
python tests/test_pipeline_full.py --date 2026-04-10 --reset
# Expected: PASSED: 62  WARNED: 0  FAILED: 0

# 7. Launch the analytics dashboard
python main.py analytics setup      # run once to create OLAP views
python main.py analytics dashboard  # open http://localhost:8501
```

---

## Pipeline Concepts

### Batch vs. Stream

| | Batch | Stream |
|---|---|---|
| **Frequency** | Once per day | Multiple times per hour |
| **Content** | Full dimension snapshots | Incremental transaction records |
| **Input directory** | `data/input/batch/YYYY-MM-DD/` | `data/input/stream/YYYY-MM-DD/HH/` |
| **Loads** | `dim_*` tables | `fact_orders`, `fact_tickets`, `fact_ticket_events` |
| **Key concern** | SCD change detection | Orphan FK resolution |

Batch runs first each day; the reconciliation job fires automatically afterwards to resolve any orphans that the stream loaded earlier with a `-1` placeholder.

---

### SCD2 — Slowly Changing Dimensions

When a **tracked field** changes on a dimension (e.g. a customer moves region), the pipeline:

1. **Closes** the old row by setting `valid_to = today` and `is_current = false`.
2. **Inserts** a new row with `valid_from = today`, `valid_to = NULL`, and `is_current = true`.

This preserves the full history so analytical queries can reconstruct "what did we know at the time of an order?".

| Dimension | Tracked (SCD2) — creates new version | In-place (SCD1) — overwrites |
|---|---|---|
| `dim_customer` | `segment_name`, `region_name` | — |
| `dim_driver` | `is_active`, `region_name` | — |
| `dim_restaurant` | `is_active`, `price_tier` | `rating_avg` |
| `dim_agent` | `team_name`, `skill_level`, `is_active` | — |

Static dimensions (`dim_reason`, `dim_channel`, `dim_priority`, `dim_date`) never version — they use their natural keys as primary keys.

---

### Orphan Lifecycle

A stream fact (e.g. an order) may arrive **before** its dimension row is loaded by batch. The pipeline never blocks ingestion — it follows three automatic steps:

```
STEP 1 — Detect (during stream load)
  └─► Missing FK? Set to -1. Save raw source ID. Write row to orphan_tracking.

STEP 2 — Reconcile (auto-runs after each batch)
  └─► Dimension has now arrived? Mark orphan as resolved in orphan_tracking.

STEP 3 — Backfill (post-reconcile)
  └─► Insert fact_orders v2 with the real FK.
      Both v1 (audit) and v2 (corrected) are kept.
```

**Analysts always query `warehouse.orders_clean`**, a view that returns only the latest version per order:

```sql
-- orders_clean automatically hides v1 if v2 exists
SELECT * FROM warehouse.orders_clean WHERE order_id = 'abc-123';
```

If an orphan is not resolved within 3 retries (i.e. after 3 batch runs), it is moved to `pipeline_audit.quarantine`.

---

### Validation Stages

Every file passes through three validation stages before any data reaches the warehouse:

| Stage | Checks | On failure |
|---|---|---|
| **Structural** | Required columns present, correct file format | Entire file rejected |
| **Critical** | Type correctness, null constraints, value ranges, regex patterns | Single row quarantined |
| **Logical** | Cross-field business rules (e.g. `delivered_at > order_created_at`) | Row loads; violation counted in quality metrics |

38 schema contracts cover every input entity. Quarantined records are stored in `pipeline_audit.quarantine` as full JSON with error details, so they can be inspected and reprocessed.

```sql
-- Inspect recent quarantine reasons
SELECT entity_type, error_details, COUNT(*)
FROM pipeline_audit.quarantine
GROUP BY entity_type, error_details
ORDER BY count DESC;
```

---

### Idempotency

The pipeline uses two layers to guarantee running the same file twice has no effect:

1. **File level** — SHA-256 hash of the file is stored in `file_tracker` before parsing begins. Same hash → file is skipped entirely.
2. **Row level** — All fact inserts use `ON CONFLICT DO NOTHING`.

---

## Warehouse Schema

### Star Schema Overview

```
                      dim_date
                          │
dim_customer ─────────────┤
dim_driver   ─────────────┼──► fact_orders ──────────► fact_tickets ──► fact_ticket_events
dim_restaurant ───────────┤                                  │
                          │                             dim_agent
                    dim_priority                        dim_reason
                    dim_channel                         dim_priority
                                                        dim_channel
```

### Dimensions

| Table | SCD Type | PK | Notes |
|---|---|---|---|
| `dim_date` | Type 0 (static) | `date_key` | One row per hour; covers 2020–2030 (~96,000 rows) |
| `dim_customer` | Type 2 | `customer_key` | `-1` = Unknown member; PII masked |
| `dim_driver` | Type 2 | `driver_key` | `-1` = Unknown member |
| `dim_restaurant` | Type 2 + 1 | `restaurant_key` | `-1` = Unknown; `rating_avg` overwrites |
| `dim_agent` | Type 2 | `agent_key` | PII masked; no `-1` seed needed |
| `dim_reason` | Type 0 | `reason_id` | ~10 rows; natural key as PK |
| `dim_channel` | Type 0 | `channel_id` | 4 rows (app, chat, phone, email) |
| `dim_priority` | Type 0 | `priority_id` | Holds SLA thresholds per priority level |

### Fact Tables

| Table | Grain | Key Notes |
|---|---|---|
| `fact_orders` | One row per order per version | `version=1` original; `version=2` backfilled after orphan resolved. Query via `orders_clean` view. |
| `fact_tickets` | One row per ticket | SLA breach flags pre-calculated as booleans at insert time. |
| `fact_ticket_events` | One row per status transition | Links to `fact_tickets`, not directly to orders. |

### Audit Tables (`pipeline_audit.*`)

| Table | Purpose |
|---|---|
| `pipeline_run_log` | One row per pipeline execution — status, file counts, record totals |
| `file_tracker` | SHA-256 hash + status per file for idempotency |
| `pipeline_quality_metrics` | Quarantine rate, null rate, orphan rate per entity per run |
| `orphan_tracking` | Per-orphan resolution status and retry count |
| `quarantine` | Full JSON of every rejected record with error details |

---

## CLI Reference

| Command | Description |
|---|---|
| `python main.py init-db` | Apply DDL; populate `dim_date` |
| `python main.py init-db --with-seed` | Same + insert `-1` Unknown member rows **(required on first run)** |
| `python main.py batch --date YYYY-MM-DD` | Load daily dimension snapshot |
| `python main.py stream --date YYYY-MM-DD --hour HH` | Load one hour of fact files |
| `python main.py stream --watch` | Continuous daemon — polls every 15–30 s |
| `python main.py analytics setup` | Create the 6 OLAP views (run once) |
| `python main.py analytics dashboard` | Launch Streamlit at `localhost:8501` |

---

## Input File Reference

### Batch — `data/input/batch/<YYYY-MM-DD>/`

| File | Loads into | Format |
|---|---|---|
| `customers.csv` | `dim_customer` (SCD2) | CSV |
| `drivers.csv` | `dim_driver` (SCD2) | CSV |
| `restaurants.json` | `dim_restaurant` (SCD2) | JSON |
| `agents.csv` | `dim_agent` (SCD2) | CSV |
| `channels.csv` | `dim_channel` (static) | CSV |
| `priorities.csv` | `dim_priority` (static) | CSV |
| `reasons.csv` | `dim_reason` (static) | CSV |

### Stream — `data/input/stream/<YYYY-MM-DD>/<HH>/`

| File | Loads into | Format |
|---|---|---|
| `orders.json` | `fact_orders` | JSON |
| `tickets.csv` | `fact_tickets` | CSV |
| `ticket_events.json` | `fact_ticket_events` | JSON |

---

## Configuration

Copy `.env.example` to `.env` and edit before running.

**Required:**

```env
POSTGRES_PASSWORD=your_password
PII_HASH_PEPPER=<64-char hex — generate with: python -c "import secrets; print(secrets.token_hex(32))">
```

**Key optional settings:**

```env
BATCH_INPUT_DIR=data/input/batch
STREAM_INPUT_DIR=data/input/stream

DB_POOL_MIN=2
DB_POOL_MAX=10

MAX_ORPHAN_RATE=0.50       # alert if orphan rate exceeds 50%
ALERTING_ENABLED=false     # set true + configure SMTP to enable email alerts
```

All settings are documented in `.env.example`.

---

## Project Structure

```
FastFeast-Python-Project/
│
├── main.py                        # CLI entrypoint (init-db | batch | stream | analytics)
│
├── config/
│   ├── settings.py                # Pydantic settings model (reads .env)
│   ├── orchestrator.py            # Wires connection pool, DDL, routes commands
│   └── schema_manager.py          # Applies DDL; seeds Unknown member rows
│
├── warehouse/
│   ├── connection.py              # Connection pool helpers
│   ├── dwh_ddl.sql                # Star schema DDL (dimensions + facts)
│   ├── audit_ddl.sql              # pipeline_audit schema DDL
│   ├── analytics_ddl.sql          # 6 OLAP views for dashboard
│   └── seed.sql                   # -1 Unknown member rows for SCD2 dims
│
├── loaders/
│   ├── base_scd2_loader.py        # Abstract base: change detection, version history
│   ├── dim_customer_loader.py     # SCD2 + PII masking
│   ├── dim_driver_loader.py       # SCD2 + PII exclusion
│   ├── dim_restaurant_loader.py   # SCD2 (is_active) + SCD1 (rating_avg)
│   ├── dim_agent_loader.py        # SCD2 + PII masking
│   ├── dim_date_loader.py         # Generates hourly rows for 2020–2030 (~96,000 rows)
│   ├── dim_static_loader.py       # UPSERT loader for reason/channel/priority
│   ├── fact_orders_loader.py      # Orders with orphan detection
│   ├── fact_tickets_loader.py     # Tickets with SLA flag calculation
│   └── fact_events_loader.py      # Ticket status events
│
├── pipelines/
│   ├── batch_pipeline.py          # Orchestrates daily dimension load
│   ├── stream_pipeline.py         # Orchestrates hourly fact load
│   ├── reconciliation_job.py      # Post-batch orphan resolution + backfill trigger
│   └── watcher.py                 # Continuous polling daemon (--watch mode)
│
├── handlers/
│   ├── orphan_handler.py          # Step 1 — write orphan_tracking rows
│   ├── backfill_handler.py        # Step 3 — insert fact v2 rows
│   └── quarantine_handler.py      # Route rejected records to quarantine table
│
├── validators/
│   ├── schema_registry.py         # 38 schema contracts (one per entity)
│   └── schema_validator.py        # 3-stage validation engine
│
├── quality/
│   ├── metrics_tracker.py         # Writes to pipeline_quality_metrics after each load
│   └── quality_report.py          # Generates optional PDF quality report
│
├── utils/
│   ├── file_tracker.py            # SHA-256 hashing + dedup check
│   ├── logger.py                  # Structured JSON logger
│   ├── PII_handler.py             # Name masking, hashing, two-layer encryption
│   ├── pii_policy.py              # Field exclusion / retention policy
│   └── date_utils.py              # Date parsing helpers
│
├── alerting/
│   └── alert_service.py           # Async SMTP alerting on pipeline failures
│
├── analytics/
│   └── dashboard.py               # Streamlit dashboard (reads OLAP views)
│
├── data_generators/               # Synthetic Egyptian-locale data generators
│   ├── generate_master_data.py    # Run once — creates base OLTP master records
│   ├── generate_batch_data.py     # Creates daily dimension snapshot files
│   ├── generate_stream_data.py    # Creates hourly transaction files
│   ├── add_new_customers.py       # Simulates mid-day customer signups
│   ├── add_new_drivers.py         # Simulates mid-day driver onboarding
│   └── simulate_day.py            # Runs all 24 stream hours for a full day
│
├── tests/
│   ├── test_schema.py             # 278 schema validation unit tests (no DB needed)
│   ├── test_loaders.py            # Loader logic unit tests
│   ├── test_e2e_pipeline.py       # Full integration test
│   └── test_pipeline_full.py      # Health check — 7 sections, 62 assertions
│
└── data/
    ├── master/                    # OLTP source master CSVs (generated once)
    ├── input/batch/               # Daily dimension snapshots
    ├── input/stream/              # Hourly fact files
    └── quarantine/                # Rejected records (local backup)
```

---

## Data Generators

Generators produce realistic Egyptian-locale data with ~5–15% intentionally dirty rows to exercise validation and quarantine logic.

```bash
# Run once at project start
python data_generators/generate_master_data.py

# Generate one day's batch files
python data_generators/generate_batch_data.py --date 2026-04-10

# Simulate mid-day signups (optional)
python data_generators/add_new_customers.py --count 5
python data_generators/add_new_drivers.py --count 3

# Generate one hour of stream data
python data_generators/generate_stream_data.py --date 2026-04-10 --hour 14

# Generate all 24 hours at once
python data_generators/simulate_day.py --date 2026-04-10
```

**Recommended execution order:**
1. `generate_master_data.py` — once only
2. `generate_batch_data.py` — each simulated day
3. `add_new_customers.py` / `add_new_drivers.py` — optional, simulates intra-day arrivals
4. `generate_stream_data.py` — one or more hours per day

---

## Troubleshooting

### Database won't connect

```bash
docker-compose ps                  # check container status
docker-compose up -d postgres      # restart if stopped
```

### FK constraint error on stream load (`customer_key=-1 not found`)

The Unknown member seed rows are missing. Run:

```bash
python main.py init-db --with-seed
```

### `PII_HASH_PEPPER` missing or invalid

Generate a new pepper and add it to `.env`:

```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

### Stream file silently skipped (already processed)

The file hash exists in `file_tracker`. Clear stream tracker entries and retry:

```bash
python reset_stream.py
```

### Orphans never resolve

Check whether the dimension record exists in the warehouse:

```bash
python check_orphans.py
```

If it's still missing, the source record was likely quarantined during batch (dirty data). Once a clean version arrives in a future batch run, the reconciliation job creates the v2 row automatically.

### High quarantine rate

Inspect the reasons directly:

```sql
SELECT entity_type, error_details, COUNT(*)
FROM pipeline_audit.quarantine
GROUP BY entity_type, error_details
ORDER BY count DESC;
```

### `reportlab` not installed (PDF quality report)

```bash
pip install reportlab
```

---

## Notes on PII Handling

| Field | Treatment |
|---|---|
| Customer `full_name` | Dropped — not stored in warehouse |
| Customer `email`, `phone` | SHA-256 hashed (stored as `email_hash`, `phone_hash`) |
| Agent name | Partial mask — first 3 chars kept, rest replaced with `*` |
| Agent email / phone | SHA-256 hashed |
| Driver name | Partial mask — first 3 chars kept, rest replaced with `*` |
| Driver phone / national ID | SHA-256 hashed |

The `PII_HASH_PEPPER` secret is used as a cryptographic salt for all hashed fields. Never commit it to version control.