"""
loaders/fact_orders_loader.py
─────────────────────────────
Loads micro-batch order records into warehouse.fact_orders.

ORPHAN HANDLING FLOW:
─────────────────────
This is the FIRST stage of orphan handling - DETECTION during initial load.

ORPHAN HANDLING:
  1. DETECTION (this file): FK=-1 for missing dims, store original ID in original_orphan_* columns,
     track in orphan_tracking table
  2. RESOLUTION (reconciliation_job + backfill_handler): When dims arrive via batch,
     create v2 with resolved FKs, keep v1 for audit

Pipeline contract (WAP pattern):
  1. Idempotency check   — skip if file already processed (SHA-256 + file_tracker)
  2. Read + validate     — quarantine schema-invalid records
  3. Deduplicate         — drop exact duplicate order_ids within the file
  4. Resolve surrogates  — map natural IDs → surrogate keys; -1 for orphans (SEE ORPHAN HANDLING ABOVE)
  5. Insert              — ON CONFLICT (order_id, version) DO NOTHING (layer-2 idempotency)
  6. Track orphans       — batch-insert into pipeline_audit.orphan_tracking
  7. Mark file done      — update file_tracker to 'success'

"""
from collections import defaultdict

import pandas as pd

from alerting.alert_service import send_alert
from handlers.quarantine_handler import send_batch_to_quarantine
from utils.file_tracker import (
    compute_file_hash,
    is_file_processed,
    mark_file_failed,
    mark_file_success,
    register_file
)
from utils.logger import (
    get_logger_name,
    log_record_rejected,
    log_stage_complete,
    log_stage_start
)
from utils.readers import reader
from utils.retry import db_retry
from utils.timing import StageTimer
from validators.schema_registry import get_contract
from validators.schema_validator import validate_entity
from handlers.orphan_handler import (
    load_orphan_dimension_surrogate_maps,
    resolve_order_dimension_surrogates,
    track_order_dimension_orphans,
)
from warehouse.connection import execute_values, get_dict_cursor

logger   = get_logger_name(__name__)
CONTRACT = get_contract("source_orders")


# ═══════════════════════════════════════════════════════════════════════════
#  SURROGATE KEY RESOLUTION
# ═══════════════════════════════════════════════════════════════════════════

@db_retry
def _load_dim_date_map() -> dict:
    """Map (full_date, hour) → date_key for order_created_at resolution (not orphan logic)."""
    with get_dict_cursor() as cur:
        cur.execute("""
            SELECT date_key, full_date, hour
            FROM warehouse.dim_date
        """)
        return {(r["full_date"], r["hour"]): r["date_key"] for r in cur.fetchall()}


def _resolve_keys(
    df: pd.DataFrame,
    cust_map: dict,
    drv_map: dict,
    rest_map: dict,
    date_map: dict,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Map source natural IDs → surrogate keys for every row in df.

    Orphan rules (delegated to resolve_order_dimension_surrogates):
      - not in map or missing natural → surrogate -1; original_orphan_* holds source id when -1
    date_key=None (timestamp not in dim_date) → row moved to bad_rows for quarantine.

    Returns: (good_df, bad_df)
      good_df  — rows with a valid date_key, ready to insert
      bad_df   — rows where date_key could not be resolved (quarantine these)
    """
    customer_keys, driver_keys, restaurant_keys  = [], [], []
    orphan_cust, orphan_drv, orphan_rest        = [], [], []
    date_keys                                    = []
    bad_indices                                  = []

    for idx, row in df.iterrows():
        cid = int(row["customer_id"])   if pd.notna(row.get("customer_id"))   else None
        did = int(row["driver_id"])     if pd.notna(row.get("driver_id"))     else None
        rid = int(row["restaurant_id"]) if pd.notna(row.get("restaurant_id")) else None

        ts = pd.to_datetime(row["order_created_at"])
        date_key = date_map.get((ts.date(), ts.hour))

        # date_key is NOT NULL in DDL — quarantine rows we can't resolve
        if date_key is None:
            bad_indices.append(idx)
            continue

        ck, dk, rk, oc, od, orest = resolve_order_dimension_surrogates(
            cid, did, rid, cust_map, drv_map, rest_map
        )

        customer_keys.append(ck)
        driver_keys.append(dk)
        restaurant_keys.append(rk)
        date_keys.append(date_key)

        orphan_cust.append(oc)
        orphan_drv.append(od)
        orphan_rest.append(orest)

    good = df.drop(index=bad_indices).copy()
    bad  = df.loc[bad_indices].copy()

    if not good.empty:
        good["customer_key"]                = customer_keys
        good["driver_key"]                  = driver_keys
        good["restaurant_key"]              = restaurant_keys
        good["date_key"]                    = date_keys
        good["original_orphan_customer_id"] = orphan_cust
        good["original_orphan_driver_id"]   = orphan_drv
        good["original_orphan_restaurant_id"] = orphan_rest

    return good, bad


# ═══════════════════════════════════════════════════════════════════════════
#  MAIN LOADER
# ═══════════════════════════════════════════════════════════════════════════

def load(file_path: str, run_id: int) -> None:
    with StageTimer("fact_orders_load") as timer:
        log_stage_start(logger, "fact_orders_load", file=file_path)

        # ── 1. Idempotency check ──────────────────────────────────────────
        file_hash = compute_file_hash(file_path)
        if is_file_processed(file_path, file_hash):
            logger.info("file_skipped", file=file_path, reason="already_processed")
            return

        # ── 2. Register file as in-progress ──────────────────────────────
        register_file(run_id, file_path, file_hash, file_type="stream")

        # ── 3. Read file ──────────────────────────────────────────────────
        try:
            df = reader(file_path)
            if df is None or df.empty:
                mark_file_success(file_path, file_hash, 0, 0, 0)
                return
        except Exception as e:
            mark_file_failed(file_path, file_hash, str(e))
            send_alert("read_error", f"Cannot read {file_path}: {e}", run_id)
            return

        total = len(df)

        # ── 4. Schema validation — quarantine critical failures ───────────
        errors      = validate_entity(df, "source_orders")
        bad_indices = {e.row_index for e in errors if e.level == "critical"}
        valid_indices = [i for i in bad_indices if 0 <= i < len(df)]
        bad         = df.loc[valid_indices] if valid_indices else pd.DataFrame()
        good        = df.drop(index=valid_indices) if valid_indices else df.copy()

        if not bad.empty:
            errors_by_row: dict = defaultdict(list)
            for e in errors:
                if e.level == "critical":
                    errors_by_row[e.row_index].append(e.reason)

            send_batch_to_quarantine([
                {
                    "source_file":     file_path,
                    "entity_type":     "source_orders",
                    "raw_record":      df.loc[idx].to_dict(),
                    "error_type":      "schema_validation",
                    "error_details":   " | ".join(reasons),
                    "pipeline_run_id": run_id,
                }
                for idx, reasons in errors_by_row.items()
            ])
            for _, r in bad.iterrows():
                log_record_rejected(logger, r.to_dict(), reason="schema_validation")

        if good.empty:
            mark_file_success(file_path, file_hash, total, 0, len(bad))
            return

        # ── 5. Deduplicate within file ────────────────────────────────────
        good = good.drop_duplicates("order_id").copy()

        # ── 6. Resolve surrogate keys ─────────────────────────────────────
        cust_map, drv_map, rest_map = load_orphan_dimension_surrogate_maps()
        date_map = _load_dim_date_map()
        good, date_bad = _resolve_keys(good, cust_map, drv_map, rest_map, date_map)

        # Quarantine rows whose timestamp had no matching date_key in dim_date
        if not date_bad.empty:
            send_batch_to_quarantine([
                {
                    "source_file":     file_path,
                    "entity_type":     "source_orders",
                    "raw_record":      row.to_dict(),
                    "error_type":      "referential_integrity",
                    "error_details":   "order_created_at has no matching date_key in dim_date",
                    "pipeline_run_id": run_id,
                }
                for _, row in date_bad.iterrows()
            ])
            for _, r in date_bad.iterrows():
                log_record_rejected(logger, r.to_dict(), reason="missing_date_key")

        if good.empty:
            quarantined = len(bad) + len(date_bad)
            mark_file_success(file_path, file_hash, total, 0, quarantined)
            return

        good["version"]      = 1
        
        # is_backfilled: FALSE if has orphans (needs v2 later), TRUE if all dims resolved
        good["is_backfilled"] = good["original_orphan_customer_id"].isna() & \
                            good["original_orphan_driver_id"].isna() & \
                            good["original_orphan_restaurant_id"].isna()
        # ── 7. Insert into warehouse ──────────────────────────────────────
        columns = [
            "order_id",
            "customer_key", "driver_key", "restaurant_key",
            "region_id", "date_key",
            "order_amount", "delivery_fee", "discount_amount", "total_amount",
            "order_status", "payment_method",
            "order_created_at", "delivered_at",
            "original_orphan_customer_id",
            "original_orphan_driver_id",
            "original_orphan_restaurant_id",
            "version", "is_backfilled",
        ]
        # float NaN in nullable int columns must become None for PostgreSQL (avoids "integer out of range")
        rows = good[columns].astype(object).where(pd.notnull(good[columns]), None).to_dict("records")

        try:
            inserted = execute_values(
                f"""
                INSERT INTO warehouse.fact_orders ({','.join(columns)})
                VALUES %s
                ON CONFLICT (order_id, version) DO NOTHING
                """,
                rows,
            )
        except Exception as e:
            mark_file_failed(file_path, file_hash, str(e))
            send_alert("db_write_error", f"Insert failed for {file_path}: {e}", run_id)
            return

        # ── 8. Track orphan references ────────────────────────────────────
        # Insert into orphan_tracking (one row per orphan type). 
        # ON CONFLICT DO NOTHING makes it idempotent.
        orphan_rows = good[
            good["original_orphan_customer_id"].notna()
            | good["original_orphan_driver_id"].notna()
            | good["original_orphan_restaurant_id"].notna()
        ]
        if not orphan_rows.empty:
            track_order_dimension_orphans(orphan_rows)

        # ── 9. Mark file as done ──────────────────────────────────────────
        quarantined = len(bad) + len(date_bad)
        mark_file_success(file_path, file_hash, total, inserted, quarantined)

        log_stage_complete(
            logger, "fact_orders_load",
            records=inserted,
            latency_ms=timer.duration_ms,
        )