"""
loaders/fact_orders_loader.py
─────────────────────────────
Loads micro-batch order records into warehouse.fact_orders.

Pipeline contract (WAP pattern):
  1. Idempotency check   — skip if file already processed (SHA-256 + file_tracker)
  2. Read + validate     — quarantine schema-invalid records
  3. Deduplicate         — drop exact duplicate order_ids within the file
  4. Resolve surrogates  — map natural IDs → surrogate keys; -1 for orphans
  5. Insert              — ON CONFLICT (order_id, version) DO NOTHING (layer-2 idempotency)
  6. Track orphans       — batch-insert into pipeline_audit.orphan_tracking
  7. Mark file done      — update file_tracker to 'success'

Orphan handling (orders only — customer / driver / restaurant):
  - Orphan dimension references → surrogate key set to -1 (Unknown member)
  - Original natural ID stored in original_orphan_*_id for reconciliation
  - Orphan rows tracked in pipeline_audit.orphan_tracking (one row per orphan type per order)
  - ON CONFLICT DO NOTHING on orphan_tracking → safe to re-run

"""
from collections import defaultdict
from datetime import datetime, timezone

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
from warehouse.connection import execute_many, execute_values, get_dict_cursor

logger   = get_logger_name(__name__)
CONTRACT = get_contract("source_orders")


# ═══════════════════════════════════════════════════════════════════════════
#  SURROGATE KEY RESOLUTION
# ═══════════════════════════════════════════════════════════════════════════

@db_retry
def _build_dim_maps() -> tuple[dict, dict, dict, dict]:
    """
    Load current active natural-ID → surrogate-key mappings from all
    three orphan dimensions, plus the date map.

    Returns: (cust_map, drv_map, rest_map, date_map)

    None keys are explicitly excluded so that NULL dim rows (the -1 Unknown
    member) never accidentally match a source record whose ID arrived as None.
    """
    with get_dict_cursor() as cur:

        cur.execute("""
            SELECT customer_id, customer_key
            FROM warehouse.dim_customer
            WHERE customer_id IS NOT NULL
              AND is_current = TRUE
        """)
        cust_map = {int(r["customer_id"]): r["customer_key"] for r in cur.fetchall()}

        cur.execute("""
            SELECT driver_id, driver_key
            FROM warehouse.dim_driver
            WHERE driver_id IS NOT NULL
              AND is_current = TRUE
        """)
        drv_map = {int(r["driver_id"]): r["driver_key"] for r in cur.fetchall()}

        cur.execute("""
            SELECT restaurant_id, restaurant_key
            FROM warehouse.dim_restaurant
            WHERE restaurant_id IS NOT NULL
              AND is_current = TRUE
        """)
        rest_map = {int(r["restaurant_id"]): r["restaurant_key"] for r in cur.fetchall()}

        cur.execute("""
            SELECT date_key, full_date, hour
            FROM warehouse.dim_date
        """)
        date_map = {(r["full_date"], r["hour"]): r["date_key"] for r in cur.fetchall()}

    return cust_map, drv_map, rest_map, date_map


def _resolve_keys(
    df: pd.DataFrame,
    cust_map: dict,
    drv_map: dict,
    rest_map: dict,
    date_map: dict,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Map source natural IDs → surrogate keys for every row in df.

    Orphan rules:
      - customer / driver / restaurant not found → surrogate key = -1
      - original natural ID stored in original_orphan_*_id
    date_key=None (timestamp not in dim_date) → row moved to bad_rows for quarantine.

    Returns: (good_df, bad_df)
      good_df  — rows with a valid date_key, ready to insert
      bad_df   — rows where date_key could not be resolved (quarantine these)
    """
    customer_keys, driver_keys, restaurant_keys  = [], [], []
    orphan_cust, orphan_drv, orphan_rest         = [], [], []
    date_keys                                    = []
    bad_indices                                  = []

    for idx, row in df.iterrows():
        cid = int(row["customer_id"])   if pd.notna(row.get("customer_id"))   else None
        did = int(row["driver_id"])     if pd.notna(row.get("driver_id"))     else None
        rid = int(row["restaurant_id"]) if pd.notna(row.get("restaurant_id")) else None

        ck = cust_map.get(cid, -1)  if cid is not None else -1
        dk = drv_map.get(did,  -1)  if did is not None else -1
        rk = rest_map.get(rid, -1)  if rid is not None else -1

        ts       = pd.to_datetime(row["order_created_at"])
        date_key = date_map.get((ts.date(), ts.hour))

        # date_key is NOT NULL in DDL — quarantine rows we can't resolve
        if date_key is None:
            bad_indices.append(idx)
            continue

        customer_keys.append(ck)
        driver_keys.append(dk)
        restaurant_keys.append(rk)
        date_keys.append(date_key)

        # Store original IDs only when the surrogate resolved to -1 (orphan)
        orphan_cust.append(cid if ck == -1 else None)
        orphan_drv.append(did  if dk == -1 else None)
        orphan_rest.append(rid  if rk == -1 else None)

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
#  ORPHAN AUDIT TRACKING
# ═══════════════════════════════════════════════════════════════════════════

@db_retry
def _track_orphans(df: pd.DataFrame) -> int:
    """
    Batch-insert unresolved FK references into pipeline_audit.orphan_tracking.

    One row per orphan type per order_id.
    ON CONFLICT DO NOTHING → idempotent on re-runs.
    Returns total rows inserted.
    """
    now  = datetime.now(timezone.utc)
    rows = []

    for _, row in df.iterrows():
        order_id = str(row["order_id"])

        if pd.notna(row.get("original_orphan_customer_id")):
            rows.append((order_id, "customer", int(row["original_orphan_customer_id"]), now))

        if pd.notna(row.get("original_orphan_driver_id")):
            rows.append((order_id, "driver", int(row["original_orphan_driver_id"]), now))

        if pd.notna(row.get("original_orphan_restaurant_id")):
            rows.append((order_id, "restaurant", int(row["original_orphan_restaurant_id"]), now))

    if not rows:
        return 0

    # ON CONFLICT DO NOTHING — safe to call multiple times for the same order
    sql = """
        INSERT INTO pipeline_audit.orphan_tracking
            (order_id, orphan_type, raw_id, detected_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """
    inserted = execute_many(sql, rows)
    logger.debug("orphan_tracking_inserted", count=inserted)
    return inserted


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
        bad         = df.loc[list(bad_indices)] if bad_indices else pd.DataFrame()
        good        = df.drop(index=list(bad_indices))

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
        cust_map, drv_map, rest_map, date_map = _build_dim_maps()
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
        good["is_backfilled"] = False

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
        rows = good[columns].where(good[columns].notna(), other=None).to_dict("records")

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
        orphan_rows = good[
            good["original_orphan_customer_id"].notna()
            | good["original_orphan_driver_id"].notna()
            | good["original_orphan_restaurant_id"].notna()
        ]
        if not orphan_rows.empty:
            _track_orphans(orphan_rows)

        # ── 9. Mark file as done ──────────────────────────────────────────
        quarantined = len(bad) + len(date_bad)
        mark_file_success(file_path, file_hash, total, inserted, quarantined)

        log_stage_complete(
            logger, "fact_orders_load",
            records=inserted,
            latency_ms=timer.duration_ms,
        )