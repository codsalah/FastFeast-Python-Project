"""
loaders/fact_tickets_loader.py
──────────────────────────────
Loads micro-batch ticket records into warehouse.fact_tickets.

Pipeline contract (WAP pattern):
  1. Idempotency check   — skip if file already processed
  2. Read + validate     — quarantine schema-invalid records
  3. Deduplicate         — drop duplicate ticket_ids within the file
  4. Resolve surrogates  — map all FK natural IDs → surrogate keys
  5. Quarantine unresolvables — tickets whose order_id / agent_id /
                                 reason_key / channel_key can't be resolved
  6. Compute SLA metrics — first_response_minutes, resolution_minutes,
                           sla_*_breached flags
  7. Insert into warehouse
  8. Mark file done

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
from warehouse.connection import execute_values, get_dict_cursor

logger   = get_logger_name(__name__)
CONTRACT = get_contract("source_tickets")


# ═══════════════════════════════════════════════════════════════════════════
#  DIMENSION MAP LOADING
# ═══════════════════════════════════════════════════════════════════════════

@db_retry
def _build_dim_maps() -> dict:
    """
    Load all lookup maps needed for ticket surrogate key resolution.

    Returns a dict of maps keyed by entity name.
    None keys are excluded from all dimension maps to prevent the Unknown
    member (-1) from accidentally matching a source row with a null ID.
    """
    with get_dict_cursor() as cur:

        # Inherit customer_key directly from the order row (order_id → customer_key)
        # customer_key on ticket = customer_key on order.
        cur.execute("""
            SELECT DISTINCT ON (order_id)
                order_id,
                order_key,
                customer_key,
                driver_key,
                restaurant_key
            FROM warehouse.fact_orders
            ORDER BY order_id, version DESC
        """)
        rows = cur.fetchall()
        order_map        = {r["order_id"]: r["order_key"]        for r in rows}
        order_cust_map   = {r["order_id"]: r["customer_key"]     for r in rows}
        order_drv_map    = {r["order_id"]: r["driver_key"]       for r in rows}
        order_rest_map   = {r["order_id"]: r["restaurant_key"]   for r in rows}

        cur.execute("""
            SELECT agent_id, agent_key
            FROM warehouse.dim_agent
            WHERE agent_id IS NOT NULL AND is_current = TRUE
        """)
        agent_map = {int(r["agent_id"]): r["agent_key"] for r in cur.fetchall()}

        cur.execute("""
            SELECT reason_id
            FROM warehouse.dim_reason
            WHERE reason_id IS NOT NULL
        """)
        reason_map = {int(r["reason_id"]): r["reason_id"] for r in cur.fetchall()}

        cur.execute("""
            SELECT channel_id
            FROM warehouse.dim_channel
            WHERE channel_id IS NOT NULL
        """)
        channel_map = {int(r["channel_id"]): r["channel_id"] for r in cur.fetchall()}

        # priority_id is the PK of dim_priority (no separate surrogate in DDL)
        cur.execute("""
            SELECT priority_id, sla_first_response_min, sla_resolution_min
            FROM warehouse.dim_priority
        """)
        sla_map = {
            r["priority_id"]: (r["sla_first_response_min"], r["sla_resolution_min"])
            for r in cur.fetchall()
        }

        cur.execute("""
            SELECT date_key, full_date, hour
            FROM warehouse.dim_date
        """)
        date_map = {(r["full_date"], r["hour"]): r["date_key"] for r in cur.fetchall()}

    return {
        "order_map":      order_map,
        "order_cust_map": order_cust_map,
        "order_drv_map":  order_drv_map,
        "order_rest_map": order_rest_map,
        "agent_map":      agent_map,
        "reason_map":     reason_map,
        "channel_map":    channel_map,
        "sla_map":        sla_map,
        "date_map":       date_map,
    }


# ═══════════════════════════════════════════════════════════════════════════
#  SURROGATE KEY RESOLUTION + SLA CALCULATION
# ═══════════════════════════════════════════════════════════════════════════

def _resolve_keys(
    df: pd.DataFrame,
    maps: dict,
) -> tuple[pd.DataFrame, list[dict]]:
    """
    Resolve all FK surrogate keys and compute SLA metrics for each ticket row.

    Rows that cannot resolve required FKs (order_key, agent_key, reason_id,
    channel_id, date_key) are collected as quarantine_records and excluded
    from the returned good_df.

    customer_key / driver_key / restaurant_key are inherited from the matched
    order row — NOT re-resolved from source natural IDs.

    Returns: (good_df, quarantine_records)
    """
    order_map      = maps["order_map"]
    order_cust_map = maps["order_cust_map"]
    order_drv_map  = maps["order_drv_map"]
    order_rest_map = maps["order_rest_map"]
    agent_map      = maps["agent_map"]
    reason_map     = maps["reason_map"]
    channel_map    = maps["channel_map"]
    sla_map        = maps["sla_map"]
    date_map       = maps["date_map"]

    resolved_rows    = []
    quarantine_records = []
    skip_indices     = set()

    for idx, row in df.iterrows():
        order_id_str = str(row["order_id"])
        created_at   = pd.to_datetime(row["created_at"])

        # ── Required FK lookups ───────────────────────────────────────────
        order_key  = order_map.get(order_id_str)
        agent_key  = agent_map.get(int(row["agent_id"])) if pd.notna(row.get("agent_id")) else None
        reason_key = reason_map.get(int(row["reason_id"])) if pd.notna(row.get("reason_id")) else None
        channel_key = channel_map.get(int(row["channel_id"])) if pd.notna(row.get("channel_id")) else None
        date_key   = date_map.get((created_at.date(), created_at.hour))

        missing = []
        if order_key  is None: missing.append("order_id not found in fact_orders")
        if agent_key  is None: missing.append("agent_id not found in dim_agent")
        if reason_key is None: missing.append("reason_id not found in dim_reason")
        if channel_key is None: missing.append("channel_id not found in dim_channel")
        if date_key   is None: missing.append("created_at has no matching date_key in dim_date")

        if missing:
            skip_indices.add(idx)
            quarantine_records.append({
                "idx":          idx,
                "error_details": " | ".join(missing),
            })
            continue

        # ── Inherit FK keys from the matched order row (may be -1 for orphans) ──
        customer_key   = order_cust_map.get(order_id_str, -1)
        driver_key     = order_drv_map.get(order_id_str, -1)
        restaurant_key = order_rest_map.get(order_id_str, -1)

        # ── priority_id (direct — dim_priority PK is priority_id in DDL) ────────
        priority_id = int(row["priority_id"]) if pd.notna(row.get("priority_id")) else None

        # ── Static dimension keys (natural keys = PKs) ───────────────────────────
        resolved_reason_id  = reason_map.get(int(row["reason_id"])) if pd.notna(row.get("reason_id")) else None
        resolved_channel_id = channel_map.get(int(row["channel_id"])) if pd.notna(row.get("channel_id")) else None

        # ── SLA timestamps from source ────────────────────────────────────
        fr_at  = pd.to_datetime(row["first_response_at"]) if pd.notna(row.get("first_response_at")) else None
        res_at = pd.to_datetime(row["resolved_at"])       if pd.notna(row.get("resolved_at"))       else None

        # ── SLA minute calculations ───────────────────────────────────────
        fr_min  = round((fr_at  - created_at).total_seconds() / 60, 2) if fr_at  is not None else None
        res_min = round((res_at - created_at).total_seconds() / 60, 2) if res_at is not None else None

        # ── SLA breach flags — guard with `is not None`, NOT truthiness ──
        # fr_min=0.0 is a valid response time and must not be treated as falsy
        sla_fr_min, sla_res_min = sla_map.get(priority_id, (None, None)) if priority_id is not None else (None, None)

        sla_fr_breached  = (fr_min  > sla_fr_min)  if (fr_min  is not None and sla_fr_min  is not None) else None
        sla_res_breached = (res_min > sla_res_min) if (res_min is not None and sla_res_min is not None) else None

        resolved_rows.append({
            "idx":                         idx,
            "order_key":                   order_key,
            "customer_key":                customer_key,
            "driver_key":                  driver_key,
            "restaurant_key":              restaurant_key,
            "agent_key":                   agent_key,
            "reason_id":                   resolved_reason_id,
            "priority_id":                 priority_id,
            "channel_id":                  resolved_channel_id,
            "date_key":                    date_key,
            "first_response_minutes":      fr_min,
            "resolution_minutes":          res_min,
            "sla_first_response_breached": sla_fr_breached,
            "sla_resolution_breached":     sla_res_breached,
        })

    good = df.drop(index=list(skip_indices)).copy()

    if resolved_rows:
        resolved_df = (
            pd.DataFrame(resolved_rows)
            .set_index("idx")
        )
        # Drop source columns that will be overwritten by resolved values
        overlap = [c for c in resolved_df.columns if c in good.columns]
        good    = good.drop(columns=overlap)
        good    = good.join(resolved_df)

    return good, quarantine_records


# ═══════════════════════════════════════════════════════════════════════════
#  MAIN LOADER
# ═══════════════════════════════════════════════════════════════════════════

def load(file_path: str, run_id: int) -> None:
    with StageTimer("fact_tickets_load") as timer:
        log_stage_start(logger, "fact_tickets_load", file=file_path)

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
        errors      = validate_entity(df, "source_tickets")
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
                    "entity_type":     "source_tickets",
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
        good = good.drop_duplicates("ticket_id").copy()

        # ── 6. Resolve surrogate keys + SLA computation ───────────────────
        maps              = _build_dim_maps()
        good, fk_quarantine = _resolve_keys(good, maps)

        # Quarantine rows whose required FKs could not be resolved
        if fk_quarantine:
            original_df = df  # use original df for raw_record
            send_batch_to_quarantine([
                {
                    "source_file":     file_path,
                    "entity_type":     "source_tickets",
                    "raw_record":      original_df.loc[q["idx"]].to_dict(),
                    "error_type":      "referential_integrity",
                    "error_details":   q["error_details"],
                    "pipeline_run_id": run_id,
                }
                for q in fk_quarantine
            ])
            for q in fk_quarantine:
                log_record_rejected(
                    logger,
                    original_df.loc[q["idx"]].to_dict(),
                    reason="referential_integrity",
                )

        if good.empty:
            quarantined = len(bad) + len(fk_quarantine)
            mark_file_success(file_path, file_hash, total, 0, quarantined)
            return

        # ── 7. Insert into warehouse ──────────────────────────────────────
        # Column order must match warehouse.fact_tickets DDL exactly.
        # reason_id and channel_id are used directly (static dims use natural keys as PK).
        # sla_first_due_at / sla_resolve_due_at pass through from source CSV.
        columns = [
            "ticket_id",
            "order_key", "order_id",
            "customer_key", "driver_key", "restaurant_key",
            "agent_key", "reason_id", "priority_id", "channel_id",
            "date_key",
            "status", "refund_amount",
            "sla_first_response_breached", "sla_resolution_breached",
            "first_response_minutes", "resolution_minutes",
            "created_at", "first_response_at", "resolved_at",
            "sla_first_due_at", "sla_resolve_due_at",
        ]
        rows = good[columns].astype(object).where(pd.notnull(good[columns]), None).to_dict("records")

        try:
            inserted = execute_values(
                f"""
                INSERT INTO warehouse.fact_tickets ({','.join(columns)})
                VALUES %s
                ON CONFLICT (ticket_id) DO NOTHING
                """,
                rows,
            )
        except Exception as e:
            mark_file_failed(file_path, file_hash, str(e))
            send_alert("db_write_error", f"Insert failed for {file_path}: {e}", run_id)
            return

        # ── 8. Mark file as done ──────────────────────────────────────────
        quarantined = len(bad) + len(fk_quarantine)
        mark_file_success(file_path, file_hash, total, inserted, quarantined)

        log_stage_complete(
            logger, "fact_tickets_load",
            records=inserted,
            latency_ms=timer.duration_ms,
        )