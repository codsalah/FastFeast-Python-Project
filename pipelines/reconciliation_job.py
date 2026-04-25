"""
pipelines/reconciliation_job.py
───────────────────────────────
Orphan reconciliation orchestration (STEP 2 of 3).

Runs after batch loads: finds orphans that can now resolve, calls backfill for v2.

Flow: Load orphans → Check dims → Create v2 (resolved) → Update tracking
Max 3 retries then quarantine. Triggered by batch_pipeline.run().
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import pandas as pd

from handlers.orphan_handler import load_orphan_dimension_surrogate_maps
from handlers.quarantine_handler import send_batch_to_quarantine
from utils.logger import get_logger_name
from utils.retry import db_retry
from warehouse.connection import get_cursor, get_dict_cursor

logger = get_logger_name(__name__)

ORPHAN_TYPES = frozenset({"customer", "driver", "restaurant"})
DEFAULT_MAX_RETRIES = 3
MAX_FIXES_PER_ORDER_PER_PASS = 20


@db_retry
def insert_fact_order_backfill_version(
    fact: dict[str, Any],
    new_ck: int,
    new_dk: int,
    new_rk: int,
    new_version: int,
    *,
    is_backfilled: bool,
) -> bool:
    """
    Insert one new fact_orders row for the same order_id with incremented version.

    Returns True if inserted, False if ON CONFLICT skipped.
    """
    new_oc = None if new_ck != -1 else fact.get("original_orphan_customer_id")
    new_od = None if new_dk != -1 else fact.get("original_orphan_driver_id")
    new_or = None if new_rk != -1 else fact.get("original_orphan_restaurant_id")

    cols = (
        "order_id, customer_key, driver_key, restaurant_key, region_id, date_key, "
        "order_amount, delivery_fee, discount_amount, total_amount, order_status, payment_method, "
        "order_created_at, delivered_at, original_orphan_customer_id, original_orphan_driver_id, "
        "original_orphan_restaurant_id, version, is_backfilled"
    )
    sql = f"""
        INSERT INTO warehouse.fact_orders ({cols})
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s
        )
        ON CONFLICT (order_id, version) DO NOTHING
    """
    params = (
        fact["order_id"],
        new_ck,
        new_dk,
        new_rk,
        fact["region_id"],
        fact["date_key"],
        fact["order_amount"],
        fact["delivery_fee"],
        fact["discount_amount"],
        fact["total_amount"],
        fact["order_status"],
        fact["payment_method"],
        fact["order_created_at"],
        fact["delivered_at"],
        new_oc,
        new_od,
        new_or,
        new_version,
        is_backfilled,
    )
    with get_cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount > 0


def _coerce_int(val: Any) -> int | None:
    """Normalize a cell value to int or None (handles None, NaN, bool, Decimal)."""
    if val is None:
        return None
    if isinstance(val, bool):
        return None
    if isinstance(val, float) and pd.isna(val):
        return None
    if isinstance(val, int):
        return int(val)
    if isinstance(val, Decimal):
        return int(val)
    try:
        if pd.isna(val):
            return None
    except TypeError:
        pass
    return int(val)


@db_retry
def _fetch_unresolved_orphans() -> list[dict[str, Any]]:
    """Return all open orphan_tracking rows for customer / driver / restaurant."""
    with get_dict_cursor() as cur:
        cur.execute(
            """
            SELECT tracking_id, order_id, orphan_type, raw_id, retry_count, detected_at
            FROM pipeline_audit.orphan_tracking
            WHERE is_resolved = FALSE
              AND orphan_type IN ('customer', 'driver', 'restaurant')
            ORDER BY order_id, orphan_type
            """
        )
        return list(cur.fetchall())


@db_retry
def _fetch_unresolved_orphans_for_order(order_id: str) -> list[dict[str, Any]]:
    """Return unresolved orphan_tracking rows for a single order_id."""
    with get_dict_cursor() as cur:
        cur.execute(
            """
            SELECT tracking_id, order_id, orphan_type, raw_id, retry_count, detected_at
            FROM pipeline_audit.orphan_tracking
            WHERE is_resolved = FALSE AND order_id = %s
            ORDER BY orphan_type
            """,
            (order_id,),
        )
        return list(cur.fetchall())


@db_retry
def _fetch_latest_fact_order(order_id: str) -> dict[str, Any] | None:
    """Latest fact_orders row for order_id (highest version, then order_key)."""
    with get_dict_cursor() as cur:
        cur.execute(
            """
            SELECT order_key, order_id, customer_key, driver_key, restaurant_key,
                   region_id, date_key, order_amount, delivery_fee, discount_amount, total_amount,
                   order_status, payment_method, order_created_at, delivered_at,
                   original_orphan_customer_id, original_orphan_driver_id, original_orphan_restaurant_id,
                   version, is_backfilled
            FROM warehouse.fact_orders
            WHERE order_id = %s
            ORDER BY version DESC, order_key DESC
            LIMIT 1
            """,
            (order_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None


def _natural_from_fact_and_pending(
    fact: dict[str, Any], pending: list[dict[str, Any]], otype: str
) -> int | None:
    """
    Natural id to use when resolving one dimension: prefer original_orphan_* on the latest
    fact row; if null, use raw_id from orphan_tracking for that orphan_type.
    """
    col = {
        "customer": "original_orphan_customer_id",
        "driver": "original_orphan_driver_id",
        "restaurant": "original_orphan_restaurant_id",
    }[otype]
    v = _coerce_int(fact.get(col))
    if v is not None:
        return v
    for r in pending:
        if str(r["orphan_type"]) == otype:
            return _coerce_int(r["raw_id"])
    return None


def _next_single_dimension_fix(
    fact: dict[str, Any],
    cust_map: dict[int, int],
    drv_map: dict[int, int],
    rest_map: dict[int, int],
    pending: list[dict[str, Any]],
) -> tuple[str, int, int, int] | None:
    """
    If exactly one missing parent can now be resolved from dimensions, return
    (dimension_name, new_customer_key, new_driver_key, new_restaurant_key).
    Priority: customer, then driver, then restaurant — at most one fix per call.
    """
    ck = int(fact["customer_key"])
    dk = int(fact["driver_key"])
    rk = int(fact["restaurant_key"])

    if ck == -1:
        nid = _natural_from_fact_and_pending(fact, pending, "customer")
        if nid is not None and nid in cust_map:
            return ("customer", cust_map[nid], dk, rk)
    if dk == -1:
        nid = _natural_from_fact_and_pending(fact, pending, "driver")
        if nid is not None and nid in drv_map:
            return ("driver", ck, drv_map[nid], rk)
    if rk == -1:
        nid = _natural_from_fact_and_pending(fact, pending, "restaurant")
        if nid is not None and nid in rest_map:
            return ("restaurant", ck, dk, rest_map[nid])
    return None


def _orphan_still_broken(otype: str, ck: int, dk: int, rk: int) -> bool:
    """True if the tracking row's dimension is still -1 on the fact row."""
    if otype == "customer":
        return ck == -1
    if otype == "driver":
        return dk == -1
    if otype == "restaurant":
        return rk == -1
    return False


@db_retry
def _mark_orphan_resolved(tracking_id: int) -> None:
    """Set is_resolved on a tracking row after a successful dimension fix or redundant open row."""
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_audit.orphan_tracking
            SET is_resolved = TRUE, resolved_at = now()
            WHERE tracking_id = %s AND is_resolved = FALSE
            """,
            (tracking_id,),
        )


@db_retry
def _increment_retry(tracking_id: int) -> None:
    """Bump retry_count after a batch pass where the parent still did not exist in dims."""
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_audit.orphan_tracking
            SET retry_count = retry_count + 1
            WHERE tracking_id = %s AND is_resolved = FALSE
            """,
            (tracking_id,),
        )


@db_retry
def _fetch_retry_count(tracking_id: int) -> int:
    """Current retry_count for a tracking row (0 if missing)."""
    with get_dict_cursor() as cur:
        cur.execute(
            "SELECT retry_count FROM pipeline_audit.orphan_tracking WHERE tracking_id = %s",
            (tracking_id,),
        )
        row = cur.fetchone()
        return int(row["retry_count"]) if row else 0


@db_retry
def _delete_orphan_row(tracking_id: int) -> None:
    """Remove tracking row after quarantine handoff (exhausted retries)."""
    with get_cursor() as cur:
        cur.execute("DELETE FROM pipeline_audit.orphan_tracking WHERE tracking_id = %s", (tracking_id,))


def reconcile_orphan_orders(
    *,
    pipeline_run_id: int | None,
    max_retries: int = DEFAULT_MAX_RETRIES,
) -> dict[str, int]:
    """
    One reconciliation pass: for each order with pending orphans, insert new fact versions
    where possible, then increment retry or quarantine exhausted rows.
    Does not modify fact_tickets.
    """
    stats = {
        "orders_touched": 0,
        "fact_versions_inserted": 0,
        "orphans_marked_resolved": 0,
        "retries_incremented": 0,
        "orphans_quarantined": 0,
    }

    cust_map, drv_map, rest_map = load_orphan_dimension_surrogate_maps()
    orphans = _fetch_unresolved_orphans()
    if not orphans:
        logger.info("orphan_reconcile_no_pending")
        return stats

    by_order: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in orphans:
        by_order[str(row["order_id"])].append(row)

    for order_id, _initial_olist in by_order.items():
        fact0 = _fetch_latest_fact_order(order_id)
        if not fact0:
            logger.warning("orphan_reconcile_missing_fact", order_id=order_id)
            continue

        stats["orders_touched"] += 1
        iterations = 0

        while iterations < MAX_FIXES_PER_ORDER_PER_PASS:
            pending = _fetch_unresolved_orphans_for_order(order_id)
            if not pending:
                break

            fact = _fetch_latest_fact_order(order_id)
            if not fact:
                break

            fix = _next_single_dimension_fix(fact, cust_map, drv_map, rest_map, pending)
            if fix is None:
                break

            dim_fixed, nck, ndk, nrk = fix
            nv = int(fact["version"]) + 1
            fully = nck != -1 and ndk != -1 and nrk != -1
            inserted = insert_fact_order_backfill_version(
                fact, nck, ndk, nrk, nv, is_backfilled=fully
            )
            if not inserted:
                logger.warning(
                    "orphan_reconcile_insert_skipped",
                    order_id=order_id,
                    version=nv,
                    reason="conflict_or_no_rowcount",
                )
                break

            stats["fact_versions_inserted"] += 1
            logger.info(
                "orphan_reconcile_new_order_version",
                order_id=order_id,
                version=nv,
                fixed_dimension=dim_fixed,
                customer_key=nck,
                driver_key=ndk,
                restaurant_key=nrk,
                is_backfilled=fully,
            )

            for orow in pending:
                if str(orow["orphan_type"]) == dim_fixed:
                    _mark_orphan_resolved(int(orow["tracking_id"]))
                    stats["orphans_marked_resolved"] += 1
                    break

            iterations += 1

        pending_final = _fetch_unresolved_orphans_for_order(order_id)
        fact = _fetch_latest_fact_order(order_id)
        if not fact or not pending_final:
            continue

        eff_ck = int(fact["customer_key"])
        eff_dk = int(fact["driver_key"])
        eff_rk = int(fact["restaurant_key"])
        quarantine_batch: list[dict[str, Any]] = []

        for orow in pending_final:
            tid = int(orow["tracking_id"])
            otype = str(orow["orphan_type"])
            if otype not in ORPHAN_TYPES:
                continue

            if not _orphan_still_broken(otype, eff_ck, eff_dk, eff_rk):
                _mark_orphan_resolved(tid)
                stats["orphans_marked_resolved"] += 1
                continue

            rc_before = _fetch_retry_count(tid)
            if rc_before >= max_retries:
                raw_id = int(orow["raw_id"])
                quarantine_batch.append(
                    {
                        "source_file": "pipelines/reconciliation_job.py",
                        "entity_type": "orphan_reconciliation",
                        "raw_record": {
                            "order_id": order_id,
                            "orphan_type": otype,
                            "raw_id": raw_id,
                            "retry_count": rc_before,
                            "latest_fact_keys": {
                                "customer_key": eff_ck,
                                "driver_key": eff_dk,
                                "restaurant_key": eff_rk,
                            },
                            "exhausted_at": datetime.now(timezone.utc).isoformat(),
                        },
                        "error_type": "orphan",
                        "error_details": (
                            f"Orphan reconciliation exhausted (retry_count>={max_retries}); "
                            f"{otype} raw_id={raw_id} still missing in dimension."
                        ),
                        "orphan_type": otype,
                        "raw_orphan_id": str(raw_id),
                        "pipeline_run_id": pipeline_run_id,
                    }
                )
                _delete_orphan_row(tid)
                stats["orphans_quarantined"] += 1
                logger.warning(
                    "orphan_reconcile_quarantined",
                    order_id=order_id,
                    orphan_type=otype,
                    raw_id=raw_id,
                    retry_count=rc_before,
                )
            else:
                _increment_retry(tid)
                stats["retries_incremented"] += 1
                rc_after = _fetch_retry_count(tid)
                logger.info(
                    "orphan_reconcile_retry",
                    order_id=order_id,
                    orphan_type=otype,
                    retry_count=rc_after,
                    max_retries=max_retries,
                )

        if quarantine_batch:
            send_batch_to_quarantine(quarantine_batch)

    logger.info("orphan_reconcile_pass_complete", **stats)
    return stats


def run_reconciliation_after_batch(run_id: int, max_retries: int = DEFAULT_MAX_RETRIES) -> dict[str, int]:
    """Entry point after a batch pipeline run (passes pipeline_run_id for quarantine linkage)."""
    return reconcile_orphan_orders(pipeline_run_id=run_id, max_retries=max_retries)
