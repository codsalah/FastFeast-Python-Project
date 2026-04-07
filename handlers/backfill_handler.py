"""
handlers/backfill_handler.py
────────────────────────────
Orphan resolution - creates v2 when dims available (STEP 3 of 3).

Versioning: v1 (orphan, FK=-1) → v2 (resolved FKs). Keep both for audit.

is_backfilled=TRUE only when ALL FKs resolved (none = -1).
Never UPDATE - always INSERT new version.
"""

from __future__ import annotations

from typing import Any

from utils.retry import db_retry
from warehouse.connection import get_cursor


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
    Insert one new fact_orders row for the same business order_id with incremented version.

    - Copies measures and timestamps from the latest fact dict.
    - Sets new customer_key / driver_key / restaurant_key (one of them typically moved off -1).
    - Clears original_orphan_* for any dimension that is no longer -1; keeps naturals for dims still -1.
    - is_backfilled=True only when all three FKs are resolved (none are -1).

    Returns True if a row was inserted; False if ON CONFLICT (order_id, version) skipped.
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


def run_backfill_after_batch(run_id: int, max_retries: int | None = None) -> dict[str, int]:
    """
    Run the full post-batch reconciliation pass (orphan job). Lazy-imports the job module so
    reconciliation_job can safely import insert_fact_order_backfill_version from this file.
    """
    from pipelines.reconciliation_job import DEFAULT_MAX_RETRIES, run_reconciliation_after_batch

    lim = max_retries if max_retries is not None else DEFAULT_MAX_RETRIES
    return run_reconciliation_after_batch(run_id, max_retries=lim)
