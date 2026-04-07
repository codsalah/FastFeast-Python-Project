"""
handlers/orphan_handler.py
──────────────────────────
Orphan detection at stream load (STEP 1 of 3).

Flow: 
    1) Load dim maps  
    2) Resolve FKs (-1 if missing, store original ID)  
    3) Track in orphan_tracking

Lifecycle: Stream (detect -1) → Batch (dims arrive) → Reconcile (create v2) → Backfill (resolved FKs)
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

from utils.logger import get_logger_name
from utils.retry import db_retry
from warehouse.connection import execute_many, get_dict_cursor

logger = get_logger_name(__name__)


@db_retry
def load_orphan_dimension_surrogate_maps() -> tuple[dict[int, int], dict[int, int], dict[int, int]]:
    """
    Load natural key → surrogate key for the three orphan-capable SCD2 dimensions.

    Only rows with non-null natural id and is_current = TRUE participate. Used by
    fact_orders_loader (stream) and reconciliation_job (batch) so dimension lookup
    stays identical in both paths.
    """
    with get_dict_cursor() as cur:
        cur.execute(
            """
            SELECT customer_id, customer_key
            FROM warehouse.dim_customer
            WHERE customer_id IS NOT NULL AND is_current = TRUE
            """
        )
        cust = {int(r["customer_id"]): int(r["customer_key"]) for r in cur.fetchall()}

        cur.execute(
            """
            SELECT driver_id, driver_key
            FROM warehouse.dim_driver
            WHERE driver_id IS NOT NULL AND is_current = TRUE
            """
        )
        drv = {int(r["driver_id"]): int(r["driver_key"]) for r in cur.fetchall()}

        cur.execute(
            """
            SELECT restaurant_id, restaurant_key
            FROM warehouse.dim_restaurant
            WHERE restaurant_id IS NOT NULL AND is_current = TRUE
            """
        )
        rest = {int(r["restaurant_id"]): int(r["restaurant_key"]) for r in cur.fetchall()}

    return cust, drv, rest


def resolve_order_dimension_surrogates(
    customer_id: int | None,
    driver_id: int | None,
    restaurant_id: int | None,
    cust_map: dict[int, int],
    drv_map: dict[int, int],
    rest_map: dict[int, int],
) -> tuple[int, int, int, int | None, int | None, int | None]:
    """
    Map stream order naturals to warehouse surrogate keys (Unknown member = -1).

    Rules:
      - Missing natural id or id not in map → surrogate -1.
      - original_orphan_* is set to the source natural id only when surrogate is -1;
        otherwise None (resolved parent).

    Returns:
        (customer_key, driver_key, restaurant_key,
         original_orphan_customer_id, original_orphan_driver_id, original_orphan_restaurant_id)
    """
    ck = cust_map.get(customer_id, -1) if customer_id is not None else -1
    dk = drv_map.get(driver_id, -1) if driver_id is not None else -1
    rk = rest_map.get(restaurant_id, -1) if restaurant_id is not None else -1

    orig_c = customer_id if ck == -1 else None
    orig_d = driver_id if dk == -1 else None
    orig_r = restaurant_id if rk == -1 else None

    return ck, dk, rk, orig_c, orig_d, orig_r


def _optional_natural_id(row: Any, col: str) -> int | None:
    """Read optional int from a DataFrame row; None if missing or NaN."""
    v = row.get(col) if hasattr(row, "get") else getattr(row, col, None)
    if v is None:
        return None
    if isinstance(v, float) and pd.isna(v):
        return None
    try:
        if pd.isna(v):
            return None
    except TypeError:
        pass
    return int(v)


@db_retry
def track_order_dimension_orphans(df: pd.DataFrame) -> int:
    """
    Persist unresolved dimension parents to pipeline_audit.orphan_tracking.

    Expects columns original_orphan_* already populated by resolve_order_dimension_surrogates
    (via the loader). One row per (order_id, orphan_type); ON CONFLICT DO NOTHING.
    """
    now = datetime.now(timezone.utc)
    rows: list[tuple[Any, ...]] = []

    for _, row in df.iterrows():
        order_id = str(row["order_id"])

        cid = _optional_natural_id(row, "original_orphan_customer_id")
        if cid is not None:
            rows.append((order_id, "customer", cid, now))

        did = _optional_natural_id(row, "original_orphan_driver_id")
        if did is not None:
            rows.append((order_id, "driver", did, now))

        rid = _optional_natural_id(row, "original_orphan_restaurant_id")
        if rid is not None:
            rows.append((order_id, "restaurant", rid, now))

    if not rows:
        return 0

    sql = """
        INSERT INTO pipeline_audit.orphan_tracking (order_id, orphan_type, raw_id, detected_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (order_id, orphan_type) DO NOTHING
    """
    n = execute_many(sql, rows)
    logger.info("orphan_tracking_inserted", count=n)
    return n
