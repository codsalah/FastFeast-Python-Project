from datetime import datetime, timezone

from warehouse.connection import get_cursor, execute_many
from utils.retry import db_retry
from utils.logger import get_logger_name

logger = get_logger_name(__name__)


@db_retry
def load_dimension_ids() -> dict | None:
    """
    Load current active natural IDs from warehouse dimension tables.
    Returns a dict of sets keyed by entity type, or None if the warehouse
    is unreachable after all retries.

    Callers MUST check for None before calling detect_orphans —
    passing None into detect_orphans skips orphan detection for that batch,
    which is safer than running it against empty sets (false positives).
    """
    dimension_ids = {
        "customer_ids":   set(),
        "driver_ids":     set(),
        "restaurant_ids": set()
    }

    try:
        with get_cursor() as cur:

            # --- Load customer IDs ---
            cur.execute(
                """ SELECT customer_id
                    FROM warehouse.dim_customer
                    WHERE customer_id IS NOT NULL
                    AND is_current = true""")
            dimension_ids["customer_ids"] = {row[0] for row in cur.fetchall()}

            # --- Load driver IDs ---
            cur.execute(
                """SELECT driver_id
                FROM warehouse.dim_driver
                WHERE driver_id IS NOT NULL
                  AND is_current = true""")
            dimension_ids["driver_ids"] = {row[0] for row in cur.fetchall()}

            # --- Load restaurant IDs ---
            cur.execute(
                """SELECT restaurant_id
                   FROM warehouse.dim_restaurant
                   WHERE restaurant_id IS NOT NULL
                   AND is_current = true""")
            dimension_ids["restaurant_ids"] = {row[0] for row in cur.fetchall()}

    except Exception as e:
        logger.error("dimension_ids_load_failed", error=str(e))
        return None

    logger.info(
        "dimension_ids_loaded",
        customers=len(dimension_ids["customer_ids"]),
        drivers=len(dimension_ids["driver_ids"]),
        restaurants=len(dimension_ids["restaurant_ids"]),
    )

    return dimension_ids


def detect_orphans(record: dict, dimension_ids: dict | None) -> dict:
    """
    Check a record for unresolvable foreign keys.
    Returns a dict with is_orphan bool and list of orphaned fields.

    If dimension_ids is None (warehouse unavailable), orphan detection is
    skipped and is_orphan is returned as False — the record is passed through
    unverified rather than being falsely quarantined.
    """
    if dimension_ids is None:
        logger.warning("orphan_detection_skipped", reason="dimension_ids unavailable")
        return {"is_orphan": False, "orphaned_fields": []}

    orphaned_fields = []

    ## --- CHECK customer_id ---
    customer_id = record.get("customer_id")
    if customer_id is not None:
        if int(customer_id) not in dimension_ids.get("customer_ids", set()):
            orphaned_fields.append({
                "field":       "customer_id",
                "raw_id":      customer_id,
                "orphan_type": "customer"
            })

    # --- CHECK driver_id ---
    driver_id = record.get("driver_id")
    if driver_id is not None:
        if int(driver_id) not in dimension_ids.get("driver_ids", set()):
            orphaned_fields.append({
                "field":       "driver_id",
                "raw_id":      driver_id,
                "orphan_type": "driver"
            })

    # --- CHECK restaurant_id ---
    restaurant_id = record.get("restaurant_id")
    if restaurant_id is not None:
        if int(restaurant_id) not in dimension_ids.get("restaurant_ids", set()):
            orphaned_fields.append({
                "field":       "restaurant_id",
                "raw_id":      restaurant_id,
                "orphan_type": "restaurant"
            })

    result = {
        "is_orphan":       len(orphaned_fields) > 0,
        # len > 0 means at least one field was orphaned → True, else False
        "orphaned_fields": orphaned_fields
    }

    if result["is_orphan"]:
        order_id = record.get("order_id", "unknown")
        summary = ", ".join(
            f"{o['orphan_type']}={o['raw_id']}"
            for o in orphaned_fields
        )
        logger.warning(
            "orphan_detected",
            order_id=order_id,
            orphans=summary,
        )

    return result


@db_retry
def record_orphan_tracking(order_id: str, orphaned_fields: list) -> int:
    """
    Insert unresolved FK references into pipeline_audit.orphan_tracking.
    Uses ON CONFLICT DO NOTHING for idempotency — safe to call multiple times.
    Returns count of rows inserted.
    """
    if not orphaned_fields:
        return 0

    sql = """
        INSERT INTO pipeline_audit.orphan_tracking
            (order_id, orphan_type, raw_id, detected_at)
        VALUES
            (%s, %s, %s, %s)
        ON CONFLICT (order_id, orphan_type) DO NOTHING"""
    
    # WHY ON CONFLICT DO NOTHING? 
    # duplicate tracking rows not inserted -> [layer 2 of idempotency handling]
    # This makes the function idempotent —> safe to call multiple times.

    now = datetime.now(timezone.utc)

    # Turn each orphaned field into a tuple of data ready to insert into a database.
    rows = [ (order_id, o["orphan_type"], int(o["raw_id"]), now) for o in orphaned_fields ]

    try:
        inserted = execute_many(sql, rows)
        logger.debug(
            "orphan_tracking_inserted",
            order_id=order_id,
            count=inserted,
        )
        return inserted

    except Exception as e:
        logger.error(
            "orphan_tracking_failed",
            order_id=order_id,
            error=str(e),
        )
        return 0


def is_orphan(record: dict, dimension_ids: dict | None) -> bool:
    """Returns True if the record has any unresolvable FK.
    Returns False if dimension_ids is None (detection skipped, not failed).
    """
    return detect_orphans(record, dimension_ids)["is_orphan"]