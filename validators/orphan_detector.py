from warehouse.connection import get_cursor, execute_many
from utils.retry import db_retry
from utils.logger import get_logger_name

logger = get_logger_name(__name__)


def load_dimension_ids() -> dict:
    """
    Load current active natural IDs from warehouse dimension tables.
    Returns a dict of sets keyed by entity type.
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

        logger.info(
            "dimension_ids_loaded",
            customers=len(dimension_ids["customer_ids"]),
            drivers=len(dimension_ids["driver_ids"]),
            restaurants=len(dimension_ids["restaurant_ids"]),
        )

    except Exception as e:
        logger.error("dimension_ids_load_failed", error=str(e))

    return dimension_ids


def detect_orphans(record: dict, dimension_ids: dict) -> dict:
    """
    Check a record for unresolvable foreign keys.
    Returns a dict with is_orphan bool and list of orphaned fields.
    """
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
        if driver_id not in dimension_ids.get("driver_ids", set()):
            orphaned_fields.append({
                "field":       "driver_id",
                "raw_id":      driver_id,
                "orphan_type": "driver"
            })

    # --- CHECK restaurant_id ---
    restaurant_id = record.get("restaurant_id")
    if restaurant_id is not None:
        if restaurant_id not in dimension_ids.get("restaurant_ids", set()):
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
            (order_id, orphan_type, raw_id, is_resolved, retry_count, detected_at)
        VALUES
            (%s, %s, %s, false, 0, now())
        ON CONFLICT DO NOTHING
    """

    rows = [
        (order_id, o["orphan_type"], int(o["raw_id"]))
        for o in orphaned_fields
    ]

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


def is_orphan(record: dict, dimension_ids: dict) -> bool:
    """Returns True if the record has any unresolvable FK."""
    return detect_orphans(record, dimension_ids)["is_orphan"]