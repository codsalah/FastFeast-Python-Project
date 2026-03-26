from datetime import datetime, timezone
import logging
logger = logging.getLogger(__name__)
from utils.db import get_cursor, execute_many


def load_dimension_ids(conn):

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
                    FROM fastfeast.dim_customer
                    WHERE customer_id IS NOT NULL
                    AND is_current = true""")
            dimension_ids["customer_ids"] = {row[0] for row in cur.fetchall()}

            # --- Load driver IDs ---
            cur.execute(
                """SELECT driver_id
                FROM fastfeast.dim_driver
                WHERE driver_id IS NOT NULL
                  AND is_current = true""")
            dimension_ids["driver_ids"] = {row[0] for row in cur.fetchall()}

            # --- Load restaurant IDs ---
            cur.execute(
                """SELECT restaurant_id
                   FROM fastfeast.dim_restaurant
                   WHERE restaurant_id IS NOT NULL
                   AND is_current = true""")
            dimension_ids["restaurant_ids"] = {row[0] for row in cur.fetchall()}

        logger.info(
            f"[ORPHAN DETECTOR] Dimension ID sets loaded — "
            f"{len(dimension_ids['customer_ids'])} customers | "
            f"{len(dimension_ids['driver_ids'])} drivers | "
            f"{len(dimension_ids['restaurant_ids'])} restaurants"
        )

    except Exception as e:
        logger.error(f"[ORPHAN DETECTOR] Failed to load dimension IDs: {e}")

    return dimension_ids

 

def detect_orphans(record, dimension_ids):
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
            f"[ORPHAN DETECTOR] Orphan detected | order_id={order_id} | {summary}"
        )

    return result

def record_orphan_tracking(conn, order_id, orphaned_fields):
    if not orphaned_fields:
        return 0

    # FOCUS HERE <_______________________________________>
    sql = """
        INSERT INTO pipeline_audit.orphan_staging
            (record_type, record_id, missing_fk_type, missing_fk_value,
             source_file, arrived_at, status)
        VALUES
            (%s, %s, %s, %s, %s, %s, 'pending')
        ON CONFLICT DO NOTHING """
    
    # WHY ON CONFLICT DO NOTHING? 
    # duplicate tracking rows not inserted -> [layer 2 of idempotency handling]
    # This makes the function idempotent —> safe to call multiple times.

    now = datetime.now(timezone.utc)

    # Turn each orphaned field into a tuple of data ready to insert into a database.
    rows = [ (order_id, o["orphan_type"], o["raw_id"], now) for o in orphaned_fields ]

    try:
        inserted = execute_many(sql, rows)

        logger.debug(
            f"[ORPHAN DETECTOR] {inserted} tracking row(s) inserted | order_id={order_id}"
        )
        return inserted

    except Exception as e:
        logger.error(
            f"[ORPHAN DETECTOR] Failed to insert tracking rows "
            f"for order_id={order_id}: {e}"
        )
        return 0

# Returns True if the record has ANY orphaned FK, False if all clean.
def is_orphan(record, dimension_ids):

    return detect_orphans(record, dimension_ids)["is_orphan"]
