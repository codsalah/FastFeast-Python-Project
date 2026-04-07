"""
test: Orphans resolved across batches
Shows how orphans are resolved in subsequent batches
"""

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from config.settings import get_settings
from warehouse.connection import init_pool, get_conn
import json
from datetime import datetime

def test_orphan_resolution_flow():
    """
    Simple test showing orphan resolution across 2 batches:
    
    BATCH 1 (Day 1):
    - Insert some dimensions (customers: 100, 101, drivers: 100)
    - Stream orders:
      * ORDER_1: customer=999 (orphan), driver=100 (exists) → ORPHAN
      * ORDER_2: customer=100 (exists), driver=888 (orphan) → ORPHAN
      * ORDER_3: customer=100 (exists), driver=100 (exists) → NORMAL
    
    BATCH 2 (Day 2):
    - Insert NEW dimension: customer=999 (now exists!)
    - Run reconciliation
    - ORDER_1 should get v2 with resolved customer_key
    - ORDER_2 still orphan (driver=888 still missing)
    """
    
    init_pool(get_settings())
    
    with get_conn() as conn:
        cur = conn.cursor()
        
        print("ORPHAN RESOLUTION FLOW TEST")
        print("=" * 60)
        
        # ============================================================
        # BATCH 1: Day 1 - Initial Setup
        # ============================================================
        print("\nBATCH 1: Day 1 - Initial Setup")
        print("-" * 40)
        
        # Insert dimensions for Day 1
        cur.execute("""
            INSERT INTO warehouse.dim_customer (customer_id, customer_name_masked, gender, segment_name, region_name, city_name, signup_date, valid_from, valid_to, is_current)
            VALUES 
                (100, 'Customer 100', 'M', 'Premium', 'North', 'City', '2020-01-01', '2020-01-01', '9999-12-31', TRUE),
                (101, 'Customer 101', 'F', 'Standard', 'South', 'City', '2020-01-01', '2020-01-01', '9999-12-31', TRUE)
            ON CONFLICT DO NOTHING
        """)
        
        cur.execute("""
            INSERT INTO warehouse.dim_driver (driver_id, driver_name, vehicle_type, shift, region_name, city_name, is_current, valid_from, valid_to)
            VALUES 
                (100, 'Driver 100', 'Car', 'Day', 'North', 'City', TRUE, '2020-01-01', '9999-12-31')
            ON CONFLICT DO NOTHING
        """)
        
        cur.execute("""
            INSERT INTO warehouse.dim_restaurant (restaurant_id, restaurant_name, category_name, region_name, city_name, is_current, valid_from, valid_to)
            VALUES 
                (100, 'Restaurant 100', 'Fast Food', 'North', 'City', TRUE, '2020-01-01', '9999-12-31')
            ON CONFLICT DO NOTHING
        """)
        
        conn.commit()
        print("  Inserted dimensions: customers 100, 101, driver 100, restaurant 100")
        
        # Create Day 1 stream orders file
        day1 = datetime.now().strftime("%Y-%m-%d")
        hour1 = datetime.now().hour
        stream_dir1 = f"data/input/stream/{day1}/{hour1:02d}"
        os.makedirs(stream_dir1, exist_ok=True)
        
        orders_day1 = [
            {
                'order_id': 'ORDER_1',  # Will be orphan (customer 999 missing)
                'customer_id': '999',   # Missing!
                'driver_id': '100',     # Exists
                'restaurant_id': '100', # Exists
                'region_id': 1,
                'date_key': 52609,
                'payment_method': 'card',
                'order_created_at': '2026-04-06 10:00:00',
                'delivered_at': '2026-04-06 11:00:00',
                'order_amount': '100.00',
                'delivery_fee': '5.00',
                'discount_amount': '0.00',
                'total_amount': '105.00',
                'order_status': 'Delivered'
            },
            {
                'order_id': 'ORDER_2',  # Will be orphan (driver 888 missing)
                'customer_id': '100',   # Exists
                'driver_id': '888',     # Missing!
                'restaurant_id': '100', # Exists
                'region_id': 1,
                'date_key': 52609,
                'payment_method': 'card',
                'order_created_at': '2026-04-06 10:00:00',
                'delivered_at': '2026-04-06 11:00:00',
                'order_amount': '150.00',
                'delivery_fee': '5.00',
                'discount_amount': '0.00',
                'total_amount': '155.00',
                'order_status': 'Delivered'
            },
            {
                'order_id': 'ORDER_3',  # Normal - no orphans
                'customer_id': '100',   # Exists
                'driver_id': '100',     # Exists
                'restaurant_id': '100', # Exists
                'region_id': 1,
                'date_key': 52609,
                'payment_method': 'card',
                'order_created_at': '2026-04-06 10:00:00',
                'delivered_at': '2026-04-06 11:00:00',
                'order_amount': '200.00',
                'delivery_fee': '5.00',
                'discount_amount': '0.00',
                'total_amount': '205.00',
                'order_status': 'Delivered'
            }
        ]
        
        with open(f"{stream_dir1}/orders.json", 'w') as f:
            json.dump({"orders": orders_day1}, f, indent=2)
        
        print(f"  Created stream file: {stream_dir1}/orders.json")
        print(f"     - ORDER_1: customer=999 (will be orphan)")
        print(f"     - ORDER_2: driver=888 (will be orphan)")
        print(f"     - ORDER_3: all exist (normal)")
        
        # Load Day 1 orders
        from loaders.fact_orders_loader import load
        
        cur.execute("""
            INSERT INTO pipeline_audit.pipeline_run_log (run_type, run_date, status, started_at)
            VALUES ('batch1_day1', CURRENT_DATE, 'running', now())
            RETURNING run_id
        """)
        run_id1 = cur.fetchone()[0]
        conn.commit()
        
        load(f"{stream_dir1}/orders.json", run_id1)
        print(f"  Loaded Day 1 orders (run_id: {run_id1})")
        
        # Check Day 1 results
        cur.execute("""
            SELECT order_id, customer_key, driver_key, restaurant_key,
                   original_orphan_customer_id, original_orphan_driver_id,
                   version, is_backfilled
            FROM warehouse.fact_orders
            WHERE order_id IN ('ORDER_1', 'ORDER_2', 'ORDER_3')
            ORDER BY order_id
        """)
        
        print("\n  DAY 1 RESULTS (Initial Load):")
        print("  " + "-" * 50)
        for row in cur.fetchall():
            order_id, ck, dk, rk, oc, od, version, is_backfilled = row
            orphan_type = []
            if ck == -1:
                orphan_type.append("CUSTOMER")
            if dk == -1:
                orphan_type.append("DRIVER")
            
            orphan_str = ", ".join(orphan_type) if orphan_type else "NONE"
            
            print(f"  {order_id}:")
            print(f"     Customer: {ck} {'[ORPHAN]' if ck == -1 else '[OK]'} | Driver: {dk} {'[ORPHAN]' if dk == -1 else '[OK]'}")
            print(f"     Orphan IDs: customer={oc}, driver={od}")
            print(f"     Version: {version} | Is Backfilled: {is_backfilled}")
            print(f"     Orphan Type: {orphan_str}")
            print()
        
        # ============================================================
        # BATCH 2: Day 2 - Resolve Some Orphans
        # ============================================================
        print("\nBATCH 2: Day 2 - Resolve Some Orphans")
        print("-" * 40)
        
        # Now insert the missing customer 999!
        cur.execute("""
            INSERT INTO warehouse.dim_customer (customer_id, customer_name_masked, gender, segment_name, region_name, city_name, signup_date, valid_from, valid_to, is_current)
            VALUES 
                (999, 'Customer 999 (New)', 'M', 'Premium', 'North', 'City', '2020-01-01', '2020-01-01', '9999-12-31', TRUE)
            ON CONFLICT DO NOTHING
        """)
        
        # Also insert driver 888
        cur.execute("""
            INSERT INTO warehouse.dim_driver (driver_id, driver_name, vehicle_type, shift, region_name, city_name, is_current, valid_from, valid_to)
            VALUES 
                (888, 'Driver 888 (New)', 'Bike', 'Night', 'North', 'City', TRUE, '2020-01-01', '9999-12-31')
            ON CONFLICT DO NOTHING
        """)
        
        conn.commit()
        print("  Inserted NEW dimensions: customer 999, driver 888")
        print("  These should resolve the orphans!")
        
        # Run reconciliation
        print("\n  Running Reconciliation Job...")
        from pipelines.reconciliation_job import reconcile_orphan_orders
        
        stats = reconcile_orphan_orders(pipeline_run_id=None)
        print(f"  Reconciliation complete!")
        print(f"     Stats: {stats}")
        
        # Check Day 2 results
        cur.execute("""
            SELECT order_id, customer_key, driver_key, restaurant_key,
                   original_orphan_customer_id, original_orphan_driver_id,
                   version, is_backfilled
            FROM warehouse.fact_orders
            WHERE order_id IN ('ORDER_1', 'ORDER_2', 'ORDER_3')
            ORDER BY order_id, version
        """)
        
        print("\n  DAY 2 RESULTS (After Reconciliation):")
        print("  " + "-" * 50)
        
        current_order = None
        for row in cur.fetchall():
            order_id, ck, dk, rk, oc, od, version, is_backfilled = row
            
            if order_id != current_order:
                print(f"\n  {order_id}:")
                current_order = order_id
            
            orphan_type = []
            if ck == -1:
                orphan_type.append("CUSTOMER")
            if dk == -1:
                orphan_type.append("DRIVER")
            
            orphan_str = ", ".join(orphan_type) if orphan_type else "RESOLVED"
            version_marker = "NEW" if version > 1 else "ORIGINAL"
            
            print(f"     [v{version}] {version_marker}")
            print(f"        Customer: {ck} {'[ORPHAN]' if ck == -1 else '[OK]'} | Driver: {dk} {'[ORPHAN]' if dk == -1 else '[OK]'}")
            print(f"        Is Backfilled: {is_backfilled}")
            print(f"        Status: {orphan_str}")
        
        # Check orphan tracking
        cur.execute("""
            SELECT order_id, orphan_type, raw_id, is_resolved, retry_count
            FROM pipeline_audit.orphan_tracking
            WHERE order_id IN ('ORDER_1', 'ORDER_2')
            ORDER BY order_id, orphan_type
        """)
        
        print("\n  Checking Results...")
        print("  " + "=" * 50)
        for row in cur.fetchall():
            order_id, orphan_type, raw_id, is_resolved, retry_count = row
            status = "RESOLVED" if is_resolved else "PENDING"
            print(f"     {order_id} | {orphan_type}={raw_id} | {status} (retries: {retry_count})")
        
        # Summary
        print(f"\n" + "=" * 60)
        print(f"SUMMARY:")
        print(f"   ORDER_1: Started with orphan customer 999 -> Should be RESOLVED in v2")
        print(f"   ORDER_2: Started with orphan driver 888 -> Should be RESOLVED in v2")
        print(f"   ORDER_3: Normal order -> v1 only, is_backfilled=TRUE")
        print(f"\n   Expected: 2 new versions (v2) created for ORDER_1 and ORDER_2")
        print(f"   Expected: Orphan tracking records marked as resolved")

if __name__ == "__main__":
    test_orphan_resolution_flow()
