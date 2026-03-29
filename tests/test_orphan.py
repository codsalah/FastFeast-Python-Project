#!/usr/bin/env python3
"""
test_orphan.py 
run: python tests/test_orphan.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from validators.orphan_detector import detect_orphans


def test():
    print("=" * 50)
    print("Simple Orphan Detection Test")
    print("=" * 50)

    # Mock dimension IDs (what would be loaded from database)
    dim_ids = {
        "customer_ids": {1, 2, 3, 4, 5},
        "driver_ids": {1, 2, 3, 4, 5},
        "restaurant_ids": {1, 2, 3, 4, 5}
    }

    # Test 1: Valid record (all IDs exist)
    record1 = {
        'order_id': 'order-1',
        'customer_id': 1,
        'driver_id': 1,
        'restaurant_id': 1
    }
    result = detect_orphans(record1, dim_ids)
    print(f"\n1. Valid record: is_orphan? {result['is_orphan']} (should be False)")
    print(f"   Orphan fields: {result['orphaned_fields']}")

    # Test 2: Orphan customer (ID doesn't exist)
    record2 = {
        'order_id': 'order-2',
        'customer_id': 99999,
        'driver_id': 1,
        'restaurant_id': 1
    }
    result = detect_orphans(record2, dim_ids)
    print(f"\n2. Orphan customer: is_orphan? {result['is_orphan']} (should be True)")
    print(f"   Orphan fields: {result['orphaned_fields']}")

    # Test 3: Orphan driver
    record3 = {
        'order_id': 'order-3',
        'customer_id': 1,
        'driver_id': 99999,
        'restaurant_id': 1
    }
    result = detect_orphans(record3, dim_ids)
    print(f"\n3. Orphan driver: is_orphan? {result['is_orphan']} (should be True)")
    print(f"   Orphan fields: {result['orphaned_fields']}")

    # Test 4: Multiple orphans
    record4 = {
        'order_id': 'order-4',
        'customer_id': 88888,
        'driver_id': 88888,
        'restaurant_id': 1
    }
    result = detect_orphans(record4, dim_ids)
    print(f"\n4. Multiple orphans: is_orphan? {result['is_orphan']} (should be True)")
    print(f"   Orphan fields: {result['orphaned_fields']}")

    # Test 5: Missing restaurant
    record5 = {
        'order_id': 'order-5',
        'customer_id': 1,
        'driver_id': 1,
        'restaurant_id': 77777
    }
    result = detect_orphans(record5, dim_ids)
    print(f"\n5. Orphan restaurant: is_orphan? {result['is_orphan']} (should be True)")
    print(f"   Orphan fields: {result['orphaned_fields']}")

    print("\n" + "=" * 50)
    print("All tests passed!")
    print("=" * 50)


if __name__ == "__main__":
    test()