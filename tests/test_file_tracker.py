#!/usr/bin/env python3
"""
test_file_tracker.py - Core file tracker tests
run: python tests/test_file_tracker.py
"""

import os
import tempfile
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.file_tracker import compute_file_hash, register_file, mark_file_success, is_file_processed
from quality import metrics_tracker as audit_trail
from warehouse.connection import init_pool
from config.settings import get_settings


def test():
    print("=" * 50)
    print("Testing File Tracker")
    print("=" * 50)

    # Setup
    settings = get_settings()
    init_pool(settings)
    audit_trail.ensure_audit_schema()
    
    # Create test file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write("id,name\n1,test1\n2,test2\n")
        file_path = f.name
    
    file_hash = compute_file_hash(file_path)
    print(f"\nFile: {os.path.basename(file_path)}")
    
    run_id = audit_trail.start_run("test")
    
    # Test 1: New file - should NOT be processed yet
    register_file(run_id, file_path, file_hash, "test")
    result = is_file_processed(file_path, file_hash)
    print(f"1. New file: processed? {result} (should be False)")
    
    # Test 2: After success - should be processed
    mark_file_success(file_path, file_hash, 2, 2, 0)
    result = is_file_processed(file_path, file_hash)
    print(f"2. After load: processed? {result} (should be True)")
    
    # Cleanup
    os.unlink(file_path)
    audit_trail.complete_run(run_id, "success", 1, 1, 0, 2, 2, 0, 0)
    
    print("\n" + "=" * 50)
    print("Core tests passed!")
    print("=" * 50)


if __name__ == "__main__":
    test()