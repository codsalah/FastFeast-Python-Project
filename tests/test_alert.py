"""
test_alert.py
─────────────
Test script for pipeline alerting.

How to use:
    python tests/test_alert.py

Requirements:
    .env must have SMTP credentials filled in.

"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import time
from alerting.alert_service import send_alert
from utils.logger import configure_logging
configure_logging(log_dir="logs", level="INFO")

send_alert(
    error_type="Test Alert",
    message="This is a test. Pipeline alerting is working correctly.",
    run_id=1
)

time.sleep(5)
print("Done — check your email.")