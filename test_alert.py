"""
test_alert.py
─────────────
Test script for pipeline alerting.

How to use:
    python test_alert.py

Requirements:
    .env must have SMTP credentials filled in.
"""
import time
from pipeline.alerting import send_alert
from pipeline.logging.logger import configure_logging
configure_logging(log_dir="logs", level="INFO")

send_alert(
    error_type="Test Alert",
    message="This is a test. Pipeline alerting is working correctly.",
    run_id=1
)

time.sleep(5)
print("Done — check your email.")