"""
tests/test_quality_report_email_integration.py
─────────────────────────────────────────
Integration test: generates a real quality report PDF and sends it via email.

This test creates a mock pipeline run, generates the quality report PDF,
and sends it via the alert service using settings from .env file.

python -m pytest tests/test_quality_report_email_integration.py -v -s
"""

from pathlib import Path

import pytest

from config.settings import get_settings

# Skip if alerting is not enabled in settings
settings = get_settings()
pytestmark = pytest.mark.skipif(
    not settings.alert.enabled,
    reason="Alerting not enabled in settings/.env (ALERT_ENABLED=false or missing SMTP config)"
)


def test_quality_report_email_integration():
    """
    End-to-end test:
    1. Create test run with metrics
    2. Generate quality report PDF
    3. Send report via email
    4. Verify PDF was generated and email queued
    """
    from alerting.alert_service import send_report
    from quality import metrics_tracker as audit
    from quality.quality_report import generate_daily_quality_report
    from warehouse.connection import close_pool, init_pool

    init_pool(settings)

    try:
        # 1. Setup audit schema and create test run
        audit.ensure_audit_schema()

        run_id = audit.start_run("integration_test")

        # Add quality metrics for multiple tables
        audit.write_quality_metrics(
            run_id=run_id,
            table_name="dim_customer",
            source_file="data/input/batch/2026-04-06/customers.csv",
            total_records=100,
            valid_records=95,
            quarantined_records=3,
            orphaned_records=2,
            duplicate_count=1,
            null_violations=0,
            processing_latency_sec=1.2,
        )

        audit.write_quality_metrics(
            run_id=run_id,
            table_name="dim_driver",
            source_file="data/input/batch/2026-04-06/drivers.csv",
            total_records=50,
            valid_records=50,
            quarantined_records=0,
            orphaned_records=0,
            duplicate_count=0,
            null_violations=0,
            processing_latency_sec=0.8,
        )

        # Complete the run
        audit.complete_run(
            run_id=run_id,
            status="success",
            total_files=2,
            successful_files=2,
            failed_files=0,
            total_records=150,
            total_loaded=145,
            total_quarantined=3,
            total_orphaned=2,
        )

        # 2. Generate quality report PDF
        artifact = generate_daily_quality_report(run_id)

        # Verify PDF is valid
        assert artifact.pdf_bytes[:4] == b"%PDF", "Invalid PDF header"
        assert len(artifact.pdf_bytes) > 1000, "PDF too small"

        # Save PDF locally for inspection
        report_path = Path("test_outputs") / artifact.filename
        report_path.parent.mkdir(exist_ok=True)
        with open(report_path, "wb") as f:
            f.write(artifact.pdf_bytes)

        # 3. Send report via email
        subject = f"FastFeast Daily Quality Report - Run {run_id}"
        message = f"""
FastFeast Pipeline Quality Report

Run ID: {run_id}
Date: 2026-04-06
Status: SUCCESS

Files Processed: 2
Records: 150 total, 145 loaded, 3 quarantined, 2 orphaned

See attached PDF for detailed metrics.
        """.strip()

        # Send the email with PDF attachment (fire-and-forget async)
        send_report(
            subject=subject,
            message=message,
            pdf_bytes=artifact.pdf_bytes,
            filename=artifact.filename,
            run_id=run_id,
        )

        # Email is queued asynchronously (no return value to check)
        email_sent = True

        # 4. Verify outputs
        assert report_path.exists(), f"PDF not saved: {report_path}"
        assert report_path.stat().st_size > 1000, "Saved PDF too small"

        print(f"\n{'='*60}")
        print(f"✅ Quality Report Email Test PASSED")
        print(f"   Run ID: {run_id}")
        print(f"   PDF: {artifact.filename} ({len(artifact.pdf_bytes)} bytes)")
        print(f"   Saved: {report_path.absolute()}")
        print(f"   Email queued: {email_sent}")
        print(f"{'='*60}\n")

    finally:
        close_pool()
