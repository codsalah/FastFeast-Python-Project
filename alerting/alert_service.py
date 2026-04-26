"""
pipeline/alerting.py
────────────────────
Async email alerting for the FastFeast pipeline.

Sends a fire-and-forget email notification in a background daemon thread
whenever the pipeline encounters a failure. Never blocks the pipeline.
No alert is sent for successful processing.

Trigger conditions 
    • File parsing errors        → stream_processor.py
    • Schema validation failures → batch_processor.py
    • Database writing failures  → batch_processor.py, stream_processor.py
    • High orphan rate exceeded  → orphan_handler.py
    • Unhandled pipeline crash   → main.py
"""

import atexit
import smtplib
import threading
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

from config.settings import get_settings
from utils.logger import get_logger_name, log_alert_fired

logger = get_logger_name("alerting")

_bg_threads: list[threading.Thread] = []


@atexit.register
def _flush_background_email_threads() -> None:
    """
    Best-effort: wait briefly for background email threads.

    Rationale: prevents interpreter shutdown races where a daemon thread tries to write logs
    while Python is finalizing, which can crash the process on some platforms.
    """
    for t in list(_bg_threads):
        try:
            if t.is_alive():
                t.join(timeout=5)
        except Exception:
            # Never raise during interpreter shutdown
            pass


def _start_bg_thread(target, args) -> None:
    t = threading.Thread(target=target, args=args, daemon=True)
    _bg_threads.append(t)
    t.start()


class AlertService:
    """Simple wrapper for sending alerts."""

    def send_alert(self, error_type: str, message: str, run_id: int = None) -> None:
        """Fire-and-forget async alert. Never blocks the pipeline."""
        _send_alert_async(error_type, message, run_id)

    def send_report(self, subject: str, message: str, pdf_bytes: bytes, filename: str, run_id: int = None) -> None:
        """Fire-and-forget async report email with PDF attachment."""
        _send_report_async(subject, message, pdf_bytes, filename, run_id)


def send_alert(error_type: str, message: str, run_id: int = None) -> None:
    """
    Fire-and-forget async alert.
    Spawns a daemon thread — never blocks the pipeline.
    Only call this on FAILURE, never on success.
    """
    _send_alert_async(error_type, message, run_id)


def send_report(subject: str, message: str, pdf_bytes: bytes, filename: str, run_id: int = None) -> None:
    """Fire-and-forget async report email with PDF attachment."""
    _send_report_async(subject, message, pdf_bytes, filename, run_id)


def _send_alert_async(error_type: str, message: str, run_id: int = None) -> None:
    """
    Fire-and-forget async alert.
    Spawns a daemon thread — never blocks the pipeline.
    """
    settings = get_settings()

    if not settings.alert.enabled:
        logger.info("alerting_disabled", reason="ALERTING_ENABLED=false in config")
        return

    _start_bg_thread(_send_email, (error_type, message, run_id))


def _send_report_async(subject: str, message: str, pdf_bytes: bytes, filename: str, run_id: int = None) -> None:
    settings = get_settings()

    if not settings.alert.enabled:
        logger.info("reporting_disabled", reason="ALERTING_ENABLED=false in config")
        return

    if not settings.alert.report_recipients:
        logger.info("reporting_skipped", reason="REPORT_RECIPIENTS empty")
        return

    _start_bg_thread(_send_report_email, (subject, message, pdf_bytes, filename, run_id))


def _send_email(error_type: str, message: str, run_id) -> None:
    """
    Internal — runs inside background thread.
    Always logs the alert event first.
    Then attempts email — failure is silent and never crashes the pipeline.
    """
    settings = get_settings()

    log_alert_fired(
        logger,
        alert_type = error_type,
        severity   = "CRITICAL",
        message    = message,
        run_id     = run_id or "N/A",
    )

    recipients = settings.alert.alert_recipients
    if not recipients:
        logger.info("alert_skipped", reason="ALERT_RECIPIENTS empty", error_type=error_type)
        return

    # Then attempt email separately
    try:
        subject = (
            f"[FastFeast ALERT] {error_type} — "
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

        body = f"""
FastFeast Pipeline Alert
========================
Error Type : {error_type}
Run ID     : {run_id or 'N/A'}
Time       : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Details:
{message}

-- FastFeast Pipeline
        """.strip()

        msg            = MIMEText(body, "plain")
        msg["Subject"] = subject
        msg["From"]    = f"{settings.alert.sender_name} <{settings.alert.smtp_user}>"
        msg["To"]      = ", ".join(recipients)

        with smtplib.SMTP(settings.alert.smtp_host, settings.alert.smtp_port) as server:
            server.starttls()
            server.login(settings.alert.smtp_user, settings.alert.smtp_password)
            server.sendmail(
                settings.alert.smtp_user,
                recipients,
                msg.as_string()
            )

    except Exception as e:
        # Never raise — just log silently
        logger.error("alert_email_failed", error=str(e))


def _send_report_email(subject: str, message: str, pdf_bytes: bytes, filename: str, run_id) -> None:
    settings = get_settings()

    log_alert_fired(
        logger,
        alert_type="QUALITY_REPORT",
        severity="INFO",
        message=f"{subject} (attachment: {filename})",
        run_id=run_id or "N/A",
    )

    try:
        msg = MIMEMultipart()
        msg["Subject"] = subject
        msg["From"] = f"{settings.alert.sender_name} <{settings.alert.smtp_user}>"
        msg["To"] = ", ".join(settings.alert.report_recipients)

        body = f"""
FastFeast Daily Quality Report
==============================
Run ID: {run_id or 'N/A'}
Generated At: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{message}

This email contains an aggregate quality report attachment for operational review.

Regards,
FastFeast Data Platform
        """.strip()
        msg.attach(MIMEText(body, "plain"))

        part = MIMEApplication(pdf_bytes, _subtype="pdf")
        part.add_header("Content-Disposition", "attachment", filename=filename)
        msg.attach(part)

        with smtplib.SMTP(settings.alert.smtp_host, settings.alert.smtp_port) as server:
            server.starttls()
            server.login(settings.alert.smtp_user, settings.alert.smtp_password)
            server.sendmail(
                settings.alert.smtp_user,
                settings.alert.report_recipients,
                msg.as_string(),
            )

    except Exception as e:
        logger.error("report_email_failed", error=str(e), filename=filename)