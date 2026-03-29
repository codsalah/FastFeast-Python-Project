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

import smtplib
import threading
from email.mime.text import MIMEText
from datetime import datetime

from config.settings import get_settings
from pipeline.logging.logger import get_logger_name, log_alert_fired

logger = get_logger_name("alerting")


def send_alert(error_type: str, message: str, run_id: int = None) -> None:
    """
    Fire-and-forget async alert.
    Spawns a daemon thread — never blocks the pipeline.
    Only call this on FAILURE, never on success.
    """
    settings = get_settings()

    if not settings.alert.enabled:
        logger.info("alerting_disabled", reason="ALERTING_ENABLED=false in config")
        return

    thread = threading.Thread(
        target = _send_email,
        args   = (error_type, message, run_id),
        daemon = True
    )
    thread.start()

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
        msg["To"]      = ", ".join(settings.alert.alert_recipients)

        with smtplib.SMTP(settings.alert.smtp_host, settings.alert.smtp_port) as server:
            server.starttls()
            server.login(settings.alert.smtp_user, settings.alert.smtp_password)
            server.sendmail(
                settings.alert.smtp_user,
                settings.alert.alert_recipients,
                msg.as_string()
            )

    except Exception as e:
        # Never raise — just log silently
        logger.error("alert_email_failed", error=str(e))