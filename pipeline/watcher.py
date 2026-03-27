"""
pipeline/watcher.py
───────────────────
Directory polling for batch and stream files.

Two pollers run in separate threads:
    BatchPoller  — watches from 11PM, processes 13 batch files once per day,
                   then sleeps until 11PM tomorrow
    StreamPoller — scans continuously throughout the day for stream files
                   that arrive at irregular intervals

FUTURE CHANGES NEEDED:
    1. Real FileProcessor — currently TestProcessor (prints only)
       Replace with actual FileProcessor that validates + loads to DB
    2. Fallback Batch Handling — implement when batch files missing after window
    3. Configurable batch window — move hardcoded hours to config.yaml
"""

import os
import time
import threading
from datetime import date, datetime, timedelta

from pipeline.logging.logger import (
    get_logger_name,
    log_stage_start,
    log_stage_complete,
    log_alert_fired,
)
from pipeline.logging import audit_trail

logger = get_logger_name(__name__)

# ── Expected files ────────────────────────────────────────────

BATCH_FILES = [
    "customers.csv",
    "drivers.csv",
    "agents.csv",
    "regions.csv",
    "reasons.csv",
    "categories.csv",
    "segments.csv",
    "teams.csv",
    "channels.csv",
    "priorities.csv",
    "reason_categories.csv",
    "restaurants.json",
    "cities.json",
]

STREAM_FILES = [
    "orders.json",
    "tickets.csv",
    "ticket_events.json",
]

# Batch window: only watch between these hours
# TODO: Move to config.yaml
BATCH_WINDOW_START = 23   # 11PM — before this, batch never arrives
BATCH_WINDOW_END   = 1    # 1AM — past this with files missing → log CRITICAL alert


# ── File stability check ──────────────────────────────────────

def is_file_stable(filepath: str, wait_sec: int = 1, max_attempts: int = 3) -> bool:
    """Check if file is done being written by comparing size over multiple attempts."""
    try:
        previous_size = -1
        current_size = os.path.getsize(filepath)
        
        for attempt in range(max_attempts):
            if previous_size == current_size and attempt > 0:
                return True
            previous_size = current_size
            time.sleep(wait_sec)
            current_size = os.path.getsize(filepath)
        
        # After max attempts, if size changed, assume still writing
        return previous_size == current_size
    except OSError:
        return False

# ── Batch Poller ──────────────────────────────────────────────

class BatchPoller:
    """
    Watches for today's 13 batch files.

    Behaviour:
    - Before 11PM: sleep until 11PM
    - 11PM onward: scan every poll_interval seconds until all 13 files found
    - All 13 done: sleep until 11PM tomorrow
    - If batch still missing after 1AM: log CRITICAL alert and send email
    """

    def __init__(self, batch_base_dir: str, processor, alerter=None, poll_interval: int = 60):
        self.batch_base_dir = batch_base_dir
        self.processor = processor
        self.poll_interval = poll_interval
        self.running = False
        self._thread = None
        self.alerter = alerter

    def start(self):
        self.running = True
        self._thread = threading.Thread(
            target=self._run,
            name="BatchPoller",
            daemon=True
        )
        self._thread.start()
        logger.info("BatchPoller started", poll_interval=self.poll_interval)
        return self._thread

    def stop(self):
        self.running = False
        logger.info("BatchPoller stopped")

    def _run(self):
        current_day = None
        alert_sent = False

        while self.running:
            today = date.today().isoformat()
            now = datetime.now()

            # ── Midnight reset ─────────────────────────────────
            if today != current_day:
                logger.info("BatchPoller: new day", date=today)
                current_day = today
                alert_sent = False

            # ── Before window start: sleep until window start ───
            if now.hour < BATCH_WINDOW_START:
                wake_at = datetime.combine(
                    now.date(),
                    datetime.min.time().replace(hour=BATCH_WINDOW_START)
                )
                sleep_sec = (wake_at - now).total_seconds()
                logger.info(
                    "BatchPoller: before batch window, sleeping",
                    sleep_min=round(sleep_sec / 60),
                    wake_at=f"{BATCH_WINDOW_START:02d}:00"
                )
                time.sleep(sleep_sec)
                continue

            # ── Batch folder not here yet ──────────────────────
            batch_dir = os.path.join(self.batch_base_dir, today)

            if not os.path.exists(batch_dir):
                logger.debug("BatchPoller: waiting for batch dir", batch_dir=batch_dir)
                time.sleep(self.poll_interval)
                continue

            # ── Scan all 13 expected files ─────────────────────
            for filename in BATCH_FILES:
                filepath = os.path.join(batch_dir, filename)

                # Check if already processed using audit_trail (persistent)
                if audit_trail.is_file_processed(filepath):
                    continue

                if not os.path.exists(filepath):
                    continue

                if not is_file_stable(filepath):
                    logger.debug(
                        "BatchPoller: file still writing",
                        file=filepath,
                    )
                    continue

                # File is ready — log and process
                log_stage_start(logger, stage_name="file_detection", file=filepath, poller="batch")

                try:
                    self.processor.process(filepath, file_type="batch")
                except Exception as e:
                    logger.error("BatchPoller: processor failed", file=filepath, error=str(e))
                    # Alert already sent by processor

            # ── Check if ALL 13 files are done ─────────────────
            all_done = all(
                audit_trail.is_file_processed(os.path.join(batch_dir, f))
                for f in BATCH_FILES
            )

            if all_done:
                tomorrow_11pm = datetime.combine(
                    now.date() + timedelta(days=1),
                    datetime.min.time().replace(hour=BATCH_WINDOW_START)
                )
                sleep_sec = (tomorrow_11pm - now).total_seconds()

                log_stage_complete(
                    logger,
                    stage_name="file_detection",
                    records=len(BATCH_FILES),
                    latency_ms=0.0,
                    poller="batch",
                    sleep_hours=round(sleep_sec / 3600, 1),
                    wake_at=f"{BATCH_WINDOW_START:02d}:00 tomorrow"
                )
                time.sleep(sleep_sec)

            else:
                missing = [
                    f for f in BATCH_FILES
                    if not audit_trail.is_file_processed(os.path.join(batch_dir, f))
                ]

                # ── Past window end and still missing — alert once ────
                if now.hour >= BATCH_WINDOW_END and not alert_sent:
                    log_alert_fired(
                        logger,
                        alert_type="BATCH_FILES_MISSING",
                        severity="CRITICAL",
                        message=f"Past {BATCH_WINDOW_END:02d}:00 and batch still incomplete",
                        stage="file_detection",
                        date=today,
                        missing=missing,
                        missing_count=len(missing),
                    )

                    # Send email alert if alerter is configured
                    if self.alerter and hasattr(self.alerter, "send_alert"):
                        self.alerter.send_alert(
                            error_type="BATCH_FILES_MISSING",
                            message=f"Date: {today}\nMissing files: {missing}\nMissing count: {len(missing)}/13",
                            run_id=None
                        )
                    else:
                        logger.warning("BatchPoller: alerter not configured", alert_type="BATCH_FILES_MISSING")

                    alert_sent = True

                else:
                    logger.debug(
                        "BatchPoller: batch incomplete, still waiting",
                        processed=len(BATCH_FILES) - len(missing),
                        total=len(BATCH_FILES),
                        missing=missing,
                    )

                time.sleep(self.poll_interval)


# ── Stream Poller ─────────────────────────────────────────────

class StreamPoller:
    """
    Scans all HH/ subfolders under today's stream directory.
    Runs continuously, scanning every poll_interval seconds.
    Calls processor.process() for each new file found.
    Each file is reported exactly once (persistent across restarts via audit_trail).

    Hour folders are scanned oldest → newest for correct ordering.
    Cross-restart dedup: handled by audit_trail.is_file_processed()
    """

    def __init__(self, stream_base_dir: str, processor, alerter=None, poll_interval: int = 30):
        self.stream_base_dir = stream_base_dir
        self.processor = processor
        self.poll_interval = poll_interval
        self.running = False
        self._thread = None
        self.alerter = alerter  # Reserved for future stream alerts

    def start(self):
        self.running = True
        self._thread = threading.Thread(
            target=self._run,
            name="StreamPoller",
            daemon=True
        )
        self._thread.start()
        logger.info("StreamPoller started", poll_interval=self.poll_interval)
        return self._thread

    def stop(self):
        self.running = False
        logger.info("StreamPoller stopped")

    def _run(self):
        while self.running:
            today = date.today().isoformat()
            stream_dir = os.path.join(self.stream_base_dir, today)

            if not os.path.exists(stream_dir):
                logger.debug("StreamPoller: no stream dir yet", date=today)
                time.sleep(self.poll_interval)
                continue

            # Scan all HH/ subfolders sorted oldest → newest
            try:
                hour_folders = sorted([
                    f for f in os.listdir(stream_dir)
                    if os.path.isdir(os.path.join(stream_dir, f))
                ])
            except OSError as e:
                logger.error("StreamPoller: cannot read stream dir", dir=stream_dir, error=str(e))
                time.sleep(self.poll_interval)
                continue

            for hour_folder in hour_folders:
                hour_path = os.path.join(stream_dir, hour_folder)

                for filename in STREAM_FILES:
                    filepath = os.path.join(hour_path, filename)

                    if not os.path.exists(filepath):
                        continue

                    # Check if already processed using audit_trail (persistent)
                    if audit_trail.is_file_processed(filepath):
                        continue

                    if not is_file_stable(filepath):
                        logger.debug(
                            "StreamPoller: file still writing",
                            file=filepath,
                            hour=hour_folder,
                        )
                        continue

                    # File is ready — log and process
                    log_stage_start(
                        logger,
                        stage_name="file_detection",
                        file=filepath,
                        poller="stream",
                        hour=hour_folder,
                    )

                    try:
                        self.processor.process(filepath, file_type="stream")
                    except Exception as e:
                        logger.error("StreamPoller: processor failed", file=filepath, error=str(e))
                        # Alert already sent by processor

            time.sleep(self.poll_interval)