"""
pipeline/watcher.py
───────────────────
Directory polling for batch and stream files.

Two pollers run in separate threads:
    BatchPoller  — watches from 6AM, processes 13 batch files once per day,
                   then sleeps until 6AM tomorrow
    StreamPoller — scans continuously throughout the day for stream files
                   that arrive at irregular intervals

FUTURE CHANGES NEEDED:
    1. BatchPoller.processor.process() — currently calls TestProcessor (prints only)
       Later: replace with real FileProcessor that validates + loads to DB
    2. processed_today set — currently in-memory only, lost on restart
       Later: replace with audit_trail.is_file_processed(filepath) check
    3. processed set — same as above
       Later: replace with audit_trail.is_file_processed(filepath) check
    4. Alerter — currently missing, batch failure only logs a warning
       Later: wire in AlertConfig from settings to send email on batch missing
    5. _get_fallback_batch_dir — not implemented yet (IF NEEDED)
       Later: implement fallback to yesterday's batch when today's never arrives
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
BATCH_WINDOW_START = 23   # 11PM  — before this, batch never arrives
BATCH_WINDOW_END   = 1  # 1AM  — past this with files missing → log CRITICAL alert


# ── File stability check ──────────────────────────────────────

def is_file_stable(filepath: str, wait_sec: int = 1) -> bool:
    """
    Check if a file is done being written.
    Compare size before and after wait_sec seconds.
    If size is unchanged the file is safe to read.

    wait_sec: 1 for local files, 2-3 for network drives.
    """
    try:
        size_before = os.path.getsize(filepath)
        time.sleep(wait_sec)
        size_after  = os.path.getsize(filepath)
        return size_before == size_after
    except OSError:
        return False


# ── Batch Poller ──────────────────────────────────────────────

class BatchPoller:
    """
    Watches for today's 13 batch files.

    Behaviour:
    - Before 11PM: sleep until 11PM — batch never arrives this early
    - 11PM onward: scan every poll_interval seconds until all 13 files found
    - All 13 done: sleep until 11PM tomorrow — no point scanning again today
    - If batch still missing after 1AM: log CRITICAL alert once, keep watching
    - 11PM: reset for the new day
    """

    def __init__(self, batch_base_dir: str, processor, alerter, poll_interval: int = 60):
        self.batch_base_dir = batch_base_dir
        self.processor      = processor
        self.poll_interval  = poll_interval
        self.running        = False
        self._thread        = None

        self.alerter = alerter

    def start(self):
        self.running = True
        self._thread = threading.Thread(
            target=self._run,
            name="BatchPoller",
            daemon=True
        )
        self._thread.start()
        logger.info("BatchPoller started")
        return self._thread

    def stop(self):
        self.running = False
        logger.info("BatchPoller stopped")

    def _run(self):

        # CURRENT: in-memory set — lost every time pipeline restarts
        # FUTURE:  remove this set entirely
        #          replace every (if filepath in processed_today) check
        #          with: audit_trail.is_file_processed(filepath)
        processed_today = set()
        current_day     = None
        alert_sent      = False   # prevents duplicate alerts on the same day

        while self.running:
            today = date.today().isoformat()
            now   = datetime.now()

            # ── Midnight reset ─────────────────────────────────
            if today != current_day:
                logger.info("BatchPoller: new day", date=today)
                processed_today = set()   # FUTURE: no set to clear — file_tracking handles this
                current_day     = today
                alert_sent      = False

            # ── Before 6AM: sleep until 6AM ───────────────────
            if now.hour < BATCH_WINDOW_START:
                wake_at   = datetime.combine(
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

                # CURRENT: check in-memory set
                # FUTURE:  if audit_trail.is_file_processed(filepath): continue
                if filepath in processed_today:
                    continue

                if not os.path.exists(filepath):
                    continue

                if not is_file_stable(filepath):
                    logger.debug(
                        "BatchPoller: file still writing",
                        file=filepath,
                    )
                    continue

                # File is ready — log stage start then hand to processor
                log_stage_start(logger, stage_name="file_detection", file=filepath, poller="batch")

                # CURRENT: calls TestProcessor which only prints
                # FUTURE:  calls real FileProcessor which:
                #          1. marks file as in_progress in file_tracking
                #          2. reads and parses file
                #          3. validates schema and business rules
                #          4. checks orphans
                #          5. loads to warehouse
                #          6. marks file as success in file_tracking
                self.processor.process(filepath, file_type="batch")

                # CURRENT: add to in-memory set
                # FUTURE:  remove — audit_trail.mark_file_success() handles dedup
                processed_today.add(filepath)

            # ── Check if ALL 13 files are done ─────────────────
            all_done = all(
                os.path.join(batch_dir, f) in processed_today
                for f in BATCH_FILES
                # FUTURE: replace with:
                # audit_trail.is_file_processed(os.path.join(batch_dir, f))
            )

            if all_done:
                tomorrow_6am = datetime.combine(
                    now.date() + timedelta(days=1),
                    datetime.min.time().replace(hour=BATCH_WINDOW_START)
                )
                sleep_sec = (tomorrow_6am - now).total_seconds()

                # All 13 done — log stage complete then sleep until tomorrow
                log_stage_complete(
                    logger,
                    stage_name = "file_detection",
                    records    = len(BATCH_FILES),
                    latency_ms = 0.0,
                    poller     = "batch",
                    sleep_hours= round(sleep_sec / 3600, 1),
                    wake_at    = f"{BATCH_WINDOW_START:02d}:00 tomorrow"
                )
                time.sleep(sleep_sec)

            else:
                missing = [
                    f for f in BATCH_FILES
                    if os.path.join(batch_dir, f) not in processed_today
                    # FUTURE: replace condition with audit_trail check
                ]

                # ── Past 2PM and still missing — alert once ────
                if now.hour >= BATCH_WINDOW_END and not alert_sent:
                    log_alert_fired(
                        logger,
                        alert_type    = "BATCH_FILES_MISSING",
                        severity      = "CRITICAL",
                        message       = f"Past {BATCH_WINDOW_END:02d}:00 and batch still incomplete",
                        stage         = "file_detection",
                        date          = today,
                        missing       = missing,
                        missing_count = len(missing),
                    )

                    # Send real email alert
                    if hasattr(self, "alerter") and self.alerter:
                        if hasattr(self.alerter, "send_alert"):
                            self.alerter.send_alert(
                                error_type="BATCH_FILES_MISSING",
                                message=f"Date: {today}\nMissing: {missing}"
                            )
                        elif hasattr(self.alerter, "send_async"):
                            self.alerter.send_async(
                                subject="CRITICAL: Batch files missing",
                                body=f"Date: {today}\nMissing: {missing}"
                            )

                    # FUTURE: load fallback from yesterday's batch
                    # fallback_dir = self._get_fallback_batch_dir(today)
                    # if fallback_dir:
                    #     self._load_fallback(fallback_dir)
                    # else:
                    #     logger.critical("No fallback batch available")

                    alert_sent = True

                else:
                    logger.debug(
                        "BatchPoller: batch incomplete, still waiting",
                        processed=len(processed_today),
                        total=len(BATCH_FILES),
                        missing=missing,
                    )

                time.sleep(self.poll_interval)

    # FUTURE: implement this method
    # def _get_fallback_batch_dir(self, today: str) -> str | None:
    #     """Find most recent previous complete batch as fallback."""
    #     for days_back in range(1, 8):
    #         past_date = (
    #             datetime.strptime(today, "%Y-%m-%d") - timedelta(days=days_back)
    #         ).strftime("%Y-%m-%d")
    #         past_dir  = os.path.join(self.batch_base_dir, past_date)
    #         past_done = all(
    #             os.path.exists(os.path.join(past_dir, f))
    #             for f in BATCH_FILES
    #         )
    #         if past_done:
    #             logger.warning("BatchPoller: fallback batch found", fallback_dir=past_dir)
    #             return past_dir
    #     return None


# ── Stream Poller ─────────────────────────────────────────────

class StreamPoller:
    """
    Scans all HH/ subfolders under today's stream directory.
    Runs continuously, scanning every poll_interval seconds.
    Calls processor.process() for each new file found.
    Each file is reported exactly once per session.

    Files arrive at irregular intervals throughout the day — not on a fixed schedule.
    Hour folders are scanned oldest → newest for correct ordering on recovery
    after an unexpected restart.

    Within-session dedup:  handled by processed set (current)
    Cross-restart dedup:   handled by file_tracking table (future)
    """

    def __init__(self, stream_base_dir: str, processor, poll_interval: int = 30):
        self.stream_base_dir = stream_base_dir
        self.processor       = processor
        self.poll_interval   = poll_interval
        self.running         = False
        self._thread         = None

    def start(self):
        self.running = True
        self._thread = threading.Thread(
            target=self._run,
            name="StreamPoller",
            daemon=True
        )
        self._thread.start()
        logger.info("StreamPoller started")
        return self._thread

    def stop(self):
        self.running = False
        logger.info("StreamPoller stopped")

    def _run(self):

        # CURRENT: in-memory set — works within one session only
        # FUTURE:  remove this set entirely
        #          replace (if filepath in processed) check
        #          with: audit_trail.is_file_processed(filepath)
        processed = set()

        while self.running:
            today      = date.today().isoformat()
            stream_dir = os.path.join(self.stream_base_dir, today)

            if not os.path.exists(stream_dir):
                logger.debug("StreamPoller: no stream dir yet", date=today)
                time.sleep(self.poll_interval)
                continue

            # Scan all HH/ subfolders sorted oldest → newest
            # Ensures correct processing order on recovery after unexpected restart
            hour_folders = sorted([
                f for f in os.listdir(stream_dir)
                if os.path.isdir(os.path.join(stream_dir, f))
            ])

            for hour_folder in hour_folders:
                hour_path = os.path.join(stream_dir, hour_folder)

                for filename in STREAM_FILES:
                    filepath = os.path.join(hour_path, filename)

                    if not os.path.exists(filepath):
                        continue

                    # CURRENT: check in-memory set
                    # FUTURE:  if audit_trail.is_file_processed(filepath): continue
                    if filepath in processed:
                        continue

                    if not is_file_stable(filepath):
                        logger.debug(
                            "StreamPoller: file still writing",
                            file=filepath,
                            hour=hour_folder,
                        )
                        continue

                    # File is ready — log stage start then hand to processor
                    log_stage_start(
                        logger,
                        stage_name = "file_detection",
                        file       = filepath,
                        poller     = "stream",
                        hour       = hour_folder,
                    )

                    # CURRENT: calls TestProcessor which only prints
                    # FUTURE:  calls real FileProcessor which:
                    #          1. marks in_progress in file_tracking
                    #          2. reads file (pandas / json.load)
                    #          3. validates schema + business rules
                    #          4. deduplicates on ID column
                    #          5. checks nulls → quarantine
                    #          6. checks orphans → orphan_staging
                    #          7. loads valid records to warehouse
                    #          8. calculates SLA fields (fact_tickets)
                    #          9. logs quality metrics
                    #          10. marks success in file_tracking
                    self.processor.process(filepath, file_type="stream")

                    # CURRENT: add to in-memory set
                    # FUTURE:  remove — audit_trail.mark_file_success() handles this
                    processed.add(filepath)

            time.sleep(self.poll_interval)