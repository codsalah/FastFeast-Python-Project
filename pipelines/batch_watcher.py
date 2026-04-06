"""
pipelines/batch_watcher.py
Polls for daily batch snapshot files (typically the overnight window).

Responsibility: detect when batch files arrive on disk and hand them
                to whatever processor is given at construction time.
                Nothing else.

Behaviour:
  - Before 11PM : sleeps until 11PM
  - 11PM onward : scans every poll_interval seconds until all 13 files are found
  - All 13 done  : sleeps until 11PM tomorrow
  - Past 1AM with files still missing → fires one CRITICAL alert
"""

from __future__ import annotations

import os
import threading
import time
from datetime import date, datetime, timedelta

from quality import metrics_tracker as audit_trail
from utils.file_utils import is_file_stable
from utils.logger import (
    get_logger_name,
    log_alert_fired,
    log_stage_complete,
    log_stage_start,
)

logger = get_logger_name(__name__)

# All files expected under data/input/batch/<YYYY-MM-DD>/
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

BATCH_WINDOW_START = 23  # 11PM — before this hour batch files never arrive
BATCH_WINDOW_END   = 1   # 1AM  — past this hour with files still missing → CRITICAL


class BatchPoller:
    """
    Watches for today's 13 batch files under batch_base_dir/<YYYY-MM-DD>/.

    Start with .start() → runs in a daemon thread.
    Stop  with .stop()  → signals the thread to exit.
    """

    def __init__(
        self,
        batch_base_dir: str,
        processor=None,  # No longer required - batch processing handled internally
        alerter=None,
        poll_interval: int = 60,
    ):
        self.batch_base_dir = batch_base_dir
        self.processor      = processor       # kept for backward compatibility, but not used
        self.alerter        = alerter         # must implement .send_alert(...) or None
        self.poll_interval  = poll_interval
        self.running        = False
        self._thread: threading.Thread | None = None

    def start(self) -> threading.Thread:
        self.running = True
        self._thread = threading.Thread(target=self._run, name="BatchPoller", daemon=True)
        self._thread.start()
        logger.info("BatchPoller started", poll_interval=self.poll_interval)
        return self._thread

    def stop(self) -> None:
        self.running = False
        logger.info("BatchPoller stopped")

    # ── internal ──────────────────────────────────────────────────────────────

    def _run(self) -> None:
        current_day: str | None = None
        alert_sent = False

        while self.running:
            today = date.today().isoformat()
            now   = datetime.now()

            # reset per-day state at midnight
            if today != current_day:
                logger.info("BatchPoller: new day", date=today)
                current_day = today
                alert_sent  = False

            # before 11PM: sleep until 11PM
            if now.hour < BATCH_WINDOW_START:
                wake_at   = datetime.combine(now.date(), datetime.min.time().replace(hour=BATCH_WINDOW_START))
                sleep_sec = (wake_at - now).total_seconds()
                logger.info(
                    "BatchPoller: before batch window, sleeping",
                    sleep_min=round(sleep_sec / 60),
                    wake_at=f"{BATCH_WINDOW_START:02d}:00",
                )
                time.sleep(sleep_sec)
                continue

            batch_dir = os.path.join(self.batch_base_dir, today)

            if not os.path.exists(batch_dir):
                logger.debug("BatchPoller: waiting for batch dir", batch_dir=batch_dir)
                time.sleep(self.poll_interval)
                continue

            # Check if all batch files are present and stable
            all_files_present = True
            for filename in BATCH_FILES:
                filepath = os.path.join(batch_dir, filename)
                if not os.path.exists(filepath) or not is_file_stable(filepath):
                    all_files_present = False
                    break

            if all_files_present:
                # All batch files are ready - trigger the batch pipeline
                log_stage_start(logger, stage_name="batch_processing", poller="batch", date=today)
                try:
                    from pipelines.batch_pipeline import BatchPipeline
                    from config.settings import get_settings
                    
                    settings = get_settings()
                    batch_pipeline = BatchPipeline(
                        batch_date=date.fromisoformat(today),
                        settings=settings,
                        manage_pool=False  # Pool already managed by watcher
                    )
                    batch_pipeline.run()
                    
                    logger.info("BatchPoller: batch processing completed", date=today)
                    
                except Exception as e:
                    logger.error("BatchPoller: batch processing failed", date=today, error=str(e))
                    # Don't mark files as processed on failure
                    
                # Sleep until next batch window
                tomorrow_11pm = datetime.combine(
                    now.date() + timedelta(days=1),
                    datetime.min.time().replace(hour=BATCH_WINDOW_START),
                )
                sleep_sec = (tomorrow_11pm - now).total_seconds()
                log_stage_complete(
                    logger,
                    stage_name="batch_processing",
                    records=len(BATCH_FILES),
                    latency_ms=0.0,
                    poller="batch",
                    sleep_hours=round(sleep_sec / 3600, 1),
                    wake_at=f"{BATCH_WINDOW_START:02d}:00 tomorrow",
                )
                time.sleep(sleep_sec)
                continue

            else:
                missing = [
                    f for f in BATCH_FILES
                    if not audit_trail.is_file_processed(os.path.join(batch_dir, f))
                ]

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
                    if self.alerter and hasattr(self.alerter, "send_alert"):
                        self.alerter.send_alert(
                            error_type="BATCH_FILES_MISSING",
                            message=(
                                f"Date: {today}\n"
                                f"Missing files: {missing}\n"
                                f"Missing count: {len(missing)}/13"
                            ),
                            run_id=None,
                        )
                    else:
                        logger.warning("BatchPoller: alerter not configured")
                    alert_sent = True
                else:
                    logger.debug(
                        "BatchPoller: batch incomplete, still waiting",
                        processed=len(BATCH_FILES) - len(missing),
                        total=len(BATCH_FILES),
                        missing=missing,
                    )

                time.sleep(self.poll_interval)