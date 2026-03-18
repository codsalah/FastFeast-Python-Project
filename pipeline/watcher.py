"""
pipeline/watcher.py
───────────────────
Directory polling for batch and stream files.

Two pollers run in separate threads:
    BatchPoller  — watches from 6AM, processes 13 batch files once per day,
                   then sleeps until 6AM tomorrow
    StreamPoller — scans every 30 seconds continuously throughout the day

FUTURE CHANGES NEEDED:
    1. BatchPoller.processor.process() — currently calls TestProcessor (prints only)
       Later: replace with real FileProcessor that validates + loads to DB
    2. processed_today set — currently in-memory only, lost on restart
       Later: replace with file_tracking table check
    3. processed set — same as above
       Later: replace with file_tracking table check
    4. Alerter — currently missing, batch failure only logs a warning
       Later: wire in AlertConfig from settings to send email on batch missing
    5. _get_fallback_batch_dir — not implemented yet (IF NEEDED)
       Later: implement fallback to yesterday's batch when today's never arrives

CHECK:
   LOGGING
"""

import os
import time
import logging
import threading
from datetime import date, datetime, timedelta

logger = logging.getLogger(__name__)

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
BATCH_WINDOW_START = 6   # 6AM  — before this, batch never arrives
BATCH_WINDOW_END   = 14  # 2PM  — after all 13 done, sleep until next 6AM, but if past 2PM and still missing, keep watching + log warning


# ── File stability check ──────────────────────────────────────

def is_file_stable(filepath: str, wait_sec: int = 1) -> bool:
    """
    Check if a file is done being written.
    Compare size before and after wait_sec seconds.
    If size is the same the file is safe to read.

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
    - Before 6AM: sleep until 6AM — batch never arrives this early
    - 6AM onward: scan every poll_interval seconds until all 13 files found
    - All 13 done: sleep until 6AM tomorrow — no point scanning again today
    - If batch still missing after 2PM: keep watching + log warning
    - Midnight: reset for the new day
    """

    def __init__(self, batch_base_dir: str, processor, poll_interval: int = 60):
        self.batch_base_dir = batch_base_dir
        self.processor      = processor
        self.poll_interval  = poll_interval
        self.running        = False
        self._thread        = None

        # FUTURE: add alerter parameter
        # def __init__(self, batch_base_dir, processor, alerter, poll_interval=60):
        #     self.alerter = alerter

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
        #          replace every check (if filepath in processed_today)
        #          with: file_tracker.get_status(filepath) == "success"
        #          where file_tracker is a FileTracker instance from pipeline/file_tracker.py
        processed_today = set()
        current_day     = None
        alert_sent      = False   # prevents sending duplicate alerts same day

        while self.running:
            today = date.today().isoformat()
            now   = datetime.now()

            # ── Midnight reset ─────────────────────────────────
            if today != current_day:
                logger.info(f"BatchPoller: new day {today}")
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
                    f"BatchPoller: before batch window. "
                    f"Sleeping {sleep_sec/60:.0f} min until 6AM."
                )
                time.sleep(sleep_sec)
                continue

            # ── Batch folder not here yet ──────────────────────
            batch_dir = os.path.join(self.batch_base_dir, today)

            if not os.path.exists(batch_dir):
                logger.debug(f"BatchPoller: waiting for {batch_dir}")
                time.sleep(self.poll_interval)
                continue

            # ── Scan all 13 expected files ─────────────────────
            for filename in BATCH_FILES:
                filepath = os.path.join(batch_dir, filename)

                # CURRENT: check in-memory set
                # FUTURE:  status = file_tracker.get_status(filepath)
                #          if status == "success": continue
                #          if status == "in_progress": pass  ← reprocess crashed file
                #          if status is None: pass           ← new file, process it
                if filepath in processed_today:
                    continue

                if not os.path.exists(filepath):
                    continue

                if not is_file_stable(filepath):
                    logger.debug(f"BatchPoller: file still writing: {filepath}")
                    continue

                logger.info(f"BatchPoller: found → {filepath}")

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
                # FUTURE:  remove this line — file_tracking table handles dedup
                processed_today.add(filepath)

            # ── Check if ALL 13 files are done ─────────────────
            all_done = all(
                os.path.join(batch_dir, f) in processed_today
                for f in BATCH_FILES
                # FUTURE: replace with:
                # file_tracker.get_status(os.path.join(batch_dir, f)) == "success"
            )

            if all_done:
                tomorrow_6am = datetime.combine(
                    now.date() + timedelta(days=1),
                    datetime.min.time().replace(hour=BATCH_WINDOW_START)
                )
                sleep_sec = (tomorrow_6am - now).total_seconds()
                logger.info(
                    f"BatchPoller: all {len(BATCH_FILES)} files processed. "
                    f"Sleeping {sleep_sec/3600:.1f}h until 6AM tomorrow."
                )
                time.sleep(sleep_sec)

            else:
                missing = [
                    f for f in BATCH_FILES
                    if os.path.join(batch_dir, f) not in processed_today
                    # FUTURE: replace condition with file_tracking check
                ]

                # ── Past 2PM and still missing — alert once ────
                if now.hour >= BATCH_WINDOW_END and not alert_sent:
                    logger.critical(
                        f"BatchPoller: CRITICAL — past {BATCH_WINDOW_END}:00, "
                        f"batch still incomplete. Missing: {missing}"
                    )

                    # FUTURE: send real alert
                    # self.alerter.send_async(
                    #     subject="CRITICAL: Batch files missing",
                    #     body=f"Date: {today}\nMissing: {missing}"
                    # )

                    # FUTURE: load fallback from yesterday's batch
                    # fallback_dir = self._get_fallback_batch_dir(today)
                    # if fallback_dir:
                    #     self._load_fallback(fallback_dir)
                    # else:
                    #     logger.critical("No fallback batch available")

                    alert_sent = True

                else:
                    logger.debug(
                        f"BatchPoller: {len(processed_today)}/{len(BATCH_FILES)} done. "
                        f"Still waiting for: {missing}"
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
    #             logger.warning(f"BatchPoller: fallback found at {past_dir}")
    #             return past_dir
    #     return None


# ── Stream Poller ─────────────────────────────────────────────

class StreamPoller:
    """
    Scans all HH/ subfolders under today's stream directory.
    Runs every 30 seconds continuously.
    Calls processor.process() for each new file found.
    Each file is reported exactly once per session.

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
        #          with: file_tracker.get_status(filepath) == "success"
        processed = set()

        while self.running:
            today      = date.today().isoformat()
            stream_dir = os.path.join(self.stream_base_dir, today)

            if not os.path.exists(stream_dir):
                logger.debug(f"StreamPoller: no stream dir yet for {today}")
                time.sleep(self.poll_interval)
                continue

            # Scan all HH/ subfolders sorted oldest → newest
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
                    # FUTURE:  status = file_tracker.get_status(filepath)
                    #          if status == "success":     continue
                    #          if status == "in_progress": pass  ← crashed, reprocess
                    #          if status is None:          pass  ← new file
                    if filepath in processed:
                        continue

                    if not is_file_stable(filepath):
                        logger.debug(f"StreamPoller: file still writing: {filepath}")
                        continue

                    logger.info(f"StreamPoller: found → {filepath}")

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
                    # FUTURE:  remove — file_tracking handles this permanently
                    processed.add(filepath)

            time.sleep(self.poll_interval)