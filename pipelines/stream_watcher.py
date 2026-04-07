"""
pipelines/stream_watcher.py
Polls stream_input_dir/<date>/<HH>/ for micro-batch stream files.

Responsibility: detect when stream files arrive on disk and hand them
                to whatever processor is given at construction time.
                Nothing else.

Behaviour:
  - Runs continuously throughout the day
  - Scans all HH/ sub-folders oldest → newest (so earlier hours are never skipped)
  - Each file is handed to the processor exactly once (persistent dedup via audit_trail)
  - Cross-restart safe: audit_trail survives process restarts
"""

from __future__ import annotations

import os
import threading
import time
from datetime import date

from quality import metrics_tracker as audit_trail
from utils.file_utils import is_file_stable
from utils.logger import get_logger_name, log_stage_start

logger = get_logger_name(__name__)

# All files expected under stream_input_dir/<date>/<HH>/
STREAM_FILES = [
    "orders.json",
    "tickets.csv",
    "ticket_events.json",
]


class StreamPoller:
    """
    Scans all HH/ sub-folders under today's stream directory.

    Start with .start() → runs in a daemon thread.
    Stop  with .stop()  → signals the thread to exit.
    """

    def __init__(
        self,
        stream_base_dir: str,
        processor,
        alerter=None,
        poll_interval: int = 30,
    ):
        self.stream_base_dir = stream_base_dir
        self.processor       = processor   # must implement .process(filepath, file_type)
        self.alerter         = alerter     # reserved for future stream-level alerts
        self.poll_interval   = poll_interval
        self.running         = False
        self._thread: threading.Thread | None = None

    def start(self) -> threading.Thread:
        self.running = True
        self._thread = threading.Thread(target=self._run, name="StreamPoller", daemon=True)
        self._thread.start()
        logger.info("StreamPoller started", poll_interval=self.poll_interval)
        return self._thread

    def stop(self) -> None:
        self.running = False
        logger.info("StreamPoller stopped")

    # ── internal ──────────────────────────────────────────────────────────────

    def _run(self) -> None:
        while self.running:
            today      = date.today().isoformat()
            stream_dir = os.path.join(self.stream_base_dir, today)

            if not os.path.exists(stream_dir):
                logger.debug("StreamPoller: no stream dir yet", date=today)
                time.sleep(self.poll_interval)
                continue

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
                    if audit_trail.is_file_processed(filepath):
                        continue
                    if not is_file_stable(filepath):
                        logger.debug("StreamPoller: file still writing", file=filepath, hour=hour_folder)
                        continue

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

            time.sleep(self.poll_interval)