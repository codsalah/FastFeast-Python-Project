"""
pipelines/stream_pipeline.py
───────────────────
Directory polling for batch and stream files.

Two pollers run in separate threads:
    BatchPoller  — watches from 11PM, processes 13 batch files once per day,
                   then sleeps until 11PM tomorrow
    StreamPoller — scans continuously throughout the day for stream files
                   that arrive at irregular intervals
"""

import os
import time
import threading
import sys
from pathlib import Path
from datetime import date, datetime, timedelta

from utils.logger import (
    get_logger_name,
    log_stage_start,
    log_stage_complete,
    log_alert_fired,
    configure_logging
)
from utils.file_utils import is_file_stable, detect_format
from utils.file_tracker import compute_file_hash

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from quality import metrics_tracker as audit_trail
from warehouse.connection import init_pool, close_pool
from config.settings import get_settings
from loaders import fact_orders_loader, fact_tickets_loader, fact_events_loader

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
BATCH_WINDOW_START = 23   # 11PM
BATCH_WINDOW_END   = 1    # 1AM


# ── Processor ─────────────────────────────────────────────────

class RealStreamProcessor:
    """
    Actual stream processor that routes files to their respective loaders.
    """
    def __init__(self, run_id: int):
        self.run_id = run_id
        self.processed_count = 0

    def process(self, filepath: str, file_type: str) -> None:
        filename = os.path.basename(filepath)
        
        # Consistent file tracking and registration
        if audit_trail.is_file_processed(filepath):
            logger.info("file_skipped", file=filename, reason="already_processed")
            return

        # Registration happens inside the loaders, but we ensure routing is correct
        try:
            if "orders" in filename.lower():
                fact_orders_loader.load(filepath, self.run_id)
            elif "tickets" in filename.lower() and "events" not in filename.lower():
                fact_tickets_loader.load(filepath, self.run_id)
            elif "event" in filename.lower():
                fact_events_loader.load(filepath, self.run_id)
            else:
                logger.warning("StreamProcessor: unknown file type", file=filename)
                return

            self.processed_count += 1
        except Exception as e:
            logger.error("processor_execution_failed", file=filename, error=str(e))
            # The loaders themselves handle mark_file_failed, but we catch top-level for safety


# ── Batch Poller ──────────────────────────────────────────────

class BatchPoller:
    def __init__(self, batch_base_dir: str, processor, alerter=None, poll_interval: int = 60):
        self.batch_base_dir = batch_base_dir
        self.processor = processor
        self.poll_interval = poll_interval
        self.running = False
        self._thread = None
        self.alerter = alerter

    def start(self):
        self.running = True
        self._thread = threading.Thread(target=self._run, name="BatchPoller", daemon=True)
        self._thread.start()
        logger.info("BatchPoller started", poll_interval=self.poll_interval)
        return self._thread

    def stop(self):
        self.running = False
        logger.info("BatchPoller stopped")

    def _run(self):
        current_day = None
        while self.running:
            today = date.today().isoformat()
            now = datetime.now()
            if today != current_day:
                current_day = today
            
            if now.hour < BATCH_WINDOW_START:
                time.sleep(60)
                continue

            batch_dir = os.path.join(self.batch_base_dir, today)
            if not os.path.exists(batch_dir):
                time.sleep(self.poll_interval)
                continue

            for filename in BATCH_FILES:
                filepath = os.path.join(batch_dir, filename)
                if audit_trail.is_file_processed(filepath):
                    continue
                if os.path.exists(filepath) and is_file_stable(filepath):
                    try:
                        self.processor.process(filepath, file_type="batch")
                    except Exception as e:
                        logger.error("BatchPoller failed", file=filepath, error=str(e))
            
            time.sleep(self.poll_interval)


# ── Stream Poller ─────────────────────────────────────────────

class StreamPoller:
    def __init__(self, stream_base_dir: str, processor, alerter=None, poll_interval: int = 30):
        self.stream_base_dir = stream_base_dir
        self.processor = processor
        self.poll_interval = poll_interval
        self.running = False
        self._thread = None

    def start(self):
        self.running = True
        self._thread = threading.Thread(target=self._run, name="StreamPoller", daemon=True)
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
                time.sleep(self.poll_interval)
                continue

            try:
                hour_folders = sorted([f for f in os.listdir(stream_dir) if os.path.isdir(os.path.join(stream_dir, f))])
            except OSError:
                time.sleep(self.poll_interval)
                continue

            for hour_folder in hour_folders:
                hour_path = os.path.join(stream_dir, hour_folder)
                for filename in STREAM_FILES:
                    filepath = os.path.join(hour_path, filename)
                    if not os.path.exists(filepath) or audit_trail.is_file_processed(filepath):
                        continue
                    if is_file_stable(filepath):
                        try:
                            self.processor.process(filepath, file_type="stream")
                        except Exception as e:
                            logger.error("StreamPoller failed", file=filepath, error=str(e))
            
            time.sleep(self.poll_interval)


# ── CLI Runner ────────────────────────────────────────────────

def run_watcher():
    settings = get_settings()
    init_pool(settings)
    audit_trail.ensure_audit_schema()
    run_id = audit_trail.start_run("watcher")
    processor = RealStreamProcessor(run_id=run_id)
    batch_poller = BatchPoller(settings.batch_input_dir, processor, poll_interval=30)
    stream_poller = StreamPoller(settings.stream_input_dir, processor, poll_interval=10)
    batch_poller.start()
    stream_poller.start()
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        batch_poller.stop()
        stream_poller.stop()
        audit_trail.complete_run(run_id, "stopped", processor.processed_count, processor.processed_count, 0, 0, 0, 0, 0)
        close_pool()

def process_single_hour(target_date: date, target_hour: int):
    settings = get_settings()
    init_pool(settings)
    audit_trail.ensure_audit_schema()
    run_id = audit_trail.start_run("stream_one_off")
    processor = RealStreamProcessor(run_id=run_id)
    stream_dir = os.path.join(settings.stream_input_dir, target_date.isoformat(), f"{target_hour:02d}")
    if not os.path.exists(stream_dir):
        logger.error("Stream directory not found", dir=stream_dir)
        audit_trail.complete_run(run_id, "failed", 0, 0, 0, 0, 0, 0, 0, error_message="Dir not found")
        close_pool()
        return
    files_found = 0
    for filename in STREAM_FILES:
        filepath = os.path.join(stream_dir, filename)
        if os.path.exists(filepath):
            files_found += 1
            processor.process(filepath, "stream")
    audit_trail.complete_run(run_id, "success", files_found, processor.processed_count, files_found - processor.processed_count, 0, 0, 0, 0)
    logger.info("One-off stream processing complete", date=str(target_date), hour=target_hour, files=files_found)
    close_pool()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=date.fromisoformat)
    parser.add_argument("--hour", type=int)
    parser.add_argument("--watcher", action="store_true")
    args = parser.parse_args()
    configure_logging(log_dir="logs", level="INFO")
    if args.date and args.hour is not None:
        process_single_hour(args.date, args.hour)
    else:
        run_watcher()