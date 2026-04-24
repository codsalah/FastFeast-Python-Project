"""
pipelines/watcher.py
Two-thread watcher orchestrator for stream --watch mode.

Thread model:
  - StreamPollerThread: always runs, scans stream files every 30 seconds.
  - BatchPollerThread: only active during 11PM-1AM window, sleeps otherwise.

This module contains scheduling only. Business logic stays in batch/stream pipelines.
"""

from __future__ import annotations

import os
import threading
import time
from datetime import date, datetime, time as dt_time, timedelta

from alerting.alert_service import AlertService
from config.settings import Settings, get_settings
from pipelines.batch_pipeline import BatchPipeline
from pipelines.stream_pipeline import RealStreamProcessor, process_pending_stream_files
from quality import metrics_tracker as audit_trail
from utils.file_utils import is_file_stable
from utils.logger import get_logger_name
from warehouse.connection import close_pool, init_pool

logger = get_logger_name(__name__)

def is_in_batch_window(now: datetime, *, start_hour: int, end_hour: int) -> bool:
    """True from 23:00 to 00:59."""
    return now.hour >= start_hour or now.hour < end_hour


def _get_batch_dir(batch_base_dir: str, target_date: date) -> str:
    return os.path.join(batch_base_dir, target_date.isoformat())


def _batch_cycle_date(now: datetime, *, end_hour: int) -> date:
    """
    Date label for the current batch cycle:
      - 23:00-23:59 -> today
      - 00:00-00:59 -> yesterday
    """
    if now.hour < end_hour:
        return now.date() - timedelta(days=1)
    return now.date()


def _next_batch_window_start(now: datetime, *, start_hour: int) -> datetime:
    if now.hour < start_hour:
        return datetime.combine(now.date(), dt_time(hour=start_hour))
    return datetime.combine(now.date() + timedelta(days=1), dt_time(hour=start_hour))


def _missing_or_unstable_batch_files(
    *,
    batch_base_dir: str,
    target_date: date,
    required_files: list[str],
) -> list[str]:
    batch_dir = _get_batch_dir(batch_base_dir, target_date)
    missing: list[str] = []
    for filename in required_files:
        filepath = os.path.join(batch_dir, filename)

        if audit_trail.is_file_processed(filepath):
            continue
        if not os.path.exists(filepath):
            missing.append(filename)
            continue
        if not is_file_stable(filepath):
            missing.append(filename)
    return missing


class StreamPollerThread:
    """Continuously scans stream files and processes new arrivals."""

    def __init__(self, *, settings: Settings, processor: RealStreamProcessor, stop_event: threading.Event):
        self.settings = settings
        self.processor = processor
        self.stop_event = stop_event
        self.thread = threading.Thread(target=self._run, name="StreamPollerThread", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def join(self, timeout: float | None = None) -> None:
        self.thread.join(timeout=timeout)

    def _run(self) -> None:
        logger.info("stream_poller_started", poll_seconds=self.settings.stream_poll_seconds)
        while not self.stop_event.is_set():
            today = date.today()
            processed_count = process_pending_stream_files(
                stream_base_dir=self.settings.stream_input_dir,
                processor=self.processor,
                target_date=today,
            )
            if processed_count:
                logger.info("stream_files_processed", date=today.isoformat(), count=processed_count)

            self.stop_event.wait(self.settings.stream_poll_seconds)


class BatchPollerThread:
    """
    Watches daily batch only in 11PM-1AM window.
    Outside window, it sleeps until next 11PM (no polling).
    """

    def __init__(
        self,
        *,
        settings: Settings,
        alerter: AlertService,
        stop_event: threading.Event,
        poll_seconds: int,
    ):
        self.settings = settings
        self.alerter = alerter
        self.stop_event = stop_event
        self.poll_seconds = poll_seconds
        self.batch_window_start_hour = settings.batch_window_start_hour
        self.batch_window_end_hour = settings.batch_window_end_hour
        self.required_files = list(settings.batch_required_files)
        self.completed_cycles: set[date] = set()
        self.alerted_cycles: set[date] = set()
        self.active_cycle: date | None = None
        self.thread = threading.Thread(target=self._run, name="BatchPollerThread", daemon=True)

    def start(self) -> None:
        self.thread.start()

    def join(self, timeout: float | None = None) -> None:
        self.thread.join(timeout=timeout)

    def _sleep_until_next_window(self, now: datetime) -> None:
        wake_at = _next_batch_window_start(now, start_hour=self.batch_window_start_hour)
        sleep_seconds = max(0, (wake_at - now).total_seconds())
        logger.info(
            "batch_poller_sleeping_until_window",
            wake_at=wake_at.isoformat(),
            sleep_minutes=round(sleep_seconds / 60, 1),
        )
        self.stop_event.wait(sleep_seconds)

    def _send_incomplete_alert_if_needed(self, cycle_date: date) -> None:
        if cycle_date in self.completed_cycles or cycle_date in self.alerted_cycles:
            return

        missing = _missing_or_unstable_batch_files(
            batch_base_dir=self.settings.batch_input_dir,
            target_date=cycle_date,
            required_files=self.required_files,
        )
        if not missing:
            return

        logger.error(
            "batch_incomplete_after_window",
            cycle_date=cycle_date.isoformat(),
            missing=missing,
            missing_count=len(missing),
        )
        self.alerter.send_alert(
            error_type="BATCH_FILES_MISSING",
            message=(
                f"Date: {cycle_date.isoformat()}\n"
                f"Missing or unstable files: {missing}\n"
                f"Missing count: {len(missing)}/{len(self.required_files)}"
            ),
            run_id=None,
        )
        self.alerted_cycles.add(cycle_date)

    def _handle_outside_window(self, now: datetime) -> None:
        # If we just left a monitored cycle and it did not complete, alert once.
        if self.active_cycle is not None:
            self._send_incomplete_alert_if_needed(self.active_cycle)
            self.active_cycle = None
        self._sleep_until_next_window(now)

    def _run(self) -> None:
        logger.info(
            "batch_poller_started",
            poll_seconds=self.poll_seconds,
            batch_window=f"{self.batch_window_start_hour:02d}:00-{self.batch_window_end_hour:02d}:00",
        )

        while not self.stop_event.is_set():
            now = datetime.now()

            if not is_in_batch_window(
                now,
                start_hour=self.batch_window_start_hour,
                end_hour=self.batch_window_end_hour,
            ):
                self._handle_outside_window(now)
                continue

            cycle_date = _batch_cycle_date(now, end_hour=self.batch_window_end_hour)
            self.active_cycle = cycle_date

            if cycle_date in self.completed_cycles:
                self._sleep_until_next_window(now)
                continue

            missing = _missing_or_unstable_batch_files(
                batch_base_dir=self.settings.batch_input_dir,
                target_date=cycle_date,
                required_files=self.required_files,
            )
            if not missing:
                logger.info("batch_ready_running_pipeline", cycle_date=cycle_date.isoformat())
                BatchPipeline(
                    batch_date=cycle_date,
                    settings=self.settings,
                    manage_pool=False,
                ).run()
                self.completed_cycles.add(cycle_date)
                self.active_cycle = None
                continue

            logger.debug(
                "batch_waiting_for_files",
                cycle_date=cycle_date.isoformat(),
                ready=len(self.required_files) - len(missing),
                total=len(self.required_files),
                missing=missing,
            )
            self.stop_event.wait(self.poll_seconds)


def run_continuous_watch(
    *,
    settings: Settings | None = None,
    manage_pool: bool = True,
) -> None:
    """
    Start exactly two worker threads:
      1) Stream poller (always on)
      2) Batch poller (window-based)
    """
    settings = settings or get_settings()
    if manage_pool:
        init_pool(settings)

    audit_trail.ensure_audit_schema()
    run_id = audit_trail.start_run("stream")
    processor = RealStreamProcessor(run_id=run_id)
    alerter = AlertService()
    stop_event = threading.Event()

    stream_poller = StreamPollerThread(
        settings=settings,
        processor=processor,
        stop_event=stop_event,
    )
    batch_poller = BatchPollerThread(
        settings=settings,
        alerter=alerter,
        stop_event=stop_event,
        poll_seconds=settings.batch_poll_seconds,
    )

    logger.info(
        "watcher_started",
        mode="two_threads",
        stream_thread="always_on",
        batch_thread="window_based",
    )

    try:
        stream_poller.start()
        batch_poller.start()
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("watcher_stopping", reason="KeyboardInterrupt")
        stop_event.set()
        stream_poller.join(timeout=5)
        batch_poller.join(timeout=5)
        totals = audit_trail.get_run_record_totals(run_id)
        file_totals = audit_trail.get_run_file_totals(run_id)
        audit_trail.complete_run(
            run_id,
            "stopped",
            file_totals["total_files"],
            file_totals["successful_files"],
            file_totals["failed_files"],
            totals["total_records"],
            totals["total_loaded"],
            totals["total_quarantined"],
            totals["total_orphaned"],
        )
        if manage_pool:
            close_pool()
