"""
pipelines/watcher.py
Continuous watcher: composes BatchPoller + StreamPoller into one process.

Responsibility: this is the ONLY file that imports from both batch_watcher
                and stream_watcher. It wires them together with a shared
                processor and runs until Ctrl+C.

Run via: python main.py stream --watch
"""

from __future__ import annotations

import time

from alerting.alert_service import AlertService
from config.settings import Settings, get_settings
from pipelines.batch_watcher import BatchPoller
from pipelines.stream_pipeline import RealStreamProcessor
from pipelines.stream_watcher import StreamPoller
from quality import metrics_tracker as audit_trail
from utils.logger import get_logger_name
from warehouse.connection import close_pool, init_pool

logger = get_logger_name(__name__)


def run_watcher(
    *,
    settings: Settings | None = None,
    manage_pool: bool = True,
) -> None:
    """
    Start BatchPoller + StreamPoller in background threads and block until Ctrl+C.

    Args:
        settings:    Falls back to get_settings() if None.
        manage_pool: When False the caller (Orchestrator) already owns the pool.
    """
    settings = settings or get_settings()
    if manage_pool:
        init_pool(settings)

    audit_trail.ensure_audit_schema()
    run_id    = audit_trail.start_run("watcher")
    processor = RealStreamProcessor(run_id=run_id)
    alerter   = AlertService()
    poll      = settings.poll_interval_seconds

    batch_poller  = BatchPoller(
        settings.batch_input_dir,
        alerter=alerter,
        poll_interval=max(poll, 30),
    )
    stream_poller = StreamPoller(
        settings.stream_input_dir,
        processor,
        alerter=alerter,
        poll_interval=max(poll // 2, 10),
    )

    batch_poller.start()
    stream_poller.start()
    logger.info("watcher_running", hint="Press Ctrl+C to stop")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("watcher_stopping", reason="KeyboardInterrupt")
        batch_poller.stop()
        stream_poller.stop()
        audit_trail.complete_run(
            run_id,
            "stopped",
            processor.processed_count,
            processor.processed_count,
            0, 0, 0, 0, 0,
        )
        if manage_pool:
            close_pool()