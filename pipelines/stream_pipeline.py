"""
pipelines/stream_pipeline.py
Routes micro-batch stream files to the correct fact loaders.

Responsibility: load stream fact files (orders, tickets, events).
                Nothing about batch dimensions or watcher threads.

Modes:
  process_single_hour(date, hour)  — one-off load for a single hour folder
  RealStreamProcessor              — reusable processor handed to pollers by watcher.py

Run via: python main.py stream --date YYYY-MM-DD --hour H
"""

from __future__ import annotations

import os
from datetime import date

from config.settings import Settings, get_settings
from loaders import fact_events_loader, fact_orders_loader, fact_tickets_loader
from pipelines.stream_watcher import STREAM_FILES
from quality import metrics_tracker as audit_trail
from utils.logger import configure_logging, get_logger_name
from warehouse.connection import close_pool, init_pool

logger = get_logger_name(__name__)


class RealStreamProcessor:
    """
    Routes a stream file to the correct fact loader.

    This class is intentionally unaware of batch files or watcher threads.
    It is instantiated by watcher.py (for continuous mode) and by
    process_single_hour() (for one-off mode).
    """

    def __init__(self, run_id: int):
        self.run_id          = run_id
        self.processed_count = 0

    def process(self, filepath: str, file_type: str = "stream") -> None:
        filename = os.path.basename(filepath)

        # Batch files are not this processor's concern
        if file_type == "batch":
            logger.warning(
                "batch_file_detected_in_stream_processor",
                file=filename,
                hint="Run: python main.py batch --date YYYY-MM-DD",
            )
            return

        if audit_trail.is_file_processed(filepath):
            logger.info("file_skipped", file=filename, reason="already_processed")
            return

        try:
            lower = filename.lower()
            if "orders" in lower:
                fact_orders_loader.load(filepath, self.run_id)
            elif "tickets" in lower and "events" not in lower:
                fact_tickets_loader.load(filepath, self.run_id)
            elif "event" in lower:
                fact_events_loader.load(filepath, self.run_id)
            else:
                logger.warning("stream_unknown_file", file=filename)
                return
            self.processed_count += 1
        except Exception as e:
            logger.error("stream_processor_failed", file=filename, error=str(e))


def process_single_hour(
    target_date: date,
    target_hour: int,
    *,
    settings: Settings | None = None,
    manage_pool: bool = True,
) -> None:
    """
    Load all stream files under stream_input_dir/<date>/<HH>/ in one shot.

    Args:
        target_date: Date folder to process.
        target_hour: Hour folder to process (0-23).
        settings:    Falls back to get_settings() if None.
        manage_pool: When False the caller (Orchestrator) already owns the pool.
    """
    settings = settings or get_settings()
    if manage_pool:
        init_pool(settings)

    audit_trail.ensure_audit_schema()
    run_id    = audit_trail.start_run("stream_one_off")
    processor = RealStreamProcessor(run_id=run_id)

    stream_dir = os.path.join(
        settings.stream_input_dir,
        target_date.isoformat(),
        f"{target_hour:02d}",
    )

    if not os.path.exists(stream_dir):
        logger.error("stream_dir_missing", dir=stream_dir)
        audit_trail.complete_run(
            run_id, "failed", 0, 0, 0, 0, 0, 0, 0,
            error_message="stream dir not found",
        )
        if manage_pool:
            close_pool()
        return

    files_found = 0
    for filename in STREAM_FILES:
        filepath = os.path.join(stream_dir, filename)
        if os.path.exists(filepath):
            files_found += 1
            processor.process(filepath, "stream")

    totals = audit_trail.get_run_record_totals(run_id)
    audit_trail.complete_run(
        run_id,
        "success",
        files_found,
        processor.processed_count,
        max(0, files_found - processor.processed_count),
        totals["total_records"],
        totals["total_loaded"],
        totals["total_quarantined"],
        totals["total_orphaned"],
    )
    logger.info(
        "stream_one_off_complete",
        date=str(target_date),
        hour=target_hour,
        files_found=files_found,
        files_processed=processor.processed_count,
    )
    if manage_pool:
        close_pool()


# ── direct execution ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Stream pipeline — one-off hour load")
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    parser.add_argument("--hour", type=int, required=True, help="Hour folder 0-23")
    args = parser.parse_args()

    configure_logging(log_dir="logs", level="INFO")
    process_single_hour(args.date, args.hour, manage_pool=True)