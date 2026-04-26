"""
pipelines/stream_pipeline.py
Stream fact loads.

Modes:
  process_single_hour(date, hour)  — one-off load for a single hour folder
  process_pending_stream_files()   — poll-friendly scanner for new stream files
  RealStreamProcessor              — routes each file to fact loaders

Run via: python main.py stream --date YYYY-MM-DD --hour H
"""

from __future__ import annotations

import os
from datetime import date

from config.settings import Settings, get_settings
from loaders import fact_events_loader, fact_orders_loader, fact_tickets_loader
from quality import metrics_tracker as audit_trail
from utils.file_utils import is_file_stable
from utils.logger import configure_logging, get_logger_name, log_stage_start
from warehouse.connection import close_pool, init_pool

logger = get_logger_name(__name__)

STREAM_FILES = [
    "orders.json",
    "tickets.csv",
    "ticket_events.json",
]


class RealStreamProcessor:
    """Routes a stream file to the correct fact loader."""

    def __init__(self, run_id: int):
        self.run_id = run_id
        self.processed_count = 0

    def process(self, filepath: str) -> None:
        filename = os.path.basename(filepath)

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


def process_pending_stream_files(
    *,
    stream_base_dir: str,
    processor: RealStreamProcessor,
    target_date: date | None = None,
) -> int:
    """
    Process all currently available and stable stream files for one date.

    Returns:
        Number of files successfully processed in this scan.
    """
    process_date = target_date or date.today()
    stream_dir = os.path.join(stream_base_dir, process_date.isoformat())
    processed_before = processor.processed_count

    if not os.path.exists(stream_dir):
        return 0

    try:
        hour_folders = sorted(
            f for f in os.listdir(stream_dir) if os.path.isdir(os.path.join(stream_dir, f))
        )
    except OSError as e:
        logger.error("stream_dir_read_failed", dir=stream_dir, error=str(e))
        return 0

    for hour_folder in hour_folders:
        hour_path = os.path.join(stream_dir, hour_folder)
        for filename in STREAM_FILES:
            filepath = os.path.join(hour_path, filename)
            if not os.path.exists(filepath):
                continue
            if audit_trail.is_file_processed(filepath):
                continue
            if not is_file_stable(filepath):
                logger.debug("stream_file_still_writing", file=filepath, hour=hour_folder)
                continue
            log_stage_start(
                logger,
                stage_name="file_detection",
                file=filepath,
                poller="stream",
                hour=hour_folder,
            )
            processor.process(filepath)

    return processor.processed_count - processed_before


def process_single_hour(
    target_date: date,
    target_hour: int,
    *,
    settings: Settings | None = None,
    manage_pool: bool = True,
) -> None:
    """
    Load all stream files under stream_input_dir/<date>/<HH>/.

    Args:
        manage_pool: When False, the caller already owns the DB pool (e.g. main.py).
    """
    settings = settings or get_settings()
    if manage_pool:
        init_pool(settings)

    audit_trail.ensure_audit_schema()
    run_id = audit_trail.start_run("stream")
    processor = RealStreamProcessor(run_id=run_id)

    stream_dir = os.path.join(
        settings.stream_input_dir,
        target_date.isoformat(),
        f"{target_hour:02d}",
    )

    if not os.path.exists(stream_dir):
        logger.error("stream_dir_missing", dir=stream_dir)
        audit_trail.complete_run(
            run_id,
            "failed",
            0,
            0,
            0,
            0,
            0,
            0,
            0,
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
            processor.process(filepath)

    totals = audit_trail.get_run_record_totals(run_id)
    file_totals = audit_trail.get_run_file_totals(run_id)
    audit_trail.complete_run(
        run_id,
        "success",
        file_totals["total_files"],
        file_totals["successful_files"],
        file_totals["failed_files"],
        totals["total_records"],
        totals["total_loaded"],
        totals["total_quarantined"],
        totals["total_orphaned"],
    )
    logger.info(
        "stream_hour_complete",
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

    settings = get_settings()
    configure_logging(log_dir=settings.log_dir, level=settings.log_level)
    process_single_hour(args.date, args.hour, manage_pool=True)
