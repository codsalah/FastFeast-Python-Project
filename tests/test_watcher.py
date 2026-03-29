"""
test_watcher.py
Live watcher test that verifies file detection, registration, and tracking.
Run with Ctrl+C to stop.
"""

import os
import sys
import signal
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import pipelines.stream_pipeline as watcher_module
from pipelines.stream_pipeline import BatchPoller, StreamPoller
from warehouse.connection import init_pool
from config.settings import get_settings
from utils.logger import configure_logging
from quality import metrics_tracker as audit_trail
import alerting.alert_service as alerting
BATCH_DIR = "scripts/data/input/batch"
STREAM_DIR = "scripts/data/input/stream"
BATCH_POLL_INTERVAL = 10
STREAM_POLL_INTERVAL = 5

# Override batch window for testing
watcher_module.BATCH_WINDOW_START = datetime.now().hour
watcher_module.BATCH_WINDOW_END = min(datetime.now().hour + 8, 23)

#watcher_module.BATCH_WINDOW_END = datetime.now().hour # to cheack alerting for missing batch files

configure_logging(log_dir="logs", level="INFO")


class TestProcessor:
    def __init__(self, run_id: int):
        self.run_id = run_id
        self.batch_count = 0
        self.stream_count = 0

    def process(self, filepath: str, file_type: str) -> None:
        filename = os.path.basename(filepath)
        folder = os.path.basename(os.path.dirname(filepath))

        audit_trail.register_file(self.run_id, filepath, file_type)

        if file_type == "batch":
            self.batch_count += 1
            print(f"[BATCH {self.batch_count:02d}] {folder}/{filename}")
        else:
            self.stream_count += 1
            print(f"[STREAM {self.stream_count:02d}] {folder}/{filename}")

        audit_trail.mark_file_success(filepath, records_total=1, records_loaded=1, records_quarantined=0)


def shutdown_handler(batch_poller, stream_poller, processor, run_id):
    def shutdown(sig, frame):
        print("\n\nStopping watchers...")
        batch_poller.stop()
        stream_poller.stop()

        audit_trail.complete_run(
            run_id=run_id,
            status="success",
            total_files=processor.batch_count + processor.stream_count,
            successful_files=processor.batch_count + processor.stream_count,
            failed_files=0,
            total_records=processor.batch_count + processor.stream_count,
            total_loaded=processor.batch_count + processor.stream_count,
            total_quarantined=0,
            total_orphaned=0,
        )

        print(f"\nBatch files: {processor.batch_count}")
        print(f"Stream files: {processor.stream_count}")
        print(f"Run ID: {run_id}")
        print("\nLogs: logs/pipeline.log")
        sys.exit(0)
    return shutdown


def main():
    settings = get_settings()
    init_pool(settings)
    audit_trail.ensure_warehouse_schema()

    run_id = audit_trail.start_run("watcher_test")
    processor = TestProcessor(run_id=run_id)

    batch_poller = BatchPoller(
        batch_base_dir=BATCH_DIR,
        processor=processor,
        alerter=alerting,
        poll_interval=BATCH_POLL_INTERVAL
    )

    stream_poller = StreamPoller(
        stream_base_dir=STREAM_DIR,
        processor=processor,
        poll_interval=STREAM_POLL_INTERVAL
    )

    shutdown = shutdown_handler(batch_poller, stream_poller, processor, run_id)
    signal.signal(signal.SIGINT, shutdown)

    print(f"Batch dir: {BATCH_DIR}")
    print(f"Stream dir: {STREAM_DIR}")
    print("\nGenerate test data in another terminal:")
    print("  cd scripts")
    print("  python generate_batch_data.py --date 2026-03-27")
    print("  python generate_stream_data.py --date 2026-03-27 --hour 9")
    print("\nPress Ctrl+C to stop\n")

    batch_poller.start()
    stream_poller.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        shutdown(None, None)


if __name__ == "__main__":
    main()