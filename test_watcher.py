"""
test_watcher.py
───────────────
Live watcher test — runs until Ctrl+C.

How to use:
    Terminal 1 (project root): python test_watcher.py
    Terminal 2 (scripts/):      python generate_batch_data.py --date 2026-03-18
                                python generate_stream_data.py --date 2026-03-18 --hour 9

Logging:
    All structured JSON logs are written by watcher.py itself.
    This file only calls configure_logging() once to set up the log file.
    Logs are written to: logs/pipeline.log
"""

import os
import sys
import signal
import pipeline.watcher as watcher_module

from datetime import datetime
from pipeline.watcher import BatchPoller, StreamPoller
from pipeline.logging.logger import configure_logging
import pipeline.alerting as alerting

# ── Constants ─────────────────────────────────────────────────
BATCH_DIR  = "scripts/data/input/batch"
STREAM_DIR = "scripts/data/input/stream"

BATCH_POLL_INTERVAL  = 10   # seconds — 60 in production
STREAM_POLL_INTERVAL = 5    # seconds — 30 in production

# ── Override batch window so test works at any hour of day ────
watcher_module.BATCH_WINDOW_START = datetime.now().hour
watcher_module.BATCH_WINDOW_END = min(datetime.now().hour + 8, 23)

# ── Logging setup ─────────────────────────────────────────────
# Called once here — watcher.py handles all actual log calls
configure_logging(log_dir="logs", level="INFO")


# ── Processor ─────────────────────────────────────────────────
class TestProcessor:
    """
    Receives detected files from both pollers.
    Prints each file to terminal with a running counter.
    In production this would be replaced by the real FileProcessor.

    Note: No logging here — watcher.py already logs every file detection
    via log_stage_start(). This class only prints to terminal for
    live visibility during testing.
    """

    def __init__(self):
        self.batch_count  = 0
        self.stream_count = 0

    def process(self, filepath: str, file_type: str) -> None:
        filename   = os.path.basename(filepath)
        parent_dir = os.path.basename(os.path.dirname(filepath))

        if file_type == "batch":
            self.batch_count += 1
            print(f"\n  [BATCH  {self.batch_count:02d}] {parent_dir}/{filename}")

        elif file_type == "stream":
            self.stream_count += 1
            print(f"\n  [STREAM {self.stream_count:02d}] {parent_dir}/{filename}")


# ── Shutdown ──────────────────────────────────────────────────
def build_shutdown_handler(batch_poller, stream_poller, processor):
    """
    Returns a shutdown function that stops both pollers
    and prints a summary before exiting.
    Registered as Ctrl+C handler before join() is called.
    """
    def shutdown(sig, frame):
        print("\n\n" + "=" * 55)
        print("  Stopping watchers...")
        batch_poller.stop()
        stream_poller.stop()
        print(f"  Batch files detected:  {processor.batch_count}")
        print(f"  Stream files detected: {processor.stream_count}")
        print(f"  Logs written to:       logs/pipeline.log")
        print("=" * 55)
        sys.exit(0)

    return shutdown


# ── Banner ────────────────────────────────────────────────────
def print_banner():
    print("=" * 55)
    print("  FastFeast — Live Watcher Test")
    print("=" * 55)
    print(f"  Batch dir:    {BATCH_DIR}")
    print(f"  Stream dir:   {STREAM_DIR}")
    print(f"  Batch window: {watcher_module.BATCH_WINDOW_START}:00 — {watcher_module.BATCH_WINDOW_END}:00")
    print(f"  Batch poll:   every {BATCH_POLL_INTERVAL}s  (production: 60s)")
    print(f"  Stream poll:  every {STREAM_POLL_INTERVAL}s  (production: 30s)")
    print(f"  Logs:         logs/pipeline.log")
    print()
    print("  Drop files from Terminal 2:")
    print("  cd scripts")
    print("  python generate_batch_data.py --date 2026-03-18")
    print("  python generate_stream_data.py --date 2026-03-18 --hour 9")
    print()
    print("  Press Ctrl+C to stop")
    print("=" * 55)


# ── Main ──────────────────────────────────────────────────────
def main():

    processor = TestProcessor()

    batch_poller = BatchPoller(
        batch_base_dir = BATCH_DIR,
        processor      = processor,
        alerter        = alerting,
        poll_interval  = BATCH_POLL_INTERVAL
    )

    stream_poller = StreamPoller(
        stream_base_dir = STREAM_DIR,
        processor       = processor,
        poll_interval   = STREAM_POLL_INTERVAL
    )

    # Register Ctrl+C shutdown handler
    shutdown = build_shutdown_handler(batch_poller, stream_poller, processor)
    signal.signal(signal.SIGINT, shutdown)

    print_banner()

    batch_thread  = batch_poller.start()
    stream_thread = stream_poller.start()

    # Keep main thread alive — join(timeout=1) lets Ctrl+C fire on Windows
    try:
        while True:
            batch_thread.join(timeout=1)
            stream_thread.join(timeout=1)
    except KeyboardInterrupt:
        shutdown(None, None)


if __name__ == "__main__":
    main()