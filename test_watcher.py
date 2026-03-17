"""
tests/test_watcher.py
─────────────────────
Live watcher test — runs until Ctrl+C.

How to use:
    Terminal 1: python tests/test_watcher.py
    Terminal 2: python scripts/generate_stream_data.py --date 2026-03-17 --hour 10
"""

import os
import sys
import signal
import logging
import pipeline.watcher as watcher_module

from datetime import datetime
from pipeline.watcher import BatchPoller, StreamPoller

# ── Constants ─────────────────────────────────────────────────
BATCH_DIR  = "scripts/data/input/batch"
STREAM_DIR = "scripts/data/input/stream"

BATCH_POLL_INTERVAL  = 10   # seconds — 60 in production
STREAM_POLL_INTERVAL = 5    # seconds — 30 in production

# ── Override batch window so test works at any hour of day ────
watcher_module.BATCH_WINDOW_START = datetime.now().hour
watcher_module.BATCH_WINDOW_END   = datetime.now().hour + 8

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s | %(threadName)-12s | %(message)s",
    datefmt= "%H:%M:%S"
)


# ── Processor ─────────────────────────────────────────────────
class TestProcessor:
    """
    Receives detected files from both pollers.
    Prints each file and keeps a running count.
    In production this would be replaced by the real FileProcessor.
    """

    def __init__(self):
        self.batch_count  = 0
        self.stream_count = 0

    def process(self, filepath: str, file_type: str) -> None:
        filename    = os.path.basename(filepath)
        parent_dir  = os.path.basename(os.path.dirname(filepath))

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
    print()
    print("  Send files from Terminal 2:")
    print("  cd scripts")
    print("  python generate_stream_data.py --date 2026-03-17 --hour 10")
    print()
    print("  Press Ctrl+C to stop")
    print("=" * 55)


# ── Main ──────────────────────────────────────────────────────
def main():

    processor = TestProcessor()

    batch_poller = BatchPoller(
        batch_base_dir = BATCH_DIR,
        processor      = processor,
        poll_interval  = BATCH_POLL_INTERVAL
    )

    stream_poller = StreamPoller(
        stream_base_dir = STREAM_DIR,
        processor       = processor,
        poll_interval   = STREAM_POLL_INTERVAL
    )

    # Register shutdown 
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