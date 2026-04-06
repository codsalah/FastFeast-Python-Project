#!/usr/bin/env python3
"""
FastFeast — single CLI entrypoint.

Flow:
  config (.env) → Orchestrator → DB pool + DDL → pipeline

Commands:
  init-db                              Apply DDL, dim_date, optional seed
  batch   --date YYYY-MM-DD            Daily dimension load + orphan reconciliation
  stream  --date YYYY-MM-DD --hour H   One-off stream fact load for one hour
  stream  --watch                      Continuous watcher (batch + stream pollers)
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from config.orchestrator import Orchestrator
from config.settings import get_settings


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="fastfeast",
        description="FastFeast data pipelines",
        epilog=(
            "Run batch after dimension files are ready — "
            "orphan reconciliation runs automatically inside batch."
        ),
    )
    sub = p.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init-db", help="Apply DDL, dim_date, optional -1 seed rows")
    p_init.add_argument("--with-seed", action="store_true", help="Insert warehouse/seed.sql")

    p_batch = sub.add_parser("batch", help="Daily dimension load for a calendar date")
    p_batch.add_argument("--date", type=date.fromisoformat, required=True)

    p_stream = sub.add_parser("stream", help="Stream facts: one hour or continuous watch")
    p_stream.add_argument("--date", type=date.fromisoformat, default=None)
    p_stream.add_argument("--hour", type=int, default=None, help="Hour folder 0-23")
    p_stream.add_argument("--watch", action="store_true", help="Run continuous watcher until Ctrl+C")

    return p


def main() -> int:
    args     = build_parser().parse_args()
    settings = get_settings()
    orch     = Orchestrator(settings)

    if args.command == "init-db":
        orch.init_db(with_seed=args.with_seed)
        return 0

    if args.command == "batch":
        orch.run_batch(batch_date=args.date)
        return 0

    if args.command == "stream":
        if args.watch or (args.date is None and args.hour is None):
            orch.run_stream_watch()
            return 0
        if args.date is None or args.hour is None:
            print("Error: provide both --date and --hour, or use --watch.")
            return 2
        if not (0 <= args.hour <= 23):
            print("Error: --hour must be 0-23.")
            return 2
        orch.run_stream_hour(args.date, args.hour)
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())