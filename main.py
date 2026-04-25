#!/usr/bin/env python3
"""
FastFeast — single CLI entrypoint.

Flow:
  config (.env) → config.runtime (pool + inline DDL bootstrap) → pipelines

Commands:
  init-db                              Apply DDL, dim_date, optional seed
  batch   --date YYYY-MM-DD            Daily dimension load + orphan reconciliation
  stream  --date YYYY-MM-DD --hour H   One-off stream fact load for one hour
  stream  --watch                      Continuous watcher (batch + stream pollers)
  analytics setup                      Create analytics views (OLAP layer)
  analytics dashboard                  Launch Streamlit dashboard
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

from warehouse.connection import get_conn

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from analytics import ensure_analytics_schema
from config.settings import Settings, get_settings
from loaders.dim_date_loader import load_dim_date
from pipelines.batch_pipeline import BatchPipeline
from pipelines.stream_pipeline import process_single_hour
from pipelines.watcher import run_continuous_watch
from utils.logger import configure_logging, get_logger_name
from warehouse.connection import close_pool, health_check, init_pool

logger = get_logger_name(__name__)


def _apply_sql_file(path: Path) -> None:
    """Execute SQL file directly on database."""
    if not path.exists():
        logger.warning("ddl_missing", path=str(path))
        return
    ddl = path.read_text(encoding="utf-8")
    with get_conn() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(ddl)
    logger.info("schema_applied", name=path.stem, path=str(path))


def apply_audit_ddl(settings: Settings) -> None:
    """Apply audit schema DDL."""
    _apply_sql_file(Path("warehouse/audit_ddl.sql"))


def ensure_warehouse_schema(*, with_seed: bool = False) -> None:
    """Apply warehouse DDL and optionally seed data."""
    _apply_sql_file(Path("warehouse/dwh_ddl.sql"))
    if with_seed:
        _apply_sql_file(Path("warehouse/seed.sql"))


def init_database(settings: Settings, *, with_seed: bool = False) -> None:
    """Apply audit + warehouse DDL, optional seed, dim_date; then close the pool."""
    configure_logging(log_dir=settings.log_dir, level=settings.log_level)
    settings.ensure_directories()
    init_pool(settings)
    try:
        apply_audit_ddl(settings)
        ensure_warehouse_schema(with_seed=with_seed)
        load_dim_date(
            start_year=settings.date_dim_start_year,
            end_year=settings.date_dim_end_year,
        )
    finally:
        close_pool()


def pipeline_startup(settings: Settings) -> None:
    """Logging, directories, pool, health check, base schema (no seed)."""
    configure_logging(log_dir=settings.log_dir, level=settings.log_level)
    settings.ensure_directories()
    init_pool(settings)
    if not health_check():
        raise RuntimeError("Database health check failed")
    ensure_warehouse_schema(with_seed=False)


def pipeline_shutdown() -> None:
    close_pool()


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

    p_analytics = sub.add_parser("analytics", help="Analytics setup and dashboard")
    p_analytics.add_argument(
        "action", 
        choices=["setup", "dashboard"],
        help="setup: create analytics views | dashboard: launch Streamlit"
    )

    return p


def main() -> int:
    args     = build_parser().parse_args()
    settings = get_settings()

    if args.command == "init-db":
        init_database(settings, with_seed=args.with_seed)
        return 0

    if args.command == "batch":
        pipeline_startup(settings)
        try:
            BatchPipeline(args.date, settings=settings, manage_pool=False).run()
        finally:
            pipeline_shutdown()
        return 0

    if args.command == "stream":
        if args.watch or (args.date is None and args.hour is None):
            pipeline_startup(settings)
            try:
                run_continuous_watch(settings=settings, manage_pool=False)
            finally:
                pipeline_shutdown()
            return 0
        if args.date is None or args.hour is None:
            print("Error: provide both --date and --hour, or use --watch.")
            return 2
        if not (0 <= args.hour <= 23):
            print("Error: --hour must be 0-23.")
            return 2
        pipeline_startup(settings)
        try:
            process_single_hour(args.date, args.hour, settings=settings, manage_pool=False)
        finally:
            pipeline_shutdown()
        return 0

    if args.command == "analytics":
        if args.action == "setup":
            pipeline_startup(settings)
            try:
                ensure_analytics_schema()
                logger.info("analytics_schema_ready")
            finally:
                pipeline_shutdown()
            print("Analytics views created successfully.")
            return 0
        if args.action == "dashboard":
            import subprocess
            import os
            dashboard_path = _ROOT / "analytics" / "dashboard.py"
            print(f"Launching dashboard at {dashboard_path}...")
            subprocess.run([sys.executable, "-m", "streamlit", "run", str(dashboard_path)])
            return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())