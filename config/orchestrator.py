"""
config/orchestrator.py
Composition root: owns logging, DB pool, DDL, and pipeline entrypoints.

Responsibility: infrastructure wiring only. Every method here follows the
same pattern — startup → delegate to the right pipeline → shutdown.
The pipelines themselves know nothing about the pool or DDL.

Commands routed here from main.py:
  init-db                           → init_db()
  batch  --date YYYY-MM-DD          → run_batch()
  stream --date YYYY-MM-DD --hour H → run_stream_hour()
  stream --watch                    → run_stream_watch()
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from config.schema_manager import SchemaManager
from config.settings import Settings
from utils.logger import configure_logging, get_logger_name
from warehouse.connection import close_pool, health_check, init_pool

logger = get_logger_name(__name__)


@dataclass
class Orchestrator:
    settings: Settings

    # ── infrastructure lifecycle ──────────────────────────────────────────────

    def _startup(self) -> None:
        configure_logging(log_dir=self.settings.log_dir, level="INFO")
        self.settings.ensure_directories()
        init_pool(self.settings)
        if not health_check():
            raise RuntimeError("Database health check failed.")
        SchemaManager(self.settings).ensure_all(with_seed=False)

    def _shutdown(self) -> None:
        close_pool()

    # ── public entrypoints ────────────────────────────────────────────────────

    def init_db(self, *, with_seed: bool = False) -> None:
        """Apply DDL, populate dim_date, optionally seed unknown-member sentinels."""
        configure_logging(log_dir=self.settings.log_dir, level="INFO")
        self.settings.ensure_directories()
        init_pool(self.settings)
        try:
            if not health_check():
                raise RuntimeError("Database health check failed.")
            SchemaManager(self.settings).ensure_all(with_seed=with_seed)
            logger.info("init_db_complete", with_seed=with_seed)
        finally:
            close_pool()

    def run_batch(self, batch_date: date) -> None:
        """Load daily dimension snapshots; orphan reconciliation runs automatically."""
        from pipelines.batch_pipeline import BatchPipeline

        self._startup()
        try:
            BatchPipeline(batch_date, settings=self.settings, manage_pool=False).run()
        finally:
            self._shutdown()

    def run_stream_hour(self, d: date, hour: int) -> None:
        """One-off load of all stream facts under stream_input_dir/<d>/<HH>/."""
        from pipelines.stream_pipeline import process_single_hour

        self._startup()
        try:
            process_single_hour(d, hour, settings=self.settings, manage_pool=False)
        finally:
            self._shutdown()

    def run_stream_watch(self) -> None:
        """Start BatchPoller + StreamPoller; blocks until Ctrl+C."""
        from pipelines.watcher import run_watcher

        self._startup()
        try:
            run_watcher(settings=self.settings, manage_pool=False)
        finally:
            self._shutdown()