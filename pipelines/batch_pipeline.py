"""
pipelines/batch_pipeline.py
Daily dimension snapshot load with automatic orphan reconciliation.

Responsibility: load dimension files for a given date, then trigger
                orphan reconciliation. Nothing about stream or watchers.

Reconciliation flow (fully automatic):
  1. Load dimensions (customers, drivers, restaurants, agents, static data)
  2. Trigger orphan reconciliation after dimension refresh
  3. Resolved orphans get new fact_orders versions
  4. Persistent orphans are retried then quarantined

Run via: python main.py batch --date YYYY-MM-DD
"""

from __future__ import annotations

import json
import os
from datetime import date
from typing import Any, Callable, Dict, Optional

import pandas as pd

from config.settings import Settings, get_settings
from handlers.backfill_handler import run_backfill_after_batch
from loaders.dim_agent_loader import DimAgentLoader
from loaders.dim_customer_loader import DimCustomerLoader
from loaders.dim_driver_loader import DimDriverLoader
from loaders.dim_restaurant_loader import DimRestaurantLoader
from loaders.dim_static_loader import StaticDimLoader
from quality import metrics_tracker as audit_trail
from utils.logger import configure_logging, get_logger_name
from warehouse.connection import close_pool, init_pool

logger = get_logger_name(__name__)


class BatchPipeline:
    """
    Loads all dimension files for a given date and triggers orphan reconciliation.

    Usage via Orchestrator (pool already owned by caller):
        BatchPipeline(batch_date, settings=settings, manage_pool=False).run()

    Standalone usage (manages its own pool):
        BatchPipeline(batch_date).run()
    """

    def __init__(
        self,
        batch_date: date,
        *,
        settings: Settings | None = None,
        manage_pool: bool = True,
    ):
        self.batch_date  = batch_date
        self.batch_dir: Optional[str] = None
        self.run_id: Optional[int]    = None
        self.settings    = settings or get_settings()
        self.manage_pool = manage_pool
        self.stats: Dict[str, int] = {
            "files_processed":   0,
            "files_successful":  0,
            "files_failed":      0,
            "records_loaded":    0,
            "records_total":     0,
            "records_quarantined": 0,
            "records_orphaned":  0,
        }

    # ── directory discovery ───────────────────────────────────────────────────

    def candidate_batch_directories(self) -> list[str]:
        """All locations we look for the batch folder (first match wins)."""
        d = str(self.batch_date)
        return [
            os.path.normpath(os.path.join(self.settings.batch_input_dir, d)),
            os.path.normpath(os.path.join("data/input/batch", d)),
            os.path.normpath(os.path.join("data_generators/data/input/batch", d)),
            os.path.normpath(os.path.join("scripts/data/input/batch", d)),
        ]

    def find_batch_directory(self) -> Optional[str]:
        for path in self.candidate_batch_directories():
            if os.path.isdir(path):
                return path
        return None

    # ── public entry point ────────────────────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        if self.manage_pool:
            init_pool(self.settings)

        self.batch_dir = self.find_batch_directory()
        audit_trail.ensure_audit_schema()
        self.run_id = audit_trail.start_run("batch")

        try:
            if not self.batch_dir:
                return self._run_reconciliation_only()
            return self._run_full_load()

        except Exception as e:
            logger.error("batch_pipeline_failed", error=str(e))
            audit_trail.complete_run(
                run_id=self.run_id,
                status="failed",
                total_files=self.stats["files_processed"],
                successful_files=self.stats["files_successful"],
                failed_files=self.stats["files_failed"],
                total_records=self.stats["records_total"],
                total_loaded=self.stats["records_loaded"],
                total_quarantined=self.stats["records_quarantined"],
                total_orphaned=self.stats["records_orphaned"],
                error_message=str(e),
            )
            return self.stats

        finally:
            if self.manage_pool:
                close_pool()

    # ── private: no batch directory found ────────────────────────────────────

    def _run_reconciliation_only(self) -> Dict[str, Any]:
        logger.warning(
            "batch_directory_missing",
            batch_date=str(self.batch_date),
            tried=self.candidate_batch_directories(),
        )
        print(
            f"\nNo batch folder for {self.batch_date}. Checked:\n  "
            + "\n  ".join(self.candidate_batch_directories())
            + f"\n\nGenerate data (from repo root):\n"
            f"  python data_generators/generate_batch_data.py --date {self.batch_date}\n"
            "\nRunning orphan reconciliation only (dimensions unchanged).\n"
        )
        rstats = run_backfill_after_batch(self.run_id)
        logger.info("batch_orphan_reconciliation", run_id=self.run_id, **rstats)
        audit_trail.complete_run(
            run_id=self.run_id,
            status="no_data",
            total_files=0,
            successful_files=0,
            failed_files=0,
            total_records=0,
            total_loaded=0,
            total_quarantined=0,
            total_orphaned=0,
            error_message="batch directory missing; reconciliation_only",
        )
        self._export_quarantine()
        return self.stats

    # ── private: full dimension load ──────────────────────────────────────────

    def _run_full_load(self) -> Dict[str, Any]:
        logger.info("batch_pipeline_started", batch_date=str(self.batch_date), batch_dir=self.batch_dir)

        self._load_static_files(StaticDimLoader(self.batch_dir))

        self._load_main_file(
            "customers.csv",
            lambda f: DimCustomerLoader(self.batch_dir).load(pd.read_csv(f), self.batch_date, f, self.run_id),
            "dim_customer",
        )
        self._load_main_file(
            "drivers.csv",
            lambda f: DimDriverLoader(self.batch_dir).load(pd.read_csv(f), self.batch_date, f, self.run_id),
            "dim_driver",
        )
        self._load_main_file(
            "restaurants.json",
            lambda f: DimRestaurantLoader(self.batch_dir).load(pd.DataFrame(json.load(open(f))), self.batch_date, f, self.run_id),
            "dim_restaurant",
        )
        self._load_main_file(
            "agents.csv",
            lambda f: DimAgentLoader(self.batch_dir).load(pd.read_csv(f), self.batch_date, f, self.run_id),
            "dim_agent",
        )

        self._track_reference_files()

        rstats = run_backfill_after_batch(self.run_id)
        logger.info("batch_orphan_reconciliation", run_id=self.run_id, **rstats)

        status = "success" if self.stats["files_failed"] == 0 else "partial"
        audit_trail.complete_run(
            run_id=self.run_id,
            status=status,
            total_files=self.stats["files_processed"],
            successful_files=self.stats["files_successful"],
            failed_files=self.stats["files_failed"],
            total_records=self.stats["records_total"],
            total_loaded=self.stats["records_loaded"],
            total_quarantined=self.stats["records_quarantined"],
            total_orphaned=self.stats["records_orphaned"],
        )
        self._export_quarantine()
        return self.stats

    # ── private: file-level helpers ───────────────────────────────────────────

    def _load_static_files(self, static_loader: StaticDimLoader) -> None:
        for filename, loader_func in [
            ("channels.csv",   static_loader.load_channels),
            ("priorities.csv", static_loader.load_priorities),
            ("reasons.csv",    static_loader.load_reasons),
        ]:
            file_path = os.path.join(self.batch_dir, filename)
            if not os.path.exists(file_path) or audit_trail.is_file_processed(file_path):
                continue

            audit_trail.register_file(self.run_id, file_path, "batch")
            self.stats["files_processed"] += 1
            try:
                count = loader_func()
                audit_trail.mark_file_success(file_path, count, count, 0)
                self.stats["files_successful"] += 1
                self.stats["records_loaded"]   += count
                self.stats["records_total"]    += count
            except Exception as e:
                logger.error("static_file_load_failed", file=file_path, error=str(e))
                audit_trail.mark_file_failed(file_path, str(e))
                self.stats["files_failed"] += 1

    def _load_main_file(self, filename: str, loader_func: Callable, table_name: str) -> None:
        file_path = os.path.join(self.batch_dir, filename)
        if not os.path.exists(file_path) or audit_trail.is_file_processed(file_path):
            return

        audit_trail.register_file(self.run_id, file_path, "batch")
        self.stats["files_processed"] += 1
        try:
            result         = loader_func(file_path)
            records_loaded = result.inserted + result.scd2_updated + result.scd1_updated

            audit_trail.mark_file_success(file_path, result.total_in, records_loaded, 0)
            audit_trail.write_quality_metrics(
                run_id=self.run_id,
                table_name=table_name,
                source_file=file_path,
                total_records=result.total_in,
                valid_records=records_loaded,
                quarantined_records=len(result.errors),
                orphaned_records=0,
                duplicate_count=0,
                null_violations=len(result.errors),
                processing_latency_sec=0.0,
                quality_details={
                    "inserted":         result.inserted,
                    "scd2_updated":     result.scd2_updated,
                    "scd1_updated":     result.scd1_updated,
                    "same_day_updated": result.same_day_updated,
                    "unchanged":        result.unchanged,
                },
            )
            self.stats["files_successful"]    += 1
            self.stats["records_loaded"]      += records_loaded
            self.stats["records_total"]       += result.total_in
            self.stats["records_quarantined"] += len(result.errors)
        except Exception as e:
            logger.error("main_file_load_failed", file=file_path, error=str(e))
            audit_trail.mark_file_failed(file_path, str(e))
            self.stats["files_failed"] += 1

    def _track_reference_files(self) -> None:
        for filename in ["regions.csv", "cities.json", "segments.csv", "categories.csv", "teams.csv", "reason_categories.csv"]:
            file_path = os.path.join(self.batch_dir, filename)
            if not os.path.exists(file_path) or audit_trail.is_file_processed(file_path):
                continue
            audit_trail.register_file(self.run_id, file_path, "reference")
            audit_trail.mark_file_success(file_path, 0, 0, 0)
            logger.debug("reference_file_tracked", file=filename)

    def _export_quarantine(self) -> None:
        qf = audit_trail.export_quarantine_to_file(self.run_id)
        if qf:
            logger.info("quarantine_exported_to_disk", path=qf)
            print(f"Quarantine records exported to: {qf}")


# ── direct execution ──────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Batch dimension pipeline")
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    args = parser.parse_args()

    configure_logging(log_dir="logs", level="INFO")
    stats = BatchPipeline(args.date, manage_pool=True).run()

    print(f"\n{'=' * 50}")
    print("BATCH PIPELINE COMPLETE")
    print(f"Date    : {args.date}")
    print(f"Files   : {stats['files_successful']}/{stats['files_processed']}")
    print(f"Records : {stats['records_loaded']}/{stats['records_total']}")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()