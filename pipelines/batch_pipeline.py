"""
pipelines/batch_pipeline.py
Production batch pipeline orchestrator with centralized file tracking.
"""

import os
import sys
import pandas as pd
import json
from datetime import date
from pathlib import Path
from typing import Dict, Callable, Any, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from warehouse.connection import init_pool, close_pool
from config.settings import get_settings
from utils.logger import configure_logging, get_logger_name
from quality import metrics_tracker as audit_trail

from loaders.dim_static_loader import StaticDimLoader
from loaders.dim_customer_loader import DimCustomerLoader
from loaders.dim_driver_loader import DimDriverLoader
from loaders.dim_restaurant_loader import DimRestaurantLoader
from loaders.dim_agent_loader import DimAgentLoader

logger = get_logger_name(__name__)


class BatchPipeline:
    def __init__(self, batch_date: date):
        self.batch_date = batch_date
        self.batch_dir = None
        self.run_id = None
        self.settings = get_settings()
        self.stats = {
            "files_processed": 0, "files_successful": 0, "files_failed": 0,
            "records_loaded": 0, "records_total": 0,
        }
    
    def find_batch_directory(self) -> Optional[str]:
        possible_paths = [
            f"data/input/batch/{self.batch_date}",
            f"data_generators/data/input/batch/{self.batch_date}",
            f"scripts/data/input/batch/{self.batch_date}",
        ]
        for path in possible_paths:
            if os.path.exists(path):
                return path
        return None
    
    def run(self) -> Dict[str, Any]:
        init_pool(self.settings)
        
        self.batch_dir = self.find_batch_directory()
        if not self.batch_dir:
            raise FileNotFoundError(f"Batch directory not found for {self.batch_date}")
        
        logger.info("batch_pipeline_started", batch_date=str(self.batch_date), batch_dir=self.batch_dir)
        
        audit_trail.ensure_audit_schema()
        self.run_id = audit_trail.start_run("batch")
        
        try:
            static_loader = StaticDimLoader(self.batch_dir)
            customer_loader = DimCustomerLoader(self.batch_dir)
            driver_loader = DimDriverLoader(self.batch_dir)
            restaurant_loader = DimRestaurantLoader(self.batch_dir)
            agent_loader = DimAgentLoader(self.batch_dir)
            
            self._load_static_files(static_loader)
            
            self._load_main_file("customers.csv", lambda f: customer_loader.load(pd.read_csv(f), self.batch_date, f, self.run_id), "dim_customer")
            self._load_main_file("drivers.csv", lambda f: driver_loader.load(pd.read_csv(f), self.batch_date, f, self.run_id), "dim_driver")
            self._load_main_file("restaurants.json", lambda f: restaurant_loader.load(pd.DataFrame(json.load(open(f))), self.batch_date, f, self.run_id), "dim_restaurant")
            self._load_main_file("agents.csv", lambda f: agent_loader.load(pd.read_csv(f), self.batch_date, f, self.run_id), "dim_agent")
            
            self._track_reference_files()
            
            status = "success" if self.stats["files_failed"] == 0 else "partial"
            audit_trail.complete_run(
                run_id=self.run_id, status=status,
                total_files=self.stats["files_processed"],
                successful_files=self.stats["files_successful"],
                failed_files=self.stats["files_failed"],
                total_records=self.stats["records_total"],
                total_loaded=self.stats["records_loaded"],
                total_quarantined=0, total_orphaned=0,
            )
            return self.stats
            
        except Exception as e:
            logger.error("batch_pipeline_failed", error=str(e))
            audit_trail.complete_run(
                run_id=self.run_id, status="failed",
                total_files=self.stats["files_processed"],
                successful_files=self.stats["files_successful"],
                failed_files=self.stats["files_failed"],
                total_records=self.stats["records_total"],
                total_loaded=self.stats["records_loaded"],
                total_quarantined=0, total_orphaned=0,
                error_message=str(e),
            )
            raise
        finally:
            close_pool()
    
    def _load_static_files(self, static_loader: StaticDimLoader):
        static_files = [
            ("channels.csv", static_loader.load_channels),
            ("priorities.csv", static_loader.load_priorities),
            ("reasons.csv", static_loader.load_reasons),
        ]
        
        for filename, loader_func in static_files:
            file_path = os.path.join(self.batch_dir, filename)
            if not os.path.exists(file_path):
                continue
            
            if audit_trail.is_file_processed(file_path):
                continue
            
            audit_trail.register_file(self.run_id, file_path, "batch")
            self.stats["files_processed"] += 1
            
            try:
                loaded_count = loader_func()
                audit_trail.mark_file_success(file_path, loaded_count, loaded_count, 0)
                self.stats["files_successful"] += 1
                self.stats["records_loaded"] += loaded_count
                self.stats["records_total"] += loaded_count
            except Exception as e:
                audit_trail.mark_file_failed(file_path, str(e))
                self.stats["files_failed"] += 1
                raise
    
    def _load_main_file(self, filename: str, loader_func: Callable, table_name: str):
        file_path = os.path.join(self.batch_dir, filename)
        if not os.path.exists(file_path):
            return
        
        if audit_trail.is_file_processed(file_path):
            return
        
        audit_trail.register_file(self.run_id, file_path, "batch")
        self.stats["files_processed"] += 1
        
        try:
            result = loader_func(file_path)
            records_loaded = result.inserted + result.scd2_updated + result.scd1_updated
            audit_trail.mark_file_success(file_path, result.total_in, records_loaded, 0)
            
            # Write quality metrics for this file
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
                    "inserted": result.inserted,
                    "scd2_updated": result.scd2_updated,
                    "scd1_updated": result.scd1_updated,
                    "same_day_updated": result.same_day_updated,
                    "unchanged": result.unchanged,
                }
            )
            
            self.stats["files_successful"] += 1
            self.stats["records_loaded"] += records_loaded
            self.stats["records_total"] += result.total_in
        except Exception as e:
            audit_trail.mark_file_failed(file_path, str(e))
            self.stats["files_failed"] += 1
            raise
    
    def _track_reference_files(self):
        reference_files = [
            "regions.csv", "cities.json", "segments.csv",
            "categories.csv", "teams.csv", "reason_categories.csv",
        ]
        
        for filename in reference_files:
            file_path = os.path.join(self.batch_dir, filename)
            if not os.path.exists(file_path):
                continue
            
            if audit_trail.is_file_processed(file_path):
                continue
            
            audit_trail.register_file(self.run_id, file_path, "reference")
            audit_trail.mark_file_success(file_path, 0, 0, 0)
            logger.debug("reference_file_tracked", file=filename)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=date.fromisoformat, required=True)
    args = parser.parse_args()
    
    configure_logging(log_dir="logs", level="INFO")
    pipeline = BatchPipeline(args.date)
    stats = pipeline.run()
    
    print(f"\n{'='*50}")
    print(f"BATCH PIPELINE COMPLETE")
    print(f"Date: {args.date}")
    print(f"Files: {stats['files_successful']}/{stats['files_processed']}")
    print(f"Records: {stats['records_loaded']}/{stats['records_total']}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()