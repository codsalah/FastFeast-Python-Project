import os
import sys
import argparse
import subprocess
import pandas as pd
from datetime import date, timedelta
from warehouse.connection import init_pool, close_pool
from config.settings import get_settings
from loaders.dim_customer_loader import DimCustomerLoader
from loaders.dim_agent_loader import DimAgentLoader
from loaders.dim_driver_loader import DimDriverLoader
from loaders.dim_restaurant_loader import DimRestaurantLoader
from loaders.dim_static_loader import load_static_dimensions
from utils.logger import get_logger_name
from utils.logger import configure_logging

logger = get_logger_name(__name__)
configure_logging()

SIMULATE_DAY_SCRIPT = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "data_generators", "simulate_day.py"
)

# Possible batch locations (STICK TO ONE PATH)
# POSSIBLE_BATCH_PATHS = [
#     "data/input/batch",
#     "data_generators/data/input/batch",
#     "scripts/data/input/batch",
# ]


BASE_BATCH_PATH = "data/input/batch"


def find_batch_dir(batch_date: str) -> str:
    batch_dir = os.path.join(BASE_BATCH_PATH, batch_date)

    if os.path.exists(batch_dir):
        return batch_dir

    logger.error(   
        "batch_dir_not_found",
        batch_date=batch_date,
        attempted_path=batch_dir,
    )

    return None


def invoke_simulate_day(batch_date: date, verbose: bool = False) -> bool:
    date_str = batch_date.strftime("%Y-%m-%d")
    cmd = [sys.executable, SIMULATE_DAY_SCRIPT, "--date", date_str, "--skip-master"]

    if verbose:
        cmd.append("--verbose")
        
    logger.info("invoke_simulate_day", date=date_str, cmd=cmd)
    result = subprocess.run(cmd, text=True)

    if result.returncode != 0:
        logger.error(
            "simulate_day_failed",
            date=date_str,
            return_code=result.returncode,
        )
        return False
    logger.info("simulate_day_success", date=date_str)
    return True

def verify_all_scd2(start_date: date, num_days: int, invoke: bool = False, verbose: bool = False):
    init_pool(get_settings())

    batch_dates = [start_date + timedelta(days=i) for i in range(num_days)]

    scd2_loaders = [
        DimCustomerLoader(),
        DimAgentLoader(),
        DimDriverLoader(),
        DimRestaurantLoader(),
    ]

    try:
        for b_date in batch_dates:
            date_str = b_date.strftime("%Y-%m-%d")
            print(f"\n{'='*20} BATCH: {date_str} {'='*20}")

            # Optionally generate data first
            if invoke:
                ok = invoke_simulate_day(b_date, verbose=verbose)
                if not ok:
                    print(f"[WARN] Skipping loader run for {date_str} due to simulation failure.")
                    continue

            # FIND THE BATCH DIRECTORY
            batch_dir = find_batch_dir(date_str)
            if not batch_dir:
                print(f"[ERROR] Batch directory not found for {date_str}")
                print("Searched in:")
                for base_path in POSSIBLE_BATCH_PATHS:
                    print(f"  - {os.path.join(base_path, date_str)}")
                continue

            print(f"Using batch directory: {batch_dir}")

            # LOAD STATIC DIMENSIONS
            print("\n[STATIC DIMENSIONS]")
            static_results = load_static_dimensions(batch_dir, run_id=0)  # run_id ignored
            for table, count in static_results.items():
                print(f"  {table}: {count} records loaded")

            # LOAD SCD2 DIMENSIONS
            print("\n[SCD2 DIMENSIONS]")
            for loader in scd2_loaders:
                entity = loader.table_name.split(".")[-1].replace("dim_", "")

                # restaurants are saved as .json by the batch generator; everything else is .csv
                csv_path  = os.path.join(batch_dir, f"{entity}s.csv")
                json_path = os.path.join(batch_dir, f"{entity}s.json")

                if os.path.exists(csv_path):
                    fpath = csv_path
                    df = pd.read_csv(fpath)
                elif os.path.exists(json_path):
                    import json as _json
                    fpath = json_path
                    with open(fpath) as f:
                        df = pd.DataFrame(_json.load(f))
                else:
                    print(f"[-] Skip: no batch file found for '{entity}s' on {date_str}")
                    continue

                print(f"[*] Processing {loader.__class__.__name__} ({os.path.basename(fpath)})...")
                res = loader.load(df, b_date, fpath)
                print(
                    f"    -> Total: {res.total_in} | Inserted: {res.inserted} "
                    f"| SCD2: {res.scd2_updated} | SCD1: {res.scd1_updated} "
                    f"| Unchanged: {res.unchanged}"
                )

    finally:
        close_pool()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify SCD2 loaders over a date range in the warehouse.")
    parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        default=date(2026, 4, 1),
        help="Start date in YYYY-MM-DD format (default: 2026-04-01)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=10,
        help="Number of days to run (default: 10)",
    )
    parser.add_argument(
        "--invoke",
        action="store_true",
        default=False,
        help="If set, invoke simulate_day.py (with --skip-master) for each date before loading.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Pass --verbose to simulate_day.py when --invoke is set.",
    )

    args = parser.parse_args()
    verify_all_scd2(
        start_date=args.start_date,
        num_days=args.days,
        invoke=args.invoke,
        verbose=args.verbose,
    )

"""
# Just load (data must already exist)
python3 -m loaders.verify_loaders --start-date 2026-04-01 --days 7

# Simulate + load for each day
python3 -m loaders.verify_loaders --start-date 2026-04-01 --days 7 --invoke

# Simulate + load with verbose simulation output
python3 -m loaders.verify_loaders --start-date 2026-04-01 --days 7 --invoke --verbose
"""
