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

SIMULATE_DAY_SCRIPT = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "data_generators", "simulate_day.py"
)


def invoke_simulate_day(batch_date: date, verbose: bool = False) -> bool:
    """Run simulate_day.py for the given date with --skip-master."""
    date_str = batch_date.strftime("%Y-%m-%d")
    cmd = [sys.executable, SIMULATE_DAY_SCRIPT, "--date", date_str, "--skip-master"]
    if verbose:
        cmd.append("--verbose")

    print(f"\n>>> Invoking simulate_day.py for {date_str} <<<")
    result = subprocess.run(cmd, text=True)
    if result.returncode != 0:
        print(f"[ERROR] simulate_day.py failed for {date_str}")
        return False
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

            for loader in scd2_loaders:
                entity = loader.table_name.split(".")[-1].replace("dim_", "")

                # restaurants are saved as .json by the batch generator; everything else is .csv
                csv_path  = f"data/input/batch/{date_str}/{entity}s.csv"
                json_path = f"data/input/batch/{date_str}/{entity}s.json"

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