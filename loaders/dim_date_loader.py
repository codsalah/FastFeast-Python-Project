"""
loaders/dim_date_loader.py
──────────────────────────
Populate the dim_date table with date/time dimension data.

This creates a standard date dimension covering 2020-2030 with:
- Date attributes (day, month, year, quarter)
- Time attributes (hour, time_of_day)
- Flags (is_weekend, is_holiday)

Usage:
    from loaders.dim_date_loader import load_dim_date
    load_dim_date(start_year=2020, end_year=2030)
"""

from datetime import datetime, timedelta
from warehouse.connection import get_cursor
from utils.logger import get_logger_name

logger = get_logger_name(__name__)

# Egyptian holidays (fixed dates)
FIXED_HOLIDAYS = {
    # Format: (month, day): "Holiday Name"
    (1, 1): "New Year's Day",
    (1, 7): "Christmas (Orthodox)",
    (1, 25): "Revolution Day (2011)",
    (4, 25): "Sinai Liberation Day",
    (5, 1): "Labor Day",
    (6, 30): "Revolution Day (2013)",
    (7, 23): "Revolution Day (1952)",
    (10, 6): "Armed Forces Day",
}


def is_holiday(date_obj) -> bool:
    """Check if a date is an Egyptian holiday."""
    return (date_obj.month, date_obj.day) in FIXED_HOLIDAYS


def get_time_of_day(hour: int) -> str:
    """Classify hour into time of day bucket."""
    if 5 <= hour < 12:
        return "morning"
    elif 12 <= hour < 17:
        return "afternoon"
    elif 17 <= hour < 21:
        return "evening"
    else:
        return "night"


def generate_date_rows(start_year: int = 2020, end_year: int = 2030):
    """Generate all date dimension rows."""
    rows = []
    date_key = 1

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            for day in range(1, 32):
                try:
                    date_obj = datetime(year, month, day)
                except ValueError:
                    continue  # Skip invalid dates (e.g., Feb 30)

                for hour in range(24):
                    day_of_week = date_obj.strftime("%A")
                    is_weekend = day_of_week in ["Friday", "Saturday"]

                    rows.append((
                        date_key,
                        date_obj.date(),
                        day,
                        month,
                        year,
                        (month - 1) // 3 + 1,  # quarter
                        day_of_week,
                        hour,
                        get_time_of_day(hour),
                        is_weekend,
                        is_holiday(date_obj),
                    ))
                    date_key += 1

    return rows


def load_dim_date(start_year: int = 2020, end_year: int = 2030) -> int:
    """
    Populate dim_date table with date dimension data.
    
    Args:
        start_year: Starting year (default 2020)
        end_year: Ending year (default 2030)
    
    Returns:
        Number of rows inserted
    """
    rows = generate_date_rows(start_year, end_year)

    sql = """
        INSERT INTO warehouse.dim_date (
            date_key, full_date, day, month, year, quarter,
            day_of_week, hour, time_of_day, is_weekend, is_holiday
        ) VALUES %s
        ON CONFLICT (date_key) DO NOTHING
    """

    try:
        with get_cursor() as cur:
            from psycopg2.extras import execute_values
            execute_values(cur, sql, rows)
            inserted = cur.rowcount

        logger.info("dim_date_loaded",
                    rows_generated=len(rows),
                    rows_inserted=inserted,
                    start_year=start_year,
                    end_year=end_year)
        return inserted

    except Exception as e:
        logger.error("dim_date_load_failed", error=str(e))
        raise


if __name__ == "__main__":
    # Quick test
    count = load_dim_date()
    print(f"Loaded {count} rows into dim_date")
