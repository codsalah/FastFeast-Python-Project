import pandas as pd
from datetime import date, timedelta


# ------------------------------------Defining Holidays and Weekends------------------------------------------
FIXED_HOLIDAYS = {
    date(2026,  1,  1): "New Year's Day",
    date(2026,  1,  7): "Coptic Christmas",
    date(2026,  1, 25): "January 25 Revolution",
    date(2026,  4, 25): "Sinai Liberation Day",
    date(2026,  5,  1): "Labour Day",
    date(2026,  6, 30): "June 30 Revolution",
    date(2026,  7, 23): "July 23 Revolution",
    date(2026, 10,  6): "Armed Forces Day",
}

ISLAMIC_HOLIDAYS = {
    date(2026,  3, 19): "Eid Al-Fitr",
    date(2026,  3, 20): "Eid Al-Fitr",
    date(2026,  3, 21): "Eid Al-Fitr",
    date(2026,  3, 22): "Eid Al-Fitr",
    date(2026,  3, 23): "Eid Al-Fitr",
    date(2026,  5, 27): "Eid Al-Adha",
    date(2026,  5, 28): "Eid Al-Adha",
    date(2026,  5, 29): "Eid Al-Adha",
    date(2026,  6, 26): "Islamic New Year",
    date(2026,  8, 26): "Prophet's Birthday",
}

ALL_HOLIDAYS = {**FIXED_HOLIDAYS, **ISLAMIC_HOLIDAYS}

# -----------------------------------------Helper Functions-------------------------------------------------
def get_quarter(month: int) -> int:
    return (month - 1) // 3 + 1


def get_day_of_week(d: date) -> str:
    # Python weekday(): 0=Monday ... 4=Friday, 5=Saturday, 6=Sunday
    return d.strftime("%A")


def is_egyptian_weekend(d: date) -> bool:
    # Friday = 4, Saturday = 5
    return d.weekday() in (4, 5)


def get_time_of_day(hour: int) -> str:
    if 5 <= hour < 12:
        return "Morning"
    elif 12 <= hour < 17:
        return "Afternoon"
    elif 17 <= hour < 21:
        return "Evening"
    else:
        return "Night"


def make_date_key(d: date, hour: int) -> int:
    # Format: YYYYMMDDHHH → e.g. 2026010100 for Jan 1 at 00:00
    return int(f"{d.strftime('%Y%m%d')}{hour:02d}")

# -----------------------------------------Load Function------------------------------------------------
def generate_dim_date_rows(year: int = 2026) -> pd.DataFrame:
    rows = []
    start = date(year, 1, 1)
    end   = date(year, 12, 31)
    delta = timedelta(days=1)

    current = start
    while current <= end:
        is_weekend = is_egyptian_weekend(current)
        is_holiday = current in ALL_HOLIDAYS

        for hour in range(24):
            rows.append({
                "date_key": make_date_key(current, hour),
                "full_date": current,
                "day": current.day,
                "month": current.month,
                "year": current.year,
                "quarter": get_quarter(current.month),
                "day_of_week": get_day_of_week(current),
                "hour": hour,
                "time_of_day": get_time_of_day(hour),
                "is_weekend": is_weekend,
                "is_holiday": is_holiday,
            })
        current += delta

    return pd.DataFrame(rows)
