from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

LONDON = ZoneInfo("Europe/London")

def hhmm_to_dt(service_date: date, hhmm: str):
    """
    Convert "HHMM" into a timezone-aware datetime in Europe/London.
    Returns None for blank.
    """
    hhmm = (hhmm or "").strip()
    if not hhmm:
        return None
    if len(hhmm) != 4 or not hhmm.isdigit():
        raise ValueError(f"Bad HHMM value: {hhmm}")

    h = int(hhmm[:2])
    m = int(hhmm[2:])
    return datetime(service_date.year, service_date.month, service_date.day, h, m, tzinfo=LONDON)

def roll_if_next_day(dep: datetime, maybe: datetime | None):
    """
    If time is earlier than departure time, treat as next day (after midnight rollover).
    """
    if maybe is None:
        return None
    if maybe < dep:
        return maybe + timedelta(days=1)
    return maybe
