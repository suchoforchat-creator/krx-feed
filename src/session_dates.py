"""KRX completed-session dates, independent of delayed collection timestamps."""
import csv
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
CALENDAR = Path(__file__).resolve().parents[1] / "config" / "krx_closed_dates.csv"


def closed_dates(path=CALENDAR):
    with Path(path).open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    result = set()
    for row in rows:
        day = date.fromisoformat(row["date"])
        if day in result or not row.get("reason") or not row.get("source"):
            raise ValueError("KRX_CALENDAR_INVALID")
        result.add(day)
    if not result:
        raise ValueError("KRX_CALENDAR_EMPTY")
    return result


def is_session(day, closed=None):
    closed = closed_dates() if closed is None else closed
    if day > max(closed):
        raise ValueError("KRX_CALENDAR_EXPIRED")
    if day < min(closed):
        raise ValueError("KRX_CALENDAR_BEFORE_COVERAGE")
    return day.weekday() < 5 and day not in closed


def previous_session(day, closed=None):
    closed = closed_dates() if closed is None else closed
    day -= timedelta(days=1)
    while not is_session(day, closed):
        day -= timedelta(days=1)
    return day


def completed_session(now, phase=None, closed=None):
    if now.tzinfo is None:
        raise ValueError("AWARE_COLLECTION_TIME_REQUIRED")
    now = now.astimezone(KST)
    closed = closed_dates() if closed is None else closed
    day = now.date()
    current_is_session = is_session(day, closed)
    if phase == "0730" or now.time() < time(15, 30) or not current_is_session:
        return previous_session(day, closed)
    return day
