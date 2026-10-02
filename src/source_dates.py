"""Keep collection time separate from provider quote dates in the 12-column CSV."""
from datetime import date, datetime
from numbers import Number
import re

import pandas as pd

TOKEN = "source_date="
UNKNOWN = "source_date=UNKNOWN"


def quote_day(value):
    if value is None or isinstance(value, Number):
        return None
    try:
        stamp = pd.Timestamp(value)
        return None if pd.isna(stamp) else stamp.date()
    except (ValueError, TypeError):
        return None


def note_date(note):
    values = re.findall(r"(?:^|;)source_date=([^;]+)", str(note or ""))
    if len(values) != 1:
        return None
    try:
        return date.fromisoformat(values[0])
    except ValueError:
        return None


def bind_note(note, day):
    text = str(note or "")
    # A single authoritative token; do not keep contradictory aliases.
    parts = [p for p in text.split(";") if p and not p.startswith(TOKEN)]
    parts.append(TOKEN + (day.isoformat() if isinstance(day, date) else "UNKNOWN"))
    return ";".join(parts)


def latest_source_date(frame, field):
    if frame is None or frame.empty or not {"field", "value", "ts_kst"}.issubset(frame.columns):
        return None
    subset = frame.loc[frame["field"] == field].copy()
    subset["value"] = pd.to_numeric(subset["value"], errors="coerce")
    subset = subset.dropna(subset=["value", "ts_kst"]).sort_values("ts_kst")
    if subset.empty:
        return None
    row = subset.iloc[-1]
    if "source_date" in subset.columns and pd.notna(row.get("source_date")):
        return quote_day(row.get("source_date"))
    if TOKEN in str(row.get("notes", "")):
        return note_date(row["notes"])
    return quote_day(row["ts_kst"])
