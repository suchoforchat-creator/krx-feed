from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
import pytz
from dateutil import parser

KST = pytz.timezone("Asia/Seoul")

SCHEMA_COLUMNS = [
    "ts_kst",
    "asset",
    "key",
    "value",
    "unit",
    "window",
    "change_abs",
    "change_pct",
    "source",
    "quality",
    "url",
    "notes",
]

ALLOWED_QUALITIES = {"primary", "secondary", "final", "tagged", "prelim", "preliminary"}


@dataclass
class TimeConfig:
    tz: timezone

    @classmethod
    def from_name(cls, name: str) -> "TimeConfig":
        return cls(pytz.timezone(name))


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def kst_now() -> datetime:
    return datetime.now(KST)


def to_kst(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = KST.localize(dt)
    else:
        dt = dt.astimezone(KST)
    return dt


def ts_string(dt: datetime) -> str:
    return to_kst(dt).strftime("%Y-%m-%d %H:%M")


def iso_ts(dt: datetime) -> str:
    return to_kst(dt).isoformat()


def make_timestamp(tz_name: str, dt: datetime) -> str:
    tz = pytz.timezone(tz_name)
    if dt.tzinfo is None:
        localized = tz.localize(dt)
    else:
        localized = dt.astimezone(tz)
    return localized.strftime("%Y-%m-%d %H:%M")


def parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return to_kst(value)
    return to_kst(parser.parse(str(value)))


def calc_log_returns(series: pd.Series) -> pd.Series:
    return np.log(series / series.shift(1))


def calc_simple_returns(series: pd.Series) -> pd.Series:
    return series.pct_change()


def rolling_vol(series: pd.Series, window: int) -> float:
    if len(series) < window:
        return float("nan")
    returns = calc_log_returns(series).dropna()
    if returns.empty:
        return float("nan")
    return float(np.sqrt(252) * returns.tail(window).std())


def rolling_corr(series_a: pd.Series, series_b: pd.Series, window: int) -> float:
    if len(series_a) < window or len(series_b) < window:
        return float("nan")
    return float(series_a.tail(window).corr(series_b.tail(window)))


def safe_div(numer: float, denom: float) -> float:
    if denom == 0:
        return float("nan")
    return numer / denom


def write_json(path: Path, payload: Any) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)


def load_yaml(path: Path) -> Dict[str, Any]:
    import yaml

    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def clip_numeric(value: Any, precision: int = 6) -> Optional[float]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return float(round(float(value), precision))


def flatten_records(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(rec) for rec in records]


def ensure_schema(row: Dict[str, Any]) -> Dict[str, Any]:
    missing = [col for col in SCHEMA_COLUMNS if col not in row]
    if missing:
        raise ValueError(f"missing columns: {missing}")
    quality = str(row.get("quality", "")).lower()
    if quality not in ALLOWED_QUALITIES:
        raise ValueError(f"invalid quality: {row.get('quality')}")
    return row


def count_non_null(records: Iterable[Dict[str, Any]], required_keys: Iterable[str]) -> float:
    rows = list(records)
    required = set(required_keys)
    hits = sum(1 for row in rows if row.get("key") in required and row.get("value") not in (None, ""))
    return hits / max(1, len(required))


def coverage_ratio(
    records: Iterable[Dict[str, Any]],
    required_keys: Iterable[Tuple[str, str] | str],
) -> float:
    required: Set[Tuple[str, str]] = set()
    for key in required_keys:
        if isinstance(key, tuple):
            required.add(key)
        else:
            required.add(("", str(key)))

    if not required:
        return 1.0

    covered: Set[Tuple[str, str]] = set()
    for record in records:
        asset = str(record.get("asset", ""))
        field = str(record.get("key", ""))
        pair = (asset, field)
        if pair not in required:
            continue
        value = record.get("value")
        if value not in (None, ""):
            covered.add(pair)

    return len(covered) / len(required)
