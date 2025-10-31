from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import numpy as np
import pandas as pd
import pytz
from dateutil import parser

KST = pytz.timezone("Asia/Seoul")


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


def _synthetic(base: float, amplitude: float, periods: int = 120) -> List[float]:
    return [base + amplitude * math.sin(i / max(1, periods - 1) * 3.5) for i in range(periods)]


def synthetic_series(base: float, amplitude: float, periods: int = 120, seed: Optional[int] = None) -> pd.Series:
    if seed is not None:
        np.random.seed(seed)
    values = _synthetic(base, amplitude, periods)
    noise = np.random.normal(scale=amplitude * 0.02, size=len(values)) if seed is not None else 0
    if isinstance(noise, np.ndarray):
        values = [v + n for v, n in zip(values, noise)]
    end = pd.Timestamp(kst_now()).normalize()
    index = pd.date_range(end=end, periods=len(values), freq="B")
    return pd.Series(values, index=index)


def clip_numeric(value: Any, precision: int = 6) -> Optional[float]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return float(round(float(value), precision))


def flatten_records(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [dict(rec) for rec in records]


def count_non_null(records: Iterable[Dict[str, Any]], required_keys: Iterable[str]) -> float:
    rows = list(records)
    required = set(required_keys)
    hits = sum(1 for row in rows if row.get("key") in required and row.get("value") not in (None, ""))
    return hits / max(1, len(required))


def coverage_ratio(records: Iterable[Dict[str, Any]], required_keys: Iterable[str]) -> float:
    required = list(required_keys)
    filled = {key for rec in records if rec.get("key") in required and rec.get("value") not in (None, "") for key in [rec["key"]]}
    return len(filled) / max(1, len(required))
