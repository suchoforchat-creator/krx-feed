from __future__ import annotations

import json
import os
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Iterable, Iterator, Optional, Sequence

import math
from statistics import pstdev

from zoneinfo import ZoneInfo


TS_FORMAT = "%Y-%m-%d %H:%M"
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
QUALITY_VALUES = {"primary", "secondary", "final"}
KST = ZoneInfo("Asia/Seoul")


def aware_now(tz_name: str = "Asia/Seoul") -> datetime:
    tz = ZoneInfo(tz_name)
    return datetime.now(tz)


def ensure_tz(dt: datetime, tz_name: str) -> datetime:
    tz = ZoneInfo(tz_name)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=tz)
    return dt.astimezone(tz)


def to_kst(dt: datetime, tz_name: str = "Asia/Seoul") -> datetime:
    return ensure_tz(dt, tz_name).astimezone(KST)


def ts_to_str(dt: datetime) -> str:
    return dt.strftime(TS_FORMAT)


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    try:
        if isinstance(value, str):
            value = value.replace(",", "")
        return float(value)
    except (TypeError, ValueError):
        return default


def _clean_series(series: Sequence[float | None]) -> list[float]:
    return [float(x) for x in series if x is not None]


def calc_log_returns(series: Sequence[float | None]) -> list[float]:
    values = _clean_series(series)
    returns: list[float] = []
    for prev, curr in zip(values, values[1:]):
        if prev == 0:
            continue
        returns.append(math.log(curr / prev))
    return returns


def realized_volatility(series: Sequence[float | None], window: int) -> Optional[float]:
    values = _clean_series(series)
    if len(values) < window + 1:
        return None
    log_returns = calc_log_returns(values)[-window:]
    if not log_returns:
        return None
    std = 0.0 if len(log_returns) == 1 else pstdev(log_returns)
    return float(math.sqrt(252) * std)


def rolling_corr(series_a: Sequence[float | None], series_b: Sequence[float | None], window: int) -> Optional[float]:
    a = _clean_series(series_a)
    b = _clean_series(series_b)
    length = min(len(a), len(b))
    if length < window:
        return None
    subset_a = a[-window:]
    subset_b = b[-window:]
    mean_a = sum(subset_a) / window
    mean_b = sum(subset_b) / window
    cov = sum((x - mean_a) * (y - mean_b) for x, y in zip(subset_a, subset_b))
    var_a = sum((x - mean_a) ** 2 for x in subset_a)
    var_b = sum((y - mean_b) ** 2 for y in subset_b)
    if var_a == 0 or var_b == 0:
        return None
    return float(cov / math.sqrt(var_a * var_b))


def simple_return(series: Sequence[float | None], periods: int) -> Optional[float]:
    values = _clean_series(series)
    if len(values) <= periods:
        return None
    end = values[-1]
    start = values[-periods - 1]
    if start == 0:
        return None
    return float(end / start - 1)


def basis(futures: float, spot: float) -> Optional[float]:
    if spot in (0, None):
        return None
    return float((futures - spot) / spot)


def bp_change(series: Sequence[float | None]) -> Optional[float]:
    values = _clean_series(series)
    if len(values) < 2:
        return None
    return float((values[-1] - values[-2]) * 100)


def pct_change(series: Sequence[float | None]) -> Optional[float]:
    values = _clean_series(series)
    if len(values) < 2:
        return None
    prev = values[-2]
    if prev == 0:
        return None
    return float((values[-1] - prev) / prev)


@dataclass
class RetryConfig:
    attempts: int = 3
    wait_initial: float = 1.0
    wait_max: float = 8.0


def retryable(func: Callable[..., Any], config: RetryConfig | None = None) -> Callable[..., Any]:
    config = config or RetryConfig()

    def wrapped(*args: Any, **kwargs: Any) -> Any:
        delay = config.wait_initial
        last_exc: Exception | None = None
        for attempt in range(config.attempts):
            try:
                return func(*args, **kwargs)
            except Exception as exc:  # pragma: no cover - defensive
                last_exc = exc
                if attempt == config.attempts - 1:
                    raise
                time.sleep(delay)
                delay = min(delay * 2, config.wait_max)
        if last_exc:
            raise last_exc

    return wrapped


@contextmanager
def json_log_writer(path: str) -> Iterator[list[dict[str, Any]]]:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    events: list[dict[str, Any]] = []
    try:
        yield events
    finally:
        with open(path, "w", encoding="utf-8") as fp:
            json.dump(events, fp, ensure_ascii=False, indent=2)


def coverage_ratio(records: Iterable[dict[str, Any]], required: set[tuple[str, str]]) -> float:
    available = {(row["asset"], row["key"]) for row in records if row.get("value") is not None}
    if not required:
        return 1.0
    hit = len(required & available)
    return hit / len(required)


def ensure_schema(row: dict[str, Any]) -> dict[str, Any]:
    ordered = {col: row.get(col) for col in SCHEMA_COLUMNS}
    quality = ordered.get("quality")
    if quality not in QUALITY_VALUES:
        raise ValueError(f"invalid quality: {quality}")
    return ordered


def make_timestamp(tz_name: str = "Asia/Seoul", dt: Optional[datetime] = None) -> str:
    base = dt or aware_now(tz_name)
    return ts_to_str(to_kst(base, tz_name))


def ensure_notes(notes: Optional[str]) -> str:
    return notes or ""


def sanitize_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    sanitized: list[dict[str, Any]] = []
    for row in rows:
        row = dict(row)
        row.setdefault("notes", "")
        sanitized.append(ensure_schema(row))
    return sanitized
