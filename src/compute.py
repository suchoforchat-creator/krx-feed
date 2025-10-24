from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Optional

from . import utils


@dataclass
class RecordBuilder:
    ts_kst: str

    def make(
        self,
        asset: str,
        key: str,
        value: Optional[float],
        *,
        unit: str = "",
        window: str = "",
        change_abs: Optional[float] = None,
        change_pct: Optional[float] = None,
        source: str,
        quality: str,
        url: str,
        notes: str | None = None,
    ) -> dict[str, Any]:
        record = {
            "ts_kst": self.ts_kst,
            "asset": asset,
            "key": key,
            "value": None if value is None else float(value),
            "unit": unit,
            "window": window,
            "change_abs": None if change_abs is None else float(change_abs),
            "change_pct": None if change_pct is None else float(change_pct),
            "source": source,
            "quality": quality,
            "url": url,
            "notes": utils.ensure_notes(notes),
        }
        return utils.ensure_schema(record)


def ts_now(tz: str) -> str:
    return utils.make_timestamp(tz)


def compute_hv(prices: Iterable[float], window: int = 30) -> Optional[float]:
    return utils.realized_volatility(list(prices), window)


def compute_correlation(series_a: Iterable[float], series_b: Iterable[float], window: int = 20) -> Optional[float]:
    return utils.rolling_corr(list(series_a), list(series_b), window)


def compute_basis(futures_price: float, spot_price: float) -> Optional[float]:
    return utils.basis(futures_price, spot_price)


def compute_realized_vol(series: Iterable[float], window: int) -> Optional[float]:
    return compute_hv(series, window)


def compute_simple_return(series: Iterable[float], periods: int) -> Optional[float]:
    return utils.simple_return(list(series), periods)


def compute_bp_change(series: Iterable[float]) -> Optional[float]:
    return utils.bp_change(list(series))


def compute_pct_change(series: Iterable[float]) -> Optional[float]:
    return utils.pct_change(list(series))


def promote_final_quality(rows: list[dict[str, Any]], notes: Optional[str] = None) -> list[dict[str, Any]]:
    promoted: list[dict[str, Any]] = []
    for row in rows:
        new_row = dict(row)
        new_row["quality"] = "final"
        if notes:
            new_row["notes"] = notes
        promoted.append(utils.ensure_schema(new_row))
    return promoted


def latest_timestamp(rows: Iterable[dict[str, Any]]) -> Optional[datetime]:
    timestamps = []
    for row in rows:
        ts_str = row.get("ts_kst")
        if not ts_str:
            continue
        try:
            timestamps.append(datetime.strptime(ts_str, utils.TS_FORMAT))
        except ValueError:
            continue
    return max(timestamps) if timestamps else None

