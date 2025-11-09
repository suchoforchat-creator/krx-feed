from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Callable

import numpy as np
import pandas as pd

from datetime import datetime
import logging

from .utils import (
    SCHEMA_COLUMNS,
    clip_numeric,
    coverage_ratio,
    ensure_schema,
    rolling_corr,
    rolling_vol,
    ts_string,
)

logger = logging.getLogger(__name__)


def _debug_value(name: str, value: float, validator: Callable[[float], bool]) -> None:
    """간단한 디버깅 헬퍼.

    계산된 값이 기대 범위를 벗어날 경우 로그로 남겨 이후 조사에 활용한다.
    validator는 True/False를 돌려주는 함수이며, False가 나오면 경고를 남긴다.
    """

    if np.isnan(value):
        # NaN은 계산 재료가 부족한 경우이므로 조용히 넘어간다.
        return
    try:
        if not validator(value):
            logger.debug("compute::_debug_value :: %s -> suspicious value %.6f", name, value)
    except Exception as exc:  # pragma: no cover - 방어적 장치
        logger.debug("compute::_debug_value :: validator failure for %s (%s)", name, exc)


REQUIRED_KEYS = [
    "KOSPI:idx",
    "KOSDAQ:idx",
    "KOSPI:adv",
    "KOSPI:dec",
    "KOSPI:unch",
    "KOSDAQ:adv",
    "KOSDAQ:dec",
    "KOSDAQ:unch",
    "KOSPI:trin",
    "KOSPI:turnover",
    "KOSPI:limit_up",
    "KOSPI:limit_down",
    "K200:hv30",
    "ES:basis",
    "NQ:basis",
    "SOX:ret_1w",
    "SOX:ret_1m",
    "USD/KRW:spot",
    "DXY:proxy",
    "2s10s_US:spread",
    "2s10s_KR:spread",
    "UST:2y",
    "UST:10y",
    "KR:3y",
    "KR:10y",
    "WTI:spot",
    "Brent:spot",
    "Gold:spot",
    "Copper:spot",
    "BTC:spot",
    "BTC:corr20",
]


@dataclass
class SeriesBundle:
    asset: str
    field: str
    series: pd.Series
    source: str
    quality: str
    url: str


def compute_hv(prices: Sequence[float], window: int) -> float:
    if window <= 0:
        raise ValueError("window must be positive")
    if len(prices) < 2:
        return float("nan")
    log_returns: List[float] = []
    for prev, curr in zip(prices, prices[1:]):
        if prev == 0:
            continue
        log_returns.append(math.log(curr / prev))
    if len(log_returns) < window:
        return float("nan")
    tail = log_returns[-window:]
    mean = sum(tail) / window
    variance = sum((val - mean) ** 2 for val in tail) / window
    return math.sqrt(252 * variance)


def compute_correlation(series_a: Sequence[float], series_b: Sequence[float], window: int) -> float:
    if window <= 1:
        raise ValueError("window must be greater than 1")
    if len(series_a) < window or len(series_b) < window:
        return float("nan")
    tail_a = list(series_a)[-window:]
    tail_b = list(series_b)[-window:]
    mean_a = sum(tail_a) / window
    mean_b = sum(tail_b) / window
    cov = sum((a - mean_a) * (b - mean_b) for a, b in zip(tail_a, tail_b))
    var_a = sum((a - mean_a) ** 2 for a in tail_a)
    var_b = sum((b - mean_b) ** 2 for b in tail_b)
    if var_a == 0 or var_b == 0:
        return float("nan")
    return cov / math.sqrt(var_a * var_b)


def compute_basis(future: float, spot: float) -> float:
    if spot == 0:
        raise ValueError("spot price cannot be zero")
    return (future / spot) - 1


class RecordBuilder:
    def __init__(self, ts: str | datetime) -> None:
        if isinstance(ts, datetime):
            self._ts_kst = ts_string(ts)
        else:
            self._ts_kst = str(ts)

    def make(
        self,
        asset: str,
        key: str,
        value: float,
        *,
        unit: str = "",
        window: str = "",
        change_abs: float | None = None,
        change_pct: float | None = None,
        source: str = "",
        quality: str = "primary",
        url: str = "",
        notes: str = "",
    ) -> Dict[str, object]:
        record: Dict[str, object] = {
            "ts_kst": self._ts_kst,
            "asset": asset,
            "key": key,
            "value": clip_numeric(value),
            "unit": unit,
            "window": window,
            "change_abs": clip_numeric(change_abs) if change_abs is not None else None,
            "change_pct": clip_numeric(change_pct) if change_pct is not None else None,
            "source": source,
            "quality": quality,
            "url": url,
            "notes": notes,
        }
        # ensure optional fields are serialisable
        for column in SCHEMA_COLUMNS:
            record.setdefault(column, None)
        ensure_schema(record)
        return record


def _series_from_raw(raw: Dict[str, pd.DataFrame], asset: str, field: str) -> SeriesBundle:
    frame = raw.get(asset)
    if frame is None or frame.empty:
        return SeriesBundle(asset, field, pd.Series(dtype=float), "", "", "")
    required = {"field", "ts_kst", "value"}
    if not required.issubset(frame.columns):
        return SeriesBundle(asset, field, pd.Series(dtype=float), "", "", "")

    subset = frame[frame["field"] == field].copy()
    if subset.empty:
        return SeriesBundle(asset, field, pd.Series(dtype=float), "", "", "")
    subset["ts_kst"] = pd.to_datetime(subset["ts_kst"])
    subset.sort_values("ts_kst", inplace=True)
    subset["value"] = pd.to_numeric(subset["value"], errors="coerce")
    series = subset.set_index("ts_kst")["value"].dropna()
    source = str(subset["source"].iloc[-1]) if "source" in subset else ""
    quality = str(subset["quality"].iloc[-1]) if "quality" in subset else "primary"
    url = str(subset["url"].iloc[-1]) if "url" in subset else ""
    return SeriesBundle(asset, field, series, source, quality, url)


def _latest(series: pd.Series) -> float:
    return float(series.iloc[-1]) if not series.empty else float("nan")


def _prev(series: pd.Series) -> float:
    if len(series) < 2:
        return float("nan")
    return float(series.iloc[-2])


def _change(series: pd.Series) -> float:
    return _latest(series) - _prev(series)


def _pct_change(series: pd.Series) -> float:
    prev = _prev(series)
    if np.isnan(prev) or prev == 0:
        return float("nan")
    return _change(series) / prev


def _record(
    ts_kst: str,
    asset: str,
    key: str,
    value: float,
    unit: str,
    window: str,
    change_abs: float,
    change_pct: float,
    source: str,
    quality: str,
    url: str,
    notes: str = "",
) -> Dict:
    return {
        "ts_kst": ts_kst,
        "asset": asset,
        "key": key,
        "value": clip_numeric(value),
        "unit": unit,
        "window": window,
        "change_abs": clip_numeric(change_abs),
        "change_pct": clip_numeric(change_pct),
        "source": source or "KIS",
        "quality": quality or "primary",
        "url": url,
        "notes": notes,
    }


def compute_records(ts, raw: Dict[str, pd.DataFrame], notes: Optional[Dict[str, str]] = None) -> List[Dict]:
    ts_kst = ts_string(ts)
    records: List[Dict] = []
    notes_map = notes or {}

    def note(asset: str, key: str) -> str:
        return notes_map.get(f"{asset}:{key}", "")

    kospi = _series_from_raw(raw, "KOSPI", "close")
    kosdaq = _series_from_raw(raw, "KOSDAQ", "close")
    k200 = _series_from_raw(raw, "K200", "close")
    spx = _series_from_raw(raw, "SPX", "close")
    ndx = _series_from_raw(raw, "NDX", "close")
    sox = _series_from_raw(raw, "SOX", "close")
    es = _series_from_raw(raw, "ES", "close")
    nq = _series_from_raw(raw, "NQ", "close")
    dx = _series_from_raw(raw, "DX", "close")
    usdkrw = _series_from_raw(raw, "USD/KRW", "close")
    kr3y = _series_from_raw(raw, "KR3Y", "yield")
    kr10y = _series_from_raw(raw, "KR10Y", "yield")
    ust2y = _series_from_raw(raw, "UST2Y", "yield")
    ust10y = _series_from_raw(raw, "UST10Y", "yield")

    adv_kospi = _series_from_raw(raw, "KOSPI", "adv_count")
    dec_kospi = _series_from_raw(raw, "KOSPI", "dec_count")
    unch_kospi = _series_from_raw(raw, "KOSPI", "unch_count")
    vol_adv = _series_from_raw(raw, "KOSPI", "adv_value")
    vol_dec = _series_from_raw(raw, "KOSPI", "dec_value")
    limit_up = _series_from_raw(raw, "KOSPI", "limit_up")
    limit_down = _series_from_raw(raw, "KOSPI", "limit_down")
    turnover = _series_from_raw(raw, "KOSPI", "turnover")

    adv_kosdaq = _series_from_raw(raw, "KOSDAQ", "adv_count")
    dec_kosdaq = _series_from_raw(raw, "KOSDAQ", "dec_count")
    unch_kosdaq = _series_from_raw(raw, "KOSDAQ", "unch_count")

    btc = _series_from_raw(raw, "BTC", "close")
    wti = _series_from_raw(raw, "WTI", "close")
    brent = _series_from_raw(raw, "Brent", "close")
    gold = _series_from_raw(raw, "Gold", "close")
    copper = _series_from_raw(raw, "Copper", "close")

    records.extend(
        [
            _record(
                ts_kst,
                "KOSPI",
                "idx",
                _latest(kospi.series),
                "pt",
                "1D",
                _change(kospi.series),
                _pct_change(kospi.series),
                kospi.source,
                kospi.quality,
                kospi.url,
                notes=note("KOSPI", "idx"),
            ),
            _record(
                ts_kst,
                "KOSDAQ",
                "idx",
                _latest(kosdaq.series),
                "pt",
                "1D",
                _change(kosdaq.series),
                _pct_change(kosdaq.series),
                kosdaq.source,
                kosdaq.quality,
                kosdaq.url,
                notes=note("KOSDAQ", "idx"),
            ),
        ]
    )

    def add_simple(asset: str, key: str, bundle: SeriesBundle, unit: str = "count") -> None:
        records.append(
            _record(
                ts_kst,
                asset,
                key,
                _latest(bundle.series),
                unit,
                "1D",
                _change(bundle.series),
                _pct_change(bundle.series),
                bundle.source,
                bundle.quality,
                bundle.url,
                notes=note(asset, key),
            )
        )

    add_simple("KOSPI", "adv", adv_kospi)
    add_simple("KOSPI", "dec", dec_kospi)
    add_simple("KOSPI", "unch", unch_kospi)
    add_simple("KOSDAQ", "adv", adv_kosdaq)
    add_simple("KOSDAQ", "dec", dec_kosdaq)
    add_simple("KOSDAQ", "unch", unch_kosdaq)
    add_simple("KOSPI", "limit_up", limit_up)
    add_simple("KOSPI", "limit_down", limit_down)

    trin_value = float("nan")
    adv_cnt = _latest(adv_kospi.series)
    dec_cnt = _latest(dec_kospi.series)
    adv_val = _latest(vol_adv.series)
    dec_val = _latest(vol_dec.series)
    if all(not np.isnan(v) and v for v in [adv_cnt, dec_cnt, adv_val, dec_val]):
        trin_value = (adv_cnt / dec_cnt) / (adv_val / dec_val)
    records.append(
        _record(
            ts_kst,
            "KOSPI",
            "trin",
            trin_value,
            "ratio",
            "1D",
            float("nan"),
            float("nan"),
            vol_adv.source,
            vol_adv.quality,
            vol_adv.url,
            notes=note("KOSPI", "trin"),
        )
    )

    records.append(
        _record(
            ts_kst,
            "KOSPI",
            "turnover",
            _latest(turnover.series),
            "krw_bn",
            "1D",
            _change(turnover.series),
            _pct_change(turnover.series),
            turnover.source,
            turnover.quality,
            turnover.url,
            notes=note("KOSPI", "turnover"),
        )
    )

    hv30 = rolling_vol(k200.series, 30)
    records.append(
        _record(
            ts_kst,
            "K200",
            "hv30",
            hv30,
            "vol",
            "30D",
            float("nan"),
            float("nan"),
            k200.source,
            k200.quality,
            k200.url,
            notes=note("K200", "hv30"),
        )
    )

    def returns(series: pd.Series, window: int) -> float:
        if len(series) <= window:
            return float("nan")
        return float(series.iloc[-1] / series.iloc[-window - 1] - 1)

    def basis(fut: pd.Series, spot: pd.Series) -> float:
        if fut.empty or spot.empty:
            return float("nan")
        return float(fut.iloc[-1] / spot.iloc[-1] - 1)

    records.extend(
        [
            _record(
                ts_kst,
                "ES",
                "basis",
                basis(es.series, spx.series),
                "ratio",
                "1D",
                float("nan"),
                float("nan"),
                es.source,
                es.quality,
                es.url,
                notes=note("ES", "basis"),
            ),
            _record(
                ts_kst,
                "NQ",
                "basis",
                basis(nq.series, ndx.series),
                "ratio",
                "1D",
                float("nan"),
                float("nan"),
                nq.source,
                nq.quality,
                nq.url,
                notes=note("NQ", "basis"),
            ),
            _record(
                ts_kst,
                "SOX",
                "ret_1w",
                returns(sox.series, 5),
                "return",
                "1W",
                float("nan"),
                float("nan"),
                sox.source,
                sox.quality,
                sox.url,
                notes=note("SOX", "ret_1w"),
            ),
            _record(
                ts_kst,
                "SOX",
                "ret_1m",
                returns(sox.series, 21),
                "return",
                "1M",
                float("nan"),
                float("nan"),
                sox.source,
                sox.quality,
                sox.url,
                notes=note("SOX", "ret_1m"),
            ),
        ]
    )

    records.append(
        _record(
            ts_kst,
            "USD/KRW",
            "spot",
            _latest(usdkrw.series),
            "krw",
            "1D",
            _change(usdkrw.series),
            _pct_change(usdkrw.series),
            usdkrw.source,
            usdkrw.quality,
            usdkrw.url,
            notes=note("USD/KRW", "spot"),
        )
    )

    dxy_value = _latest(dx.series)
    records.append(
        _record(
            ts_kst,
            "DXY",
            "proxy",
            dxy_value,
            "index",
            "1D",
            float("nan"),
            float("nan"),
            dx.source or "KIS-proxy",
            dx.quality or "primary",
            dx.url,
            notes=note("DXY", "proxy") or "proxy",
        )
    )

    def yield_record(bundle: SeriesBundle, asset: str, key: str) -> None:
        base_note = note(asset, key)
        note_value = base_note or ("proxy" if asset == "UST" else "")
        records.append(
            _record(
                ts_kst,
                asset,
                key,
                _latest(bundle.series),
                "pct",
                "1D",
                _change(bundle.series),
                _pct_change(bundle.series),
                bundle.source,
                bundle.quality,
                bundle.url,
                notes=note_value,
            )
        )

    yield_record(ust2y, "UST", "2y")
    yield_record(ust10y, "UST", "10y")
    yield_record(kr3y, "KR", "3y")
    yield_record(kr10y, "KR", "10y")

    def add_spread(asset: str, key: str, long_leg: SeriesBundle, short_leg: SeriesBundle) -> None:
        """미국/한국 2s10s 스프레드를 계산하고 디버깅 정보를 남긴다."""

        long_latest = _latest(long_leg.series)
        short_latest = _latest(short_leg.series)
        value = float("nan")
        if not np.isnan(long_latest) and not np.isnan(short_latest):
            value = (long_latest - short_latest) * 100.0
            _debug_value(f"{asset}:{key}", value, lambda v: abs(v) < 1000)

        combined_source = "+".join(
            sorted(
                {
                    src
                    for src in [long_leg.source, short_leg.source]
                    if src
                }
            )
        )
        combined_quality = long_leg.quality or short_leg.quality
        combined_url = " ".join(
            [part for part in [long_leg.url, short_leg.url] if part]
        )

        records.append(
            _record(
                ts_kst,
                asset,
                key,
                value,
                "bp",
                "1D",
                float("nan"),
                float("nan"),
                combined_source or long_leg.source,
                combined_quality,
                combined_url,
                notes=note(asset, key),
            )
        )

    add_spread("2s10s_US", "spread", ust10y, ust2y)
    add_spread("2s10s_KR", "spread", kr10y, kr3y)

    for bundle, asset, unit in [
        (wti, "WTI", "usd"),
        (brent, "Brent", "usd"),
        (gold, "Gold", "usd"),
        (copper, "Copper", "usd"),
        (btc, "BTC", "usd"),
    ]:
        records.append(
            _record(
                ts_kst,
                asset,
                "spot",
                _latest(bundle.series),
                unit,
                "1D",
                _change(bundle.series),
                _pct_change(bundle.series),
                bundle.source,
                bundle.quality,
                bundle.url,
                notes=note(asset, "spot"),
            )
        )

    btc_corr = rolling_corr(
        np.log(btc.series).diff().dropna(),
        np.log(nq.series).diff().dropna(),
        20,
    )
    records.append(
        _record(
            ts_kst,
            "BTC",
            "corr20",
            btc_corr,
            "corr",
            "20D",
            float("nan"),
            float("nan"),
            btc.source,
            btc.quality,
            btc.url,
            notes=note("BTC", "corr20"),
        )
    )

    return records


def check_coverage(records: Iterable[Dict]) -> float:
    filled = {
        f"{row.get('asset')}:{row.get('key')}"
        for row in records
        if row.get("value") not in (None, "")
    }
    return len(filled.intersection(set(REQUIRED_KEYS))) / max(1, len(REQUIRED_KEYS))
