from __future__ import annotations

import math
from typing import List

from .. import compute

FUT_SOURCE = "CME"
SPOT_SOURCE = "S&P"
URL_FUT = "https://www.cmegroup.com"
URL_SPOT = "https://www.spglobal.com"


def _synthetic(base: float, amplitude: float, periods: int = 90) -> List[float]:
    return [base + amplitude * math.cos(i / (periods - 1) * math.pi) for i in range(periods)]


def _change(series: List[float]) -> tuple[float | None, float | None]:
    if len(series) < 2:
        return None, None
    latest = float(series[-1])
    prev = float(series[-2])
    diff = latest - prev
    pct = None if prev == 0 else diff / prev
    return diff, pct


def fetch(phase: str, tz: str) -> list[dict[str, object]]:
    ts_kst = compute.ts_now(tz)
    builder = compute.RecordBuilder(ts_kst)

    es_future = _synthetic(4500, 60)
    spx_spot = _synthetic(4480, 55)
    nq_future = _synthetic(15500, 180)
    ndx_spot = _synthetic(15450, 170)
    sox_index = _synthetic(3800, 120)

    rows: list[dict[str, object]] = []

    es_change_abs, es_change_pct = _change(es_future)
    nq_change_abs, nq_change_pct = _change(nq_future)

    es_basis = compute.compute_basis(es_future[-1], spx_spot[-1])
    nq_basis = compute.compute_basis(nq_future[-1], ndx_spot[-1])

    for asset, series, source, url, change_abs, change_pct in [
        ("ES", es_future, FUT_SOURCE, URL_FUT, es_change_abs, es_change_pct),
        ("NQ", nq_future, FUT_SOURCE, URL_FUT, nq_change_abs, nq_change_pct),
    ]:
        rows.append(
            builder.make(
                asset,
                "price",
                series[-1],
                unit="pt",
                window="1D",
                change_abs=change_abs,
                change_pct=change_pct,
                source=source,
                quality="primary",
                url=url,
            )
        )

    rows.append(
        builder.make(
            "ES",
            "basis",
            es_basis,
            unit="ratio",
            window="spot",
            source=FUT_SOURCE,
            quality="primary",
            url=URL_FUT,
        )
    )
    rows.append(
        builder.make(
            "NQ",
            "basis",
            nq_basis,
            unit="ratio",
            window="spot",
            source=FUT_SOURCE,
            quality="primary",
            url=URL_FUT,
        )
    )

    for asset, series in [("S&P500", spx_spot), ("NDX", ndx_spot), ("SOX", sox_index)]:
        change_abs, change_pct = _change(series)
        rows.append(
            builder.make(
                asset,
                "spot",
                series[-1],
                unit="pt",
                window="1D",
                change_abs=change_abs,
                change_pct=change_pct,
                source=SPOT_SOURCE,
                quality="primary",
                url=URL_SPOT,
            )
        )
        for window, periods in [("1W", 5), ("1M", 21)]:
            ret = compute.compute_simple_return(series, periods)
            rows.append(
                builder.make(
                    asset,
                    f"return_{window.lower()}",
                    ret,
                    unit="pct",
                    window=window,
                    source=SPOT_SOURCE,
                    quality="primary",
                    url=URL_SPOT,
                )
            )

    return rows
