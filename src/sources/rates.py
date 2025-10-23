from __future__ import annotations

import math
from typing import List

from .. import compute

UST_SOURCE = "UST"
BOK_SOURCE = "BOK"
FRED_URL = "https://fred.stlouisfed.org"
BOK_URL = "https://ecos.bok.or.kr"


def _synthetic(base: float, amplitude: float, periods: int = 90) -> List[float]:
    return [base + amplitude * math.cos(i / (periods - 1) * 2.5) for i in range(periods)]


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

    ust2y = _synthetic(4.2, 0.05)
    ust10y = _synthetic(4.5, 0.06)
    kr3y = _synthetic(3.4, 0.04)
    kr10y = _synthetic(3.6, 0.05)
    tips10y = _synthetic(1.9, 0.03)

    rows: list[dict[str, object]] = []

    for asset, series, source, url in [
        ("UST2Y", ust2y, UST_SOURCE, FRED_URL),
        ("UST10Y", ust10y, UST_SOURCE, FRED_URL),
        ("KR3Y", kr3y, BOK_SOURCE, BOK_URL),
        ("KR10Y", kr10y, BOK_SOURCE, BOK_URL),
        ("TIPS10Y", tips10y, UST_SOURCE, FRED_URL),
    ]:
        change_abs, _ = _change(series)
        bp_move = compute.compute_bp_change(series)
        rows.append(
            builder.make(
                asset,
                "yield",
                series[-1],
                unit="pct",
                window="1D",
                change_abs=change_abs,
                source=source,
                quality="primary",
                url=url,
            )
        )
        rows.append(
            builder.make(
                asset,
                "change_1d_bp",
                bp_move,
                unit="bp",
                window="1D",
                source=source,
                quality="primary",
                url=url,
            )
        )

    spread = float((ust10y[-1] - ust2y[-1]) * 100)
    rows.append(
        builder.make(
            "2s10s",
            "spread",
            spread,
            unit="bp",
            window="1D",
            source=UST_SOURCE,
            quality="primary",
            url=FRED_URL,
        )
    )

    return rows
