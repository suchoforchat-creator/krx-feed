from __future__ import annotations

import math
from typing import List

from .. import compute

GOLD_SOURCE = "CME"
OIL_SOURCE = "ICE"
COPPER_SOURCE = "CME"
GOLD_URL = "https://www.cmegroup.com"
OIL_URL = "https://www.theice.com"
COPPER_URL = "https://www.cmegroup.com"


def _synthetic(base: float, amplitude: float, periods: int = 90) -> List[float]:
    return [base + amplitude * math.sin(i / (periods - 1) * 2.2) for i in range(periods)]


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

    gold = _synthetic(1950, 15)
    wti = _synthetic(78, 2.5)
    copper = _synthetic(4.1, 0.08)

    rows: list[dict[str, object]] = []

    for asset, series, source, url, unit in [
        ("Gold", gold, GOLD_SOURCE, GOLD_URL, "USD/oz"),
        ("WTI", wti, OIL_SOURCE, OIL_URL, "USD/bbl"),
        ("Copper", copper, COPPER_SOURCE, COPPER_URL, "USD/lb"),
    ]:
        change_abs, change_pct = _change(series)
        rows.append(
            builder.make(
                asset,
                "price",
                series[-1],
                unit=unit,
                window="1D",
                change_abs=change_abs,
                change_pct=change_pct,
                source=source,
                quality="primary",
                url=url,
            )
        )

    brent_curve = {
        "curve_M1": 82.5,
        "curve_M2": 82.2,
        "curve_M3": 81.9,
        "curve_M6": 81.2,
        "curve_M12": 79.8,
    }
    for key, value in brent_curve.items():
        rows.append(
            builder.make(
                "Brent",
                key,
                value,
                unit="USD/bbl",
                window=key.split("_")[-1],
                source=OIL_SOURCE,
                quality="primary",
                url=OIL_URL,
            )
        )

    return rows
