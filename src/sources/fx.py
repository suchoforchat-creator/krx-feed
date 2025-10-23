from __future__ import annotations

import math
from typing import List

from .. import compute

PRIMARY_SOURCE = "BOK"
DXY_SOURCE = "ICE"
PRIMARY_URL = "https://ecos.bok.or.kr"
DXY_URL = "https://www.theice.com"


def _synthetic(base: float, amplitude: float, periods: int = 90) -> List[float]:
    return [base + amplitude * math.sin(i / (periods - 1) * 4) for i in range(periods)]


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

    usdkrw = _synthetic(1300, 7)
    dxy = _synthetic(100, 1.5)
    kospi = _synthetic(2500, 25)

    rows: list[dict[str, object]] = []

    change_abs, change_pct = _change(usdkrw)
    rows.append(
        builder.make(
            "USD/KRW",
            "spot",
            usdkrw[-1],
            unit="KRW",
            window="1D",
            change_abs=change_abs,
            change_pct=change_pct,
            source=PRIMARY_SOURCE,
            quality="primary",
            url=PRIMARY_URL,
        )
    )

    for window, periods in [("1D", 1), ("5D", 5)]:
        vol = compute.compute_realized_vol(usdkrw, periods)
        rows.append(
            builder.make(
                "USD/KRW",
                f"vol_{window.lower()}",
                vol,
                unit="vol",
                window=window,
                source=PRIMARY_SOURCE,
                quality="primary",
                url=PRIMARY_URL,
            )
        )

    corr_fx = compute.compute_correlation(usdkrw, kospi, 20)
    rows.append(
        builder.make(
            "USD/KRW",
            "corr_kospi_20d",
            corr_fx,
            unit="corr",
            window="20D",
            source=PRIMARY_SOURCE,
            quality="primary",
            url=PRIMARY_URL,
        )
    )

    dxy_change_abs, dxy_change_pct = _change(dxy)
    rows.append(
        builder.make(
            "DXY",
            "idx",
            dxy[-1],
            unit="idx",
            window="1D",
            change_abs=dxy_change_abs,
            change_pct=dxy_change_pct,
            source=DXY_SOURCE,
            quality="primary",
            url=DXY_URL,
        )
    )

    corr_dxy = compute.compute_correlation(dxy, kospi, 20)
    rows.append(
        builder.make(
            "DXY",
            "corr_kospi_20d",
            corr_dxy,
            unit="corr",
            window="20D",
            source=DXY_SOURCE,
            quality="primary",
            url=DXY_URL,
        )
    )

    return rows
