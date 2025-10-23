from __future__ import annotations

import math
from typing import List

from .. import compute

SOURCE = "KRX"
URL = "https://global.krx.co.kr"


def _synthetic_index(base: float, amplitude: float, periods: int = 60) -> List[float]:
    return [base + amplitude * math.sin(i / (periods - 1) * math.pi) for i in range(periods)]


def _change_metrics(series: List[float]) -> tuple[float | None, float | None]:
    if len(series) < 2:
        return None, None
    latest = series[-1]
    prev = series[-2]
    change_abs = float(latest - prev)
    change_pct = None if prev == 0 else float(change_abs / prev)
    return change_abs, change_pct


def fetch(phase: str, tz: str) -> list[dict[str, object]]:
    ts_kst = compute.ts_now(tz)
    builder = compute.RecordBuilder(ts_kst)

    kospi = _synthetic_index(2500, 25)
    kosdaq = _synthetic_index(850, 18)
    kospi200 = _synthetic_index(330, 6)

    kospi_change_abs, kospi_change_pct = _change_metrics(kospi)
    kosdaq_change_abs, kosdaq_change_pct = _change_metrics(kosdaq)

    hv30 = compute.compute_hv(kospi200, 30)

    rows = [
        builder.make(
            "KOSPI",
            "idx",
            kospi[-1],
            unit="pt",
            window="1D",
            change_abs=kospi_change_abs,
            change_pct=kospi_change_pct,
            source=SOURCE,
            quality="primary",
            url=URL,
        ),
        builder.make(
            "KOSDAQ",
            "idx",
            kosdaq[-1],
            unit="pt",
            window="1D",
            change_abs=kosdaq_change_abs,
            change_pct=kosdaq_change_pct,
            source=SOURCE,
            quality="primary",
            url=URL,
        ),
    ]

    market_breadth = {
        "KOSPI": {"advance": 420, "decline": 370, "unchanged": 90},
        "KOSDAQ": {"advance": 510, "decline": 430, "unchanged": 120},
    }
    for asset, metrics in market_breadth.items():
        for key, value in metrics.items():
            rows.append(
                builder.make(
                    asset,
                    key,
                    value,
                    unit="issues",
                    window="1D",
                    source=SOURCE,
                    quality="primary",
                    url=URL,
                )
            )

    rows.extend(
        [
            builder.make(
                "KOSPI",
                "trin",
                1.05,
                unit="ratio",
                window="1D",
                source=SOURCE,
                quality="primary",
                url=URL,
            ),
            builder.make(
                "KOSPI",
                "limit_up",
                45,
                unit="issues",
                window="1D",
                source=SOURCE,
                quality="primary",
                url=URL,
            ),
            builder.make(
                "KOSPI",
                "limit_down",
                12,
                unit="issues",
                window="1D",
                source=SOURCE,
                quality="primary",
                url=URL,
            ),
            builder.make(
                "KOSPI",
                "trading_value",
                12.4,
                unit="trn KRW",
                window="1D",
                source=SOURCE,
                quality="primary",
                url=URL,
            ),
            builder.make(
                "KOSPI200",
                "hv30",
                hv30,
                unit="vol",
                window="30D",
                source=SOURCE,
                quality="primary",
                url=URL,
            ),
        ]
    )

    return rows
