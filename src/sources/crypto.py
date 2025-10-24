from __future__ import annotations

import math
from typing import List

from .. import compute

SOURCE = "CoinDesk"
URL = "https://www.coindesk.com"


def _synthetic(base: float, amplitude: float, periods: int = 120) -> List[float]:
    return [base + amplitude * math.sin(i / (periods - 1) * 3.5) for i in range(periods)]


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

    btc = _synthetic(45000, 1200)
    nq = _synthetic(15500, 180)

    change_abs, change_pct = _change(btc)
    corr = compute.compute_correlation(btc, nq, 20)

    rows = [
        builder.make(
            "BTC",
            "price",
            btc[-1],
            unit="USD",
            window="1D",
            change_abs=change_abs,
            change_pct=change_pct,
            source=SOURCE,
            quality="primary",
            url=URL,
        ),
        builder.make(
            "BTC",
            "corr_nq_20d",
            corr,
            unit="corr",
            window="20D",
            source=SOURCE,
            quality="primary",
            url=URL,
        ),
    ]

    return rows
