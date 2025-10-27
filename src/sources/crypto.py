from __future__ import annotations

import math
from typing import Dict, List, Tuple

from .. import compute, utils

SOURCE = "Yahoo Finance"
URL_TEMPLATE = "https://finance.yahoo.com/quote/{ticker}"


def _synthetic(base: float, amplitude: float, periods: int = 120) -> List[float]:
    return [base + amplitude * math.sin(i / (periods - 1) * 3.5) for i in range(periods)]


def _change(series: List[float]) -> tuple[float | None, float | None]:
    return utils.series_change(series)


def _load_series(ticker: str, *, limit: int = 200) -> Tuple[List[float], Dict[str, object]]:
    payload = utils.yahoo_close_series(ticker, period="6mo", limit=limit)
    return list(payload["series"]), payload


def fetch(phase: str, tz: str) -> dict[str, object]:
    ts_kst = compute.ts_now(tz)
    builder = compute.RecordBuilder(ts_kst)

    rows: list[dict[str, object]] = []
    debug_payload: Dict[str, object] = {}

    btc_series: List[float]
    btc_source = SOURCE
    btc_notes = "잠정(secondary)"
    try:
        btc_series, raw = _load_series("BTC-USD")
        debug_payload["BTC-USD"] = raw
    except Exception as exc:  # pragma: no cover - network fallback
        btc_series = _synthetic(45000, 1200)
        btc_source = "synthetic-fallback"
        btc_notes = f"fallback: {type(exc).__name__}"
        debug_payload["BTC-USD_error"] = {"message": str(exc)}

    nq_series: List[float]
    nq_source = SOURCE
    try:
        nq_series, raw_nq = _load_series("^NDX")
        debug_payload["^NDX"] = raw_nq
    except Exception as exc:  # pragma: no cover - network fallback
        nq_series = _synthetic(15500, 180)
        nq_source = "synthetic-fallback"
        debug_payload["^NDX_error"] = {"message": str(exc)}

    change_abs, change_pct = _change(btc_series)

    rows.append(
        builder.make(
            "BTC",
            "price",
            btc_series[-1] if btc_series else None,
            unit="USD",
            window="1D",
            change_abs=change_abs,
            change_pct=change_pct,
            source=btc_source,
            quality="secondary",
            url=URL_TEMPLATE.format(ticker="BTC-USD"),
            notes=btc_notes,
        )
    )

    corr = compute.compute_correlation(btc_series, nq_series, 20) if btc_series and nq_series else None
    corr_notes = ""
    if nq_source != SOURCE:
        corr_notes = "NQ series fallback"

    rows.append(
        builder.make(
            "BTC",
            "corr_nq_20d",
            corr,
            unit="corr",
            window="20D",
            source=btc_source if nq_source == SOURCE else "mixed",
            quality="secondary",
            url=URL_TEMPLATE.format(ticker="^NDX"),
            notes=corr_notes or btc_notes,
        )
    )

    return {"rows": rows, "debug": debug_payload}
