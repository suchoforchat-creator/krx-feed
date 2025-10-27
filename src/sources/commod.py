from __future__ import annotations

import math
from typing import Dict, List, Tuple

from .. import compute, utils

SOURCE = "Yahoo Finance"
URL_TEMPLATE = "https://finance.yahoo.com/quote/{ticker}"


def _synthetic(base: float, amplitude: float, periods: int = 90) -> List[float]:
    return [base + amplitude * math.sin(i / (periods - 1) * 2.2) for i in range(periods)]


def _change(series: List[float]) -> tuple[float | None, float | None]:
    return utils.series_change(series)


def _load_series(ticker: str, *, period: str = "6mo", limit: int = 200) -> Tuple[List[float], Dict[str, object]]:
    payload = utils.yahoo_close_series(ticker, period=period, limit=limit)
    return list(payload["series"]), payload


def fetch(phase: str, tz: str) -> dict[str, object]:
    ts_kst = compute.ts_now(tz)
    builder = compute.RecordBuilder(ts_kst)

    rows: list[dict[str, object]] = []
    debug_payload: Dict[str, object] = {}

    assets = {
        "Gold": {"ticker": "GC=F", "unit": "USD/oz", "fallback": (1950, 15)},
        "WTI": {"ticker": "CL=F", "unit": "USD/bbl", "fallback": (78, 2.5)},
        "Copper": {"ticker": "HG=F", "unit": "USD/lb", "fallback": (4.1, 0.08)},
    }

    for asset, meta in assets.items():
        ticker = meta["ticker"]
        unit = meta["unit"]
        base, amplitude = meta["fallback"]
        source = SOURCE
        notes = "잠정(secondary)"
        try:
            series, raw = _load_series(ticker)
            debug_payload[ticker] = raw
        except Exception as exc:  # pragma: no cover - network fallback
            series = _synthetic(base, amplitude)
            source = "synthetic-fallback"
            notes = f"fallback: {type(exc).__name__}"
            debug_payload[f"{ticker}_error"] = {"message": str(exc)}

        change_abs, change_pct = _change(series)
        rows.append(
            builder.make(
                asset,
                "price",
                series[-1] if series else None,
                unit=unit,
                window="1D",
                change_abs=change_abs,
                change_pct=change_pct,
                source=source,
                quality="secondary",
                url=URL_TEMPLATE.format(ticker=ticker),
                notes=notes,
            )
        )

    brent_tickers = {
        "curve_M1": "BZ=F",
        "curve_M2": "B0=F",
        "curve_M3": "B1=F",
        "curve_M6": "B2=F",
        "curve_M12": "B3=F",
    }

    front_price: float | None = None
    for key, ticker in brent_tickers.items():
        source = SOURCE
        notes = "잠정(secondary)"
        try:
            series, raw = _load_series(ticker, period="3mo", limit=90)
            debug_payload[ticker] = raw
            value = series[-1] if series else None
            if key == "curve_M1":
                front_price = value
        except Exception as exc:  # pragma: no cover - network fallback
            value = None
            source = "synthetic-fallback"
            notes = f"fallback: {type(exc).__name__}"
            debug_payload[f"{ticker}_error"] = {"message": str(exc)}
        if value is None and front_price is not None:
            value = front_price
            notes = (notes + "; derived from front") if notes else "derived from front"
        elif value is None and front_price is None:
            synthetic_curve = _synthetic(82.5, 1.0)
            value = synthetic_curve[-1]
            notes = "fallback: no brent data"
            source = "synthetic-fallback"
        rows.append(
            builder.make(
                "Brent",
                key,
                value,
                unit="USD/bbl",
                window=key.split("_")[-1],
                source=source,
                quality="secondary",
                url=URL_TEMPLATE.format(ticker=ticker),
                notes=notes,
            )
        )

    return {"rows": rows, "debug": debug_payload}
