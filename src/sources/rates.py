from __future__ import annotations

from __future__ import annotations

import math
from typing import Dict, List, Tuple

from .. import compute, utils

SOURCE = "Yahoo Finance"
URL_TEMPLATE = "https://finance.yahoo.com/quote/{ticker}"


def _synthetic_series(base: float, amplitude: float, periods: int = 180) -> List[float]:
    return [base + amplitude * math.cos(i / (periods - 1) * 2.5) for i in range(periods)]


def _load_series(ticker: str, *, period: str = "1y", limit: int = 260) -> Tuple[List[float], Dict[str, object]]:
    payload = utils.yahoo_close_series(ticker, period=period, limit=limit)
    return list(payload["series"]), payload


def fetch(phase: str, tz: str) -> dict[str, object]:
    ts_kst = compute.ts_now(tz)
    builder = compute.RecordBuilder(ts_kst)

    rows: list[dict[str, object]] = []
    debug_payload: Dict[str, object] = {}

    tickers = {
        "UST2Y": "^UST2Y",
        "UST10Y": "^TNX",
        "KR3Y": "^KRWGB3Y",
        "KR10Y": "^KRWGB10Y",
        "TIPS10Y": "TIP",
    }

    series_map: dict[str, List[float]] = {}
    notes_map: dict[str, str] = {}
    sources: dict[str, str] = {}

    for asset, ticker in tickers.items():
        source = SOURCE
        notes = "잠정(secondary)"
        try:
            series, raw = _load_series(ticker)
            debug_payload[ticker] = raw
        except Exception as exc:  # pragma: no cover - network fallback
            defaults = {
                "UST2Y": (4.2, 0.05),
                "UST10Y": (4.5, 0.06),
                "KR3Y": (3.4, 0.04),
                "KR10Y": (3.6, 0.05),
                "TIPS10Y": (1.9, 0.03),
            }
            base, amplitude = defaults.get(asset, (3.0, 0.03))
            series = _synthetic_series(base, amplitude)
            source = "synthetic-fallback"
            notes = f"fallback: {type(exc).__name__}"
            debug_payload[f"{ticker}_error"] = {"message": str(exc)}
        series_map[asset] = series
        notes_map[asset] = notes
        sources[asset] = source

    ust2y = series_map["UST2Y"]
    ust10y = series_map["UST10Y"]

    for asset, series in series_map.items():
        change_abs, _ = utils.series_change(series)
        bp_move = compute.compute_bp_change(series)
        rows.append(
            builder.make(
                asset,
                "yield",
                series[-1] if series else None,
                unit="pct",
                window="1D",
                change_abs=change_abs,
                source=sources[asset],
                quality="secondary",
                url=URL_TEMPLATE.format(ticker=tickers[asset]),
                notes=notes_map[asset],
            )
        )
        rows.append(
            builder.make(
                asset,
                "change_1d_bp",
                bp_move,
                unit="bp",
                window="1D",
                source=sources[asset],
                quality="secondary",
                url=URL_TEMPLATE.format(ticker=tickers[asset]),
                notes=notes_map[asset],
            )
        )

    spread = None
    if ust2y and ust10y:
        spread = float((ust10y[-1] - ust2y[-1]) * 100)

    rows.append(
        builder.make(
            "2s10s",
            "spread",
            spread,
            unit="bp",
            window="1D",
            source=sources.get("UST10Y", SOURCE),
            quality="secondary",
            url=URL_TEMPLATE.format(ticker=tickers["UST10Y"]),
            notes=notes_map.get("UST10Y", ""),
        )
    )

    return {"rows": rows, "debug": debug_payload}
