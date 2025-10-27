from __future__ import annotations

import math
from typing import Dict, List, Tuple

from .. import compute, utils

SOURCE = "Yahoo Finance"
URL_TEMPLATE = "https://finance.yahoo.com/quote/{ticker}"


def _synthetic_series(base: float, amplitude: float, periods: int = 120) -> List[float]:
    return [base + amplitude * math.sin(i / (periods - 1) * 4) for i in range(periods)]


def _load_series(ticker: str, *, period: str = "6mo", limit: int = 200) -> Tuple[List[float], Dict[str, object]]:
    payload = utils.yahoo_close_series(ticker, period=period, limit=limit)
    return list(payload["series"]), payload


def fetch(phase: str, tz: str) -> dict[str, object]:
    ts_kst = compute.ts_now(tz)
    builder = compute.RecordBuilder(ts_kst)

    rows: list[dict[str, object]] = []
    debug_payload: Dict[str, object] = {}

    tickers = {
        "USD/KRW": "KRW=X",
        "DXY": "DX-Y.NYB",
        "KOSPI": "^KS11",
    }

    series_map: dict[str, List[float]] = {}
    notes_map: dict[str, str] = {}
    sources: dict[str, str] = {}

    for asset, ticker in tickers.items():
        notes = "잠정(secondary)"
        source = SOURCE
        try:
            series, raw = _load_series(ticker)
            debug_payload[ticker] = raw
        except Exception as exc:  # pragma: no cover - network fallback
            base = 1300 if asset == "USD/KRW" else 100 if asset == "DXY" else 2500
            amplitude = 7 if asset == "USD/KRW" else 1.5 if asset == "DXY" else 25
            series = _synthetic_series(base, amplitude)
            source = "synthetic-fallback"
            notes = f"fallback: {type(exc).__name__}"
            debug_payload[f"{ticker}_error"] = {"message": str(exc)}
        series_map[asset] = series
        notes_map[asset] = notes
        sources[asset] = source

    usdkrw_series = series_map["USD/KRW"]
    dxy_series = series_map["DXY"]
    kospi_series = series_map["KOSPI"]

    change_abs, change_pct = utils.series_change(usdkrw_series)
    rows.append(
        builder.make(
            "USD/KRW",
            "spot",
            usdkrw_series[-1] if usdkrw_series else None,
            unit="KRW",
            window="1D",
            change_abs=change_abs,
            change_pct=change_pct,
            source=sources["USD/KRW"],
            quality="secondary",
            url=URL_TEMPLATE.format(ticker=tickers["USD/KRW"]),
            notes=notes_map["USD/KRW"],
        )
    )

    for window, periods in (("1D", 1), ("5D", 5)):
        vol = compute.compute_realized_vol(usdkrw_series, periods)
        rows.append(
            builder.make(
                "USD/KRW",
                f"vol_{window.lower()}",
                vol,
                unit="vol",
                window=window,
                source=sources["USD/KRW"],
                quality="secondary",
                url=URL_TEMPLATE.format(ticker=tickers["USD/KRW"]),
                notes=notes_map["USD/KRW"],
            )
        )

    corr_fx = compute.compute_correlation(usdkrw_series, kospi_series, 20)
    rows.append(
        builder.make(
            "USD/KRW",
            "corr_kospi_20d",
            corr_fx,
            unit="corr",
            window="20D",
            source=sources["USD/KRW"],
            quality="secondary",
            url=URL_TEMPLATE.format(ticker=tickers["USD/KRW"]),
            notes=notes_map["USD/KRW"],
        )
    )

    dxy_change_abs, dxy_change_pct = utils.series_change(dxy_series)
    rows.append(
        builder.make(
            "DXY",
            "idx",
            dxy_series[-1] if dxy_series else None,
            unit="idx",
            window="1D",
            change_abs=dxy_change_abs,
            change_pct=dxy_change_pct,
            source=sources["DXY"],
            quality="secondary",
            url=URL_TEMPLATE.format(ticker=tickers["DXY"]),
            notes=notes_map["DXY"],
        )
    )

    corr_dxy = compute.compute_correlation(dxy_series, kospi_series, 20)
    rows.append(
        builder.make(
            "DXY",
            "corr_kospi_20d",
            corr_dxy,
            unit="corr",
            window="20D",
            source=sources["DXY"],
            quality="secondary",
            url=URL_TEMPLATE.format(ticker=tickers["DXY"]),
            notes=notes_map["DXY"],
        )
    )

    return {"rows": rows, "debug": debug_payload}
