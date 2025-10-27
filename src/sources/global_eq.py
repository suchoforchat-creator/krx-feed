from __future__ import annotations

import math
from typing import Dict, List, Tuple

from .. import compute, utils

SOURCE = "Yahoo Finance"
URL_TEMPLATE = "https://finance.yahoo.com/quote/{ticker}"


def _synthetic_series(base: float, amplitude: float, *, periods: int = 120) -> List[float]:
    return [base + amplitude * math.cos(i / (periods - 1) * math.pi) for i in range(periods)]


def _load_series(ticker: str, *, period: str = "6mo", limit: int = 180) -> Tuple[List[float], Dict[str, object]]:
    payload = utils.yahoo_close_series(ticker, period=period, limit=limit)
    return list(payload["series"]), payload


def fetch(phase: str, tz: str) -> dict[str, object]:
    ts_kst = compute.ts_now(tz)
    builder = compute.RecordBuilder(ts_kst)

    rows: list[dict[str, object]] = []
    debug_payload: Dict[str, object] = {}

    series_map: dict[str, List[float]] = {}
    sources: dict[str, str] = {}
    notes_map: dict[str, str] = {}

    tickers = {
        "ES": "ES=F",
        "NQ": "NQ=F",
        "S&P500": "^GSPC",
        "NDX": "^NDX",
        "SOX": "^SOX",
    }

    for asset, ticker in tickers.items():
        source = SOURCE
        notes = "잠정(secondary)"
        try:
            series, raw = _load_series(ticker)
            debug_payload[ticker] = raw
        except Exception as exc:  # pragma: no cover - network fallback
            base = 4500 if asset in {"ES", "S&P500"} else 15500 if asset in {"NQ", "NDX"} else 3800
            amplitude = 60 if asset in {"ES", "S&P500"} else 180 if asset in {"NQ", "NDX"} else 120
            series = _synthetic_series(base, amplitude)
            source = "synthetic-fallback"
            notes = f"fallback: {type(exc).__name__}"
            debug_payload[f"{ticker}_error"] = {"message": str(exc)}
        series_map[asset] = series
        sources[asset] = source
        notes_map[asset] = notes

    es_series = series_map["ES"]
    nq_series = series_map["NQ"]
    spx_series = series_map["S&P500"]
    ndx_series = series_map["NDX"]
    sox_series = series_map["SOX"]

    for asset, series in ("ES", es_series), ("NQ", nq_series):
        change_abs, change_pct = utils.series_change(series)
        rows.append(
            builder.make(
                asset,
                "price",
                series[-1] if series else None,
                unit="pt",
                window="1D",
                change_abs=change_abs,
                change_pct=change_pct,
                source=sources[asset],
                quality="secondary",
                url=URL_TEMPLATE.format(ticker=tickers[asset]),
                notes=notes_map[asset],
            )
        )

    basis_pairs = (("ES", es_series, spx_series), ("NQ", nq_series, ndx_series))
    for asset, fut_series, spot_series in basis_pairs:
        basis = compute.compute_basis(
            fut_series[-1] if fut_series else None,
            spot_series[-1] if spot_series else None,
        )
        rows.append(
            builder.make(
                asset,
                "basis",
                basis,
                unit="ratio",
                window="spot",
                source=sources[asset],
                quality="secondary",
                url=URL_TEMPLATE.format(ticker=tickers[asset]),
                notes=notes_map[asset],
            )
        )

    for asset, series in (
        ("S&P500", spx_series),
        ("NDX", ndx_series),
        ("SOX", sox_series),
    ):
        change_abs, change_pct = utils.series_change(series)
        rows.append(
            builder.make(
                asset,
                "spot",
                series[-1] if series else None,
                unit="pt",
                window="1D",
                change_abs=change_abs,
                change_pct=change_pct,
                source=sources[asset],
                quality="secondary",
                url=URL_TEMPLATE.format(ticker=tickers[asset]),
                notes=notes_map[asset],
            )
        )
        for window, periods in (("1W", 5), ("1M", 21)):
            ret = compute.compute_simple_return(series, periods)
            rows.append(
                builder.make(
                    asset,
                    f"return_{window.lower()}",
                    ret,
                    unit="pct",
                    window=window,
                    source=sources[asset],
                    quality="secondary",
                    url=URL_TEMPLATE.format(ticker=tickers[asset]),
                    notes=notes_map[asset],
                )
            )

    return {"rows": rows, "debug": debug_payload}
