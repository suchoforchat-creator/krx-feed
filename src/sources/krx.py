from __future__ import annotations

from typing import Any, Dict, List

from .. import compute, utils

SOURCE = "Yahoo Finance"
URL_TEMPLATE = "https://finance.yahoo.com/quote/{ticker}"

INDEX_TICKERS = {
    "KOSPI": "^KS11",
    "KOSDAQ": "^KQ11",
    "KOSPI200": "^KS200",
}

KOSPI_COMPONENTS = [
    "005930.KS",
    "000660.KS",
    "373220.KS",
    "207940.KS",
    "005380.KS",
    "068270.KS",
    "051910.KS",
    "035420.KS",
    "012330.KS",
    "028260.KS",
    "105560.KS",
    "055550.KS",
    "096770.KS",
    "066570.KS",
    "003550.KS",
    "034730.KS",
    "017670.KS",
    "015760.KS",
    "006400.KS",
    "003670.KS",
]

KOSDAQ_COMPONENTS = [
    "035720.KQ",
    "035760.KQ",
    "091990.KQ",
    "196170.KQ",
    "251270.KQ",
    "263750.KQ",
    "241840.KQ",
    "068760.KQ",
    "247540.KQ",
    "086520.KQ",
    "352820.KQ",
    "293490.KQ",
    "365340.KQ",
    "225570.KQ",
    "141080.KQ",
    "035080.KQ",
    "041510.KQ",
    "042000.KQ",
    "078340.KQ",
    "039490.KQ",
]


def _component_metrics(components: List[str], debug_payload: Dict[str, Any]) -> dict[str, Any]:
    advance = decline = unchanged = 0
    adv_volume = dec_volume = 0.0
    limit_up = limit_down = 0
    trading_value = 0.0

    for ticker in components:
        try:
            payload = utils.yahoo_ohlcv(ticker, period="10d", limit=2, fields=("Close", "Volume"))
            debug_payload[ticker] = payload
            closes = payload["fields"].get("Close", [])
            volumes = payload["fields"].get("Volume", [])
            if len(closes) < 2:
                continue
            prev, curr = closes[-2], closes[-1]
            if prev is None or curr is None:
                continue
            diff = curr - prev
            if diff > 0:
                advance += 1
                if len(volumes) >= 1 and volumes[-1] is not None:
                    adv_volume += volumes[-1]
            elif diff < 0:
                decline += 1
                if len(volumes) >= 1 and volumes[-1] is not None:
                    dec_volume += volumes[-1]
            else:
                unchanged += 1
            if prev:
                pct_change = diff / prev
                if pct_change >= 0.3:
                    limit_up += 1
                if pct_change <= -0.3:
                    limit_down += 1
            if len(volumes) >= 1 and volumes[-1] is not None:
                trading_value += curr * volumes[-1]
        except Exception as exc:  # pragma: no cover - network fallback
            debug_payload[f"{ticker}_error"] = {"message": str(exc)}
            continue

    trin = None
    if advance > 0 and decline > 0 and adv_volume > 0 and dec_volume > 0:
        trin = (advance / decline) / (adv_volume / dec_volume)

    return {
        "advance": advance,
        "decline": decline,
        "unchanged": unchanged,
        "limit_up": limit_up,
        "limit_down": limit_down,
        "trading_value_trn": trading_value / 1_000_000_000_000 if trading_value else 0.0,
        "trin": trin,
    }


def fetch(phase: str, tz: str) -> dict[str, object]:
    ts_kst = compute.ts_now(tz)
    builder = compute.RecordBuilder(ts_kst)

    rows: list[dict[str, object]] = []
    debug_payload: Dict[str, Any] = {}

    index_series: dict[str, List[float]] = {}
    notes_map: dict[str, str] = {}
    sources: dict[str, str] = {}

    for asset, ticker in INDEX_TICKERS.items():
        source = SOURCE
        notes = "잠정(secondary)"
        try:
            payload = utils.yahoo_close_series(ticker, period="6mo", limit=120)
            debug_payload[ticker] = payload
            series = list(payload["series"])
        except Exception as exc:  # pragma: no cover - network fallback
            base = 2500 if asset == "KOSPI" else 850 if asset == "KOSDAQ" else 330
            amplitude = 25 if asset in {"KOSPI", "KOSDAQ"} else 6
            series = [base + amplitude * ((i % 2) - 0.5) for i in range(120)]
            source = "synthetic-fallback"
            notes = f"fallback: {type(exc).__name__}"
            debug_payload[f"{ticker}_error"] = {"message": str(exc)}
        index_series[asset] = series
        notes_map[asset] = notes
        sources[asset] = source

    for asset in ("KOSPI", "KOSDAQ"):
        series = index_series[asset]
        change_abs, change_pct = utils.series_change(series)
        rows.append(
            builder.make(
                asset,
                "idx",
                series[-1] if series else None,
                unit="pt",
                window="1D",
                change_abs=change_abs,
                change_pct=change_pct,
                source=sources[asset],
                quality="secondary",
                url=URL_TEMPLATE.format(ticker=INDEX_TICKERS[asset]),
                notes=notes_map[asset],
            )
        )

    kospi_metrics = _component_metrics(KOSPI_COMPONENTS, debug_payload)
    kosdaq_metrics = _component_metrics(KOSDAQ_COMPONENTS, debug_payload)

    for asset, metrics in ("KOSPI", kospi_metrics), ("KOSDAQ", kosdaq_metrics):
        rows.append(
            builder.make(
                asset,
                "advance",
                metrics["advance"],
                unit="issues",
                window="1D",
                source=SOURCE,
                quality="secondary",
                url="https://finance.yahoo.com",
            )
        )
        rows.append(
            builder.make(
                asset,
                "decline",
                metrics["decline"],
                unit="issues",
                window="1D",
                source=SOURCE,
                quality="secondary",
                url="https://finance.yahoo.com",
            )
        )
        rows.append(
            builder.make(
                asset,
                "unchanged",
                metrics["unchanged"],
                unit="issues",
                window="1D",
                source=SOURCE,
                quality="secondary",
                url="https://finance.yahoo.com",
            )
        )

    rows.append(
        builder.make(
            "KOSPI",
            "trading_value",
            kospi_metrics["trading_value_trn"],
            unit="trn KRW",
            window="1D",
            source=SOURCE,
            quality="secondary",
            url="https://finance.yahoo.com",
        )
    )

    rows.append(
        builder.make(
            "KOSPI",
            "trin",
            kospi_metrics["trin"],
            unit="ratio",
            window="1D",
            source=SOURCE,
            quality="secondary",
            url="https://finance.yahoo.com",
            notes="components-derived",
        )
    )

    rows.append(
        builder.make(
            "KOSPI",
            "limit_up",
            kospi_metrics["limit_up"],
            unit="issues",
            window="1D",
            source=SOURCE,
            quality="secondary",
            url="https://finance.yahoo.com",
        )
    )
    rows.append(
        builder.make(
            "KOSPI",
            "limit_down",
            kospi_metrics["limit_down"],
            unit="issues",
            window="1D",
            source=SOURCE,
            quality="secondary",
            url="https://finance.yahoo.com",
        )
    )

    hv30 = compute.compute_hv(index_series["KOSPI200"], 30)
    rows.append(
        builder.make(
            "KOSPI200",
            "hv30",
            hv30,
            unit="vol",
            window="30D",
            source=sources["KOSPI200"],
            quality="secondary",
            url=URL_TEMPLATE.format(ticker=INDEX_TICKERS["KOSPI200"]),
            notes=notes_map["KOSPI200"],
        )
    )

    return {"rows": rows, "debug": debug_payload}
