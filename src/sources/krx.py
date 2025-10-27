from __future__ import annotations

import math
from typing import Any, Dict, List

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from .. import compute, utils

POLLING_URL = "https://polling.finance.naver.com/api/realtime"
INDEX_PAGE = "https://finance.naver.com/sise/sise_index.naver?code={code}"
USER_AGENT = {"User-Agent": "Mozilla/5.0"}


def _synthetic_index(base: float, amplitude: float, periods: int = 60) -> List[float]:
    return [base + amplitude * math.sin(i / (periods - 1) * math.pi) for i in range(periods)]


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def _fetch_index_snapshot(code: str) -> Dict[str, Any]:
    resp = requests.get(
        POLLING_URL,
        params={"query": f"SERVICE_INDEX:{code}"},
        headers=USER_AGENT,
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    areas = data.get("result", {}).get("areas", [])
    if not areas:
        raise ValueError(f"no snapshot data for {code}")
    entry = areas[0].get("datas", [{}])[0]
    if not entry:
        raise ValueError(f"empty data payload for {code}")
    return entry


def _index_metrics(entry: Dict[str, Any]) -> tuple[float, float | None, float | None, float | None]:
    value = float(entry.get("nv", 0)) / 100
    change_abs = float(entry.get("cv", 0)) / 100 if entry.get("cv") is not None else None
    change_pct = float(entry.get("cr")) / 100 if entry.get("cr") is not None else None
    trading_value = entry.get("aa")
    if trading_value is not None:
        trading_value = float(trading_value) / 1000_000  # convert to trillion KRW
    return value, change_abs, change_pct, trading_value


def fetch(phase: str, tz: str) -> dict[str, object]:
    ts_kst = compute.ts_now(tz)
    builder = compute.RecordBuilder(ts_kst)

    debug_payload: Dict[str, Any] = {}
    rows: list[dict[str, object]] = []

    synthetic_map = {
        "KOSPI": (2500, 25),
        "KOSDAQ": (850, 18),
    }

    for asset, code in (("KOSPI", "KOSPI"), ("KOSDAQ", "KOSDAQ")):
        try:
            snapshot = _fetch_index_snapshot(code)
            debug_payload[asset] = snapshot
            price, change_abs, change_pct, trading_value = _index_metrics(snapshot)
            rows.append(
                builder.make(
                    asset,
                    "idx",
                    price,
                    unit="pt",
                    window="1D",
                    change_abs=change_abs,
                    change_pct=change_pct,
                    source="Naver Finance",
                    quality="secondary",
                    url=INDEX_PAGE.format(code=code),
                    notes="잠정(secondary)",
                )
            )
            if trading_value is not None and asset == "KOSPI":
                rows.append(
                    builder.make(
                        "KOSPI",
                        "trading_value",
                        trading_value,
                        unit="trn KRW",
                        window="1D",
                        source="Naver Finance",
                        quality="secondary",
                        url=INDEX_PAGE.format(code=code),
                        notes="잠정(secondary)",
                    )
                )
        except Exception as exc:  # pragma: no cover - network fallback
            base, amplitude = synthetic_map[asset]
            synthetic = _synthetic_index(base, amplitude)
            change_abs, change_pct = utils.series_change(synthetic)
            rows.append(
                builder.make(
                    asset,
                    "idx",
                    synthetic[-1],
                    unit="pt",
                    window="1D",
                    change_abs=change_abs,
                    change_pct=change_pct,
                    source="synthetic-fallback",
                    quality="secondary",
                    url="",
                    notes=f"fallback: {type(exc).__name__}",
                )
            )
            debug_payload[f"{asset}_error"] = {"message": str(exc)}

    breadth = {
        "KOSPI": {"advance": 0, "decline": 0, "unchanged": 0},
        "KOSDAQ": {"advance": 0, "decline": 0, "unchanged": 0},
    }
    debug_payload["breadth"] = {
        "note": "KRX breadth endpoints require credentials; emitting placeholders",
    }
    for asset, metrics in breadth.items():
        for key, value in metrics.items():
            rows.append(
                builder.make(
                    asset,
                    key,
                    value,
                    unit="issues",
                    window="1D",
                    source="unavailable",
                    quality="secondary",
                    url="",
                    notes="placeholder - 데이터 수동 확인 필요",
                )
            )

    rows.extend(
        [
            builder.make(
                "KOSPI",
                "trin",
                0.0,
                unit="ratio",
                window="1D",
                source="unavailable",
                quality="secondary",
                url="",
                notes="placeholder - 추후 실데이터 연결 필요",
            ),
            builder.make(
                "KOSPI",
                "limit_up",
                0,
                unit="issues",
                window="1D",
                source="unavailable",
                quality="secondary",
                url="",
                notes="placeholder",
            ),
            builder.make(
                "KOSPI",
                "limit_down",
                0,
                unit="issues",
                window="1D",
                source="unavailable",
                quality="secondary",
                url="",
                notes="placeholder",
            ),
        ]
    )

    kospi200 = _synthetic_index(330, 6)
    hv30 = compute.compute_hv(kospi200, 30)
    rows.append(
        builder.make(
            "KOSPI200",
            "hv30",
            hv30,
            unit="vol",
            window="30D",
            source="synthetic-fallback",
            quality="secondary",
            url="",
            notes="placeholder",
        )
    )

    return {"rows": rows, "debug": debug_payload}
