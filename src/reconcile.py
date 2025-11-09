from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

DEFAULT_THRESHOLDS = {
    "idx": 0.3,
    "spot": 0.1,
    "basis": 0.05,
    "ret_1w": 0.01,
    "ret_1m": 0.01,
    "hv30": 0.1,
    "spot_fx": 0.05,
    "yield": 1.0,
    "2s10s_spread": 5.0,
    "trin": 0.1,
}


def _threshold_for(row: Dict) -> float:
    key = row.get("key", "")
    asset = row.get("asset", "")
    if key in ("idx", "adv", "dec", "unch", "limit_up", "limit_down"):
        return DEFAULT_THRESHOLDS["idx"]
    if asset == "USD/KRW" and key == "spot":
        return DEFAULT_THRESHOLDS["spot_fx"]
    if key in ("basis", "ret_1w", "ret_1m"):
        return DEFAULT_THRESHOLDS.get(key, 0.05)
    if key == "hv30":
        return DEFAULT_THRESHOLDS["hv30"]
    if key in ("2y", "10y", "3y", "10y"):
        return DEFAULT_THRESHOLDS["yield"]
    if key == "spread" and asset in {"2s10s_US", "2s10s_KR"}:
        return DEFAULT_THRESHOLDS["2s10s_spread"]
    if key == "trin":
        return DEFAULT_THRESHOLDS["trin"]
    return DEFAULT_THRESHOLDS.get(key, 0.1)


def reconcile(records: List[Dict], daily_path: Path) -> List[Dict]:
    if not daily_path.exists():
        for row in records:
            row["quality"] = "final"
        return records

    previous = pd.read_csv(daily_path)
    previous.set_index(["asset", "key", "window"], inplace=True)

    for row in records:
        row["quality"] = "final"
        idx = (row.get("asset"), row.get("key"), row.get("window"))
        if idx in previous.index:
            old_value = previous.loc[idx].get("value")
            try:
                old_value = float(old_value)
            except (TypeError, ValueError):
                old_value = None
            new_value = row.get("value")
            if old_value is not None and new_value is not None:
                diff = abs(float(new_value) - float(old_value))
                threshold = _threshold_for(row)
                if diff >= threshold:
                    row["notes"] = "revised"
    return records
