from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List

try:
    import yaml  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    yaml = None

from . import compute


@dataclass
class Thresholds:
    index: float = 0.3
    rates: float = 0.01
    fx: float = 0.05
    commodity: float = 0.1
    hv_corr: float = 0.01

    @classmethod
    def from_file(cls, path: str) -> "Thresholds":
        data = _load_config(path)
        raw = data.get("thresholds", {}) if isinstance(data, dict) else {}
        return cls(
            index=float(raw.get("index", cls.index)),
            rates=float(raw.get("rates", cls.rates)),
            fx=float(raw.get("fx", cls.fx)),
            commodity=float(raw.get("commodity", cls.commodity)),
            hv_corr=float(raw.get("hv_corr", cls.hv_corr)),
        )


def _coerce(value: str):
    value = value.strip()
    if not value:
        return ""
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        if not inner:
            return []
        return [item.strip() for item in inner.split(",") if item.strip()]
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    try:
        return float(value)
    except ValueError:
        return value.strip('"\'')


def _fallback_parse(text: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    current: Dict[str, Any] | None = None
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line or line.lstrip().startswith("#"):
            continue
        if not line.startswith(" "):
            key, _, rest = line.partition(":")
            key = key.strip()
            value = rest.strip()
            if value:
                result[key] = _coerce(value)
                current = None
            else:
                current = {}
                result[key] = current
        else:
            if current is None:
                continue
            sub_line = line.strip()
            sub_key, _, rest = sub_line.partition(":")
            current[sub_key.strip()] = _coerce(rest.strip())
    return result


def _load_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    with open(path, "r", encoding="utf-8") as fp:
        text = fp.read()
    if yaml is not None:
        data = yaml.safe_load(text) or {}
        if isinstance(data, dict):
            return data
        return {}
    return _fallback_parse(text)


def _load_assets(config_path: str) -> Dict[str, List[str]]:
    data = _load_config(config_path)
    assets = data.get("assets", {}) if isinstance(data, dict) else {}
    normalized: Dict[str, List[str]] = {}
    for category, items in assets.items():
        if isinstance(items, list):
            normalized[category] = [str(item) for item in items]
    return normalized


def _asset_category(asset: str, key: str, assets: Dict[str, List[str]]) -> str:
    for category, items in assets.items():
        if asset in items:
            return category
    if "corr" in key.lower() or "hv" in key.lower() or "vol" in key.lower():
        return "hv_corr"
    if "basis" in key.lower():
        return "commodity"
    return "index"


def reconcile_rows(
    existing: List[dict[str, Any]],
    new_rows: list[dict[str, Any]],
    *,
    config_path: str,
) -> list[dict[str, Any]]:
    thresholds = Thresholds.from_file(config_path)
    assets = _load_assets(config_path)
    threshold_map = {
        "index": thresholds.index,
        "rates": thresholds.rates,
        "fx": thresholds.fx,
        "commodity": thresholds.commodity,
        "hv_corr": thresholds.hv_corr,
    }
    promoted = compute.promote_final_quality(new_rows)
    if not existing:
        return promoted

    latest_lookup: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in existing:
        key = (
            row.get("asset", ""),
            row.get("key", ""),
            row.get("window", ""),
        )
        current = latest_lookup.get(key)
        if current is None or str(current.get("ts_kst")) <= str(row.get("ts_kst")):
            latest_lookup[key] = row
    for row in promoted:
        asset = row["asset"]
        key = row["key"]
        window = row.get("window", "")
        prev = latest_lookup.get((asset, key, window))
        if prev is None:
            continue
        prev_value = prev.get("value")
        new_value = row.get("value")
        if prev_value is None or new_value is None:
            continue
        category = _asset_category(asset, key, assets)
        threshold = threshold_map.get(category, thresholds.index)
        if abs(float(new_value) - float(prev_value)) >= threshold:
            row["notes"] = "revised"
    return promoted
