from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")


def _parse_numeric(value: str) -> Any:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    normalized = text.replace(",", "")
    try:
        number = float(normalized)
    except ValueError:
        return value
    if number.is_integer():
        return int(number)
    return round(number, 4)


def _coerce_row(row: dict[str, str]) -> dict[str, Any]:
    coerced: dict[str, Any] = {}
    for key, value in row.items():
        if key == "time_kst":
            if value:
                dt = datetime.strptime(value, "%Y-%m-%d %H:%M").replace(tzinfo=KST)
                coerced["ts_kst"] = dt.isoformat()
            else:
                coerced["ts_kst"] = None
        else:
            coerced[key] = _parse_numeric(value)
    return coerced


def _convert_history_row(row: dict[str, str]) -> dict[str, Any]:
    converted: dict[str, Any] = {}
    for key, value in row.items():
        if key == "time_kst":
            converted[key] = value
        else:
            converted[key] = _parse_numeric(value)
    return converted


def build_json(
    latest_csv: str | Path,
    history_csv: str | Path,
    latest_json: str | Path,
    history_json: str | Path,
) -> None:
    latest_path = Path(latest_csv)
    history_path = Path(history_csv)
    latest_payload_path = Path(latest_json)
    history_payload_path = Path(history_json)

    with latest_path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        try:
            latest_row = next(reader)
        except StopIteration:
            latest_data: dict[str, Any] = {}
        else:
            latest_data = _coerce_row(latest_row)

    with history_path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        history_data = [_convert_history_row(row) for row in reader]

    latest_payload_path.write_text(
        json.dumps(latest_data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    history_payload_path.write_text(
        json.dumps(history_data, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
