from __future__ import annotations

import csv
import os
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

QUALITY_ORDER = {
    "final": 4,
    "primary": 3,
    "secondary": 2,
    "prelim": 1,
    "preliminary": 1,
}

AGG_COLUMNS = [
    "time_kst",
    "kospi",
    "kosdaq",
    "kospi_adv",
    "kospi_dec",
    "kospi_unch",
    "kosdaq_adv",
    "kosdaq_dec",
    "kosdaq_unch",
    "usdkrw",
    "dxy",
    "ust2y",
    "ust10y",
    "kr3y",
    "kr10y",
    "tips10y",
    "wti",
    "brent",
    "gold",
    "copper",
    "btc",
    "k200_hv30",
    "src_tag",
    "quality",
]


COLUMN_MAP = {
    ("KOSPI", "idx"): "kospi",
    ("KOSPI", "advance"): "kospi_adv",
    ("KOSPI", "decline"): "kospi_dec",
    ("KOSPI", "unchanged"): "kospi_unch",
    ("KOSDAQ", "idx"): "kosdaq",
    ("KOSDAQ", "advance"): "kosdaq_adv",
    ("KOSDAQ", "decline"): "kosdaq_dec",
    ("KOSDAQ", "unchanged"): "kosdaq_unch",
    ("USD/KRW", "spot"): "usdkrw",
    ("DXY", "idx"): "dxy",
    ("UST2Y", "yield"): "ust2y",
    ("UST10Y", "yield"): "ust10y",
    ("KR3Y", "yield"): "kr3y",
    ("KR10Y", "yield"): "kr10y",
    ("TIPS10Y", "yield"): "tips10y",
    ("WTI", "price"): "wti",
    ("Brent", "price"): "brent",
    ("Brent", "curve_M1"): "brent",
    ("Gold", "price"): "gold",
    ("Copper", "price"): "copper",
    ("BTC", "price"): "btc",
    ("KOSPI200", "hv30"): "k200_hv30",
}


class _ValueSlot:
    __slots__ = ("value", "quality", "source", "timestamp")

    def __init__(self, *, value: str, quality: str, source: str, timestamp: datetime) -> None:
        self.value = value
        self.quality = quality
        self.source = source
        self.timestamp = timestamp

    def better_than(self, other: "_ValueSlot" | None) -> bool:
        if other is None:
            return True
        left = QUALITY_ORDER.get(self.quality.lower(), 0)
        right = QUALITY_ORDER.get(other.quality.lower(), 0)
        if left != right:
            return left > right
        return self.timestamp >= other.timestamp


def _parse_timestamp(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%d %H:%M")


def build_snapshot_row(rows: Iterable[dict[str, str]]) -> dict[str, str]:
    slots: dict[str, _ValueSlot | None] = defaultdict(lambda: None)
    sources: set[str] = set()
    best_quality_rank = 0
    snapshot_time: datetime | None = None

    for row in rows:
        if all(col in row for col in AGG_COLUMNS):
            raise ValueError("Aggregated row detected; expected detail rows")
        asset = row.get("asset")
        key = row.get("key")
        value = row.get("value", "")
        if not asset or not key:
            continue
        column = COLUMN_MAP.get((asset, key))
        if column is None:
            continue
        ts_text = row.get("ts_kst") or row.get("time_kst")
        if not ts_text:
            continue
        try:
            timestamp = _parse_timestamp(ts_text)
        except ValueError:
            continue
        quality = row.get("quality", "")
        source = row.get("source", "")
        slot = _ValueSlot(value=value, quality=quality, source=source, timestamp=timestamp)
        if slot.better_than(slots[column]):
            slots[column] = slot
        if snapshot_time is None or timestamp > snapshot_time:
            snapshot_time = timestamp

    result = {col: "" for col in AGG_COLUMNS}
    if snapshot_time is None:
        raise ValueError("No usable rows to build snapshot")
    result["time_kst"] = snapshot_time.strftime("%Y-%m-%d %H:%M")

    for column, slot in slots.items():
        if slot is None:
            continue
        result[column] = slot.value
        sources.add(slot.source)
        rank = QUALITY_ORDER.get(slot.quality.lower(), 0)
        if rank > best_quality_rank:
            best_quality_rank = rank
            result["quality"] = slot.quality
    if not result["quality"]:
        result["quality"] = "secondary"

    if sources:
        result["src_tag"] = "+".join(sorted(s for s in sources if s))
    else:
        result["src_tag"] = ""

    return result


def upsert_history(path: str | Path, row: dict[str, str], days: int) -> None:
    history_path = Path(path)
    rows: list[dict[str, str]] = []
    if history_path.exists():
        with history_path.open("r", encoding="utf-8", newline="") as fp:
            reader = csv.DictReader(fp)
            rows = [dict(item) for item in reader]

    new_rows: list[dict[str, str]] = []
    replaced = False
    for existing in rows:
        if existing.get("time_kst") == row.get("time_kst"):
            if not replaced:
                new_rows.append(row)
                replaced = True
            continue
        new_rows.append(existing)
    if not replaced:
        new_rows.append(row)

    try:
        anchor = _parse_timestamp(row["time_kst"])
    except (ValueError, KeyError):
        anchor = None
    if anchor is not None:
        cutoff = anchor - timedelta(days=days)
        filtered: list[dict[str, str]] = []
        for item in new_rows:
            ts_text = item.get("time_kst")
            try:
                ts_val = _parse_timestamp(ts_text) if ts_text else None
            except ValueError:
                ts_val = None
            if ts_val is None or ts_val >= cutoff:
                filtered.append(item)
        new_rows = filtered

    new_rows.sort(key=lambda r: r.get("time_kst", ""))

    history_path.parent.mkdir(parents=True, exist_ok=True)
    with history_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=AGG_COLUMNS)
        writer.writeheader()
        for item in new_rows:
            writer.writerow({col: item.get(col, "") for col in AGG_COLUMNS})


def _build_daily_index(index_path: Path) -> None:
    daily_dir = index_path.parent
    repo = os.getenv("GITHUB_REPOSITORY", "")
    branch = os.getenv("GITHUB_REF_NAME") or "main"
    entries = []
    for csv_path in sorted(daily_dir.glob("*.csv")):
        if csv_path.name == index_path.name:
            continue
        date_part = csv_path.stem
        if len(date_part) == 8 and date_part.isdigit():
            try:
                date_part = datetime.strptime(date_part, "%Y%m%d").strftime("%Y-%m-%d")
            except ValueError:
                pass
        url = csv_path.as_posix()
        if repo:
            try:
                rel = csv_path.relative_to(Path.cwd())
            except ValueError:
                rel = csv_path
            url = f"https://raw.githubusercontent.com/{repo}/{branch}/{rel.as_posix()}"
        entries.append({"date_kst": date_part, "url": url})

    with index_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=["date_kst", "url"])
        writer.writeheader()
        for row in entries:
            writer.writerow(row)


def process(latest_path: str, history_path: str, *, days: int, index_path: str | None = None) -> None:
    detail_path = Path(latest_path)
    with detail_path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        detail_rows = [row for row in reader]

    snapshot = build_snapshot_row(detail_rows)

    detail_path.parent.mkdir(parents=True, exist_ok=True)
    with detail_path.open("w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=AGG_COLUMNS)
        writer.writeheader()
        writer.writerow({col: snapshot.get(col, "") for col in AGG_COLUMNS})

    upsert_history(history_path, snapshot, days)

    if index_path:
        index_file = Path(index_path)
        index_file.parent.mkdir(parents=True, exist_ok=True)
        _build_daily_index(index_file)
