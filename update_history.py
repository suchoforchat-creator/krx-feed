from __future__ import annotations

import argparse
import csv
import os
from collections import OrderedDict
from datetime import datetime
from typing import Iterable, List, Dict, Any

QUALITY_PRIORITY = {"final": 3, "primary": 2, "secondary": 1}
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

VALUE_MAPPING = {
    ("KOSPI", "idx"): "kospi",
    ("KOSDAQ", "idx"): "kosdaq",
    ("KOSPI", "advance"): "kospi_adv",
    ("KOSPI", "decline"): "kospi_dec",
    ("KOSPI", "unchanged"): "kospi_unch",
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
    ("Brent", "curve_M1"): "brent",
    ("Gold", "price"): "gold",
    ("Copper", "price"): "copper",
    ("BTC", "price"): "btc",
    ("KOSPI200", "hv30"): "k200_hv30",
}


def _load_detail_rows(path: str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    with open(path, "r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        return [dict(row) for row in reader]


def _quality_rank(value: str | None) -> int:
    if value is None:
        return 0
    return QUALITY_PRIORITY.get(value, 0)


def _parse_ts(ts_value: str | None) -> datetime:
    if not ts_value:
        return datetime.min
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(ts_value, fmt)
        except ValueError:
            continue
    return datetime.min


def _format_number(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return ""
        value = value.replace(",", "")
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if number != number:  # NaN check
        return ""
    return format(number, "g")


def build_snapshot_row(rows: Iterable[Dict[str, Any]]) -> Dict[str, str]:
    candidates = list(rows)
    if not candidates:
        raise ValueError("No rows available for snapshot")
    required_fields = {"asset", "key", "value"}
    if not required_fields.issubset(candidates[0].keys()):
        raise ValueError("Latest snapshot must contain detailed schema rows")
    result = OrderedDict((column, "") for column in AGG_COLUMNS)

    best_ts = max((_parse_ts(row.get("ts_kst")) for row in candidates), default=datetime.min)
    if best_ts is datetime.min:
        raise ValueError("Invalid timestamps in latest snapshot")
    result["time_kst"] = best_ts.strftime("%Y-%m-%d %H:%M")

    by_key: Dict[tuple[str, str], Dict[str, Any]] = {}
    for row in candidates:
        asset = row.get("asset", "")
        key = row.get("key", "")
        mapping = VALUE_MAPPING.get((asset, key))
        if not mapping:
            continue
        existing = by_key.get((asset, key))
        quality = _quality_rank(row.get("quality"))
        ts_value = _parse_ts(row.get("ts_kst"))
        if existing is None:
            by_key[(asset, key)] = {"row": row, "quality": quality, "ts": ts_value}
            continue
        if quality > existing["quality"] or (
            quality == existing["quality"] and ts_value >= existing["ts"]
        ):
            by_key[(asset, key)] = {"row": row, "quality": quality, "ts": ts_value}

    for (asset, key), payload in by_key.items():
        column = VALUE_MAPPING[(asset, key)]
        value = payload["row"].get("value")
        result[column] = _format_number(value)

    sources = sorted({row.get("source", "") for row in candidates if row.get("source")})
    result["src_tag"] = "|".join(sources) if sources else ""
    result["quality"] = "final"
    return result


def _read_history(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        return [dict(row) for row in reader]


def _write_csv(path: str, rows: List[Dict[str, str]], header: List[str]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: row.get(col, "") for col in header})


def _date_key(row: Dict[str, str]) -> str:
    timestamp = row.get("time_kst", "")
    if not timestamp:
        return ""
    return timestamp.split(" ")[0]


def upsert_history(history_path: str, new_row: Dict[str, str], *, days: int) -> List[Dict[str, str]]:
    if days <= 0:
        raise ValueError("days must be positive")
    records = _read_history(history_path)
    by_date: Dict[str, Dict[str, str]] = {}
    for row in records:
        date_key = _date_key(row)
        if not date_key:
            continue
        by_date[date_key] = {col: row.get(col, "") for col in AGG_COLUMNS}
    date_key = _date_key(new_row)
    if not date_key:
        raise ValueError("new row missing time_kst")
    by_date[date_key] = {col: new_row.get(col, "") for col in AGG_COLUMNS}
    ordered_dates = sorted(by_date.keys())
    trimmed = ordered_dates[-days:]
    updated = [by_date[date] for date in trimmed]
    _write_csv(history_path, updated, AGG_COLUMNS)
    return updated


def update_daily_index(daily_dir: str, index_path: str, *, limit: int = 180) -> None:
    if limit <= 0:
        raise ValueError("limit must be positive")
    if not os.path.isdir(daily_dir):
        return
    entries: List[Dict[str, str]] = []
    repository = os.getenv("GITHUB_REPOSITORY")
    base_url = None
    if repository:
        base_url = f"https://raw.githubusercontent.com/{repository}/main/out/daily/"
    files = sorted(
        [
            name
            for name in os.listdir(daily_dir)
            if name.endswith(".csv") and len(name) >= 12
        ],
        reverse=True,
    )
    for name in files[:limit]:
        date_token = name.split(".")[0]
        try:
            parsed = datetime.strptime(date_token, "%Y%m%d")
        except ValueError:
            continue
        formatted_date = parsed.strftime("%Y-%m-%d")
        url = base_url + name if base_url else os.path.join("out", "daily", name)
        entries.append({"date_kst": formatted_date, "url": url})
    if not entries and not os.path.exists(index_path):
        return
    _write_csv(index_path, entries, ["date_kst", "url"])


def write_latest(latest_path: str, row: Dict[str, str]) -> None:
    _write_csv(latest_path, [row], AGG_COLUMNS)


def process(latest_path: str, history_path: str, *, days: int, index_path: str | None = None) -> None:
    detail_rows = _load_detail_rows(latest_path)
    snapshot = build_snapshot_row(detail_rows)
    write_latest(latest_path, snapshot)
    upsert_history(history_path, snapshot, days=days)
    if index_path is not None:
        daily_dir = os.path.join(os.path.dirname(index_path), "..", "daily")
        daily_dir = os.path.normpath(daily_dir)
        update_daily_index(daily_dir, index_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update history snapshots")
    parser.add_argument("--latest", required=True, help="Path to latest detailed CSV")
    parser.add_argument("--history", required=True, help="Path to consolidated history CSV")
    parser.add_argument("--days", type=int, default=90, help="Number of days to keep in history")
    parser.add_argument(
        "--index",
        dest="index_path",
        default=None,
        help="Optional path to write daily index CSV",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    index_path = args.index_path
    if index_path is None:
        out_dir = os.path.dirname(os.path.abspath(args.latest))
        index_path = os.path.join(out_dir, "daily", "index.csv")
    process(args.latest, args.history, days=args.days, index_path=index_path)


if __name__ == "__main__":
    main()
