from __future__ import annotations

import csv
import os
from datetime import datetime
from typing import Iterable

from . import utils

OUT_DIR = "out"
LATEST_PATH = os.path.join(OUT_DIR, "latest.csv")
DAILY_DIR = os.path.join(OUT_DIR, "daily")
LOG_DIR = os.path.join(OUT_DIR, "logs")
DEBUG_DIR = os.path.join(OUT_DIR, "debug")
RETENTION_DAYS = 180


def _ensure_directories() -> None:
    for path in [OUT_DIR, DAILY_DIR, LOG_DIR, DEBUG_DIR]:
        os.makedirs(path, exist_ok=True)


def _parse_date(ts_kst: str) -> datetime:
    return datetime.strptime(ts_kst, utils.TS_FORMAT)


def _daily_path(ts_kst: str) -> str:
    date_str = _parse_date(ts_kst).strftime("%Y%m%d")
    return os.path.join(DAILY_DIR, f"{date_str}.csv")


def _write_csv(path: str, rows: list[dict[str, object]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=utils.SCHEMA_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _load_existing(path: str) -> list[dict[str, object]]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        return [utils.ensure_schema(row) for row in reader]


def _retention_cleanup() -> None:
    files = sorted(
        [
            os.path.join(DAILY_DIR, f)
            for f in os.listdir(DAILY_DIR)
            if f.endswith(".csv")
        ]
    )
    if len(files) <= RETENTION_DAYS:
        return
    for path in files[:-RETENTION_DAYS]:
        try:
            os.remove(path)
        except OSError:
            continue


def _maybe_upload_r2(path: str) -> None:
    access_key = os.getenv("R2_ACCESS_KEY")
    secret_key = os.getenv("R2_SECRET_KEY")
    bucket = os.getenv("R2_BUCKET")
    if not (access_key and secret_key and bucket):
        return
    try:
        import boto3
    except Exception:
        return
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("R2_ENDPOINT"),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    key = os.path.relpath(path, OUT_DIR)
    client.upload_file(path, bucket, key)


def _merge_rows(existing: list[dict[str, object]], new_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    combined: dict[tuple[str, str, str], dict[str, object]] = {}
    for row in existing + new_rows:
        key = (
            row.get("asset", ""),
            row.get("key", ""),
            row.get("window", ""),
        )
        current = combined.get(key)
        if current is None or str(current.get("ts_kst")) <= str(row.get("ts_kst")):
            combined[key] = row
    merged = list(combined.values())
    merged.sort(key=lambda r: (r.get("asset", ""), r.get("key", ""), r.get("window", ""), r.get("ts_kst", "")))
    return merged


def write_rows(rows: Iterable[dict[str, object]], *, latest: bool = True) -> None:
    sanitized = utils.sanitize_rows(rows)
    if not sanitized:
        return
    _ensure_directories()
    ts_kst = sanitized[0]["ts_kst"]
    daily_path = _daily_path(ts_kst)
    existing = _load_existing(daily_path)
    merged = _merge_rows(existing, sanitized)
    _write_csv(daily_path, merged)
    if latest:
        _write_csv(LATEST_PATH, merged)
    _maybe_upload_r2(daily_path)
    if latest:
        _maybe_upload_r2(LATEST_PATH)
    _retention_cleanup()


def write_debug(asset: str, phase: str, content: str) -> str:
    _ensure_directories()
    filename = f"{asset}_{phase}.html"
    path = os.path.join(DEBUG_DIR, filename)
    with open(path, "w", encoding="utf-8") as fp:
        fp.write(content)
    return path
