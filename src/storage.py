from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

from .utils import ensure_dir, iso_ts

RAW_DIR = Path("raw")
OUT_DIR = Path("out")
DAILY_DIR = OUT_DIR / "daily"
LOG_DIR = OUT_DIR / "logs"
DEBUG_DIR = OUT_DIR / "debug"


def write_raw(asset: str, phase: str, frame: pd.DataFrame) -> Path:
    date_str = datetime.now().strftime("%Y%m%d")
    path = RAW_DIR / asset / f"{date_str}_{phase}.parquet"
    ensure_dir(path.parent)
    frame.to_parquet(path, index=False)
    return path


def _to_dataframe(rows: Iterable[Dict]) -> pd.DataFrame:
    frame = pd.DataFrame(list(rows))
    if frame.empty:
        return pd.DataFrame(columns=[
            "ts_kst",
            "asset",
            "key",
            "value",
            "unit",
            "window",
            "change_abs",
            "change_pct",
            "source",
            "quality",
            "url",
            "notes",
        ])
    return frame


def write_latest(rows: Iterable[Dict]) -> Path:
    frame = _to_dataframe(rows)
    ensure_dir(OUT_DIR)
    path = OUT_DIR / "latest.csv"
    frame.to_csv(path, index=False)
    return path


def write_daily(rows: Iterable[Dict], ts: datetime) -> Path:
    frame = _to_dataframe(rows)
    ensure_dir(DAILY_DIR)
    date_str = ts.strftime("%Y%m%d")
    path = DAILY_DIR / f"{date_str}.csv"
    frame.to_csv(path, index=False)
    return path


def cleanup_daily(retention_days: int = 180) -> None:
    files = sorted(DAILY_DIR.glob("*.csv"))
    if len(files) <= retention_days:
        return
    excess = files[:-retention_days]
    for target in excess:
        target.unlink(missing_ok=True)


def append_log(date: datetime, event: str, payload: Dict) -> None:
    ensure_dir(LOG_DIR)
    path = LOG_DIR / f"runner_{date.strftime('%Y%m%d')}.json"
    record = {"event": event, "ts": iso_ts(date), **payload}
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def write_debug(name: str, html: str) -> Path:
    ensure_dir(DEBUG_DIR)
    path = DEBUG_DIR / f"{name}.html"
    with path.open("w", encoding="utf-8") as fh:
        fh.write(html)
    return path
