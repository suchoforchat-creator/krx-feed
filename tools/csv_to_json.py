#!/usr/bin/env python3
"""Convert pipeline CSV artifacts into JSON outputs."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable

import numpy as np
import pandas as pd
from pandas.api.types import is_float_dtype

KST = timezone(timedelta(hours=9))


def _remove_commas(value: Any) -> Any:
    if isinstance(value, str):
        return value.replace(",", "")
    return value


def _coerce_numeric(df: pd.DataFrame) -> pd.DataFrame:
    for column in df.columns:
        series = df[column]
        if series.dtype == object:
            cleaned = series.map(
                lambda value: _remove_commas(value)
                if not (isinstance(value, str) and value.strip() == "")
                else None
            )
        else:
            cleaned = series
        converted = pd.to_numeric(cleaned, errors="coerce")
        if converted.notna().any():
            df[column] = converted
        else:
            df[column] = cleaned
        if is_float_dtype(df[column]):
            df[column] = df[column].round(4)
    return df


def _sanitise_record(items: Iterable[tuple[str, Any]]) -> Dict[str, Any]:
    record: Dict[str, Any] = {}
    for key, value in items:
        if isinstance(value, str) and value.strip() == "":
            record[key] = None
            continue
        if pd.isna(value):
            record[key] = None
            continue
        if isinstance(value, np.generic):
            value = value.item()
        record[key] = value
    return record


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"CSV not found: {path}")
    try:
        df = pd.read_csv(path)
    except Exception as exc:  # pragma: no cover - surface upstream error
        raise RuntimeError(f"Failed to read CSV {path}: {exc}") from exc
    return df


def build_json(latest_path: Path, history_path: Path, latest_json: Path, history_json: Path) -> None:
    latest_df = _coerce_numeric(_load_csv(latest_path))
    history_df = _coerce_numeric(_load_csv(history_path))

    if latest_df.empty:
        raise ValueError(f"Latest CSV is empty: {latest_path}")

    latest_record = _sanitise_record(latest_df.iloc[0].items())
    latest_record["ts_kst"] = datetime.now(KST).isoformat(timespec="seconds")

    history_records = [
        _sanitise_record(row.items()) for _, row in history_df.iterrows()
    ]

    latest_json.parent.mkdir(parents=True, exist_ok=True)
    history_json.parent.mkdir(parents=True, exist_ok=True)

    with latest_json.open("w", encoding="utf-8") as fh:
        json.dump(latest_record, fh, ensure_ascii=False)
        fh.write("\n")

    with history_json.open("w", encoding="utf-8") as fh:
        json.dump(history_records, fh, ensure_ascii=False)
        fh.write("\n")


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert CSV artifacts to JSON.")
    parser.add_argument("--latest", default="out/latest.csv", type=Path)
    parser.add_argument("--history", default="out/history.csv", type=Path)
    parser.add_argument("--latest-json", default="out/latest.json", type=Path)
    parser.add_argument("--history-json", default="out/history.json", type=Path)
    return parser.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(argv)
    try:
        build_json(args.latest, args.history, args.latest_json, args.history_json)
    except Exception as exc:
        print(f"Failed to build JSON artifacts: {exc}", file=sys.stderr)
        raise


if __name__ == "__main__":
    main()
