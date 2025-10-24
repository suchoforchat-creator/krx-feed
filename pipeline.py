from __future__ import annotations

import argparse
import csv
import importlib
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Callable, Dict, List

from src import compute, reconcile, storage, utils

SOURCES: Dict[str, Callable[[str, str], List[dict[str, Any]]]] = {
    "krx": lambda phase, tz: importlib.import_module("src.sources.krx").fetch(phase, tz),
    "global_eq": lambda phase, tz: importlib.import_module("src.sources.global_eq").fetch(phase, tz),
    "fx": lambda phase, tz: importlib.import_module("src.sources.fx").fetch(phase, tz),
    "rates": lambda phase, tz: importlib.import_module("src.sources.rates").fetch(phase, tz),
    "commod": lambda phase, tz: importlib.import_module("src.sources.commod").fetch(phase, tz),
    "crypto": lambda phase, tz: importlib.import_module("src.sources.crypto").fetch(phase, tz),
}

CORE_REQUIREMENTS: set[tuple[str, str]] = {
    ("KOSPI", "idx"),
    ("KOSDAQ", "idx"),
    ("KOSPI", "advance"),
    ("KOSPI", "decline"),
    ("KOSPI", "unchanged"),
    ("KOSDAQ", "advance"),
    ("KOSDAQ", "decline"),
    ("KOSDAQ", "unchanged"),
    ("KOSPI", "trin"),
    ("KOSPI", "limit_up"),
    ("KOSPI", "limit_down"),
    ("KOSPI", "trading_value"),
    ("KOSPI200", "hv30"),
    ("ES", "price"),
    ("ES", "basis"),
    ("NQ", "price"),
    ("NQ", "basis"),
    ("S&P500", "spot"),
    ("S&P500", "return_1w"),
    ("S&P500", "return_1m"),
    ("NDX", "spot"),
    ("NDX", "return_1w"),
    ("NDX", "return_1m"),
    ("SOX", "spot"),
    ("SOX", "return_1w"),
    ("SOX", "return_1m"),
    ("USD/KRW", "spot"),
    ("USD/KRW", "vol_1d"),
    ("USD/KRW", "vol_5d"),
    ("USD/KRW", "corr_kospi_20d"),
    ("DXY", "idx"),
    ("DXY", "corr_kospi_20d"),
    ("UST2Y", "yield"),
    ("UST2Y", "change_1d_bp"),
    ("UST10Y", "yield"),
    ("UST10Y", "change_1d_bp"),
    ("KR3Y", "yield"),
    ("KR3Y", "change_1d_bp"),
    ("KR10Y", "yield"),
    ("KR10Y", "change_1d_bp"),
    ("TIPS10Y", "yield"),
    ("TIPS10Y", "change_1d_bp"),
    ("2s10s", "spread"),
    ("Gold", "price"),
    ("WTI", "price"),
    ("Brent", "curve_M1"),
    ("Brent", "curve_M2"),
    ("Brent", "curve_M3"),
    ("Brent", "curve_M6"),
    ("Brent", "curve_M12"),
    ("Copper", "price"),
    ("BTC", "price"),
    ("BTC", "corr_nq_20d"),
}


def _debug_row(asset: str, phase: str, tz: str, error: Exception) -> dict[str, Any]:
    ts_kst = compute.ts_now(tz)
    builder = compute.RecordBuilder(ts_kst)
    return builder.make(
        asset,
        "error",
        None,
        unit="",
        window="",
        source="system",
        quality="secondary",
        url="",
        notes=f"{type(error).__name__}: {error}",
    )


def _debug_html(error: Exception) -> str:
    return f"<html><body><pre>{error}</pre></body></html>"


def _daily_path_from_ts(ts_kst: str) -> str:
    date = datetime.strptime(ts_kst, utils.TS_FORMAT)
    return os.path.join(storage.DAILY_DIR, f"{date.strftime('%Y%m%d')}.csv")


def _load_daily_rows(path: str) -> List[dict[str, Any]]:
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        return [utils.ensure_schema(row) for row in reader]


def run_pipeline(phase: str, tz: str, reconcile_flag: bool) -> int:
    ts_kst = compute.ts_now(tz)
    required = CORE_REQUIREMENTS
    log_name = datetime.strptime(ts_kst, utils.TS_FORMAT).strftime("runner_%Y%m%d.json")
    log_path = os.path.join(storage.LOG_DIR, log_name)
    results: list[dict[str, Any]] = []

    with utils.json_log_writer(log_path) as events:
        events.append({"phase": phase, "ts_kst": ts_kst, "status": "started"})
        with ThreadPoolExecutor(max_workers=8) as executor:
            future_map = {
                executor.submit(fetch, phase, tz): name for name, fetch in SOURCES.items()
            }
            for future in as_completed(future_map):
                name = future_map[future]
                try:
                    rows = future.result()
                    results.extend(rows)
                    events.append({"source": name, "status": "ok", "count": len(rows)})
                except Exception as exc:  # pragma: no cover - defensive
                    debug_path = storage.write_debug(name, phase, _debug_html(exc))
                    error_row = _debug_row(name.upper(), phase, tz, exc)
                    results.append(error_row)
                    events.append({
                        "source": name,
                        "status": "error",
                        "error": str(exc),
                        "debug": debug_path,
                    })

        coverage = utils.coverage_ratio(results, required)
        events.append({"metric": "coverage", "value": coverage})

        rows_to_write = results
        if reconcile_flag:
            daily_path = _daily_path_from_ts(ts_kst)
            existing = _load_daily_rows(daily_path)
            reconciled = reconcile.reconcile_rows(existing, results, config_path="conf.yml")
            rows_to_write = reconciled
            events.append({"reconcile": True, "rows": len(reconciled)})
        else:
            events.append({"reconcile": False, "rows": len(rows_to_write)})

        storage.write_rows(rows_to_write, latest=True)
        events.append({"status": "completed"})

    if coverage < 0.8:
        return 2
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Market data pipeline")
    parser.add_argument("--phase", required=True, help="Phase identifier, e.g., 0730 or 1700")
    parser.add_argument("--tz", default="Asia/Seoul", help="Timezone name")
    parser.add_argument("--reconcile", action="store_true", help="Run reconciliation logic")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    exit_code = run_pipeline(args.phase, args.tz, args.reconcile)
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
