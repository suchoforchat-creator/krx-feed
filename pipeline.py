from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

import update_history
from src import compute, reconcile
from src.kis import KISClient, market
from src.sources import commod_crypto
from src.sources.dxy import DXYCollector
from src.sources.krx_breadth import KRXBreadthCollector, determine_target
from src.sources.kr_rates import KRXKorRates
from src.sources.us_yields import USTYieldCollector
from src.storage import append_log, cleanup_daily, write_daily, write_latest, write_raw
from src.universe import load_universe
from src.utils import KST, load_yaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="KRX feed pipeline")
    parser.add_argument("--phase", required=True)
    parser.add_argument("--tz", default="Asia/Seoul")
    parser.add_argument("--reconcile", action="store_true")
    return parser.parse_args()


def _store_raw(asset: str, phase: str, frame: pd.DataFrame) -> None:
    safe_name = asset.lower().replace("/", "_")
    write_raw(safe_name, phase, frame)


def collect_raw(config: Dict, phase: str) -> Tuple[Dict[str, pd.DataFrame], Dict[str, str], Dict[str, list[str]]]:
    client = KISClient(config)
    universe = load_universe(config)
    raw_frames: Dict[str, pd.DataFrame] = {}
    failure_notes: Dict[str, str] = {}
    metrics: Dict[str, list[str]] = {}
    run_ts = datetime.now(KST)
    target_date, _ = determine_target(run_ts)
    breadth_collector = KRXBreadthCollector()
    rate_collector = KRXKorRates()
    ust_collector = USTYieldCollector()
    dxy_collector = DXYCollector()

    for asset in ["KOSPI", "KOSDAQ", "K200", "SPX", "NDX", "SOX"]:
        frame = market.index_series(client, asset)
        raw_frames[asset] = frame
        _store_raw(asset, phase, frame)

    fx_frame = market.fx_series(client, "USDKRW")
    raw_frames["USD/KRW"] = fx_frame
    _store_raw("USD_KRW", phase, fx_frame)

    futures_map = {
        "ES": config.get("futures", {}).get("es", "ES"),
        "NQ": config.get("futures", {}).get("nq", "NQ"),
    }
    for alias, symbol in futures_map.items():
        unit = "pt"
        frame = market.futures_series(client, symbol, alias=alias, unit=unit)
        raw_frames[alias] = frame
        _store_raw(alias, phase, frame)

    breadth_result = breadth_collector.collect(run_ts)
    for asset, frame in breadth_result.frames.items():
        existing = raw_frames.get(asset)
        if existing is not None and not existing.empty:
            combined = pd.concat([existing, frame], ignore_index=True)
        else:
            combined = frame
        raw_frames[asset] = combined
        _store_raw(asset, phase, combined)
    failure_notes.update(breadth_result.notes)

    rate_result = rate_collector.fetch(target_date)
    for asset, frame in rate_result.frames.items():
        raw_frames[asset] = frame
        _store_raw(asset, phase, frame)
    failure_notes.update(rate_result.notes)

    ust_frames, ust_notes = ust_collector.collect(target_date)
    for asset, frame in ust_frames.items():
        raw_frames[asset] = frame
        _store_raw(asset, phase, frame)
    failure_notes.update(ust_notes)

    dxy_frame, dxy_notes = dxy_collector.collect(target_date)
    if not dxy_frame.empty:
        raw_frames["DXY"] = dxy_frame
        _store_raw("DXY", phase, dxy_frame)
    failure_notes.update(dxy_notes)

    if getattr(client, "symbol_not_found", set()):
        metrics["symbol_not_found"] = sorted(client.symbol_not_found)

    commodities = commod_crypto.fetch()
    for asset, result in commodities.items():
        frame = result.frame
        raw_frames[asset] = frame
        if not frame.empty:
            _store_raw(asset, phase, frame)
        if result.note:
            failure_notes[f"{asset}:spot"] = result.note

    return raw_frames, failure_notes, metrics


def mark_eod(frame: pd.DataFrame) -> pd.DataFrame:
    """필요한 자산/키에 window="EOD" 태그를 붙여 history 업서트 대상임을 표시합니다."""

    if frame.empty:
        # 비어 있는 경우 그대로 반환하면 이후 로직이 자연스럽게 넘어갑니다.
        return frame

    required_columns = {"asset", "key", "window"}
    missing = required_columns.difference(frame.columns)
    if missing:
        # 필수 컬럼이 없다면 디버깅을 위해 그대로 반환하여 후속 단계에서 KeyError가 발생하도록 둡니다.
        return frame

    frame = frame.copy()

    eod_keys = {
        ("KOSPI", "idx"),
        ("KOSDAQ", "idx"),
        ("KOSPI", "advance"),
        ("KOSPI", "decline"),
        ("KOSPI", "unchanged"),
        ("KOSDAQ", "advance"),
        ("KOSDAQ", "decline"),
        ("KOSDAQ", "unchanged"),
        ("USD/KRW", "spot"),
        ("DXY", "idx"),
        ("UST2Y", "yield"),
        ("UST10Y", "yield"),
        ("KR3Y", "yield"),
        ("KR10Y", "yield"),
        ("TIPS10Y", "yield"),
        ("WTI", "price"),
        ("Brent", "curve_M1"),
        ("Gold", "price"),
        ("Copper", "price"),
        ("BTC", "price"),
        ("KOSPI200", "hv30"),
    }

    # (asset, key) 튜플을 만들어 빠르게 필터링합니다.
    mask = frame[["asset", "key"]].apply(lambda row: (row["asset"], row["key"]) in eod_keys, axis=1)
    frame.loc[mask, "window"] = "EOD"
    return frame


def main() -> int:
    args = parse_args()
    config = load_yaml(Path("conf.yml"))

    ts = datetime.now(KST)
    append_log(ts, "start", {"phase": args.phase})

    try:
        raw_frames, notes, metrics = collect_raw(config, args.phase)
        append_log(ts, "raw", {"assets": list(raw_frames)})
        if metrics.get("symbol_not_found"):
            append_log(ts, "monitor", {"symbol_not_found": metrics["symbol_not_found"]})
        records = compute.compute_records(ts, raw_frames, notes)
        if args.phase in {"1700", "EOD"}:
            records_df = pd.DataFrame(records)
            records_df = mark_eod(records_df)
            records = records_df.to_dict("records")
        coverage = compute.check_coverage(records)
        append_log(ts, "coverage", {"ratio": coverage})

        latest_path = write_latest(records)
        daily_path = write_daily(records, ts)
        cleanup_daily()

        if coverage < 0.8:
            append_log(ts, "warning", {"reason": "coverage", "ratio": coverage})

        if args.reconcile:
            reconciled = reconcile.reconcile(records, daily_path)
            if args.phase in {"1700", "EOD"}:
                reconciled_df = pd.DataFrame(reconciled)
                reconciled_df = mark_eod(reconciled_df)
                reconciled = reconciled_df.to_dict("records")
            write_latest(reconciled)
            write_daily(reconciled, ts)

        if args.phase in {"1700", "EOD"}:
            debug_report = update_history.upsert_from_latest(
                "out/latest.csv", "out/history.csv", debug_dir="debug/1700"
            )
            print(
                "[history-upsert]",
                json.dumps(
                    {
                        "steps": debug_report.steps,
                        "field_status": debug_report.field_status,
                    },
                    ensure_ascii=False,
                ),
            )

        append_log(ts, "success", {"phase": args.phase})
        return 0
    except Exception as exc:  # pragma: no cover
        append_log(ts, "failure", {"error": str(exc)})
        return 1


if __name__ == "__main__":
    sys.exit(main())
