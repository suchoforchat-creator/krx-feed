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


def mark_eod(frame: pd.DataFrame) -> pd.DataFrame:
    """1700 배치에서 history 업서트 대상 항목만 window="EOD"로 표기합니다."""

    # 초심자 팁: DataFrame은 항상 복사본을 만들어 수정하면 원본 데이터 손상을 방지할 수 있습니다.
    updated = frame.copy()

    # history.csv에서 요구하는 (asset, key) 목록입니다.
    eod_pairs = {
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
        # 원자재·암호화폐 레코드는 compute 모듈에서 key="spot"으로 생성됩니다.
        ("WTI", "spot"),
        ("Brent", "spot"),
        ("Gold", "spot"),
        ("Copper", "spot"),
        ("BTC", "spot"),
        ("KOSPI200", "hv30"),
    }

    # window 컬럼이 없으면 빈 문자열로 채워 디버깅 시 결측 여부를 쉽게 확인합니다.
    if "window" not in updated.columns:
        updated["window"] = ""
    else:
        updated["window"] = updated["window"].fillna("")

    # (asset, key) 튜플이 EOD 대상인지 판별해 window 값을 "EOD"로 덮어씁니다.
    pairs = list(zip(updated.get("asset", ""), updated.get("key", "")))
    mask = [pair in eod_pairs for pair in pairs]
    updated.loc[mask, "window"] = "EOD"

    return updated


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

        # 17:00 KST 배치에서는 history 업서트를 위해 window="EOD" 플래그를 미리 지정합니다.
        if args.phase in {"1700", "EOD"}:
            records_frame = mark_eod(pd.DataFrame(records))
            records = records_frame.to_dict("records")

        coverage = compute.check_coverage(records)
        append_log(ts, "coverage", {"ratio": coverage})

        latest_path = write_latest(records)
        daily_path = write_daily(records, ts)
        cleanup_daily()

        if coverage < 0.8:
            append_log(ts, "warning", {"reason": "coverage", "ratio": coverage})

        if args.reconcile:
            reconciled = reconcile.reconcile(records, daily_path)
            write_latest(reconciled)
            write_daily(reconciled, ts)

        # 17:00 배치에서는 latest.csv를 기반으로 history.csv를 업서트하고 결과를 JSON으로 출력합니다.
        if args.phase in {"1700", "EOD"}:
            debug_dir = Path("debug") / "1700"
            debug_dir.mkdir(parents=True, exist_ok=True)

            report = update_history.upsert_from_latest(
                latest_path,
                Path("out") / "history.csv",
                debug_dir=debug_dir,
            )
            print(
                "[history-upsert]",
                json.dumps(
                    {
                        "steps": report.steps,
                        "field_status": report.field_status,
                    },
                    ensure_ascii=False,
                ),
            )

            # 초심자 디버깅 팁: history.csv가 비어 있으면 downstream 분석이 모두 실패합니다.
            # 따라서 즉시 파일 존재 여부와 크기를 검사해 문제가 생기면 구체적인 정보를 남깁니다.
            history_path = Path("out") / "history.csv"
            if (not history_path.exists()) or history_path.stat().st_size == 0:
                error_payload = {
                    "reason": "missing_or_empty_history",
                    "history_path": str(history_path),
                    "latest_path": str(latest_path),
                    "timestamp_kst": datetime.now(KST).isoformat(),
                }
                debug_file = debug_dir / "history_upsert_validation_error.json"
                debug_file.write_text(json.dumps(error_payload, ensure_ascii=False, indent=2))
                print("[history-upsert] ERROR:", json.dumps(error_payload, ensure_ascii=False))
                raise SystemExit(2)
            else:
                print(
                    "[history-upsert] OK:",
                    json.dumps(
                        {
                            "history_path": str(history_path),
                            "size": history_path.stat().st_size,
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
