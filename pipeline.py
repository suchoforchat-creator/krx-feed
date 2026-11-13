from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import shutil
from tempfile import TemporaryDirectory
from datetime import datetime, date
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd
import requests
import yfinance as yf  # VIX 0순위 소스로 사용

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
from src.session_dates import completed_session, is_session
from src.session_inputs import history_raw_as_of
from src.source_dates import bind_note, quote_day
from src.failure_diagnostic import write_failure, write_history_input_review


def mark_eod(frame: pd.DataFrame) -> pd.DataFrame:
    """Use the same mapping as history; never override it with stale aliases."""
    updated = frame.copy()
    if "window" not in updated:
        updated["window"] = ""
    mask = [(a, k) in update_history.LATEST_TO_HISTORY
            for a, k in zip(updated.get("asset", []), updated.get("key", []))]
    updated.loc[mask, "window"] = "EOD"
    return updated


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="KRX feed pipeline")
    parser.add_argument("--phase", required=True, choices=["0730", "1700", "EOD", "recovery"])
    parser.add_argument("--tz", default="Asia/Seoul")
    parser.add_argument("--reconcile", action="store_true")
    parser.add_argument("--expected-target-date", default="")
    parser.add_argument("--expected-observation-date", default="")
    return parser.parse_args()


def _store_raw(asset: str, phase: str, frame: pd.DataFrame) -> None:
    safe_name = asset.lower().replace("/", "_")
    write_raw(safe_name, phase, frame)


# ---------------------------------------------------------------------------
# VIX 수집 유틸리티
# ---------------------------------------------------------------------------

UA_HDR = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
}


def _now_kst_str() -> str:
    """KST 기준 현재 시각을 문자열(초 단위)로 반환합니다."""

    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def _http_get(url: str, *, timeout: int = 10, headers: Dict[str, str] | None = None) -> requests.Response:
    """공통 GET 래퍼: UA를 기본으로 넣고 HTTP 오류를 즉시 표시합니다."""

    response = requests.get(url, timeout=timeout, headers=headers or UA_HDR)
    # 초심자 팁: raise_for_status()로 4xx/5xx를 바로 예외로 전환하여
    #           다음 소스로 빠르게 폴백할 수 있습니다.
    response.raise_for_status()
    return response


def _fail(tried: list[tuple[str, str]], name: str, reason: str) -> None:
    """시도한 소스와 실패 사유를 기록하는 헬퍼."""

    # 실패 메시지는 너무 길면 notes 컬럼을 넘칠 수 있으니 80자로 자릅니다.
    tried.append((name, (reason or "")[:80]))


def _http_json(url: str, *, timeout: int = 10, headers: Dict[str, str] | None = None) -> Dict:
    """JSON 응답을 바로 파싱하는 헬퍼."""

    return _http_get(url, timeout=timeout, headers=headers).json()


def _rec(
    asset: str,
    key: str,
    value: float | None,
    unit: str,
    *,
    window: str = "1D",
    source: str = "",
    quality: str = "",
    url: str = "",
    notes: str = "",
    source_date: date | None = None,
) -> Dict[str, object]:
    """latest.csv 한 행을 쉽게 만들기 위한 도우미."""
    observed = datetime.now(KST)
    notes = bind_note(notes, source_date)
    if source_date is None:
        notes += ";freshness=SOURCE_DATE_UNKNOWN"
    else:
        age = (observed.date() - source_date).days
        notes += ";source_age_calendar_days=" + str(age)
        notes += ";freshness=" + ("PRIOR_DATE_REFERENCE" if age > 0 else "SOURCE_DATE_MATCHES_COLLECTION_DATE")
        if age < 0:
            value = None
            notes += ";future_source_date_rejected"
    return {
        "ts_kst": observed.strftime("%Y-%m-%d %H:%M:%S"),
        "asset": asset,
        "key": key,
        "value": value,
        "unit": unit,
        "window": window,
        "change_abs": None,
        "change_pct": None,
        "source": source,
        "quality": quality,
        "url": url,
        "notes": notes,
    }


def fetch_vix() -> Dict[str, object]:
    """VIX를 다중 소스에서 순차적으로 시도해 한 개 레코드로 반환합니다."""

    tried: list[tuple[str, str]] = []

    # 0) yfinance (^VIX)
    # - GitHub Actions 환경에서 비교적 안정적인 공개 종가 소스입니다.
    # - `history(period="2d")`를 써서 최신 값이 NaN일 때 바로 이전 영업일을 대신 사용합니다.
    try:
        ticker = yf.Ticker("^VIX")
        hist = ticker.history(period="2d", interval="1d")

        if not hist.empty and "Close" in hist.columns:
            closes = pd.to_numeric(hist["Close"], errors="coerce").dropna()
            value = float(closes.iloc[-1])
            if 8 <= value <= 150:
                return _rec(
                    "VIX",
                    "spot",
                    value,
                    "pt",
                    source="yfinance",
                    quality="secondary",
                    url="https://finance.yahoo.com/quote/%5EVIX",
                    source_date=quote_day(closes.index[-1]),
                )
            _fail(tried, "yfinance", f"out_of_range:{value}")
        else:
            _fail(tried, "yfinance", "empty_history_or_no_close")
    except Exception as exc:  # pragma: no cover - 네트워크/파싱 실패 대비
        _fail(tried, "yfinance", f"{type(exc).__name__}:{exc}")

    # ① Cboe 공식 JSON
    try:
        url = "https://cdn.cboe.com/api/global/us_indices/indicators/VIX"
        hdr = UA_HDR | {
            "Accept": "application/json",
            "Referer": "https://www.cboe.com/",
            "Origin": "https://www.cboe.com",
        }
        payload = _http_json(url, timeout=12, headers=hdr)
        last_value = None
        if isinstance(payload, dict):
            data = payload.get("data") or []
            if isinstance(data, list) and data:
                entry = data[0]
                last_value = entry.get("last") or entry.get("value")
        if last_value is not None and 8 <= float(last_value) <= 150:
            return _rec("VIX", "spot", float(last_value), "pt", source="cboe", quality="final", url=url)
        _fail(tried, "cboe", "no_last_value")
    except Exception as exc:  # pragma: no cover - 네트워크/파싱 실패 대비
        _fail(tried, "cboe", f"{type(exc).__name__}:{exc}")

    # ①-2 Cboe quotes JSON(공식 보조 엔드포인트)
    try:
        url = "https://cdn.cboe.com/api/global/us_indices/quotes/VIX.json"
        hdr = UA_HDR | {
            "Accept": "application/json",
            "Referer": "https://www.cboe.com/indices/",
        }
        payload = _http_json(url, timeout=12, headers=hdr)
        last_value = None
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, dict) and "VIX" in data:
                last_value = data["VIX"].get("last")
            elif isinstance(data, list) and data:
                last_value = data[0].get("last")
        if last_value is not None and 8 <= float(last_value) <= 150:
            return _rec("VIX", "spot", float(last_value), "pt", source="cboe_quotes", quality="final", url=url)
        _fail(tried, "cboe_quotes", "no_last_value")
    except Exception as exc:  # pragma: no cover
        _fail(tried, "cboe_quotes", f"{type(exc).__name__}:{exc}")

    # ② Yahoo Finance JSON API
    try:
        url = "https://query1.finance.yahoo.com/v7/finance/quote?symbols=%5EVIX"
        payload = _http_json(url, timeout=10, headers=UA_HDR)
        result = (payload.get("quoteResponse", {}) or {}).get("result", [])
        if result:
            price = result[0].get("regularMarketPrice")
            if price is not None and 8 <= float(price) <= 150:
                return _rec("VIX", "spot", float(price), "pt", source="yahoo_quote", quality="secondary", url=url)
        _fail(tried, "yahoo_quote", "no_regularMarketPrice")
    except Exception as exc:  # pragma: no cover
        _fail(tried, "yahoo_quote", f"{type(exc).__name__}:{exc}")

    # ②-2 Yahoo Finance HTML
    try:
        url = "https://finance.yahoo.com/quote/%5EVIX"
        html = _http_get(url, timeout=10).text
        match = re.search(r'"regularMarketPrice":\s*\{"raw":\s*([0-9.]+)', html)
        if match:
            value = float(match.group(1))
            if 8 <= value <= 150:
                return _rec("VIX", "spot", value, "pt", source="yahoo_html", quality="secondary", url=url)
        _fail(tried, "yahoo_html", "pattern_not_found")
    except Exception as exc:  # pragma: no cover
        _fail(tried, "yahoo_html", f"{type(exc).__name__}:{exc}")

    # ③ Stooq CSV
    try:
        url = "https://stooq.com/q/d/l/?s=vix&i=d"
        df = pd.read_csv(url)
        columns = {c.lower(): c for c in df.columns}
        close_col = columns.get("close") or columns.get("zamkniecie")
        if close_col:
            value = float(df.tail(1)[close_col].iloc[0])
            if 8 <= value <= 150:
                return _rec("VIX", "spot", value, "pt", source="stooq", quality="secondary", url=url,
                            source_date=quote_day(df.tail(1)[columns["date"]].iloc[0]) if "date" in columns else None)
            _fail(tried, "stooq", f"out_of_range:{value}")
        else:
            _fail(tried, "stooq", f"no_close_col:{list(df.columns)[:5]}")
    except Exception as exc:  # pragma: no cover
        _fail(tried, "stooq", f"{type(exc).__name__}:{exc}")

    # ④ MarketWatch HTML
    try:
        url = "https://www.marketwatch.com/investing/index/vix"
        html = _http_get(url, timeout=10, headers=UA_HDR | {"Referer": "https://www.marketwatch.com/"}).text
        match = re.search(r'<bg-quote[^>]*class="[^"]*value[^"]*"[^>]*>([0-9.]+)</bg-quote>', html)
        match = match or re.search(r'"instrument-price-last"\s*:\s*"([0-9.]+)"', html)
        match = match or re.search(r'"price"\s*:\s*([0-9.]+)', html)
        if match:
            value = float(match.group(1))
            if 8 <= value <= 150:
                return _rec(
                    "VIX",
                    "spot",
                    value,
                    "pt",
                    source="marketwatch",
                    quality="secondary",
                    url=url,
                )
        _fail(tried, "marketwatch", "pattern_not_found")
    except Exception as exc:  # pragma: no cover
        _fail(tried, "marketwatch", f"{type(exc).__name__}:{exc}")

    # ⑤ TradingView HTML
    try:
        url = "https://www.tradingview.com/symbols/TVC-VIX/"
        html = _http_get(url, timeout=10).text
        match = re.search(r'__NEXT_DATA__"\s*type="application/json">\s*({.*})\s*</script>', html)
        if match:
            import json as _json

            data = _json.loads(match.group(1))
            # 페이지 구조 변화에 대비해 JSON 전체를 문자열로 변환해 숫자 패턴을 찾습니다.
            blob = str(data)
            price_match = re.search(r'"lp"\s*:\s*([0-9.]+)', blob) or re.search(r'"last"\s*:\s*([0-9.]+)', blob) or re.search(r'"price"\s*:\s*([0-9.]+)', blob)
            if price_match:
                value = float(price_match.group(1))
                if 8 <= value <= 150:
                    return _rec("VIX", "spot", value, "pt", source="tradingview", quality="secondary", url=url)
        _fail(tried, "tradingview", "pattern_not_found")
    except Exception as exc:  # pragma: no cover
        _fail(tried, "tradingview", f"{type(exc).__name__}:{exc}")

    # 모든 시도가 실패하면 값 없이 실패 노트를 남깁니다.
    note = "parse_failed:" + ("|".join([f"{src}:{msg}" for src, msg in tried]) or "no_source_matched")
    urls = ";".join([src for src, _ in tried]) or "none"
    return _rec("VIX", "spot", None, "pt", source="all_sources_failed", notes=note, url=urls)


def collect_raw(config: Dict, phase: str, *, run_ts=None, target_date=None) -> Tuple[Dict[str, pd.DataFrame], Dict[str, str], Dict[str, list[str]]]:
    client = KISClient(config)
    universe = load_universe(config)
    raw_frames: Dict[str, pd.DataFrame] = {}
    failure_notes: Dict[str, str] = {}
    metrics: Dict[str, list[str]] = {}
    run_ts = run_ts or datetime.now(KST)
    target_date = target_date or completed_session(run_ts, phase)
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


def checked_target(args, ts):
    """A manual expectation constrains the live clock; it never relabels data."""
    if args.phase not in {"0730", "1700", "EOD", "recovery"}:
        raise ValueError("UNSUPPORTED_PHASE")
    if getattr(args, "tz", "Asia/Seoul") != "Asia/Seoul":
        raise ValueError("UNSUPPORTED_TIMEZONE")
    target = completed_session(ts, args.phase)
    expected = getattr(args, "expected_target_date", "")
    observed = getattr(args, "expected_observation_date", "")
    if args.phase == "recovery" and (not expected or not observed):
        raise ValueError("RECOVERY_DATE_BINDING_REQUIRED")
    for text, actual in ((expected, target), (observed, ts.date())):
        if text:
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
                raise ValueError("EXACT_DATE_FORMAT_REQUIRED")
            if date.fromisoformat(text) != actual:
                raise ValueError("EXPECTED_DATE_MISMATCH")
    if args.phase == "recovery" and not is_session(ts.date()):
        raise ValueError("RECOVERY_OBSERVATION_SESSION_REQUIRED")
    return target


def _same_history_content(previous: Path, candidate: Path) -> bool:
    """Reuse an identical validated bucket without refreshing its observation time.

    Both inputs include every historical row. Only the newly observed timestamp
    is ignored; values, metric source dates, quality and all other lineage stay
    part of the comparison. This is not an alternative to source validation.
    """
    if not previous.exists():
        return False
    frames = [pd.read_csv(path, dtype=str, keep_default_na=False)
              for path in (previous, candidate)]
    for frame in frames:
        if list(frame.columns) != update_history.HISTORY_COLUMNS:
            return False
        frame["src_tag"] = frame["src_tag"].map(
            lambda text: "|".join(token for token in text.split("|")
                                  if not token.startswith("observation_time=")))
    return frames[0].equals(frames[1])


def _validate_existing_history_header(path: Path) -> None:
    """Do not let the legacy loader silently repair a damaged published schema."""
    if not path.exists():
        return
    with path.open(encoding="utf-8-sig", newline="") as handle:
        header = next(csv.reader(handle, strict=True), None)
    if (header != update_history.HISTORY_COLUMNS
            or len(header) != len(set(header))):
        raise ValueError("HISTORY_SCHEMA_MISMATCH")


def update_completed_history(raw_frames, notes, vix_record, records, ts, target_date, phase):
    """Use the already collected raw series for one strictly verified session.

    Morning and closing runs share this path. A temporary directory isolates
    failed history validation from both the preserved history and latest output.
    The caller alone decides whether a known morning source delay is partial.
    """
    debug_dir = Path("debug") / ("0730" if phase == "0730" else "1700")
    history_raw, selection = history_raw_as_of(raw_frames, target_date)
    history_records = compute.compute_records(ts, history_raw, notes)
    history_records.append(dict(vix_record))
    history_frame = mark_eod(pd.DataFrame(history_records))
    write_history_input_review(debug_dir / "history_inputs.json", ts,
                               target_date, records, history_records, selection)
    history_path = Path("out/history.csv")
    _validate_existing_history_header(history_path)
    with TemporaryDirectory(prefix="krx-history-") as staging:
        staging = Path(staging)
        history_input = staging / "history_input.csv"
        staged_history = staging / "history.csv"
        history_frame.to_csv(history_input, index=False)
        if history_path.exists():
            shutil.copyfile(history_path, staged_history)
        report = update_history.upsert_from_latest(
            history_input, staged_history, debug_dir=debug_dir,
            now=ts, target_date=target_date, require_source_dates=True)
        if not staged_history.exists() or staged_history.stat().st_size == 0:
            raise ValueError("HISTORY_OUTPUT_MISSING")
        unchanged = _same_history_content(history_path, staged_history)
        if not unchanged:
            history_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(staged_history, history_path)
    result = {"history_status": "UNCHANGED_VERIFIED" if unchanged else "UPDATED_VERIFIED",
              "history_target": target_date.isoformat()}
    print("[history-upsert]", json.dumps({**result, "steps": report.steps,
                                          "field_status": report.field_status}, ensure_ascii=False))
    return result


def main() -> int:
    args = parse_args()
    config = load_yaml(Path("conf.yml"))

    ts = datetime.now(KST)
    append_log(ts, "start", {"phase": args.phase})

    try:
        target_date = checked_target(args, ts)
        raw_frames, notes, metrics = collect_raw(config, args.phase, run_ts=ts, target_date=target_date)
        append_log(ts, "raw", {"assets": list(raw_frames)})
        if metrics.get("symbol_not_found"):
            append_log(ts, "monitor", {"symbol_not_found": metrics["symbol_not_found"]})
        records = compute.compute_records(ts, raw_frames, notes)

        # VIX는 다른 지표와 동일하게 latest.csv에 남겨 history 업서트에도 활용합니다.
        # 네트워크 상태에 따라 어떤 소스가 성공했는지 쉽게 확인할 수 있도록 notes/source를 그대로 기록합니다.
        vix_record = fetch_vix()
        records.append(vix_record)

        coverage = compute.check_coverage(records)
        append_log(ts, "coverage", {"ratio": coverage})

        if coverage < 0.8:
            append_log(ts, "warning", {"reason": "coverage", "ratio": coverage})

        # Compare the preserved prior observation, not the file just written.
        daily_path = Path("out/daily") / (ts.strftime("%Y%m%d") + ".csv")
        if args.reconcile:
            records = reconcile.reconcile(records, daily_path)

        # Morning's same raw inputs can repair a delayed previous close without
        # another collection or schedule. Dates/values are never relabelled.
        try:
            history_result = update_completed_history(
                raw_frames, notes, vix_record, records, ts, target_date, args.phase)
        except ValueError as exc:
            # Only the known missing core source-date outcome is a morning
            # partial result. Schema errors, corruption and closing/recovery
            # failures still stop publication rather than masquerade as success.
            if args.phase != "0730" or str(exc) not in {
                    "SOURCE_DATE_NOT_READY:KOSPI", "SOURCE_DATE_NOT_READY:KOSDAQ"}:
                raise
            evidence = write_failure(Path("debug/0730/history_failure.json"), exc, "morning_history")
            history_result = {"history_status": "WAITING_SOURCE",
                              "history_target": target_date.isoformat(),
                              "history_reason_code": evidence["reason_code"]}
            append_log(ts, "warning", {"phase": args.phase, **history_result})
            print("[history-upsert] PARTIAL", json.dumps(history_result, ensure_ascii=False))

        write_latest(records)
        write_daily(records, ts)
        if args.phase != "recovery":
            cleanup_daily()
        event = "partial_success" if history_result["history_status"] == "WAITING_SOURCE" else "success"
        append_log(ts, event, {"phase": args.phase, "latest_status": "PUBLISHED",
                              "daily_status": "PUBLISHED", **history_result})
        return 0
    except Exception as exc:  # pragma: no cover
        safe_phase = args.phase if args.phase in {"0730", "1700", "EOD", "recovery"} else "unknown"
        evidence = write_failure(Path("debug") / safe_phase / "pipeline_failure.json", exc, "pipeline_main")
        append_log(ts, "failure", {"phase": safe_phase, "error_type": evidence["exception_type"]})
        return 1


if __name__ == "__main__":
    sys.exit(main())
