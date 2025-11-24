from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime
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
        ("VIX", "spot"),
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
) -> Dict[str, object]:
    """latest.csv 한 행을 쉽게 만들기 위한 도우미."""

    return {
        "ts_kst": _now_kst_str(),
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

    # 0) yfinance (^VIX) — GitHub Actions에서 가장 안정적인 1차 소스
    #    초심자 팁: yfinance는 내부적으로 UA/쿠키를 적절히 설정해 주므로 직접 HTML을 파싱하는 것보다 실패 확률이 낮습니다.
    try:
        ticker = yf.Ticker("^VIX")
        # 2일치 일봉을 가져와 가장 최신 종가를 사용합니다.
        hist = ticker.history(period="2d", interval="1d")
        if not hist.empty and "Close" in hist.columns:
            value = float(hist["Close"].iloc[-1])
            if 8 <= value <= 150:
                return _rec(
                    "VIX",
                    "spot",
                    value,
                    "pt",
                    source="yfinance",
                    quality="secondary",
                    url="https://finance.yahoo.com/quote/%5EVIX",
                )
            _fail(tried, "yfinance", f"out_of_range:{value}")
        else:
            _fail(tried, "yfinance", "empty_history_or_no_close")
    except Exception as exc:  # pragma: no cover
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
                return _rec("VIX", "spot", value, "pt", source="stooq", quality="secondary", url=url)
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

        # VIX는 다른 지표와 동일하게 latest.csv에 남겨 history 업서트에도 활용합니다.
        # 네트워크 상태에 따라 어떤 소스가 성공했는지 쉽게 확인할 수 있도록 notes/source를 그대로 기록합니다.
        records.append(fetch_vix())

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

            # 초심자 디버깅 팁: history.csv가 실제로 생성되었는지 즉시 확인하면,
            # 이후 단계에서 "파일이 없어 업서트가 안 됐다" 같은 문제를 빠르게 찾을 수 있습니다.
            from pathlib import Path as _Path

            history_path = _Path("out/history.csv")
            if (not history_path.exists()) or history_path.stat().st_size == 0:
                print("[history-upsert] ERROR: out/history.csv missing or empty")
                # SystemExit(2)를 던지면 GitHub Actions와 로컬 실행 모두 실패로 표시되어,
                # 사용자가 로그를 확인하고 원인을 추적할 수 있습니다.
                raise SystemExit(2)

            print(
                "[history-upsert] OK",
                json.dumps(
                    {
                        "path": str(history_path),
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
