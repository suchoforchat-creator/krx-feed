from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict

import pandas as pd

from src import compute, reconcile
from src.kis import KISClient, breadth, market
from src.sources import commod_crypto
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


def collect_raw(config: Dict, phase: str) -> Dict[str, pd.DataFrame]:
    client = KISClient(config)
    universe = load_universe(config)
    raw_frames: Dict[str, pd.DataFrame] = {}

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
        "DX": config.get("futures", {}).get("dx", "DX"),
        "UST2Y": config.get("futures", {}).get("ust2y", "ZT"),
        "UST5Y": config.get("futures", {}).get("ust5y", "ZF"),
        "UST10Y": config.get("futures", {}).get("ust10y", "ZN"),
    }
    for alias, symbol in futures_map.items():
        frame = market.futures_series(client, symbol, alias=alias)
        raw_frames[alias] = frame
        _store_raw(alias, phase, frame)

    snaps = market.equity_snapshots(client, universe)
    stats = breadth.adv_dec_unch(snaps)
    now = datetime.now(KST)
    for market_name in ["kospi", "kosdaq"]:
        asset = market_name.upper()
        for field, series in stats.items():
            value = float(series.get(market_name, 0))
            unit = "krw_bn" if "value" in field else "count"
            frame = pd.DataFrame(
                {
                    "ts_kst": [now],
                    "asset": [asset],
                    "field": [field],
                    "value": [value],
                    "unit": [unit],
                    "source": ["KIS" if client.use_live else "KIS-sim"],
                    "quality": ["primary"],
                    "url": [config.get("kis", {}).get("base_url", "")],
                }
            )
            existing = raw_frames.get(asset)
            raw_frames[asset] = pd.concat([existing, frame], ignore_index=True) if existing is not None else frame
    turnover_value = float(snaps["value_traded"].sum())
    turnover_frame = pd.DataFrame(
        {
            "ts_kst": [now],
            "asset": ["KOSPI"],
            "field": ["turnover"],
            "value": [turnover_value],
            "unit": ["krw_bn"],
            "source": ["KIS" if client.use_live else "KIS-sim"],
            "quality": ["primary"],
            "url": [config.get("kis", {}).get("base_url", "")],
        }
    )
    raw_frames["KOSPI"] = pd.concat([raw_frames["KOSPI"], turnover_frame], ignore_index=True)
    _store_raw("KOSPI", phase, raw_frames["KOSPI"])
    _store_raw("KOSDAQ", phase, raw_frames["KOSDAQ"])

    kor_yields = market.kor_yields(client)
    for col, asset in [("kr3y", "KR3Y"), ("kr10y", "KR10Y")]:
        frame = pd.DataFrame(
            {
                "ts_kst": kor_yields["ts_kst"],
                "asset": asset,
                "field": "yield",
                "value": kor_yields[col],
                "unit": "bp",
                "source": kor_yields["source"],
                "quality": kor_yields["quality"],
                "url": kor_yields["url"],
            }
        )
        raw_frames[asset] = frame
        _store_raw(asset, phase, frame)

    def proxy(frame: pd.DataFrame, scale: float, base: float, alias: str) -> pd.DataFrame:
        values = frame["value"].astype(float)
        proxy_values = base + (values - values.mean()) * scale
        out = pd.DataFrame(
            {
                "ts_kst": frame["ts_kst"],
                "asset": alias,
                "field": "yield",
                "value": proxy_values,
                "unit": "bp",
                "source": frame["source"],
                "quality": frame["quality"],
                "url": frame["url"],
            }
        )
        return out

    raw_frames["UST2Y"] = proxy(raw_frames["UST2Y"], 0.8, 4.5, "UST2Y")
    raw_frames["UST10Y"] = proxy(raw_frames["UST10Y"], 0.6, 4.2, "UST10Y")
    _store_raw("UST2Y", phase, raw_frames["UST2Y"])
    _store_raw("UST10Y", phase, raw_frames["UST10Y"])

    commodities = commod_crypto.fetch()
    for asset, frame in commodities.items():
        raw_frames[asset] = frame
        _store_raw(asset, phase, frame)

    return raw_frames


def main() -> int:
    args = parse_args()
    config = load_yaml(Path("conf.yml"))

    ts = datetime.now(KST)
    append_log(ts, "start", {"phase": args.phase})

    try:
        raw_frames = collect_raw(config, args.phase)
        append_log(ts, "raw", {"assets": list(raw_frames)})
        records = compute.compute_records(ts, raw_frames)
        coverage = compute.check_coverage(records)
        append_log(ts, "coverage", {"ratio": coverage})
        if coverage < 0.8:
            write_latest(records)
            write_daily(records, ts)
            append_log(ts, "failure", {"reason": "coverage"})
            return 2

        latest_path = write_latest(records)
        daily_path = write_daily(records, ts)
        cleanup_daily()

        if args.reconcile:
            reconciled = reconcile.reconcile(records, daily_path)
            write_latest(reconciled)
            write_daily(reconciled, ts)

        append_log(ts, "success", {"phase": args.phase})
        return 0
    except Exception as exc:  # pragma: no cover
        append_log(ts, "failure", {"error": str(exc)})
        return 1


if __name__ == "__main__":
    sys.exit(main())
