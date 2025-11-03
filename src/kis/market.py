from __future__ import annotations

from typing import Dict

import pandas as pd

from .client import KISClient


def _inject_defaults(frame: pd.DataFrame, client: KISClient, unit: str, field: str, asset: str) -> pd.DataFrame:
    frame = frame.copy()
    if "asset" not in frame.columns:
        frame["asset"] = asset
    if "field" not in frame.columns:
        frame["field"] = field
    if "unit" not in frame.columns:
        frame["unit"] = unit
    if "source" not in frame.columns:
        frame["source"] = "KIS" if client.use_live else "KIS-fallback"
    if "quality" not in frame.columns:
        frame["quality"] = "primary" if client.use_live else "secondary"
    if "url" not in frame.columns:
        frame["url"] = client.config.get("kis", {}).get("base_url", "")
    return frame


def index_series(client: KISClient, name: str, periods: int = 120) -> pd.DataFrame:
    frame = client.get_index_series(name, periods)
    return _inject_defaults(frame, client, "pt", "close", name)


def fx_series(client: KISClient, name: str, periods: int = 120) -> pd.DataFrame:
    frame = client.get_fx_series(name, periods)
    return _inject_defaults(frame, client, "krw", "close", "USD/KRW")


def futures_series(client: KISClient, name: str, periods: int = 120, alias: str | None = None, unit: str = "pt") -> pd.DataFrame:
    frame = client.get_futures_series(name, periods)
    return _inject_defaults(frame, client, unit, "close", alias or name)


def equity_snapshots(client: KISClient, universe: pd.DataFrame) -> pd.DataFrame:
    snap = client.get_equity_universe(universe)
    return snap


def kor_yields(client: KISClient) -> pd.DataFrame:
    frame = client.get_kor_yields()
    if "source" not in frame.columns:
        frame["source"] = "KIS" if client.use_live else "KIS-fallback"
    if "quality" not in frame.columns:
        frame["quality"] = "primary" if client.use_live else "secondary"
    if "url" not in frame.columns:
        frame["url"] = client.config.get("kis", {}).get("base_url", "")
    return frame
