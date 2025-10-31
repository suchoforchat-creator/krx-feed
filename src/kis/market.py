from __future__ import annotations

from typing import Dict

import pandas as pd

from .client import KISClient


def index_series(client: KISClient, name: str, periods: int = 120) -> pd.DataFrame:
    frame = client.get_index_series(name, periods)
    frame["asset"] = name
    frame["field"] = "close"
    frame["unit"] = "pt"
    frame["source"] = "KIS" if client.use_live else "KIS-sim"
    frame["quality"] = "primary"
    frame["url"] = client.config.get("kis", {}).get("base_url", "")
    return frame


def fx_series(client: KISClient, name: str, periods: int = 120) -> pd.DataFrame:
    frame = client.get_fx_series(name, periods)
    frame["asset"] = "USD/KRW"
    frame["field"] = "close"
    frame["unit"] = "krw"
    frame["source"] = "KIS" if client.use_live else "KIS-sim"
    frame["quality"] = "primary"
    frame["url"] = client.config.get("kis", {}).get("base_url", "")
    return frame


def futures_series(client: KISClient, name: str, periods: int = 120, alias: str | None = None, unit: str = "pt") -> pd.DataFrame:
    frame = client.get_futures_series(name, periods)
    frame["asset"] = alias or name
    frame["field"] = "close"
    frame["unit"] = unit
    frame["source"] = "KIS" if client.use_live else "KIS-sim"
    frame["quality"] = "primary"
    frame["url"] = client.config.get("kis", {}).get("base_url", "")
    return frame


def equity_snapshots(client: KISClient, universe: pd.DataFrame) -> pd.DataFrame:
    snap = client.get_equity_universe(universe)
    return snap


def kor_yields(client: KISClient) -> pd.DataFrame:
    frame = client.get_kor_yields()
    frame["source"] = "KIS" if client.use_live else "KIS-sim"
    frame["quality"] = "primary"
    frame["url"] = client.config.get("kis", {}).get("base_url", "")
    return frame
