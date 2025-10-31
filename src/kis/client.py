from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_fixed

from ..utils import synthetic_series


@dataclass
class KISClient:
    config: Dict[str, Any]

    def __post_init__(self) -> None:
        self.mode = self.config.get("kis", {}).get("mode", "simulation")
        self.fixtures = self.config.get("fixtures", {})
        self.token_cache = Path(self.config.get("kis", {}).get("token_cache", "cache/kis_token.json"))
        self.appkey = os.getenv(self.config.get("kis", {}).get("appkey_env", ""), "")
        self.appsecret = os.getenv(self.config.get("kis", {}).get("appsecret_env", ""), "")

    @property
    def use_live(self) -> bool:
        return self.mode == "live" and self.appkey and self.appsecret

    def get_token(self) -> Dict[str, Any]:
        if not self.use_live:
            return {"access_token": "simulation", "expires_in": 3600}
        if self.token_cache.exists():
            with self.token_cache.open("r", encoding="utf-8") as fh:
                cached = json.load(fh)
                expiry = datetime.fromisoformat(cached.get("expires_at"))
                if expiry > datetime.utcnow() + timedelta(minutes=5):
                    return cached
        token = {"access_token": "placeholder", "expires_at": (datetime.utcnow() + timedelta(hours=1)).isoformat()}
        self.token_cache.parent.mkdir(parents=True, exist_ok=True)
        with self.token_cache.open("w", encoding="utf-8") as fh:
            json.dump(token, fh)
        return token

    def _synthetic(self, group: str, name: str, periods: int = 120) -> pd.Series:
        section = self.fixtures.get(group, {})
        params = section.get(name.upper()) or section.get(name) or {}
        base = float(params.get("base", 100))
        amplitude = float(params.get("amplitude", 5))
        return synthetic_series(base, amplitude, periods=periods, seed=hash(name) % 10_000)

    def get_index_series(self, name: str, periods: int = 120) -> pd.DataFrame:
        series = self._synthetic("indexes", name, periods)
        return pd.DataFrame({"ts_kst": series.index, "value": series.values})

    def get_fx_series(self, name: str, periods: int = 120) -> pd.DataFrame:
        series = self._synthetic("fx", name, periods)
        return pd.DataFrame({"ts_kst": series.index, "value": series.values})

    def get_futures_series(self, name: str, periods: int = 120) -> pd.DataFrame:
        series = self._synthetic("futures", name, periods)
        return pd.DataFrame({"ts_kst": series.index, "value": series.values})

    def get_equity_universe(self, universe: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for _, row in universe.iterrows():
            series = self._synthetic("indexes", row["code"], periods=2)
            value = float(series.iloc[-1])
            prev = float(series.iloc[-2]) if len(series) > 1 else value
            change = value - prev
            rows.append(
                {
                    "code": row["code"],
                    "market": row["market"],
                    "close": value,
                    "prev_close": prev,
                    "change": change,
                    "value_traded": abs(change) * 10 + 5,
                    "limit_flag": "upper" if change > 3 else "lower" if change < -3 else "neutral",
                }
            )
        return pd.DataFrame(rows)

    def get_kor_yields(self) -> pd.DataFrame:
        base3 = self._synthetic("indexes", "KR3Y", periods=120)
        base10 = self._synthetic("indexes", "KR10Y", periods=120)
        return pd.DataFrame({
            "ts_kst": base3.index,
            "kr3y": base3.values,
            "kr10y": base10.values,
        })
