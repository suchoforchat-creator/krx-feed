from __future__ import annotations

import json
import logging
import os
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import yfinance as yf
from pykrx import stock

from ..utils import KST, kst_now, synthetic_series

logger = logging.getLogger(__name__)


def _to_kst_index(index: pd.Index) -> pd.DatetimeIndex:
    idx = pd.DatetimeIndex(index)
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    return idx.tz_convert(KST)


def _merge_frames(frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()
    merged = None
    for key, frame in frames.items():
        temp = frame.set_index("ts_kst")["value"].rename(key)
        merged = temp.to_frame() if merged is None else merged.join(temp, how="outer")
    merged = merged.sort_index().dropna(how="all")
    merged = merged.tail(120)
    merged = merged.reset_index()
    return merged


@dataclass
class KISClient:
    config: Dict[str, Any]

    def __post_init__(self) -> None:
        kis_cfg = self.config.get("kis", {})
        self.mode = kis_cfg.get("mode", "simulation")
        self.fixtures = self.config.get("fixtures", {})
        self.token_cache = Path(kis_cfg.get("token_cache", "cache/kis_token.json"))
        self.appkey = os.getenv(kis_cfg.get("appkey_env", ""), "")
        self.appsecret = os.getenv(kis_cfg.get("appsecret_env", ""), "")
        self.fallback = self.config.get("fallback", {})

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

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _synthetic_frame(self, group: str, name: str, periods: int = 120) -> pd.DataFrame:
        section = self.fixtures.get(group, {})
        params = section.get(name.upper()) or section.get(name) or {}
        base = float(params.get("base", 100))
        amplitude = float(params.get("amplitude", 5))
        series = synthetic_series(base, amplitude, periods=periods, seed=hash(name) % 10_000)
        frame = pd.DataFrame({"ts_kst": series.index, "value": series.values})
        frame["source"] = "synthetic"
        frame["quality"] = "secondary"
        frame["url"] = ""
        return frame

    def _yf_history(self, symbol: str, periods: int = 120) -> pd.DataFrame:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=FutureWarning)
            data = yf.download(symbol, period="1y", interval="1d", progress=False, auto_adjust=False, threads=False)
        close = data.get("Close")
        if close is None or close.dropna().empty:
            raise ValueError(f"no close data for {symbol}")
        close = close.dropna().tail(periods)
        idx = _to_kst_index(close.index)
        frame = pd.DataFrame(
            {
                "ts_kst": idx,
                "value": close.values,
                "source": f"YahooFinance({symbol})",
                "quality": "secondary",
                "url": f"https://finance.yahoo.com/quote/{symbol}",
            }
        )
        return frame

    def _fallback_symbol(self, group: str, name: str) -> Optional[str]:
        section = self.fallback.get(group, {})
        return section.get(name)

    # ------------------------------------------------------------------
    # Data accessors
    # ------------------------------------------------------------------
    def get_index_series(self, name: str, periods: int = 120) -> pd.DataFrame:
        if self.use_live:
            return self._synthetic_frame("indexes", name, periods)
        symbol = self._fallback_symbol("indexes", name)
        if symbol:
            try:
                return self._yf_history(symbol, periods)
            except Exception as exc:  # pragma: no cover - network dependent
                logger.warning("index fallback failed for %s (%s): %s", name, symbol, exc)
        return self._synthetic_frame("indexes", name, periods)

    def get_fx_series(self, name: str, periods: int = 120) -> pd.DataFrame:
        if self.use_live:
            return self._synthetic_frame("fx", name, periods)
        symbol = self._fallback_symbol("fx", name)
        if symbol:
            try:
                return self._yf_history(symbol, periods)
            except Exception as exc:  # pragma: no cover - network dependent
                logger.warning("fx fallback failed for %s (%s): %s", name, symbol, exc)
        return self._synthetic_frame("fx", name, periods)

    def get_futures_series(self, name: str, periods: int = 120) -> pd.DataFrame:
        if self.use_live:
            return self._synthetic_frame("futures", name, periods)
        symbol = self._fallback_symbol("futures", name)
        if symbol:
            try:
                return self._yf_history(symbol, periods)
            except Exception as exc:  # pragma: no cover - network dependent
                logger.warning("futures fallback failed for %s (%s): %s", name, symbol, exc)
        return self._synthetic_frame("futures", name, periods)

    def _pykrx_snapshots(self) -> pd.DataFrame:
        today = kst_now()
        for offset in range(10):
            target = today - timedelta(days=offset)
            date_str = target.strftime("%Y%m%d")
            kospi = stock.get_market_ohlcv_by_ticker(date_str, market="KOSPI")
            kosdaq = stock.get_market_ohlcv_by_ticker(date_str, market="KOSDAQ")
            if not kospi.empty or not kosdaq.empty:
                break
        frames = []
        for df, market_label in ((kospi, "kospi"), (kosdaq, "kosdaq")):
            if df.empty:
                continue
            temp = df.reset_index().rename(columns={"티커": "code"})
            temp["market"] = market_label
            close = pd.to_numeric(temp.get("종가"), errors="coerce")
            pct = pd.to_numeric(temp.get("등락률"), errors="coerce") / 100.0
            denom = 1 + pct
            denom = denom.replace({0: pd.NA})
            prev_close = close / denom
            prev_close = prev_close.fillna(close)
            change = close - prev_close
            value_traded = pd.to_numeric(temp.get("거래대금"), errors="coerce") / 1e9
            limit_flag = pd.Series("neutral", index=temp.index)
            limit_flag.loc[pct >= 0.295] = "upper"
            limit_flag.loc[pct <= -0.295] = "lower"
            frames.append(
                pd.DataFrame(
                    {
                        "code": temp["code"],
                        "market": market_label,
                        "close": close,
                        "prev_close": prev_close,
                        "change": change,
                        "value_traded": value_traded.fillna(0),
                        "limit_flag": limit_flag,
                    }
                )
            )
        if not frames:
            raise ValueError("pykrx snapshots unavailable")
        return pd.concat(frames, ignore_index=True)

    def get_equity_universe(self, universe: pd.DataFrame) -> pd.DataFrame:
        if self.use_live:
            return self._synthetic_universe(universe)
        try:
            snaps = self._pykrx_snapshots()
            return snaps
        except Exception as exc:  # pragma: no cover - network dependent
            logger.warning("pykrx fallback failed: %s", exc)
            return self._synthetic_universe(universe)

    def _synthetic_universe(self, universe: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for _, row in universe.iterrows():
            series = self._synthetic_frame("indexes", row["code"], periods=2)
            value = float(series.iloc[-1]["value"])
            prev = float(series.iloc[-2]["value"]) if len(series) > 1 else value
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
        if self.use_live:
            return self._synthetic_kor_yields()
        frames: Dict[str, pd.DataFrame] = {}
        for alias, symbol in self.fallback.get("yields", {}).items():
            try:
                frames[alias.lower()] = self._yf_history(symbol)
            except Exception as exc:  # pragma: no cover - network dependent
                logger.warning("yield fallback failed for %s (%s): %s", alias, symbol, exc)
        merged = _merge_frames(frames)
        if not merged.empty:
            merged = merged.rename(columns={col: col for col in merged.columns})
            merged["source"] = "YahooFinance"
            merged["quality"] = "secondary"
            merged["url"] = "https://finance.yahoo.com"
            merged = merged.rename(columns={"kr3y": "kr3y", "kr10y": "kr10y"})
            return merged
        return self._synthetic_kor_yields()

    def _synthetic_kor_yields(self) -> pd.DataFrame:
        base3 = self._synthetic_frame("indexes", "KR3Y")
        base10 = self._synthetic_frame("indexes", "KR10Y")
        merged = _merge_frames({"kr3y": base3, "kr10y": base10})
        merged["source"] = "synthetic"
        merged["quality"] = "secondary"
        merged["url"] = ""
        return merged
