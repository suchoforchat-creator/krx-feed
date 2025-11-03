from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import requests
import yfinance as yf
from pykrx import stock
from tenacity import retry, stop_after_attempt, wait_exponential

from ..utils import KST, kst_now

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
        self.mode = kis_cfg.get("mode", "auto")
        self.token_cache = Path(kis_cfg.get("token_cache", "cache/kis_token.json"))
        self.appkey = os.getenv(kis_cfg.get("appkey_env", ""), "")
        self.appsecret = os.getenv(kis_cfg.get("appsecret_env", ""), "")
        self.fallback = self.config.get("fallback", {})
        self.series_meta = kis_cfg.get("series", {})
        self.base_url = kis_cfg.get("rest_base_url", kis_cfg.get("base_url", "https://openapi.koreainvestment.com:9443"))
        self.api_domain = kis_cfg.get("api_domain", self.base_url)
        self.token_url = kis_cfg.get("token_url", f"{self.base_url}/oauth2/tokenP")
        self.session = requests.Session()
        self._cached_token: Optional[Dict[str, Any]] = None

    @property
    def use_live(self) -> bool:
        if self.mode == "simulation":
            return False
        if self.mode == "live":
            return bool(self.appkey and self.appsecret)
        # auto 모드: 키가 존재하면 실시간 사용
        return bool(self.appkey and self.appsecret)

    def get_token(self) -> Dict[str, Any]:
        if not self.use_live:
            return {"access_token": "simulation", "expires_at": (datetime.utcnow() + timedelta(hours=1)).isoformat()}
        if self._cached_token is None and self.token_cache.exists():
            with self.token_cache.open("r", encoding="utf-8") as fh:
                cached = json.load(fh)
            expires_at = cached.get("expires_at")
            if expires_at and datetime.fromisoformat(expires_at) > datetime.utcnow() + timedelta(minutes=2):
                self._cached_token = cached
        if self._cached_token and datetime.fromisoformat(self._cached_token["expires_at"]) > datetime.utcnow() + timedelta(minutes=2):
            return self._cached_token
        token = self._request_token()
        self._cached_token = token
        self.token_cache.parent.mkdir(parents=True, exist_ok=True)
        with self.token_cache.open("w", encoding="utf-8") as fh:
            json.dump(token, fh)
        return token

    def _request_token(self) -> Dict[str, Any]:
        payload = {"grant_type": "client_credentials", "appkey": self.appkey, "appsecret": self.appsecret}
        response = self.session.post(self.token_url, json=payload, timeout=15)
        response.raise_for_status()
        data = response.json()
        access_token = data.get("access_token")
        if not access_token:
            raise RuntimeError(f"KIS token 응답 오류: {data}")
        expires_in = int(data.get("expires_in", 3600))
        expiry = datetime.utcnow() + timedelta(seconds=expires_in - 60)
        return {"access_token": access_token, "expires_at": expiry.isoformat()}

    # ------------------------------------------------------------------
    # REST helpers
    # ------------------------------------------------------------------
    def _auth_headers(self, tr_id: str) -> Dict[str, str]:
        token = self.get_token()
        return {
            "Authorization": f"Bearer {token['access_token']}",
            "appkey": self.appkey,
            "appsecret": self.appsecret,
            "tr_id": tr_id,
            "custtype": "P",
        }

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=4))
    def _rest(self, method: str, path: str, *, params: Optional[Dict[str, Any]] = None, json_body: Optional[Dict[str, Any]] = None, tr_id: str) -> Dict[str, Any]:
        if not self.use_live:
            raise RuntimeError("live 모드가 아님")
        url = f"{self.api_domain}{path}"
        response = self.session.request(
            method.upper(),
            url,
            params=params,
            json=json_body,
            headers=self._auth_headers(tr_id),
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
        if data.get("rt_cd") not in (None, "0"):
            raise RuntimeError(f"KIS 응답 오류: {data}")
        return data

    @staticmethod
    def _pick_column(frame: pd.DataFrame, candidates: list[str]) -> Optional[pd.Series]:
        for col in candidates:
            if col in frame.columns:
                series = frame[col]
                if series.notna().any():
                    return series
        return None

    def _normalize_timeseries(self, items: Any, periods: int, meta: Dict[str, Any]) -> pd.DataFrame:
        frame = pd.DataFrame(items)
        if frame.empty:
            raise ValueError("빈 데이터")
        date_candidates = [meta.get("date_field")] if meta.get("date_field") else []
        date_candidates += [
            "stck_bsop_date",
            "bsop_date",
            "bas_dt",
            "base_date",
            "biz_dt",
            "xymd",
            "trd_dd",
            "date",
        ]
        time_candidates = [meta.get("time_field")] if meta.get("time_field") else []
        time_candidates += ["stck_bsop_time", "hhmm", "time", "cntg_hour", "tm"]
        value_candidates = [meta.get("value_field")] if meta.get("value_field") else []
        value_candidates += [
            "stck_prpr",
            "stck_clpr",
            "clpr",
            "clos",
            "close",
            "ovrs_prpr",
            "ovrs_clpr",
            "last",
            "deal_prc",
            "prpr",
            "idx_clpr",
        ]

        date_series = None
        for col in date_candidates:
            if col and col in frame.columns:
                date_series = frame[col]
                break
        if date_series is None:
            raise ValueError("날짜 컬럼을 찾을 수 없습니다")

        time_series = None
        for col in time_candidates:
            if col and col in frame.columns:
                time_series = frame[col]
                break

        value_series = self._pick_column(frame, [c for c in value_candidates if c])
        if value_series is None:
            raise ValueError("값 컬럼을 찾을 수 없습니다")

        ts = date_series.astype(str).str.replace("-", "")
        if time_series is not None:
            hhmm = time_series.astype(str).str.zfill(6).str[:6]
            ts = ts + hhmm
            parsed = pd.to_datetime(ts, format="%Y%m%d%H%M%S", errors="coerce")
        else:
            parsed = pd.to_datetime(ts, format="%Y%m%d", errors="coerce")
        parsed = parsed.dt.tz_localize("Asia/Seoul", nonexistent="shift_forward", ambiguous="NaT")

        values = pd.to_numeric(value_series, errors="coerce")
        frame = pd.DataFrame({"ts_kst": parsed, "value": values})
        frame = frame.dropna(subset=["ts_kst", "value"]).drop_duplicates(subset=["ts_kst"])
        frame = frame.sort_values("ts_kst").tail(periods)
        frame["source"] = meta.get("source", "KIS")
        frame["quality"] = meta.get("quality", "primary")
        if meta.get("url"):
            frame["url"] = meta["url"]
        return frame.reset_index(drop=True)

    def _series_meta(self, group: str, name: str) -> Dict[str, Any]:
        section = self.series_meta.get(group, {})
        return section.get(name, {})

    def _fetch_series(self, group: str, name: str, periods: int = 120) -> pd.DataFrame:
        meta = self._series_meta(group, name)
        if not meta:
            raise KeyError(name)
        path = meta.get("path")
        if not path:
            raise ValueError(f"경로 누락: {group}/{name}")
        tr_id = meta.get("tr_id", "")
        params = meta.get("params", {}).copy()
        if meta.get("period_param"):
            params[meta["period_param"]] = str(periods)
        payload = self._rest(meta.get("method", "GET"), path, params=params, json_body=meta.get("json"), tr_id=tr_id)
        result_path = meta.get("result_path")
        items = payload.get(result_path) if result_path else None
        if items is None:
            for key in ("output2", "output1", "output"):
                if key in payload:
                    items = payload[key]
                    break
        if items is None:
            raise ValueError(f"KIS 응답 파싱 실패: {payload}")
        frame = self._normalize_timeseries(items, periods, meta)
        if meta.get("unit"):
            frame["unit"] = meta["unit"]
        return frame

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _yf_history(self, symbol: str, periods: int = 120) -> pd.DataFrame:
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
            try:
                frame = self._fetch_series("indexes", name, periods)
                return frame
            except Exception as exc:
                logger.warning("KIS 지수 조회 실패(%s): %s", name, exc)
        symbol = self._fallback_symbol("indexes", name)
        if symbol:
            try:
                return self._yf_history(symbol, periods)
            except Exception as exc:  # pragma: no cover - network dependent
                logger.warning("index fallback failed for %s (%s): %s", name, symbol, exc)
        return pd.DataFrame()

    def get_fx_series(self, name: str, periods: int = 120) -> pd.DataFrame:
        if self.use_live:
            try:
                frame = self._fetch_series("fx", name, periods)
                return frame
            except Exception as exc:
                logger.warning("KIS 환율 조회 실패(%s): %s", name, exc)
        symbol = self._fallback_symbol("fx", name)
        if symbol:
            try:
                return self._yf_history(symbol, periods)
            except Exception as exc:  # pragma: no cover - network dependent
                logger.warning("fx fallback failed for %s (%s): %s", name, symbol, exc)
        return pd.DataFrame()

    def get_futures_series(self, name: str, periods: int = 120) -> pd.DataFrame:
        if self.use_live:
            try:
                frame = self._fetch_series("futures", name, periods)
                return frame
            except Exception as exc:
                logger.warning("KIS 선물 조회 실패(%s): %s", name, exc)
        symbol = self._fallback_symbol("futures", name)
        if symbol:
            try:
                return self._yf_history(symbol, periods)
            except Exception as exc:  # pragma: no cover - network dependent
                logger.warning("futures fallback failed for %s (%s): %s", name, symbol, exc)
        return pd.DataFrame()

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
            raise RuntimeError("KIS 실거래 모드에서는 universe snapshot 구현 필요")
        try:
            snaps = self._pykrx_snapshots()
            return snaps
        except Exception as exc:  # pragma: no cover - network dependent
            logger.warning("pykrx fallback failed: %s", exc)
            return pd.DataFrame()

    def get_kor_yields(self) -> pd.DataFrame:
        if self.use_live:
            try:
                kr3 = self._fetch_series("yields", "KR3Y")
                kr10 = self._fetch_series("yields", "KR10Y")
                merged = _merge_frames({"kr3y": kr3, "kr10y": kr10})
                if not merged.empty:
                    merged["source"] = "KIS"
                    merged["quality"] = "primary"
                    merged["url"] = self.series_meta.get("yields", {}).get("KR3Y", {}).get("url", self.base_url)
                    return merged
            except Exception as exc:
                logger.warning("KIS 국채수익률 조회 실패: %s", exc)
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
        return pd.DataFrame()
