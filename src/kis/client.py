from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, time
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import pandas as pd
import requests
import yfinance as yf
from pykrx import bond, stock
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
        self.symbol_not_found: set[str] = set()
        self.yield_failure_meta: Dict[str, Dict[str, str]] = {}

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
        data = yf.download(
            symbol,
            period="1y",
            interval="1d",
            progress=False,
            auto_adjust=False,
            threads=False,
        )
        if data.empty:
            raise ValueError(f"empty response for {symbol}")

        close = data
        if isinstance(close.columns, pd.MultiIndex):
            # Yahoo가 멀티 인덱스 컬럼으로 반환하는 경우 Close 레벨만 선택
            level0 = close.columns.get_level_values(0)
            if "Close" in level0:
                close = close.xs("Close", axis=1, level=0)
        if isinstance(close, pd.DataFrame):
            if "Close" in close.columns:
                close = close["Close"]
            elif close.shape[1] == 1:
                close = close.iloc[:, 0]
        if not isinstance(close, pd.Series):
            raise ValueError(f"unable to locate close column for {symbol}")

        close = pd.to_numeric(close, errors="coerce").dropna()
        if close.empty:
            raise ValueError(f"no close data for {symbol}")

        close = close.tail(periods)
        idx = _to_kst_index(close.index)
        length = len(close)
        data_dict = {
            "ts_kst": list(idx),
            "value": close.to_numpy().reshape(length),
            "source": [f"YahooFinance({symbol})"] * length,
            "quality": ["secondary"] * length,
            "url": [f"https://finance.yahoo.com/quote/{symbol}"] * length,
        }
        frame = pd.DataFrame(data_dict)
        return frame

    def _fallback_symbol(self, group: str, name: str) -> Optional[str]:
        section = self.fallback.get(group, {})
        return section.get(name)

    def _pykrx_kor_yields(self, periods: int = 120) -> pd.DataFrame:
        records: list[dict[str, Any]] = []
        candidates = {
            "kr3y": [
                "국고채(3년)",
                "3년",
                "3년물",
                "3Y",
                "3-year",
                "국채3년",
            ],
            "kr10y": [
                "국고채(10년)",
                "10년",
                "10년물",
                "10Y",
                "10-year",
                "국채10년",
            ],
        }

        def pick(series_df: pd.DataFrame, key: str) -> pd.Series:
            for column in candidates[key]:
                if column in series_df.columns:
                    values = pd.to_numeric(series_df[column], errors="coerce")
                    if values.notna().any():
                        return values
            return pd.Series(dtype=float)

        today = datetime.now(KST).date()
        for offset in range(periods * 3):
            current = today - timedelta(days=offset)
            date_str = current.strftime("%Y%m%d")
            try:
                daily = bond.get_otc_treasury_yields(date_str)
            except Exception as exc:  # pragma: no cover - network dependent
                logger.debug("pykrx 국채수익률 조회 실패(%s): %s", date_str, exc)
                continue

            if daily is None or daily.empty:
                continue

            daily = daily.reset_index(drop=True)
            three = pick(daily, "kr3y")
            ten = pick(daily, "kr10y")

            if three.empty or ten.empty:
                continue

            value3 = three.iloc[-1]
            value10 = ten.iloc[-1]
            if pd.isna(value3) or pd.isna(value10):
                continue

            ts = datetime.combine(current, time(17, 0), tzinfo=KST)
            records.append(
                {
                    "ts_kst": ts,
                    "kr3y": float(value3),
                    "kr10y": float(value10),
                }
            )

            if len(records) >= periods:
                break

        frame = pd.DataFrame(records)
        if frame.empty:
            return pd.DataFrame(
                columns=["ts_kst", "kr3y", "kr10y", "source", "quality", "url"]
            )

        frame = frame.drop_duplicates(subset=["ts_kst"]).sort_values("ts_kst").tail(periods)
        frame["source"] = "pykrx"
        frame["quality"] = "secondary"
        frame["url"] = "https://www.kofiabond.or.kr"
        return frame.reset_index(drop=True)

    def _fetch_yield_with_retry(self, alias: str, periods: int = 120) -> pd.DataFrame:
        last_exc: Optional[Exception] = None
        for attempt in range(3):
            try:
                frame = self._fetch_series("yields", alias, periods)
                if frame.empty:
                    raise ValueError("empty response")
                return frame
            except Exception as exc:
                last_exc = exc
                logger.debug("KIS %s 수익률 %d차 시도 실패: %s", alias, attempt + 1, exc)
        if last_exc:
            raise last_exc
        raise RuntimeError(f"{alias} fetch failed")

    def _ecos_kor_yields(self, targets: Iterable[str], periods: int = 120) -> tuple[Dict[str, pd.DataFrame], Dict[str, str]]:
        ecos_cfg = self.config.get("ecos", {})
        if not ecos_cfg:
            return {}, {alias: "ecos_unconfigured" for alias in targets}

        api_key_env = ecos_cfg.get("api_key_env")
        api_key = ecos_cfg.get("api_key")
        if not api_key and api_key_env:
            api_key = os.getenv(api_key_env, "")
        if not api_key:
            return {}, {alias: "ecos_api_key_missing" for alias in targets}

        base_url = ecos_cfg.get("base_url", "https://ecos.bok.or.kr/api/StatisticSearch")
        timeout = ecos_cfg.get("timeout", 25)
        end = kst_now().date()
        start = end - timedelta(days=periods * 3)
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")

        results: Dict[str, pd.DataFrame] = {}
        failures: Dict[str, str] = {}
        series_meta = ecos_cfg.get("series", {})

        for alias in targets:
            meta = series_meta.get(alias)
            if not meta:
                failures[alias] = "ecos_series_missing"
                continue
            statistic = meta.get("statistic")
            if not statistic:
                failures[alias] = "ecos_statistic_missing"
                continue
            cycle = meta.get("cycle", "DD")
            items = list(meta.get("items", []))
            while len(items) < 3:
                items.append("")
            start_row = int(meta.get("start_row", 1))
            end_row = int(meta.get("end_row", max(200, periods * 3)))
            url = "/".join(
                [
                    base_url.rstrip("/"),
                    api_key,
                    "json",
                    "kr",
                    str(start_row),
                    str(end_row),
                    statistic,
                    cycle,
                    start_str,
                    end_str,
                    *(item or "" for item in items[:3]),
                ]
            )
            try:
                resp = self.session.get(url, timeout=timeout)
                resp.raise_for_status()
                payload = resp.json()
            except Exception as exc:
                logger.warning("ECOS %s 요청 실패: %s", alias, exc)
                failures[alias] = "ecos_request_failed"
                continue

            rows = payload.get("StatisticSearch", {}).get("row", [])
            if not rows:
                failures[alias] = "ecos_empty"
                continue

            frame = pd.DataFrame(rows)
            if "TIME" not in frame or "DATA_VALUE" not in frame:
                failures[alias] = "ecos_field_missing"
                continue

            frame["ts_kst"] = pd.to_datetime(frame["TIME"], format="%Y%m%d", errors="coerce")
            frame["ts_kst"] = frame["ts_kst"].dt.tz_localize(KST, nonexistent="shift_forward", ambiguous="NaT")
            frame["value"] = pd.to_numeric(frame["DATA_VALUE"], errors="coerce")
            frame = frame.dropna(subset=["ts_kst", "value"]).sort_values("ts_kst").tail(periods)
            if frame.empty:
                failures[alias] = "ecos_empty"
                continue

            frame = frame[["ts_kst", "value"]]
            frame["source"] = "BOK_ECOS"
            frame["quality"] = "secondary"
            frame["url"] = meta.get("url", base_url)
            results[alias] = frame.reset_index(drop=True)

        return results, failures

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

    def get_futures_series(self, name: str, periods: int = 120, alias: Optional[str] = None) -> pd.DataFrame:
        if self.use_live:
            try:
                frame = self._fetch_series("futures", name, periods)
                return frame
            except Exception as exc:
                logger.warning("KIS 선물 조회 실패(%s): %s", name, exc)
        lookup_key = alias or name
        symbol = self._fallback_symbol("futures", lookup_key)
        if symbol:
            try:
                return self._yf_history(symbol, periods)
            except Exception as exc:  # pragma: no cover - network dependent
                logger.warning("futures fallback failed for %s (%s): %s", lookup_key, symbol, exc)
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
        self.symbol_not_found.clear()
        self.yield_failure_meta = {}

        frames: Dict[str, pd.DataFrame] = {}
        used_sources: list[str] = []
        used_urls: list[str] = []
        missing: set[str] = {"KR3Y", "KR10Y"}

        if self.use_live:
            for alias in list(missing):
                try:
                    frame = self._fetch_yield_with_retry(alias)
                    frames[alias.lower()] = frame
                    missing.discard(alias)
                    src = "KIS"
                    url = self.series_meta.get("yields", {}).get(alias, {}).get("url", self.base_url)
                    if src not in used_sources:
                        used_sources.append(src)
                    if url and url not in used_urls:
                        used_urls.append(url)
                    self.yield_failure_meta.pop(alias, None)
                except Exception as exc:
                    logger.warning("KIS 국채수익률 %s 조회 실패: %s", alias, exc)
                    meta = self.series_meta.get("yields", {}).get(alias, {})
                    url = meta.get("url", self.base_url)
                    self.yield_failure_meta[alias] = {"reason": "KIS_error", "url": url}

        if missing:
            ecos_frames, ecos_failures = self._ecos_kor_yields(missing)
            for alias, frame in ecos_frames.items():
                if frame.empty:
                    continue
                frames[alias.lower()] = frame
                missing.discard(alias)
                src = str(frame.get("source", pd.Series(["BOK_ECOS"])).iloc[-1]) if not frame.empty else "BOK_ECOS"
                url = str(frame.get("url", pd.Series([""])).iloc[-1]) if not frame.empty else ""
                if src and src not in used_sources:
                    used_sources.append(src)
                if url and url not in used_urls:
                    used_urls.append(url)
                self.yield_failure_meta.pop(alias, None)
            for alias, reason in ecos_failures.items():
                if alias in missing:
                    meta = self.config.get("ecos", {}).get("series", {}).get(alias, {})
                    url = meta.get("url", self.config.get("ecos", {}).get("base_url", ""))
                    self.yield_failure_meta[alias] = {"reason": reason, "url": url}

        if missing:
            pykrx_frame = self._pykrx_kor_yields()
            if not pykrx_frame.empty:
                for alias, column in ("KR3Y", "kr3y"), ("KR10Y", "kr10y"):
                    if alias in missing and column in pykrx_frame.columns:
                        series = pykrx_frame[["ts_kst", column]].dropna()
                        if series.empty:
                            continue
                        temp = series.rename(columns={column: "value"})
                        temp["source"] = "pykrx"
                        temp["quality"] = "secondary"
                        temp["url"] = pykrx_frame.get("url", pd.Series(["https://www.kofiabond.or.kr"]))
                        frames[alias.lower()] = temp
                        missing.discard(alias)
                        if "pykrx" not in used_sources:
                            used_sources.append("pykrx")
                        url = str(temp.get("url", pd.Series([""])).iloc[-1]) if not temp.empty else ""
                        if url and url not in used_urls:
                            used_urls.append(url)
                        self.yield_failure_meta.pop(alias, None)

        if missing:
            for alias in missing:
                meta = self.series_meta.get("yields", {}).get(alias, {})
                url = meta.get("url", self.base_url)
                self.symbol_not_found.add(alias)
                self.yield_failure_meta[alias] = {"reason": "symbol_not_found", "url": url}

        if not frames:
            return pd.DataFrame()

        merged = _merge_frames(frames)
        if merged.empty:
            return pd.DataFrame()

        if not used_sources:
            used_sources.append("secondary")
        merged["source"] = "+".join(dict.fromkeys(used_sources))
        qualities = []
        for df in frames.values():
            if "quality" in df.columns and not df.empty:
                qualities.append(str(df["quality"].iloc[-1]))
        merged["quality"] = "primary" if "KIS" in used_sources else ("secondary" if qualities else "secondary")
        merged["url"] = " ".join(dict.fromkeys([u for u in used_urls if u]))
        return merged
