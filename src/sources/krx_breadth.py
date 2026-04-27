"""KRX 전종목 등락률 메뉴에서 A/D·TRIN·거래대금·상/하한가를 산출한다.

초심자도 이해하기 쉽도록 각 함수에 상세 주석을 달았고, 예상과 다른
값이 나오면 즉시 디버깅할 수 있도록 로그와 노트 체계를 통일했다.
성공 시에도 ``notes="ok"``를 채워 최신 CSV에서 상태를 쉽게 확인할 수 있다.
"""

from __future__ import annotations

import io
import logging
import math
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import requests
from pykrx import stock

from ..utils import KST
from .krx_client import KrxClient


logger = logging.getLogger(__name__)


KRX_ENDPOINTS = {
    "MDC0201020102": {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT01602",
        "params": {"adjStkPrc": "1"},
    }
}

ENDPOINT_URL = "https://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd"

ID_PRIORITY = ["ISU_SRT_CD", "ISU_CD", "isuCd", "isuSrtCd"]
CLOSE_COLUMNS = ["TDD_CLSPRC", "CLSPRC", "stck_clpr", "stckPrpr"]
VOLUME_COLUMNS = ["ACC_TRDVOL", "ACC_TRDVOL", "acml_vol", "acmlTrdVol"]
VALUE_COLUMNS = ["ACC_TRDVAL", "ACC_TRD_AMT", "acml_trdval", "acmlTrdAmt"]
CHANGE_COLUMNS = ["FLUC_RT", "CMPPREVDD_PRC", "stckPrdyCtrt"]
LIMIT_TEXT_COLUMNS = ["ETC_TP_NM", "FLUC_TP_CD", "flucTpCd"]

EXCLUDED_SECURITY_GROUPS = {"EF", "EN", "EW", "KO", "IF", "MF", "RT", "DR"}


def _is_business_day(target: date) -> bool:
    return target.weekday() < 5


def _previous_business_day(target: date) -> date:
    current = target - timedelta(days=1)
    while not _is_business_day(current):
        current -= timedelta(days=1)
    return current


def determine_target(now: datetime) -> Tuple[date, bool]:
    now_kst = now.astimezone(KST)
    current_date = now_kst.date()
    current_time = now_kst.time()

    morning_start = dtime(hour=7, minute=0)
    morning_end = dtime(hour=8, minute=0)
    evening_start = dtime(hour=16, minute=50)
    evening_end = dtime(hour=17, minute=30)

    should_wait = False

    if morning_start <= current_time < morning_end:
        target = _previous_business_day(current_date)
    elif evening_start <= current_time <= evening_end:
        if _is_business_day(current_date):
            target = current_date
            should_wait = True
        else:
            target = _previous_business_day(current_date)
    else:
        target = (
            _previous_business_day(current_date)
            if current_time < dtime(15, 30)
            else current_date
        )

    return target, should_wait


@dataclass
class BreadthResult:
    frames: Dict[str, pd.DataFrame]
    notes: Dict[str, str]


class KRXBreadthCollector:
    MENU_ID = "MDC0201020102"
    PYKRX_URL = "https://github.com/sharebook-kr/pykrx"
    NAVER_URL = "https://finance.naver.com/sise/sise_market_sum.naver"

    def __init__(
        self,
        client: KrxClient | None = None,
        *,
        poll_seconds: int = 20,
        poll_timeout: int = 480,
    ) -> None:
        self._client = client or KrxClient()
        self._poll_seconds = poll_seconds
        self._poll_timeout = poll_timeout

    @staticmethod
    def _select_column(frame: pd.DataFrame, candidates: Iterable[str]) -> str:
        for name in candidates:
            if name in frame.columns:
                return name
        raise KeyError(f"column_missing:{','.join(candidates)}")

    @staticmethod
    def _parse_numeric_text(text: str) -> float:
        if text is None:
            return float("nan")
        value = str(text).strip()
        if value == "":
            return float("nan")
        multiplier = 1.0
        if value.endswith("억"):
            multiplier = 100_000_000.0
            value = value[:-1]
        elif value.endswith("만"):
            multiplier = 10_000.0
            value = value[:-1]
        value = value.replace(",", "").replace("%", "")
        try:
            return float(value) * multiplier
        except ValueError:
            return float("nan")

    @classmethod
    def _to_numeric(cls, series: pd.Series) -> pd.Series:
        if series.empty:
            return pd.Series(dtype=float)
        return series.astype(str).map(cls._parse_numeric_text).astype(float)

    @staticmethod
    def _filter_common_shares(frame: pd.DataFrame) -> pd.DataFrame:
        result = frame.copy()
        if "SECUGRP_ID" in result.columns:
            result = result.loc[~result["SECUGRP_ID"].isin(EXCLUDED_SECURITY_GROUPS)]
        if "INVST_TP_NM" in result.columns:
            result = result.loc[
                ~result["INVST_TP_NM"].astype(str).str.contains(
                    "ETF|ETN|ELW|KONEX", na=False
                )
            ]
        if "SRTSLSYN" in result.columns:
            result = result.loc[result["SRTSLSYN"].astype(str) != "Y"]
        return result

    def _endpoint_payload(self, menu_id: str, target: date, market: str) -> Dict[str, str]:
        endpoint = KRX_ENDPOINTS.get(menu_id)
        if endpoint is None:
            raise KeyError(f"endpoint_missing:{menu_id}")
        params = dict(endpoint.get("params", {}))
        params.update(
            {
                "strtDd": target.strftime("%Y%m%d"),
                "endDd": target.strftime("%Y%m%d"),
                "mktId": {"KOSPI": "STK", "KOSDAQ": "KSQ"}[market],
            }
        )
        return {"bld": endpoint["bld"], **params}

    def _fetch_board(self, target: date, market: str) -> pd.DataFrame:
        payload = self._endpoint_payload(self.MENU_ID, target, market)
        raw = self._client.fetch_json(self.MENU_ID, payload.pop("bld"), payload)
        rows = raw.get("output") or raw.get("OutBlock_1") or []
        frame = pd.DataFrame(rows)
        if frame.empty:
            raise ValueError("empty_frame")
        return frame

    def _prepare_frame(self, frame: pd.DataFrame, *, is_prev: bool) -> pd.DataFrame:
        filtered = self._filter_common_shares(frame)
        id_column = self._select_column(filtered, ID_PRIORITY)
        filtered = filtered.copy()
        filtered["ID"] = filtered[id_column].astype(str).str.strip()
        filtered = filtered.loc[filtered["ID"] != ""]
        filtered.drop_duplicates("ID", keep="last", inplace=True)

        close_col = self._select_column(filtered, CLOSE_COLUMNS)
        close = self._to_numeric(filtered[close_col])
        filtered["PRC_cur" if not is_prev else "PRC_prev"] = close

        if is_prev:
            volume_col = self._select_column(filtered, VOLUME_COLUMNS)
            value_col = self._select_column(filtered, VALUE_COLUMNS)
            filtered["VOL_prev"] = self._to_numeric(filtered[volume_col])
            filtered["VAL_prev"] = self._to_numeric(filtered[value_col])
            return filtered[["ID", "PRC_prev", "VOL_prev", "VAL_prev"]]

        volume_col = self._select_column(filtered, VOLUME_COLUMNS)
        value_col = self._select_column(filtered, VALUE_COLUMNS)
        change_col = self._select_column(filtered, CHANGE_COLUMNS)

        filtered["VOL_cur"] = self._to_numeric(filtered[volume_col])
        filtered["VAL_cur"] = self._to_numeric(filtered[value_col])
        filtered["CHG_RT"] = self._to_numeric(filtered[change_col])

        for candidate in LIMIT_TEXT_COLUMNS:
            if candidate in filtered.columns:
                filtered["LIMIT_TXT"] = filtered[candidate].astype(str)
                break
        else:
            filtered["LIMIT_TXT"] = ""

        return filtered[["ID", "PRC_cur", "VOL_cur", "VAL_cur", "CHG_RT", "LIMIT_TXT"]]

    def _aggregate_market(
        self,
        target_date: date,
        market: str,
        current: pd.DataFrame,
        previous: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, Dict[str, str]]:
        notes: Dict[str, str] = {}
        url = (
            "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd"
            f"?menuId={self.MENU_ID}&market={market}"
        )

        cur_df = self._prepare_frame(current, is_prev=False)
        prev_df = self._prepare_frame(previous, is_prev=True)
        merged = cur_df.merge(prev_df, on="ID", how="inner", validate="one_to_one")
        merged.dropna(subset=["PRC_cur", "PRC_prev"], inplace=True)
        if merged.empty:
            reason = "no_overlap"
            for key in [
                "advance",
                "decline",
                "unchanged",
                "trading_value",
                "limit_up",
                "limit_down",
                "trin",
            ]:
                notes[f"{market}:{key}"] = f"parse_failed:{url},{reason}"
            return pd.DataFrame(), notes

        advance_mask = merged["PRC_cur"] > merged["PRC_prev"]
        decline_mask = merged["PRC_cur"] < merged["PRC_prev"]
        unchanged_mask = merged["PRC_cur"] == merged["PRC_prev"]

        advance = float(advance_mask.sum())
        decline = float(decline_mask.sum())
        unchanged = float(unchanged_mask.sum())

        trading_value = float(merged["VAL_cur"].sum())
        trading_note = "ok"
        if trading_value < 0:
            trading_note = f"range_violation:{url},lt_zero"
            trading_value = float("nan")

        change = merged["CHG_RT"].fillna(0)
        limit_text = merged["LIMIT_TXT"].astype(str)
        up_mask = (change >= 30) | limit_text.str.contains("상한|\+30", na=False)
        down_mask = (change <= -30) | limit_text.str.contains("하한|-30", na=False)
        limit_up = float(up_mask.sum())
        limit_down = float(down_mask.sum())

        adv_volume = float(merged.loc[advance_mask, "VOL_cur"].sum())
        dec_volume = float(merged.loc[decline_mask, "VOL_cur"].sum())
        trin_value = float("nan")
        trin_note = ""
        if all(val > 0 for val in [advance, decline, adv_volume, dec_volume]):
            trin_value = (advance / decline) / (adv_volume / dec_volume)
            if not 0.1 <= trin_value <= 10:
                trin_note = f"range_violation:{url},0.1-10"
                logger.debug(
                    "krx_breadth::_aggregate_market :: TRIN out of range market=%s value=%.4f",
                    market,
                    trin_value,
                )
                trin_value = float("nan")
        else:
            trin_note = f"upstream_missing:{url},zero_volume"

        ts = datetime.combine(target_date, dtime(hour=15, minute=30), tzinfo=KST)
        records: List[Dict[str, object]] = []

        def register(field: str, value: float, unit: str, note: str) -> None:
            notes[f"{market}:{field}"] = note
            records.append(
                {
                    "ts_kst": ts,
                    "asset": market,
                    "field": field,
                    "value": value,
                    "unit": unit,
                    "window": "EOD",
                    "source": "krx",
                    "quality": "final",
                    "url": url,
                    "notes": note,
                }
            )

        register("advance", advance, "issues", "ok")
        register("decline", decline, "issues", "ok")
        register("unchanged", unchanged, "issues", "ok")
        register("trading_value", trading_value, "KRW", trading_note)
        register("limit_up", limit_up, "issues", "ok")
        register("limit_down", limit_down, "issues", "ok")

        if math.isnan(trin_value):
            notes[f"{market}:trin"] = trin_note or f"parse_failed:{url},trin_unavailable"
        else:
            register("trin", trin_value, "ratio", "ok")

        return pd.DataFrame(records), notes

    def _fetch_pykrx_snapshot(self, target: date, market: str) -> Tuple[pd.DataFrame, date]:
        """pykrx로 전 종목 스냅샷을 받아온다.

        KRX API가 막혔을 때를 대비한 폴백이며, 가장 가까운 영업일을 찾아
        빈 DataFrame을 피하도록 한다.
        """

        # 초심자 팁: 공휴일/주말에는 데이터가 비어 있으니 최근 날짜부터 확인한다.
        errors: list[str] = []
        for offset in range(10):
            probe_date = target - timedelta(days=offset)
            date_str = probe_date.strftime("%Y%m%d")
            # 1) 우선 OHLCV API를 시도한다.
            #    일부 런타임에서 pykrx 내부 파서가 컬럼 불일치로 예외를 던질 수 있어
            #    날짜 루프 안에서 예외를 삼키고 다음 후보 날짜를 이어서 시도한다.
            try:
                snapshot = stock.get_market_ohlcv_by_ticker(date_str, market=market)
                if not snapshot.empty and {"등락률", "거래량", "거래대금"}.issubset(set(snapshot.columns)):
                    return snapshot, probe_date
            except Exception as exc:  # pragma: no cover - 외부 API/pykrx 버전 의존
                errors.append(f"ohlcv:{date_str}:{type(exc).__name__}")
                logger.debug(
                    "krx_breadth::_fetch_pykrx_snapshot OHLCV failed market=%s date=%s err=%s",
                    market,
                    date_str,
                    exc,
                )

            # 2) OHLCV가 실패하면 price_change API를 대체 경로로 시도한다.
            #    이 API는 등락률/거래량/거래대금을 제공하므로 A/D/TRIN 계산에 충분하다.
            try:
                baseline = _previous_business_day(probe_date)
                fallback = stock.get_market_price_change_by_ticker(
                    baseline.strftime("%Y%m%d"),
                    date_str,
                    market=market,
                )
                if not fallback.empty and {"등락률", "거래량", "거래대금"}.issubset(set(fallback.columns)):
                    return fallback, probe_date
            except Exception as exc:  # pragma: no cover - 외부 API/pykrx 버전 의존
                errors.append(f"price_change:{date_str}:{type(exc).__name__}")
                logger.debug(
                    "krx_breadth::_fetch_pykrx_snapshot price_change failed market=%s date=%s err=%s",
                    market,
                    date_str,
                    exc,
                )
                continue

        # 디버깅 시스템: 마지막에 어떤 API/날짜 조합이 실패했는지 에러 체인으로 남긴다.
        chain = " > ".join(errors[-6:]) if errors else "no_response"
        raise ValueError(f"pykrx_empty_frame|chain:{chain}")

    def _aggregate_snapshot(
        self,
        target_date: date,
        market: str,
        snapshot: pd.DataFrame,
        actual_date: date,
    ) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """pykrx 스냅샷으로 A/D/TRIN 등 지표를 계산한다."""

        url = self.PYKRX_URL
        notes: Dict[str, str] = {}
        # 등락률(%)로 상승/하락/보합을 계산한다.
        pct = pd.to_numeric(snapshot.get("등락률"), errors="coerce").fillna(0.0)
        volume = pd.to_numeric(snapshot.get("거래량"), errors="coerce").fillna(0.0)
        value_traded = pd.to_numeric(snapshot.get("거래대금"), errors="coerce").fillna(0.0)

        advance_mask = pct > 0
        decline_mask = pct < 0
        unchanged_mask = pct == 0

        advance = float(advance_mask.sum())
        decline = float(decline_mask.sum())
        unchanged = float(unchanged_mask.sum())

        trading_value = float(value_traded.sum())
        trading_note = "fallback:pykrx"
        if trading_value < 0:
            trading_note = f"range_violation:{url},lt_zero"
            trading_value = float("nan")

        # KRX 상/하한 기준(±30%)에 맞추기 위해 29.5%를 임계치로 둔다.
        limit_threshold = 29.5
        limit_up = float((pct >= limit_threshold).sum())
        limit_down = float((pct <= -limit_threshold).sum())

        adv_volume = float(volume[advance_mask].sum())
        dec_volume = float(volume[decline_mask].sum())
        trin_value = float("nan")
        trin_note = ""
        if all(val > 0 for val in [advance, decline, adv_volume, dec_volume]):
            trin_value = (advance / decline) / (adv_volume / dec_volume)
            if not 0.1 <= trin_value <= 10:
                trin_note = f"range_violation:{url},0.1-10"
                trin_value = float("nan")
        else:
            trin_note = f"upstream_missing:{url},zero_volume"

        ts = datetime.combine(actual_date, dtime(hour=15, minute=30), tzinfo=KST)
        date_note = "fallback:pykrx"
        if actual_date != target_date:
            date_note = f"fallback:pykrx:date_shift:{actual_date.strftime('%Y%m%d')}"

        records: List[Dict[str, object]] = []

        def register(field: str, value: float, unit: str, note: str) -> None:
            notes[f"{market}:{field}"] = note
            records.append(
                {
                    "ts_kst": ts,
                    "asset": market,
                    "field": field,
                    "value": value,
                    "unit": unit,
                    "window": "EOD",
                    "source": "pykrx",
                    "quality": "secondary",
                    "url": url,
                    "notes": note,
                }
            )

        register("advance", advance, "issues", date_note)
        register("decline", decline, "issues", date_note)
        register("unchanged", unchanged, "issues", date_note)
        register("trading_value", trading_value, "KRW", trading_note)
        register("limit_up", limit_up, "issues", date_note)
        register("limit_down", limit_down, "issues", date_note)

        if math.isnan(trin_value):
            notes[f"{market}:trin"] = trin_note or f"parse_failed:{url},trin_unavailable"
        else:
            register("trin", trin_value, "ratio", date_note)

        return pd.DataFrame(records), notes

    def _fetch_naver_market_sum(self, market: str) -> pd.DataFrame:
        """네이버 시가총액 페이지에서 전종목 데이터를 수집한다."""

        sosok = {"KOSPI": "0", "KOSDAQ": "1"}[market]
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://finance.naver.com/",
        }
        first_url = f"{self.NAVER_URL}?sosok={sosok}&page=1"
        response = requests.get(first_url, headers=headers, timeout=20)
        response.raise_for_status()

        try:
            import bs4  # type: ignore
        except Exception as exc:  # pragma: no cover - 환경 의존
            raise RuntimeError(f"bs4_missing:{exc}") from exc

        soup = bs4.BeautifulSoup(response.text, "lxml")
        last_link = soup.select_one("td.pgRR > a")
        last_page = 1
        if last_link and "page=" in last_link.get("href", ""):
            try:
                last_page = int(last_link["href"].split("page=")[-1])
            except ValueError:
                last_page = 1

        frames: List[pd.DataFrame] = []
        for page in range(1, last_page + 1):
            page_url = f"{self.NAVER_URL}?sosok={sosok}&page={page}"
            page_response = requests.get(page_url, headers=headers, timeout=20)
            page_response.raise_for_status()
            tables = pd.read_html(io.StringIO(page_response.text))
            if len(tables) < 2:
                continue
            page_frame = tables[1]
            page_frame = page_frame.dropna(subset=["종목명"]).copy()
            if not page_frame.empty:
                frames.append(page_frame)
        if not frames:
            raise ValueError("empty_frame")
        return pd.concat(frames, ignore_index=True)

    def _aggregate_naver_market(
        self,
        target_date: date,
        market: str,
        frame: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """네이버 시가총액 표로 A/D/TRIN 등을 계산한다."""

        url = self.NAVER_URL
        notes: Dict[str, str] = {}

        price = self._to_numeric(frame.get("현재가", pd.Series(dtype=str)))
        pct = self._to_numeric(frame.get("등락률", pd.Series(dtype=str)))
        volume = self._to_numeric(frame.get("거래량", pd.Series(dtype=str)))

        advance_mask = pct > 0
        decline_mask = pct < 0
        unchanged_mask = pct == 0

        advance = float(advance_mask.sum())
        decline = float(decline_mask.sum())
        unchanged = float(unchanged_mask.sum())

        # 거래대금은 제공되지 않으므로 현재가 * 거래량으로 근사한다.
        trading_value = float((price * volume).sum())
        trading_note = "fallback:naver"
        if trading_value < 0:
            trading_note = f"range_violation:{url},lt_zero"
            trading_value = float("nan")

        limit_threshold = 29.5
        limit_up = float((pct >= limit_threshold).sum())
        limit_down = float((pct <= -limit_threshold).sum())

        adv_volume = float(volume[advance_mask].sum())
        dec_volume = float(volume[decline_mask].sum())
        trin_value = float("nan")
        trin_note = ""
        if all(val > 0 for val in [advance, decline, adv_volume, dec_volume]):
            trin_value = (advance / decline) / (adv_volume / dec_volume)
            if not 0.1 <= trin_value <= 10:
                trin_note = f"range_violation:{url},0.1-10"
                trin_value = float("nan")
        else:
            trin_note = f"upstream_missing:{url},zero_volume"

        logger.debug(
            "krx_breadth::_aggregate_naver_market :: market=%s rows=%d adv=%d dec=%d unch=%d",
            market,
            len(frame),
            advance,
            decline,
            unchanged,
        )

        ts = datetime.combine(target_date, dtime(hour=15, minute=30), tzinfo=KST)
        records: List[Dict[str, object]] = []

        def register(field: str, value: float, unit: str, note: str) -> None:
            notes[f"{market}:{field}"] = note
            records.append(
                {
                    "ts_kst": ts,
                    "asset": market,
                    "field": field,
                    "value": value,
                    "unit": unit,
                    "window": "EOD",
                    "source": "naver",
                    "quality": "secondary",
                    "url": url,
                    "notes": note,
                }
            )

        register("advance", advance, "issues", "fallback:naver")
        register("decline", decline, "issues", "fallback:naver")
        register("unchanged", unchanged, "issues", "fallback:naver")
        register("trading_value", trading_value, "KRW", trading_note)
        register("limit_up", limit_up, "issues", "fallback:naver")
        register("limit_down", limit_down, "issues", "fallback:naver")

        if math.isnan(trin_value):
            notes[f"{market}:trin"] = trin_note or f"parse_failed:{url},trin_unavailable"
        else:
            register("trin", trin_value, "ratio", "fallback:naver")

        return pd.DataFrame(records), notes

    @staticmethod
    def _fetch_widget_counts(target: date) -> Dict[str, int] | None:
        try:
            response = requests.get(
                "https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd",
                timeout=15,
            )
            response.raise_for_status()
            data = response.json()
        except Exception as exc:  # pragma: no cover
            logger.debug("krx_breadth::_fetch_widget_counts :: %s", exc)
            return None

        try:
            boards = data["result"]["businessDay"]["data"]
        except (KeyError, TypeError):
            return None

        target_text = target.strftime("%Y-%m-%d")
        result: Dict[str, int] = {}
        for board in boards:
            if board.get("bssGp") not in {"KOSPI", "KOSDAQ"}:
                continue
            if board.get("isuTrdDd") != target_text:
                continue
            result[f"{board['bssGp']}:advance"] = int(board.get("upCnt", 0))
            result[f"{board['bssGp']}:decline"] = int(board.get("dnCnt", 0))
            result[f"{board['bssGp']}:unchanged"] = int(board.get("eqCnt", 0))
        return result

    def collect(self, now: datetime) -> BreadthResult:
        target_date, should_wait = determine_target(now)
        wait_enabled = should_wait and os.getenv("SKIP_KRX_WAIT", "0") != "1"
        deadline = time.time() + self._poll_timeout
        last_error: Exception | None = None

        while True:
            try:
                frames: Dict[str, pd.DataFrame] = {}
                notes: Dict[str, str] = {}
                previous_date = _previous_business_day(target_date)
                for market in ("KOSPI", "KOSDAQ"):
                    current = self._fetch_board(target_date, market)
                    prev = self._fetch_board(previous_date, market)
                    aggregated, metric_notes = self._aggregate_market(
                        target_date, market, current, prev
                    )
                    if not aggregated.empty:
                        frames[market] = aggregated
                    notes.update(metric_notes)
                return BreadthResult(frames=frames, notes=notes)
            except Exception as exc:  # pragma: no cover
                last_error = exc
                logger.warning("KRX breadth primary fetch failed: %s", exc)
                if not wait_enabled or time.time() >= deadline:
                    break
                time.sleep(self._poll_seconds)

        # 1차 실패 시 pykrx로 전 종목 스냅샷을 받아 계산한다.
        try:
            frames = {}
            notes = {}
            for market in ("KOSPI", "KOSDAQ"):
                snapshot, actual_date = self._fetch_pykrx_snapshot(target_date, market)
                aggregated, metric_notes = self._aggregate_snapshot(
                    target_date, market, snapshot, actual_date
                )
                if not aggregated.empty:
                    frames[market] = aggregated
                notes.update(metric_notes)
            if frames:
                logger.debug("KRX breadth fallback: using pykrx snapshot")
                return BreadthResult(frames=frames, notes=notes)
        except Exception as exc:  # pragma: no cover - 네트워크/외부 라이브러리 의존
            last_error = exc
            logger.warning("KRX breadth pykrx fallback failed: %s", exc)

        # 2차 실패 시 네이버 시가총액 페이지에서 전종목 데이터를 가져온다.
        try:
            frames = {}
            notes = {}
            for market in ("KOSPI", "KOSDAQ"):
                market_frame = self._fetch_naver_market_sum(market)
                aggregated, metric_notes = self._aggregate_naver_market(
                    target_date, market, market_frame
                )
                if not aggregated.empty:
                    frames[market] = aggregated
                notes.update(metric_notes)
            if frames:
                logger.debug("KRX breadth fallback: using Naver market summary")
                return BreadthResult(frames=frames, notes=notes)
        except Exception as exc:  # pragma: no cover - 네트워크/HTML 의존
            last_error = exc
            logger.warning("KRX breadth Naver fallback failed: %s", exc)

        notes: Dict[str, str] = {}
        frames: Dict[str, pd.DataFrame] = {}
        widget_counts = self._fetch_widget_counts(target_date)
        url = "https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd"
        if widget_counts:
            logger.debug("KRX breadth fallback: using widget counts")
            for market in ("KOSPI", "KOSDAQ"):
                advance = widget_counts.get(f"{market}:advance")
                decline = widget_counts.get(f"{market}:decline")
                unchanged = widget_counts.get(f"{market}:unchanged")
                if advance is None or decline is None or unchanged is None:
                    continue
                ts = datetime.combine(target_date, dtime(hour=15, minute=30), tzinfo=KST)
                records = [
                    {
                        "ts_kst": ts,
                        "asset": market,
                        "field": "advance",
                        "value": float(advance),
                        "unit": "issues",
                        "window": "EOD",
                        "source": "krx-widget",
                        "quality": "secondary",
                        "url": url,
                        "notes": "fallback:widget",
                    },
                    {
                        "ts_kst": ts,
                        "asset": market,
                        "field": "decline",
                        "value": float(decline),
                        "unit": "issues",
                        "window": "EOD",
                        "source": "krx-widget",
                        "quality": "secondary",
                        "url": url,
                        "notes": "fallback:widget",
                    },
                    {
                        "ts_kst": ts,
                        "asset": market,
                        "field": "unchanged",
                        "value": float(unchanged),
                        "unit": "issues",
                        "window": "EOD",
                        "source": "krx-widget",
                        "quality": "secondary",
                        "url": url,
                        "notes": "fallback:widget",
                    },
                ]
                frames[market] = pd.DataFrame(records)
                notes[f"{market}:advance"] = "fallback:widget"
                notes[f"{market}:decline"] = "fallback:widget"
                notes[f"{market}:unchanged"] = "fallback:widget"
            for key in ["limit_up", "limit_down", "trading_value", "trin"]:
                notes[f"KOSPI:{key}"] = f"parse_failed:{url},fallback_missing"
                notes[f"KOSDAQ:{key}"] = f"parse_failed:{url},fallback_missing"
        else:
            reason = str(last_error) if last_error else "unknown"
            logger.error("KRX breadth fallback failed: %s", reason)
            for market in ("KOSPI", "KOSDAQ"):
                for key in [
                    "advance",
                    "decline",
                    "unchanged",
                    "limit_up",
                    "limit_down",
                    "trading_value",
                    "trin",
                ]:
                    notes[f"{market}:{key}"] = f"parse_failed:{ENDPOINT_URL},{reason}"

        return BreadthResult(frames=frames, notes=notes)
