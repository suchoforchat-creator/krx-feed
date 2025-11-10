"""KRX 전종목 등락률 메뉴에서 A/D·TRIN·거래대금·상/하한가를 산출한다.

초심자도 이해하기 쉽도록 각 함수에 상세 주석을 달았고, 예상과 다른
값이 나오면 즉시 디버깅할 수 있도록 로그와 노트 체계를 통일했다.
성공 시에도 ``notes="ok"``를 채워 최신 CSV에서 상태를 쉽게 확인할 수 있다.
"""

from __future__ import annotations

import logging
import math
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
ㅈㅈㅈfrom typing import Dict, Iterable, List, Tuple

import pandas as pd
import requests

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
