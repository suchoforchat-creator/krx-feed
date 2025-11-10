"""KRX 전종목 등락률 데이터를 이용해 EOD 브레드스 지표를 집계한다.

이 모듈은 사용자가 디버깅하기 쉽도록 다음과 같은 원칙을 따른다.

1. **명확한 주석** – 각 함수마다 어떤 데이터를 다루는지, 실패 시 어떤 경로로
   폴백을 시도하는지 자세히 설명한다.
2. **디버깅 로그** – 예상치 못한 값이 계산되면 logger.debug로 즉시 남겨
   후속 분석이 가능하게 한다.
3. **노트 규칙 준수** – 모든 실패는 `parse_failed:<url>,<reason>` 형식으로
   기록해 latest/history에서 원인 추적이 가능하다.

실제 실행 환경에서는 Playwright를 이용해 `getJsonData.cmd` 요청의 `bld`와
파라미터를 추출한 뒤, 아래 상수에 캐시해 두고 사용한다. 테스트 환경에서는
상수가 그대로 사용되므로 네트워크 호출 없이도 동작한다.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from typing import Dict, Tuple

import pandas as pd
import requests

from ..utils import KST
from .krx_client import KrxClient


logger = logging.getLogger(__name__)


# Playwright로 확보한 엔드포인트 메타데이터. bld가 바뀌면 discovery를 통해
# 업데이트해야 한다. discovery 절차: 브라우저에서 메뉴 페이지를 열고
# `getJsonData.cmd` 요청을 캡처한 뒤, 아래 상수를 수정한다.
KRX_ENDPOINTS = {
    "MDC0201020102": {
        "bld": "dbms/MDC/STAT/standard/MDCSTAT01602",
        "params": {
            "adjStkPrc": "1",
        },
    }
}


EXCLUDED_SECURITY_GROUPS = {"EF", "EN", "EW", "KO", "IF", "MF", "RT", "DR"}


def _is_business_day(target: date) -> bool:
    """주말을 제외한 영업일 여부를 판정한다."""

    return target.weekday() < 5


def _previous_business_day(target: date) -> date:
    """가장 가까운 이전 영업일을 반환한다."""

    current = target - timedelta(days=1)
    while not _is_business_day(current):
        current -= timedelta(days=1)
    return current


def determine_target(now: datetime) -> Tuple[date, bool]:
    """배치 실행 시각으로부터 타깃 영업일과 신선도 대기 여부를 계산한다."""

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
        target = _previous_business_day(current_date) if current_time < dtime(15, 30) else current_date

    return target, should_wait


@dataclass
class BreadthResult:
    frames: Dict[str, pd.DataFrame]
    notes: Dict[str, str]


class KRXBreadthCollector:
    """KRX 전종목 등락률 표에서 A/D·거래대금·TRIN을 계산한다."""

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

    # ------------------------------------------------------------------
    # 데이터 취득 헬퍼
    # ------------------------------------------------------------------
    def _endpoint_payload(self, menu_id: str, target: date, market: str) -> Dict[str, str]:
        """Playwright로 확보한 form 데이터를 target date에 맞춰 조립한다."""

        endpoint = KRX_ENDPOINTS.get(menu_id)
        if endpoint is None:
            raise KeyError(f"endpoint_missing:{menu_id}")
        params = dict(endpoint.get("params", {}))
        params.update({
            "strtDd": target.strftime("%Y%m%d"),
            "endDd": target.strftime("%Y%m%d"),
            "mktId": {"KOSPI": "STK", "KOSDAQ": "KSQ"}[market],
        })
        return {"bld": endpoint["bld"], **params}

    def _fetch_board(self, target: date, market: str) -> pd.DataFrame:
        """KRX JSON API에서 시장별 전종목 데이터를 내려받는다."""

        payload = self._endpoint_payload(self.MENU_ID, target, market)
        raw = self._client.fetch_json(self.MENU_ID, payload.pop("bld"), payload)
        rows = raw.get("output") or raw.get("OutBlock_1") or []
        frame = pd.DataFrame(rows)
        if frame.empty:
            raise ValueError("empty_frame")
        return frame

    # ------------------------------------------------------------------
    # 데이터 정규화 및 집계
    # ------------------------------------------------------------------
    @staticmethod
    def _clean_numeric(series: pd.Series) -> pd.Series:
        """천단위 구분자를 제거하고 float로 변환한다."""

        return pd.to_numeric(series.astype(str).str.replace(",", ""), errors="coerce")

    def _filter_common_shares(self, frame: pd.DataFrame) -> pd.DataFrame:
        """보통주만 남기고 ETF·ETN 등은 제외한다."""

        if "SECUGRP_ID" in frame.columns:
            frame = frame.loc[~frame["SECUGRP_ID"].isin(EXCLUDED_SECURITY_GROUPS)].copy()
        if "INVST_TP_NM" in frame.columns:
            frame = frame.loc[~frame["INVST_TP_NM"].astype(str).str.contains("ETF|ETN|ELW|KONEX", na=False)]
        if "SRTSLSYN" in frame.columns:
            frame = frame.loc[frame["SRTSLSYN"].astype(str) != "Y"]
        return frame

    def _limit_flags(self, frame: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
        """상·하한가 여부를 계산한다."""

        close = self._clean_numeric(frame.get("TDD_CLSPRC", pd.Series(dtype=float)))
        upper = self._clean_numeric(frame.get("UPLMT_PRC", pd.Series(dtype=float)))
        lower = self._clean_numeric(frame.get("LWLMT_PRC", pd.Series(dtype=float)))

        # limit flag가 별도 문자열로 제공될 때를 대비해 텍스트 열도 확인한다.
        limit_text = frame.get("ETC_TP_NM", pd.Series(dtype=str)).astype(str)

        up_mask = (upper > 0) & (close >= upper)
        down_mask = (lower > 0) & (close <= lower)

        up_mask |= limit_text.str.contains("상한", na=False)
        down_mask |= limit_text.str.contains("하한", na=False)

        return up_mask, down_mask

    def _aggregate_market(self, frame: pd.DataFrame, market: str, target_date: date) -> pd.DataFrame:
        """시장별로 집계한 결과를 raw 프레임으로 반환한다."""

        frame = self._filter_common_shares(frame)
        close = self._clean_numeric(frame.get("TDD_CLSPRC", pd.Series(dtype=float)))
        base = self._clean_numeric(frame.get("BAS_PRC", pd.Series(dtype=float)))
        diff = close - base

        advance_mask = diff > 0
        decline_mask = diff < 0
        unchanged_mask = diff == 0

        volume = self._clean_numeric(frame.get("ACC_TRDVOL", pd.Series(dtype=float))).fillna(0)
        turnover = self._clean_numeric(frame.get("ACC_TRDVAL", pd.Series(dtype=float))).fillna(0)

        limit_up_mask, limit_down_mask = self._limit_flags(frame)

        advance_count = int(advance_mask.sum())
        decline_count = int(decline_mask.sum())
        unchanged_count = int(unchanged_mask.sum())
        limit_up_count = int(limit_up_mask.sum())
        limit_down_count = int(limit_down_mask.sum())
        trading_value = float(turnover.sum())

        advance_volume = float(volume[advance_mask].sum())
        decline_volume = float(volume[decline_mask].sum())

        trin = float("nan")
        if all(val > 0 for val in [advance_count, decline_count, advance_volume, decline_volume]):
            trin = (advance_count / decline_count) / (advance_volume / decline_volume)
            if not 0.1 <= trin <= 10:
                logger.debug(
                    "krx_breadth::_aggregate_market :: suspicious TRIN (market=%s, value=%.4f)",
                    market,
                    trin,
                )

        ts = datetime.combine(target_date, dtime(hour=15, minute=30), tzinfo=KST)
        url = f"https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId={self.MENU_ID}"

        def build(field: str, value: float, unit: str, note: str = "", *, quality: str = "final") -> Dict[str, object]:
            """공통 레코드를 생성한다. note가 비어 있으면 나중에 채운다."""

            return {
                "ts_kst": ts,
                "asset": market,
                "field": field,
                "value": value,
                "unit": unit,
                "window": "EOD",
                "source": "krx",
                "quality": quality,
                "url": url,
                "notes": note,
            }

        records = [
            build("advance", float(advance_count), "issues"),
            build("decline", float(decline_count), "issues"),
            build("unchanged", float(unchanged_count), "issues"),
            build("limit_up", float(limit_up_count), "issues"),
            build("limit_down", float(limit_down_count), "issues"),
            build("trading_value", trading_value, "KRW"),
            build("advance_volume", advance_volume, "shares"),
            build("decline_volume", decline_volume, "shares"),
            build("trin", trin, "ratio"),
        ]

        return pd.DataFrame(records)

    # ------------------------------------------------------------------
    # 폴백 로직
    # ------------------------------------------------------------------
    @staticmethod
    def _fetch_widget_counts(target: date) -> Dict[str, int] | None:
        """KRX 메인 위젯에서 상승/보합/하락 수치를 가져온다."""

        try:
            response = requests.get(
                "https://data.krx.co.kr/contents/MDC/MAIN/main/index.cmd",
                timeout=15,
            )
            response.raise_for_status()
            data = response.json()
        except Exception as exc:  # pragma: no cover - 네트워크 의존
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

    # ------------------------------------------------------------------
    # 외부 인터페이스
    # ------------------------------------------------------------------
    def collect(self, now: datetime) -> BreadthResult:
        """지정 시각 기준으로 KOSPI/KOSDAQ EOD 브레드스 지표를 수집한다."""

        target_date, should_wait = determine_target(now)
        wait_enabled = should_wait and os.getenv("SKIP_KRX_WAIT", "0") != "1"
        deadline = time.time() + self._poll_timeout
        last_error: Exception | None = None

        while True:
            try:
                frames = {}
                notes: Dict[str, str] = {}
                for market in ("KOSPI", "KOSDAQ"):
                    board = self._fetch_board(target_date, market)
                    frames[market] = self._aggregate_market(board, market, target_date)
                return BreadthResult(frames=frames, notes=notes)
            except Exception as exc:  # pragma: no cover - 네트워크 의존
                last_error = exc
                logger.warning("KRX breadth primary fetch failed: %s", exc)
                if not wait_enabled or time.time() >= deadline:
                    break
                time.sleep(self._poll_seconds)

        # 1차 시도가 모두 실패했을 때 폴백으로 메인 위젯을 시도한다.
        notes = {}
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
            for key in ["limit_up", "limit_down", "trading_value", "trin", "advance_volume", "decline_volume"]:
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
                    "advance_volume",
                    "decline_volume",
                    "trin",
                ]:
                    notes[f"{market}:{key}"] = f"parse_failed:{self.MENU_ID},{reason}"

        return BreadthResult(frames=frames, notes=notes)

