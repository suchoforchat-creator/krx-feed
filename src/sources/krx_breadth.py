"""KRX EOD 등락/거래대금 수집기."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from typing import Dict, Tuple

import pandas as pd

from ..utils import KST
from .krx_client import KrxClient


logger = logging.getLogger(__name__)


def _is_business_day(target: date) -> bool:
    """주말을 제외한 영업일 판정 (공휴일은 별도 관리 필요)."""

    return target.weekday() < 5


def _previous_business_day(target: date) -> date:
    """가장 가까운 이전 영업일을 반환한다."""

    current = target - timedelta(days=1)
    while not _is_business_day(current):
        current -= timedelta(days=1)
    return current


def determine_target(now: datetime) -> Tuple[date, bool]:
    """배치 실행 시각에 따라 타깃 영업일과 신선도 대기 여부를 결정한다."""

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
        # 그 외 시간대는 가장 최근 영업일로 맞춘다.
        target = _previous_business_day(current_date) if current_time < dtime(15, 30) else current_date

    return target, should_wait


@dataclass
class BreadthResult:
    frames: Dict[str, pd.DataFrame]
    notes: Dict[str, str]


class KRXBreadthCollector:
    """KRX 전종목 등락률 테이블을 파싱해 A/D/거래대금을 집계한다."""

    MENU_ID = "MDC0201020102"
    BLD = "dbms/MDC/STAT/standard/MDCSTAT01602"

    def __init__(self, client: KrxClient | None = None, *, poll_seconds: int = 20, poll_timeout: int = 480) -> None:
        self._client = client or KrxClient()
        self._poll_seconds = poll_seconds
        self._poll_timeout = poll_timeout

    def _fetch_board(self, target_date: date, market: str) -> pd.DataFrame:
        """시장(KOSPI/KOSDAQ)별 전종목 데이터를 내려받는다."""

        market_map = {"KOSPI": "STK", "KOSDAQ": "KSQ"}
        payload = {
            "strtDd": target_date.strftime("%Y%m%d"),
            "endDd": target_date.strftime("%Y%m%d"),
            "mktId": market_map[market],
            "adjStkPrc": "1",
        }
        raw = self._client.fetch_json(self.MENU_ID, self.BLD, payload)
        rows = raw.get("OutBlock_1") or raw.get("output") or []
        frame = pd.DataFrame(rows)
        if frame.empty:
            raise ValueError("empty_frame")
        return frame

    @staticmethod
    def _clean_numeric(series: pd.Series) -> pd.Series:
        """천단위 구분자를 제거하고 숫자로 변환."""

        return pd.to_numeric(series.astype(str).str.replace(",", ""), errors="coerce")

    def _summarise(self, frame: pd.DataFrame, market: str, target_date: date) -> pd.DataFrame:
        """전종목 데이터를 A/D/거래대금으로 요약한다."""

        allowed_market = {"KOSPI", "KOSDAQ"}
        if market not in allowed_market:
            raise ValueError(f"unsupported market {market}")

        exclusions = {"EF", "EN", "EW", "KO", "IF", "MF", "RT", "DR"}
        if "SECUGRP_ID" in frame.columns:
            frame = frame.loc[~frame["SECUGRP_ID"].isin(exclusions)]

        close = self._clean_numeric(frame.get("TDD_CLSPRC", pd.Series(dtype=float)))
        base = self._clean_numeric(frame.get("BAS_PRC", pd.Series(dtype=float)))
        diff = close - base

        adv = int((diff > 0).sum())
        dec = int((diff < 0).sum())
        unch = int((diff == 0).sum())

        turnover_raw = self._clean_numeric(frame.get("ACC_TRDVAL", pd.Series(dtype=float)))
        turnover = float(turnover_raw.sum() / 1_000_000_000)

        ts = datetime.combine(target_date, dtime(15, 30), tzinfo=KST)
        url = f"https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId={self.MENU_ID}"

        data = [
            {
                "ts_kst": ts,
                "asset": market,
                "field": "adv_count",
                "value": adv,
                "unit": "count",
                "source": "krx",
                "quality": "final",
                "url": url,
            },
            {
                "ts_kst": ts,
                "asset": market,
                "field": "dec_count",
                "value": dec,
                "unit": "count",
                "source": "krx",
                "quality": "final",
                "url": url,
            },
            {
                "ts_kst": ts,
                "asset": market,
                "field": "unch_count",
                "value": unch,
                "unit": "count",
                "source": "krx",
                "quality": "final",
                "url": url,
            },
            {
                "ts_kst": ts,
                "asset": market,
                "field": "turnover",
                "value": turnover,
                "unit": "krw_bn",
                "source": "krx",
                "quality": "final",
                "url": url,
            },
        ]

        return pd.DataFrame(data)

    def collect(self, now: datetime) -> BreadthResult:
        """현재 시각 기준으로 타깃 일자를 결정하고 데이터를 수집한다."""

        target_date, should_wait = determine_target(now)
        wait_enabled = should_wait and os.getenv("SKIP_KRX_WAIT", "0") != "1"

        deadline = time.time() + self._poll_timeout
        notes: Dict[str, str] = {}
        frames: Dict[str, pd.DataFrame] = {}

        while True:
            try:
                kospi_raw = self._fetch_board(target_date, "KOSPI")
                kosdaq_raw = self._fetch_board(target_date, "KOSDAQ")
                frames["KOSPI"] = self._summarise(kospi_raw, "KOSPI", target_date)
                frames["KOSDAQ"] = self._summarise(kosdaq_raw, "KOSDAQ", target_date)
                break
            except Exception as exc:  # pragma: no cover - 네트워크 의존
                logger.warning("KRX breadth fetch failed: %s", exc)
                notes["KOSPI:adv"] = f"parse_failed:{self.MENU_ID},{exc}"
                notes["KOSDAQ:adv"] = f"parse_failed:{self.MENU_ID},{exc}"
                notes["KOSPI:dec"] = f"parse_failed:{self.MENU_ID},{exc}"
                notes["KOSDAQ:dec"] = f"parse_failed:{self.MENU_ID},{exc}"
                notes["KOSPI:unch"] = f"parse_failed:{self.MENU_ID},{exc}"
                notes["KOSDAQ:unch"] = f"parse_failed:{self.MENU_ID},{exc}"
                notes["KOSPI:turnover"] = f"parse_failed:{self.MENU_ID},{exc}"
                if not wait_enabled or time.time() >= deadline:
                    return BreadthResult(frames={}, notes=notes)
                time.sleep(self._poll_seconds)
                continue

        return BreadthResult(frames=frames, notes=notes)

