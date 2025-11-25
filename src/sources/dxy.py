"""DXY(달러 인덱스) 수집기.

v5 요구 사항에 맞춰 **Stooq → MarketWatch → TradingView** 순서로 시도하고,
각 단계에 자세한 주석과 디버깅 로그를 남겨 초심자도 문제를 추적할 수
있도록 구성했다. 모든 요청은 동일한 User-Agent/Accept-Language/Referer를
사용하며, 실패 시 ``notes``에 ``parse_failed:<url>,<reason>`` 형식으로 반드시
사유를 남긴다.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dtime
from typing import Dict, Iterable, Tuple

import pandas as pd
import requests

from ..utils import KST


logger = logging.getLogger(__name__)


STOOQ_PRIMARY_URL = "https://stooq.com/q/d/l/?s=usdidx&i=d"
STOOQ_SECONDARY_URL = "https://stooq.com/q/d/l/?s=dxy&i=d"
MARKETWATCH_URL = "https://www.marketwatch.com/investing/index/dxy"
TRADINGVIEW_URL = "https://www.tradingview.com/symbols/TVC-DXY/"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.marketwatch.com/",
}


@dataclass
class DXYFrame:
    frame: pd.DataFrame
    note: str


class DXYCollector:
    """DXY 지수를 다양한 공개 소스에서 추출한다."""

    def __init__(self, session: requests.Session | None = None, timeout: int = 20) -> None:
        self._session = session or requests.Session()
        # 모든 요청이 동일한 헤더를 사용하도록 기본 헤더를 설정한다.
        self._session.headers.update(DEFAULT_HEADERS)
        self._timeout = timeout

    # ------------------------------------------------------------------
    # 디버깅 도우미: context와 추가 정보를 함께 로그로 남긴다.
    # ------------------------------------------------------------------
    def _debug(self, context: str, **extra: object) -> None:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("DXYCollector::%s %s", context, extra)

    # ------------------------------------------------------------------
    # 공통 요청 함수: 최대 2회 재시도(총 3회)를 하면서 지수 백오프를 적용한다.
    # ------------------------------------------------------------------
    def _request(self, url: str) -> requests.Response | None:
        delay_schedule = [0, 2, 6]
        for attempt, delay in enumerate(delay_schedule, start=1):
            if delay:
                # 초심자가 흐름을 이해할 수 있도록 대기 시간도 로그로 남긴다.
                self._debug("request_sleep", url=url, delay=delay, attempt=attempt)
                time.sleep(delay)
            try:
                response = self._session.get(url, timeout=self._timeout)
                response.raise_for_status()
                self._debug("request_success", url=url, attempt=attempt)
                return response
            except Exception as exc:  # pragma: no cover - 네트워크 예외
                self._debug("request_failed", url=url, attempt=attempt, error=str(exc))
        return None

    def _build_frame(
        self,
        value: float | None,
        *,
        source: str,
        quality: str,
        url: str,
        note: str,
        target: date,
    ) -> DXYFrame:
        ts = datetime.combine(target, dtime(hour=17, minute=0), tzinfo=KST)
        if value is None:
            return DXYFrame(frame=pd.DataFrame(), note=note)
        if not 70 <= float(value) <= 130:
            self._debug("range_violation", value=value, url=url)
            return DXYFrame(frame=pd.DataFrame(), note=f"range_violation:{url},70-130")
        frame = pd.DataFrame(
            {
                "ts_kst": [ts],
                "asset": ["DXY"],
                "field": ["idx"],
                "value": [float(value)],
                "unit": ["idx"],
                "window": ["1D"],
                "source": [source],
                "quality": [quality],
                "url": [url],
                "notes": [note],
            }
        )
        return DXYFrame(frame=frame, note=note or "ok")

    # ------------------------------------------------------------------
    # 1) Stooq CSV 파서: 1차/2차 URL을 순서대로 시도한다.
    # ------------------------------------------------------------------
    def _fetch_stooq(self, urls: Iterable[str]) -> Tuple[float | None, str | None]:
        for url in urls:
            response = self._request(url)
            if response is None:
                continue
            rows = [line.strip().split(",") for line in response.text.splitlines() if line.strip()]
            if len(rows) < 2:
                self._debug("stooq_no_rows", url=url)
                continue
            header, *data_rows = rows
            if "Close" not in header:
                self._debug("stooq_no_close", url=url, header=header)
                continue
            close_index = header.index("Close")
            # 역순으로 스캔하여 가장 최근 비어 있지 않은 값을 찾는다.
            for row in reversed(data_rows):
                if len(row) <= close_index or not row[close_index].strip():
                    continue
                try:
                    value = float(row[close_index].replace(",", ""))
                    self._debug("stooq_success", url=url, value=value)
                    return value, url
                except ValueError:
                    self._debug("stooq_value_error", url=url, raw=row[close_index])
                    continue
        return None, None

    # ------------------------------------------------------------------
    # 2) MarketWatch HTML 파서: 여러 셀렉터를 순서대로 시도한다.
    # ------------------------------------------------------------------
    def _fetch_marketwatch(self) -> float | None:
        response = self._request(MARKETWATCH_URL)
        if response is None:
            return None

        try:
            import bs4  # type: ignore
        except Exception:  # pragma: no cover - 선택적 의존성
            self._debug("marketwatch_missing_bs4")
            return None

        soup = bs4.BeautifulSoup(response.text, "lxml")
        candidates = [
            soup.select_one("bg-quote.value"),
            soup.select_one("meta[name='price']"),
            soup.select_one(".intraday__price span"),
        ]
        for node in candidates:
            if node is None:
                continue
            text = node.get("content") if node.name == "meta" else node.text
            if not text:
                continue
            cleaned = text.strip().replace(",", "")
            try:
                value = float(cleaned)
                self._debug("marketwatch_success", value=value)
                return value
            except ValueError:
                self._debug("marketwatch_value_error", raw=text)
                continue
        return None

    # ------------------------------------------------------------------
    # 3) TradingView 파서: __NEXT_DATA__ JSON에서 close 값을 찾는다.
    # ------------------------------------------------------------------
    def _fetch_tradingview(self) -> float | None:
        response = self._request(TRADINGVIEW_URL)
        if response is None:
            return None

        try:
            import bs4  # type: ignore
        except Exception:  # pragma: no cover
            self._debug("tradingview_missing_bs4")
            return None

        soup = bs4.BeautifulSoup(response.text, "lxml")
        script_node = soup.select_one("script#__NEXT_DATA__")
        if script_node is None or not script_node.text:
            self._debug("tradingview_no_script")
            return None

        try:
            data = json.loads(script_node.text)
            ticker = data["props"]["pageProps"]["symbols"][0]
            value = float(ticker["lp"])
            self._debug("tradingview_success", value=value)
            return value
        except Exception as exc:  # pragma: no cover - JSON 구조 변경 대비
            self._debug("tradingview_parse_error", error=str(exc))
            return None

    def collect(self, target: date) -> Tuple[pd.DataFrame, Dict[str, str]]:
        notes: Dict[str, str] = {}
        stooq_value, stooq_url = self._fetch_stooq(
            [STOOQ_PRIMARY_URL, STOOQ_SECONDARY_URL]
        )
        if stooq_value is not None and stooq_url is not None:
            result = self._build_frame(
                stooq_value,
                source="stooq",
                quality="secondary",
                url=stooq_url,
                note="ok:stooq",
                target=target,
            )
            notes["DXY:idx"] = result.note or "ok:stooq"
            return result.frame, notes

        marketwatch_value = self._fetch_marketwatch()
        if marketwatch_value is not None:
            result = self._build_frame(
                marketwatch_value,
                source="marketwatch",
                quality="secondary",
                url=MARKETWATCH_URL,
                note="fallback:marketwatch",
                target=target,
            )
            notes["DXY:idx"] = result.note or "fallback:marketwatch"
            return result.frame, notes

        tradingview_value = self._fetch_tradingview()
        if tradingview_value is not None:
            result = self._build_frame(
                tradingview_value,
                source="tradingview",
                quality="secondary",
                url=TRADINGVIEW_URL,
                note="fallback:tradingview",
                target=target,
            )
            notes["DXY:idx"] = result.note or "fallback:tradingview"
            return result.frame, notes

        failure_note = f"parse_failed:{STOOQ_PRIMARY_URL},all_sources_failed"
        notes["DXY:idx"] = failure_note
        return pd.DataFrame(), notes
