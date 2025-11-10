"""DXY(달러 인덱스) 수집기.

ICE API 사용이 금지되었기 때문에 MarketWatch → Stooq → TradingView 순으로
HTML/CSV를 파싱한다. 각 단계는 초심자도 따라 하기 쉽도록 상세 주석과
디버깅 로그를 제공하며, 성공 시에도 ``notes="ok"``를 기록해 상태를 확인할
수 있도록 했다.
"""

from __future__ import annotations

import csv
import io
import logging
from dataclasses import dataclass
from datetime import date, datetime, time as dtime
from typing import Dict, Tuple

import pandas as pd
import requests

from ..utils import KST


logger = logging.getLogger(__name__)


MARKETWATCH_URL = "https://www.marketwatch.com/investing/index/dxy"
STOOQ_URL = "https://stooq.com/q/d/l/?s=usdidx&i=d"
TRADINGVIEW_URL = "https://www.tradingview.com/symbols/TVC-DXY/"
TRADINGVIEW_JSON = "https://symbol-search.tradingview.com/symbol_search/?text=DXY&hl=1"


@dataclass
class DXYFrame:
    frame: pd.DataFrame
    note: str


class DXYCollector:
    """DXY 지수를 다양한 공개 소스에서 추출한다."""

    def __init__(self, session: requests.Session | None = None, timeout: int = 20) -> None:
        self._session = session or requests.Session()
        self._timeout = timeout

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
            logger.debug("DXYCollector::_build_frame :: range_violation value=%.4f", value)
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

    def _fetch_marketwatch(self) -> float | None:
        try:
            response = self._session.get(MARKETWATCH_URL, timeout=self._timeout)
            response.raise_for_status()
        except Exception as exc:  # pragma: no cover
            logger.debug("DXYCollector::_fetch_marketwatch :: %s", exc)
            return None

        try:
            import bs4  # type: ignore
        except Exception:
            return None

        soup = bs4.BeautifulSoup(response.text, "lxml")
        node = soup.select_one("bg-quote.value") or soup.select_one(".intraday__price span")
        if node is None:
            return None
        text = node.text.strip().replace(",", "")
        try:
            return float(text)
        except ValueError:
            return None

    def _fetch_stooq(self) -> float | None:
        try:
            response = self._session.get(STOOQ_URL, timeout=self._timeout)
            response.raise_for_status()
        except Exception as exc:  # pragma: no cover
            logger.debug("DXYCollector::_fetch_stooq :: %s", exc)
            return None

        stream = io.StringIO(response.text)
        reader = csv.DictReader(stream)
        rows = list(reader)
        if not rows:
            return None
        last = rows[-1]
        try:
            return float(last.get("Close", ""))
        except ValueError:
            return None

    def _fetch_tradingview(self) -> float | None:
        try:
            response = self._session.get(TRADINGVIEW_JSON, timeout=self._timeout)
            response.raise_for_status()
            data = response.json()
        except Exception as exc:  # pragma: no cover
            logger.debug("DXYCollector::_fetch_tradingview :: %s", exc)
            return None

        try:
            first = data["symbols"][0]
            return float(first.get("close", ""))
        except (KeyError, IndexError, TypeError, ValueError):
            return None

    def collect(self, target: date) -> Tuple[pd.DataFrame, Dict[str, str]]:
        notes: Dict[str, str] = {}

        marketwatch_value = self._fetch_marketwatch()
        if marketwatch_value is not None:
            result = self._build_frame(
                marketwatch_value,
                source="marketwatch",
                quality="secondary",
                url=MARKETWATCH_URL,
                note="ok",
                target=target,
            )
            notes["DXY:idx"] = result.note or "ok"
            return result.frame, notes

        stooq_value = self._fetch_stooq()
        if stooq_value is not None:
            result = self._build_frame(
                stooq_value,
                source="stooq",
                quality="secondary",
                url=STOOQ_URL,
                note="fallback:stooq",
                target=target,
            )
            notes["DXY:idx"] = result.note or "fallback:stooq"
            return result.frame, notes

        tradingview_value = self._fetch_tradingview()
        if tradingview_value is not None:
            result = self._build_frame(
                tradingview_value,
                source="tradingview",
                quality="secondary",
                url=TRADINGVIEW_URL,
ㅈㅈ                note="fallback:tradingview",
                target=target,
            )
            notes["DXY:idx"] = result.note or "fallback:tradingview"
            return result.frame, notes

        notes["DXY:idx"] = f"parse_failed:{MARKETWATCH_URL},all_sources_failed"
        return pd.DataFrame(), notes
