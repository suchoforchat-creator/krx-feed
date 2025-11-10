"""DXY 지수 수집기.

ICE → MarketWatch → TradingView 순으로 시도하며, 실패 시 parse_failed 노트를 남긴다.
코드는 초심자도 이해할 수 있도록 상세 주석과 디버깅 로그를 포함한다.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, time as dtime
from typing import Dict, Tuple

import pandas as pd
import requests

from ..utils import KST


logger = logging.getLogger(__name__)


ICE_URL = "https://www.theice.com/api/productguide/marketdata/contract/DX"
MARKETWATCH_URL = "https://www.marketwatch.com/investing/index/dxy"
TRADINGVIEW_URL = "https://symbol-search.tradingview.com/symbol_search/?text=DXY&hl=1"


@dataclass
class DXYFrame:
    frame: pd.DataFrame
    note: str


class DXYCollector:
    """DXY 값을 여러 소스에서 시도하는 수집기."""

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
        """공통 DataFrame을 생성하고 범위 체크를 수행한다."""

        ts = datetime.combine(target, dtime(hour=17, minute=0), tzinfo=KST)
        if value is None:
            return DXYFrame(frame=pd.DataFrame(), note=note)
        if not 70 <= float(value) <= 130:
            logger.debug(
                "DXYCollector::_build_frame :: range_violation value=%.4f", value
            )
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
        return DXYFrame(frame=frame, note="")

    def _fetch_ice(self) -> float | None:
        """ICE Product Guide API를 시도한다."""

        try:
            response = self._session.get(ICE_URL, timeout=self._timeout)
            response.raise_for_status()
            data = response.json()
        except Exception as exc:  # pragma: no cover - 네트워크 의존
            logger.debug("DXYCollector::_fetch_ice :: %s", exc)
            return None

        try:
            return float(data["last"])
        except (KeyError, TypeError, ValueError):
            logger.debug("DXYCollector::_fetch_ice :: unexpected payload %s", data)
            return None

    def _fetch_marketwatch(self) -> float | None:
        """MarketWatch HTML에서 지수를 파싱한다."""

        try:
            response = self._session.get(MARKETWATCH_URL, timeout=self._timeout)
            response.raise_for_status()
        except Exception as exc:  # pragma: no cover - 네트워크 의존
            logger.debug("DXYCollector::_fetch_marketwatch :: %s", exc)
            return None

        try:
            import bs4  # type: ignore
        except Exception:  # pragma: no cover - 의존성 없을 수도 있음
            return None

        soup = bs4.BeautifulSoup(response.text, "lxml")
        node = soup.select_one("bg-quote.value")
        if node is None:
            return None
        text = node.text.replace(",", "")
        try:
            return float(text)
        except ValueError:
            return None

    def _fetch_tradingview(self) -> float | None:
        """TradingView 심볼 검색 응답에서 값을 추출한다."""

        try:
            response = self._session.get(TRADINGVIEW_URL, timeout=self._timeout)
            response.raise_for_status()
            data = response.json()
        except Exception as exc:  # pragma: no cover - 네트워크 의존
            logger.debug("DXYCollector::_fetch_tradingview :: %s", exc)
            return None

        try:
            first = data["symbols"][0]
            value = first.get("close")
            if value is None:
                return None
            return float(value)
        except (KeyError, IndexError, TypeError, ValueError):
            return None

    def collect(self, target: date) -> Tuple[pd.DataFrame, Dict[str, str]]:
        """DXY 지수를 수집하고 (데이터프레임, 노트)를 반환한다."""

        notes: Dict[str, str] = {}

        ice_value = self._fetch_ice()
        if ice_value is not None:
            result = self._build_frame(ice_value, source="ice", quality="final", url=ICE_URL, note="", target=target)
            if result.note:
                notes["DXY:idx"] = result.note
            return result.frame, notes

        marketwatch_value = self._fetch_marketwatch()
        if marketwatch_value is not None:
            note = "fallback:marketwatch"
            result = self._build_frame(
                marketwatch_value,
                source="marketwatch",
                quality="secondary",
                url=MARKETWATCH_URL,
                note=note,
                target=target,
            )
            if result.note:
                notes["DXY:idx"] = result.note
            else:
                notes["DXY:idx"] = note
            return result.frame, notes

        tradingview_value = self._fetch_tradingview()
        if tradingview_value is not None:
            note = "fallback:tradingview"
            result = self._build_frame(
                tradingview_value,
                source="tradingview",
                quality="secondary",
                url=TRADINGVIEW_URL,
                note=note,
                target=target,
            )
            if result.note:
                notes["DXY:idx"] = result.note
            else:
                notes["DXY:idx"] = note
            return result.frame, notes

        notes["DXY:idx"] = f"parse_failed:{ICE_URL},all_sources_failed"
        return pd.DataFrame(), notes
