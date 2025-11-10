"""미국 2Y/10Y 국채 수익률 수집기.

요구 사항 정리:
* 선물(ZT/ZN) 기반 값은 금지. 재무부·FRED·MarketWatch 순서로 시도한다.
* 모든 실패는 notes에 `parse_failed:<url>,<reason>` 형식으로 남긴다.
* 값은 퍼센트 단위(예: 4.12)이며 0~20% 범위를 벗어나면 range_violation으로 처리한다.
* 초심자도 이해할 수 있도록 각 단계에 상세 주석과 디버깅 로그를 추가한다.
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


TREASURY_URL = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv"
FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
MARKETWATCH_URLS = {
    "UST2Y": "https://www.marketwatch.com/investing/bond/tmubmusd02y",
    "UST10Y": "https://www.marketwatch.com/investing/bond/tmubmusd10y",
}


@dataclass
class YieldFrame:
    frame: pd.DataFrame
    note: str


class USTYieldCollector:
    """재무부 → FRED → MarketWatch 순으로 2Y/10Y 수익률을 구한다."""

    def __init__(self, session: requests.Session | None = None, timeout: int = 30) -> None:
        self._session = session or requests.Session()
        self._timeout = timeout

    # ------------------------------------------------------------------
    # 1) 미국 재무부 CSV 파싱
    # ------------------------------------------------------------------
    def _fetch_treasury(self, target: date) -> Dict[str, float] | None:
        """재무부 daily treasury CSV에서 타깃 일자의 수익률을 찾는다."""

        params = {"data": "yieldCurve", "field_tdr_date_value": target.strftime("%Y")}
        try:
            response = self._session.get(TREASURY_URL, params=params, timeout=self._timeout)
            response.raise_for_status()
        except Exception as exc:  # pragma: no cover - 네트워크 의존
            logger.debug("USTYieldCollector::_fetch_treasury :: %s", exc)
            return None

        stream = io.StringIO(response.text)
        reader = csv.DictReader(stream)
        result: Dict[str, float] = {}
        target_text = target.strftime("%m/%d/%Y")
        for row in reader:
            if row.get("Date") != target_text:
                continue
            try:
                result["UST2Y"] = float(row.get("2 yr", ""))
                result["UST10Y"] = float(row.get("10 yr", ""))
            except ValueError:
                logger.debug("Treasury row parse failed: %s", row)
            break
        return result if result else None

    # ------------------------------------------------------------------
    # 2) FRED CSV 폴백
    # ------------------------------------------------------------------
    def _fetch_fred(self, series_id: str, target: date) -> float | None:
        """FRED에서 제공하는 CSV를 내려받아 일자별 수익률을 찾는다."""

        url = FRED_URL.format(series_id=series_id)
        try:
            response = self._session.get(url, timeout=self._timeout)
            response.raise_for_status()
        except Exception as exc:  # pragma: no cover - 네트워크 의존
            logger.debug("USTYieldCollector::_fetch_fred :: %s", exc)
            return None

        stream = io.StringIO(response.text)
        reader = csv.DictReader(stream)
        target_text = target.strftime("%Y-%m-%d")
        for row in reader:
            if row.get("DATE") == target_text:
                try:
                    value = float(row.get(series_id, ""))
                except ValueError:
                    return None
                return value
        return None

    # ------------------------------------------------------------------
    # 3) MarketWatch 보조
    # ------------------------------------------------------------------
    def _fetch_marketwatch(self, asset: str) -> float | None:
        """MarketWatch 페이지에서 실시간 수익률을 파싱한다."""

        url = MARKETWATCH_URLS[asset]
        try:
            response = self._session.get(url, timeout=self._timeout)
            response.raise_for_status()
        except Exception as exc:  # pragma: no cover - 네트워크 의존
            logger.debug("USTYieldCollector::_fetch_marketwatch :: %s", exc)
            return None

        try:
            import bs4  # type: ignore
        except Exception:  # pragma: no cover - 의존성 없을 수도 있음
            return None

        soup = bs4.BeautifulSoup(response.text, "lxml")
        value_node = soup.select_one("bg-quote.value")
        if value_node is None:
            return None
        try:
            return float(value_node.text.replace("%", ""))
        except ValueError:
            return None

    # ------------------------------------------------------------------
    def _build_frame(
        self,
        asset: str,
        value: float | None,
        source: str,
        url: str,
        quality: str,
        note: str,
        target: date,
    ) -> YieldFrame:
        """공통 DataFrame과 노트를 생성한다."""

        ts = datetime.combine(target, dtime(hour=17, minute=0), tzinfo=KST)
        if value is None:
            return YieldFrame(frame=pd.DataFrame(), note=note)

        if not 0 <= float(value) <= 20:
            logger.debug(
                "USTYieldCollector::_build_frame :: range_violation asset=%s value=%.4f", asset, value
            )
            return YieldFrame(frame=pd.DataFrame(), note=f"range_violation:{url},0-20pct")

        frame = pd.DataFrame(
            {
                "ts_kst": [ts],
                "asset": [asset],
                "field": ["yield"],
                "value": [float(value)],
                "unit": ["pct"],
                "window": ["1D"],
                "source": [source],
                "quality": [quality],
                "url": [url],
                "notes": [note],
            }
        )
        return YieldFrame(frame=frame, note="")

    def collect(self, target: date) -> Tuple[Dict[str, pd.DataFrame], Dict[str, str]]:
        """지정 일자의 UST2Y/UST10Y 수익률을 가져와 DataFrame과 노트를 돌려준다."""

        frames: Dict[str, pd.DataFrame] = {}
        notes: Dict[str, str] = {}

        treasury_data = self._fetch_treasury(target)

        for asset, series_id in {"UST2Y": "DGS2", "UST10Y": "DGS10"}.items():
            value = treasury_data.get(asset) if treasury_data else None
            if value is not None:
                result = self._build_frame(asset, value, "treasury", TREASURY_URL, "final", "", target)
            else:
                fred_value = self._fetch_fred(series_id, target)
                if fred_value is not None:
                    result = self._build_frame(asset, fred_value, "fred", FRED_URL.format(series_id=series_id), "secondary", "", target)
                else:
                    mw_value = self._fetch_marketwatch(asset)
                    if mw_value is not None:
                        result = self._build_frame(asset, mw_value, "marketwatch", MARKETWATCH_URLS[asset], "secondary", "", target)
                    else:
                        reason = "treasury_fred_marketwatch_failed"
                        notes[f"{asset}:yield"] = f"parse_failed:{TREASURY_URL},{reason}"
                        logger.warning("UST yield fetch failed for %s", asset)
                        result = YieldFrame(frame=pd.DataFrame(), note="")

            if result.note:
                notes[f"{asset}:yield"] = result.note
            if not result.frame.empty:
                frames[asset] = result.frame

        return frames, notes

