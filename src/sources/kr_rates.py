"""한국 국채 수익률 (KR3Y/KR10Y) 수집기 강화판.

요구 사항 요약
--------------
* KRX 장외 채권수익률 표를 1순위로 사용하고, 실패 시 KOFIA → 한국은행 ECOS
  → Investing.com 순으로 폴백한다.
* 합성 데이터는 금지. 어떤 소스에서도 값을 구하지 못하면 값은 비워 두고
  ``notes="parse_failed:<url>,<reason>"``을 기록한다.
* 0 < 수익률 < 10 범위를 벗어나면 ``range_violation``으로 처리한다.
* 성공 시에도 ``notes="ok"`` 등 명시적인 상태 값을 남겨 디버깅을 돕는다.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from typing import Dict, Optional, Tuple

import pandas as pd
import requests

from ..utils import KST
from .krx_client import KrxClient


logger = logging.getLogger(__name__)


KRX_URL = "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC020104040401"
KOFIA_URL = "https://www.kofiabond.or.kr/websquare/websquare.html?divisionId=MBIS01010010000000"
ECOS_URL = "https://ecos.bok.or.kr/"
INVESTING_URLS = {
    "KR3Y": "https://www.investing.com/rates-bonds/south-korea-3-year-bond-yield",
    "KR10Y": "https://www.investing.com/rates-bonds/south-korea-10-year-bond-yield",
}

YIELD_COLUMNS = ["LST_ORD_BAS_YD", "LST_ORD_YD", "수익률", "YLD", "APPL_YD"]


def _previous_business_day(target: date) -> date:
    current = target - timedelta(days=1)
    while current.weekday() >= 5:
        current -= timedelta(days=1)
    return current


@dataclass
class KrRatesResult:
    frames: Dict[str, pd.DataFrame]
    notes: Dict[str, str]


class KRXKorRates:
    MENU_ID = "MDC020104040401"
    BLD = "dbms/MDC/STAT/standard/MDCSTAT11401"

    def __init__(self, client: KrxClient | None = None, session: requests.Session | None = None) -> None:
        self._client = client or KrxClient()
        self._session = session or requests.Session()

    @staticmethod
    def _clean(value: object) -> float:
        text = str(value).strip().replace(",", "")
        if text == "" or text.lower() == "nan":
            return float("nan")
        try:
            return float(text)
        except ValueError:
            return float("nan")

    @staticmethod
    def _filter_rows(frame: pd.DataFrame, keyword: str) -> pd.DataFrame:
        def contains(row: pd.Series, token: str) -> bool:
            return row.astype(str).str.contains(token, na=False).any()

        mask_kind = frame.apply(lambda row: contains(row, "국고"), axis=1)
        mask_maturity = frame.apply(lambda row: contains(row, keyword), axis=1)
        return frame.loc[mask_kind & mask_maturity]

    def _select_column(self, frame: pd.DataFrame, candidates) -> Optional[str]:
        for name in candidates:
            if name in frame.columns:
                return name
        return None

    def _fetch_krx_table(self, target: date) -> pd.DataFrame:
        payload = {"trdDd": target.strftime("%Y%m%d"), "inqTpCd": "T"}
        raw = self._client.fetch_json(self.MENU_ID, self.BLD, payload)
        rows = raw.get("output") or raw.get("OutBlock_1") or []
        frame = pd.DataFrame(rows)
        if frame.empty:
            raise ValueError("empty_frame")
        return frame

    def _fetch_krx(self, target: date, asset: str, keyword: str) -> Tuple[Optional[Dict[str, object]], Optional[str]]:
        try:
            frame = self._fetch_krx_table(target)
        except Exception as exc:  # pragma: no cover
            return None, f"parse_failed:{KRX_URL},{exc}"

        subset = self._filter_rows(frame, keyword)
        if subset.empty:
            return None, f"parse_failed:{KRX_URL},missing"

        yield_col = self._select_column(subset, YIELD_COLUMNS)
        if yield_col is None:
            return None, f"parse_failed:{KRX_URL},yield_column_missing"

        value = self._clean(subset[yield_col].iloc[-1])
        if not (0 < value < 10):
            logger.debug("kr_rates::_fetch_krx :: range_violation asset=%s value=%s", asset, value)
            return None, f"range_violation:{KRX_URL},0-10pct"

        prev_date = _previous_business_day(target)
        prev_value = float("nan")
        try:
            prev_frame = self._fetch_krx_table(prev_date)
            prev_subset = self._filter_rows(prev_frame, keyword)
            if not prev_subset.empty:
                prev_col = self._select_column(prev_subset, YIELD_COLUMNS)
                if prev_col:
                    prev_value = self._clean(prev_subset[prev_col].iloc[-1])
        except Exception as exc:  # pragma: no cover
            logger.debug("kr_rates::_fetch_krx previous failed :: %s", exc)
            prev_value = float("nan")

        if not (0 < prev_value < 10):
            prev_value = None

        return (
            {
                "value": float(value),
                "prev": float(prev_value) if prev_value is not None else None,
                "prev_date": prev_date if prev_value is not None else None,
                "source": "krx",
                "quality": "final",
                "url": KRX_URL,
                "note": "ok",
            },
            None,
        )

    def _fetch_kofia(self, target: date, asset: str, keyword: str) -> Tuple[Optional[Dict[str, object]], Optional[str]]:
        try:
            response = self._session.get(KOFIA_URL, timeout=20)
            response.raise_for_status()
        except Exception as exc:  # pragma: no cover
            return None, f"parse_failed:{KOFIA_URL},{exc}"

        try:
            import bs4  # type: ignore
        except Exception:
            return None, f"parse_failed:{KOFIA_URL},bs4_missing"

        soup = bs4.BeautifulSoup(response.text, "lxml")
        text = soup.get_text(" ")
        pattern = re.compile(rf"국고\s*채?\s*{keyword}\s*([0-9]+\.?[0-9]*)")
        match = pattern.search(text)
        if not match:
            return None, f"parse_failed:{KOFIA_URL},pattern_missing"
        value = self._clean(match.group(1))
        if not (0 < value < 10):
            return None, f"range_violation:{KOFIA_URL},0-10pct"
        return (
            {
                "value": float(value),
                "prev": None,
                "prev_date": None,
                "source": "kofia",
                "quality": "final",
                "url": KOFIA_URL,
                "note": "ok",
            },
            None,
        )

    def _fetch_ecos(self, target: date, asset: str, keyword: str) -> Tuple[Optional[Dict[str, object]], Optional[str]]:
        return None, f"parse_failed:{ECOS_URL},auth_required"

    def _fetch_investing(self, target: date, asset: str, keyword: str) -> Tuple[Optional[Dict[str, object]], Optional[str]]:
        url = INVESTING_URLS[asset]
        try:
            response = self._session.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
            response.raise_for_status()
        except Exception as exc:  # pragma: no cover
            return None, f"parse_failed:{url},{exc}"

        try:
            import bs4  # type: ignore
        except Exception:
            return None, f"parse_failed:{url},bs4_missing"

        soup = bs4.BeautifulSoup(response.text, "lxml")
        price_node = soup.select_one(".instrument-price_last__KQzyA") or soup.select_one("span[data-test='instrument-price-last']")
        if price_node is None:
            return None, f"parse_failed:{url},node_missing"
        value = self._clean(price_node.text)
        if not (0 < value < 10):
            return None, f"range_violation:{url},0-10pct"
        return (
            {
                "value": float(value),
                "prev": None,
                "prev_date": None,
                "source": "investing",
                "quality": "secondary",
                "url": url,
                "note": "fallback:investing",
            },
            None,
        )

    def _build_frame(self, asset: str, target: date, payload: Dict[str, object]) -> pd.DataFrame:
        ts = datetime.combine(target, dtime(hour=17, minute=0), tzinfo=KST)
        rows = [
            {
                "ts_kst": ts,
                "asset": asset,
                "field": "yield",
                "value": payload["value"],
                "unit": "pct",
                "window": "1D",
                "source": payload["source"],
                "quality": payload["quality"],
                "url": payload["url"],
                "notes": payload["note"],
            }
        ]
        prev_value = payload.get("prev")
        prev_date = payload.get("prev_date")
        if prev_value is not None and isinstance(prev_date, date):
            rows.append(
                {
                    "ts_kst": datetime.combine(prev_date, dtime(hour=17, minute=0), tzinfo=KST),
                    "asset": asset,
                    "field": "yield",
                    "value": prev_value,
                    "unit": "pct",
                    "window": "1D",
                    "source": payload["source"],
                    "quality": payload["quality"],
                    "url": payload["url"],
                    "notes": "historical",
                }
            )
        return pd.DataFrame(rows)

    def fetch(self, target: date) -> KrRatesResult:
        notes: Dict[str, str] = {}
        frames: Dict[str, pd.DataFrame] = {}

        assets = {"KR3Y": "3년", "KR10Y": "10년"}
        for asset, keyword in assets.items():
            payload: Optional[Dict[str, object]] = None
            failure_reason: Optional[str] = None
            for fetcher in (self._fetch_krx, self._fetch_kofia, self._fetch_ecos, self._fetch_investing):
                result, error = fetcher(target, asset, keyword)
                if result is not None:
                    payload = result
                    break
                if error:
                    failure_reason = error
                    logger.debug("kr_rates::fetch fallback asset=%s reason=%s", asset, error)
            if payload is None:
                notes[f"{asset}:yield"] = failure_reason or f"parse_failed:{asset},unknown"
                continue
            frame = self._build_frame(asset, target, payload)
            frames[asset] = frame
            notes[f"{asset}:yield"] = payload.get("note", "ok") or "ok"

        return KrRatesResult(frames=frames, notes=notes)
