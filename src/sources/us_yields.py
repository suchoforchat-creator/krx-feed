"""미국 2년/10년 국채 수익률 수집기(v5).

요구 사항
------------
* 선물 프록시(ZT/ZN)는 사용하지 않고 FRED → 재무부 TextView → MarketWatch 순으로 시도한다.
* 모든 네트워크 요청은 공통 헤더(User-Agent, Accept-Language, Referer)를 사용하고,
  최대 2회 재시도(총 3회)와 2초→6초 지수 백오프를 적용한다.
* 성공·실패 여부와 파싱 세부 내용은 ``logger.debug``를 통해 출력해 초심자가 직접
  디버깅할 수 있게 했다.
* 값은 퍼센트 단위(예: ``4.21``)이며 0~10% 범위를 벗어나면 ``range_violation``으로
  처리한다.
"""

from __future__ import annotations

import csv
import io
import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, time as dtime
from typing import Dict, Tuple

import pandas as pd
import requests

from ..utils import KST


logger = logging.getLogger(__name__)


FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
TREASURY_TEXTVIEW_URL = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
    "TextView?type=daily_treasury_yield_curve&field_tdr_date_value={year}"
)
MARKETWATCH_URLS = {
    "UST2Y": "https://www.marketwatch.com/investing/bond/tmubmusd02y",
    "UST10Y": "https://www.marketwatch.com/investing/bond/tmubmusd10y",
}
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.marketwatch.com/",
}


@dataclass
class YieldFrame:
    """파이프라인에서 기대하는 DataFrame과 디버깅 노트를 묶어둔다."""

    frame: pd.DataFrame
    note: str


class USTYieldCollector:
    """미국 2Y/10Y 수익률을 순차적으로 수집한다."""

    SERIES_IDS = {"UST2Y": "DGS2", "UST10Y": "DGS10"}

    def __init__(self, session: requests.Session | None = None, timeout: int = 30) -> None:
        self._session = session or requests.Session()
        self._session.headers.update(DEFAULT_HEADERS)
        self._timeout = timeout

    # ------------------------------------------------------------------
    # 공통 디버그 로거. context 이름과 키워드 인수를 찍어준다.
    # ------------------------------------------------------------------
    def _debug(self, context: str, **extra: object) -> None:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("USTYieldCollector::%s %s", context, extra)

    # ------------------------------------------------------------------
    # 공통 요청 헬퍼: 최대 2회 재시도(총 3회), 2초→6초 백오프.
    # ------------------------------------------------------------------
    def _request(self, url: str) -> requests.Response | None:
        delays = [0, 2, 6]
        for attempt, delay in enumerate(delays, start=1):
            if delay:
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

    # ------------------------------------------------------------------
    # 1) FRED CSV 파서: 최근 7영업일 내에서 유효한 값을 찾는다.
    # ------------------------------------------------------------------
    def _fetch_fred(self, series_id: str) -> Tuple[float | None, str | None]:
        url = FRED_URL.format(series_id=series_id)
        response = self._request(url)
        if response is None:
            return None, None

        reader = list(csv.DictReader(io.StringIO(response.text)))
        for row in reversed(reader[-7:] or reader):
            raw_value = (row.get(series_id) or "").strip()
            if not raw_value or raw_value in {"NaN", "."}:
                continue
            try:
                value = float(raw_value)
            except ValueError:
                self._debug("fred_value_error", row=row)
                continue
            if 0 < value < 10:
                self._debug("fred_success", series=series_id, value=value)
                return value, url
            self._debug("fred_range_violation", series=series_id, value=value)
        return None, None

    # ------------------------------------------------------------------
    # 2) 재무부 TextView HTML 파서: 연도·일자 후퇴 로직 포함.
    # ------------------------------------------------------------------
    def _fetch_treasury_textview(self, target: date) -> Dict[str, float]:
        result: Dict[str, float] = {}
        years = [target.year - offset for offset in range(0, 3)]
        for year in years:
            url = TREASURY_TEXTVIEW_URL.format(year=year)
            response = self._request(url)
            if response is None:
                continue
            try:
                tables = pd.read_html(io.StringIO(response.text))
            except Exception as exc:  # pragma: no cover - HTML 구조 변경 대비
                self._debug("treasury_read_html_error", url=url, error=str(exc))
                continue

            candidate = None
            for table in tables:
                cols = {col.strip(): idx for idx, col in enumerate(table.columns)}
                if "Date" in cols and "2 Yr" in cols and "10 Yr" in cols:
                    candidate = table
                    break
            if candidate is None:
                self._debug("treasury_no_table", url=url)
                continue

            candidate["Date"] = pd.to_datetime(candidate["Date"], errors="coerce")
            candidate = candidate.dropna(subset=["Date"]).set_index("Date")

            for offset in range(0, 6):  # 최대 5영업일 후퇴
                day = target - timedelta(days=offset)
                try:
                    row = candidate.loc[pd.Timestamp(day)]
                except KeyError:
                    continue
                if isinstance(row, pd.DataFrame):
                    row = row.iloc[0]
                for column, asset in {"2 Yr": "UST2Y", "10 Yr": "UST10Y"}.items():
                    try:
                        value = float(row[column])
                    except (TypeError, ValueError):
                        continue
                    if 0 < value < 10:
                        result.setdefault(asset, value)
                if result:
                    self._debug(
                        "treasury_success",
                        url=url,
                        days_back=offset,
                        values={k: round(v, 4) for k, v in result.items()},
                    )
                    return result
        return result

    # ------------------------------------------------------------------
    # 3) MarketWatch HTML 파서: CSS 셀렉터 여러 개를 시도한다.
    # ------------------------------------------------------------------
    def _fetch_marketwatch(self, asset: str) -> Tuple[float | None, str | None]:
        url = MARKETWATCH_URLS[asset]
        response = self._request(url)
        if response is None:
            return None, None

        try:
            import bs4  # type: ignore
        except Exception:  # pragma: no cover
            self._debug("marketwatch_missing_bs4")
            return None, None

        soup = bs4.BeautifulSoup(response.text, "lxml")
        candidates = [
            soup.select_one("bg-quote.value"),
            soup.select_one("meta[name='price']"),
        ]
        for node in candidates:
            if node is None:
                continue
            text = node.get("content") if node.name == "meta" else node.text
            if not text:
                continue
            cleaned = text.strip().replace("%", "").replace(",", "")
            try:
                value = float(cleaned)
            except ValueError:
                self._debug("marketwatch_value_error", asset=asset, raw=text)
                continue
            if 0 < value < 10:
                self._debug("marketwatch_success", asset=asset, value=value)
                return value, url
            self._debug("marketwatch_range_violation", asset=asset, value=value)
        return None, None

    # ------------------------------------------------------------------
    # 공통 프레임 생성기. notes는 빈 문자열이 되지 않도록 기본값을 준다.
    # ------------------------------------------------------------------
    def _build_frame(
        self,
        *,
        asset: str,
        value: float | None,
        source: str,
        url: str,
        quality: str,
        note: str,
        target: date,
    ) -> YieldFrame:
        ts = datetime.combine(target, dtime(hour=17, minute=0), tzinfo=KST)
        if value is None:
            return YieldFrame(frame=pd.DataFrame(), note=note)
        if not 0 < float(value) < 10:
            self._debug("build_frame_range_violation", asset=asset, value=value)
            return YieldFrame(
                frame=pd.DataFrame(),
                note=f"range_violation:{url},0-10pct",
            )

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
                "notes": [note or "ok"],
            }
        )
        return YieldFrame(frame=frame, note=note or "ok")

    # ------------------------------------------------------------------
    # 외부 호출 진입점.
    # ------------------------------------------------------------------
    def collect(self, target: date) -> Tuple[Dict[str, pd.DataFrame], Dict[str, str]]:
        frames: Dict[str, pd.DataFrame] = {}
        notes: Dict[str, str] = {}

        treasury_values = self._fetch_treasury_textview(target)

        for asset, series_id in self.SERIES_IDS.items():
            note_text = ""
            value: float | None
            source = ""
            url = ""
            quality = "secondary"

            # 1) FRED 시도
            value, url = self._fetch_fred(series_id)
            if value is not None and url is not None:
                source = "fred"
                note_text = "ok:fred"
            else:
                # 2) 재무부 TextView 값 활용
                if asset in treasury_values:
                    value = treasury_values[asset]
                    url = TREASURY_TEXTVIEW_URL.format(year=target.year)
                    source = "treasury"
                    quality = "final"
                    note_text = "fallback:treasury"
                else:
                    # 3) MarketWatch HTML 파싱
                    value, url = self._fetch_marketwatch(asset)
                    if value is not None and url is not None:
                        source = "marketwatch"
                        note_text = "fallback:marketwatch"

            if value is None or url is None:
                failure_url = FRED_URL.format(series_id=series_id)
                notes[f"{asset}:yield"] = f"parse_failed:{failure_url},all_sources_failed"
                self._debug("collect_failure", asset=asset)
                continue

            frame_bundle = self._build_frame(
                asset=asset,
                value=value,
                source=source,
                url=url,
                quality=quality,
                note=note_text,
                target=target,
            )
            if frame_bundle.note:
                notes[f"{asset}:yield"] = frame_bundle.note
            if not frame_bundle.frame.empty:
                frames[asset] = frame_bundle.frame

        return frames, notes
