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
from io import StringIO
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
import requests

from ..utils import KST
from ..kis.client import KISClient
from .krx_client import KrxClient


logger = logging.getLogger(__name__)


KRX_URL = "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC020104040401"
KOFIA_URL = "https://www.kofiabond.or.kr/websquare/websquare.html?divisionId=MBIS01010010000000"
ECOS_URL = "https://ecos.bok.or.kr/"
ECOS_STAT_URL = "https://ecos.bok.or.kr/api/StatisticSearch"
NAVER_INTEREST_URL = "https://finance.naver.com/marketindex/interestDailyQuote.naver?marketindexCd={code}&page=1"
INVESTING_URLS = {
    "KR3Y": "https://www.investing.com/rates-bonds/south-korea-3-year-bond-yield",
    "KR10Y": "https://www.investing.com/rates-bonds/south-korea-10-year-bond-yield",
}

YIELD_COLUMNS = ["LST_ORD_BAS_YD", "LST_ORD_YD", "수익률", "YLD", "APPL_YD"]
ECOS_STAT_CODES = {
    # 060Y001: 시장금리(일별) 구버전/대표 코드
    # 817Y002: 일부 런타임에서 동일 계열 금리 데이터가 이 코드에만 존재하는 케이스 대응
    "KR3Y": ["060Y001", "817Y002"],
    "KR10Y": ["060Y001", "817Y002"],
}
ECOS_ITEM_CODES = {
    # 현장에서 7자리(0103000) / 9자리(010300000) 혼용 이슈가 발생해 둘 다 시도한다.
    "KR3Y": ["0101000", "010100000"],
    "KR10Y": ["0103000", "010300000"],
}
ECOS_ITEM_KEYWORDS = {
    # ECOS ITEM_CODE가 개편되면 고정 코드(0103000)로는 empty가 발생할 수 있다.
    # 아래 키워드로 ITEM_NAME을 탐색해 현재 유효한 코드를 자동 복구한다.
    "KR3Y": ["국고채", "3년"],
    "KR10Y": ["국고채", "10년"],
}
NAVER_CODES = {
    # 네이버 코드가 수시로 바뀌거나 폐기되므로 후보를 순차 시도한다.
    "KR3Y": ["IRR_GOVT03Y"],
    "KR10Y": ["IRR_GOVT10Y", "IRR_GOVT10YR", "IRR_GOVT10Y_KR"],
}


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
        self._kis_cache: pd.DataFrame | None = None
        self._kis_client: KISClient | None = None
        # ECOS item-code 자동 탐색 결과를 캐시하면 같은 실행에서 중복 네트워크를 줄일 수 있다.
        self._ecos_item_cache: dict[str, str] = {}

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

        # KRX 컬럼명이 자주 바뀌므로 "국고" 외에도 "국채"를 허용해 파싱 안정성을 높인다.
        mask_kind = frame.apply(lambda row: contains(row, "국고") or contains(row, "국채"), axis=1)
        # 만기 표기가 "3년/10년/3Y/10Y/3-year/10-year" 등으로 바뀔 수 있어 후보를 넓힌다.
        maturity_tokens = {keyword, keyword.replace("년", "Y"), keyword.replace("년", "-year")}
        mask_maturity = frame.apply(lambda row: any(contains(row, token) for token in maturity_tokens), axis=1)
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
        # 1) 기존 정규식(빠른 경로): "국고채 10년 2.93"
        pattern = re.compile(rf"국고\s*채?\s*{keyword}\s*([0-9]+\.?[0-9]*)")
        match = pattern.search(text)
        value = self._clean(match.group(1)) if match else float("nan")
        # 2) 보강 경로: 표(tr) 단위로 "국고"/"국채"+만기(10년) 포함 행에서 숫자를 찾는다.
        #    사이트 마크업이 자주 바뀌어도 텍스트 기반으로 최대한 복구하기 위한 로직이다.
        if not (0 < value < 10):
            for row in soup.select("tr"):
                row_text = re.sub(r"\s+", " ", row.get_text(" ", strip=True))
                if ("국고" not in row_text and "국채" not in row_text) or keyword not in row_text:
                    continue
                numbers = re.findall(r"\d+(?:\.\d+)?", row_text)
                # 디버깅 편의: 숫자 후보들을 로그로 남기면 파싱 실패 시 원인 추적이 쉽다.
                logger.debug("kr_rates::_fetch_kofia row match asset=%s row=%s numbers=%s", asset, row_text, numbers)
                if not numbers:
                    continue
                for token in reversed(numbers):
                    parsed = self._clean(token)
                    if 0 < parsed < 10:
                        value = parsed
                        break
                if 0 < value < 10:
                    break

        if not (0 < value < 10):
            return None, f"parse_failed:{KOFIA_URL},pattern_missing"
        return (
            {
                "value": float(value),
                "prev": None,
                "prev_date": None,
                    "source": "kofia",
                    "quality": "final",
                    "url": KOFIA_URL,
                    # 디버깅 편의: 어떤 경로(정규식/행스캔)로 성공했는지 notes에 남긴다.
                    "note": "ok:kofia",
                },
                None,
            )

    def _fetch_ecos(self, target: date, asset: str, keyword: str) -> Tuple[Optional[Dict[str, object]], Optional[str]]:
        api_key = os.getenv("BOK_API_KEY", "").strip() or "sample"

        item_codes = ECOS_ITEM_CODES.get(asset)
        if not item_codes:
            return None, f"parse_failed:{ECOS_URL},item_code_missing"
        stat_codes = ECOS_STAT_CODES.get(asset, ["060Y001"])
        if not stat_codes:
            stat_codes = ["060Y001"]

        # 디버깅 편의: 하드코드 item_code가 만료되는 경우를 대비해 자동 탐색을 시도한다.
        # 1) 기본 코드로 먼저 조회
        # 2) empty면 StatisticItemList로 현재 코드를 탐색해 재시도
        candidate_item_codes = list(dict.fromkeys(item_codes))

        end = target.strftime("%Y%m%d")
        start = (target - timedelta(days=40)).strftime("%Y%m%d")
        for stat_code in stat_codes:
            discovered = self._discover_ecos_item_code(api_key, asset, stat_code)
            item_code_candidates = list(candidate_item_codes)
            if discovered and discovered not in item_code_candidates:
                item_code_candidates.append(discovered)

            for candidate_code in item_code_candidates:
                url = "/".join(
                    [
                        ECOS_STAT_URL,
                        api_key,
                        "json",
                        "kr",
                        "1",
                        "400",
                        stat_code,
                        "DD",
                        start,
                        end,
                        candidate_code,
                        "",
                        "",
                    ]
                )
                try:
                    response = self._session.get(url, timeout=20)
                    response.raise_for_status()
                    payload = response.json()
                except Exception as exc:  # pragma: no cover
                    return None, f"parse_failed:{ECOS_URL},{exc}"

                # ECOS는 HTTP 200이어도 RESULT 코드로 에러를 돌려줄 수 있어 별도 점검한다.
                result = payload.get("RESULT", {}) if isinstance(payload, dict) else {}
                if isinstance(result, dict) and result.get("CODE") not in (None, "INFO-000"):
                    logger.debug(
                        "kr_rates::_fetch_ecos result_error asset=%s stat=%s item=%s code=%s msg=%s",
                        asset,
                        stat_code,
                        candidate_code,
                        result.get("CODE"),
                        result.get("MESSAGE"),
                    )
                    continue

                rows = payload.get("StatisticSearch", {}).get("row", [])
                if not rows:
                    logger.debug(
                        "kr_rates::_fetch_ecos empty rows asset=%s stat=%s item=%s payload_keys=%s",
                        asset,
                        stat_code,
                        candidate_code,
                        list(payload.keys()) if isinstance(payload, dict) else [],
                    )
                    continue

                frame = pd.DataFrame(rows)
                if "TIME" not in frame.columns or "DATA_VALUE" not in frame.columns:
                    logger.debug("kr_rates::_fetch_ecos missing fields asset=%s columns=%s", asset, list(frame.columns))
                    continue

                frame["TIME"] = pd.to_datetime(frame["TIME"], format="%Y%m%d", errors="coerce")
                frame["DATA_VALUE"] = pd.to_numeric(frame["DATA_VALUE"], errors="coerce")
                frame = frame.dropna(subset=["TIME", "DATA_VALUE"]).sort_values("TIME")
                if frame.empty:
                    continue

                valid = frame[(frame["DATA_VALUE"] > 0) & (frame["DATA_VALUE"] < 10)]
                if valid.empty:
                    logger.debug(
                        "kr_rates::_fetch_ecos range_violation asset=%s sample=%s",
                        asset,
                        frame[["TIME", "DATA_VALUE"]].tail(5).to_dict("records"),
                    )
                    continue

                current = float(valid.iloc[-1]["DATA_VALUE"])
                prev = float(valid.iloc[-2]["DATA_VALUE"]) if len(valid) >= 2 else None
                prev_date = valid.iloc[-2]["TIME"].date() if len(valid) >= 2 else None
                return (
                    {
                        "value": current,
                        "prev": prev,
                        "prev_date": prev_date,
                        "source": "BOK_ECOS",
                        "quality": "secondary",
                        "url": ECOS_URL,
                        "note": f"fallback:ecos:{stat_code}:{candidate_code}",
                    },
                    None,
                )

        return None, f"parse_failed:{ECOS_URL},empty"

    def _discover_ecos_item_code(self, api_key: str, asset: str, stat_code: str) -> Optional[str]:
        """ECOS StatisticItemList에서 현재 유효한 item code를 탐색한다.

        사용자 로그에서 KR10Y가 `parse_failed:.../empty`로 반복되므로,
        통계 항목 코드 변경 가능성을 자동으로 점검하기 위한 보강 로직이다.
        """
        cache_key = f"{asset}:{stat_code}"
        cached = self._ecos_item_cache.get(cache_key)
        if cached:
            return cached

        keywords = ECOS_ITEM_KEYWORDS.get(asset, [])
        if not keywords:
            return None

        urls = [
            # ECOS 공식 문서 포맷(권장)
            "/".join([
                "https://ecos.bok.or.kr/api/StatisticItemList",
                api_key,
                "json",
                "kr",
                "1",
                "1000",
                stat_code,
            ]),
            # 일부 런타임에서 StatisticSearch 베이스를 재사용하는 케이스를 대비한 보조 포맷
            "/".join([
                ECOS_STAT_URL,
                api_key,
                "json",
                "kr",
                "1",
                "1000",
                "StatisticItemList",
                stat_code,
            ]),
        ]

        for url in urls:
            try:
                resp = self._session.get(url, timeout=20)
                resp.raise_for_status()
                payload = resp.json()
            except Exception as exc:
                logger.debug("kr_rates::_discover_ecos_item_code request failed asset=%s url=%s err=%s", asset, url, exc)
                continue

            rows = payload.get("StatisticItemList", {}).get("row", []) if isinstance(payload, dict) else []
            if not rows:
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                name = str(row.get("ITEM_NAME", ""))
                code = str(row.get("ITEM_CODE", "")).strip()
                if not code:
                    continue
                normalized = re.sub(r"\s+", "", name)
                if all(token in normalized for token in keywords):
                    self._ecos_item_cache[cache_key] = code
                    logger.debug("kr_rates::_discover_ecos_item_code asset=%s stat=%s code=%s name=%s", asset, stat_code, code, name)
                    return code
        return None

    def _discover_naver_code(self, asset: str) -> Optional[str]:
        """네이버 메인에서 동적으로 금리 코드를 찾는다.

        KR10Y 코드가 폐기/변경되는 경우가 있어, 정적 코드 실패 시 마지막으로 호출한다.
        """
        label_map = {"KR3Y": "국고채3년", "KR10Y": "국고채10년"}
        target_label = label_map.get(asset)
        if not target_label:
            return None
        try:
            response = self._session.get("https://finance.naver.com/marketindex/", timeout=20, headers={"User-Agent": "Mozilla/5.0"})
            response.raise_for_status()
        except Exception as exc:
            logger.debug("kr_rates::_discover_naver_code request failed asset=%s err=%s", asset, exc)
            return None
        try:
            import bs4  # type: ignore
        except Exception:
            return None
        soup = bs4.BeautifulSoup(response.text, "lxml")
        # 텍스트 비교를 위해 공백/특수문자를 제거한 정규화 문자열을 사용한다.
        normalized_target = re.sub(r"[^가-힣0-9A-Za-z]", "", target_label)
        for link in soup.select("a[href*='marketindexCd=']"):
            href = link.get("href", "")
            text = re.sub(r"[^가-힣0-9A-Za-z]", "", link.get_text(" ", strip=True))
            if normalized_target and normalized_target in text:
                match = re.search(r"marketindexCd=([A-Z0-9_]+)", href)
                if match:
                    discovered = match.group(1)
                    logger.debug("kr_rates::_discover_naver_code asset=%s discovered=%s", asset, discovered)
                    return discovered
        return None

    def _fetch_naver(self, target: date, asset: str, keyword: str) -> Tuple[Optional[Dict[str, object]], Optional[str]]:
        codes = NAVER_CODES.get(asset)
        if not codes:
            return None, f"parse_failed:naver,code_missing:{asset}"
        all_codes = list(codes)
        discovered = self._discover_naver_code(asset)
        if discovered and discovered not in all_codes:
            all_codes.append(discovered)

        last_error: Optional[str] = None
        for code in all_codes:
            url = NAVER_INTEREST_URL.format(code=code)
            try:
                response = self._session.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
                response.raise_for_status()
                response.encoding = "euc-kr"
            except Exception as exc:  # pragma: no cover
                last_error = f"parse_failed:{url},{exc}"
                continue
            try:
                import bs4  # type: ignore
            except Exception:
                return None, f"parse_failed:{url},bs4_missing"

            soup = bs4.BeautifulSoup(response.text, "lxml")
            # 네이버가 tbody를 제거하는 경우가 있어 table tr까지 폭넓게 수집한다.
            rows = soup.select("table tbody tr") or soup.select("table tr")

            values: list[float] = []
            dates: list[date] = []
            for row in rows:
                cols = [col.get_text(" ", strip=True) for col in row.select("td, th")]
                if len(cols) < 2:
                    continue
                parsed = self._clean(cols[1])
                if not (0 < parsed < 10):
                    continue
                values.append(float(parsed))
                try:
                    dates.append(datetime.strptime(cols[0], "%Y.%m.%d").date())
                except Exception:
                    dates.append(target)

            # HTML 구조가 또 변하면 pandas.read_html로 한 번 더 파싱을 시도한다.
            if not values:
                try:
                    tables = pd.read_html(StringIO(response.text))
                except Exception as exc:
                    logger.debug("kr_rates::_fetch_naver read_html failed asset=%s code=%s err=%s", asset, code, exc)
                    tables = []
                for table in tables:
                    if table.shape[1] < 2:
                        continue
                    for _, rec in table.iloc[:, :2].iterrows():
                        parsed = self._clean(rec.iloc[1])
                        if not (0 < parsed < 10):
                            continue
                        values.append(float(parsed))
                        try:
                            dates.append(pd.to_datetime(rec.iloc[0], errors="raise").date())
                        except Exception:
                            dates.append(target)

            if not values:
                logger.debug("kr_rates::_fetch_naver no values asset=%s code=%s", asset, code)
                last_error = f"parse_failed:{url},empty_table"
                continue

            prev = values[1] if len(values) >= 2 else None
            prev_date = dates[1] if len(dates) >= 2 else None
            return (
                {
                    "value": values[0],
                    "prev": prev,
                    "prev_date": prev_date,
                    "source": "naver",
                    "quality": "secondary",
                    "url": url,
                    # 디버깅 편의: 어떤 marketindexCd가 실제로 성공했는지 기록한다.
                    "note": f"fallback:naver:{code}",
                },
                None,
            )
        return None, (last_error or f"parse_failed:naver,no_code_success:{asset}")

    def _load_conf(self) -> Dict[str, object]:
        try:
            import yaml  # type: ignore
        except Exception:
            return {}
        conf_path = Path(__file__).resolve().parents[2] / "conf.yml"
        if not conf_path.exists():
            return {}
        try:
            raw = yaml.safe_load(conf_path.read_text(encoding="utf-8"))
            return raw if isinstance(raw, dict) else {}
        except Exception as exc:
            logger.debug("kr_rates::_load_conf failed: %s", exc)
            return {}

    def _fetch_fixture(self, target: date, asset: str, keyword: str) -> Tuple[Optional[Dict[str, object]], Optional[str]]:
        """네트워크 차단 환경용 최종 폴백.

        README 요구사항(실소스 모두 실패 시 fixtures.kor_yields 사용)에 맞춰,
        conf.yml의 `fixtures.kor_yields`에서 최근 스냅샷 값을 읽는다.
        """
        conf = self._load_conf()
        fixtures = (conf.get("fixtures", {}) if isinstance(conf, dict) else {})  # type: ignore[union-attr]
        rows = fixtures.get("kor_yields", []) if isinstance(fixtures, dict) else []
        if not isinstance(rows, list) or not rows:
            return None, "parse_failed:fixture,empty"

        # 디버깅 편의: 날짜 파싱 가능한 행만 남기고 최신 2개를 뽑아 현재/이전값을 구성한다.
        parsed_rows: list[tuple[date, float, float]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            try:
                row_date = pd.to_datetime(row.get("date"), errors="raise").date()
                kr3y = float(row.get("kr3y"))
                kr10y = float(row.get("kr10y"))
            except Exception:
                continue
            if not (0 < kr3y < 10 and 0 < kr10y < 10):
                continue
            parsed_rows.append((row_date, kr3y, kr10y))
        if not parsed_rows:
            return None, "parse_failed:fixture,no_valid_rows"

        parsed_rows.sort(key=lambda item: item[0])
        latest_date, latest_3y, latest_10y = parsed_rows[-1]
        prev_tuple = parsed_rows[-2] if len(parsed_rows) >= 2 else None

        value = latest_3y if asset == "KR3Y" else latest_10y
        prev = None
        prev_date = None
        if prev_tuple is not None:
            prev_date = prev_tuple[0]
            prev = prev_tuple[1] if asset == "KR3Y" else prev_tuple[2]

        logger.debug(
            "kr_rates::_fetch_fixture asset=%s latest_date=%s target=%s value=%s",
            asset,
            latest_date,
            target,
            value,
        )
        return (
            {
                "value": float(value),
                "prev": float(prev) if prev is not None else None,
                "prev_date": prev_date,
                "source": "fixture",
                "quality": "secondary",
                "url": str(fixtures.get("kor_yields_url", "https://www.kofiabond.or.kr")),
                "note": "fallback:fixture",
            },
            None,
        )

    def _fetch_kis(self, target: date, asset: str, keyword: str) -> Tuple[Optional[Dict[str, object]], Optional[str]]:
        # KIS API 키가 있는 런타임에서는 이 경로가 KR10Y 복구에 가장 안정적이다.
        if self._kis_cache is None:
            conf = self._load_conf()
            if not conf:
                return None, "parse_failed:KIS,conf_missing"
            try:
                self._kis_client = KISClient(conf)
                self._kis_cache = self._kis_client.get_kor_yields()
            except Exception as exc:
                return None, f"parse_failed:KIS,{exc}"
        frame = self._kis_cache
        if frame is None or frame.empty:
            # 디버깅 편의: KISClient 내부 실패 메타(reason/url)를 에러 문자열에 포함한다.
            # 예) parse_failed:KIS,empty|KR10Y:ecos_empty@https://ecos...
            failure_meta = getattr(self._kis_client, "yield_failure_meta", {})
            detail_parts: list[str] = []
            if isinstance(failure_meta, dict):
                for alias, info in failure_meta.items():
                    if not isinstance(info, dict):
                        continue
                    reason = str(info.get("reason", "unknown"))
                    url = str(info.get("url", ""))
                    detail_parts.append(f"{alias}:{reason}@{url}" if url else f"{alias}:{reason}")
            detail = "|".join(detail_parts)
            return None, (f"parse_failed:KIS,empty|{detail}" if detail else "parse_failed:KIS,empty")
        col = asset.lower()
        if col not in frame.columns:
            return None, f"parse_failed:KIS,column_missing:{col}"
        series = pd.to_numeric(frame[col], errors="coerce").dropna()
        if series.empty:
            return None, f"parse_failed:KIS,value_missing:{col}"
        value = float(series.iloc[-1])
        if not (0 < value < 10):
            return None, "range_violation:KIS,0-10pct"
        prev = float(series.iloc[-2]) if len(series) >= 2 else None
        prev_date = None
        if len(series) >= 2 and "ts_kst" in frame.columns:
            ts = pd.to_datetime(frame["ts_kst"], errors="coerce").dropna()
            if len(ts) >= 2:
                prev_date = ts.iloc[-2].date()
        return (
            {
                "value": value,
                "prev": prev,
                "prev_date": prev_date,
                "source": "KIS",
                "quality": "final",
                "url": "https://finance.koreainvestment.com/bond",
                "note": "fallback:kis",
            },
            None,
        )

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
        # Investing.com은 data-test 속성만 유지되는 경우가 있어 span/div 모두 확인한다.
        price_node = (
            soup.select_one(".instrument-price_last__KQzyA")
            or soup.select_one("span[data-test='instrument-price-last']")
            or soup.select_one("div[data-test='instrument-price-last']")
        )
        if price_node is None:
            # 마지막 수단으로 정규식 파싱을 시도해 HTML 구조 변경에 대비한다.
            match = re.search(
                r'data-test="instrument-price-last"[^>]*>([^<]+)</',
                response.text,
            )
            if not match:
                return None, f"parse_failed:{url},node_missing"
            value = self._clean(match.group(1))
        else:
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
            failure_chain: list[str] = []
            for fetcher in (
                self._fetch_krx,
                self._fetch_kofia,
                self._fetch_ecos,
                self._fetch_kis,
                self._fetch_naver,
                self._fetch_investing,
                self._fetch_fixture,
            ):
                result, error = fetcher(target, asset, keyword)
                if result is not None:
                    payload = result
                    break
                if error:
                    failure_reason = error
                    failure_chain.append(error)
                    logger.debug("kr_rates::fetch fallback asset=%s reason=%s", asset, error)
            if payload is None:
                # 마지막 에러만 남기면 원인 추적이 어려워서 체인 형태로 함께 기록한다.
                notes[f"{asset}:yield"] = " | ".join(failure_chain) if failure_chain else (failure_reason or f"parse_failed:{asset},unknown")
                continue
            # fixture로 내려온 경우, 왜 fixture까지 왔는지 직전 실패 체인을 notes에 함께 남겨
            # latest.csv 한 줄만 봐도 디버깅 가능한 형태를 만든다.
            if payload.get("source") == "fixture":
                prefix = str(payload.get("note", "fallback:fixture"))
                chain = " > ".join(failure_chain[-4:]) if failure_chain else "unknown"
                payload["note"] = f"{prefix}|chain:{chain}"
                logger.warning("kr_rates::fetch fixture_used asset=%s chain=%s", asset, chain)
            frame = self._build_frame(asset, target, payload)
            frames[asset] = frame
            notes[f"{asset}:yield"] = payload.get("note", "ok") or "ok"

        return KrRatesResult(frames=frames, notes=notes)
