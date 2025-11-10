"""한국 국채 수익률 (KR3Y/KR10Y) 수집기."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, time as dtime
from typing import Dict

import pandas as pd

from ..utils import KST
from .krx_client import KrxClient


logger = logging.getLogger(__name__)


@dataclass
class KrRatesResult:
    frames: Dict[str, pd.DataFrame]
    notes: Dict[str, str]


class KRXKorRates:
    """KRX 장외 채권수익률 표를 이용해 3년/10년 국채 금리를 가져온다."""

    MENU_ID = "MDC020104040401"
    BLD = "dbms/MDC/STAT/standard/MDCSTAT11401"

    def __init__(self, client: KrxClient | None = None) -> None:
        self._client = client or KrxClient()

    @staticmethod
    def _clean(series: pd.Series) -> pd.Series:
        """숫자 문자열을 float로 변환한다."""

        return pd.to_numeric(series.astype(str).str.replace(",", ""), errors="coerce")

    def fetch(self, target: date) -> KrRatesResult:
        payload = {"trdDd": target.strftime("%Y%m%d"), "inqTpCd": "T"}
        notes: Dict[str, str] = {}
        try:
            raw = self._client.fetch_json(self.MENU_ID, self.BLD, payload)
        except Exception as exc:  # pragma: no cover - 네트워크 의존
            message = f"parse_failed:{self.MENU_ID},{exc}"
            notes["KR3Y:yield"] = message
            notes["KR10Y:yield"] = message
            return KrRatesResult(frames={}, notes=notes)

        rows = raw.get("output") or raw.get("OutBlock_1") or []
        frame = pd.DataFrame(rows)
        if frame.empty:
            message = f"parse_failed:{self.MENU_ID},empty"
            notes["KR3Y:yield"] = message
            notes["KR10Y:yield"] = message
            return KrRatesResult(frames={}, notes=notes)

        ts = datetime.combine(target, dtime(hour=17, minute=0), tzinfo=KST)
        url = f"https://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId={self.MENU_ID}"

        frames: Dict[str, pd.DataFrame] = {}

        for alias, keyword in (("KR3Y", "3년"), ("KR10Y", "10년")):
            subset = frame.loc[frame["ITM_TP_NM"].astype(str).str.contains(keyword, na=False)].copy()
            if subset.empty:
                notes[f"{alias}:yield"] = f"parse_failed:{self.MENU_ID},missing"
                continue
            subset.sort_values("ITM_TP_NM", inplace=True)
            latest = subset.iloc[-1]
            value = self._clean(pd.Series([latest.get("LST_ORD_BAS_YD")])).iloc[0]
            if pd.isna(value):
                notes[f"{alias}:yield"] = f"parse_failed:{self.MENU_ID},invalid"
                continue
            if not 0 <= float(value) <= 20:
                logger.debug(
                    "kr_rates::fetch :: suspicious value alias=%s value=%.4f", alias, float(value)
                )
                notes[f"{alias}:yield"] = f"range_violation:{url},0-20pct"
                continue
            frames[alias] = pd.DataFrame(
                {
                    "ts_kst": [ts],
                    "asset": [alias],
                    "field": ["yield"],
                    "value": [float(value)],
                    "unit": ["pct"],
                    "window": ["1D"],
                    "source": ["krx"],
                    "quality": ["final"],
                    "url": [url],
                    "notes": [""],
                }
            )

        return KrRatesResult(frames=frames, notes=notes)

