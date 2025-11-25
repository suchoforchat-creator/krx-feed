"""KRX HTTP 유틸리티.

본 모듈은 KRX 정보데이터시스템에서 제공하는 JSON API를 호출하기 위한 공통
세션 래퍼를 정의한다. 각 호출은 referer/cookie 요구사항이 까다롭기 때문에
반복 코드를 한 곳에 모아 재사용한다.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Dict, Any

import requests


logger = logging.getLogger(__name__)


@dataclass
class KrxClient:
    """KRX JSON API를 호출하기 위한 가벼운 세션 래퍼."""

    timeout: int = 30

    def __post_init__(self) -> None:
        # KRX는 user-agent 및 referer 검증을 하기 때문에 브라우저 흉내를 낸다.
        user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
        self._base = "https://data.krx.co.kr"
        self._session = requests.Session()
        self._base_headers = {
            "User-Agent": os.getenv("KRX_USER_AGENT", user_agent),
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Connection": "keep-alive",
        }

    def _bootstrap(self, menu_id: str) -> None:
        """필수 쿠키를 얻기 위해 메뉴 페이지를 한 번 조회한다."""

        referer = f"{self._base}/contents/MDC/MDI/mdiLoader/index.cmd"
        params = {"menuId": menu_id}
        try:
            self._session.get(
                referer,
                params=params,
                headers=self._base_headers,
                timeout=self.timeout,
            )
        except requests.RequestException as exc:  # pragma: no cover - 네트워크 의존
            logger.debug("krx bootstrap failed: %s", exc)
            raise

    def fetch_json(self, menu_id: str, bld: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """지정된 화면(menu_id)에 해당하는 JSON 데이터를 조회한다."""

        self._bootstrap(menu_id)
        post_headers = {
            **self._base_headers,
            "Referer": f"{self._base}/contents/MDC/MDI/mdiLoader/index.cmd?menuId={menu_id}",
            "Origin": self._base,
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }
        payload = {"bld": bld}
        payload.update(params)
        try:
            response = self._session.post(
                f"{self._base}/comm/bldAttendant/getJsonData.cmd",
                headers=post_headers,
                data=payload,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:  # pragma: no cover - 네트워크 의존
            logger.debug("krx fetch failed (menu=%s, bld=%s): %s", menu_id, bld, exc)
            raise

