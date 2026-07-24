from datetime import date

import pandas as pd
import pytest

from src.kis.client import KISClient
from src.sources import kr_rates as kr_rates_module
from src.sources.kr_rates import KRXKorRates


class _FakeResponse:
    def __init__(self, text: str = "", status_code: int = 200, payload: dict | None = None) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = "utf-8"
        self.payload = payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"http_{self.status_code}")

    def json(self) -> dict:
        if self.payload is None:
            raise RuntimeError("json_payload_missing")
        return self.payload


class _FakeSession:
    def __init__(self, mapping: dict[str, _FakeResponse]) -> None:
        self.mapping = mapping
        self.requested_urls: list[str] = []

    def get(self, url: str, timeout: int = 20, headers: dict | None = None):  # noqa: D401 - requests.Session 시그니처와 유사하게 유지
        # 디버깅 편의를 위해 URL 매핑이 없으면 즉시 실패시켜 어떤 URL이 빠졌는지 테스트에서 확인 가능하다.
        self.requested_urls.append(url)
        if url not in self.mapping:
            raise RuntimeError(f"unexpected_url:{url}")
        return self.mapping[url]


def test_fetch_naver_tries_multiple_codes_for_kr10y():
    bad_url = "https://finance.naver.com/marketindex/interestDailyQuote.naver?marketindexCd=IRR_GOVT10Y&page=1"
    good_url = "https://finance.naver.com/marketindex/interestDailyQuote.naver?marketindexCd=IRR_GOVT10YR&page=1"
    marketindex_url = "https://finance.naver.com/marketindex/"
    session = _FakeSession(
        {
            # 1차 후보는 비어 있는 테이블(실서비스에서 자주 관찰되는 실패 형태).
            bad_url: _FakeResponse("<table><tbody></tbody></table>"),
            # 2차 후보는 정상 데이터 2행.
            good_url: _FakeResponse(
                """
                <table><tbody>
                    <tr><td>2026.04.24</td><td>3.55</td></tr>
                    <tr><td>2026.04.23</td><td>3.50</td></tr>
                </tbody></table>
                """
            ),
            marketindex_url: _FakeResponse("<html></html>"),
        }
    )
    loader = KRXKorRates(session=session)
    payload, error = loader._fetch_naver(date(2026, 4, 24), "KR10Y", "10년")
    assert error is None
    assert payload is not None
    assert payload["value"] == 3.55
    assert payload["prev"] == 3.50
    assert payload["url"] == good_url


def test_discover_naver_code_from_marketindex_page():
    marketindex_url = "https://finance.naver.com/marketindex/"
    discovered = "IRR_GOVT10Y_NEW"
    html = f"""
    <html>
      <body>
        <a href="/marketindex/interestDailyQuote.naver?marketindexCd={discovered}&page=1">국고채 10년</a>
      </body>
    </html>
    """
    session = _FakeSession({marketindex_url: _FakeResponse(html)})
    loader = KRXKorRates(session=session)
    assert loader._discover_naver_code("KR10Y") == discovered


def test_fetch_leaves_value_empty_when_all_sources_failed(monkeypatch: pytest.MonkeyPatch):
    loader = KRXKorRates(session=_FakeSession({}))

    # 모든 소스가 실패한 상황을 가정한다.
    monkeypatch.setattr(loader, "_fetch_krx", lambda *args, **kwargs: (None, "parse_failed:krx,403"))
    monkeypatch.setattr(loader, "_fetch_kofia", lambda *args, **kwargs: (None, "parse_failed:kofia,empty"))
    monkeypatch.setattr(loader, "_fetch_ecos", lambda *args, **kwargs: (None, "parse_failed:ecos,empty"))
    monkeypatch.setattr(loader, "_fetch_kis", lambda *args, **kwargs: (None, "parse_failed:kis,conf_missing"))
    monkeypatch.setattr(loader, "_fetch_naver", lambda *args, **kwargs: (None, "parse_failed:naver,empty_table"))
    monkeypatch.setattr(loader, "_fetch_investing", lambda *args, **kwargs: (None, "parse_failed:investing,403"))
    result = loader.fetch(date(2026, 4, 24))
    # 값은 비어 있어야 하므로 프레임이 만들어지지 않는다.
    assert "KR10Y" not in result.frames
    note = result.notes["KR10Y:yield"]
    # 디버깅용 실패 체인 문자열은 notes 딕셔너리에 남아야 한다.
    assert "parse_failed:krx,403" in note
    assert "parse_failed:investing,403" in note


def test_fetch_naver_parses_table_without_tbody():
    url = "https://finance.naver.com/marketindex/interestDailyQuote.naver?marketindexCd=IRR_GOVT10Y&page=1"
    marketindex_url = "https://finance.naver.com/marketindex/"
    html = """
    <table>
      <tr><th>날짜</th><th>금리</th></tr>
      <tr><td>2026.04.24</td><td>3.61</td></tr>
      <tr><td>2026.04.23</td><td>3.59</td></tr>
    </table>
    """
    session = _FakeSession({url: _FakeResponse(html), marketindex_url: _FakeResponse("<html></html>")})
    loader = KRXKorRates(session=session)
    payload, error = loader._fetch_naver(date(2026, 4, 24), "KR10Y", "10년")
    assert error is None
    assert payload is not None
    assert payload["value"] == 3.61
    assert payload["prev"] == 3.59


def test_fetch_ecos_uses_current_kr10y_series_with_sample_key(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("BOK_API_KEY", raising=False)
    url = (
        "https://ecos.bok.or.kr/api/StatisticSearch/sample/json/kr/1/10/"
        "817Y002/D/20260315/20260424/010210000"
    )
    session = _FakeSession(
        {
            url: _FakeResponse(
                payload={
                    "StatisticSearch": {
                        "list_total_count": 2,
                        "row": [
                            {
                                "ITEM_CODE1": "010210000",
                                "ITEM_NAME1": "국고채(10년)",
                                "TIME": "20260423",
                                "DATA_VALUE": "3.591",
                            },
                            {
                                "ITEM_CODE1": "010210000",
                                "ITEM_NAME1": "국고채(10년)",
                                "TIME": "20260424",
                                "DATA_VALUE": "3.612",
                            },
                        ]
                    }
                }
            )
        }
    )
    loader = KRXKorRates(session=session)

    payload, error = loader._fetch_ecos(date(2026, 4, 24), "KR10Y", "10년")

    assert error is None
    assert payload is not None
    assert payload["value"] == 3.612
    assert payload["prev"] == 3.591
    assert payload["prev_date"] == date(2026, 4, 23)
    assert payload["source"] == "BOK_ECOS"
    assert payload["note"] == "fallback:ecos:817Y002:010210000"
    assert session.requested_urls == [url]


def test_fetch_ecos_sample_key_reads_last_page(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("BOK_API_KEY", raising=False)
    first_url = (
        "https://ecos.bok.or.kr/api/StatisticSearch/sample/json/kr/1/10/"
        "817Y002/D/20260315/20260424/010210000"
    )
    last_url = (
        "https://ecos.bok.or.kr/api/StatisticSearch/sample/json/kr/20/29/"
        "817Y002/D/20260315/20260424/010210000"
    )
    session = _FakeSession(
        {
            first_url: _FakeResponse(
                payload={
                    "StatisticSearch": {
                        "list_total_count": 29,
                        "row": [
                            {
                                "ITEM_CODE1": "010210000",
                                "ITEM_NAME1": "국고채(10년)",
                                "TIME": "20260316",
                                "DATA_VALUE": "3.4",
                            }
                        ],
                    }
                }
            ),
            last_url: _FakeResponse(
                payload={
                    "StatisticSearch": {
                        "list_total_count": 29,
                        "row": [
                            {
                                "ITEM_CODE1": "010210000",
                                "ITEM_NAME1": "국고채(10년)",
                                "TIME": "20260423",
                                "DATA_VALUE": "3.591",
                            },
                            {
                                "ITEM_CODE1": "010210000",
                                "ITEM_NAME1": "국고채(10년)",
                                "TIME": "20260424",
                                "DATA_VALUE": "3.612",
                            },
                        ],
                    }
                }
            ),
        }
    )

    payload, error = KRXKorRates(session=session)._fetch_ecos(date(2026, 4, 24), "KR10Y", "10년")

    assert error is None
    assert payload is not None
    assert payload["value"] == 3.612
    assert payload["prev"] == 3.591
    assert session.requested_urls == [first_url, last_url]


def test_fetch_ecos_rejects_company_bond_as_kr10y(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("BOK_API_KEY", raising=False)
    monkeypatch.setitem(kr_rates_module.ECOS_ITEM_CODES, "KR10Y", ["010300000"])
    url = (
        "https://ecos.bok.or.kr/api/StatisticSearch/sample/json/kr/1/10/"
        "817Y002/D/20260315/20260424/010300000"
    )
    session = _FakeSession(
        {
            url: _FakeResponse(
                payload={
                    "StatisticSearch": {
                        "list_total_count": 1,
                        "row": [
                            {
                                "ITEM_CODE1": "010300000",
                                "ITEM_NAME1": "회사채(3년, AA-)",
                                "TIME": "20260424",
                                "DATA_VALUE": "4.653",
                            }
                        ],
                    }
                }
            )
        }
    )

    payload, error = KRXKorRates(session=session)._fetch_ecos(date(2026, 4, 24), "KR10Y", "10년")

    assert payload is None
    assert error is not None
    assert "series_mismatch" in error


def test_pykrx_kor_yields_uses_business_date_index(monkeypatch: pytest.MonkeyPatch):
    def fake_yields(_start: str, _end: str, kind: str) -> pd.DataFrame:
        values = [3.21, 3.22] if kind == "국고채3년" else [3.55, 3.56]
        return pd.DataFrame(
            {"수익률": values, "대비": [0.01, 0.01]},
            index=["20260723", "20260724"],
        )

    monkeypatch.setattr("src.kis.client.bond.get_otc_treasury_yields", fake_yields)

    frame = KISClient({})._pykrx_kor_yields(periods=1)

    assert len(frame) == 1
    assert frame.iloc[0]["ts_kst"].date() == date(2026, 7, 24)
    assert frame.iloc[0]["kr3y"] == 3.22
    assert frame.iloc[0]["kr10y"] == 3.56
