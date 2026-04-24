from datetime import date

import pytest

from src.sources.kr_rates import KRXKorRates


class _FakeResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code
        self.encoding = "utf-8"

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"http_{self.status_code}")


class _FakeSession:
    def __init__(self, mapping: dict[str, _FakeResponse]) -> None:
        self.mapping = mapping

    def get(self, url: str, timeout: int = 20, headers: dict | None = None):  # noqa: D401 - requests.Session 시그니처와 유사하게 유지
        # 디버깅 편의를 위해 URL 매핑이 없으면 즉시 실패시켜 어떤 URL이 빠졌는지 테스트에서 확인 가능하다.
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


def test_fetch_adds_failure_chain_when_fixture_used(monkeypatch: pytest.MonkeyPatch):
    loader = KRXKorRates(session=_FakeSession({}))

    # 앞단 소스들은 모두 실패했다고 가정하고, 마지막 fixture만 성공하도록 목킹한다.
    monkeypatch.setattr(loader, "_fetch_krx", lambda *args, **kwargs: (None, "parse_failed:krx,403"))
    monkeypatch.setattr(loader, "_fetch_kofia", lambda *args, **kwargs: (None, "parse_failed:kofia,empty"))
    monkeypatch.setattr(loader, "_fetch_ecos", lambda *args, **kwargs: (None, "parse_failed:ecos,empty"))
    monkeypatch.setattr(loader, "_fetch_kis", lambda *args, **kwargs: (None, "parse_failed:kis,conf_missing"))
    monkeypatch.setattr(loader, "_fetch_naver", lambda *args, **kwargs: (None, "parse_failed:naver,empty_table"))
    monkeypatch.setattr(loader, "_fetch_investing", lambda *args, **kwargs: (None, "parse_failed:investing,403"))

    def _fixture(*args, **kwargs):
        return (
            {
                "value": 3.6,
                "prev": 3.5,
                "prev_date": date(2026, 4, 23),
                "source": "fixture",
                "quality": "secondary",
                "url": "https://example.com/fixture",
                "note": "fallback:fixture",
            },
            None,
        )

    monkeypatch.setattr(loader, "_fetch_fixture", _fixture)
    result = loader.fetch(date(2026, 4, 24))
    assert "KR10Y" in result.frames
    notes = result.frames["KR10Y"]["notes"].tolist()
    # 디버깅용 체인 문자열이 notes 컬럼으로 전달되어야 한다.
    assert any("fallback:fixture|chain:" in value for value in notes)
