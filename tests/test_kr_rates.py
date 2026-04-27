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
