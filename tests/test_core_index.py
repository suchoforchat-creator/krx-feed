"""공개 일봉 보완의 네트워크 없는 원천·날짜·보존 계약 시험."""
from datetime import date, datetime
import json
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
import requests

from src.sources import core_index as core


KST = ZoneInfo("Asia/Seoul")
NOW = datetime(2026, 11, 14, 0, 45, tzinfo=KST)
TARGET = date(2026, 11, 13)
SECRET = "TEST_SECRET_MUST_NOT_APPEAR"


def raw(day=date(2026, 11, 12), value=100, asset="KOSPI"):
    return pd.DataFrame([dict(ts_kst=datetime.combine(day, datetime.min.time(), tzinfo=KST),
                             source_date=day, value=float(value), field="close", asset=asset,
                             unit="pt", source="fixture", quality="secondary", url=SECRET)])


def xml(asset="KOSPI", rows=None, **attrs):
    rows = rows if rows is not None else ["20261112|100|110|90|105|1000", "20261113|105|120|100|115|2000"]
    attributes = dict(symbol=asset, name=core.NAMES[asset], timeframe="day", precision="2", count=str(len(rows)))
    attributes.update(attrs)
    text = "<protocol><chartdata " + " ".join(k + '=\"' + v + '\"' for k, v in attributes.items()) + ">"
    text += "".join('<item data="' + row + '"/>' for row in rows)
    return (text + "</chartdata></protocol>").encode("utf-8")


class Response:
    def __init__(self, payload, status=200, headers=None):
        self.payload, self.status_code = payload, status
        self.headers = headers or {}
        self.closed = False

    def iter_content(self, chunk_size):
        for i in range(0, len(self.payload), chunk_size):
            yield self.payload[i:i+chunk_size]

    def close(self):
        self.closed = True


def install(monkeypatch, payload=None, status=200, headers=None, error=None):
    calls = []
    response = Response(payload if payload is not None else xml(), status, headers)

    def get(url, **kwargs):
        calls.append((url, kwargs))
        if error is not None:
            raise error
        return response

    monkeypatch.setattr(core.requests, "get", get)
    return calls, response


@pytest.mark.parametrize("asset", ["KOSPI", "KOSDAQ"])
def test_current_core_never_queries(monkeypatch, asset):
    calls, _ = install(monkeypatch)
    original = raw(TARGET, asset=asset)
    result, review = core.supplement_core_index(original, asset, TARGET, NOW)
    assert calls == []
    assert review["status"] == "PRIMARY_READY" and review["request_count"] == 0
    assert result is not original
    pd.testing.assert_frame_equal(original, result)
    assert SECRET not in json.dumps(review)


@pytest.mark.parametrize("asset", ["KOSPI", "KOSDAQ"])
def test_stale_numeric_success_is_supplemented_once(monkeypatch, asset):
    calls, response = install(monkeypatch, xml(asset))
    original = raw(asset=asset)
    before = original.copy(deep=True)
    result, review = core.supplement_core_index(original, asset, TARGET, NOW)
    assert len(calls) == 1 and response.closed
    assert calls[0][0] == core.URL
    assert calls[0][1]["params"] == dict(symbol=asset, timeframe="day", count=120, requestType="0")
    assert calls[0][1]["timeout"] == 12 and calls[0][1]["allow_redirects"] is False
    assert result.iloc[-1]["value"] == 115
    assert result.iloc[-1]["source_date"] == TARGET
    assert result.iloc[-1]["ts_kst"] == datetime(2026, 11, 13, 15, 30, tzinfo=KST)
    assert result.iloc[-1]["quality"] == "secondary"
    assert result.iloc[0]["value"] == 100  # 11/12 보조값105로 원문100을 덮지 않는다.
    pd.testing.assert_frame_equal(original, before)
    assert review["status"] == "SUPPLEMENTED" and review["supplement_row_count"] == 1
    assert SECRET not in json.dumps(review)


def test_empty_primary_uses_verified_history_without_future_target(monkeypatch):
    install(monkeypatch, xml(rows=["20261112|100|110|90|105|1000", "20261113|105|120|100|115|2000", "20261116|115|125|110|120|500"]))
    now = datetime(2026, 11, 16, 11, 0, tzinfo=KST)
    result, review = core.supplement_core_index(pd.DataFrame(), "KOSPI", TARGET, now)
    assert list(result["source_date"]) == [date(2026, 11, 12), TARGET]
    assert review["supplement_row_count"] == 2 and review["supplier_source_date"] == "2026-11-16"


def test_morning_intraday_primary_is_preserved_with_prior_close(monkeypatch):
    install(monkeypatch)
    original = raw(date(2026, 11, 16), 130)
    now = datetime(2026, 11, 16, 11, 0, tzinfo=KST)
    result, _ = core.supplement_core_index(original, "KOSPI", TARGET, now)
    assert list(result["value"]) == [130, 115]
    assert list(result["source_date"]) == [date(2026, 11, 16), TARGET]
    assert len(original) == 1


@pytest.mark.parametrize("change", [
    {"value": -1}, {"value": float("inf")}, {"value": float("nan")},
    {"unit": "%"}, {"asset": "KOSDAQ"},
])
def test_broken_primary_target_is_not_overwritten(monkeypatch, change):
    calls, _ = install(monkeypatch)
    original = raw(TARGET)
    for key, value in change.items():
        original.loc[0, key] = value
    with pytest.raises(ValueError, match="^CORE_INDEX_PRIMARY_INVALID$"):
        core.supplement_core_index(original, "KOSPI", TARGET, NOW)
    assert calls == []


def test_primary_duplicate_conflict_rejected(monkeypatch):
    calls, _ = install(monkeypatch)
    original = pd.concat([raw(TARGET), raw(TARGET, 101)], ignore_index=True)
    with pytest.raises(ValueError, match="^CORE_INDEX_PRIMARY_INVALID$"):
        core.supplement_core_index(original, "KOSPI", TARGET, NOW)
    assert calls == []


@pytest.mark.parametrize("target,now", [
    (date(2026, 11, 16), datetime(2026, 11, 16, 11, 0, tzinfo=KST)),
    (date(2026, 11, 14), NOW),
    (TARGET, datetime(2026, 11, 14, 0, 45)),
    (datetime(2026, 11, 13), NOW),
])
def test_invalid_target_before_network(monkeypatch, target, now):
    calls, _ = install(monkeypatch)
    with pytest.raises(ValueError, match="^CORE_INDEX_TARGET_INVALID$"):
        core.supplement_core_index(raw(), "KOSPI", target, now)
    assert calls == []


@pytest.mark.parametrize("payload,reason", [
    (xml(symbol="KOSDAQ"), "INDEX_DEFINITION_MISMATCH"),
    (xml(name="다른지수"), "INDEX_DEFINITION_MISMATCH"),
    (xml(timeframe="week"), "INDEX_DEFINITION_MISMATCH"),
    (xml(precision="0"), "INDEX_DEFINITION_MISMATCH"),
    (xml(count="3"), "ROW_COUNT_INVALID"),
    (b"<html><body>access denied</body></html>", "XML_SCHEMA_INVALID"),
    (b"not xml " + SECRET.encode(), "XML_INVALID"),
    (b'<!DOCTYPE protocol [<!ENTITY x "secret">]>' + xml(), "XML_DECLARATION_REJECTED"),
    (xml(rows=["20261113|105|120|100|115|2000", "20261113|105|120|100|116|2000"]), "DUPLICATE_DATE_CONFLICT"),
    (xml(rows=["20261113|105|120|100|NaN|2000"]), "OHLC_INVALID"),
    (xml(rows=["20261113|105|120|100|999|2000"]), "OHLC_INVALID"),
    (xml(rows=["20261116|105|120|100|115|2000"]), "FUTURE_SOURCE_DATE"),
    (xml(rows=["20261114|105|120|100|115|2000"]), "NON_SESSION_SOURCE_DATE"),
    (xml(rows=["20261112|105|120|100|115|2000"]), "TARGET_DATE_MISSING"),
    (xml(rows=["20261313|105|120|100|115|2000"]), "ROW_DATE_INVALID"),
    (xml(rows=["2026-11-13|105|120|100|115|2000"]), "ROW_FORMAT_INVALID"),
])
def test_bad_supplier_preserves_primary_and_safe_reason(monkeypatch, payload, reason):
    calls, response = install(monkeypatch, payload)
    original = raw()
    result, review = core.supplement_core_index(original, "KOSPI", TARGET, NOW)
    pd.testing.assert_frame_equal(original, result)
    assert len(calls) == 1 and response.closed
    assert review["status"] == "UNAVAILABLE" and review["reason_code"] == reason
    assert SECRET not in json.dumps(review)


@pytest.mark.parametrize("status,reason", [(403, "ACCESS_RESTRICTED"), (429, "ACCESS_RESTRICTED"), (302, "HTTP_NOT_OK"), (500, "HTTP_NOT_OK")])
def test_http_errors_do_not_retry_or_follow_redirects(monkeypatch, status, reason):
    calls, response = install(monkeypatch, status=status)
    result, review = core.supplement_core_index(raw(), "KOSPI", TARGET, NOW)
    assert len(result) == 1 and len(calls) == 1 and response.closed
    assert review["reason_code"] == reason


@pytest.mark.parametrize("error,reason", [(requests.Timeout(SECRET), "NETWORK_TIMEOUT"), (requests.ConnectionError(SECRET), "NETWORK_ERROR")])
def test_network_failure_does_not_leak_or_retry(monkeypatch, error, reason):
    calls, _ = install(monkeypatch, error=error)
    original = raw()
    result, review = core.supplement_core_index(original, "KOSPI", TARGET, NOW)
    assert len(calls) == 1 and review["reason_code"] == reason
    pd.testing.assert_frame_equal(original, result)
    assert SECRET not in json.dumps(review)


@pytest.mark.parametrize("declared", [str(core.MAX_BYTES + 1), "invalid"])
def test_declared_body_size_rejected(monkeypatch, declared):
    calls, response = install(monkeypatch, headers={"Content-Length": declared})
    result, review = core.supplement_core_index(raw(), "KOSPI", TARGET, NOW)
    assert len(calls) == 1 and response.closed
    assert review["reason_code"] == "RESPONSE_SIZE_INVALID" and len(result) == 1


def test_stream_size_bound(monkeypatch):
    _, response = install(monkeypatch, b"x" * (core.MAX_BYTES + 1))
    _, review = core.supplement_core_index(raw(), "KOSPI", TARGET, NOW)
    assert response.closed and review["reason_code"] == "RESPONSE_SIZE_INVALID"


def test_repeated_identical_supplier_row_not_double_counted(monkeypatch):
    row = "20261113|105|120|100|115|2000"
    install(monkeypatch, xml(rows=[row, row]))
    result, review = core.supplement_core_index(raw(), "KOSPI", TARGET, NOW)
    assert len(result) == 2 and review["supplement_row_count"] == 1


def test_target_presence_does_not_require_source_date_column(monkeypatch):
    calls, _ = install(monkeypatch)
    original = raw(TARGET).drop(columns="source_date")
    result, review = core.supplement_core_index(original, "KOSPI", TARGET, NOW)
    assert review["status"] == "PRIMARY_READY" and calls == []
    pd.testing.assert_frame_equal(result, original)


def test_explicit_unknown_is_not_relabelled_from_timestamp(monkeypatch):
    install(monkeypatch)
    original = raw(TARGET)
    original.loc[0, "source_date"] = "UNKNOWN"
    result, review = core.supplement_core_index(original, "KOSPI", TARGET, NOW)
    assert review["status"] == "SUPPLEMENTED"
    assert result.iloc[0]["source_date"] == "UNKNOWN"
    assert result.iloc[1]["source_date"] == TARGET


@pytest.mark.parametrize("asset", ["KOSPI", "KOSDAQ"])
def test_real_supplier_euc_kr_declaration(monkeypatch, asset):
    payload = ('<?xml version="1.0" encoding="EUC-KR"?>\n' + xml(asset).decode("utf-8")).encode("euc-kr")
    install(monkeypatch, payload)
    result, review = core.supplement_core_index(raw(asset=asset), asset, TARGET, NOW)
    assert review["status"] == "SUPPLEMENTED" and result.iloc[-1]["value"] == 115


@pytest.mark.parametrize("payload,reason", [
    (b'<?xml version="1.0" encoding="unknown"?>' + xml(), "XML_ENCODING_UNSUPPORTED"),
    (b'<?xml version="1.0" encoding="UTF-8"?>\xff' + xml(), "XML_ENCODING_INVALID"),
])
def test_bad_encoding_is_bounded_supplier_failure(monkeypatch, payload, reason):
    install(monkeypatch, payload)
    result, review = core.supplement_core_index(raw(), "KOSPI", TARGET, NOW)
    assert review["status"] == "UNAVAILABLE" and review["reason_code"] == reason
    assert len(result) == 1
