"""실제 순간/원천일 보존과 시간 자료형 실패의 제한된 진단 계약."""
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import json

import numpy as np
import pandas as pd
import pytest
import pytz

from src.compute import _series_from_raw, compute_records
from src.failure_diagnostic import report_pipeline_status, write_failure
from src.source_dates import note_date
from src.time_contract import (
    RAW_NAIVE_TIMEZONE, TIME_FAILURE, TimeContractError,
    normalize_kst_times,
)


def mixed_values():
    return [pytz.timezone("Asia/Seoul").localize(datetime(2026, 11, 13, 9, 0, 0, 123456)),
            datetime(2026, 11, 16, 15, 30, tzinfo=ZoneInfo("Asia/Seoul")),
            datetime(2026, 11, 17, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 11, 17, 11, 0, tzinfo=timezone(timedelta(hours=9))),
            pd.Timestamp("2026-11-17T12:00:00.123456789+09:00")]


def test_mixed_aware_preserves_every_instant_and_index():
    original = pd.Series(mixed_values(), dtype=object, index=[4, 2, 2, 8, 9], name="ts_kst")
    before = original.copy(deep=True)
    actual = normalize_kst_times(original, asset="KOSPI")
    assert isinstance(actual.dtype, pd.DatetimeTZDtype)
    assert str(actual.dt.tz) == "Asia/Seoul"
    assert list(actual.index) == list(original.index)
    assert actual.name == "ts_kst"
    assert [stamp.value for stamp in actual] == [pd.Timestamp(stamp).value for stamp in original]
    pd.testing.assert_series_equal(original, before)


@pytest.mark.parametrize("value", ["2026-11-17T00:02:03Z", "2026-11-17 09:02:03+09:00",
                                    "2026-11-16T19:02:03-0500"])
def test_explicit_offset_iso_does_not_shift_instant(value):
    actual = normalize_kst_times([value])
    assert actual.iloc[0] == pd.Timestamp("2026-11-17 09:02:03+09:00")


@pytest.mark.parametrize("value", [datetime(2026, 11, 17, 9), "2026-11-17 09:00:00",
                                    np.datetime64("2026-11-17T09:00:00")])
def test_naive_requires_explicit_kst_contract_not_utc_guess(value):
    with pytest.raises(TimeContractError) as caught:
        normalize_kst_times([value])
    assert caught.value.safe_context()["reason"] == "NAIVE_TIMEZONE_UNDECLARED"
    actual = normalize_kst_times([value], naive_timezone=RAW_NAIVE_TIMEZONE)
    assert actual.iloc[0] == pd.Timestamp("2026-11-17 09:00:00+09:00")
    assert actual.iloc[0].hour == 9


def test_naive_and_aware_explicit_contract_can_share_one_column():
    actual = normalize_kst_times([datetime(2026, 11, 17, 9), datetime(2026, 11, 17, tzinfo=timezone.utc)],
                                 naive_timezone=RAW_NAIVE_TIMEZONE)
    assert actual.iloc[0] == actual.iloc[1]


@pytest.mark.parametrize("value,reason", [
    (None, "INPUT_MISSING"), (pd.NaT, "INPUT_MISSING"), (pd.NA, "INPUT_MISSING"),
    (float("nan"), "INPUT_MISSING"), (np.datetime64("NaT"), "INPUT_MISSING"),
    (1794873600, "INPUT_TYPE_UNSUPPORTED"), (True, "INPUT_TYPE_UNSUPPORTED"),
    ("not-a-date SECRET_TOKEN", "INVALID_TIMESTAMP"),
    ("2026-11-17 09:00:00 KST", "INVALID_TIMESTAMP"),
    ("2026-11-17 09:00:00 UNKNOWN", "INVALID_TIMESTAMP"),
    ("2026-11-31T09:00:00+09:00", "INVALID_TIMESTAMP"),
    ("2026-11-17T09:00:00+99:00", "INVALID_TIMESTAMP"),
])
def test_bad_values_are_neither_dropped_nor_coerced(value, reason):
    original = pd.Series([mixed_values()[0], value], dtype=object)
    before = original.copy(deep=True)
    with pytest.raises(TimeContractError) as caught:
        normalize_kst_times(original, asset="KOSPI", naive_timezone=RAW_NAIVE_TIMEZONE)
    assert str(caught.value) == TIME_FAILURE
    context = caught.value.safe_context()
    assert context["reason"] == reason
    assert context["row_count"] == 2 and context["row_position"] == 1
    assert "SECRET_TOKEN" not in json.dumps(context)
    pd.testing.assert_series_equal(original, before)


def test_unknown_naive_contract_rejected_before_inference():
    with pytest.raises(TimeContractError) as caught:
        normalize_kst_times(["2026-11-17"], naive_timezone="UTC")
    assert caught.value.safe_context()["reason"] == "UNKNOWN_TIMEZONE_CONTRACT"


@pytest.mark.parametrize("value", ["1988-10-09 02:30:00", "1988-05-08 02:30:00"])
def test_historical_korean_dst_ambiguity_not_silently_guessed(value):
    with pytest.raises(TimeContractError) as caught:
        normalize_kst_times([value], naive_timezone=RAW_NAIVE_TIMEZONE)
    assert caught.value.safe_context()["reason"] == "CONVERSION_FAILED"


def test_raw_copy_preserves_provider_date_values_and_original_timezone_types():
    raw = pd.DataFrame({"ts_kst": pd.Series([
        datetime(2026, 10, 31, 20, tzinfo=ZoneInfo("America/New_York")),
        pytz.timezone("Asia/Seoul").localize(datetime(2026, 11, 2, 9))], dtype=object),
        "source_date": [date(2026, 10, 31), date(2026, 11, 2)],
        "field": ["close", "close"], "value": [100.25, 101.75]})
    before = raw.copy(deep=True)
    normalized = _series_from_raw({"SPX": raw}, "SPX", "close")
    assert normalized.series.index[0].date() == date(2026, 11, 1)
    assert normalized.source_date == date(2026, 11, 2)
    assert list(normalized.series) == list(raw.value)
    assert _series_from_raw({"KOSDAQ": None}, "KOSDAQ", "close").series.empty
    assert _series_from_raw({"K200": pd.DataFrame()}, "K200", "close").series.empty
    pd.testing.assert_frame_equal(raw, before)


def test_compute_boundary_preserves_quote_date_and_does_not_drop_bad_row():
    raw = pd.DataFrame({"ts_kst": pd.Series([
        pytz.timezone("Asia/Seoul").localize(datetime(2026, 11, 13, 9)),
        datetime(2026, 11, 16, 15, 30, tzinfo=ZoneInfo("Asia/Seoul"))], dtype=object),
        "source_date": [date(2026, 11, 13), date(2026, 11, 16)],
        "asset": ["KOSPI"] * 2, "field": ["close"] * 2, "value": [5290.53, 5100.5],
        "unit": ["pt"] * 2, "source": ["YahooFinance(^KS11)", "NaverFinance(KOSPI)"],
        "quality": ["fallback"] * 2, "url": [""] * 2})
    original = raw.copy(deep=True)
    records = compute_records(pd.Timestamp("2026-11-17T02:36:00+09:00"), {"KOSPI": raw}, {})
    result = next(row for row in records if row["asset"] == "KOSPI" and row["key"] == "idx")
    assert result["value"] == 5100.5 and note_date(result["notes"]) == date(2026, 11, 16)
    pd.testing.assert_frame_equal(raw, original)
    invalid = raw.copy(deep=True)
    invalid.loc[1, "ts_kst"] = None
    with pytest.raises(TimeContractError):
        compute_records(pd.Timestamp("2026-11-17T02:36:00+09:00"), {"KOSPI": invalid}, {})


@pytest.mark.parametrize("case", ["unused_asset", "unused_asset_not_frame", "unused_field", "schema_field_missing",
                                   "schema_value_missing", "schema_time_missing"])
def test_unconsumed_rows_and_preexisting_missing_schema_are_not_global_gates(case):
    ts = pd.Timestamp("2026-11-17T02:36:00+09:00")
    base = pd.DataFrame({"ts_kst": [pd.Timestamp("2026-11-16T15:30:00+09:00")],
                         "field": ["close"], "value": [100.0], "source_date": ["2026-11-16"]})
    raw = {"KOSPI": base.copy(deep=True), "KOSDAQ": base.copy(deep=True)}
    expected = pd.DataFrame(compute_records(ts, raw, {}))
    invalid = pd.DataFrame({"ts_kst": ["invalid"], "field": ["close"], "value": [1]})
    if case == "unused_asset":
        raw["UNUSED_ASSET"] = invalid
    elif case == "unused_asset_not_frame":
        raw["UNUSED_ASSET"] = object()
    elif case == "unused_field":
        invalid["field"] = "unused_field"
        raw["KOSPI"] = pd.concat([base, invalid], ignore_index=True)
    else:
        missing = {"schema_field_missing": "field", "schema_value_missing": "value",
                   "schema_time_missing": "ts_kst"}[case]
        raw["BTC"] = invalid.drop(columns=[missing])
    actual = pd.DataFrame(compute_records(ts, raw, {}))
    assert len(actual) == 32
    pd.testing.assert_frame_equal(actual, expected)


@pytest.mark.parametrize("field", ["close", "advance", "trading_value"])
def test_actually_consumed_field_with_invalid_time_still_fails(field):
    raw = {"KOSPI": pd.DataFrame({"field": [field], "value": [1], "ts_kst": ["invalid"]})}
    with pytest.raises(TimeContractError) as caught:
        compute_records(pd.Timestamp("2026-11-17T02:36:00+09:00"), raw, {})
    assert caught.value.safe_context()["asset"] == "KOSPI"


@pytest.mark.parametrize("metadata,expected_day", [
    ({}, date(2026, 10, 31)),
    ({"source_date": "2026-10-30"}, date(2026, 10, 30)),
    ({"notes": "source_date=2026-10-29"}, date(2026, 10, 29)),
    ({"notes": "source_date=UNKNOWN"}, None),
])
def test_consumed_date_preserves_original_timezone_fallback_and_metadata_precedence(metadata, expected_day):
    frame = pd.DataFrame([{"ts_kst": datetime(2026, 10, 31, 20, tzinfo=ZoneInfo("America/New_York")),
                           "field": "close", "value": 100, **metadata}])
    before = frame.copy(deep=True)
    result = _series_from_raw({"SPX": frame}, "SPX", "close")
    assert result.series.index[0].date() == date(2026, 11, 1)
    assert result.source_date == expected_day
    pd.testing.assert_frame_equal(frame, before)


def test_provenance_uses_last_valid_consumed_row_with_duplicate_original_index():
    frame = pd.DataFrame([
        {"ts_kst": datetime(2026, 11, 16, 15, 30, tzinfo=ZoneInfo("Asia/Seoul")),
         "field": "close", "value": 101, "source_date": "2026-11-16"},
        {"ts_kst": pytz.timezone("Asia/Seoul").localize(datetime(2026, 11, 13, 9)),
         "field": "close", "value": 100, "source_date": "2026-11-13"},
        {"ts_kst": datetime(2026, 11, 17, 9, tzinfo=ZoneInfo("Asia/Seoul")),
         "field": "close", "value": None, "source_date": "2026-11-17"},
    ], index=[0, 0, 0])
    original = frame.copy(deep=True)
    result = _series_from_raw({"KOSPI": frame}, "KOSPI", "close")
    assert list(result.series) == [100, 101]
    assert result.source_date == date(2026, 11, 16)
    pd.testing.assert_frame_equal(frame, original)


def test_time_error_diagnostic_only_records_allowlisted_context(tmp_path, capsys, monkeypatch):
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    try:
        normalize_kst_times([mixed_values()[0], "SECRET_TOKEN"], asset="SECRET_ASSET")
    except TimeContractError as exc:
        evidence = write_failure(tmp_path / "failure.json", exc, "pipeline_main")
    assert evidence["reason_code"] == TIME_FAILURE
    assert evidence["exception_type"] == "TimeContractError"
    context = evidence["time_contract"]
    assert context["asset"] == "OTHER" and context["dtype_kind"] == "OBJECT"
    assert context["timezone_kinds"] == ["PYTZ"]
    assert context["versions"]["pandas"] == pd.__version__
    assert context["versions"]["numpy"] == np.__version__
    assert "SECRET" not in (tmp_path / "failure.json").read_text()
    result = report_pipeline_status(datetime(2026, 11, 17, 2, 36, tzinfo=ZoneInfo("Asia/Seoul")),
                                    "1700", "failure", date(2026, 11, 16), reason=TIME_FAILURE)
    assert result["reason_code"] == TIME_FAILURE
    assert "UNCLASSIFIED" not in capsys.readouterr().out


def test_empty_time_series_is_valid_but_does_not_invent_rows():
    original = pd.Series([], dtype=object, name="ts_kst")
    normalized = normalize_kst_times(original)
    assert normalized.empty and isinstance(normalized.dtype, pd.DatetimeTZDtype)


def test_safe_context_copies_cannot_mutate_saved_evidence():
    exc = TimeContractError(reason="SECRET", asset="KOSPI", row_count=2, dtype_kind="OBJECT",
                            timezone_kinds={"PYTZ", "SECRET"})
    changed = exc.safe_context()
    changed["versions"]["pandas"] = "SECRET"
    changed["timezone_kinds"].append("SECRET")
    assert "SECRET" not in json.dumps(exc.safe_context())
