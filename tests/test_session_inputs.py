"""네트워크 없는 history cutoff / 원문 불변 / 기존 날짜 검사 회귀시험."""
from datetime import date, datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pytest

from src import compute
from src.session_inputs import history_raw_as_of
from src.source_dates import latest_source_date, note_date
from src.utils import SCHEMA_COLUMNS
import update_history

KST = ZoneInfo("Asia/Seoul")
TARGET = date(2026, 11, 9)
OBSERVED = datetime(2026, 11, 10, 12, 15, tzinfo=KST)


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("TEST_NETWORK_FORBIDDEN")
    monkeypatch.setattr("socket.socket.connect", forbidden)
    monkeypatch.setattr("requests.sessions.Session.request", forbidden)


def raw(days=("2026-11-09", "2026-11-10"), values=(100, 110), field="close"):
    return pd.DataFrame({
        "ts_kst": [pd.Timestamp(day, tz=KST) for day in days],
        "source_date": [date.fromisoformat(day) for day in days],
        "field": [field] * len(days),
        "value": list(values),
        "source": ["fixture"] * len(days),
        "quality": ["secondary"] * len(days),
        "url": ["https://example.invalid/offline"] * len(days),
        "notes": ["preserve original"] * len(days),
    })


def publish(tmp_path, inputs):
    records = compute.compute_records(OBSERVED, inputs)
    path = tmp_path / "history-input.csv"
    history = tmp_path / "history.csv"
    pd.DataFrame(records, columns=SCHEMA_COLUMNS).to_csv(path, index=False)
    update_history.upsert_from_latest(path, history, now=OBSERVED,
                                     target_date=TARGET, require_source_dates=True)
    return pd.read_csv(history, dtype=str).fillna("")


def test_two_dates_leave_snapshot_unchanged_and_publish_completed_day(tmp_path):
    inputs = {asset: raw() for asset in ("KOSPI", "KOSDAQ")}
    before = {asset: frame.copy(deep=True) for asset, frame in inputs.items()}
    # Reproduce the failure path: latest-only projection loses the target day.
    with pytest.raises(ValueError, match="SOURCE_DATE_NOT_READY:KOSPI"):
        publish(tmp_path, inputs)
    selected, diagnostic = history_raw_as_of(inputs, TARGET)
    result = publish(tmp_path, selected)
    assert result.iloc[0]["time_kst"] == "2026-11-09 15:30:00"
    assert result.iloc[0]["kospi"] == "100.0"
    assert diagnostic["input_rows"] == 4
    assert diagnostic["kept_rows"] == 2
    assert diagnostic["excluded_after_target_rows"] == 2
    for asset in inputs:
        pd.testing.assert_frame_equal(inputs[asset], before[asset])
        pd.testing.assert_frame_equal(selected[asset], before[asset].iloc[:1])
        assert selected[asset] is not inputs[asset]
    selected["KOSPI"].loc[0, "value"] = 999
    assert inputs["KOSPI"].loc[0, "value"] == 100


@pytest.mark.parametrize("metadata, expected_day", [
    ({"source_date": "UNKNOWN"}, None),
    ({"source_date": ""}, None),
    ({"source_date": "bad-date"}, None),
    ({"source_date": 20261109}, None),
    ({"source_date": None, "notes": "source_date=UNKNOWN"}, None),
    ({"source_date": None, "notes": "source_date=2026-11-09;source_date=2026-11-08"}, None),
    ({"source_date": None, "notes": "source_date=2026-11-08"}, date(2026, 11, 8)),
    ({"source_date": None, "notes": "plain"}, TARGET),
    # Explicit provider date has precedence, exactly as in latest_source_date.
    ({"source_date": TARGET, "notes": "source_date=UNKNOWN"}, TARGET),
])
def test_date_precedence_matches_canonical_selector(metadata, expected_day):
    frame = raw(days=("2026-11-09",), values=(100,))
    for key, value in metadata.items():
        frame[key] = pd.Series([value], dtype=object)
    assert latest_source_date(frame, "close") == expected_day
    selected, diagnostic = history_raw_as_of({"KOSPI": frame}, TARGET)
    assert len(selected["KOSPI"]) == int(expected_day is not None)
    assert diagnostic["excluded_unknown_source_date_rows"] == int(expected_day is None)


def test_missing_column_from_concatenated_source_uses_notes_then_original_timestamp():
    first = raw(days=("2026-11-09",), values=(100,))
    second = raw(days=("2026-11-08",), values=(200,), field="advance").drop(columns="source_date")
    frame = pd.concat([first, second], ignore_index=True)
    before = frame.copy(deep=True)
    selected, diagnostic = history_raw_as_of({"KOSPI": frame}, TARGET)
    pd.testing.assert_frame_equal(selected["KOSPI"], before)
    assert latest_source_date(selected["KOSPI"], "advance") == date(2026, 11, 8)
    assert diagnostic["kept_rows"] == 2


def test_cutoff_keeps_whole_field_history_for_return_and_volatility():
    days = pd.date_range("2026-09-25", "2026-11-10", freq="B")
    values = [100 + i * 1.1 + (i % 3) for i in range(len(days))]
    frame = raw(days=tuple(day.date().isoformat() for day in days), values=values)
    selected, diagnostic = history_raw_as_of({"K200": frame, "SOX": frame}, TARGET)
    expected = frame.loc[frame["source_date"] <= TARGET].copy()
    pd.testing.assert_frame_equal(selected["K200"], expected)
    actual_records = compute.compute_records(OBSERVED, selected)
    expected_records = compute.compute_records(OBSERVED, {"K200": expected, "SOX": expected})
    for asset, key in (("K200", "hv30"), ("SOX", "ret_1w"), ("SOX", "ret_1m")):
        actual = next(r for r in actual_records if (r["asset"], r["key"]) == (asset, key))
        reference = next(r for r in expected_records if (r["asset"], r["key"]) == (asset, key))
        assert actual["value"] is not None
        assert np.isfinite(actual["value"])
        assert actual == reference
        assert note_date(actual["notes"]) == TARGET
    assert diagnostic["excluded_after_target_rows"] == 2


@pytest.mark.parametrize("missing_asset", ["KOSPI", "KOSDAQ"])
@pytest.mark.parametrize("missing_kind", ["older", "newer", "unknown", "nonpositive"])
def test_missing_target_keeps_existing_history_and_core_guard(tmp_path, missing_asset, missing_kind):
    inputs = {asset: raw(days=("2026-11-09",), values=(100,))
              for asset in ("KOSPI", "KOSDAQ")}
    if missing_kind == "older":
        inputs[missing_asset] = raw(days=("2026-11-06",), values=(100,))
    elif missing_kind == "newer":
        inputs[missing_asset] = raw(days=("2026-11-10",), values=(100,))
    elif missing_kind == "unknown":
        inputs[missing_asset]["source_date"] = "UNKNOWN"
    else:
        inputs[missing_asset]["value"] = 0
    selected, _ = history_raw_as_of(inputs, TARGET)
    history = tmp_path / "history.csv"
    history.write_bytes(b"preserve exact historical bytes\n")
    with pytest.raises(ValueError, match="SOURCE_DATE_NOT_READY:" + missing_asset):
        publish(tmp_path, selected)
    assert history.read_bytes() == b"preserve exact historical bytes\n"


def test_every_field_filtered_without_reordering_or_rewriting():
    first = raw(field="close")
    second = raw(values=(300, 400), field="advance")
    frame = pd.concat([first, second])  # duplicate indices intentionally retained
    before = frame.copy(deep=True)
    selected, diagnostic = history_raw_as_of({"KOSPI": frame}, TARGET)
    pd.testing.assert_frame_equal(selected["KOSPI"], frame.iloc[[0, 2]])
    pd.testing.assert_frame_equal(frame, before)
    assert diagnostic["assets"]["KOSPI"]["kept_rows"] == 2
    assert diagnostic["assets"]["KOSPI"]["excluded_after_target_rows"] == 2


def test_empty_none_and_unverifiable_schema_remain_non_authoritative():
    invalid = pd.DataFrame({"value": [100], "source_date": [TARGET]})
    empty = raw().iloc[:0]
    selected, diagnostic = history_raw_as_of({"EMPTY": empty, "NONE": None, "INVALID": invalid}, TARGET)
    pd.testing.assert_frame_equal(selected["EMPTY"], empty)
    assert selected["NONE"] is None
    pd.testing.assert_frame_equal(selected["INVALID"], invalid.iloc[:0])
    assert diagnostic["excluded_unknown_source_date_rows"] == 1


@pytest.mark.parametrize("target", [None, "2026-11-09", datetime(2026, 11, 9)])
def test_only_exact_date_is_accepted(target):
    with pytest.raises(ValueError, match="EXACT_TARGET_DATE_REQUIRED"):
        history_raw_as_of({}, target)
