"""실제 main 진입점의 완료장 복구·최신관측 분리 오프라인 시험."""
from datetime import date, datetime
import json
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

import pipeline
from src.source_dates import note_date
from src.utils import SCHEMA_COLUMNS

KST = ZoneInfo("Asia/Seoul")
NOW = datetime(2026, 11, 10, 12, 15, tzinfo=KST)
SECRET = "FIXTURE_SECRET_MUST_NOT_APPEAR_IN_DIAGNOSTICS"


def raw(days=("2026-11-09", "2026-11-10"), values=(100, 110)):
    return pd.DataFrame({
        "ts_kst": [pd.Timestamp(day, tz=KST) for day in days],
        "source_date": [date.fromisoformat(day) for day in days],
        "field": ["close"] * len(days),
        "value": list(values),
        "source": ["fixture"] * len(days),
        "quality": ["secondary"] * len(days),
        "url": ["https://example.invalid/" + SECRET] * len(days),
        "notes": [SECRET] * len(days),
    })


def setup_main(monkeypatch, tmp_path, *, inputs=None, now=NOW, **overrides):
    args = {
        "phase": "recovery", "tz": "Asia/Seoul", "reconcile": True,
        "expected_target_date": "2026-11-09", "expected_observation_date": "2026-11-10",
    }
    args.update(overrides)
    inputs = inputs if inputs is not None else {asset: raw() for asset in ("KOSPI", "KOSDAQ")}
    calls = {"collect": [], "vix": 0, "cleanup": 0}

    class Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz) if tz is not None else now.replace(tzinfo=None)

    def collect(config, phase, *, run_ts, target_date):
        calls["collect"].append({"phase": phase, "run_ts": run_ts, "target_date": target_date})
        return inputs, {"KOSPI:idx": SECRET, "KOSDAQ:idx": SECRET}, {}

    def vix():
        calls["vix"] += 1
        return pipeline._rec("VIX", "spot", 20, "pt", source="fixture",
                             source_date=date(2026, 11, 9), notes=SECRET)

    def cleanup():
        calls["cleanup"] += 1

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(pipeline, "datetime", Frozen)
    monkeypatch.setattr(pipeline, "parse_args", lambda: SimpleNamespace(**args))
    monkeypatch.setattr(pipeline, "load_yaml", lambda p: {})
    monkeypatch.setattr(pipeline, "collect_raw", collect)
    monkeypatch.setattr(pipeline, "fetch_vix", vix)
    monkeypatch.setattr(pipeline, "cleanup_daily", cleanup)
    return inputs, calls


def core(frame, asset="KOSPI"):
    return frame.loc[(frame["asset"] == asset) & (frame["key"] == "idx")].iloc[0]


def write_old_daily(tmp_path, *, kospi=90, kosdaq=80):
    path = tmp_path / "out/daily/20261110.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for asset, value in (("KOSPI", kospi), ("KOSDAQ", kosdaq)):
        rows.append({
            "ts_kst": "2026-11-10 10:00", "asset": asset, "key": "idx",
            "value": value, "unit": "pt", "window": "1D", "change_abs": 0,
            "change_pct": 0, "source": "previous-fixture", "quality": "secondary",
            "url": "https://example.invalid/old", "notes": "source_date=2026-11-10",
        })
    pd.DataFrame(rows, columns=SCHEMA_COLUMNS).to_csv(path, index=False)
    return path


def write_old_history(path):
    """A valid prior output, distinct from dedicated malformed-schema fixtures."""
    path.parent.mkdir(parents=True, exist_ok=True)
    row = {column: "" for column in pipeline.update_history.HISTORY_COLUMNS}
    row.update(time_kst="2026-11-06 15:30:00", kospi="90", kosdaq="80",
               src_tag="previous-fixture", quality="secondary")
    pd.DataFrame([row], columns=pipeline.update_history.HISTORY_COLUMNS).to_csv(path, index=False)
    return path.read_bytes()


def test_recovery_main_keeps_current_snapshot_and_prior_completed_history(monkeypatch, tmp_path):
    inputs, calls = setup_main(monkeypatch, tmp_path)
    originals = {asset: frame.copy(deep=True) for asset, frame in inputs.items()}
    assert pipeline.main() == 0
    assert calls["collect"] == [{"phase": "recovery", "run_ts": NOW,
                                  "target_date": date(2026, 11, 9)}]
    assert calls["vix"] == 1
    assert calls["cleanup"] == 0
    latest = pd.read_csv(tmp_path / "out/latest.csv")
    daily = pd.read_csv(tmp_path / "out/daily/20261110.csv")
    history = pd.read_csv(tmp_path / "out/history.csv")
    assert list(latest.columns) == SCHEMA_COLUMNS
    pd.testing.assert_frame_equal(latest, daily)
    for asset in inputs:
        pd.testing.assert_frame_equal(inputs[asset], originals[asset])
        row = core(latest, asset)
        assert row["value"] == 110
        assert row["ts_kst"] == "2026-11-10 12:15"
        assert note_date(row["notes"]) == date(2026, 11, 10)
        assert row["quality"] == "secondary"
    assert list(history["time_kst"]) == ["2026-11-09 15:30:00"]
    assert history.iloc[0]["kospi"] == 100
    assert history.iloc[0]["kosdaq"] == 100
    assert "metric_date.kospi=2026-11-09" in history.iloc[0]["src_tag"]
    assert "observation_time=2026-11-10T12:15:00+09:00" in history.iloc[0]["src_tag"]


@pytest.mark.parametrize("asset", ["KOSPI", "KOSDAQ"])
@pytest.mark.parametrize("kind", ["target_absent", "unknown", "nonpositive"])
def test_core_failure_preserves_all_existing_output_bytes(monkeypatch, tmp_path, asset, kind):
    inputs = {item: raw() for item in ("KOSPI", "KOSDAQ")}
    if kind == "target_absent":
        inputs[asset] = raw(days=("2026-11-10",), values=(110,))
    elif kind == "unknown":
        inputs[asset].loc[0, "source_date"] = "UNKNOWN"
    else:
        inputs[asset].loc[0, "value"] = 0
    _, calls = setup_main(monkeypatch, tmp_path, inputs=inputs)
    daily = write_old_daily(tmp_path)
    latest = tmp_path / "out/latest.csv"
    history = tmp_path / "out/history.csv"
    latest.write_bytes(b"existing latest bytes\r\n")
    write_old_history(history)
    before = {path: path.read_bytes() for path in (latest, daily, history)}
    assert pipeline.main() == 1
    assert len(calls["collect"]) == 1
    assert calls["vix"] == 1
    assert calls["cleanup"] == 0
    for path, content in before.items():
        assert path.read_bytes() == content
    diagnostic = json.loads((tmp_path / "debug/recovery/pipeline_failure.json").read_text())
    assert diagnostic["reason_code"] == "SOURCE_DATE_NOT_READY:" + asset


@pytest.mark.parametrize("now,overrides,reason", [
    (NOW, {"expected_target_date": "2026-11-10"}, "EXPECTED_DATE_MISMATCH"),
    (NOW, {"expected_target_date": "2026-11-08"}, "EXPECTED_DATE_MISMATCH"),
    (NOW, {"expected_observation_date": "2026-11-09"}, "EXPECTED_DATE_MISMATCH"),
    (NOW, {"expected_target_date": "2026-11-9"}, "EXACT_DATE_FORMAT_REQUIRED"),
    (NOW, {"expected_target_date": ""}, "RECOVERY_DATE_BINDING_REQUIRED"),
    (NOW, {"expected_observation_date": ""}, "RECOVERY_DATE_BINDING_REQUIRED"),
    (NOW, {"phase": "invalid"}, "UNSUPPORTED_PHASE"),
    (NOW, {"tz": "UTC"}, "UNSUPPORTED_TIMEZONE"),
    (datetime(2026, 11, 7, 12, tzinfo=KST),
     {"expected_target_date": "2026-11-06", "expected_observation_date": "2026-11-07"},
     "RECOVERY_OBSERVATION_SESSION_REQUIRED"),
    (datetime(2026, 10, 9, 12, tzinfo=KST),
     {"expected_target_date": "2026-10-08", "expected_observation_date": "2026-10-09"},
     "RECOVERY_OBSERVATION_SESSION_REQUIRED"),
])
def test_bad_phase_date_or_closed_observation_rejected_before_collection(monkeypatch, tmp_path, now, overrides, reason):
    _, calls = setup_main(monkeypatch, tmp_path, now=now, **overrides)
    assert pipeline.main() == 1
    assert calls == {"collect": [], "vix": 0, "cleanup": 0}
    assert not (tmp_path / "out/latest.csv").exists()
    assert not (tmp_path / "out/history.csv").exists()
    diagnostics = list((tmp_path / "debug").glob("*/pipeline_failure.json"))
    assert len(diagnostics) == 1
    assert json.loads(diagnostics[0].read_text())["reason_code"] == reason


def test_reconcile_reads_prior_daily_before_write_and_keeps_secondary(monkeypatch, tmp_path):
    setup_main(monkeypatch, tmp_path)
    prior = write_old_daily(tmp_path)
    prior_bytes = prior.read_bytes()
    original = pipeline.reconcile.reconcile
    seen_prior = []

    def inspect(records, daily_path):
        seen_prior.append(Path(daily_path).read_bytes())
        return original(records, daily_path)

    monkeypatch.setattr(pipeline.reconcile, "reconcile", inspect)
    assert pipeline.main() == 0
    assert seen_prior == [prior_bytes]
    latest = pd.read_csv(tmp_path / "out/latest.csv")
    for asset in ("KOSPI", "KOSDAQ"):
        row = core(latest, asset)
        assert "revised" in row["notes"].split(";")
        assert row["quality"] == "secondary"
        assert note_date(row["notes"]) == date(2026, 11, 10)


def test_safe_input_diagnostic_omits_arbitrary_notes_values_and_urls(monkeypatch, tmp_path):
    setup_main(monkeypatch, tmp_path)
    assert pipeline.main() == 0
    path = tmp_path / "debug/1700/history_inputs.json"
    text = path.read_text(encoding="utf-8")
    assert SECRET not in text
    assert "example.invalid" not in text
    evidence = json.loads(text)
    assert evidence["values_urls_messages_and_locals_omitted"] is True
    assert evidence["selection_counts"]["excluded_after_target_rows"] == 2
    assert all(item["source_date"] == "2026-11-10" for item in evidence["latest_core"])
    assert all(item["source_date"] == "2026-11-09" for item in evidence["history_core"])
    assert all(item["matches_target"] for item in evidence["history_core"])
    assert all("value" not in item and "notes" not in item and "url" not in item
               for item in evidence["history_core"] + evidence["latest_core"])


def test_safe_failure_diagnostic_does_not_copy_exception_text(monkeypatch, tmp_path):
    setup_main(monkeypatch, tmp_path)

    def fail(*args, **kwargs):
        raise ValueError(SECRET)

    monkeypatch.setattr(pipeline, "collect_raw", fail)
    assert pipeline.main() == 1
    diagnostic = (tmp_path / "debug/recovery/pipeline_failure.json").read_text(encoding="utf-8")
    assert SECRET not in diagnostic
    assert "reason_code" not in json.loads(diagnostic)
    assert json.loads(diagnostic)["exception_type"] == "ValueError"


def test_cli_rejects_unknown_phase_before_main_can_collect(monkeypatch):
    monkeypatch.setattr("sys.argv", ["pipeline.py", "--phase", "invalid"])
    with pytest.raises(SystemExit) as exc:
        pipeline.parse_args()
    assert exc.value.code == 2


def runner_events(tmp_path, day="20261110"):
    path = tmp_path / "out/logs" / ("runner_" + day + ".json")
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_morning_repairs_previous_close_using_one_collection(monkeypatch, tmp_path):
    inputs, calls = setup_main(monkeypatch, tmp_path, phase="0730")
    original = {asset: frame.copy(deep=True) for asset, frame in inputs.items()}
    assert pipeline.main() == 0
    assert len(calls["collect"]) == 1
    assert calls["vix"] == 1
    assert calls["cleanup"] == 1
    latest = pd.read_csv(tmp_path / "out/latest.csv")
    history = pd.read_csv(tmp_path / "out/history.csv")
    assert core(latest)["value"] == 110
    assert note_date(core(latest)["notes"]) == date(2026, 11, 10)
    assert history.iloc[0]["kospi"] == 100
    assert list(history["time_kst"]) == ["2026-11-09 15:30:00"]
    for asset in inputs:
        pd.testing.assert_frame_equal(inputs[asset], original[asset])
    event = runner_events(tmp_path)[-1]
    assert event["event"] == "success"
    assert event["latest_status"] == "PUBLISHED"
    assert event["daily_status"] == "PUBLISHED"
    assert event["history_status"] == "UPDATED_VERIFIED"
    assert event["history_target"] == "2026-11-09"
    assert (tmp_path / "debug/0730/history_inputs.json").exists()


@pytest.mark.parametrize("asset", ["KOSPI", "KOSDAQ"])
@pytest.mark.parametrize("existing_history", [True, False])
def test_morning_delayed_core_is_partial_preserves_history(monkeypatch, tmp_path, asset, existing_history):
    inputs = {name: raw() for name in ("KOSPI", "KOSDAQ")}
    inputs[asset] = raw(days=("2026-11-06",), values=(99,))
    _, calls = setup_main(monkeypatch, tmp_path, phase="0730", inputs=inputs)
    history = tmp_path / "out/history.csv"
    old_bytes = None
    if existing_history:
        old_bytes = write_old_history(history)
    assert pipeline.main() == 0
    assert len(calls["collect"]) == 1
    assert calls["vix"] == 1
    if existing_history:
        assert history.read_bytes() == old_bytes
    else:
        assert not history.exists()
    latest = pd.read_csv(tmp_path / "out/latest.csv")
    assert note_date(core(latest, asset)["notes"]) == date(2026, 11, 6)
    assert core(latest, asset)["value"] == 99
    event = runner_events(tmp_path)[-1]
    assert event["event"] == "partial_success"
    assert event["latest_status"] == "PUBLISHED"
    assert event["history_status"] == "WAITING_SOURCE"
    assert event["history_reason_code"] == "SOURCE_DATE_NOT_READY:" + asset
    text = (tmp_path / "debug/0730/history_failure.json").read_text(encoding="utf-8")
    assert SECRET not in text
    assert json.loads(text)["reason_code"] == "SOURCE_DATE_NOT_READY:" + asset


@pytest.mark.parametrize("phase", ["1700", "EOD", "recovery"])
def test_closing_and_explicit_recovery_remain_strict_on_source_delay(monkeypatch, tmp_path, phase):
    inputs = {name: raw(days=("2026-11-06",), values=(99,)) for name in ("KOSPI", "KOSDAQ")}
    _, calls = setup_main(monkeypatch, tmp_path, phase=phase, inputs=inputs)
    daily = write_old_daily(tmp_path)
    latest = tmp_path / "out/latest.csv"
    history = tmp_path / "out/history.csv"
    latest.write_bytes(b"old latest")
    write_old_history(history)
    preserved = {path: path.read_bytes() for path in (daily, latest, history)}
    assert pipeline.main() == 1
    assert len(calls["collect"]) == 1
    for path, expected in preserved.items():
        assert path.read_bytes() == expected
    assert runner_events(tmp_path)[-1]["event"] == "failure"


@pytest.mark.parametrize("exc", [ValueError(SECRET), PermissionError(SECRET),
                                ValueError("SOURCE_DATE_NOT_READY:notes_missing"),
                                ValueError("SOURCE_DATE_NOT_READY:KOSPI:" + SECRET)])
def test_morning_unexpected_error_is_not_partial_success(monkeypatch, tmp_path, exc):
    setup_main(monkeypatch, tmp_path, phase="0730")
    daily = write_old_daily(tmp_path)
    latest = tmp_path / "out/latest.csv"
    history = tmp_path / "out/history.csv"
    latest.write_bytes(b"old latest")
    history.write_bytes(b"old history")
    preserved = {path: path.read_bytes() for path in (daily, latest, history)}

    def fail(*args, **kwargs):
        raise exc

    monkeypatch.setattr(pipeline, "update_completed_history", fail)
    assert pipeline.main() == 1
    for path, expected in preserved.items():
        assert path.read_bytes() == expected
    events = runner_events(tmp_path)
    assert events[-1]["event"] == "failure"
    assert not any(e["event"] in {"success", "partial_success"} for e in events)
    assert SECRET not in (tmp_path / "debug/0730/pipeline_failure.json").read_text(encoding="utf-8")


def test_repeat_verified_morning_keeps_history_bytes_and_original_observation(monkeypatch, tmp_path):
    setup_main(monkeypatch, tmp_path, phase="0730")
    assert pipeline.main() == 0
    history = tmp_path / "out/history.csv"
    original = history.read_bytes()
    setup_main(monkeypatch, tmp_path, phase="0730", now=NOW.replace(minute=25))
    assert pipeline.main() == 0
    assert history.read_bytes() == original
    assert runner_events(tmp_path)[-1]["history_status"] == "UNCHANGED_VERIFIED"


def test_corrected_same_session_value_is_not_suppressed_by_reuse(monkeypatch, tmp_path):
    setup_main(monkeypatch, tmp_path, phase="0730")
    assert pipeline.main() == 0
    history = tmp_path / "out/history.csv"
    original = history.read_bytes()
    inputs = {asset: raw(values=(101, 110)) for asset in ("KOSPI", "KOSDAQ")}
    setup_main(monkeypatch, tmp_path, phase="0730", inputs=inputs, now=NOW.replace(minute=25))
    assert pipeline.main() == 0
    assert history.read_bytes() != original
    assert pd.read_csv(history).iloc[0]["kospi"] == 101
    assert runner_events(tmp_path)[-1]["history_status"] == "UPDATED_VERIFIED"


@pytest.mark.parametrize("bad_target", ["2026-11-10", "2026-11-11", "2026-11-08"])
def test_morning_cannot_claim_current_future_or_closed_session(monkeypatch, tmp_path, bad_target):
    _, calls = setup_main(monkeypatch, tmp_path, phase="0730", expected_target_date=bad_target)
    assert pipeline.main() == 1
    assert calls["collect"] == []
    assert not (tmp_path / "out/latest.csv").exists()


def test_morning_catches_delayed_close_without_backfilling_earlier_gap(monkeypatch, tmp_path):
    now = datetime(2026, 11, 13, 11, 46, tzinfo=KST)
    inputs = {asset: raw(days=("2026-11-10", "2026-11-11", "2026-11-12", "2026-11-13"),
                         values=(100, 101, 102, 103)) for asset in ("KOSPI", "KOSDAQ")}
    _, calls = setup_main(monkeypatch, tmp_path, phase="0730", inputs=inputs, now=now,
                          expected_target_date="2026-11-12", expected_observation_date="2026-11-13")
    assert pipeline.main() == 0
    assert len(calls["collect"]) == 1
    history = pd.read_csv(tmp_path / "out/history.csv")
    assert list(history["time_kst"]) == ["2026-11-12 15:30:00"]
    assert history.iloc[0]["kospi"] == 102
    assert core(pd.read_csv(tmp_path / "out/latest.csv"))["value"] == 103
    assert runner_events(tmp_path, "20261113")[-1]["history_status"] == "UPDATED_VERIFIED"


@pytest.mark.parametrize("kind", ["missing", "extra", "duplicate"])
def test_morning_rejects_damaged_history_header_before_legacy_loader(monkeypatch, tmp_path, kind):
    setup_main(monkeypatch, tmp_path, phase="0730")
    daily = write_old_daily(tmp_path)
    latest = tmp_path / "out/latest.csv"
    history = tmp_path / "out/history.csv"
    latest.write_bytes(b"existing latest bytes\r\n")
    columns = list(pipeline.update_history.HISTORY_COLUMNS)
    if kind == "missing":
        columns.remove("vix")
    elif kind == "extra":
        columns.append("unknown_column")
    else:
        columns.append("kospi")
    history.write_text(",".join(columns) + "\n", encoding="utf-8")
    original = {path: path.read_bytes() for path in (latest, daily, history)}
    upsert_calls = []

    def forbidden_upsert(*args, **kwargs):
        upsert_calls.append(True)
        raise AssertionError("The permissive legacy history loader must not run")

    monkeypatch.setattr(pipeline.update_history, "upsert_from_latest", forbidden_upsert)
    with pytest.raises(ValueError, match="^HISTORY_SCHEMA_MISMATCH$"):
        pipeline._validate_existing_history_header(history)
    assert pipeline.main() == 1
    assert upsert_calls == []
    for path, expected in original.items():
        assert path.read_bytes() == expected
    assert runner_events(tmp_path)[-1]["event"] == "failure"
