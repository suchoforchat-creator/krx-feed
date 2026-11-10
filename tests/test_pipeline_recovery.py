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
    history.write_bytes(b"existing history bytes\r\n")
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
