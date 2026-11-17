"""실제 main→collect_raw→KIS/Yahoo 선택→보완→history의 네트워크 없는 재생."""
from datetime import date, datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo
import json

import pandas as pd
import pytest
import requests

import pipeline
from src.kis.client import KISClient
from src.kis import client as kis_client
from src.sources import core_index
from src.source_dates import note_date
from src.utils import SCHEMA_COLUMNS
from src.failure_diagnostic import report_pipeline_status

KST = ZoneInfo("Asia/Seoul")


class XMLResponse:
    status_code = 200
    headers = {}

    def __init__(self, asset, day="20261113", value=101):
        name = {"KOSPI": "코스피", "KOSDAQ": "코스닥"}[asset]
        self.content = (f'<?xml version="1.0" encoding="EUC-KR"?>'
                        f'<protocol><chartdata symbol="{asset}" name="{name}" count="1" '
                        f'timeframe="day" precision="2"><item data="{day}|{value}|{value + 1}|{value - 1}|{value}|200"/>'
                        '</chartdata></protocol>').encode("euc-kr")

    def iter_content(self, chunk_size):
        yield self.content

    def close(self):
        pass


def prepare(monkeypatch, tmp_path, *, now, primary_days=("2026-11-12",), unavailable=False, phase="1700",
            target_day="2026-11-13", supplement_values=None):
    """Only the transport/data suppliers are stubs; orchestration remains real."""
    monkeypatch.chdir(tmp_path)
    cfg = {"kis": {"mode": "simulation"}, "fallback": {"indexes": {
        "KOSPI": "^KS11", "KOSDAQ": "^KQ11", "K200": "^KS200",
        "SPX": "^GSPC", "NDX": "^NDX", "SOX": "^SOX"}}}
    class Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return now.astimezone(tz) if tz else now.replace(tzinfo=None)
    monkeypatch.setattr(pipeline, "datetime", Frozen)
    monkeypatch.setattr(pipeline, "parse_args", lambda: SimpleNamespace(
        phase=phase, tz="Asia/Seoul", reconcile=True,
        expected_target_date=target_day, expected_observation_date=now.date().isoformat()))
    monkeypatch.setattr(pipeline, "load_yaml", lambda p: cfg)
    monkeypatch.setattr(pipeline, "load_universe", lambda c: pd.DataFrame())
    primary_inputs = []
    def yahoo_download(symbol, **kwargs):
        # Keep the real _yf_history/_to_kst_index and its pytz timezone. The
        # old whole-method stub replaced this boundary with ZoneInfo, hiding
        # the pandas 2.3.3 failure when the Naver supplement was concatenated.
        assert kwargs == dict(period="1y", interval="1d", progress=False,
                              auto_adjust=False, threads=False)
        frame = pd.DataFrame({"Close": [100 + i for i in range(len(primary_days))]},
                             index=pd.DatetimeIndex(primary_days))
        primary_inputs.append((symbol, frame.copy(deep=True)))
        return frame
    monkeypatch.setattr(kis_client.yf, "download", yahoo_download)
    monkeypatch.setattr(pipeline.market, "fx_series", lambda *a, **k: pd.DataFrame())
    monkeypatch.setattr(pipeline.market, "futures_series", lambda *a, **k: pd.DataFrame())
    empty = lambda *a, **k: SimpleNamespace(frames={}, notes={})
    monkeypatch.setattr(pipeline, "KRXBreadthCollector", lambda: SimpleNamespace(collect=empty))
    monkeypatch.setattr(pipeline, "KRXKorRates", lambda: SimpleNamespace(fetch=empty))
    monkeypatch.setattr(pipeline, "USTYieldCollector", lambda: SimpleNamespace(collect=lambda *a: ({}, {})))
    monkeypatch.setattr(pipeline, "DXYCollector", lambda: SimpleNamespace(collect=lambda *a: (pd.DataFrame(), {})))
    monkeypatch.setattr(pipeline.commod_crypto, "fetch", lambda: {})
    monkeypatch.setattr(pipeline, "fetch_vix", lambda: pipeline._rec(
        "VIX", "spot", 20, "pt", source="fixture", source_date=date(2026, 11, 13)))
    calls = []
    def get(url, **kwargs):
        calls.append(kwargs)
        assert url == core_index.URL
        assert kwargs["allow_redirects"] is False
        if unavailable:
            raise requests.Timeout("SECRET_DO_NOT_LOG")
        asset = kwargs["params"]["symbol"]
        return XMLResponse(asset, target_day.replace("-", ""), (supplement_values or {}).get(asset, 101))
    monkeypatch.setattr(core_index.requests, "get", get)
    return calls, primary_inputs


@pytest.mark.parametrize("now", [datetime(2026, 11, 13, 17, tzinfo=KST),
                                 datetime(2026, 11, 14, 0, 45, tzinfo=KST),
                                 datetime(2026, 11, 16, 8, 30, tzinfo=KST)])
def test_real_collection_repairs_stale_normal_response(monkeypatch, tmp_path, now):
    calls, _ = prepare(monkeypatch, tmp_path, now=now)
    assert pipeline.main() == 0
    assert len(calls) == 2
    latest = pd.read_csv("out/latest.csv")
    assert list(latest.columns) == SCHEMA_COLUMNS
    history = pd.read_csv("out/history.csv")
    assert list(history["time_kst"]) == ["2026-11-13 15:30:00"]
    assert history.iloc[0]["kospi"] == 101
    for asset in ("KOSPI", "KOSDAQ"):
        row = latest.loc[(latest.asset == asset) & (latest.key == "idx")].iloc[0]
        assert note_date(row.notes) == date(2026, 11, 13)
        assert row.source == "NaverFinance(" + asset + ")"
    diagnostic = json.loads(Path("debug/1700/core_sources.json").read_text())
    assert len(diagnostic["sources"]) == 2
    assert {r["status"] for r in diagnostic["sources"]} == {"SUPPLEMENTED"}
    assert {r["primary_source_date"] for r in diagnostic["sources"]} == {"2026-11-12"}
    assert all(r["primary_supplier_rows"] == {"YAHOO_FINANCE": 1} for r in diagnostic["sources"])


def test_real_collection_fresh_source_avoids_extra_request(monkeypatch, tmp_path):
    calls, _ = prepare(monkeypatch, tmp_path, now=datetime(2026, 11, 16, 8, tzinfo=KST),
                       primary_days=("2026-11-12", "2026-11-13"))
    assert pipeline.main() == 0
    assert calls == []


def test_run645_transport_boundary_replay(monkeypatch, tmp_path):
    """#645 preserved dates/values; real Yahoo and Naver parsers, no network.

    This intentionally does not replace _yf_history or _to_kst_index. On the
    unchanged baseline with pandas 2.3.3 the main call fails in datetime parsing.
    """
    calls, _ = prepare(monkeypatch, tmp_path,
                       now=datetime(2026, 11, 17, 2, 36, tzinfo=KST),
                       primary_days=("2026-11-13",), target_day="2026-11-16",
                       supplement_values={"KOSPI": 5100.5, "KOSDAQ": 665.65})
    original_compute = pipeline.compute.compute_records
    observed_types = []
    observed_dtypes = []

    def recording_compute(ts, raw, notes):
        observed_types.append({type(value.tzinfo).__module__.split(".")[0]
                               for value in raw["KOSPI"]["ts_kst"]})
        observed_dtypes.append(raw["KOSPI"]["ts_kst"].dtype)
        assert set(raw["KOSPI"]["source"]) == {"YahooFinance(^KS11)", "NaverFinance(KOSPI)"}
        return original_compute(ts, raw, notes)

    monkeypatch.setattr(pipeline.compute, "compute_records", recording_compute)
    assert pipeline.main() == 0
    assert len(calls) == 2
    if pd.api.types.is_object_dtype(observed_dtypes[0]):
        assert {"pytz", "zoneinfo"}.issubset(observed_types[0])
    else:
        # pandas 3.0.1 already unifies the concat's timezone type. Do not force
        # object dtype in the transport replay and hide this real version gap.
        assert isinstance(observed_dtypes[0], pd.DatetimeTZDtype)
        assert observed_types[0] == {"pytz"}
    history = pd.read_csv("out/history.csv")
    assert list(history["time_kst"]) == ["2026-11-16 15:30:00"]
    assert history.iloc[0]["kospi"] == 5100.5
    assert history.iloc[0]["kosdaq"] == 665.65
    latest = pd.read_csv("out/latest.csv")
    for asset in ("KOSPI", "KOSDAQ"):
        row = latest.query("asset == @asset and key == 'idx'").iloc[0]
        assert note_date(row.notes) == date(2026, 11, 16)
    diagnostic = json.loads(Path("debug/1700/core_sources.json").read_text())
    assert {item["status"] for item in diagnostic["sources"]} == {"SUPPLEMENTED"}
    assert {item["primary_source_date"] for item in diagnostic["sources"]} == {"2026-11-13"}


def test_current_observation_not_replaced_by_prior_completed_supplement(monkeypatch, tmp_path):
    calls, _ = prepare(monkeypatch, tmp_path, now=datetime(2026, 11, 16, 11, tzinfo=KST),
                       primary_days=("2026-11-12", "2026-11-16"))
    assert pipeline.main() == 0
    assert len(calls) == 2
    row = pd.read_csv("out/latest.csv").query("asset == 'KOSPI' and key == 'idx'").iloc[0]
    assert note_date(row.notes) == date(2026, 11, 16)
    assert row.source == "YahooFinance(^KS11)"
    assert pd.read_csv("out/history.csv").iloc[0]["kospi"] == 101


@pytest.mark.parametrize("invalid", [None, "2026-11-13 15:30:00 SECRET_ZONE"])
def test_bad_time_blocks_publication_without_touching_previous_outputs(monkeypatch, tmp_path, invalid, capsys):
    """Corrupt only one supplied row after real collection, before calculation."""
    prepare(monkeypatch, tmp_path, now=datetime(2026, 11, 14, 0, 45, tzinfo=KST))
    original_collect = pipeline.collect_raw

    def malformed_raw(*args, **kwargs):
        raw, notes, metrics = original_collect(*args, **kwargs)
        raw["KOSPI"] = raw["KOSPI"].copy(deep=True)
        raw["KOSPI"]["ts_kst"] = raw["KOSPI"]["ts_kst"].astype(object)
        raw["KOSPI"].loc[raw["KOSPI"].index[-1], "ts_kst"] = invalid
        return raw, notes, metrics

    monkeypatch.setattr(pipeline, "collect_raw", malformed_raw)
    Path("out").mkdir()
    Path("out/latest.csv").write_bytes(b"previous verified latest, unchanged")
    Path("out/history.csv").write_bytes(b"previous verified history, unchanged")
    before = {p: Path(p).read_bytes() for p in ("out/latest.csv", "out/history.csv")}
    assert pipeline.main() == 1
    assert before == {p: Path(p).read_bytes() for p in before}
    diagnostic = json.loads(Path("debug/1700/pipeline_failure.json").read_text())
    assert diagnostic["reason_code"] == "TIMEZONE_NORMALIZATION_FAILED"
    assert diagnostic["exception_type"] == "TimeContractError"
    assert diagnostic["time_contract"]["asset"] == "KOSPI"
    assert diagnostic["time_contract"]["reason"] == ("INPUT_MISSING" if invalid is None else "INVALID_TIMESTAMP")
    assert "SECRET_ZONE" not in Path("debug/1700/pipeline_failure.json").read_text()
    assert "SECRET_ZONE" not in capsys.readouterr().out


def test_both_sources_unavailable_do_not_relabel_preserved_intraday(monkeypatch, tmp_path, capsys):
    calls, _ = prepare(monkeypatch, tmp_path, now=datetime(2026, 11, 14, 0, 45, tzinfo=KST), unavailable=True)
    Path("out").mkdir()
    # Actual failure artifact contained a prior intraday latest; never reuse it as close.
    Path("out/latest.csv").write_bytes(b"preserved 11/13 13:07 intraday snapshot")
    old = {k: "" for k in pipeline.update_history.HISTORY_COLUMNS}
    old.update(time_kst="2026-11-12 15:30:00", kospi="100", kosdaq="100", src_tag="original")
    pd.DataFrame([old]).to_csv("out/history.csv", index=False)
    before = {p: Path(p).read_bytes() for p in ("out/latest.csv", "out/history.csv")}
    assert pipeline.main() == 1
    assert len(calls) == 2
    assert before == {p: Path(p).read_bytes() for p in before}
    captured = capsys.readouterr().out
    assert 'SOURCE_DATE_NOT_READY:KOSPI' in captured
    assert "SECRET_DO_NOT_LOG" not in captured
    assert "SECRET_DO_NOT_LOG" not in Path("debug/1700/core_sources.json").read_text()


def test_summary_failure_cannot_reverse_business_success(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "missing" / "summary.md"))
    result = report_pipeline_status(datetime(2026, 11, 16, 8, tzinfo=KST), "0730", "success",
                                    date(2026, 11, 13), "UPDATED_VERIFIED")
    assert result["status"] == "success"
    assert "UI_WRITE_UNAVAILABLE" in capsys.readouterr().out


def test_summary_ignores_untrusted_reason_and_history(monkeypatch, tmp_path):
    path = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(path))
    report_pipeline_status(datetime(2026, 11, 16, 8, tzinfo=KST), "0730", "failure",
                           date(2026, 11, 13), "SECRET", "SECRET")
    assert "SECRET" not in path.read_text()


@pytest.mark.parametrize("state", ["ready", "supplemented", "waiting"])
def test_real_morning_entry_selection_and_partial_boundary(monkeypatch, tmp_path, state):
    calls, _ = prepare(monkeypatch, tmp_path, now=datetime(2026, 11, 16, 8, tzinfo=KST),
                       primary_days=("2026-11-12", "2026-11-13") if state == "ready" else ("2026-11-12",),
                       unavailable=state == "waiting", phase="0730")
    Path("out").mkdir()
    row = {c: "" for c in pipeline.update_history.HISTORY_COLUMNS}
    row.update(time_kst="2026-11-12 15:30:00", kospi="100", kosdaq="100", src_tag="original")
    pd.DataFrame([row]).to_csv("out/history.csv", index=False)
    before = Path("out/history.csv").read_bytes()
    assert pipeline.main() == 0
    assert len(calls) == (0 if state == "ready" else 2)
    events = [json.loads(line) for line in Path("out/logs/runner_20261116.json").read_text().splitlines()]
    diagnostic = json.loads(Path("debug/0730/core_sources.json").read_text())
    assert len(diagnostic["sources"]) == 2
    if state == "waiting":
        assert Path("out/history.csv").read_bytes() == before
        assert events[-1]["event"] == "partial_success"
        assert events[-1]["history_status"] == "WAITING_SOURCE"
        assert {r["status"] for r in diagnostic["sources"]} == {"UNAVAILABLE"}
    else:
        assert events[-1]["event"] == "success"
        assert pd.read_csv("out/history.csv").iloc[-1].time_kst == "2026-11-13 15:30:00"


def test_repeated_real_supplement_preserves_history_bytes(monkeypatch, tmp_path):
    prepare(monkeypatch, tmp_path, now=datetime(2026, 11, 16, 8, tzinfo=KST), phase="0730")
    assert pipeline.main() == 0
    original = Path("out/history.csv").read_bytes()
    prepare(monkeypatch, tmp_path, now=datetime(2026, 11, 16, 8, 10, tzinfo=KST), phase="0730")
    assert pipeline.main() == 0
    assert Path("out/history.csv").read_bytes() == original
    events = [json.loads(line) for line in Path("out/logs/runner_20261116.json").read_text().splitlines()]
    assert events[-1]["history_status"] == "UNCHANGED_VERIFIED"
