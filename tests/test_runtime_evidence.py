"""Runtime proof and offline replay policy tests; no installation/network."""
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools import runtime_evidence as runtime
from tools import verify_time_contract_replay as replay


@pytest.fixture
def pins(tmp_path, monkeypatch):
    path = tmp_path / "constraints.txt"
    path.write_text("# approved\npandas==2.3.3\nnumpy==2.4.6\n", encoding="utf-8")
    def version(name):
        if name in {"pandas", "numpy"}:
            return {"pandas": "2.3.3", "numpy": "2.4.6"}[name]
        raise runtime.metadata.PackageNotFoundError(name)
    monkeypatch.setattr(runtime.metadata, "version", version)
    monkeypatch.setattr(runtime.platform, "python_version", lambda: "3.11.18")
    return path


def test_runtime_matches_pins_and_exact_python(pins):
    result = runtime.build_evidence(pins, "3.11.18")
    assert result["status"] == "PASS"
    assert result["constraints_sha256"] == hashlib.sha256(pins.read_bytes()).hexdigest()
    assert result["environment_dumped"] is False
    assert set(result["versions"]) == set(runtime.ALLOWED_DISTRIBUTIONS)


def test_runtime_optional_python_does_not_claim_required_environment(pins, monkeypatch):
    monkeypatch.setattr(runtime.platform, "python_version", lambda: "3.12.14")
    assert runtime.build_evidence(pins)["status"] == "PASS"
    result = runtime.build_evidence(pins, "3.11.18")
    assert result["status"] == "FAIL"
    assert {"code": "PYTHON_VERSION_MISMATCH"} in result["errors"]


def test_runtime_dependency_mismatch_fails(pins, monkeypatch):
    monkeypatch.setattr(runtime.metadata, "version", lambda _: "0.0")
    result = runtime.build_evidence(pins, "3.11.18")
    assert result["status"] == "FAIL"
    assert {r["distribution"] for r in result["errors"]} == {"pandas", "numpy"}


@pytest.mark.parametrize("text,code", [
    ("pandas>=2.3", "EXACT_RUNTIME_PIN_REQUIRED"),
    ("pandas @ https://example.invalid/x", "EXACT_RUNTIME_PIN_REQUIRED"),
    ("--extra-index-url TOKEN", "EXACT_RUNTIME_PIN_REQUIRED"),
    ("unapproved-secret-name==1.0", "RUNTIME_PIN_NOT_ALLOWLISTED"),
    ("pandas==2.3.3\npandas==2.3.3", "DUPLICATE_RUNTIME_PIN"),
    ("# nothing\n", "EMPTY_RUNTIME_CONSTRAINTS"),
])
def test_invalid_pin_contract_rejected(tmp_path, text, code):
    path = tmp_path / "pins.txt"
    path.write_text(text, encoding="utf-8")
    with pytest.raises(runtime.EvidenceError, match="^" + code + "$"):
        runtime.read_pins(path)


def test_metadata_and_environment_secrets_not_serialized(pins, monkeypatch):
    secret = "https://user:PRIVATE_TOKEN@example.invalid"
    monkeypatch.setenv("KIS_APP_SECRET", secret)
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", secret)
    monkeypatch.setattr(runtime.metadata, "version", lambda _: secret)
    result = runtime.build_evidence(pins)
    text = json.dumps(result)
    assert secret not in text and "https://" not in text and "PRIVATE_TOKEN" not in text
    assert result["status"] == "FAIL"
    assert all(value is None for value in result["versions"].values())


def test_runtime_write_digest_and_no_overwrite(pins, tmp_path):
    path = tmp_path / "debug" / "runtime.json"
    result = runtime.build_evidence(pins)
    digest = runtime.write_evidence(path, result)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
    assert path.with_suffix(".json.sha256").read_text().strip() == digest
    before = path.read_bytes()
    with pytest.raises(runtime.EvidenceError, match="EVIDENCE_OUTPUT_ALREADY_EXISTS"):
        runtime.write_evidence(path, {"bad": True})
    assert path.read_bytes() == before


def test_runtime_existing_digest_prevents_partial_overwrite(tmp_path):
    path = tmp_path / "runtime.json"
    path.with_suffix(".json.sha256").write_text("original")
    with pytest.raises(runtime.EvidenceError):
        runtime.write_evidence(path, {})
    assert not path.exists()


def test_runtime_cli_returns_failure_with_stored_evidence(pins, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(runtime.platform, "python_version", lambda: "3.12.14")
    output = tmp_path / "runtime.json"
    assert runtime.main(["--constraints", str(pins), "--require-python", "3.11.18", "--output", str(output)]) == 1
    assert json.loads(output.read_text())["status"] == "FAIL"
    assert json.loads(capsys.readouterr().out)["status"] == "FAIL"


def test_child_environment_is_allowlisted(tmp_path, monkeypatch):
    for key in ("KIS_APP_KEY", "TELEGRAM_BOT_TOKEN", "GITHUB_TOKEN", "HTTP_PROXY",
                "PYTEST_ADDOPTS", "GITHUB_STEP_SUMMARY", "PYTHONSTARTUP"):
        monkeypatch.setenv(key, "PRIVATE")
    result = replay.isolated_environment(tmp_path, tmp_path / "tmp")
    assert "PRIVATE" not in json.dumps(result)
    assert result["PYTHONPATH"] == str(tmp_path)
    assert result["PYTHONDONTWRITEBYTECODE"] == "1"
    assert result["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] == "1"


def test_child_uses_current_interpreter_and_importlib(tmp_path, monkeypatch):
    code = tmp_path / "code"
    code.mkdir()
    def run(command, **kwargs):
        assert command[0] == replay.sys.executable
        assert command[1] == "-c"
        assert "--import-mode=importlib" in command[2]
        assert "socket.connect" in command[2]
        assert kwargs["env"]["PYTHONPATH"] == str(code)
        assert kwargs["cwd"] != code
        proof, junit = Path(command[4]), Path(command[6])
        proof.write_text(json.dumps({"all_from_selected_root": True, "network_attempt_count": 0}))
        junit.write_text('<testsuites><testsuite tests="1" failures="0" errors="0" skipped="0"/></testsuites>')
        return SimpleNamespace(returncode=0, stdout=b"fixture stdout", stderr=b"")
    monkeypatch.setattr(replay.subprocess, "run", run)
    result = replay.execute("sample", code, "test_fixture.py", tmp_path)
    assert replay.successful(result)
    assert not (tmp_path / "sample" / "stdout.txt").exists()


@pytest.mark.parametrize("change", ["wrong_root", "network", "skipped", "error", "zero", "returncode"])
def test_candidate_success_is_strict(change):
    result = {"returncode": 0, "import_proof": {"all_from_selected_root": True, "network_attempt_count": 0},
              "junit": {"tests": 1, "failures": 0, "errors": 0, "skipped": 0}}
    if change == "wrong_root": result["import_proof"]["all_from_selected_root"] = False
    if change == "network": result["import_proof"]["network_attempt_count"] = 1
    if change == "skipped": result["junit"]["skipped"] = 1
    if change == "error": result["junit"]["errors"] = 1
    if change == "zero": result["junit"]["tests"] = 0
    if change == "returncode": result["returncode"] = 1
    assert not replay.successful(result)


@pytest.mark.parametrize("kind,function,matched", [
    ("ValueError", "pandas._libs.tslibs.strptime.DatetimeParseState.process_datetime", True),
    ("ValueError", "array_strptime", False),
    ("KeyError", "pandas._libs.tslibs.strptime.DatetimeParseState.process_datetime", False),
    ("ValueError", "unrelated", False),
])
def test_baseline_cause_requires_original_valueerror_strptime(tmp_path, kind, function, matched):
    path = tmp_path / "tmp" / "test_case" / "debug" / "1700" / "pipeline_failure.json"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"exception_type": kind, "frames": [
        {"file": "compute.py", "function": "_series_from_raw"},
        {"file": "strptime.pyx", "function": function}]}))
    records = replay.expected_baseline_cause(tmp_path)
    assert records[0]["value_error_strptime_match"] is matched
    assert records[0]["sha256"] == replay.sha(path)


def test_replay_refuses_existing_output(tmp_path):
    baseline, candidate, output = (tmp_path / n for n in ("baseline", "candidate", "evidence"))
    for root in (baseline, candidate):
        (root / "src").mkdir(parents=True)
        (root / "pipeline.py").write_text("# unchanged")
    test_file = candidate / replay.REPLAY_NODE.split("::")[0]
    test_file.parent.mkdir()
    test_file.write_text("# fixture")
    output.mkdir()
    (output / "prior.json").write_text("preserve")
    with pytest.raises(FileExistsError):
        replay.run_replay(baseline, candidate, output)
    assert (output / "prior.json").read_text() == "preserve"


def test_replay_refuses_baseline_output_or_same_code(tmp_path):
    with pytest.raises(replay.ReplayError, match="DISTINCT_REPLAY_ROOTS_REQUIRED"):
        replay.run_replay(tmp_path / "baseline", tmp_path / "baseline", tmp_path / "new")
    with pytest.raises(replay.ReplayError, match="DISTINCT_REPLAY_ROOTS_REQUIRED"):
        replay.run_replay(tmp_path / "baseline", tmp_path / "candidate", tmp_path / "baseline" / "evidence")


@pytest.mark.parametrize("fault", [None, "wrong_message", "wrong_frame", "source_mutation"])
def test_full_replay_requires_expected_cause_and_unchanged_sources(tmp_path, monkeypatch, fault):
    baseline, candidate, output = (tmp_path / n for n in ("baseline", "candidate", "evidence"))
    for root in (baseline, candidate):
        (root / "src").mkdir(parents=True)
        (root / "pipeline.py").write_text("# original")
    test_file = candidate / replay.REPLAY_NODE.split("::")[0]
    test_file.parent.mkdir()
    test_file.write_text("# fixture")
    calls = []
    def execute(label, root, target, destination):
        calls.append((label, root, target))
        (destination / label).mkdir()
        baseline_stage = label == "baseline"
        if baseline_stage:
            path = destination / label / "tmp" / "case" / "pipeline_failure.json"
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps({"exception_type": "ValueError", "frames": [
                {"file": "compute.py", "function": "_series_from_raw"},
                {"file": "strptime.pyx", "function": "unrelated" if fault == "wrong_frame" else
                 "pandas._libs.tslibs.strptime.DatetimeParseState.process_datetime"}]}))
        if label == "candidate-all" and fault == "source_mutation":
            (candidate / "pipeline.py").write_text("changed")
        return {"returncode": 1 if baseline_stage else 0,
                "junit": {"tests": 1, "failures": 1 if baseline_stage else 0, "errors": 0, "skipped": 0},
                "import_proof": {"all_from_selected_root": True, "network_attempt_count": 0,
                                 "exception_classifications": [{"is_value_error": True,
                                  "classification": "OTHER" if fault == "wrong_message" else "TZ_AWARE_REQUIRES_UTC"}]
                                 if baseline_stage else []}}
    monkeypatch.setattr(replay, "execute", execute)
    result = replay.run_replay(baseline, candidate, output)
    assert result["status"] == ("PASS" if fault is None else "FAIL")
    assert calls[0][1] == baseline and calls[1][1] == candidate
    assert calls[0][2] == calls[1][2]
    assert calls[2][2] == str(candidate / "tests")
    assert (output / "REPLAY_RESULT.json.sha256").read_text().strip() == replay.sha(output / "REPLAY_RESULT.json")
