"""Same interpreter, preserved baseline FAIL -> candidate PASS, fixture-only."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import xml.etree.ElementTree as ET


REPLAY_NODE = "tests/test_core_collection_integration.py::test_run645_transport_boundary_replay"
_CHILD = r'''
import json, pathlib, re, sys
root, proof, basetemp, junit, test = sys.argv[1:]
network_attempts = []
def audit(event, args):
    if event in {"socket.connect", "socket.getaddrinfo", "socket.sendto"}:
        network_attempts.append(event)
        raise RuntimeError("OFFLINE_REPLAY_NETWORK_FORBIDDEN")
sys.addaudithook(audit)
import pipeline
original_failure_writer = pipeline.write_failure
exception_classifications = []
def recording_failure(path, exc, stage):
    # Classify the known pandas message without saving messages, locals or URLs.
    is_utc_error = (type(exc) is ValueError and re.match(
        r"^Tz-aware datetime\.datetime cannot be converted to datetime64 unless utc=True(?:, at position \d+)?(?:\.|$)",
        str(exc)) is not None)
    exception_classifications.append({"is_value_error": type(exc) is ValueError,
        "classification": "TZ_AWARE_REQUIRES_UTC" if is_utc_error else "OTHER",
        "diagnostic_name": pathlib.Path(path).name})
    return original_failure_writer(path, exc, stage)
pipeline.write_failure = recording_failure
import pytest
rc = pytest.main([test, "--import-mode=importlib", "-p", "no:cacheprovider",
                  "--basetemp", basetemp, "--junitxml", junit, "-q", "--tb=short"])
base = pathlib.Path(root).resolve()
modules = {}
for name in ("pipeline", "src.compute", "src.failure_diagnostic", "src.kis.client"):
    module = sys.modules.get(name)
    try:
        relative = pathlib.Path(module.__file__).resolve().relative_to(base).as_posix()
    except (AttributeError, ValueError, TypeError):
        relative = None
    modules[name] = relative
valid = all(modules.values())
with open(proof, "x", encoding="utf-8") as handle:
    json.dump({"schema_version": 1, "modules": modules, "all_from_selected_root": valid,
               "network_attempt_count": len(network_attempts), "pytest_exit_code": int(rc),
               "exception_classifications": exception_classifications}, handle)
sys.exit(int(rc) if valid and not network_attempts else 90)
'''


class ReplayError(ValueError):
    pass


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write_json(path, payload):
    with Path(path).open("x", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2)
        handle.write("\n")


def isolated_environment(root, temporary):
    # No broker/GitHub/Telegram credentials, proxies or user pytest options.
    result = {key: os.environ[key] for key in ("PATH", "SYSTEMROOT", "WINDIR", "COMSPEC")
              if key in os.environ}
    result.update(PYTHONPATH=str(root), PYTHONDONTWRITEBYTECODE="1",
                  PYTHONNOUSERSITE="1", PYTHONIOENCODING="utf-8",
                  PYTEST_DISABLE_PLUGIN_AUTOLOAD="1", TMP=str(temporary),
                  TEMP=str(temporary), TMPDIR=str(temporary), HOME=str(temporary),
                  USERPROFILE=str(temporary), XDG_CACHE_HOME=str(temporary / "cache"))
    return result


def snapshot(root):
    """Preserve code/fixtures and existing business output, not .git metadata."""
    result = {}
    for directory in ("src", "tests", "tools", "out", "debug", "config"):
        base = root / directory
        if base.is_dir():
            for path in base.rglob("*"):
                if path.is_file() and "__pycache__" not in path.parts:
                    result[path.relative_to(root).as_posix()] = sha(path)
    for name in ("pipeline.py", "requirements.txt", "constraints-runtime.txt"):
        path = root / name
        if path.is_file():
            result[name] = sha(path)
    return result


def junit_counts(path):
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    return {name: sum(int(s.get(name, "0")) for s in suites)
            for name in ("tests", "failures", "errors", "skipped")}


def execute(label, code_root, test_target, output):
    folder = output / label
    folder.mkdir()
    cwd, temporary = folder / "cwd", folder / "environment"
    cwd.mkdir()
    temporary.mkdir()
    proof, junit, basetemp = folder / "IMPORT_PROOF.json", folder / "junit.xml", folder / "tmp"
    command = [sys.executable, "-c", _CHILD, str(code_root), str(proof), str(basetemp),
               str(junit), str(test_target)]
    completed = subprocess.run(command, cwd=cwd, env=isolated_environment(code_root, temporary),
                               capture_output=True, check=False, timeout=600)
    # Save no raw subprocess output: fixture diagnostics + JUnit carry the cause.
    result = {"returncode": completed.returncode,
              "stdout_bytes": len(completed.stdout), "stdout_sha256": hashlib.sha256(completed.stdout).hexdigest(),
              "stderr_bytes": len(completed.stderr), "stderr_sha256": hashlib.sha256(completed.stderr).hexdigest(),
              "junit": junit_counts(junit) if junit.is_file() else None,
              "import_proof": json.loads(proof.read_text(encoding="utf-8")) if proof.is_file() else None}
    write_json(folder / "PROCESS_RESULT.json", result)
    return result


def expected_baseline_cause(folder):
    evidence = []
    for path in sorted((folder / "tmp").rglob("pipeline_failure.json")):
        item = json.loads(path.read_text(encoding="utf-8"))
        frames = item.get("frames", [])
        series_positions = [i for i, frame in enumerate(frames)
                            if frame.get("file") == "compute.py"
                            and frame.get("function") == "_series_from_raw"]
        timezone_positions = [i for i, frame in enumerate(frames)
                              if frame.get("file") == "strptime.pyx"
                              and frame.get("function") ==
                              "pandas._libs.tslibs.strptime.DatetimeParseState.process_datetime"]
        matched = (item.get("exception_type") == "ValueError"
                   and bool(series_positions and timezone_positions)
                   and series_positions[-1] < timezone_positions[-1])
        evidence.append({"path": path.relative_to(folder).as_posix(), "sha256": sha(path),
                         "exception_type": item.get("exception_type"),
                         "value_error_strptime_match": matched})
    return evidence


def valid_imports(result):
    proof = result.get("import_proof") or {}
    return proof.get("all_from_selected_root") is True and proof.get("network_attempt_count") == 0


def successful(result):
    counts = result.get("junit") or {}
    return (result["returncode"] == 0 and valid_imports(result)
            and counts.get("tests", 0) > 0 and counts.get("failures") == 0
            and counts.get("errors") == 0 and counts.get("skipped") == 0)


def run_replay(baseline_root, candidate_root, output_dir):
    baseline, candidate, output = (Path(p).resolve() for p in (baseline_root, candidate_root, output_dir))
    if (baseline == candidate or baseline in candidate.parents or candidate in baseline.parents
            or baseline in output.parents or output in (baseline, candidate)):
        raise ReplayError("DISTINCT_REPLAY_ROOTS_REQUIRED")
    for root in (baseline, candidate):
        if not (root / "pipeline.py").is_file() or not (root / "src").is_dir():
            raise ReplayError("REPLAY_SOURCE_ROOT_INVALID")
    node = str(candidate / REPLAY_NODE)
    if not (candidate / REPLAY_NODE.split("::")[0]).is_file():
        raise ReplayError("REPLAY_NODE_FILE_MISSING")
    # Reject even an empty pre-existing folder; never let pytest erase old basetemp.
    output.mkdir(parents=True, exist_ok=False)
    before = {"baseline": snapshot(baseline), "candidate": snapshot(candidate)}
    write_json(output / "PREIMAGE.json", before)
    baseline_result = execute("baseline", baseline, node, output)
    causes = expected_baseline_cause(output / "baseline")
    counts = baseline_result.get("junit") or {}
    classifications = (baseline_result.get("import_proof") or {}).get("exception_classifications", [])
    expected_fail = (baseline_result["returncode"] == 1 and valid_imports(baseline_result)
                     and counts.get("tests", 0) > 0 and counts.get("errors") == 0
                     and counts.get("skipped") == 0 and counts.get("failures") == counts.get("tests")
                     and len(causes) >= counts.get("tests", 0)
                     and all(c["value_error_strptime_match"] for c in causes)
                     and len(classifications) >= counts.get("tests", 0)
                     and all(c.get("classification") == "TZ_AWARE_REQUIRES_UTC"
                             and c.get("is_value_error") is True for c in classifications))
    candidate_result = execute("candidate-same-node", candidate, node, output)
    all_result = execute("candidate-all", candidate, str(candidate / "tests"), output)
    unchanged = before == {"baseline": snapshot(baseline), "candidate": snapshot(candidate)}
    passed = expected_fail and successful(candidate_result) and successful(all_result) and unchanged
    evidence = {"schema_version": 1, "status": "PASS" if passed else "FAIL",
                "same_interpreter_for_all_stages": True, "fixture_node": REPLAY_NODE,
                "baseline_expected_failure": expected_fail, "baseline_causes": causes,
                "baseline": baseline_result, "candidate_same_node": candidate_result,
                "candidate_all": all_result, "source_and_existing_outputs_unchanged": unchanged,
                "source_preimage_sha256": sha(output / "PREIMAGE.json"),
                "network_or_live_collection_authorized": False}
    write_json(output / "REPLAY_RESULT.json", evidence)
    with (output / "REPLAY_RESULT.json.sha256").open("x", encoding="ascii") as handle:
        handle.write(sha(output / "REPLAY_RESULT.json") + "\n")
    return evidence


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-root", type=Path, required=True)
    parser.add_argument("--candidate-root", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        result = run_replay(args.baseline_root, args.candidate_root, args.output_dir)
    except (ReplayError, OSError, ValueError, subprocess.TimeoutExpired, ET.ParseError):
        print(json.dumps({"status": "FAIL", "code": "OFFLINE_REPLAY_REJECTED"}))
        return 2
    print(json.dumps({"status": result["status"], "baseline_expected_failure": result["baseline_expected_failure"],
                      "candidate_all": result["candidate_all"]["junit"]}))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
