"""Offline, allowlisted runtime versions; never serialize environment or URLs."""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata as metadata
import json
from pathlib import Path
import platform
import re


ALLOWED_DISTRIBUTIONS = (
    "pandas", "numpy", "pytz", "tzdata", "python-dateutil", "pyyaml", "requests",
    "websockets", "tenacity", "beautifulsoup4", "lxml", "yfinance", "pykrx",
    "pyarrow", "html5lib", "pytest", "curl-cffi", "certifi", "charset-normalizer",
    "idna", "urllib3", "cffi", "pycparser", "peewee", "multitasking",
    "frozendict", "platformdirs", "protobuf", "soupsieve", "typing-extensions",
    "six", "packaging", "pluggy", "iniconfig", "pygments", "colorama",
    "webencodings", "setuptools", "pip", "wheel", "xlsxwriter", "xlrd",
)
_VERSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9.!+_-]{0,79}\Z")
_PIN = re.compile(r"([A-Za-z0-9][A-Za-z0-9_.-]*)==([A-Za-z0-9][A-Za-z0-9.!+_-]{0,79})\Z")


class EvidenceError(ValueError):
    """Only fixed error codes should cross the CLI boundary."""


def canonical_name(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def read_pins(path):
    raw = Path(path).read_bytes()
    pins = {}
    for line in raw.decode("utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = _PIN.fullmatch(line)
        if match is None:
            raise EvidenceError("EXACT_RUNTIME_PIN_REQUIRED")
        name, version = canonical_name(match[1]), match[2]
        if name not in ALLOWED_DISTRIBUTIONS:
            raise EvidenceError("RUNTIME_PIN_NOT_ALLOWLISTED")
        if name in pins:
            raise EvidenceError("DUPLICATE_RUNTIME_PIN")
        pins[name] = version
    if not pins:
        raise EvidenceError("EMPTY_RUNTIME_CONSTRAINTS")
    return pins, hashlib.sha256(raw).hexdigest()


def build_evidence(constraints, require_python=None):
    pins, constraints_sha = read_pins(constraints)
    if require_python is not None and not re.fullmatch(r"\d+\.\d+\.\d+", require_python):
        raise EvidenceError("EXACT_PYTHON_VERSION_REQUIRED")
    actual_python = platform.python_version()
    versions, errors = {}, []
    for name in ALLOWED_DISTRIBUTIONS:
        try:
            value = metadata.version(name)
        except metadata.PackageNotFoundError:
            value = None
        if value is not None and not _VERSION.fullmatch(value):
            errors.append({"code": "UNSAFE_VERSION_METADATA", "distribution": name})
            value = None
        versions[name] = value
        if name in pins and value != pins[name]:
            errors.append({"code": "RUNTIME_PIN_MISMATCH", "distribution": name})
    if require_python is not None and actual_python != require_python:
        errors.append({"code": "PYTHON_VERSION_MISMATCH"})
    return {
        "schema_version": 1,
        "status": "PASS" if not errors else "FAIL",
        "python_version": actual_python,
        "required_python": require_python,
        "python_implementation": platform.python_implementation(),
        "operating_system": platform.system(),
        "constraints_sha256": constraints_sha,
        "exact_pins": pins,
        "versions": versions,
        "errors": errors,
        "metadata_policy": "ALLOWLISTED_DISTRIBUTION_VERSIONS_ONLY",
        "environment_dumped": False,
        "network_calls": 0,
    }


def write_evidence(output, evidence):
    output = Path(output)
    digest_path = output.with_name(output.name + ".sha256")
    if output.exists() or digest_path.exists():
        raise EvidenceError("EVIDENCE_OUTPUT_ALREADY_EXISTS")
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = (json.dumps(evidence, ensure_ascii=False, sort_keys=True, indent=2) + "\n").encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    with output.open("xb") as handle:
        handle.write(payload)
    # Exclusive creation preserves earlier evidence even under a competing writer.
    with digest_path.open("x", encoding="ascii") as handle:
        handle.write(digest + "\n")
    return digest


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--constraints", type=Path,
                        default=Path(__file__).resolve().parents[1] / "constraints-runtime.txt")
    parser.add_argument("--require-python")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        evidence = build_evidence(args.constraints, args.require_python)
        digest = write_evidence(args.output, evidence)
        print(json.dumps({"status": evidence["status"], "sha256": digest}))
        return 0 if evidence["status"] == "PASS" else 1
    except (EvidenceError, OSError, UnicodeError):
        # Exception strings can include paths or untrusted metadata; do not print them.
        print(json.dumps({"status": "FAIL", "code": "RUNTIME_EVIDENCE_REJECTED"}))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
