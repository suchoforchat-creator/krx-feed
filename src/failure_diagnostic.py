"""Safe exception evidence: no exception messages, source lines or locals."""
import json
import traceback
from pathlib import Path


def write_failure(path, exc, stage):
    frames = traceback.extract_tb(exc.__traceback__)
    evidence = {
        "schema_version": 1,
        "stage": stage,
        "exception_type": type(exc).__name__,
        "frames": [{"file": Path(f.filename).name, "line": f.lineno, "function": f.name}
                   for f in frames],
        "messages_and_locals_omitted": True,
    }
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return evidence

