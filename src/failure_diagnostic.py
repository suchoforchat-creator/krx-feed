"""Safe exception evidence: no exception messages, source lines or locals."""
import json
import traceback
import math
from pathlib import Path
from .source_dates import note_date


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
    # Only exact, non-secret codes are allowed; never include arbitrary errors.
    allowed = {"SOURCE_DATE_NOT_READY:KOSPI", "SOURCE_DATE_NOT_READY:KOSDAQ",
               "SOURCE_DATE_NOT_READY:empty_latest", "SOURCE_DATE_NOT_READY:notes_missing",
               "EXPECTED_DATE_MISMATCH", "RECOVERY_DATE_BINDING_REQUIRED",
               "EXACT_DATE_FORMAT_REQUIRED", "RECOVERY_OBSERVATION_SESSION_REQUIRED",
               "UNSUPPORTED_PHASE", "UNSUPPORTED_TIMEZONE", "HISTORY_OUTPUT_MISSING"}
    if str(exc) in allowed:
        evidence["reason_code"] = str(exc)
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return evidence


def write_history_input_review(path, observed_at, target_date, latest, history, selection):
    """Only core index dates/validity and row counts, never source text/URLs."""
    def core(rows):
        result = []
        for row in rows:
            if row.get("asset") not in {"KOSPI", "KOSDAQ"} or row.get("key") != "idx":
                continue
            day = note_date(row.get("notes"))
            try:
                value = float(row.get("value"))
                valid = math.isfinite(value) and value > 0
            except (TypeError, ValueError):
                valid = False
            result.append({"asset": row["asset"], "source_date": day.isoformat() if day else None,
                           "positive_finite_value": valid, "matches_target": day == target_date})
        return result
    counts = {key: int(selection[key]) for key in
              ("input_rows", "kept_rows", "excluded_after_target_rows", "excluded_unknown_source_date_rows")}
    evidence = {"schema_version": 1, "target_date": target_date.isoformat(),
                "observed_at": observed_at.isoformat(), "latest_core": core(latest),
                "history_core": core(history), "selection_counts": counts,
                "values_urls_messages_and_locals_omitted": True}
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return evidence

