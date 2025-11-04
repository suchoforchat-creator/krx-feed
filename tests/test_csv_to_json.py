from __future__ import annotations

import json
from datetime import datetime

import pandas as pd

from tools.csv_to_json import build_json


def test_build_json_produces_numeric_and_null(tmp_path):
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    latest = pd.DataFrame(
        [
            {
                "time_kst": "2025-01-02 17:00",
                "kospi": "2,500.56789",
                "quality": "final",
                "note": "",
            }
        ]
    )
    history = pd.DataFrame(
        [
            {"time_kst": "2025-01-01 17:00", "kospi": "2,400", "quality": "final"},
            {"time_kst": "2025-01-02 17:00", "kospi": "2,500.56789", "quality": "final"},
        ]
    )

    latest_path = out_dir / "latest.csv"
    history_path = out_dir / "history.csv"
    latest.to_csv(latest_path, index=False)
    history.to_csv(history_path, index=False)

    latest_json_path = out_dir / "latest.json"
    history_json_path = out_dir / "history.json"

    build_json(latest_path, history_path, latest_json_path, history_json_path)

    latest_payload = json.loads(latest_json_path.read_text(encoding="utf-8"))
    history_payload = json.loads(history_json_path.read_text(encoding="utf-8"))

    assert latest_payload["kospi"] == 2500.5679
    assert latest_payload["quality"] == "final"
    assert "ts_kst" in latest_payload

    assert history_payload[0]["kospi"] == 2400
    assert history_payload[1]["kospi"] == 2500.5679
    assert history_payload[1]["quality"] == "final"

    # ensure ISO8601 offset present
    ts = datetime.fromisoformat(latest_payload["ts_kst"])
    assert ts.tzinfo is not None
