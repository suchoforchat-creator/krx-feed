import datetime as dt

import pytest

from src import compute, utils


def test_schema_columns():
    assert utils.SCHEMA_COLUMNS == [
        "ts_kst",
        "asset",
        "key",
        "value",
        "unit",
        "window",
        "change_abs",
        "change_pct",
        "source",
        "quality",
        "url",
        "notes",
    ]


def test_record_builder_enforces_quality():
    ts = utils.make_timestamp("Asia/Seoul", dt.datetime(2024, 1, 2, 7, 30))
    builder = compute.RecordBuilder(ts)
    record = builder.make(
        "TEST",
        "value",
        123.4,
        unit="pt",
        window="1D",
        source="unit",
        quality="primary",
        url="https://example.com",
    )
    assert record["quality"] == "primary"
    assert record["ts_kst"].endswith("07:30")

    with pytest.raises(ValueError):
        utils.ensure_schema({
            "ts_kst": ts,
            "asset": "TEST",
            "key": "value",
            "value": 1.0,
            "unit": "pt",
            "window": "1D",
            "change_abs": 0,
            "change_pct": 0,
            "source": "unit",
            "quality": "invalid",
            "url": "https://example.com",
            "notes": "",
        })


def test_coverage_ratio():
    required = {("A", "x"), ("B", "y")}
    rows = [
        {"asset": "A", "key": "x", "value": 1},
        {"asset": "B", "key": "y", "value": None},
    ]
    coverage = utils.coverage_ratio(rows, required)
    assert coverage == pytest.approx(0.5)
