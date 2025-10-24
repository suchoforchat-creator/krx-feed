from __future__ import annotations

from pathlib import Path

import pytest

import update_history

SCHEMA = [
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


def make_row(ts: str, asset: str, key: str, value: str, quality: str = "final", source: str = "KRX"):
    return {
        "ts_kst": ts,
        "asset": asset,
        "key": key,
        "value": value,
        "unit": "",
        "window": "",
        "change_abs": "",
        "change_pct": "",
        "source": source,
        "quality": quality,
        "url": "https://example.com",
        "notes": "",
    }


def write_detail(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fp:
        import csv

        writer = csv.DictWriter(fp, fieldnames=SCHEMA)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def test_build_snapshot_prefers_final_quality():
    rows = [
        make_row("2024-01-01 17:00", "KOSPI", "idx", "2500", quality="primary", source="KRX"),
        make_row("2024-01-01 17:00", "KOSPI", "idx", "2550", quality="final", source="KRX"),
        make_row("2024-01-01 17:05", "USD/KRW", "spot", "1300", quality="secondary", source="FX"),
        make_row("2024-01-01 17:05", "USD/KRW", "spot", "1305", quality="final", source="FX"),
        make_row("2024-01-01 17:00", "KOSPI200", "hv30", "0.23", quality="final", source="KRX"),
    ]
    snapshot = update_history.build_snapshot_row(rows)
    assert snapshot["kospi"] == "2550"
    assert snapshot["usdkrw"] == "1305"
    assert snapshot["k200_hv30"] == "0.23"
    assert snapshot["quality"] == "final"


def test_build_snapshot_rejects_aggregated():
    aggregated = {col: "" for col in update_history.AGG_COLUMNS}
    aggregated["time_kst"] = "2024-01-01 17:00"
    with pytest.raises(ValueError):
        update_history.build_snapshot_row([aggregated])


def test_process_writes_history_and_latest(tmp_path, monkeypatch):
    latest_path = tmp_path / "out" / "latest.csv"
    history_path = tmp_path / "out" / "history.csv"
    index_path = tmp_path / "out" / "daily" / "index.csv"
    daily_dir = tmp_path / "out" / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True)
    (daily_dir / "20240101.csv").write_text("sample", encoding="utf-8")
    monkeypatch.setenv("GITHUB_REPOSITORY", "test/repo")

    detail_rows = [
        make_row("2024-01-02 17:00", "KOSPI", "idx", "2500"),
        make_row("2024-01-02 17:00", "KOSDAQ", "idx", "900"),
        make_row("2024-01-02 17:00", "USD/KRW", "spot", "1300"),
        make_row("2024-01-02 17:00", "DXY", "idx", "102"),
        make_row("2024-01-02 17:00", "UST2Y", "yield", "0.04"),
        make_row("2024-01-02 17:00", "UST10Y", "yield", "0.05"),
        make_row("2024-01-02 17:00", "KR3Y", "yield", "0.035"),
        make_row("2024-01-02 17:00", "KR10Y", "yield", "0.045"),
        make_row("2024-01-02 17:00", "TIPS10Y", "yield", "0.02"),
        make_row("2024-01-02 17:00", "WTI", "price", "75"),
        make_row("2024-01-02 17:00", "Brent", "curve_M1", "78"),
        make_row("2024-01-02 17:00", "Gold", "price", "1920"),
        make_row("2024-01-02 17:00", "Copper", "price", "3.8"),
        make_row("2024-01-02 17:00", "BTC", "price", "42000"),
        make_row("2024-01-02 17:00", "KOSPI", "advance", "300"),
        make_row("2024-01-02 17:00", "KOSPI", "decline", "500"),
        make_row("2024-01-02 17:00", "KOSPI", "unchanged", "50"),
        make_row("2024-01-02 17:00", "KOSDAQ", "advance", "400"),
        make_row("2024-01-02 17:00", "KOSDAQ", "decline", "600"),
        make_row("2024-01-02 17:00", "KOSDAQ", "unchanged", "80"),
        make_row("2024-01-02 17:00", "KOSPI200", "hv30", "0.21"),
    ]
    write_detail(latest_path, detail_rows)

    update_history.process(
        str(latest_path),
        str(history_path),
        days=90,
        index_path=str(index_path),
    )

    # latest.csv rewritten with aggregate schema
    import csv

    with latest_path.open("r", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        row = next(reader)
    assert reader.fieldnames == update_history.AGG_COLUMNS
    assert row["kospi"] == "2500"
    assert row["kosdaq"] == "900"
    assert row["usdkrw"] == "1300"
    assert row["k200_hv30"] == "0.21"
    assert row["quality"] == "final"

    with history_path.open("r", encoding="utf-8") as fp:
        history_reader = csv.DictReader(fp)
        history_rows = list(history_reader)
    assert len(history_rows) == 1
    assert history_rows[0]["kospi"] == "2500"

    with index_path.open("r", encoding="utf-8") as fp:
        index_reader = csv.DictReader(fp)
        index_rows = list(index_reader)
    assert index_reader.fieldnames == ["date_kst", "url"]
    assert index_rows[0]["date_kst"] == "2024-01-01"
    assert index_rows[0]["url"].endswith("/out/daily/20240101.csv")


def test_upsert_history_overwrites_duplicate(tmp_path):
    history_path = tmp_path / "history.csv"
    row = {col: "" for col in update_history.AGG_COLUMNS}
    row["time_kst"] = "2024-01-01 17:00"
    row["kospi"] = "2500"
    update_history.upsert_history(str(history_path), row, days=90)

    updated = row.copy()
    updated["kospi"] = "2600"
    update_history.upsert_history(str(history_path), updated, days=90)

    import csv

    with history_path.open("r", encoding="utf-8") as fp:
        reader = csv.DictReader(fp)
        rows = list(reader)
    assert len(rows) == 1
    assert rows[0]["kospi"] == "2600"
