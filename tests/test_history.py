from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

import update_history


KST = timezone(timedelta(hours=9))


def write_latest(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows)
    frame.to_csv(path, index=False)


def test_upsert_runs_without_time_guard(tmp_path: Path) -> None:
    """시간 가드를 제거했으므로 아무 시각에서도 업서트가 실행되는지 확인합니다."""

    latest_path = tmp_path / "out" / "latest.csv"
    history_path = tmp_path / "out" / "history.csv"
    write_latest(
        latest_path,
        [
            {
                "ts_kst": "2024-02-01 15:30:00",
                "asset": "KOSPI",
                "key": "idx",
                "value": 2500,
                "unit": "idx",
                "window": "EOD",
                "source": "krx",
                "quality": "final",
                "notes": "",
            }
        ],
    )

    now = datetime(2024, 2, 2, 15, 0, tzinfo=KST)
    report = update_history.upsert_from_latest(latest_path, history_path, now=now)

    # 시간 가드가 비활성화되었으므로 언제 실행해도 history.csv가 생성되어야 합니다.
    assert history_path.exists()
    frame = pd.read_csv(history_path)
    assert len(frame) == 1
    assert float(frame.iloc[0]["kospi"]) == 2500.0
    # 디버그 로그에 비활성화 메시지가 남는지도 확인합니다.
    assert any(step["message"] == "시간 가드 비활성화" for step in report.steps)


def test_upsert_creates_row(tmp_path: Path) -> None:
    latest_path = tmp_path / "out" / "latest.csv"
    history_path = tmp_path / "out" / "history.csv"
    rows = [
        {
            "ts_kst": "2024-02-02 15:30:00",
            "asset": "KOSPI",
            "key": "idx",
            "value": 2550,
            "unit": "idx",
            "window": "EOD",
            "source": "krx",
            "quality": "final",
            "notes": "",
        },
        {
            "ts_kst": "2024-02-02 15:30:00",
            "asset": "USD/KRW",
            "key": "spot",
            "value": 1320.5,
            "unit": "KRW",
            "window": "EOD",
            "source": "bok",
            "quality": "secondary",
            "notes": "",
        },
        {
            "ts_kst": "2024-02-02 10:30:00",
            "asset": "USD/KRW",
            "key": "spot",
            "value": 1310.0,
            "unit": "KRW",
            "window": "",
            "source": "bok",
            "quality": "secondary",
            "notes": "",
        },
    ]
    write_latest(latest_path, rows)

    now = datetime(2024, 2, 2, 17, 5, tzinfo=KST)
    report = update_history.upsert_from_latest(
        latest_path,
        history_path,
        now=now,
    )

    frame = pd.read_csv(history_path)
    assert len(frame) == 1
    record = frame.iloc[0]
    assert record["time_kst"] == "2024-02-02 15:30:00"
    assert float(record["kospi"]) == 2550
    assert float(record["usdkrw"]) == 1320.5
    assert record["src_tag"] == "bok|krx"
    assert record["quality"] == "secondary"
    assert report.field_status["kospi"]["status"] == "ok_eod"


def test_upsert_overwrites_existing_row(tmp_path: Path) -> None:
    latest_path = tmp_path / "out" / "latest.csv"
    history_path = tmp_path / "out" / "history.csv"

    # 최초 history 파일 생성
    initial = pd.DataFrame(
        [
            {
                "time_kst": "2024-02-02 15:30:00",
                "kospi": "2500",
                "kosdaq": "900",
                "kospi_adv": "",
                "kospi_dec": "",
                "kospi_unch": "",
                "kosdaq_adv": "",
                "kosdaq_dec": "",
                "kosdaq_unch": "",
                "usdkrw": "",
                "dxy": "",
                "ust2y": "",
                "ust10y": "",
                "kr3y": "",
                "kr10y": "",
                "tips10y": "",
                "wti": "",
                "brent": "",
                "gold": "",
                "copper": "",
                "btc": "",
                "k200_hv30": "",
                "src_tag": "krx",
                "quality": "final",
            }
        ]
    )
    history_path.parent.mkdir(parents=True, exist_ok=True)
    initial.to_csv(history_path, index=False)

    write_latest(
        latest_path,
        [
            {
                "ts_kst": "2024-02-02 15:30:00",
                "asset": "KOSPI",
                "key": "idx",
                "value": 2600,
                "unit": "idx",
                "window": "EOD",
                "source": "krx",
                "quality": "final",
                "notes": "",
            }
        ],
    )

    now = datetime(2024, 2, 2, 17, 10, tzinfo=KST)
    update_history.upsert_from_latest(latest_path, history_path, now=now)

    frame = pd.read_csv(history_path)
    assert len(frame) == 1
    assert float(frame.iloc[0]["kospi"]) == 2600


def test_upsert_any_window_fallback(tmp_path: Path) -> None:
    latest_path = tmp_path / "out" / "latest.csv"
    history_path = tmp_path / "out" / "history.csv"

    write_latest(
        latest_path,
        [
            {
                "ts_kst": "2024-02-05 15:30:00",
                "asset": "USD/KRW",
                "key": "spot",
                "value": 1333.3,
                "unit": "KRW",
                "window": "",  # 의도적으로 EOD 누락
                "source": "bok",
                "quality": "secondary",
                "notes": "",
            }
        ],
    )

    now = datetime(2024, 2, 5, 17, 3, tzinfo=KST)
    report = update_history.upsert_from_latest(latest_path, history_path, now=now)

    frame = pd.read_csv(history_path)
    record = frame.iloc[0]
    assert float(record["usdkrw"]) == 1333.3
    assert report.field_status["usdkrw"]["status"] == "ok_any"


def test_upsert_skips_out_of_range(tmp_path: Path) -> None:
    latest_path = tmp_path / "out" / "latest.csv"
    history_path = tmp_path / "out" / "history.csv"
    write_latest(
        latest_path,
        [
            {
                "ts_kst": "2024-02-02 15:30:00",
                "asset": "UST2Y",
                "key": "yield",
                "value": 25.0,
                "unit": "pct",
                "window": "EOD",
                "source": "fred",
                "quality": "final",
                "notes": "",
            }
        ],
    )

    now = datetime(2024, 2, 2, 17, 2, tzinfo=KST)
    report = update_history.upsert_from_latest(latest_path, history_path, now=now)

    frame = pd.read_csv(history_path)
    record = frame.iloc[0]
    assert pd.isna(record["ust2y"]) or record["ust2y"] == ""
    assert report.field_status["ust2y"]["status"] == "range_violation"

