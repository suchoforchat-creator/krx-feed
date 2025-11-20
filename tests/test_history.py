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


def test_upsert_runs_even_outside_previous_window(tmp_path: Path) -> None:
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

    assert history_path.exists()
    frame = pd.read_csv(history_path, dtype=str).fillna("")
    assert len(frame) == 1
    record = frame.iloc[0]
    assert record["time_kst"] == "2024-02-01 15:30:00"
    assert float(record["kospi"]) == 2500
    # 디버그 로그가 시간 가드 비활성화를 기록했는지 확인합니다.
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

    frame = pd.read_csv(history_path, dtype=str).fillna("")
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

    frame = pd.read_csv(history_path, dtype=str).fillna("")
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

    frame = pd.read_csv(history_path, dtype=str).fillna("")
    record = frame.iloc[0]
    assert float(record["usdkrw"]) == 1333.3


def test_upsert_maps_spot_keys(tmp_path: Path) -> None:
    """WTI/Brent/Gold/Copper/BTC가 spot 키로 들어와도 history에 기록되는지 검사."""

    latest_path = tmp_path / "out" / "latest.csv"
    history_path = tmp_path / "out" / "history.csv"

    rows = [
        {
            "ts_kst": "2024-02-06 15:30:00",
            "asset": asset,
            "key": "spot",
            "value": value,
            "unit": "USD",
            "window": "EOD",
            "source": "test-source",
            "quality": "final",
            "notes": "",
        }
        for asset, value in [
            ("WTI", 70.5),
            ("Brent", 75.2),
            ("Gold", 2033.4),
            ("Copper", 4.15),
            ("BTC", 61000.0),
        ]
    ]

    write_latest(latest_path, rows)

    now = datetime(2024, 2, 6, 17, 1, tzinfo=KST)
    report = update_history.upsert_from_latest(latest_path, history_path, now=now)

    frame = pd.read_csv(history_path, dtype=str).fillna("")
    record = frame.iloc[0]

    assert record["time_kst"] == "2024-02-06 15:30:00"
    assert float(record["wti"]) == 70.5
    assert float(record["brent"]) == 75.2
    assert float(record["gold"]) == 2033.4
    assert float(record["copper"]) == 4.15
    assert float(record["btc"]) == 61000.0

    # 디버깅 로그가 각 필드를 ok_eod로 표시했는지 확인하여 추후 문제가 생겼을 때 빠르게 파악할 수 있게 합니다.
    for column in ["wti", "brent", "gold", "copper", "btc"]:
        assert report.field_status[column]["status"] == "ok_eod"


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

    frame = pd.read_csv(history_path, dtype=str).fillna("")
    record = frame.iloc[0]
    assert pd.isna(record["ust2y"]) or record["ust2y"] == ""
    assert report.field_status["ust2y"]["status"] == "range_violation"


def test_upsert_writes_empty_row_when_latest_missing(tmp_path: Path) -> None:
    latest_path = tmp_path / "out" / "latest.csv"
    history_path = tmp_path / "out" / "history.csv"

    # latest.csv 헤더만 존재하고 실제 레코드가 없는 상황을 흉내냅니다.
    empty_latest = pd.DataFrame(
        columns=[
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
    )
    latest_path.parent.mkdir(parents=True, exist_ok=True)
    empty_latest.to_csv(latest_path, index=False)

    now = datetime(2024, 2, 7, 17, 1, tzinfo=KST)
    report = update_history.upsert_from_latest(latest_path, history_path, now=now)

    frame = pd.read_csv(history_path, dtype=str).fillna("")
    assert len(frame) == 1
    record = frame.iloc[0]
    # 실행 시각 날짜(2024-02-07) 기준으로 15:30 타임스탬프가 들어가는지 확인합니다.
    assert record["time_kst"] == "2024-02-07 15:30:00"
    # 값이 모두 공란이고 상태가 missing_latest로 표시되는지 검증합니다.
    assert record["kospi"] == ""
    assert report.field_status["kospi"]["status"] == "missing_latest"
    assert report.field_status["kospi"]["reason"] == "empty_latest_frame"

