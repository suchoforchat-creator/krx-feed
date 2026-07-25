from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

import update_history


KST = timezone(timedelta(hours=9))


def write_latest(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows)
    frame.to_csv(path, index=False)


def write_closed_dates(path: Path, rows: list[tuple[str, str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows, columns=["date", "reason", "source"])
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


def test_upsert_maps_kr10y_yield(tmp_path: Path) -> None:
    latest_path = tmp_path / "out" / "latest.csv"
    history_path = tmp_path / "out" / "history.csv"
    write_latest(
        latest_path,
        [
            {
                "ts_kst": "2026-07-24 17:00:00",
                "asset": "KR10Y",
                "key": "yield",
                "value": 4.447,
                "unit": "pct",
                "window": "EOD",
                "source": "BOK_ECOS",
                "quality": "secondary",
                "notes": "fallback:ecos:817Y002:010210000",
            }
        ],
    )

    update_history.upsert_from_latest(
        latest_path,
        history_path,
        now=datetime(2026, 7, 24, 17, 5, tzinfo=KST),
    )

    frame = pd.read_csv(history_path, dtype=str).fillna("")
    assert float(frame.iloc[0]["kr10y"]) == 4.447
    assert frame.iloc[0]["src_tag"] == "bok_ecos"


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


def test_upsert_maps_vix(tmp_path: Path) -> None:
    """VIX가 history.csv의 vix 컬럼으로 매핑되는지 확인합니다."""

    latest_path = tmp_path / "out" / "latest.csv"
    history_path = tmp_path / "out" / "history.csv"

    write_latest(
        latest_path,
        [
            {
                "ts_kst": "2024-02-06 15:30:00",
                "asset": "VIX",
                "key": "spot",
                "value": 14.2,
                "unit": "pt",
                "window": "EOD",
                "source": "cboe",
                "quality": "final",
                "notes": "",
            }
        ],
    )

    now = datetime(2024, 2, 6, 17, 1, tzinfo=KST)
    report = update_history.upsert_from_latest(latest_path, history_path, now=now)

    frame = pd.read_csv(history_path, dtype=str).fillna("")
    record = frame.iloc[0]

    assert float(record["vix"]) == 14.2
    assert report.field_status["vix"]["status"] == "ok_eod"


def test_upsert_keeps_large_vix_values(tmp_path: Path) -> None:
    """VIX가 다소 큰 값이어도 숫자라면 history에 남겨 디버깅할 수 있어야 합니다."""

    latest_path = tmp_path / "out" / "latest.csv"
    history_path = tmp_path / "out" / "history.csv"

    # 이전에는 상한 150으로 잘렸지만, 이제는 값이 있으면 남긴다.
    write_latest(
        latest_path,
        [
            {
                "ts_kst": "2024-02-06 15:30:00",
                "asset": "VIX",
                "key": "spot",
                "value": 180.5,
                "unit": "pt",
                "window": "EOD",
                "source": "cboe",
                "quality": "secondary",
                "notes": "",
            }
        ],
    )

    now = datetime(2024, 2, 6, 17, 1, tzinfo=KST)
    report = update_history.upsert_from_latest(latest_path, history_path, now=now)

    frame = pd.read_csv(history_path, dtype=str).fillna("")
    record = frame.iloc[0]

    assert float(record["vix"]) == 180.5
    # 범위는 벗어났지만 값은 남고, 상태에는 range_violation 힌트가 담긴다.
    assert report.field_status["vix"]["status"] in {"ok_eod", "range_violation"}


def test_upsert_accepts_second_precision_timestamps(tmp_path: Path) -> None:
    """VIX와 같이 초 단위 타임스탬프가 섞여 있어도 history에 남아야 합니다."""

    latest_path = tmp_path / "out" / "latest.csv"
    history_path = tmp_path / "out" / "history.csv"

    # KOSPI는 분 단위, VIX는 초 단위 타임스탬프를 사용해 섞여 있는 상황을 구성합니다.
    write_latest(
        latest_path,
        [
            {
                "ts_kst": "2024-02-06 15:30:00",
                "asset": "KOSPI",
                "key": "idx",
                "value": 2600.0,
                "unit": "pt",
                "window": "EOD",
                "source": "krx",
                "quality": "final",
                "notes": "",
            },
            {
                "ts_kst": "2024-02-06 15:30:22",
                "asset": "VIX",
                "key": "spot",
                "value": 19.05,
                "unit": "pt",
                "window": "1D",
                "source": "yfinance",
                "quality": "secondary",
                "notes": "",
            },
        ],
    )

    now = datetime(2024, 2, 6, 17, 1, tzinfo=KST)
    report = update_history.upsert_from_latest(latest_path, history_path, now=now)

    frame = pd.read_csv(history_path, dtype=str).fillna("")
    record = frame.iloc[0]

    assert float(record["vix"]) == 19.05
    assert report.field_status["vix"]["status"] == "ok_any"


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


def test_upsert_maps_k200_hv30(tmp_path: Path) -> None:
    """compute 단계가 생성하는 K200 hv30이 올바르게 history로 매핑되는지 확인합니다."""

    latest_path = tmp_path / "out" / "latest.csv"
    history_path = tmp_path / "out" / "history.csv"

    write_latest(
        latest_path,
        [
            {
                "ts_kst": "2024-02-10 15:30:00",
                "asset": "K200",
                "key": "hv30",
                "value": 0.33,
                "unit": "pct",
                "window": "EOD",
                "source": "compute",
                "quality": "final",
                "notes": "",
            }
        ],
    )

    now = datetime(2024, 2, 10, 17, 5, tzinfo=KST)
    report = update_history.upsert_from_latest(latest_path, history_path, now=now)

    frame = pd.read_csv(history_path, dtype=str).fillna("")
    record = frame.iloc[0]

    assert float(record["k200_hv30"]) == 0.33
    assert report.field_status["k200_hv30"]["status"] == "ok_eod"


def test_upsert_maps_tips10y(tmp_path: Path) -> None:
    """TIPS10Y 수집 후 history에 기록되는지 검증합니다."""

    latest_path = tmp_path / "out" / "latest.csv"
    history_path = tmp_path / "out" / "history.csv"

    write_latest(
        latest_path,
        [
            {
                "ts_kst": "2024-02-12 15:30:00",
                "asset": "TIPS10Y",
                "key": "yield",
                "value": 1.12,
                "unit": "pct",
                "window": "EOD",
                "source": "fred",
                "quality": "final",
                "notes": "",
            }
        ],
    )

    now = datetime(2024, 2, 12, 17, 1, tzinfo=KST)
    report = update_history.upsert_from_latest(latest_path, history_path, now=now)

    frame = pd.read_csv(history_path, dtype=str).fillna("")
    record = frame.iloc[0]

    assert float(record["tips10y"]) == 1.12
    assert report.field_status["tips10y"]["status"] == "ok_eod"


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


def test_upsert_blanks_only_krx_market_fields_on_closed_date(
    tmp_path: Path,
) -> None:
    latest_path = tmp_path / "out" / "latest.csv"
    history_path = tmp_path / "out" / "history.csv"
    closed_dates_path = tmp_path / "config" / "krx_closed_dates.csv"
    write_closed_dates(
        closed_dates_path,
        [("2026-07-17", "Constitution Day", "KRX")],
    )

    rows = [
        {
            "ts_kst": "2026-07-17 15:30:00",
            "asset": asset,
            "key": key,
            "value": value,
            "unit": "pt",
            "window": "EOD",
            "source": source,
            "quality": quality,
            "notes": "",
        }
        for asset, key, value, source, quality in [
            ("KOSPI", "idx", 6820.6, "yfinance", "final"),
            ("KOSDAQ", "idx", 791.84, "yfinance", "final"),
            ("KOSPI", "advance", 0, "pykrx", "final"),
            ("KOSPI", "decline", 0, "pykrx", "final"),
            ("KOSPI", "unchanged", 944, "pykrx", "final"),
            ("KOSDAQ", "advance", 0, "pykrx", "final"),
            ("KOSDAQ", "decline", 0, "pykrx", "final"),
            ("KOSDAQ", "unchanged", 1821, "pykrx", "final"),
            ("USD/KRW", "spot", 1387.5, "bok", "secondary"),
        ]
    ]
    write_latest(latest_path, rows)

    report = update_history.upsert_from_latest(
        latest_path,
        history_path,
        now=datetime(2026, 7, 17, 17, 5, tzinfo=KST),
        closed_dates_path=closed_dates_path,
    )

    frame = pd.read_csv(history_path, dtype=str).fillna("")
    record = frame.iloc[0]
    for column in update_history.MARKET_COLUMNS:
        assert record[column] == ""
        assert report.field_status[column]["status"] == "market_closed"
    assert float(record["usdkrw"]) == 1387.5
    assert "market_closed" in record["src_tag"].split("|")
    assert record["quality"] == "secondary"


def test_checked_in_history_sanitizes_all_closed_dates() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    history_path = repository_root / "out" / "history.csv"
    closed_dates_path = repository_root / "config" / "krx_closed_dates.csv"

    history = pd.read_csv(history_path, dtype=str).fillna("")
    closed_dates = set(
        pd.read_csv(closed_dates_path, dtype=str)["date"].astype(str)
    )
    matching = history[
        history["time_kst"].astype(str).str.slice(0, 10).isin(closed_dates)
    ]

    assert len(matching) == 12
    for _, record in matching.iterrows():
        assert all(record[column] == "" for column in update_history.MARKET_COLUMNS)
        tokens = [token for token in record["src_tag"].split("|") if token]
        assert tokens.count("market_closed") == 1
        assert any(
            record[column] != ""
            for column in [
                "usdkrw",
                "dxy",
                "ust2y",
                "ust10y",
                "kr3y",
                "kr10y",
                "tips10y",
                "wti",
                "brent",
                "gold",
                "copper",
                "btc",
                "k200_hv30",
                "vix",
            ]
        )


def test_upsert_repairs_existing_closed_rows_and_is_idempotent(
    tmp_path: Path,
) -> None:
    latest_path = tmp_path / "out" / "latest.csv"
    history_path = tmp_path / "out" / "history.csv"
    closed_dates_path = tmp_path / "config" / "krx_closed_dates.csv"
    write_closed_dates(
        closed_dates_path,
        [
            ("2026-07-17", "Constitution Day", "KRX"),
            ("2026-12-31", "Year-end market closure", "KRX"),
        ],
    )

    existing = {column: "" for column in update_history.HISTORY_COLUMNS}
    existing.update(
        {
            "time_kst": "2026-07-17 15:30:00",
            "kospi": "6820.6",
            "kosdaq": "791.84",
            "kospi_adv": "0",
            "kospi_dec": "0",
            "kospi_unch": "944",
            "kosdaq_adv": "0",
            "kosdaq_dec": "0",
            "kosdaq_unch": "1821",
            "usdkrw": "1387.5",
            "src_tag": "krx|bok",
            "quality": "secondary",
        }
    )
    history_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([existing], columns=update_history.HISTORY_COLUMNS).to_csv(
        history_path,
        index=False,
    )
    write_latest(
        latest_path,
        [
            {
                "ts_kst": "2026-07-20 15:30:00",
                "asset": "KOSPI",
                "key": "idx",
                "value": 6900,
                "unit": "pt",
                "window": "EOD",
                "source": "krx",
                "quality": "final",
                "notes": "",
            }
        ],
    )

    for _ in range(2):
        update_history.upsert_from_latest(
            latest_path,
            history_path,
            now=datetime(2026, 7, 20, 17, 5, tzinfo=KST),
            closed_dates_path=closed_dates_path,
        )

    frame = pd.read_csv(history_path, dtype=str).fillna("")
    closed_record = frame[frame["time_kst"].str.startswith("2026-07-17")].iloc[0]
    open_record = frame[frame["time_kst"].str.startswith("2026-07-20")].iloc[0]
    assert all(
        closed_record[column] == "" for column in update_history.MARKET_COLUMNS
    )
    assert float(closed_record["usdkrw"]) == 1387.5
    assert closed_record["src_tag"].split("|").count("market_closed") == 1
    assert float(open_record["kospi"]) == 6900
    assert "market_closed" not in open_record["src_tag"].split("|")


def test_upsert_fails_after_closed_date_calendar_coverage(
    tmp_path: Path,
) -> None:
    latest_path = tmp_path / "out" / "latest.csv"
    history_path = tmp_path / "out" / "history.csv"
    closed_dates_path = tmp_path / "config" / "krx_closed_dates.csv"
    write_closed_dates(
        closed_dates_path,
        [("2026-12-31", "Year-end market closure", "KRX")],
    )
    write_latest(
        latest_path,
        [
            {
                "ts_kst": "2027-01-04 15:30:00",
                "asset": "KOSPI",
                "key": "idx",
                "value": 7000,
                "unit": "pt",
                "window": "EOD",
                "source": "krx",
                "quality": "final",
                "notes": "",
            }
        ],
    )

    with pytest.raises(ValueError, match="calendar coverage expired"):
        update_history.upsert_from_latest(
            latest_path,
            history_path,
            now=datetime(2027, 1, 4, 17, 5, tzinfo=KST),
            closed_dates_path=closed_dates_path,
        )
