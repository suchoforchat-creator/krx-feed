"""History upsert helper for the 17:00 KST batch.

이 모듈은 17시 배치에서 latest.csv 안의 EOD(End of Day) 수치를 골라
history.csv에 1행으로 업서트(upsert)하는 작업을 담당합니다. 초심자도 쉽게
이해할 수 있도록 각 함수마다 단계별 주석과 디버깅 도우미를 함께 제공합니다.
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# 상수 정의 영역
# ---------------------------------------------------------------------------

# 한국 표준시(KST)는 UTC+9 입니다. datetime.now(KST)로 현재 KST 시간을 구할 수 있습니다.
KST = timezone(timedelta(hours=9))

# history.csv에 기록할 컬럼 순서입니다. (요구사항 그대로 유지)
HISTORY_COLUMNS: List[str] = [
    "time_kst",
    "kospi",
    "kosdaq",
    "kospi_adv",
    "kospi_dec",
    "kospi_unch",
    "kosdaq_adv",
    "kosdaq_dec",
    "kosdaq_unch",
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
    "src_tag",
    "quality",
]

# latest.csv의 (asset, key) 조합을 history.csv의 열 이름으로 바꾸는 매핑입니다.
LATEST_TO_HISTORY: Dict[Tuple[str, str], str] = {
    ("KOSPI", "idx"): "kospi",
    ("KOSDAQ", "idx"): "kosdaq",
    ("KOSPI", "advance"): "kospi_adv",
    ("KOSPI", "decline"): "kospi_dec",
    ("KOSPI", "unchanged"): "kospi_unch",
    ("KOSDAQ", "advance"): "kosdaq_adv",
    ("KOSDAQ", "decline"): "kosdaq_dec",
    ("KOSDAQ", "unchanged"): "kosdaq_unch",
    ("USD/KRW", "spot"): "usdkrw",
    ("DXY", "idx"): "dxy",
    ("UST2Y", "yield"): "ust2y",
    ("UST10Y", "yield"): "ust10y",
    ("KR3Y", "yield"): "kr3y",
    ("KR10Y", "yield"): "kr10y",
    ("TIPS10Y", "yield"): "tips10y",
    ("WTI", "price"): "wti",
    ("Brent", "curve_M1"): "brent",
    ("Gold", "price"): "gold",
    ("Copper", "price"): "copper",
    ("BTC", "price"): "btc",
    ("KOSPI200", "hv30"): "k200_hv30",
}

# 값 검증 범위입니다. 범위를 벗어나면 기록하지 않고 공란으로 둡니다.
VALUE_VALIDATORS: Dict[str, Tuple[Optional[float], Optional[float]]] = {
    "kospi": (0, None),
    "kosdaq": (0, None),
    "ust2y": (0, 10),
    "ust10y": (0, 10),
    "kr3y": (0, 10),
    "kr10y": (0, 10),
    "tips10y": (0, 10),
    "dxy": (70, 130),
    # 나머지 컬럼은 별도 범위 제한이 없습니다.
}

# history.csv에 기록할 기준 시각(매일 15:30:00)입니다.
EOD_TIME_STR = "15:30:00"


# ---------------------------------------------------------------------------
# 디버깅 도우미 클래스
# ---------------------------------------------------------------------------

@dataclass
class DebugReport:
    """업서트 과정의 상태를 저장해 사용자가 디버깅할 수 있도록 돕는 클래스."""

    steps: List[Dict[str, str]] = field(default_factory=list)
    field_status: Dict[str, Dict[str, str]] = field(default_factory=dict)

    def log(self, message: str, **extra: object) -> None:
        """단계별 메시지를 남깁니다."""

        payload = {"message": message}
        for key, value in extra.items():
            payload[key] = "" if value is None else str(value)
        self.steps.append(payload)

    def mark_field(self, field: str, status: str, **extra: object) -> None:
        """특정 컬럼에 대한 처리 결과를 기록합니다."""

        info = {"status": status}
        for key, value in extra.items():
            info[key] = "" if value is None else str(value)
        self.field_status[field] = info

    def dump(self, path: Path) -> None:
        """디버깅 정보를 JSON 파일로 저장합니다."""

        path.parent.mkdir(parents=True, exist_ok=True)
        data = {"steps": self.steps, "fields": self.field_status}
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# 내부 유틸리티 함수
# ---------------------------------------------------------------------------

def _load_latest(latest_path: Path, debug: DebugReport) -> pd.DataFrame:
    """latest.csv를 안전하게 로드하고 기본 컬럼을 확인합니다."""

    if not latest_path.exists():
        debug.log("latest.csv 파일이 존재하지 않아 업서트를 중단합니다", path=str(latest_path))
        return pd.DataFrame()

    frame = pd.read_csv(latest_path)
    debug.log("latest.csv 로드 완료", rows=len(frame))
    if "ts_kst" not in frame.columns:
        debug.log("ts_kst 컬럼이 없어 업서트를 중단합니다")
        return pd.DataFrame()

    for required in ["asset", "key", "value", "window", "source", "quality"]:
        if required not in frame.columns:
            frame[required] = ""

    # 날짜 비교를 위해 KST 기준 날짜 컬럼을 추가합니다.
    frame["ts_kst"] = pd.to_datetime(frame["ts_kst"], errors="coerce")
    frame["window"] = frame["window"].fillna("")
    frame["date_kst"] = frame["ts_kst"].dt.date
    return frame


def _choose_target_date(frame: pd.DataFrame, debug: DebugReport) -> Optional[date]:
    """EOD가 있으면 해당 날짜를, 없으면 ANY window 중 가장 최근 날짜를 선택합니다."""

    if frame.empty:
        return None

    working = frame.dropna(subset=["ts_kst"]).copy()
    if working.empty:
        debug.log("유효한 타임스탬프가 없어 target_date를 정하지 못했습니다")
        return None

    eod_rows = working[working["window"] == "EOD"]
    if not eod_rows.empty:
        latest_date = eod_rows["date_kst"].max()
        debug.log("EOD 기준 target_date 선택", target_date=latest_date)
        return latest_date

    fallback = working["date_kst"].max()
    debug.log("EOD 없음 → ANY window 폴백 target_date", target_date=fallback)
    return fallback


def _validate_value(column: str, value: float) -> bool:
    """값이 요구 범위 안에 있는지 확인합니다."""

    minimum, maximum = VALUE_VALIDATORS.get(column, (None, None))
    if minimum is not None and not (value > minimum or np.isclose(value, minimum)):
        return False
    if maximum is not None and not (value < maximum or np.isclose(value, maximum)):
        return False
    return True


def _select_latest_record(
    frame: pd.DataFrame,
    asset: str,
    key: str,
    target_date: date,
    debug: DebugReport,
) -> Tuple[Optional[pd.Series], str]:
    """특정 자산/키의 target_date 레코드 중 마지막 값을 선택하고 window를 함께 반환합니다."""

    column = LATEST_TO_HISTORY.get((asset, key), f"{asset}:{key}")
    subset = frame[
        (frame["asset"] == asset)
        & (frame["key"] == key)
        & (frame["date_kst"] == target_date)
    ]

    if subset.empty:
        debug.mark_field(column, "missing", reason="no_record_for_date")
        return None, ""

    eod = subset[subset["window"] == "EOD"]
    candidates = eod if not eod.empty else subset
    if eod.empty:
        debug.log(
            "EOD 없음 → ANY window 사용",
            asset=asset,
            key=key,
            target_date=str(target_date),
            candidates=len(candidates),
        )

    candidates = candidates.sort_values("ts_kst")
    chosen = candidates.iloc[-1]
    window_used = str(chosen.get("window", ""))
    debug.log(
        "레코드 선택",
        asset=asset,
        key=key,
        ts=str(chosen.get("ts_kst")),
        value=str(chosen.get("value")),
        window=window_used,
    )
    return chosen, window_used


def _build_history_row(
    frame: pd.DataFrame,
    target_date: date,
    debug: DebugReport,
) -> Dict[str, str]:
    """target_date에 해당하는 history.csv 1행을 구성합니다."""

    row: Dict[str, str] = {col: "" for col in HISTORY_COLUMNS}
    row["time_kst"] = f"{target_date} {EOD_TIME_STR}"

    sources: List[str] = []
    qualities: List[str] = []

    for (asset, key), column in LATEST_TO_HISTORY.items():
        record, window_used = _select_latest_record(frame, asset, key, target_date, debug)
        if record is None:
            continue

        value = record.get("value")
        numeric_value = pd.to_numeric(value, errors="coerce")
        if pd.isna(numeric_value):
            debug.mark_field(column, "non_numeric", raw=value)
            continue

        if not _validate_value(column, float(numeric_value)):
            debug.mark_field(column, "range_violation", raw=value)
            continue

        row[column] = str(value)

        source_text = str(record.get("source", "")).strip().lower()
        if source_text and source_text != "nan":
            sources.append(source_text)

        quality_text = str(record.get("quality", "")).strip().lower()
        if quality_text and quality_text != "nan":
            qualities.append(quality_text)
        status = "ok_eod" if window_used == "EOD" else "ok_any"
        debug.mark_field(
            column,
            status,
            raw=value,
            source=record.get("source"),
            window=window_used,
        )

    if sources:
        tags = sorted({src.split("|")[0] for src in sources if src})
        row["src_tag"] = "|".join(tags)
    else:
        row["src_tag"] = ""

    if qualities and all(q == "final" for q in qualities if q):
        row["quality"] = "final"
    elif qualities:
        row["quality"] = "secondary"
    else:
        row["quality"] = ""

    debug.log("row 구성 완료", src_tag=row["src_tag"], quality=row["quality"])
    return row


def _build_empty_history_row(target_date: date, debug: DebugReport) -> Dict[str, str]:
    """latest.csv가 비어 있을 때 사용할 빈 history 행을 만듭니다."""

    row: Dict[str, str] = {column: "" for column in HISTORY_COLUMNS}
    row["time_kst"] = f"{target_date} {EOD_TIME_STR}"

    # 초심자가 디버깅할 수 있도록 각 필드가 왜 비었는지 상태를 기록합니다.
    for column in HISTORY_COLUMNS:
        if column == "time_kst":
            continue
        debug.mark_field(column, "missing_latest", reason="empty_latest_frame")

    debug.log("latest 비어있음 → 빈 history 행 생성", target_date=str(target_date))
    return row


def _load_history(history_path: Path, debug: DebugReport) -> pd.DataFrame:
    """기존 history.csv를 로드하고 없으면 빈 DataFrame을 생성합니다."""

    if history_path.exists():
        frame = pd.read_csv(history_path, dtype=str).fillna("")
        debug.log("history.csv 로드", rows=len(frame))
    else:
        frame = pd.DataFrame(columns=HISTORY_COLUMNS)
        debug.log("history.csv가 없어 새 파일을 만듭니다")

    for column in HISTORY_COLUMNS:
        if column not in frame.columns:
            frame[column] = ""

    return frame[HISTORY_COLUMNS]


def _atomic_write(frame: pd.DataFrame, path: Path) -> None:
    """임시 파일에 먼저 쓰고 rename으로 교체하여 원자성을 확보합니다."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    frame.to_csv(tmp_path, index=False)
    shutil.move(tmp_path, path)


def upsert_from_latest(
    latest_path: str | Path,
    history_path: str | Path,
    *,
    now: Optional[datetime] = None,
    debug_dir: str | Path | None = None,
) -> DebugReport:
    """latest.csv를 읽어 history.csv에 1행을 업서트합니다.

    Parameters
    ----------
    latest_path : str | Path
        최신 지표가 담긴 CSV 경로 (latest.csv)
    history_path : str | Path
        업서트할 history.csv 경로
    now : datetime, optional
        현재 KST 시각(테스트 편의를 위한 주입). None이면 시스템 시간을 사용합니다.
    debug_dir : str | Path, optional
        디버깅 JSON을 저장할 디렉터리. None이면 파일을 저장하지 않습니다.

    Returns
    -------
    DebugReport
        전체 처리 과정을 담은 디버그 리포트
    """

    debug = DebugReport()
    latest_path = Path(latest_path)
    history_path = Path(history_path)
    now = now.astimezone(KST) if now else datetime.now(KST)
    debug.log("시간 가드 비활성화", now_kst=now.isoformat())

    latest_frame = _load_latest(latest_path, debug)

    debug_filename: Optional[str] = None
    if latest_frame.empty:
        # latest.csv 자체가 비어 있으면 실행 시각(now) 기반으로 target_date를 선택합니다.
        target_date = now.date()
        debug.log(
            "latest 데이터 없음 → 실행 시각 날짜 사용",
            target_date=str(target_date),
        )
        row = _build_empty_history_row(target_date, debug)
        debug_filename = f"history_upsert_empty_{target_date}.json"
    else:
        target_date = _choose_target_date(latest_frame, debug)
        if target_date is None:
            debug.log("target_date를 찾지 못해 업서트를 종료합니다")
            if debug_dir:
                debug.dump(Path(debug_dir) / "history_upsert_no_target.json")
            return debug
        row = _build_history_row(latest_frame, target_date, debug)
        debug_filename = f"history_upsert_{target_date}.json"

    history_frame = _load_history(history_path, debug)
    mask = history_frame["time_kst"].astype(str) == row["time_kst"]
    if mask.any():
        debug.log("기존 동일 날짜 행을 덮어씁니다", time_kst=row["time_kst"])
        history_frame = history_frame.loc[~mask].copy()
    else:
        debug.log("새 행을 추가합니다", time_kst=row["time_kst"])

    history_frame = pd.concat([history_frame, pd.DataFrame([row])], ignore_index=True)
    history_frame = history_frame[HISTORY_COLUMNS].fillna("")
    history_frame = history_frame.sort_values("time_kst").reset_index(drop=True)

    _atomic_write(history_frame, history_path)
    debug.log("history.csv 업서트 완료", path=str(history_path))

    if debug_dir and debug_filename:
        debug.dump(Path(debug_dir) / debug_filename)

    return debug


if __name__ == "__main__":
    # CLI로 직접 실행할 때 사용할 수 있는 편의 엔트리포인트입니다.
    import argparse

    parser = argparse.ArgumentParser(description="latest.csv → history.csv 업서트 도우미")
    parser.add_argument("--latest", default="out/latest.csv")
    parser.add_argument("--history", default="out/history.csv")
    parser.add_argument("--debug-dir", default=None)
    args = parser.parse_args()

    report = upsert_from_latest(args.latest, args.history, debug_dir=args.debug_dir)
    print(json.dumps({"steps": report.steps, "field_status": report.field_status}, ensure_ascii=False))


__all__ = ["upsert_from_latest", "HISTORY_COLUMNS", "LATEST_TO_HISTORY", "DebugReport"]

