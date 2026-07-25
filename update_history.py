"""History upsert helper for the 17:00 KST batch.

이 모듈은 17시 배치에서 latest.csv 안의 EOD(End of Day) 수치를 골라
history.csv에 1행으로 업서트(upsert)하는 작업을 담당합니다. 초심자도 쉽게
이해할 수 있도록 각 함수마다 단계별 주석과 디버깅 도우미를 함께 제공합니다.
"""

from __future__ import annotations

import csv
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
    "vix",
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
    # 원자재·암호화폐는 compute 단계에서 key="spot"으로 기록됩니다.
    # 기존 코드가 price/curve_M1을 찾도록 되어 있어 값이 누락되었으므로
    # history 매핑을 실제 latest.csv 키에 맞게 수정합니다.
    ("WTI", "spot"): "wti",
    ("Brent", "spot"): "brent",
    ("Gold", "spot"): "gold",
    ("Copper", "spot"): "copper",
    ("BTC", "spot"): "btc",
    # compute.py에서 K200으로 hv30을 생성하므로 매핑도 동일하게 맞춘다.
    ("K200", "hv30"): "k200_hv30",
    ("VIX", "spot"): "vix",
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
    # VIX: history에서는 숫자인지만 확인하고, 범위 초과 여부는 디버깅 용도로만 남깁니다.
    # 실거래/지표 해석 단계에서 추가 필터를 적용할 수 있도록 값을 최대한 살려둡니다.
    "vix": (0.0, None),
    # 나머지 컬럼은 별도 범위 제한이 없습니다.
}

# history.csv에 기록할 기준 시각(매일 15:30:00)입니다.
EOD_TIME_STR = "15:30:00"

# KRX 휴장일에는 거래소 시장 수치만 비우고, 해외·거시지표는 보존합니다.
# 기존 history.csv 스키마를 바꾸지 않기 위해 src_tag에 market_closed 토큰을 남깁니다.
MARKET_COLUMNS: List[str] = [
    "kospi",
    "kosdaq",
    "kospi_adv",
    "kospi_dec",
    "kospi_unch",
    "kosdaq_adv",
    "kosdaq_dec",
    "kosdaq_unch",
]
DEFAULT_CLOSED_DATES_PATH = (
    Path(__file__).resolve().parent / "config" / "krx_closed_dates.csv"
)


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
    # latest.csv의 ts_kst는 "2024-02-06 15:30"처럼 분 단위 형식과
    # "2024-02-06 15:30:22"처럼 초 단위 형식이 섞여 있습니다.
    # pandas가 하나의 포맷만 추론하면 초 단위 값이 NaT로 변해 기록이 누락될 수
    # 있으므로, format="mixed"로 두 형식을 모두 받아들이도록 안전 가드를 둡니다.
    frame["ts_kst"] = pd.to_datetime(frame["ts_kst"], errors="coerce", format="mixed")
    frame["window"] = frame["window"].fillna("")
    frame["date_kst"] = frame["ts_kst"].dt.date
    return frame


def _load_closed_dates(
    closed_dates_path: Path,
    debug: DebugReport,
) -> Dict[date, Dict[str, str]]:
    """검증된 KRX 휴장일 CSV를 읽습니다.

    달력이 없거나 손상된 상태에서 거래일을 추정하면 history.csv가 다시
    오염될 수 있으므로, 조용히 폴백하지 않고 배치를 실패시킵니다.
    """

    if not closed_dates_path.exists():
        raise FileNotFoundError(
            f"KRX closed-date calendar not found: {closed_dates_path}"
        )

    closed_dates: Dict[date, Dict[str, str]] = {}
    with closed_dates_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required_columns = {"date", "reason", "source"}
        if reader.fieldnames is None or not required_columns.issubset(
            set(reader.fieldnames)
        ):
            raise ValueError(
                "KRX closed-date calendar must contain date, reason, source columns"
            )

        for row_number, record in enumerate(reader, start=2):
            raw_date = str(record.get("date", "")).strip()
            reason = str(record.get("reason", "")).strip()
            source = str(record.get("source", "")).strip()
            if not raw_date or not reason or not source:
                raise ValueError(
                    f"Invalid KRX closed-date calendar row: {row_number}"
                )
            try:
                closed_date = date.fromisoformat(raw_date)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid KRX closed date at row {row_number}: {raw_date}"
                ) from exc
            if closed_date in closed_dates:
                raise ValueError(
                    f"Duplicate KRX closed date at row {row_number}: {raw_date}"
                )
            closed_dates[closed_date] = {
                "reason": reason,
                "source": source,
            }

    if not closed_dates:
        raise ValueError("KRX closed-date calendar is empty")

    debug.log(
        "KRX 휴장일 달력 로드",
        path=str(closed_dates_path),
        dates=len(closed_dates),
    )
    return closed_dates


def _append_src_tag(row: Dict[str, str], tag: str) -> None:
    """기존 출처 순서를 유지하면서 src_tag 토큰을 중복 없이 추가합니다."""

    source_tags = [
        item.strip()
        for item in str(row.get("src_tag", "")).split("|")
        if item.strip()
    ]
    if tag not in source_tags:
        source_tags.append(tag)
    row["src_tag"] = "|".join(source_tags)


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

        fv = float(numeric_value)
        if not _validate_value(column, fv):
            # VIX는 범위 제한을 완화했지만, 이상 수치는 디버깅 힌트로 남겨 추후 점검할 수 있게 합니다.
            if column == "vix":
                debug.mark_field(
                    column,
                    "range_violation",
                    raw=value,
                    hint="vix_validator_rejected; 확인 필요",
                )
            else:
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


def _apply_market_closed_policy(
    row: Dict[str, str],
    target_date: date,
    closed_dates: Dict[date, Dict[str, str]],
    debug: DebugReport,
) -> Dict[str, str]:
    """휴장일의 KRX 시장 필드만 비우고 market_closed 토큰을 기록합니다."""

    closed_date_info = closed_dates.get(target_date)
    if closed_date_info is None:
        return row

    for column in MARKET_COLUMNS:
        row[column] = ""
        debug.mark_field(
            column,
            "market_closed",
            reason=closed_date_info["reason"],
            calendar_source=closed_date_info["source"],
        )

    _append_src_tag(row, "market_closed")
    debug.log(
        "KRX 휴장일 시장 필드 공란 처리",
        target_date=str(target_date),
        reason=closed_date_info["reason"],
        calendar_source=closed_date_info["source"],
    )
    return row


def _normalize_closed_history_rows(
    history_frame: pd.DataFrame,
    closed_dates: Dict[date, Dict[str, str]],
    debug: DebugReport,
) -> pd.DataFrame:
    """기존 history 행도 휴장일 정책에 맞춰 결정론적으로 정규화합니다."""

    normalized = history_frame.copy()
    matched_rows = 0
    changed_rows = 0

    for row_index in normalized.index:
        timestamp_text = str(normalized.at[row_index, "time_kst"]).strip()
        try:
            row_date = date.fromisoformat(timestamp_text[:10])
        except ValueError:
            # 잘못된 타임스탬프는 이 함수에서 보정하지 않습니다. 기존 검증기가
            # 원본 오류를 그대로 드러낼 수 있도록 보존합니다.
            continue
        if row_date not in closed_dates:
            continue

        matched_rows += 1
        row_changed = False
        for column in MARKET_COLUMNS:
            if str(normalized.at[row_index, column]).strip():
                row_changed = True
            normalized.at[row_index, column] = ""

        source_tags = [
            tag.strip()
            for tag in str(normalized.at[row_index, "src_tag"]).split("|")
            if tag.strip()
        ]
        if "market_closed" not in source_tags:
            source_tags.append("market_closed")
            row_changed = True
        deduplicated_tags = list(dict.fromkeys(source_tags))
        if deduplicated_tags != source_tags:
            row_changed = True
        normalized.at[row_index, "src_tag"] = "|".join(deduplicated_tags)

        if row_changed:
            changed_rows += 1

    debug.log(
        "기존 KRX 휴장일 행 정규화",
        matched_rows=matched_rows,
        changed_rows=changed_rows,
    )
    return normalized


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
    closed_dates_path: str | Path | None = None,
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
    closed_dates_path : str | Path, optional
        KRX 휴장일 CSV 경로. None이면 저장소의 config/krx_closed_dates.csv를
        사용합니다.

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
    calendar_path = (
        Path(closed_dates_path)
        if closed_dates_path is not None
        else DEFAULT_CLOSED_DATES_PATH
    )
    closed_dates = _load_closed_dates(calendar_path, debug)

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

    calendar_coverage_end = max(closed_dates)
    if target_date > calendar_coverage_end:
        raise ValueError(
            "KRX closed-date calendar coverage expired: "
            f"{calendar_coverage_end.isoformat()}"
        )

    row = _apply_market_closed_policy(row, target_date, closed_dates, debug)

    history_frame = _load_history(history_path, debug)
    history_frame = _normalize_closed_history_rows(
        history_frame,
        closed_dates,
        debug,
    )
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
    parser.add_argument("--closed-dates", default=None)
    args = parser.parse_args()

    report = upsert_from_latest(
        args.latest,
        args.history,
        debug_dir=args.debug_dir,
        closed_dates_path=args.closed_dates,
    )
    print(json.dumps({"steps": report.steps, "field_status": report.field_status}, ensure_ascii=False))


__all__ = [
    "upsert_from_latest",
    "HISTORY_COLUMNS",
    "LATEST_TO_HISTORY",
    "MARKET_COLUMNS",
    "DebugReport",
]
