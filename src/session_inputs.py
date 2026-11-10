"""원천 날짜를 보존한 완료 거래일 history 전용 입력 선택.

최신 관측 snapshot의 원문을 수정하지 않는다. 이 함수는 완전성이나
시장 마감을 승인하지 않으며, 목표일 핵심 지수 검증은 update_history가
계속 담당한다. 과거 수익률/변동성도 같은 cutoff를 사용하도록 마지막
행만 선택하지 않고 각 field의 전체 과거 시계열을 남긴다.
"""
from datetime import date, datetime
from typing import Mapping

import pandas as pd

from .source_dates import TOKEN, note_date, quote_day


def _row_source_date(row):
    """latest_source_date와 같은 메타데이터 우선순위로 한 행을 판정한다."""
    # UNKNOWN, 빈 문자열, 숫자 등 명시된 비정상 날짜는 None이다.
    # 이를 관측 timestamp로 대체하면 원천 최신성을 합성하게 된다.
    if "source_date" in row.index and pd.notna(row.get("source_date")):
        return quote_day(row.get("source_date"))
    if TOKEN in str(row.get("notes", "")):
        return note_date(row["notes"])
    return quote_day(row.get("ts_kst"))


def history_raw_as_of(raw_frames: Mapping[str, pd.DataFrame | None], target_date: date):
    """Return (independent raw copy, safe count diagnostic) for one session.

    Explicit source dates, notes and numeric values are never rewritten.
    An absent source_date cell from concatenated frames follows the existing
    notes/timestamp fallback, but an explicit UNKNOWN/malformed date does not.
    Diagnostics contain counts only, not arbitrary source notes/URLs/content.
    """
    if not isinstance(target_date, date) or isinstance(target_date, datetime):
        raise ValueError("EXACT_TARGET_DATE_REQUIRED")

    result = {}
    diagnostic = {
        "schema_version": 1,
        "selection": "history_source_date_cutoff",
        "target_date": target_date.isoformat(),
        "assets": {},
        "input_rows": 0,
        "kept_rows": 0,
        "excluded_after_target_rows": 0,
        "excluded_unknown_source_date_rows": 0,
    }
    required = {"field", "value", "ts_kst"}
    for asset, frame in raw_frames.items():
        counts = {
            "input_rows": 0 if frame is None else len(frame),
            "kept_rows": 0,
            "excluded_after_target_rows": 0,
            "excluded_unknown_source_date_rows": 0,
        }
        if frame is None:
            result[asset] = None
        elif not required.issubset(frame.columns):
            # The canonical date selector cannot verify this frame either.
            result[asset] = frame.iloc[0:0].copy(deep=True)
            counts["excluded_unknown_source_date_rows"] = len(frame)
        else:
            keep = []
            for _, row in frame.iterrows():
                day = _row_source_date(row)
                if day is None:
                    keep.append(False)
                    counts["excluded_unknown_source_date_rows"] += 1
                elif day > target_date:
                    keep.append(False)
                    counts["excluded_after_target_rows"] += 1
                else:
                    keep.append(True)
                    counts["kept_rows"] += 1
            # Positional boolean selection preserves duplicate indices, order,
            # columns and dtypes without inserting any synthetic date column.
            result[asset] = frame.iloc[keep].copy(deep=True)
        diagnostic["assets"][asset] = counts
        for name, count in counts.items():
            diagnostic[name] += count
    return result, diagnostic
