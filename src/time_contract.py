"""원천을 수정하지 않는 계산용 시간 계약.

일반 입력의 naive 시간은 기본 거부한다. 내부 raw의 ``ts_kst`` 열은
명시적으로 한국 현지시각을 뜻하므로, 해당 소비자만 KST 계약을 지정한다.
aware 시간은 UTC를 경유하여 실제 순간을 보존하며 한 타입으로 맞춘다.
source_date는 이 모듈에서 추정하거나 변경하지 않는다.
"""
from datetime import date, datetime, timezone
import re
import sys

import numpy as np
import pandas as pd


TIME_FAILURE = "TIMEZONE_NORMALIZATION_FAILED"
RAW_NAIVE_TIMEZONE = "Asia/Seoul"
_ASSETS = {"KOSPI", "KOSDAQ", "K200", "SPX", "NDX", "SOX", "ES", "NQ",
           "USD/KRW", "DXY", "UST2Y", "UST10Y", "TIPS10Y", "KR3Y", "KR10Y",
           "WTI", "Brent", "Gold", "Copper", "BTC"}
_REASONS = {"INPUT_MISSING", "INPUT_TYPE_UNSUPPORTED", "INVALID_TIMESTAMP",
            "NAIVE_TIMEZONE_UNDECLARED", "UNKNOWN_TIMEZONE_CONTRACT", "CONVERSION_FAILED"}
_TZ_KINDS = {"PYTZ", "ZONEINFO", "UTC", "FIXED_OFFSET", "NAIVE", "OTHER"}
_DTYPES = {"DATETIME_TZ", "DATETIME_NAIVE", "OBJECT", "STRING", "OTHER"}
_ISO = re.compile(r"\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d{1,9})?)?(?:Z|[+-]\d{2}:?\d{2})?)?")


def _version(text):
    return text if isinstance(text, str) and re.fullmatch(r"[0-9][A-Za-z0-9.+-]{0,30}", text) else "unknown"


def _dtype_kind(values):
    dtype = values.dtype
    if isinstance(dtype, pd.DatetimeTZDtype):
        return "DATETIME_TZ"
    if pd.api.types.is_datetime64_dtype(dtype):
        return "DATETIME_NAIVE"
    if pd.api.types.is_object_dtype(dtype):
        return "OBJECT"
    if pd.api.types.is_string_dtype(dtype):
        return "STRING"
    return "OTHER"


def _tz_kind(stamp):
    tz = stamp.tzinfo
    if tz is None:
        return "NAIVE"
    module = type(tz).__module__
    if module.startswith("pytz"):
        return "PYTZ"
    if module == "zoneinfo":
        return "ZONEINFO"
    if tz is timezone.utc:
        return "UTC"
    if isinstance(tz, timezone):
        return "FIXED_OFFSET"
    return "OTHER"


class TimeContractError(ValueError):
    """원문 문자열·시간대 repr·예외 메시지를 보유하지 않는 진단."""

    def __init__(self, *, reason, asset, row_count, dtype_kind, timezone_kinds, position=None):
        super().__init__(TIME_FAILURE)
        self._safe = {
            "reason": reason if reason in _REASONS else "CONVERSION_FAILED",
            "asset": asset if asset in _ASSETS else "OTHER",
            "field": "ts_kst", "row_count": int(row_count),
            "dtype_kind": dtype_kind if dtype_kind in _DTYPES else "OTHER",
            "timezone_kinds": sorted(set(timezone_kinds).intersection(_TZ_KINDS)),
            "row_position": int(position) if position is not None else None,
            "versions": {"python": ".".join(str(n) for n in sys.version_info[:3]),
                         "pandas": _version(pd.__version__), "numpy": _version(np.__version__)},
        }

    def safe_context(self):
        # Fresh copies prevent callers from mutating a later serialized receipt.
        return {**self._safe, "timezone_kinds": list(self._safe["timezone_kinds"]),
                "versions": dict(self._safe["versions"])}


def normalize_kst_times(values, *, asset="OTHER", naive_timezone=None):
    """Normalize a Series copy without discarding missing/invalid timestamps.

    The explicit ``Asia/Seoul`` contract is the only supported naive policy.
    Do not use utc=True directly on naive values: that changes their instant by
    nine hours. Offset-aware strings must be ISO; timezone abbreviations and
    numeric epoch guesses are rejected before pandas can infer or drop a zone.
    """
    if not isinstance(values, pd.Series):
        values = pd.Series(values, dtype=object)
    dtype_kind, kinds, normalized = _dtype_kind(values), set(), []

    def reject(reason, position=None):
        return TimeContractError(reason=reason, asset=asset, row_count=len(values),
                                 dtype_kind=dtype_kind, timezone_kinds=kinds, position=position)

    if naive_timezone not in (None, RAW_NAIVE_TIMEZONE):
        raise reject("UNKNOWN_TIMEZONE_CONTRACT")
    for position, value in enumerate(values):
        if value is None or value is pd.NA or value is pd.NaT:
            raise reject("INPUT_MISSING", position)
        if isinstance(value, (float, np.floating)) and np.isnan(value):
            raise reject("INPUT_MISSING", position)
        if not isinstance(value, (str, date, datetime, pd.Timestamp, np.datetime64)):
            raise reject("INPUT_TYPE_UNSUPPORTED", position)
        if isinstance(value, str) and not _ISO.fullmatch(value):
            raise reject("INVALID_TIMESTAMP", position)
        try:
            stamp = pd.Timestamp(value)
        except Exception:
            raise reject("INVALID_TIMESTAMP", position) from None
        if pd.isna(stamp):
            raise reject("INPUT_MISSING", position)
        kinds.add(_tz_kind(stamp))
        if stamp.tzinfo is None and naive_timezone is None:
            raise reject("NAIVE_TIMEZONE_UNDECLARED", position)
        try:
            if stamp.tzinfo is None:
                stamp = stamp.tz_localize(RAW_NAIVE_TIMEZONE, ambiguous="raise", nonexistent="raise")
            elif stamp.utcoffset() is None:
                raise reject("INVALID_TIMESTAMP", position)
            normalized.append(stamp.tz_convert("UTC"))
        except TimeContractError:
            raise
        except Exception:
            raise reject("CONVERSION_FAILED", position) from None
    try:
        # All members are now UTC-aware even with pandas 2.3.x; a provider's
        # pytz / ZoneInfo / fixed-offset class no longer changes dtype inference.
        index = pd.DatetimeIndex(normalized, tz="UTC").tz_convert(RAW_NAIVE_TIMEZONE)
        return pd.Series(index, index=values.index.copy(), name=values.name)
    except Exception:
        raise reject("CONVERSION_FAILED") from None


