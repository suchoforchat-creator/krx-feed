"""완료장 핵심 지수의 누락된 날짜만 검증된 공개 일봉으로 보완한다.

기존 숫자·원천일은 고치지 않는다. HTTP 성공은 목표일 종가 확보가 아니며,
공급 실패를 원문 날짜 변경이나 추가 재시도로 감추지 않는다.
"""
from datetime import date, datetime, time
import hashlib
import math
import re
import time as clock
import xml.etree.ElementTree as ET

import pandas as pd
import requests

from ..session_dates import KST, completed_session, is_session
from ..source_dates import TOKEN, bind_note, note_date, quote_day


URL = "https://fchart.stock.naver.com/sise.nhn"
NAMES = {"KOSPI": "코스피", "KOSDAQ": "코스닥"}
MAX_BYTES = 2 * 1024 * 1024
MAX_ROWS = 120
TIMEOUT = 12


class SourceRejected(ValueError):
    """고정된 비밀 없는 진단 코드만 가진 외부 원천 오류."""


def _source_day(row):
    if "source_date" in row.index and pd.notna(row.get("source_date")):
        return quote_day(row.get("source_date"))
    if TOKEN in str(row.get("notes", "")):
        return note_date(row["notes"])
    return quote_day(row.get("ts_kst"))


def _number(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def _supplier_counts(frame):
    """Never copy an arbitrary provider string into diagnostics."""
    counts = {}
    for value in frame.get("source", pd.Series(dtype=str)):
        if value in {"YahooFinance(^KS11)", "YahooFinance(^KQ11)"}:
            label = "YAHOO_FINANCE"
        elif value == "KIS":
            label = "KIS"
        else:
            label = "OTHER_OR_UNKNOWN"
        counts[label] = counts.get(label, 0) + 1
    return counts


def _primary_state(frame, asset, target, observed):
    if frame.empty:
        return False, None
    required = {"ts_kst", "field", "value", "unit", "asset"}
    if not required.issubset(frame.columns):
        raise ValueError("CORE_INDEX_PRIMARY_INVALID")
    closes = frame.loc[frame["field"] == "close"]
    days = [_source_day(row) for _, row in closes.iterrows()]
    if any(day is not None and day > observed.date() for day in days):
        raise ValueError("CORE_INDEX_PRIMARY_INVALID")
    selected = [row for (_, row), day in zip(closes.iterrows(), days) if day == target]
    if selected:
        values = []
        for row in selected:
            value = _number(row["value"])
            if value is None or row["unit"] != "pt" or row["asset"] != asset:
                raise ValueError("CORE_INDEX_PRIMARY_INVALID")
            values.append(value)
        if len(set(values)) != 1:
            raise ValueError("CORE_INDEX_PRIMARY_INVALID")
    known = [day for day in days if day is not None]
    return bool(selected), max(known).isoformat() if known else None


def _download(asset):
    response = requests.get(
        URL, params={"symbol": asset, "timeframe": "day", "count": MAX_ROWS, "requestType": "0"},
        timeout=TIMEOUT, allow_redirects=False, stream=True,
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/xml,text/xml"})
    try:
        if response.status_code in (403, 429):
            raise SourceRejected("ACCESS_RESTRICTED")
        if response.status_code != 200:
            raise SourceRejected("HTTP_NOT_OK")
        declared = response.headers.get("Content-Length")
        if declared is not None:
            if not str(declared).isdigit() or int(declared) > MAX_BYTES:
                raise SourceRejected("RESPONSE_SIZE_INVALID")
        chunks, size = [], 0
        deadline = clock.monotonic() + TIMEOUT
        for chunk in response.iter_content(chunk_size=65536):
            if clock.monotonic() > deadline:
                raise SourceRejected("BODY_TIMEOUT")
            size += len(chunk)
            if size > MAX_BYTES:
                raise SourceRejected("RESPONSE_SIZE_INVALID")
            chunks.append(chunk)
        if not size:
            raise SourceRejected("EMPTY_RESPONSE")
        return b"".join(chunks)
    finally:
        response.close()


def _parse(payload, asset, observed):
    # No DTD/entity declarations, UTF-16 ambiguity, or unbounded XML input.
    if len(payload) > MAX_BYTES or b"\x00" in payload:
        raise SourceRejected("XML_INVALID")
    # Expat does not accept EUC-KR bytes directly. The official endpoint uses
    # that declaration, so decode only explicitly supported encodings first.
    declaration = re.match(br"^(?:\xef\xbb\xbf)?\s*<\?xml\s+([^?]{0,300})\?>", payload)
    declared = re.search(br"encoding\s*=\s*['\"]([^'\"]+)['\"]", declaration[1], re.I) if declaration else None
    encoding = declared[1].upper() if declared else b"UTF-8"
    codecs = {b"UTF-8": "utf-8-sig", b"UTF8": "utf-8-sig", b"EUC-KR": "euc-kr"}
    if encoding not in codecs:
        raise SourceRejected("XML_ENCODING_UNSUPPORTED")
    try:
        text = payload.decode(codecs[encoding], errors="strict")
    except UnicodeError:
        raise SourceRejected("XML_ENCODING_INVALID") from None
    upper = text.upper()
    if "<!DOCTYPE" in upper or "<!ENTITY" in upper:
        raise SourceRejected("XML_DECLARATION_REJECTED")
    try:
        root = ET.fromstring(text)
    except (ET.ParseError, ValueError, LookupError):
        raise SourceRejected("XML_INVALID") from None
    if root.tag != "protocol" or len(root) != 1 or root[0].tag != "chartdata":
        raise SourceRejected("XML_SCHEMA_INVALID")
    chart = root[0]
    if (chart.get("symbol") != asset or chart.get("name") != NAMES[asset]
            or chart.get("timeframe") != "day" or chart.get("precision") != "2"):
        raise SourceRejected("INDEX_DEFINITION_MISMATCH")
    declared_count = chart.get("count", "")
    if not re.fullmatch(r"[0-9]+", declared_count):
        raise SourceRejected("ROW_COUNT_INVALID")
    if not 1 <= len(chart) <= MAX_ROWS or int(declared_count) != len(chart):
        raise SourceRejected("ROW_COUNT_INVALID")
    records, seen = [], {}
    for item in chart:
        if item.tag != "item" or len(item):
            raise SourceRejected("XML_SCHEMA_INVALID")
        fields = item.get("data", "").split("|")
        if len(fields) != 6 or not re.fullmatch(r"[0-9]{8}", fields[0]):
            raise SourceRejected("ROW_FORMAT_INVALID")
        try:
            day = datetime.strptime(fields[0], "%Y%m%d").date()
        except ValueError:
            raise SourceRejected("ROW_DATE_INVALID") from None
        if day > observed.date():
            raise SourceRejected("FUTURE_SOURCE_DATE")
        try:
            session = is_session(day)
        except ValueError:
            raise SourceRejected("SOURCE_CALENDAR_UNSUPPORTED") from None
        if not session:
            raise SourceRejected("NON_SESSION_SOURCE_DATE")
        values = [_number(text) for text in fields[1:5]]
        if any(value is None for value in values) or not re.fullmatch(r"[0-9]+", fields[5]):
            raise SourceRejected("OHLC_INVALID")
        opened, high, low, close = values
        if low > min(opened, close) or high < max(opened, close) or low > high:
            raise SourceRejected("OHLC_INVALID")
        fingerprint = tuple(values) + (int(fields[5]),)
        if day in seen:
            if seen[day] != fingerprint:
                raise SourceRejected("DUPLICATE_DATE_CONFLICT")
            continue
        seen[day] = fingerprint
        records.append({
            "ts_kst": datetime.combine(day, time(15, 30), tzinfo=KST),
            "source_date": day, "asset": asset, "field": "close", "value": close,
            "unit": "pt", "source": "NaverFinance(" + asset + ")", "quality": "secondary",
            "url": URL, "notes": bind_note("supplement:missing_completed_session", day),
        })
    return pd.DataFrame(records).sort_values("ts_kst").reset_index(drop=True)


def supplement_core_index(frame, asset, target_date, observed_at):
    """Return an independent frame and an allowlisted review; never retry a source.

    A broken primary contract is a programming/data-integrity error, not a reason
    to overwrite it. External unavailability preserves the frame for the existing
    strict history validator. Only KOSPI/KOSDAQ use this optional fallback.
    """
    if asset not in NAMES:
        raise ValueError("CORE_INDEX_ASSET_UNSUPPORTED")
    if (not isinstance(target_date, date) or isinstance(target_date, datetime)
            or not isinstance(observed_at, datetime) or observed_at.tzinfo is None):
        raise ValueError("CORE_INDEX_TARGET_INVALID")
    observed_at = observed_at.astimezone(KST)
    if target_date > completed_session(observed_at) or not is_session(target_date):
        raise ValueError("CORE_INDEX_TARGET_INVALID")
    original = pd.DataFrame() if frame is None else frame.copy(deep=True)
    ready, primary_day = _primary_state(original, asset, target_date, observed_at)
    review = {"schema_version": 1, "asset": asset, "supplier": "NAVER_FCHART",
              "target_date": target_date.isoformat(), "observed_date": observed_at.date().isoformat(),
              "primary_source_date": primary_day, "primary_row_count": len(original),
              "primary_supplier_rows": _supplier_counts(original),
              "request_count": 0, "supplement_row_count": 0, "status": "PRIMARY_READY"}
    if ready:
        return original, review
    review["request_count"] = 1
    try:
        payload = _download(asset)
        review["supplier_response_sha256"] = hashlib.sha256(payload).hexdigest()
        extra = _parse(payload, asset, observed_at)
        review["supplier_source_date"] = max(extra["source_date"]).isoformat()
        review["supplier_row_count"] = len(extra)
        if target_date not in set(extra["source_date"]):
            raise SourceRejected("TARGET_DATE_MISSING")
        review["supplier_target_close_pt"] = float(extra.loc[extra["source_date"] == target_date, "value"].iloc[0])
        extra = extra.loc[extra["source_date"] <= target_date].copy()
        if not original.empty:
            extra = extra.loc[extra["source_date"] == target_date].copy()
        result = pd.concat([original, extra], ignore_index=True, sort=False) if not original.empty else extra
        review.update(status="SUPPLEMENTED", supplement_row_count=len(extra))
        return result.reset_index(drop=True), review
    except SourceRejected as exc:
        # SourceRejected is raised only with fixed literals in this module.
        review.update(status="UNAVAILABLE", reason_code=str(exc), exception_type="SourceRejected")
    except requests.Timeout:
        review.update(status="UNAVAILABLE", reason_code="NETWORK_TIMEOUT", exception_type="Timeout")
    except requests.RequestException:
        review.update(status="UNAVAILABLE", reason_code="NETWORK_ERROR", exception_type="RequestException")
    return original, review
