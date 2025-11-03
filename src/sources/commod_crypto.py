from __future__ import annotations

from typing import Dict

import logging
import warnings

import pandas as pd
import yfinance as yf

from ..utils import synthetic_series

logger = logging.getLogger(__name__)

YF_SOURCE = "YahooFinance"
YF_URL = "https://finance.yahoo.com"


SYMBOL_MAP = {
    "WTI": "CL=F",
    "Brent": "BZ=F",
    "Gold": "GC=F",
    "Copper": "HG=F",
    "BTC": "BTC-USD",
}


def _extract_close(data: pd.DataFrame) -> pd.Series:
    if data.empty:
        raise ValueError("empty response")
    if isinstance(data.columns, pd.MultiIndex):
        close = data.xs("Close", axis=1, level=-1, drop_level=False)
        if isinstance(close, pd.DataFrame):
            close = close.squeeze(axis=1)
    else:
        close = data.get("Close")
    if close is None:
        raise KeyError("Close column not found")
    series = pd.Series(close).dropna()
    if series.empty:
        raise ValueError("close series empty")
    series.index = pd.to_datetime(series.index)
    return series


def _fallback_series(asset: str, periods: int) -> pd.Series:
    return synthetic_series(100, 5, periods=periods, seed=hash(asset) % 10_000)


def fetch(periods: int = 120) -> Dict[str, pd.DataFrame]:
    results: Dict[str, pd.DataFrame] = {}
    for asset, symbol in SYMBOL_MAP.items():
        error_msg = ""
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=FutureWarning)
                data = yf.download(
                    symbol,
                    period="6mo",
                    interval="1d",
                    progress=False,
                    auto_adjust=False,
                    threads=False,
                )
            series = _extract_close(data).tail(periods)
        except Exception as exc:  # pragma: no cover - network dependent
            error_msg = str(exc)
            logger.warning("yfinance fallback for %s (%s): %s", asset, symbol, error_msg)
            series = _fallback_series(asset, periods)
        idx = pd.DatetimeIndex(series.index)
        if idx.tz is None:
            idx = idx.tz_localize("UTC")
        idx = idx.tz_convert("Asia/Seoul")
        frame = pd.DataFrame(
            {
                "ts_kst": idx,
                "asset": asset,
                "field": "close",
                "value": series.values,
                "unit": "usd",
                "source": YF_SOURCE if not error_msg else f"Synthetic({YF_SOURCE})",
                "quality": "secondary",
                "url": f"{YF_URL}/quote/{symbol}",
            }
        )
        results[asset] = frame
    return results
