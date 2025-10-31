from __future__ import annotations

from typing import Dict

import pandas as pd
import yfinance as yf

from ..utils import synthetic_series

YF_SOURCE = "YahooFinance"
YF_URL = "https://finance.yahoo.com"


SYMBOL_MAP = {
    "WTI": "CL=F",
    "Brent": "BZ=F",
    "Gold": "GC=F",
    "Copper": "HG=F",
    "BTC": "BTC-USD",
}


def fetch(periods: int = 120) -> Dict[str, pd.DataFrame]:
    results: Dict[str, pd.DataFrame] = {}
    for asset, symbol in SYMBOL_MAP.items():
        try:
            data = yf.download(symbol, period="6mo", interval="1d", progress=False)
            if data.empty:
                raise ValueError("empty")
            series = data["Close"].dropna().tail(periods)
        except Exception:
            fixture = synthetic_series(100, 5, periods=periods, seed=hash(asset) % 10_000)
            series = fixture
        idx = pd.DatetimeIndex(series.index)
        if idx.tz is None:
            idx = idx.tz_localize("UTC")
        idx = idx.tz_convert("Asia/Seoul")
        frame = pd.DataFrame({
            "ts_kst": idx,
            "asset": asset,
            "field": "close",
            "value": series.values,
            "unit": "usd",
            "source": YF_SOURCE,
            "quality": "secondary",
            "url": f"{YF_URL}/quote/{symbol}",
        })
        results[asset] = frame
    return results
