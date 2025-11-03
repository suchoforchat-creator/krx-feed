from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

import logging
import re
import warnings
from urllib.parse import urlparse

import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup

from ..utils import kst_now

logger = logging.getLogger(__name__)


SYMBOL_MAP = {
    "WTI": "CL=F",
    "Brent": "BZ=F",
    "Gold": "GC=F",
    "Copper": "HG=F",
    "BTC": "BTC-USD",
}

HTML_SOURCES = {
    "WTI": [
        (
            "https://www.cmegroup.com/markets/energy/crude-oil/wti-crude-oil.quotes.html",
            ["span[data-field='last']", "td.last", "span.last"],
        ),
        (
            "https://www.eia.gov/dnav/pet/pet_pri_spt_s1_d.htm",
            ["table tr td:nth-of-type(2)", "td.number"],
        ),
    ],
    "Brent": [
        (
            "https://www.ice.com/products/219/Brent-Crude-Futures/data?marketId=5137226",
            ["span[data-field='last']", "td.last"],
        ),
        (
            "https://www.eia.gov/dnav/pet/pet_pri_spt_s1_d.htm",
            ["table tr td:nth-of-type(3)", "td.number"],
        ),
    ],
    "Gold": [
        (
            "https://www.cmegroup.com/markets/metals/precious/gold.quotes.html",
            ["span[data-field='last']", "td.last"],
        ),
        (
            "https://www.lbma.org.uk/prices-and-data/gold-price",
            ["div.price-value", "span.price"],
        ),
    ],
    "Copper": [
        (
            "https://www.cmegroup.com/markets/metals/base/copper.quotes.html",
            ["span[data-field='last']", "td.last"],
        ),
        (
            "https://tradingeconomics.com/commodity/copper",
            ["span#p", "div.price"],
        ),
    ],
    "BTC": [
        (
            "https://www.coindesk.com/price/bitcoin/",
            ["span[data-price]", "div.price-large"],
        ),
        (
            "https://www.coingecko.com/en/coins/bitcoin",
            ["span.no-wrap", "span.tw-text-3xl"],
        ),
    ],
}


@dataclass
class FetchResult:
    frame: pd.DataFrame
    note: str = ""


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


def _parse_price(text: str, selectors: Tuple[str, ...]) -> float:
    soup = BeautifulSoup(text, "lxml")
    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue
        raw = node.get_text(strip=True)
        if not raw:
            continue
        cleaned = re.sub(r"[^0-9.+-]", "", raw)
        if cleaned:
            return float(cleaned)
    raise ValueError("price selector not found")


def _from_html(asset: str, url: str, selectors: Tuple[str, ...]) -> pd.DataFrame:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    value = _parse_price(response.text, selectors)
    ts = kst_now()
    frame = pd.DataFrame(
        {
            "ts_kst": [ts],
            "asset": [asset],
            "field": ["close"],
            "value": [value],
            "unit": ["usd"],
            "source": [urlparse(url).netloc or url],
            "quality": ["secondary"],
            "url": [url],
        }
    )
    return frame


def fetch(periods: int = 120) -> Dict[str, FetchResult]:
    results: Dict[str, FetchResult] = {}
    for asset, symbol in SYMBOL_MAP.items():
        frames: list[pd.DataFrame] = []
        note = ""
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
            idx = pd.DatetimeIndex(series.index)
            if idx.tz is None:
                idx = idx.tz_localize("UTC")
            idx = idx.tz_convert("Asia/Seoul")
            frames.append(
                pd.DataFrame(
                    {
                        "ts_kst": idx,
                        "asset": asset,
                        "field": "close",
                        "value": series.values,
                        "unit": "usd",
                        "source": "finance.yahoo.com",
                        "quality": "secondary",
                        "url": f"https://finance.yahoo.com/quote/{symbol}",
                    }
                )
            )
        except Exception as exc:  # pragma: no cover - network dependent
            note = f"parse_failed:https://finance.yahoo.com/quote/{symbol},{exc}"
            logger.warning("yfinance download failed for %s: %s", asset, exc)

        if not frames:
            for url, selectors in HTML_SOURCES.get(asset, []):
                try:
                    frame = _from_html(asset, url, tuple(selectors))
                    frames.append(frame)
                    note = ""
                    break
                except Exception as exc:  # pragma: no cover - network dependent
                    note = f"parse_failed:{url},{exc}"
                    logger.warning("HTML parse failed for %s (%s): %s", asset, url, exc)
                    continue

        if frames:
            frame = pd.concat(frames, ignore_index=True)
        else:
            frame = pd.DataFrame(columns=["ts_kst", "asset", "field", "value", "unit", "source", "quality", "url"])
        results[asset] = FetchResult(frame=frame, note=note)
    return results
