import math

import numpy as np
import pandas as pd
import pytest

from src.compute import compute_records
from src.sources import commod_crypto
from src.utils import rolling_corr, rolling_vol


def make_series(asset: str, field: str, values: np.ndarray, unit: str = "pt") -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=len(values), freq="B")
    return pd.DataFrame(
        {
            "ts_kst": idx,
            "asset": asset,
            "field": field,
            "value": values,
            "unit": unit,
            "source": "KIS-sim",
            "quality": "primary",
            "url": "",
        }
    )


def test_rolling_vol_matches_manual():
    idx = pd.date_range("2024-01-01", periods=40, freq="B")
    series = pd.Series(np.linspace(100, 120, 40), index=idx)
    hv = rolling_vol(series, 30)
    returns = np.log(series / series.shift(1)).dropna().tail(30)
    expected = math.sqrt(252) * returns.std()
    assert math.isclose(hv, expected, rel_tol=1e-9)


def test_rolling_corr():
    idx = pd.date_range("2024-01-01", periods=25, freq="B")
    a = pd.Series(np.arange(25, dtype=float), index=idx)
    b = pd.Series(np.arange(25, dtype=float) * 2, index=idx)
    corr = rolling_corr(a, b, 20)
    assert math.isclose(corr, 1.0)


def test_compute_records_outputs_required_keys():
    ts = pd.Timestamp("2024-05-01", tz="Asia/Seoul")
    base = np.linspace(100, 110, 40)
    raw = {
        "KOSPI": pd.concat([
            make_series("KOSPI", "close", np.linspace(2500, 2550, 40)),
            make_series("KOSPI", "adv_count", np.linspace(400, 450, 1)),
            make_series("KOSPI", "dec_count", np.linspace(350, 360, 1)),
            make_series("KOSPI", "unch_count", np.linspace(50, 55, 1)),
            make_series("KOSPI", "adv_value", np.linspace(1000, 1000, 1), unit="krw_bn"),
            make_series("KOSPI", "dec_value", np.linspace(800, 800, 1), unit="krw_bn"),
            make_series("KOSPI", "limit_up", np.linspace(5, 5, 1)),
            make_series("KOSPI", "limit_down", np.linspace(3, 3, 1)),
            make_series("KOSPI", "turnover", np.linspace(1200, 1200, 1), unit="krw_bn"),
        ], ignore_index=True),
        "KOSDAQ": pd.concat([
            make_series("KOSDAQ", "close", np.linspace(800, 820, 40)),
            make_series("KOSDAQ", "adv_count", np.linspace(300, 300, 1)),
            make_series("KOSDAQ", "dec_count", np.linspace(200, 200, 1)),
            make_series("KOSDAQ", "unch_count", np.linspace(40, 40, 1)),
        ], ignore_index=True),
        "K200": make_series("K200", "close", np.linspace(300, 320, 40)),
        "SPX": make_series("SPX", "close", np.linspace(5000, 5100, 40)),
        "NDX": make_series("NDX", "close", np.linspace(15000, 15100, 40)),
        "SOX": make_series("SOX", "close", np.linspace(5000, 5050, 40)),
        "ES": make_series("ES", "close", np.linspace(5010, 5110, 40)),
        "NQ": make_series("NQ", "close", np.linspace(15010, 15110, 40)),
        "DX": make_series("DX", "close", np.linspace(103, 104, 40), unit="index"),
        "USD/KRW": make_series("USD/KRW", "close", np.linspace(1300, 1310, 40), unit="krw"),
        "KR3Y": make_series("KR3Y", "yield", np.linspace(3.5, 3.7, 40), unit="bp"),
        "KR10Y": make_series("KR10Y", "yield", np.linspace(4.0, 4.1, 40), unit="bp"),
        "UST2Y": make_series("UST2Y", "yield", np.linspace(4.5, 4.6, 40), unit="bp"),
        "UST10Y": make_series("UST10Y", "yield", np.linspace(4.2, 4.3, 40), unit="bp"),
        "WTI": make_series("WTI", "close", np.linspace(80, 81, 40), unit="usd"),
        "Brent": make_series("Brent", "close", np.linspace(85, 86, 40), unit="usd"),
        "Gold": make_series("Gold", "close", np.linspace(1900, 1910, 40), unit="usd"),
        "Copper": make_series("Copper", "close", np.linspace(4.0, 4.1, 40), unit="usd"),
        "BTC": make_series("BTC", "close", np.linspace(60000, 60500, 40), unit="usd"),
    }

    records = compute_records(ts, raw, {})
    required = {
        ("KOSPI", "idx"),
        ("KOSDAQ", "idx"),
        ("USD/KRW", "spot"),
        ("WTI", "spot"),
        ("BTC", "spot"),
    }
    seen = {(row["asset"], row["key"]) for row in records}
    assert required.issubset(seen)


def test_commod_crypto_fallback(monkeypatch):
    def boom(*_, **__):
        raise RuntimeError("network down")

    monkeypatch.setattr(commod_crypto.yf, "download", boom)

    class FakeResponse:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self) -> None:  # pragma: no cover - simple stub
            return None

    def fake_get(url: str, timeout: int = 0):  # pragma: no cover - deterministic stub
        return FakeResponse("""<html><span data-field='last'>83.45</span></html>""")

    monkeypatch.setattr(commod_crypto.requests, "get", fake_get)
    results = commod_crypto.fetch(periods=5)
    assert set(results) == {"WTI", "Brent", "Gold", "Copper", "BTC"}
    sample = results["WTI"].frame
    assert not sample.empty
    assert pd.to_datetime(sample["ts_kst"]).dt.tz is not None
    assert pytest.approx(float(sample["value"].iloc[-1]), rel=1e-6) == 83.45
    assert results["WTI"].note == ""
