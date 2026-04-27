from datetime import date

import types
import pandas as pd

from src.sources.dxy import DXYCollector


def test_collect_uses_yfinance_fallback(monkeypatch):
    collector = DXYCollector()

    monkeypatch.setattr(collector, "_fetch_stooq", lambda urls: (None, None))
    monkeypatch.setattr(collector, "_fetch_marketwatch", lambda: None)
    monkeypatch.setattr(collector, "_fetch_tradingview", lambda: None)
    monkeypatch.setattr(collector, "_fetch_yfinance", lambda: 105.12)

    frame, notes = collector.collect(date(2026, 4, 24))
    assert not frame.empty
    assert float(frame.iloc[0]["value"]) == 105.12
    assert frame.iloc[0]["source"] == "yfinance"
    assert "fallback:yfinance" in notes["DXY:idx"]


def test_fetch_yfinance_reads_last_close(monkeypatch):
    collector = DXYCollector()

    def fake_download(*args, **kwargs):
        return pd.DataFrame({"Close": [None, 104.2, 104.8]})

    fake_module = types.SimpleNamespace(download=fake_download)
    monkeypatch.setitem(__import__("sys").modules, "yfinance", fake_module)
    value = collector._fetch_yfinance()
    assert value == 104.8


def test_fetch_yfinance_handles_multiindex_close(monkeypatch):
    collector = DXYCollector()

    def fake_download(*args, **kwargs):
        # 실제 yfinance가 반환할 수 있는 MultiIndex 형태를 재현한다.
        cols = pd.MultiIndex.from_tuples([("Close", "DX-Y.NYB"), ("Open", "DX-Y.NYB")])
        return pd.DataFrame([[104.1, 103.8], [104.9, 104.4]], columns=cols)

    fake_module = types.SimpleNamespace(download=fake_download)
    monkeypatch.setitem(__import__("sys").modules, "yfinance", fake_module)
    value = collector._fetch_yfinance()
    assert value == 104.9


def test_collect_records_failure_chain_when_all_sources_fail(monkeypatch):
    collector = DXYCollector()

    monkeypatch.setattr(collector, "_fetch_stooq", lambda urls: (None, None))
    monkeypatch.setattr(collector, "_fetch_marketwatch", lambda: None)
    monkeypatch.setattr(collector, "_fetch_tradingview", lambda: None)
    monkeypatch.setattr(collector, "_fetch_yfinance", lambda: None)

    frame, notes = collector.collect(date(2026, 4, 24))
    assert frame.empty
    assert "all_sources_failed:" in notes["DXY:idx"]
    assert "stooq_unavailable" in notes["DXY:idx"]
    assert "yfinance_unavailable" in notes["DXY:idx"]
