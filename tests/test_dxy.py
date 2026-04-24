from datetime import date

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

    import src.sources.dxy as dxy_mod

    monkeypatch.setattr(dxy_mod, "yf", None, raising=False)
    class FakeYF:
        download = staticmethod(fake_download)

    monkeypatch.setitem(__import__("sys").modules, "yfinance", FakeYF)
    value = collector._fetch_yfinance()
    assert value == 104.8
