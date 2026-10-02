"""Regression tests are fixtures only, never real authentication/collection."""
import pytest


@pytest.fixture(autouse=True)
def fixture_only_network(monkeypatch):
    def reject(*args, **kwargs):
        raise AssertionError('REGRESSION_NETWORK_FORBIDDEN')
    monkeypatch.setattr('socket.socket.connect', reject)
    monkeypatch.setattr('requests.sessions.Session.request', reject)
    try:
        import curl_cffi.requests
    except ImportError:
        return
    monkeypatch.setattr(curl_cffi.requests.Session, 'request', reject)
