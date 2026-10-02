"""Load the optional KRX fallback only when used; cache initialization failure."""
from importlib import import_module

_modules = {}
_error_type = None


class OptionalPykrxUnavailable(RuntimeError):
    pass


def get_pykrx(name):
    global _error_type
    if name not in {"stock", "bond"}:
        raise ValueError("unsupported optional module")
    if _error_type is not None:
        raise OptionalPykrxUnavailable("pykrx_initialization_failed:" + _error_type)
    if name not in _modules:
        try:
            _modules[name] = getattr(import_module("pykrx"), name)
        except Exception as exc:
            # Never preserve response bodies, credentials or authentication URLs.
            _error_type = type(exc).__name__
            raise OptionalPykrxUnavailable("pykrx_initialization_failed:" + _error_type) from None
    return _modules[name]


class LazyPykrx:
    def __init__(self, name):
        self.name = name

    def __getattr__(self, attribute):
        if attribute.startswith("_"):
            raise AttributeError(attribute)
        def invoke(*args, **kwargs):
            return getattr(get_pykrx(self.name), attribute)(*args, **kwargs)
        return invoke
