"""Thread-local numerical admission for provider response conversion.

Network and limiter waits do not acquire native capacity. Data-owned conversion,
pagination hashing and response preparation use the caller's same process gate.
"""
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar


_CALLBACK = ContextVar("data_response_native_admission", default=None)


@contextmanager
def response_admission_scope(callback):
    token = _CALLBACK.set(callback)
    try:
        yield
    finally:
        _CALLBACK.reset(token)


def response_admission():
    callback = _CALLBACK.get()
    return nullcontext() if callback is None else callback("columnar")
