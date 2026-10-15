"""Thread-owned persistent transport for the documented Tushare HTTP API."""

from __future__ import annotations

from functools import partial
from typing import Any

from bagelquant_data.core.exceptions import DataSourceError
from bagelquant_data.sources.tushare.authentication import resolve_token


class TushareClient:
    """One HTTP Session per fetch thread; quota admission stays in the source.

    Use the provider's documented JSON protocol rather than mutating SDK/global
    requests state. Sessions perform no automatic retries: the Raw ledger owns
    attempts, pacing, cancellation and pagination.
    """

    def __init__(self, token: str, session: Any) -> None:
        self._token = token
        self._session = session

    def __repr__(self) -> str:
        return "TushareClient(token=<redacted>)"

    def query(self, api_name: str, fields: str = "", **params: Any) -> Any:
        import pandas as pd
        response = self._session.post("http://api.tushare.pro", json={
            "api_name": api_name, "token": self._token, "params": params, "fields": fields}, timeout=30)
        # An HTTP failure must not become a successful empty daily scope.
        response.raise_for_status()
        body = response.json()
        if not isinstance(body, dict) or "code" not in body:
            raise DataSourceError("Tushare returned an invalid response envelope")
        if body["code"] != 0:
            raise DataSourceError(str(body.get("msg") or "Tushare request failed"))
        data = body.get("data")
        if not isinstance(data, dict) or not isinstance(data.get("fields"), list) or not isinstance(data.get("items"), list):
            raise DataSourceError("Tushare returned invalid tabular data")
        return pd.DataFrame(data["items"], columns=data["fields"])

    def close(self) -> None:
        self._session.close()

    def __getattr__(self, name: str):
        if name.startswith("_"):
            raise AttributeError(name)
        return partial(self.query, name)

    def __del__(self) -> None:
        session = self.__dict__.get("_session")
        if session is not None:
            session.close()


def build_client(token: str | None = None) -> TushareClient:
    """Create a lazy, thread-owned Session without writing credentials to disk."""

    try:
        import requests
        import pandas  # noqa: F401 - validate the complete optional transport
    except ImportError as exc:
        raise DataSourceError("Install Tushare support with: uv sync --extra tushare") from exc
    return TushareClient(resolve_token(token), requests.Session())
