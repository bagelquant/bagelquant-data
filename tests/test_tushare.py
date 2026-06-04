from __future__ import annotations

import pandas as pd
import pytest

from bagelquant_data.config import Settings
from bagelquant_data.datasource import DataRequest, DataSourceRegistry, RetryConfig
from bagelquant_data.datasource.tushare import TushareDataSource
from bagelquant_data.lake import LocalDataLake
from bagelquant_data.loader import Loader
from bagelquant_data.utils.exceptions import DataSourceAuthError, DataSourceError


class FakeTushareClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def daily(self, **params):
        self.calls.append(("daily", params))
        return pd.DataFrame(
            {
                "ts_code": ["000001.SZ", "600000.SH", "000001.SZ"],
                "trade_date": ["20240102", "20240102", "20240103"],
                "close": [10.0, 20.0, 11.0],
            }
        )

    def query(self, api_name, **params):
        self.calls.append((api_name, params))
        return pd.DataFrame({"value": [1]})


def test_tushare_requires_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)

    with pytest.raises(DataSourceAuthError):
        TushareDataSource(client=FakeTushareClient())


def test_tushare_uses_settings_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)

    source = TushareDataSource(
        settings=Settings(tushare_token="settings-token"),
        client=FakeTushareClient(),
    )

    assert source.describe("daily")["token"] == "<redacted>"
    assert "settings-token" not in repr(source)


def test_tushare_normalizes_dates_and_delegates() -> None:
    client = FakeTushareClient()
    source = TushareDataSource(token="token", client=client)

    frame = source.read(
        DataRequest(
            dataset="daily",
            filters={"ts_code": "000001.SZ"},
            start_date="2024-01-02",
            end_date=pd.Timestamp("2024-01-03"),
        )
    )

    assert frame["close"].tolist() == [10.0, 20.0, 11.0]
    assert client.calls == [
        (
            "daily",
            {
                "ts_code": "000001.SZ",
                "start_date": "20240102",
                "end_date": "20240103",
            },
        )
    ]


def test_loader_provider_panel_like_output_has_date_index() -> None:
    registry = DataSourceRegistry()
    registry.register(TushareDataSource(token="token", client=FakeTushareClient()))

    loaded = Loader(registry=registry).source("tushare").load("daily")

    assert loaded.data.index.name == "date"
    assert loaded.data.index.tolist() == [
        pd.Timestamp("2024-01-02"),
        pd.Timestamp("2024-01-02"),
        pd.Timestamp("2024-01-03"),
    ]


def test_loader_provider_panel_like_output_sorts_date_index() -> None:
    class UnsortedDailyClient(FakeTushareClient):
        def daily(self, **params):
            return pd.DataFrame(
                {
                    "ts_code": ["000001.SZ", "000001.SZ"],
                    "trade_date": ["20240103", "20240102"],
                    "close": [11.0, 10.0],
                }
            )

    registry = DataSourceRegistry()
    registry.register(TushareDataSource(token="token", client=UnsortedDailyClient()))

    loaded = Loader(registry=registry).source("tushare").load("daily")

    assert loaded.data.index.tolist() == [
        pd.Timestamp("2024-01-02"),
        pd.Timestamp("2024-01-03"),
    ]


def test_stock_basic_output_is_not_forced_to_date_index() -> None:
    class StockBasicClient(FakeTushareClient):
        def stock_basic(self, **params):
            return pd.DataFrame({"ts_code": ["000001.SZ"], "name": ["Ping An"]})

    registry = DataSourceRegistry()
    registry.register(TushareDataSource(token="token", client=StockBasicClient()))

    loaded = Loader(registry=registry).source("tushare").load("stock_basic")

    assert loaded.data.index.name is None
    assert loaded.data["ts_code"].tolist() == ["000001.SZ"]


def test_tushare_generic_query_delegates() -> None:
    client = FakeTushareClient()
    source = TushareDataSource(token="token", client=client)

    frame = source.read(
        DataRequest(
            dataset="generic",
            options={"api_name": "income", "params": {"ts_code": "000001.SZ"}},
        )
    )

    assert frame["value"].tolist() == [1]
    assert client.calls == [("income", {"ts_code": "000001.SZ"})]


def test_tushare_daily_panel_agreement_shapes_trade_date_by_code() -> None:
    registry = DataSourceRegistry()
    registry.register(TushareDataSource(token="token", client=FakeTushareClient()))

    agreement = Loader(registry=registry).source("tushare").load_panel(
        dataset="daily",
        field="close",
        universe=["000001.SZ", "600000.SH"],
        start_date="2024-01-01",
        end_date="2024-01-31",
        region="CN",
    )

    assert agreement.dataset_name == "tushare.daily.close"
    assert agreement.frame.index.tolist() == [
        pd.Timestamp("2024-01-02"),
        pd.Timestamp("2024-01-03"),
    ]
    assert agreement.frame.columns.tolist() == ["000001.SZ", "600000.SH"]
    assert agreement.frame.loc[pd.Timestamp("2024-01-02"), "600000.SH"] == 20.0


def test_tushare_daily_panel_agreement_can_read_from_lake(tmp_path) -> None:
    registry = DataSourceRegistry()
    registry.register(TushareDataSource(token="token", client=FakeTushareClient()))
    lake = LocalDataLake(tmp_path)
    lake.add(
        "tushare",
        "daily",
        pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240102"],
                "close": [10.0],
            }
        ),
    )

    agreement = Loader(registry=registry, lake=lake).source("tushare").load_panel(
        dataset="daily",
        field="close",
        universe=["000001.SZ"],
        start_date="2024-01-01",
        end_date="2024-01-31",
        region="CN",
    )

    assert agreement.metadata["origin"] == "lake"
    assert agreement.frame.loc[pd.Timestamp("2024-01-02"), "000001.SZ"] == 10.0


def test_tushare_retry_wraps_failures() -> None:
    class FailingClient:
        def daily(self, **params):
            raise RuntimeError("permission denied")

    source = TushareDataSource(
        token="token",
        client=FailingClient(),
        retry=RetryConfig(attempts=1),
    )

    with pytest.raises(DataSourceError, match="permission denied"):
        source.read(DataRequest(dataset="daily"))


def test_tushare_access_limit_retries_after_one_minute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RateLimitedClient:
        def __init__(self) -> None:
            self.calls = 0

        def daily(self, **params):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("API access limit exceeded")
            return pd.DataFrame({"value": [1]})

    sleeps: list[float] = []
    monkeypatch.setattr("bagelquant_data.datasource.tushare.time.sleep", sleeps.append)
    client = RateLimitedClient()
    source = TushareDataSource(
        token="token",
        client=client,
        retry=RetryConfig(attempts=2),
    )

    frame = source.read(DataRequest(dataset="daily"))

    assert frame["value"].tolist() == [1]
    assert client.calls == 2
    assert sleeps == [60.0]
