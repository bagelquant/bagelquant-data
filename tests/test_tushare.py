from __future__ import annotations

import pandas as pd
import polars as pl

from bagelquant_data.datasource import DataRequest, TushareDataSource


class Client:
    def daily(self, **kwargs):
        return pd.DataFrame(
            {
                "trade_date": ["20240101"],
                "ts_code": ["000001.SZ"],
                "close": [10.0],
            }
        )


def test_tushare_adapter_converts_provider_pandas_to_polars() -> None:
    source = TushareDataSource(token="token", client=Client())

    data = source.read(DataRequest(dataset="daily"))

    assert isinstance(data, pl.DataFrame)
    assert data.columns == ["time", "asset_id", "close"]
    assert str(data["time"].to_list()[0]) == "2024-01-01"
    assert data["asset_id"].to_list() == ["000001.SZ"]
