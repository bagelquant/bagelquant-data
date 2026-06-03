from bagelquant_data.datasource import (
    DataRequest,
    DataSourceRegistry,
    TushareDataSource,
)
from bagelquant_data.lake import DataLakeManager, LocalDataLake
from bagelquant_data.loader import Loader

registry = DataSourceRegistry()
registry.register(TushareDataSource())

lake = LocalDataLake(".bagelquant-data-lake")
manager = DataLakeManager(lake, registry=registry)

manager.update(
    "tushare",
    DataRequest(
        dataset="daily",
        filters={"ts_code": "000001.SZ"},
        start_date="2024-01-01",
        end_date="2024-01-31",
    ),
)

daily = Loader(registry=registry, lake=lake).source("tushare").load("daily")
daily.data.head()
