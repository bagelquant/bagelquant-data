from bagelquant_data.datasource import DataSourceRegistry, TushareDataSource
from bagelquant_data.loader import Loader

registry = DataSourceRegistry()
registry.register(TushareDataSource())

daily = Loader(registry=registry).source("tushare").load(
    "daily",
    filters={"ts_code": "000001.SZ"},
    start_date="2024-01-01",
    end_date="2024-01-31",
)

