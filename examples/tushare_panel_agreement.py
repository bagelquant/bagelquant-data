from bagelquant_data.datasource import DataSourceRegistry, TushareDataSource
from bagelquant_data.loader import Loader

registry = DataSourceRegistry()
registry.register(TushareDataSource())

retrieved = (
    Loader(registry=registry)
    .source("tushare")
    .load_panel(
        dataset="daily",
        field="close",
        universe=["000001.SZ", "600000.SH"],
        start_date="2024-01-01",
        end_date="2024-12-31",
    )
)

retrieved.data.head()
