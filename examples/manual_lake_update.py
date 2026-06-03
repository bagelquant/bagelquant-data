from bagelquant_data.datasource import DataSourceRegistry, TushareDataSource
from bagelquant_data.lake import DataLakeManager, LocalDataLake

registry = DataSourceRegistry()
registry.register(TushareDataSource())

manager = DataLakeManager(LocalDataLake(".bagelquant-data-lake"), registry=registry)
manager.update_tushare_all(
    "daily",
    start_date="2000-01-01",
    workers=4,
)
