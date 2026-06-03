from bagelquant_data.datasource import (
    DataRequest,
    DataSourceRegistry,
    TushareDataSource,
)
from bagelquant_data.lake import (
    DataLakeManager,
    LocalDataLake,
    UpdateSchedule,
)

registry = DataSourceRegistry()
registry.register(TushareDataSource())

manager = DataLakeManager(LocalDataLake(".bagelquant-data-lake"), registry=registry)
manager.periodic_update(
    "tushare-daily",
    source_name="tushare",
    request=DataRequest(dataset="daily", filters={"ts_code": "000001.SZ"}),
    schedule=UpdateSchedule(every=1, unit="days"),
)

manager.run_due()
