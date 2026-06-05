from bagelquant_data.datasource import DataSourceRegistry, TushareDataSource
from bagelquant_data.lake import (
    DataLakeManager,
    LocalDataLake,
    TushareTradingCalendarRef,
    TushareUniverseRef,
)

registry = DataSourceRegistry()
registry.register(TushareDataSource())

manager = DataLakeManager(LocalDataLake(".bagelquant-data-lake"), registry=registry)

manager.update_tushare_universe("stock_basic")
manager.update_tushare_trading_calendar(start_date="2000-01-01")
manager.update_tushare_all(
    "daily",
    start_date="2000-01-01",
    workers=4,
    universe=TushareUniverseRef(
        name="stock_basic",
        table="stock_basic",
        code_column="ts_code",
    ),
    trading_calendar=TushareTradingCalendarRef(
        name="trade_cal",
        table="trade_cal",
        date_column="cal_date",
        open_column="is_open",
    ),
)
