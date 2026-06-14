from bagelquant_data.datasource import DataSourceRegistry, TushareDataSource
from bagelquant_data.lake import DataLakeManager, LocalDataLake

registry = DataSourceRegistry()
registry.register(TushareDataSource())

lake = LocalDataLake(".bagelquant-data-lake")
manager = DataLakeManager(lake, registry=registry)

manager.update_tushare_stock_basic()
manager.update_tushare_trading_calendar(start_date="2000-01-01")
manager.register_tushare_update_table("daily")

report = manager.scan_tushare_updates(
    manager.tushare_update_specs(),
    start_date="2024-01-01",
    end_date="2024-01-31",
)

manager.execute_tushare_update_report(report, continue_on_error=True)
print(lake.read_panel_field("tushare_daily_close").head())
