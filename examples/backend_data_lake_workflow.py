from __future__ import annotations

import pandas as pd

from bagelquant_data.datasource import DataSourceRegistry, TushareDataSource
from bagelquant_data.lake import (
    DataLakeManager,
    LocalDataLake,
    TushareTableUpdateSpec,
    TushareTradingCalendarRef,
)
from bagelquant_data.loader import Loader


def progress(event: dict[str, object]) -> None:
    table = event.get("table")
    status = event.get("status", "updated")
    completed = event.get("completed")
    total = event.get("total")
    rows = event.get("rows_written")
    print(f"{table}: {status} ({completed}/{total}), rows={rows}")


registry = DataSourceRegistry()
registry.register(TushareDataSource(token="your-token"))

lake = LocalDataLake(".bagelquant-data-lake")
manager = DataLakeManager(lake, registry=registry)
loader = Loader(registry=registry, lake=lake).source("tushare")

# 1. Refresh source reference resources.
manager.update_tushare_stock_basic()
manager.update_tushare_trading_calendar(start_date="2000-01-01")

# 2. Scan a daily price update before executing provider reads.
daily_spec = TushareTableUpdateSpec(
    table="daily",
    kind="price",
    trading_calendar=TushareTradingCalendarRef(
        name="trade_cal",
        table="trade_cal",
        date_column="cal_date",
        open_column="is_open",
    ),
)
report = manager.scan_tushare_updates(
    specs=(daily_spec,),
    start_date="2024-01-01",
    end_date="2024-01-31",
)

for plan in report.plans:
    print(
        f"{plan.table}: {plan.status}, jobs={plan.estimated_job_count}, "
        f"reason={plan.reason}"
    )

# 3. Execute the reviewed report and persist snapshots.
refs = manager.execute_tushare_update_report(report, workers=4, progress=progress)
print(f"wrote {len(refs)} snapshots")

# 4. Read directly from the lake with projection and date filters.
close = lake.read(
    "tushare",
    "daily",
    columns=("close",),
    start_date="2024-01-01",
    end_date="2024-01-31",
)
print(close.head())

# 5. Load through the backend loader. This reads the lake first.
loaded = loader.load(
    "daily",
    fields=("open", "close"),
    start_date="2024-01-01",
    end_date="2024-01-31",
)
print(loaded.identity)
print(loaded.data.head())

# 6. Retrieve panel-shaped data for downstream systems.
retrieved = loader.load_panel(
    dataset="daily",
    field="close",
    universe=["000001.SZ", "600000.SH"],
    start_date="2024-01-01",
    end_date="2024-01-31",
)
print(retrieved.dataset_name)
print(retrieved.data.head())

panel = lake.read_panel_field(
    "tushare_daily_close",
    start_date="2024-01-01",
    end_date="2024-01-31",
)
print(panel.head())

# 7. Manage custom data with the same backend manager.
custom = pd.DataFrame(
    {
        "trade_date": ["20240102", "20240103"],
        "asset_id": ["demo", "demo"],
        "value": [1.0, 1.1],
    }
)
manager.add("custom", "signals", custom)
manager.edit("custom", "signals", custom.assign(value=[1.2, 1.3]))
print(manager.list_tables("custom"))
manager.delete("custom", "signals")
