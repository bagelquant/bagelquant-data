# Updates

Run one dataset, a list of datasets, or every enabled dataset for a source.

```python
lake.update.dataset("daily", source="tushare", today="2026-07-10")
lake.update.datasets(["trade_cal", "daily"], source="tushare", start="19991231")
lake.update.source("tushare", end="2026-07-10")
```

`by_daily` uses its `calendar`; `by_asset` uses its `asset_list`. The lake
deduplicates incremental records by their derived primary key before writing
their partition files.

Incremental updates default to a fallback start of `1999-12-31` and an end of
today. Existing daily datasets and assets resume on the day after their latest
local date; the supplied `start` is used only when no local data exists. Compact
`YYYYMMDD` dates are accepted.

`datasets()` and `source()` plan and display their jobs before asking whether to
run all incremental jobs, daily jobs, asset jobs, a general refresh, or quit.
Pass `confirm=False` to run all daily and asset jobs without prompting. General
datasets are refreshed only through the explicit general choice or a direct
`dataset()` call.

Pass provider-specific values for one run with `params`, for example
`lake.update.dataset("stock_basic", source="tushare", params={"exchange": "SSE"})`.
Per-run `params` override a dataset's configured `source_api_params` and
`source_api_param_sets`; planner-owned daily and asset keys continue to take
precedence. General datasets merge every expanded parameter-set response and
replace stored data only when all calls succeed.
