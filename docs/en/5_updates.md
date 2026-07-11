# Updates

Run one dataset, a list of datasets, or every enabled dataset for a source.

```python
lake.update.dataset("daily", source="tushare", today="2026-07-10")
lake.update.datasets(["trade_cal", "daily"], source="tushare")
lake.update.source("tushare")
```

`by_daily` uses its `calendar`; `by_asset` uses its `asset_list`. The lake
deduplicates incremental records by their derived primary key before writing
their partition files.

Pass provider-specific values for one run with `params`, for example
`lake.update.dataset("stock_basic", source="tushare", params={"exchange": "SSE"})`.
Per-run `params` override a dataset's configured `source_api_params` and
`source_api_param_sets`; planner-owned daily and asset keys continue to take
precedence. General datasets merge every expanded parameter-set response and
replace stored data only when all calls succeed.
