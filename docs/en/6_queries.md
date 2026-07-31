# Queries

Both query methods return a Polars `LazyFrame`; call `.collect()` only when an
eager DataFrame is required.

Use `query_general()` for every kind of dataset, especially `general` datasets
without canonical time and asset fields:

```python
stocks = lake.query.query_general("stock_basic", source="tushare", fields=["ts_code", "name"])
```

Use `query()` only for `by_daily` and `by_asset` datasets. It can filter the
canonical `(time, asset_id)` key and select output fields:

```python
close = lake.query.query(
    "daily",
    source="tushare",
    start="2026-01-01",
    end="2026-01-31",
    assets=["000001.SZ"],
    fields=["time", "asset_id", "close"],
)
```

Omit `fields` to return all stored fields. Calling `query()` on a `general`
dataset raises an error; use `query_general()` instead.

The lake prunes manifest entries before creating Polars scans. Date filters
exclude non-overlapping monthly or yearly partitions; asset filters on
`by_asset` datasets additionally select only the stable asset buckets. Polars
still receives the exact predicates and projection for pushdown.

Partitions with older compatible schemas are grouped by manifest
`schema_hash`. Each group is projected, missing columns are filled with typed
nulls, and compatible numeric columns are cast to the canonical dataset schema
before the lazy groups are concatenated. A query outside stored coverage
returns a typed empty `LazyFrame`. A manifest that references a missing file
fails explicitly and never falls back to a directory glob.
