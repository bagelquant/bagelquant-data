# BagelQuant Data

`bagelquant-data` is a Polars-native, source-agnostic data lake framework for
quantitative research.

- `lake.admin` manages sources, dataset specs, manifests, health, repair, and
  deletion.
- `lake.update` plans and runs dataset updates with retries, batching,
  pagination, and run observability.
- `lake.query` reads price panels, PIT fundamentals, events, reference data,
  and raw canonical records.
- Polars is the dataframe engine.
- Parquet is the canonical analytical storage format.
- SQLite stores mutable metadata, manifests, run state, and source/dataset
  registration.
- Tushare is implemented as the first source adapter under
  `bagelquant_data.sources.tushare`.
- Non-reference research extraction returns one field at a time as
  `time | asset_id | value`.

```python
import polars as pl

from bagelquant_data import DataLake

lake = DataLake.open("data")
spec = lake.admin.datasets.add("daily", "by_daily", reference="trade_cal")
lake.ingest_frame(
    spec,
    pl.DataFrame(
        {
            "trade_date": ["2024-01-02"],
            "ts_code": ["000001.SZ"],
            "close": [100.0],
        }
    ),
)

print(lake.admin.summary())
close = lake.query.price("daily", "close", source="custom", collect=True)
print(close)  # time, asset_id, close
```

Documentation starts at `docs/en/index.md`.

## Development

```bash
uv run pytest
uv run pyright
uv run ruff check .
```
