# BagelQuant Data

`bagelquant-data` is a Polars-native, source-agnostic data lake framework for
quantitative research.

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

from bagelquant_data import DataLake, DatasetSpec

lake = DataLake.open("data")
spec = DatasetSpec(
    name="daily",
    source="custom",
    source_dataset="daily",
    category="market",
    field_mapping={"ts_code": "ts_code", "trade_date": "trade_date"},
    required_columns=("asset_id", "time"),
    primary_key=("asset_id", "time"),
    asset_column="ts_code",
    time_column="trade_date",
    partition_strategy="year_month",
    deduplication="primary_key_last",
    sort_columns=("time", "asset_id"),
)
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

close = lake.query.field("daily", "close", source="custom", collect=True)
print(close)  # time, asset_id, close
```

Documentation is available in two languages:

- English: `docs/en/index.md`
- Chinese: `docs/cn/index.md`

## Development

```bash
uv run pytest
```
