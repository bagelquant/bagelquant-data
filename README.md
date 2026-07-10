# BagelQuant Data

`bagelquant-data` is a local Parquet and SQLite data lake for quantitative
research. Its public API has three facades: `lake.admin`, `lake.update`, and
`lake.query`.

Read the guides in order: [overview](docs/en/1_overview.md),
[quickstart](docs/en/2_quickstart.md), [datasets](docs/en/3_datasets.md),
[sources](docs/en/4_sources.md), [updates](docs/en/5_updates.md),
[queries](docs/en/6_queries.md), and [operations](docs/en/7_operations.md).

```python
import polars as pl
from bagelquant_data import DataLake, DatasetSpec

lake = DataLake.open("data")
spec = DatasetSpec("daily", "by_daily", calendar="trade_cal")
lake.ingest(spec, pl.DataFrame({"trade_date": ["20250102"], "ts_code": ["000001.SZ"], "close": [11.25]}))
print(lake.query.query("daily", source="custom", fields=["time", "asset_id", "close"]).collect())
```

`general` datasets replace one file and do not require canonical key fields.
`by_daily` and `by_asset` datasets derive the key `(time, asset_id)`; add
`primary_key_extra` when another field, such as `period`, is also unique.

```bash
uv run pytest
uv run pyright
uv run ruff check .
```
