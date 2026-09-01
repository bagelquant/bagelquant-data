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
spec = DatasetSpec(
    "daily",
    "by_daily",
    calendar="trade_cal",
    field_mappings={"trade_date": "time", "ts_code": "asset_id"},
)
lake.ingest(spec, pl.DataFrame({"trade_date": ["20250102"], "ts_code": ["000001.SZ"], "close": [11.25]}))
print(lake.query.query("daily", source="custom", fields=["time", "asset_id", "close"]).collect())
```

`general` datasets replace one file and do not require canonical key fields.
`by_daily` and `by_asset` datasets derive the key `(time, asset_id)` and must
explicitly map provider fields to those names; add `primary_key_extra` when
another field, such as `period`, is also unique.

Incremental local coverage is owned by the lake's `update_scopes` ledger.
`by_daily` records one scope per open date and request variant, and `by_asset`
records one scope per asset and request variant. A scope becomes successful
only after its canonical Parquet commit succeeds. Provider-only range checks
are stored separately in `provider_scope_checks` and never advance local data
coverage. Validated empty responses are durable `empty` scope outcomes. Daily
empties are rechecked on the next update while their dates remain within the
latest 20 requested trading sessions. Empty `by_asset` scopes become eligible
again when the requested target advances or their revision check is due. Older
empty daily scopes stay terminal. Repair
requests finish and commit before new daily scopes begin. Applications may
compact untouched historical `by_daily` backlog into provider date-range calls;
the response is still committed and recorded as individual daily scope outcomes,
so later incremental updates never repeat completed history. `by_asset` data uses
stable year/asset-bucket partitions (32 buckets by default), so a new asset
does not rewrite one whole-year file. Old metadata schemas are rejected rather
than migrated; metadata schema v3 requires a fresh lake root.

```bash
uv run pytest
uv run pyright
uv run ruff check .
```
