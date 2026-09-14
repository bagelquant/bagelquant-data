# BagelQuant Data

`bagelquant-data` is a local Parquet and SQLite data lake for quantitative
research. Its public API has three facades: `lake.admin`, `lake.update`, and
`lake.query`.

Read the guides in order: [overview](docs/en/1_overview.md),
[quickstart](docs/en/2_quickstart.md), [datasets](docs/en/3_datasets.md),
[sources](docs/en/4_sources.md), [updates](docs/en/5_updates.md),
[queries](docs/en/6_queries.md), and [operations](docs/en/7_operations.md).
Chinese PIT guides are available under [docs/zh-CN](docs/zh-CN/1_overview.md).

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

`general` stores each changed complete snapshot from an explicit update. An unchanged
refresh records a check without creating another content version. `by_daily`
tracks each declared trading or calendar date and parameter variant. Both use
monthly Parquet files backed by immutable Arrow batches in monthly SQLite
recovery journals. `lake.db` alone determines committed data and coverage.

Raw preserves provider columns, `source_time` (observation/announcement date),
`time` (version availability), and UTC `ingested_at`. Explicit `initialize`
creates a historical baseline; subsequent updates use the later of the source
date and the configured collection-day boundary. Workbench uses Shanghai date
minus one day. Unchanged checks do not create content versions. Updates recheck
the most recent three natural days; older refreshes require `mode="refresh"`.

Use `as_of_date` for a PIT snapshot, `view="versions"` for all versions, and
`observation_start`/`observation_end` independently of availability `start`/`end`.
`lake.query.observations()` returns the normal numerical date axis after version
selection. Historical initialization cannot recover provider history overwritten
before collection began.

Schema v4 and package v0.6 are a hard cut. Old databases are rejected before
writes; no migration, data deletion, or automatic provider recovery occurs.
[Versioning and recovery](docs/en/5_updates.md) describe the full contract.

```bash
uv run pytest
uv run pyright
uv run ruff check .
```
