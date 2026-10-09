# BagelQuant Data

An independent Python package for Raw datasets, typed daily DataItems, categories,
versions, coverage, point-in-time reads, frozen inputs and local recovery.
Data returns Polars frames and imports no other BagelQuant package. Workbench
supplies paths, market declarations and global runtime policy.

```python
from pathlib import Path
from datetime import date
import polars as pl
from bagelquant_data import DataLake, DatasetSpec, DataItemSpec, RawInput

lake = DataLake.open(
    data_meta_path=Path("research/data_meta.sqlite"),
    lake_path=Path("research/lake"),
)
lake.raw.ingest(
    DatasetSpec("prices", "by_date", date_kind="calendar",
                field_mappings={"time": "time", "asset_id": "asset_id"}),
    pl.DataFrame({"time": [date(2020, 1, 2)], "asset_id": ["A"], "close": [11.25]}),
    mode="initialize",
)
lake.items.register(DataItemSpec(
    "close", (RawInput("custom", "prices"),),
    time_column="source_time", value_column="close",
))
lake.items.initialize("close", start="2020-01-02", end="2020-01-02")
print(lake.items.read("close").collect())
```

| API | Responsibility |
| --- | --- |
| `lake.catalog` | Sources and independent Raw/DataItem category trees |
| `lake.raw` | Declarations, initialize/update/refresh/ingest, reads and coverage |
| `lake.items` | Typed long tables, transformations, external producers and builds |
| `lake.integrity` | Passive scans, plans, guarded baseline reopening and evidence-based local repairs |
| `lake.inputs` | Durable frozen input receipts, reads and verification |
| `bagelquant_data.exploration` | Pure statistics and Polars result tables |

Both paths are required. `data_meta_path` identifies the single Data SQLite file,
including task state and compressed Arrow recovery evidence. Immutable Parquet
generations retain year/month partitions. Read-only queries never initialize or
recover data; SQLite may create or update its WAL/SHM coordination sidecars.
`DataLake.inspect(data_meta_path=..., lake_path=...)` checks schema and lake binding
without changing any configured storage files or directories, including sidecars.
Package 0.7 and metadata schema 7 are an incompatible fresh-database cut; old
databases are rejected. No aliases, migrations or automatic history cleanup exist.

Historical daily reads are causal by default. Explicit `as_of` selects a fixed
information cutoff; `view="versions"` exposes revisions. Initialization labels
unverifiable original publication history as a baseline; `strict=True` excludes it.
Later unchanged observations preserve new availability evidence without creating
duplicate content versions. Frozen receipts pin both commit and check boundaries.

Read [overview](docs/en/1_overview.md), [quickstart](docs/en/2_quickstart.md),
[datasets](docs/en/3_datasets.md), [sources](docs/en/4_sources.md),
[updates](docs/en/5_updates.md), [queries](docs/en/6_queries.md),
[operations](docs/en/7_operations.md), [DataItems](docs/en/8_items.md) and
[exploration](docs/en/9_exploration.md). [中文文档](docs/zh-CN/1_overview.md).
AI contributors start with [AGENTS.md](AGENTS.md) and [.ai/README.md](.ai/README.md).

```bash
uv run pytest
uv run pyright
uv run ruff check .
```

Supported platforms: macOS and Linux. Windows support is retired.

[Calculation records](docs/en/10_calculation_records.md) · [计算记录](docs/zh-CN/10_calculation_records.md)
