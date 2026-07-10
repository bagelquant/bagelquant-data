# Overview

BagelQuant Data is a local research data lake. It stores canonical Parquet
files, SQLite metadata, and update history under one root directory.

The public API has three facades:

- `lake.admin` registers sources and datasets and inspects lake state.
- `lake.update` plans and runs provider updates.
- `lake.query` lazily reads stored data.

Datasets are defined only by update behavior: `general` replaces one dataset
file, `by_daily` updates missing trading dates, and `by_asset` updates each asset.
