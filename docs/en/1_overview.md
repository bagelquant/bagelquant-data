# Overview

BagelQuant Data is a local research data lake. It stores canonical Parquet
files, SQLite metadata, and update history under one root directory.

The public API has three facades:

- `lake.admin` registers sources and datasets and inspects lake state.
- `lake.update` plans and runs provider updates.
- `lake.query` lazily reads stored data.

Datasets have two update types: `general` produces complete explicit snapshots;
`by_daily` plans trading or natural dates and declared parameter variants.
Both retain immutable PIT versions in monthly files. Numerical observation time,
version availability, and actual ingestion time are separate axes.
