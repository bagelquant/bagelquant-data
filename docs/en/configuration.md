# Configuration

The primary configuration is the lake root path passed to `DataLake.open(...)`.

```python
from bagelquant_data import DataLake

lake = DataLake.open("data")
```

Opening the lake creates the storage zones under that root if they do not already exist.

## Default Local Layout

```text
data/
    lake/
    staging/
    rejected/
    metadata/lake.db
    tmp/
```

The repository also includes a sample configuration file:

```text
config/bagelquant-data.toml
```

At this stage, the Python API is the source of truth. The TOML file documents intended local defaults and can be used by future CLI or application integration.

## Dependencies

Core dependencies:

- `polars`
- `pyarrow`

Optional Tushare dependencies:

- `pandas`
- `tushare`

Install or sync with the project tooling:

```bash
uv sync
uv sync --extra tushare
```

## Credentials

Credentials are configured at runtime. They should not be written into dataset YAML, Parquet files, committed configuration files, or documentation examples.

For Tushare, either pass a token:

```python
from bagelquant_data.sources.tushare import TushareSource

source = TushareSource(token="...")
```

Or configure after registration. This saves the token into the local lake metadata DB so future runs can register `TushareSource()` and update without passing the token again:

```python
lake.sources.register(TushareSource())
lake.sources.configure_tushare(token="...")
```

Or set an environment variable:

```bash
export TUSHARE_TOKEN="..."
```

Saved source options are local to the lake root, stored under `data/metadata/lake.db`, and redacted from source listings.

## Local Development

Run tests:

```bash
uv run pytest
```

Run static checking:

```bash
uv run pyright
```

Run the thin CLI:

```bash
uv run bagelquant-data --root data status
uv run bagelquant-data --root data source-list
uv run bagelquant-data --root data dataset-list --source tushare
```

Reusable setup, update, and monitoring workflows now live in the separate
`manage-data-lake` repo. Keep this package focused on reusable lake APIs.

## What Not To Configure

Do not configure old lake layouts, legacy migration paths, or backward compatibility switches. The framework intentionally targets the new canonical design only.
