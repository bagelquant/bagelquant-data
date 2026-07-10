# Configuration

The main configuration value is the lake root path passed to `DataLake.open(...)`.

```python
from bagelquant_data import DataLake

lake = DataLake.open("data")
```

Opening a lake creates the local storage layout if it does not already exist.

## Local Layout

```text
data/
    lake/
    staging/
    rejected/
    metadata/lake.db
    tmp/
```

The repository includes `config/bagelquant-data.toml` as an example local configuration file. Runtime behavior is controlled by the Python API and persisted lake metadata.

## Dependencies

Core dependencies:

- `polars`
- `pyarrow`
- `tqdm`

Optional Tushare dependencies:

- `pandas`
- `tushare`

Install with:

```bash
uv sync
uv sync --extra tushare
```

## Credentials

Credentials are configured at runtime. Do not put secrets in dataset YAML, Parquet files, committed TOML files, or docs examples.

For Tushare, pass a token when constructing the source:

```python
from bagelquant_data.sources.tushare import TushareSource

lake.admin.sources.add(TushareSource(token="..."))
```

Or configure a registered source:

```python
lake.admin.sources.add(TushareSource())
lake.admin.sources.edit("tushare", token="...")
```

Source options are saved in `metadata/lake.db` and redacted from source listings.

## Local Checks

```bash
uv run pytest
uv run pyright
uv run ruff check .
```

The CLI is a thin wrapper around the Python API:

```bash
uv run bagelquant-data --root data status
uv run bagelquant-data --root data source-list
uv run bagelquant-data --root data dataset-list --source tushare
```
