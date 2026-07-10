# Adding A Source

A source adapter fetches provider data. Update planning belongs to the data lake, so adapters stay small and do not decide storage layout, calendar gaps, ID lists, or merge behavior.

## Protocol

Implement the source methods used by the updater:

```python
from collections.abc import Mapping
from typing import Any

import polars as pl


class MySource:
    @property
    def name(self) -> str:
        return "my_source"

    def configure(self, **options: Any) -> None:
        ...

    def test_connection(self) -> None:
        ...

    def fetch(self, source_dataset: str, request: Mapping[str, Any]) -> pl.DataFrame:
        ...
```

`request` is planned by `lake.update`. Adapters may translate generic request keys such as `start`, `end`, `date`, and `id` into provider-specific parameters before calling the SDK.

## Register

```python
from bagelquant_data import DataLake

lake = DataLake.open("data")
lake.admin.sources.add(MySource())
lake.admin.sources.edit("my_source", token="...")
lake.admin.sources.test("my_source")
```

## Credentials

Accept credentials through runtime configuration or environment variables. Do not store secrets in dataset YAML, committed config, docs, or Parquet files.

Source options persisted in SQLite are redacted from source listings when keys contain `token`, `secret`, or `password`.

## Fetching

`fetch` returns a Polars `DataFrame`.

```python
return pl.from_pandas(response.copy(deep=True))
```

## Boundary

Adapters should preserve provider fields and economically meaningful records. Canonical naming, validation, deduplication, update planning, partitioning, and manifest management belong to the lake.

## Tests

Recommended coverage:

- token-safe `repr`
- `configure` updates runtime options
- `test_connection` raises useful errors
- `fetch` returns Polars data
- provider parameter mapping preserves `start`, `end`, `date`, and `id` semantics
