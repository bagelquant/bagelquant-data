# Adding A Source

A source adapter fetches external data and optionally plans source-specific requests. It must not force changes in storage, query, finance, or management modules.

## Source Protocol

Implement the `DataSource` protocol:

```python
from collections.abc import Iterable, Mapping
from typing import Any

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.request import RequestContext


class MySource:
    @property
    def name(self) -> str:
        return "my_source"

    def configure(self, **options: Any) -> None:
        ...

    def test_connection(self) -> None:
        ...

    def fetch(
        self,
        source_dataset: str,
        request: Mapping[str, Any],
    ) -> pl.DataFrame:
        ...

    def plan_requests(
        self,
        dataset: DatasetSpec,
        context: RequestContext,
    ) -> Iterable[Mapping[str, Any]]:
        ...
```

## Register The Source

```python
from bagelquant_data import DataLake

lake = DataLake.open("data")
lake.sources.register(MySource())
lake.sources.configure("my_source", token="...")
lake.sources.test("my_source")
```

## Credential Rules

Credentials should be accepted through:

- environment variables
- runtime configuration
- local untracked configuration
- future secret-provider integrations

Do not store secrets in:

- dataset YAML
- Parquet files
- committed docs
- committed TOML files
- SQLite metadata rows

## Request Planning

`plan_requests` receives a `DatasetSpec` and `RequestContext`.

For a snapshot API, emit one request:

```python
yield {"start_date": context.start, "end_date": context.end}
```

For an asset-oriented API, emit one request per asset:

```python
for asset in context.assets or []:
    yield {"asset_id": asset}
```

For paged APIs, emit page parameters. The core update pipeline should not know about the provider's pagination details.

## Fetching

`fetch` returns a Polars `DataFrame`. If a provider SDK returns pandas, convert inside the adapter:

```python
return pl.from_pandas(response.copy(deep=True))
```

## Normalization Boundary

Source adapters should preserve source fields as much as possible. Canonical naming happens through the dataset spec and normalizer.

Do not hide economically meaningful records in the adapter. Valid revisions, restatements, repeated forecasts, and multiple point-in-time records belong in canonical storage unless validation proves they are malformed.

## Testing A Source

Recommended tests:

- token-safe `repr`
- `configure` does not persist secrets
- `test_connection` raises useful errors
- `fetch` converts provider responses to Polars
- `plan_requests` respects `start`, `end`, `assets`, and `source_options`
