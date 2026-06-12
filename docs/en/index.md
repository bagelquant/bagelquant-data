# BagelQuant Data

`bagelquant-data` is the provider-neutral data layer for BagelQuant.

It guarantees consistent, reliable, reproducible access to data. It does not
own research, portfolio construction, graph execution, backtesting, or
analytics.

Use it as a backend Python package to register providers, manage local data lake
snapshots, run provider updates, and retrieve pandas datasets or panel-shaped
objects:

```python
from bagelquant_data.datasource import DataSourceRegistry, TushareDataSource
from bagelquant_data.lake import DataLakeManager, LocalDataLake

registry = DataSourceRegistry()
registry.register(TushareDataSource(token="your-token"))

lake = LocalDataLake(".bagelquant-data-lake")
manager = DataLakeManager(lake, registry=registry)
```

## Recommended Reading

- [Quick start](quick-start.md)
- [Concepts](concepts.md)
- [Architecture](architecture.md)
- [Reference](reference/index.md)
- [Backend API](reference/backend-api.md)
- [Public API](reference/public-api.md)
- [Contracts](reference/contracts.md)
- [Panel agreements](reference/panel-agreements.md)
- [Tushare provider](reference/providers/tushare.md)
