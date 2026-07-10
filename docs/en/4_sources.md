# Sources

Sources are small adapters that fetch a Polars DataFrame for one provider
request. Register and configure them through `lake.admin.sources`.

```python
from bagelquant_data import TushareSource

lake.admin.sources.register(TushareSource())
lake.admin.sources.configure("tushare", token="...")
```

Keep credentials in environment variables or runtime configuration, never in
dataset declarations or committed files.
