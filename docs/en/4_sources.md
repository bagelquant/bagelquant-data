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


Adapters may provide the optional `wait_for_request(dataset, cancel_requested=...) -> bool` admission hook. The generic ingestion worker calls it before each provider request and stops that request when it returns false. Tushare uses this hook to coordinate workers sharing an endpoint: after a per-minute quota response, it observes a shared cooldown and spaces subsequent requests below the reported quota. The wait checks cancellation in short intervals. Other endpoints and already committed success/empty scopes remain independent.
