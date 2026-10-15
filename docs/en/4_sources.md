# Sources

A source implements `name`, `configure(**options)`, `test_connection()` and
`fetch(dataset, request) -> polars.DataFrame`. Register/configure through
`lake.catalog.sources`. Reading stored data never calls a provider.

```python
from bagelquant_data import TushareSource

lake.catalog.sources.register(TushareSource())
lake.catalog.sources.configure("tushare", token="runtime-secret")
```

Credentials remain runtime configuration and are redacted from persisted public
configuration. Dataset mappings, China-market calendars and availability
conventions are caller declarations. Data defaults to neutral UTC timing; it
does not infer exchange sessions or China semantics.

Sources support `list/get/register/configure/enable/disable/remove/test`. Removing
a source with active datasets or categories fails. Registering an adapter is
local; `test()` and update APIs perform explicit external calls. Tests use fake
providers and temporary lakes.

Tushare transport uses one thread-owned persistent `requests.Session` per fetch
thread and the provider's [documented JSON HTTP protocol](https://tushare.pro/document/2?doc_id=40).
Install `--extra tushare` for pandas and requests. Transport performs no automatic
retries: Raw owns pacing, attempts, cancellation, pagination and completeness.
HTTP or invalid-envelope failures cannot certify empty coverage. Injected fake
clients remain supported for isolated tests. This change has fake-transport
validation; no live provider validation or measured network speedup is implied.
