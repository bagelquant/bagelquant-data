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
