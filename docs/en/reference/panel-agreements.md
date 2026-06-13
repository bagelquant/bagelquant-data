# Retrieved Panels

`RetrievedPanel` is a neutral data-layer result. It is not a core adapter and
does not import or construct `bagelquant-core` objects.

It contains:

- `kind`: `numeric_panel` or `category_panel`
- `data`: a Polars DataFrame with `time`, `asset_id`, and `value` columns
- `universe`: a static asset sequence or dynamic membership DataFrame
- `calendar`: a sorted Polars Series of `time` values
- `dataset_name`: stable input name
- `metadata`: provider, request, lineage, field, and calendar metadata

Downstream code can use those plain objects explicitly:

```python
from bagelquant_core import Domain, Panel

domain = Domain(calendar=retrieved.calendar, universe=retrieved.universe)
panel = Panel.from_domain(
    retrieved.data,
    domain,
    name=retrieved.dataset_name,
    metadata=retrieved.metadata,
)
```

This preserves one-directional dependencies and keeps core responsible for
Panel semantics.
