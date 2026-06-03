# Panel Agreements

`PanelInputAgreement` is the explicit bridge from `bagelquant-data` to
`bagelquant-core`.

It contains:

- `kind`: `numeric_panel` or `category_panel`
- `frame`: a pandas DataFrame
- `domain_spec`: region, universe, start date, and end date
- `dataset_name`: stable input name
- `metadata`: provider, request, lineage, and field metadata

`bagelquant-data` validates the shape of the frame but does not construct core
objects.

```python
from bagelquant_core import Domain, Panel

domain = Domain(**agreement.domain_spec.to_core_kwargs())
panel = Panel.from_domain(
    agreement.frame,
    domain,
    name=agreement.dataset_name,
    metadata=agreement.metadata,
)
```

This preserves one-directional dependencies and keeps core responsible for
Panel semantics.
