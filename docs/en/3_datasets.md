# Datasets and categories

Declare `DatasetSpec(name, "general" | "by_date", source=...)` and register it
through `lake.raw.register(spec)`. Source/name are stable safe path components.
Provider-to-canonical field mappings are explicit. Daily datasets map a date to
`time` and security identity to `asset_id`; extra primary keys may be declared.
`date_kind="calendar"` plans natural days; trading dates require an explicit
calendar dataset. Parameter variants are declared independently of transport pagination.

`general` is fetched completely on each explicit update. Changed content creates a
new complete snapshot, including an empty snapshot. Unchanged content records a
check. Incomplete requests never replace the last complete snapshot. `by_date`
updates declared daily scopes, retaining successful and validated empty outcomes;
empty daily responses do not erase already observed records.

```python
tree = lake.catalog.raw_categories("tushare")
equity = tree.create("equity")
market = tree.create("market", parent_id=equity["id"])
tree.assign("daily", market["id"])
tree.rename(market["id"], "prices")
tree.move(market["id"], parent_id=None)
```

Each source has a separate Raw category tree. `lake.catalog.item_categories` is an
independent tree. Trees support `list/get/create/rename/move/remove/assign/members`;
cycles, duplicate sibling names and nonempty deletion fail. Category moves never
rewrite data. `raw.remove` and `items.remove` unregister objects only after active
dependencies have been removed. Committed history, recovery batches and frozen
references remain available; there is no automatic cleanup API.

TOML declarations use `raw.register_toml(path)` or `raw.register_toml_text(text)`.
Definitions are persisted and recovered on reopen; runtime producer/provider
instances must be registered again when needed for execution.
