# Operations

Inspect a lake through `lake.admin`:

```python
print(lake.admin.summary())
print(lake.admin.status.dataset("daily", source="tushare"))
print(lake.admin.runs())
```

Use `rebuild_manifest` after repairing local Parquet files and
`validate_manifest` to compare metadata with the stored files. Remove a dataset
with `lake.admin.datasets.remove`; pass `delete_data=True, confirm=True` only
when its Parquet data should also be removed.
