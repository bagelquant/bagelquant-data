# 快速开始

```python
from datetime import date
import polars as pl
from bagelquant_data import DataLake, DatasetSpec, RawInput

lake = DataLake.open(data_meta_path="example/data_meta.sqlite", lake_path="example/lake")
lake.raw.ingest(
    DatasetSpec("prices", "by_date", date_kind="calendar",
                field_mappings={"time": "time", "asset_id": "asset_id"}),
    pl.DataFrame({"time": [date(2020, 1, 2)], "asset_id": ["A"], "close": [11.25]}),
    mode="initialize",
)
print(lake.raw.observations("prices", source="custom").collect())
receipt = lake.inputs.freeze({"prices": RawInput("custom", "prices")},
                             information_cutoff="2020-01-02")
print(lake.inputs.read(receipt, "prices").collect())
print(lake.inputs.verify(receipt))
```

例子只写调用者提供的本地 frame，不调用 provider。初始化数据是历史基线，不能证明
原始发布时间；严格读取可以排除。重开时继续传入同一对路径，`read_only=True` 只验证
现有状态，缺失文件或数据库会报错；修改接口抛出 `PermissionError`。
