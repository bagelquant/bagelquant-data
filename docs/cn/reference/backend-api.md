# 后端 API

`bagelquant-data` 通过 Python API 操作。主要工作流是：

```text
DataSourceRegistry
  -> DataSource
  -> DataLakeManager
  -> LocalDataLake
  -> Loader / lake.read / lake.read_panel_field
```

## 设置

```python
from bagelquant_data.datasource import DataSourceRegistry, TushareDataSource
from bagelquant_data.lake import DataLakeManager, LocalDataLake

registry = DataSourceRegistry()
registry.register(TushareDataSource(token="your-token"))

lake = LocalDataLake(".bagelquant-data-lake")
manager = DataLakeManager(lake, registry=registry)
```

## 数据湖管理

`LocalDataLake` 负责文件系统存储。它在 source/table 分区下写入不可变 Parquet 快照，并维护 JSON catalog 指针。

```python
manager.add("custom", "prices", frame)
manager.edit("custom", "prices", corrected_frame)
manager.delete("custom", "prices")
```

可以使用 `LocalDataLake.read` 直接读取后端表，也可以使用 `read_panel_field` 读取日期乘资产的 panel 字段。

## Provider 更新

`DataLakeManager.update` 会执行一次 provider 读取并写入一个本地快照。Tushare 的生产式更新流程通常先刷新参考资源，再扫描待更新任务，最后执行确认后的 report。

## Loader

`Loader` 提供 lake-first 读取：优先读本地快照，必要时再访问 provider，并返回带 lineage、calendar、universe 和字段元数据的标准结果。
