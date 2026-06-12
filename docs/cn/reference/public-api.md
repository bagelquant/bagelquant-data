# 公开 API

`bagelquant-data` 通过 Python API 使用。包会返回 pandas 对象和普通元数据，而不是导入下游研究包。

## 顶层导出

```python
from bagelquant_data import (
    DataContract,
    DataLakeManager,
    DataRequest,
    DataSource,
    DataSourceRegistry,
    DatasetSchema,
    FieldSchema,
    LoadedDataset,
    Loader,
    LocalDataLake,
    RetrievedPanel,
    Transform,
    TushareTableUpdateSpec,
    default_registry,
)
```

## Provider 注册表

- `DataSource`：provider 适配器基类。
- `DataRequest`：dataset、字段、过滤条件、日期范围、版本、快照和选项。
- `DataSourceRegistry.register(source)`：注册适配器。
- `DataSourceRegistry.resolve(name)`：解析适配器。
- `default_registry`：进程级便利注册表。

## 数据湖

- `LocalDataLake(root)`：本地文件系统快照后端。
- `DataLakeManager(lake, registry=None)`：高层写入和更新 API。
- `DataLakeManager.add(source, dataset, frame)`：添加自定义数据。
- `DataLakeManager.edit(source, dataset, frame)`：替换数据集快照。
- `DataLakeManager.delete(source, dataset)`：删除数据集指针。
- `LocalDataLake.read(...)`：读取投影和日期过滤后的数据。
- `LocalDataLake.read_panel_field(...)`：把字段 id 读取为面板形状。

## Loader

- `Loader(registry=None, lake=None)`：优先 lake 的读取门面。
- `Loader.source(name).load(...)`：返回 `LoadedDataset`。
- `Loader.source(name).load_panel(...)`：返回 `RetrievedPanel`。
- `LoadedDataset.data`：pandas 数据。
- `RetrievedPanel.data`：单字段日期乘资产 frame。

## Tushare 辅助 API

- `TushareDataSource`：Tushare provider 适配器。
- `TushareTableUpdateSpec`：生产式表更新规格。
- `DataLakeManager.scan_tushare_updates(...)`：生成 dry-run 更新报告。
- `DataLakeManager.execute_tushare_update_report(report, workers=4)`：执行审核后的任务。

