# 管理 API

管理 API 从 `DataLake.open(...)` 暴露。

```python
from bagelquant_data import DataLake

lake = DataLake.open("data")
```

门面对象包含：

- `lake.sources`
- `lake.datasets`
- `lake.update`
- `lake.query`
- `lake.finance`
- `lake.status`

## 数据源管理

注册数据源：

```python
from bagelquant_data.sources.tushare import TushareSource

lake.sources.register(TushareSource(name="tushare"))
```

配置数据源：

```python
lake.sources.configure("tushare", token="...")
```

Tushare 便捷方法会委托给通用配置方法：

```python
lake.sources.configure_tushare(token="...")
```

列出已注册数据源：

```python
sources = lake.sources.list()
```

获取适配器：

```python
tushare = lake.sources.get("tushare")
```

测试连接：

```python
lake.sources.test("tushare")
```

移除数据源注册：

```python
lake.sources.remove("tushare")
```

移除注册不会删除标准数据。

## 数据集管理

数据集行为由 `DatasetSpec` 对象或 YAML 文件声明。

添加 spec 对象：

```python
from bagelquant_data import DatasetSpec

spec = DatasetSpec(
    name="daily",
    source="custom",
    source_dataset="daily",
    category="market",
    field_mapping={"ts_code": "ts_code", "trade_date": "trade_date"},
    required_columns=("asset_id", "time"),
    primary_key=("asset_id", "time"),
    asset_column="ts_code",
    time_column="trade_date",
    partition_strategy="year_month",
    deduplication="primary_key_last",
    sort_columns=("time", "asset_id"),
)

lake.datasets.add(spec)
```

添加 YAML spec：

```python
lake.datasets.add_from_yaml(
    "src/bagelquant_data/sources/tushare/datasets/daily.yaml"
)
```

获取数据集：

```python
spec = lake.datasets.get("daily", source="tushare")
```

列出数据集：

```python
all_datasets = lake.datasets.list()
tushare_datasets = lake.datasets.list("tushare")
```

启用或禁用数据集：

```python
lake.datasets.enable("daily", source="tushare")
lake.datasets.disable("daily", source="tushare")
```

仅移除注册，不删除数据：

```python
lake.datasets.remove("daily", source="tushare")
```

删除标准数据必须显式确认：

```python
lake.datasets.remove(
    "daily",
    source="tushare",
    delete_data=True,
    confirm=True,
)
```

## 状态和检查

汇总：

```python
summary = lake.status.summary()
```

数据集状态：

```python
status = lake.status.dataset("income", source="tushare")
```

分区 manifest：

```python
partitions = lake.status.partitions("income", source="tushare")
```

最近摄取 run：

```python
runs = lake.status.runs(limit=20)
```

失败 run：

```python
failures = lake.status.failures(dataset="income", source="tushare")
```

manifest 已知文件：

```python
files = lake.status.files("income", source="tushare")
```

普通状态查询使用 SQLite manifest 元数据，成本较低，不需要扫描所有 Parquet 文件。

## 标准记录检查

用于人工检查：

```python
records = lake.query.records(
    "income",
    source="tushare",
    limit=10,
)
```

这不是主要研究提取 API。单值面板应使用 `lake.query.field(...)`。
