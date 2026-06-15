# BagelQuant Data 文档

BagelQuant Data 是面向量化研究的本地数据湖框架。它以 Polars 作为 DataFrame 引擎，以 Parquet 作为标准分析存储格式，以 SQLite 保存可变的运行状态和元数据，并通过 `DataLake.open(...)` 暴露稳定的 Python API。

这个框架围绕两个核心思想设计：

- 数据源适配器负责获取数据，但不决定核心数据湖架构。
- 数据集行为由声明式规格定义，因此普通数据集的新增不应要求修改存储、查询、财务处理或管理模块。

## 文档导航

- [架构](architecture.md)：系统分层、存储区域、元数据、标准字段和 point-in-time 语义。
- [配置](configuration.md)：数据湖根目录、凭证、可选依赖和本地环境。
- [管理 API](management-api.md)：数据源注册、数据集注册、状态查询、检查和删除操作。
- [提取 API](extraction-api.md)：标准原始记录、单字段长面板、多字段、引用数据和观察网格。
- [财务 API](financial-api.md)：point-in-time 对齐和通用可组合财务变换。
- [增量更新](incremental-updates.md)：更新流程、暂存、标准提交、manifest 和 V1 限制。
- [Tushare 数据源](tushare.md)：内置 Tushare 适配器、凭证、数据集规格和标准时间映射。
- [新增数据源](adding-a-source.md)：如何实现新的数据源适配器。
- [新增数据集](adding-a-dataset.md)：如何编写数据集 YAML 并选择策略。
- [实施计划](implementation-plan.md)：设计目标和路线图说明。

## 第一个示例

```python
import polars as pl

from bagelquant_data import DataLake, DatasetSpec

lake = DataLake.open("data")

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

lake.ingest_frame(
    spec,
    pl.DataFrame(
        {
            "trade_date": ["20250102", "20250103"],
            "ts_code": ["000001.SZ", "000001.SZ"],
            "open": [11.20, 11.31],
            "close": [11.25, 11.37],
        }
    ),
)

close = lake.query.field("daily", "close", source="custom", collect=True)
print(close)
```

输出是只有三列的长面板：

```text
time        asset_id    close
2025-01-02  000001.SZ   11.25
2025-01-03  000001.SZ   11.37
```

## 设计边界

BagelQuant Data 不保留旧数据湖布局，也不兼容旧公开 API。当前框架是一次干净的新 API 和新存储设计。它也不提供 `eps_ttm()` 或 `roe_ttm()` 这类硬编码财务指标，而是提供滚动聚合、比率、存量平均和 point-in-time 对齐等通用积木。

## 开发检查

```bash
uv run pytest
uv run pyright
```
