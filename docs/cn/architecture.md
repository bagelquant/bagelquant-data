# 架构与设计

```text
Provider API 或本地文件
    |
    v
DataSource
    |
    v
DataLakeManager.update
    |
    v
LocalDataLake
    |
    +--> LoadedDataset
    |
    +--> RetrievedPanel
             |
             v
       下游适配器
```

## 设计哲学

- Provider 接入和研究逻辑分离。
- 本地快照是可复现读取的默认边界。
- 元数据、转换、存储和读取接口彼此独立。
- 输出保持为 Polars 和普通对象，避免向下游包产生反向依赖。

## 数据湖结构

本地 V1 存储使用 source/table 下的 Parquet 快照，可以按标准化后的 `time` 分区，并维护 JSON 元数据。

```text
lake-root/
  tushare/
    daily/
      year=2024/
        month=01/
          day=03/
            snapshots/
    income/
      year=2024/
        snapshots/
```

对 Tushare，价格表的 `time` 来自 `trade_date`；基本面表的 `time` 来自 `f_ann_date`，用于 point-in-time 读取。

读取时可以投影列并过滤日期。`LocalDataLake` 先使用分区元数据跳过不相关快照，再在读取后做精确日期过滤。

## 模块结构

- `datasource`：provider 适配器、请求对象和注册表。
- `lake`：本地存储、快照目录、直接读取和更新编排。
- `loader`：优先读取本地 lake 的检索接口。
- `metadata`：schema、contract、identity 和 lineage。
- `transform`：无状态 DataFrame 转换流水线。
- `cache`：可选缓存策略和实现。

