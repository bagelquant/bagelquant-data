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

本地 V1 存储使用 source/table/year/month 分区的 Parquet 快照，并维护 JSON 元数据。

```text
lake-root/
  tushare/
    daily/
      year=2024/
        month=01/
          snapshots/
```

读取时可以投影列并过滤日期。`LocalDataLake` 先使用分区元数据跳过不相关快照，再在读取后做精确日期过滤。

## 模块结构

- `datasource`：provider 适配器、请求对象和注册表。
- `lake`：本地存储、快照目录、直接读取和更新编排。
- `loader`：优先读取本地 lake 的检索接口。
- `metadata`：schema、contract、identity 和 lineage。
- `transform`：无状态 DataFrame 转换流水线。
- `cache`：可选缓存策略和实现。

