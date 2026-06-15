# 架构

BagelQuant Data 是一个数据源无关框架。Tushare 是第一个内置数据源，但核心模块不会导入 Tushare 专用代码。

## 分层概览

包结构按职责拆分：

- `core`：数据集规格、数据源协议、注册表、请求上下文、标准化接口、分区、去重、校验、哈希和异常。
- `sources`：数据源适配器。`sources/tushare` 是第一个实现。
- `storage`：本地文件布局、Parquet 写入、原子替换、SQLite 元数据、暂存、拒收记录和 manifest。
- `pipeline`：摄取、更新编排、校验和提交逻辑。
- `query`：标准原始记录、单字段面板、引用数据、记录检查和观察网格。
- `finance`：通用 point-in-time 和财务变换。
- `management`：公开 `DataLake` 门面以及数据源、数据集、状态、更新管理器。
- `cli`：围绕 Python API 的轻量命令行封装。

## 存储区域

打开数据湖会创建：

```text
data/
    lake/
    staging/
    rejected/
    metadata/
        lake.db
    tmp/
```

`data/lake` 保存经过校验的标准 Parquet 文件。只有这里的数据会暴露给 `lake.query` 和 `lake.finance`。

`data/staging` 保存摄取过程中的临时源响应。暂存文件可以很小，因为它们不是分析存储格式。提交成功后，对应 run 的暂存片段会被删除。

`data/rejected` 保存不能安全进入标准数据湖的记录，例如缺少标准标识、日期无效或源响应结构异常。

`data/metadata/lake.db` 是 SQLite 运行状态。它保存已注册数据源、数据集、摄取 run、分区 manifest、拒收汇总、资产状态和分区锁。元数据存储启用 WAL 模式。

`data/tmp` 预留给未来的构建片段、压缩、校验和分区替换任务。

## 标准记录

标准 Parquet 记录是行式结构。它们可以包含源响应中的多个字段，也可以包含标准化阶段新增的标准字段。

所有非引用数据集必须包含：

- `asset_id`：标准资产标识。
- `time`：观察时间或信息可获得时间。

Point-in-time 财务数据还使用：

- `period`：该记录代表的经济或会计期间。

运行字段可能包括：

- `source`
- `source_dataset`
- `ingested_at`
- `record_hash`

原始源字段应尽量保留。例如财务报表记录可以同时保留 `ann_date`、`f_ann_date`、`end_date`，并暴露标准 `time` 和 `period`。

## 公开输出契约

标准存储和公开研究输出不是同一件事。

`lake.query.raw(...)` 返回行式标准记录，适合检查、调试和高级工作流。

`lake.query.field(...)` 每次返回一个研究字段，形式是长面板：

```text
time | asset_id | requested_value_column
```

API 不会 pivot 成宽表。多字段请求返回多个独立长面板：

```python
ohlcv = lake.query.fields(
    "daily",
    ["open", "high", "low", "close", "vol"],
    source="tushare",
)
```

字典中的每个 frame 都只有三列。

## Point-In-Time 语义

`time` 和 `period` 必须区分。

`time` 表示研究者可以获得该信息的时间。

`period` 表示该记录代表的经济或会计期间。

对日频市场数据来说，`time` 通常是交易日。对财务报表来说，`time` 通常是公告日或最终公告日，`period` 是报告期截止日。

财务 API 绝不能在早于标准 `time` 的观察日暴露财务记录。

## 分区

分区由数据集规格选择。初始内置策略包括：

- `single_file`：一个数据集一个标准文件。
- `year_month`：`year=YYYY/month=MM/data.parquet`。
- `year_bucket`：`year=YYYY/bucket=BB/data.parquet`，bucket 来自 `asset_id` 的稳定哈希。

稳定 bucket 使用 Blake2b，而不是 Python 内置 `hash()`，因为 Python hash 会按进程随机化。

## 元数据 Manifest

每次标准分区写入都会更新 SQLite manifest：

- source
- dataset
- partition path
- partition values
- row count
- file size
- 最小和最大 `time`
- content hash
- schema hash
- update time

普通状态查询读取 manifest，避免扫描所有 Parquet 文件。

## 原子写入

标准 Parquet 替换遵循：

1. 在同一文件系统写临时文件。
2. 用 Polars 读回。
3. 校验行数。
4. 原子替换目标路径。
5. 更新分区 manifest。

这样可以避免未完成文件被查询 API 看到。
