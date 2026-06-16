# 增量更新

更新 API 位于 `lake.update`。

```python
lake.update.dataset("daily", source="tushare")
lake.update.datasets(["daily", "daily_basic"], source="tushare")
lake.update.source("tushare")
```

## 更新流程

V1 更新流程：

```text
request planning
-> fetch
-> staging
-> normalization
-> validation
-> deduplication
-> partition write
-> manifest update
-> staging cleanup
```

数据集行为由 `DatasetSpec` 选择，而不是由核心 pipeline 写死分支。

## 请求上下文

公开更新 API 接收：

- `start`
- `end`
- `assets`
- `workers`
- `batch_size`
- `source_options`
- `progress`
- `max_retries`
- `retry_backoff_seconds`

这些值会通过 `RequestContext` 传递。数据源适配器可以据此规划请求。
API 获取会按请求或分页并行执行，但每个数据集的标准 Parquet 提交仍保持串行。

示例：

```python
lake.update.dataset(
    "income",
    source="tushare",
    assets=["000001.SZ", "600000.SH"],
    start="2020-01-01",
    end="2026-06-15",
    batch_size=250,
)
```

对于 offset 分页的 API，可在数据集规格中加入：

```yaml
request_options:
  pagination: offset
  page_size: 5000
  limit_param: limit
  offset_param: offset
  offset_start: 0
```

每个实际分页请求都会独立重试，并计入进度和失败元数据。

## 暂存

获取到的源响应写入 `data/staging`。暂存区保留源响应边界，同时 pipeline 进行标准化和提交。

提交成功后，对应 run 的暂存片段会被删除。

## 标准提交

提交步骤：

1. 将源记录标准化为标准字段。
2. 添加分区派生列。
3. 执行框架校验。
4. 应用数据集去重策略。
5. 按数据集排序规则排序。
6. 写入受影响的标准 Parquet 文件。
7. 更新 SQLite manifest。

## 当前 V1 行为

当前实现提供新的 API 和存储基础，支持标准写入和 manifest 驱动状态查询。更高级的增量优化是后续目标：

- 资产级 content hash 跳过
- 微批分区合并
- 并发写入的分区锁
- manifest 重建工具
- 文件压缩阈值和修复操作
- 更丰富的数据集校验器

现有 API 已为这些能力预留空间，不需要改变用户侧方法名。

## 更新后状态

```python
print(lake.status.dataset("daily", source="tushare"))
print(lake.status.runs(limit=20))
```

状态默认读取 SQLite 元数据和 manifest。
