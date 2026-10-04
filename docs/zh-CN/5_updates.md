# 更新、版本与恢复

`lake.update.dataset()` 与 `lake.update.datasets()` 支持 `initialize`、`incremental` 和 `refresh`。初始化必须指定冻结的起止范围，只能用于新数据集或同一未完成初始化；完成后不能再次回填历史可用时间。普通更新和刷新仅在内容变化时追加版本。

初始化以源日期建立历史基线。其后版本使用 `max(source_time, 本地入库日期 + availability_day_offset + 截止后顺延)`。可显式声明本地 `availability_cutoff_time`（`HH:MM:SS`）；入库时刻达到或超过截止时刻便顺延一个自然日。负偏移必须同时声明截止时刻。Workbench 采用次日开盘口径：上海时间 09:30 截止、偏移 −1；达到或晚于开盘收到的修订不会进入前一信号日。自然日由下游向后对齐交易日。`ingested_at` 始终使用 UTC，同一 PIT 日期按实际入库时间和提交序号排序。空响应不会删除旧记录，无变化检查不会创建版本。

不存在供应商回填的 `baseline_repair` 选项。缺少覆盖不能授权将新下载值追溯为旧基线。原样恢复只能重放已有且校验一致的恢复日志；重新访问供应商得到的修订按新版本保存。

覆盖单位是日期 × 参数 variant。自然日包含周末，交易日使用声明日历。普通更新补齐未完成范围，并重查目标日期之前最近三个自然日；更早历史需要显式 `refresh`。所有分页必须通过主键、日期、重复页与截断校验，scope 才能成为 `success` 或 `empty`。取消会保留已完成 scope。

每个可用月份包含 `data.parquet` 与 `recovery.sqlite`。提交顺序是先写不可变 Arrow 恢复批次，再发布 Parquet，最后提交 `lake.db` 中的 manifest、版本和覆盖。只有权威数据库已登记的提交可见。

供应商抓取、响应校验与分区写入共用一个由 `workers` 限制的线程池（默认四个线程），不另外创建写线程池。独立月份最多同时提交四个分区任务；`lake.db` 与覆盖发布仍由调度线程统一执行。失败时先等待所有已提交写任务结束，再集中回滚，避免迟到的写入重新覆盖旧结果。报告中的 `bytes_read` 统计旧规范分区读取量，`bytes_written` 统计写入量，`peak_partition_in_flight` 统计排队及执行中的分区任务峰值；这些操作计数不参与内容身份。

可运行 `uv run python scripts/benchmark_updates.py --workers 1`，再以 `2`、`4`、`8` 重复，比较固定数据规模及原生线程配置下的总耗时、提交耗时、分区任务数与读写字节。基准只使用临时数据湖和模拟供应商。

```python
lake.admin.recovery_status("income", source="tushare", deep=True)
lake.admin.repair_partitions(
    "income",
    source="tushare",
    partitions=["year=2026/month=09/data.parquet"],
)
```

本地修复只能重放已提交证据。若恢复日志与 Parquet 都不能证明原批次，系统明确阻止历史恢复；不会用 Provider 最新值冒充丢失的旧版本。
