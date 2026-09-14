# 更新、版本与恢复

`lake.update.dataset()` 与 `lake.update.datasets()` 支持 `initialize`、`incremental` 和 `refresh`。初始化必须指定冻结的起止范围，只能用于新数据集或同一未完成初始化；完成后不能再次回填历史可用时间。普通更新和刷新仅在内容变化时追加版本。

初始化以源日期建立历史基线。其后版本使用 `max(source_time, 本地入库日期 + availability_day_offset)`；Workbench 配置为 Asia/Shanghai 与 −1 天。`ingested_at` 始终使用 UTC，同一 PIT 日期按实际入库时间和提交序号排序。空响应不会删除旧记录，无变化检查不会创建版本。

覆盖单位是日期 × 参数 variant。自然日包含周末，交易日使用声明日历。普通更新补齐未完成范围，并重查目标日期之前最近三个自然日；更早历史需要显式 `refresh`。所有分页必须通过主键、日期、重复页与截断校验，scope 才能成为 `success` 或 `empty`。取消会保留已完成 scope。

每个可用月份包含 `data.parquet` 与 `recovery.sqlite`。提交顺序是先写不可变 Arrow 恢复批次，再发布 Parquet，最后提交 `lake.db` 中的 manifest、版本和覆盖。只有权威数据库已登记的提交可见。

```python
lake.admin.recovery_status("income", source="tushare", deep=True)
lake.admin.repair_partitions(
    "income",
    source="tushare",
    partitions=["year=2026/month=09/data.parquet"],
)
```

本地修复只能重放已提交证据。若恢复日志与 Parquet 都不能证明原批次，系统明确阻止历史恢复；不会用 Provider 最新值冒充丢失的旧版本。
