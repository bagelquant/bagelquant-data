# PIT 查询

```python
latest = lake.query.query(
    "daily", source="tushare",
    observation_start="2026-01-01", observation_end="2026-01-31",
)
known = lake.query.query(
    "daily", source="tushare", as_of_date="2026-09-09",
    observation_start="2026-01-01", observation_end="2026-01-31",
)
versions = lake.query.query("daily", source="tushare", view="versions")
observations = lake.query.observations(
    "daily", source="tushare", start="2026-01-01",
)
```

`source_time` 是观测或公告日期，`time` 是版本可用日期。`start`/`end` 过滤可用日期；`observation_start`/`observation_end` 独立过滤源日期。`as_of_date` 先限制可见版本再选择业务键的最新值；`ingested_before` 接受带时区的实际入库截止时间。

`view="history"` 返回每个观测日在当时已知的版本；`observations()` 在版本选择后把普通数值 `time` 轴恢复为源观测日期。`lake.query.frozen()` 冻结一次计算能看到的最高提交序号，`version_evidence()` 返回不可变批次身份，无变化检查时间不参与依赖指纹。

General 使用 `query_general()`，默认读取最新完整快照，也支持 `as_of_date`、`snapshot_id`、`ingested_before` 和 `view="versions"`。`snapshots()` 会列出包括空快照在内的完整提交。
