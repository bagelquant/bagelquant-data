# 更新和执行

`batch_size` 的单位是逻辑 scope，`commit_batch_rows` 的单位是行；已完成的
Raw 缓冲还会每 `commit_interval_seconds`（默认30秒）提交。完整 general
快照及未完成的初始化全量清单不会提前发布。schema 7 保存 Provider 请求、
限流/重试等待、准备、scope 领取、提交、丢弃行数及内存/分区峰值等指标。

逻辑覆盖仍然逐日。`daily_range_backfill.window="calendar_month"` 只在同月
聚合传输，并完整 offset 分页。显式 `raw.refresh` 也可用同一按月传输重查历史，
保留旧的已提交版本；普通增量重查仍逐日。分页大小不得超过端点实际返回限额。
`initialization_scan` 可按参数组抓取完整历史，
完成全部分页后按声明的源日期筛选、拆分每日结果。调用者提供 parameter_values，
或 parameter_dataset/parameter_field 的完整本地目录；target_param、cohort_size、
separator 指定传输格式。不能用 ann_date 月份直接证明 f_ann_date 每日覆盖。
分页重复、股票组越界、内存超限和取消均不会提前宣布补齐成功。

初始化响应被静默截断时，可用 `integrity.reopen_raw_initialization` 在原范围和
原定义 hash 下提供明确原因，重新打开完成状态。存在增量/refresh 提交、已验证
检查、任何 lease（包括过期）、未完成运行/scope 或 prepared 版本时会拒绝。
该操作返回审计回执，保留旧版本与冻结输入，再用 `raw.initialize` 续跑。
补回历史仍是未验证 baseline，不能冒充原始首发证据；按月传输也支持这些重查。

`DataLake.inspect(runtime=True)` 和 `DataLake.open(runtime=True)` 使用正常 WAL
协调读取已提交 schema，避免复制大库；默认严格预检仍保持原数据库及 sidecar 不变。

```python
from bagelquant_data import ExecutionOptions

limits = ExecutionOptions(workers=2, max_in_flight=2, batch_size=16,
                          max_buffer_bytes=64 * 1024 * 1024)
lake.raw.initialize("daily", source="tushare", end="2020-12-31", config=limits)
lake.raw.update("daily", source="tushare", end="2021-01-15", config=limits)
lake.raw.refresh("daily", source="tushare", start="2020-12-01",
                 end="2020-12-31", config=limits)
```

初始化默认从 `2000-01-01` 开始，终点必须提供。固定范围和定义版本后可中断续跑，
改变范围或定义会失败。无法证明原始修订历史的数据标记历史基线。
只读 `raw.plan_updates(datasets, source=..., start=..., end=...)` 返回按日历/参数依赖
排序的 `source/dataset/mode/start/end` 动作；`items.plan_update(name, start=..., end=...)`
返回对应 DataItem 动作。新对象初始化，中断初始化先按原范围续跑，再增量至新的终点；
已完成或已有提交历史的对象走增量。有效空初始化也视为完成。计划不抓取、不发布，
未完成初始化的定义/起点变化或终点倒退会明确失败。

日增量默认重查最近三个自然日，较早终态需要显式刷新、重置或定义变化。
覆盖以声明的日期×参数 scope 为准，不以行密度推断。有效空响应持久化；全空非键值默认
无效，稀疏事件可明确声明 `allow_all_null_payload=True`。

Raw 区分 `source_time` 观测日、`time` 可用日、UTC `ingested_at`、内容修订和 commit。
当地 `availability_cutoff_time` 是排他边界，到达后先将收集日期推进一天再应用 offset；
负 offset 必须声明 cutoff。不变内容的后续收集保存新的可用性 attestation，不重复内容
generation，也不能伪造更早发布时间。

抓取、校验和分区准备共用一个显式 worker pool，SQLite 发布串行。
默认一个 worker、一个在途请求、64 MiB 缓冲。Data 不探测硬件，不决定全局准入。
自身响应队列和发布批次共享给定缓冲。原生初始化范围批次超限时丢弃该批缓存，
将已认领的每日 scope 拆小后在相同预算内重试；单日仍保留范围截断检查和分页保护。
丢弃的父批次只计实际请求，不推进覆盖。单日或 general 完整快照仍超限时明确失败。
Provider 或原生库内部内存由调用者另行预算。`progress_callback/cancel_check` 提供进度和取消，
取消保留已提交工作，未完成 scope 可续跑。Workbench 决定 worker 与原生线程总预算。

要求非空的单日请求及其首个offset分页，会在现有物理调用重试预算内重试临时空响应；持续空响应仍判为无效。允许为空的稀疏请求和分页结束空页维持原有语义。
