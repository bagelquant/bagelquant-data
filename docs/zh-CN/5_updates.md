# 更新和执行

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
日增量默认重查最近三个自然日，较早终态需要显式刷新、重置或定义变化。
覆盖以声明的日期×参数 scope 为准，不以行密度推断。有效空响应持久化；全空非键值默认
无效，稀疏事件可明确声明 `allow_all_null_payload=True`。

Raw 区分 `source_time` 观测日、`time` 可用日、UTC `ingested_at`、内容修订和 commit。
当地 `availability_cutoff_time` 是排他边界，到达后先将收集日期推进一天再应用 offset；
负 offset 必须声明 cutoff。不变内容的后续收集保存新的可用性 attestation，不重复内容
generation，也不能伪造更早发布时间。

抓取、校验和分区准备共用一个显式 worker pool，SQLite 发布串行。
默认一个 worker、一个在途请求、64 MiB 缓冲。Data 不探测硬件，不决定全局准入。
自身响应队列和发布批次共享给定缓冲；超大响应明确失败，general 完整快照必须能容纳。
Provider 或原生库内部内存由调用者另行预算。`progress_callback/cancel_check` 提供进度和取消，
取消保留已提交工作，未完成 scope 可续跑。Workbench 决定 worker 与原生线程总预算。
