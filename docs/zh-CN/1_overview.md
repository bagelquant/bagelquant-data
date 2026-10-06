# 概览

Data 是独立 Python package，负责中性的 Raw、DataItem、分类、schema、版本、覆盖、
PIT、冻结输入和本地恢复。公开入口为 `lake.catalog/raw/items/integrity/inputs`，
探索函数位于 `bagelquant_data.exploration`，不依赖 Core、BT 或 Workbench。

`DataLake.open(*, data_meta_path, lake_path, read_only=False)` 要求调用者明确提供两个路径。
`data_meta_path` 是唯一的 Data SQLite 文件，保存所有元数据、任务状态、冻结记录和压缩
Arrow 恢复批次。`lake_path` 保存 `raw/<source>/<dataset>` 与 `items/<name>` 下的
year/month 不可变 Parquet generation。文件引用使用相对路径并核对元数据库与 lake 的关系。
只读打开不创建目录、初始化数据库或自动恢复状态。

`DataLake.inspect(*, data_meta_path, lake_path)` 返回 `status`（`ready`、
`uninitialized`、`incompatible`）、`reason` 和 `schema_version`，检查已提交的
schema 和 lake 绑定，不改变原目录、文件或 SQLite sidecar。存在 WAL 时，在系统
临时目录读取稳定的元数据库/WAL 快照，包含已提交但尚未 checkpoint 的变更；
只读取 immutable 主文件会漏掉这些变更。普通只读查询保留 SQLite 的 WAL 协调，
可能创建或更新 WAL/SHM sidecar，但不修改权威数据。
存在有效的活动或崩溃遗留 rollback journal 时，探针返回不可读且不恢复；未提交但
已 spill 到主文件的页面不能证明就绪。PERSIST 模式已失效的零头 journal 允许读取。

Raw 只有 `general` 完整快照和 `by_date` 日期增量两类。DataItem 是标准长表：
`time: Date, asset_id: String, value: 声明的标量类型`。选定版本后键唯一、排序确定。
分类名称和路径不决定对象身份或文件位置。

Data 默认串行，执行调用者传入的 worker、在途任务、批次和缓冲限制；Workbench
负责硬件探测、全局 scheduler、任务准入、runtime policy 和原生线程预算。
Core 通过公开内存计算接口生成中性结果，由 Data 发布一次；Core/BT 的计算和结果
artifact 仍由各自管理。真实数据库与服务切换留在后续经授权的阶段。

Package 0.7 使用不兼容 schema 6。旧库直接拒绝，没有别名、迁移或自动历史清理。
继续阅读 [快速开始](2_quickstart.md)、[数据集](3_datasets.md)、[数据源](4_sources.md)、
[更新](5_updates.md)、[读取](6_queries.md)、[运维](7_operations.md)、
[DataItem](8_items.md) 和 [探索](9_exploration.md)。
