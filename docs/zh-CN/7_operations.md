# 完整性与恢复

```python
facts = lake.integrity.scan("daily", source="tushare", deep=True)
plan = lake.integrity.repair_plan("daily", source="tushare")
results = lake.integrity.repair(plan)
```

扫描只报告 manifest/schema/键/哈希/分区/恢复事实，不修改状态，不接管孤立文件，不调用
Provider。浅扫描比较登记文件，深扫描校验内容及 Arrow 恢复证据。修复计划固定 lake 身份
和 manifest；过期或不同 lake 的计划失败，执行期间持有数据集更新 lease。

唯一 Data SQLite 决定提交可见性。先准备其中的压缩 Arrow 批次及不可变 Parquet 文件，
再通过 SQLite 事务一起发布 manifest、schema、commit 和 coverage。未登记文件不可见，
旧的已登记 generation 作为历史证据保留。

本地修复必须重现原始哈希、schema、可用日期和身份。完整恢复批次可以恢复损坏 Parquet；
完整已校验 Parquet 只有能重现每个原始批次时才可恢复其 payload。证据不足明确失败。
全部元数据库丢失必须恢复备份，不能从散落文件或当前 Provider 响应推造历史。
没有自动历史清理、quarantine、孤立 manifest 接管或旧 schema 迁移。

`integrity` 提供运行、失败、scope、coverage、lease 状态；`raw.status/status_many` 与
`items.status` 提供对象概况。重置 scope、放弃失效 owner 都是显式操作，与扫描分开。
所有验收使用临时根和假 Provider，不访问真实工作区数据。
心跳过期只报告事实，不自动释放写入所有权。调用者确认 owner 已终止后才应显式调用
`abandon_update_owner`。发布事务同时校验 run 所有权、父 generation 和当前定义。

`integrity.backup(data_meta_path=..., lake_path=...)` 将一致的 SQLite 快照和已提交文件
导出到新的调用者路径，并返回校验后的文件哈希。`integrity.verify_backup()` 校验备份。
`DataLake.restore(backup_data_meta_path=..., backup_lake_path=...,
data_meta_path=..., lake_path=...)` 将同 schema 备份恢复到新路径；已有目标会被拒绝。
完整历史和冻结输入保留在单个元数据库中，旧物理文件可按登记 Arrow 批次重现。
