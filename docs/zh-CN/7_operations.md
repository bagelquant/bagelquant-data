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

## 存储用量与临时清理

```python
usage = lake.integrity.storage_usage()
plan = lake.integrity.temporary_cleanup_plan()
result = lake.integrity.cleanup_temporary(plan)
```

用量统计实际 lake 普通文件、Data 元数据库及 SQLite sidecar，硬链接仅计一次；区分当前与历史 generations、元数据/恢复、拒绝证据、已知临时文件和未知文件。`temporary_bytes` 包含受保护的活动临时文件，`reclaimable_temporary_bytes` 仅统计可清理候选。已注册与可用 Raw/DataItem 数量分开，可用指有非空已提交 manifest，不据此推断研究 Available Date。

清理计划固定 lake 身份、哈希、相对路径及文件内容身份。只有已注册月度目录中明确属于 Data 原子写入的临时名称可能入选；活动 writer、历史/已提交 generations、元数据与恢复证据、未知文件、符号链接及共享硬链接受保护。执行先取得数据集 writer leases，再完整复验全部候选；变更后拒绝旧计划。重复执行仅报告已不存在的候选，不扩大范围。读取用量和计划不删除文件、不释放 writer。

## 可携带声明批次

```python
payload = source.catalog.export_declarations()
plan = destination.catalog.plan_declaration_batch(payload)
if plan["valid"]:
    receipt = destination.catalog.apply_declaration_batch(
        plan, request_id="caller-owned-transfer-id",
        expected_revision=plan["expected_revision"],
    )
    retained = destination.catalog.declaration_batch_receipt(receipt["request_id"])
    verified = destination.catalog.verify_declaration_batch_receipt(receipt)
```

只读 JSON 快照 schema 为 `bagelquant.data.declarations.v1`，包含 source 描述、Raw/DataItem spec、启用/归档状态、分类树 UUID 与归属，以及语义 SHA256 revision。排除配置、凭据、运行对象、producer 实现、数据字节、提交和计算结果。Data 的自然身份仍为 Raw `(source,name)` 与 DataItem `name`；研究 UUID 和治理属于应用。

预检验证完整 prospective union，包括前向 DataItem 依赖、环、分类父节点/namespace、calendar/fan-out 依赖及有效归属。同内容声明跳过，自然键冲突禁止覆盖。分类 UUID 和 provider/item 独立空间保持不变。

提交重新校验绑定 lake 与 catalog revision，复用单对象注册内部路径，在一次 Data SQLite 事务中发布全部声明和不可变 receipt；失败整批回滚。同 request_id 与相同计划返回原 receipt，不同计划复用 ID 则拒绝。声明导入不执行下载、producer 或数据版本更新；调用者随后在运行时注册实现。receipt 校验区分保留证据 `valid` 与声明是否仍等于当前 catalog 的 `current`，无关新增声明不使它失效。

`items.status(name)["committed_definition_current"]` 比较每个当前 manifest 分区的最后提交物理 spec；仅注册新 producer revision 不宣称旧结果可用。此检查仅用已提交元数据，不扫描 Parquet。包根导出的 `RawAPI.spec_from_mapping()` 可在不创建 lake/数据库时验证声明。

`raw_categories(source).update(id,name=...,parent_id=...)` 与 item 分类 API 原子调整名称和父节点；拒绝环、跨 provider 父节点及非空删除。分类编辑不改变数据路径、版本或 receipt。

`integrity.backup(data_meta_path=..., lake_path=...)` 将一致的 SQLite 快照和已提交文件
导出到新的调用者路径，并返回校验后的文件哈希。`integrity.verify_backup()` 校验备份。
`DataLake.restore(backup_data_meta_path=..., backup_lake_path=...,
data_meta_path=..., lake_path=...)` 将同 schema 备份恢复到新路径；已有目标会被拒绝。
完整历史和冻结输入保留在单个元数据库中，旧物理文件可按登记 Arrow 批次重现。
