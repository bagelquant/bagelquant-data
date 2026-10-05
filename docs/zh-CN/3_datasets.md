# 数据集和分类

通过 `lake.raw.register(DatasetSpec(...))` 声明稳定 source/name、更新类型、字段映射、
额外主键、日期和参数范围。`by_date` 必须明确日期与 asset_id 映射；自然日使用
`date_kind="calendar"`，交易日必须声明日历数据集。参数 variant 与传输分页分开。

`general` 每次显式更新完整拉取，变化时新增完整快照（含空快照），不变时记录检查。
部分参数或分页失败不会替换上一完整快照。`by_date` 逐日保存成功或有效空响应的覆盖；
空响应不删除此前已经观察到的记录。

```python
tree = lake.catalog.raw_categories("tushare")
equity = tree.create("equity")
market = tree.create("market", parent_id=equity["id"])
tree.assign("daily", market["id"])
tree.rename(market["id"], "prices")
tree.move(market["id"], parent_id=None)
```

每个 source 有独立 Raw 分类树，`lake.catalog.item_categories` 是独立 DataItem 树。
支持 list/get/create/rename/move/remove/assign/members，禁止循环、同级重复名称和非空删除。
移动分类不移动文件。`raw.remove/items.remove` 只有在无活动依赖时注销对象；保留提交历史、
恢复证据和冻结引用，不自动清理。TOML 声明通过 `raw.register_toml/register_toml_text` 注册。
