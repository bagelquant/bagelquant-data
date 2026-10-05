# PIT 读取和冻结输入

```python
history = lake.raw.read("daily", source="tushare")
fixed = lake.raw.read("daily", source="tushare", as_of="2020-01-10",
                      observation_start="2020-01-01", observation_end="2020-01-05")
current = lake.raw.read("daily", source="tushare", view="latest")
versions = lake.raw.read_versions("daily", source="tushare")
item = lake.items.read("close")
snapshot = lake.items.read("close", view="snapshot", as_of="2020-01-10")
```

按日期 Raw 与 DataItem 默认逐观测日选择当时可见版本。固定 as_of 是独立信息截止日；
读取窗口终点不能替代它。Raw start/end 过滤可用轴，observation_start/end 过滤观测轴。
`raw.observations` 在 PIT 选择后恢复观测 time 轴。General 读取完整快照，可给 as_of，
没有隐含的逐日坐标网格。`strict=True` 排除不可验证的历史基线；后续相同值只能证明
较晚可用性。普通 LazyFrame 在创建时固定已提交 generation，延迟 collect 不换到新版本。

```python
from bagelquant_data import RawInput, ItemInput, input_read_boundary

receipt = lake.inputs.freeze({"raw": RawInput("tushare", "daily"),
                             "close": ItemInput("close")},
                             information_cutoff="2020-01-10")
print(lake.inputs.read(receipt, "close").collect())
print(lake.inputs.verify(receipt))
with input_read_boundary(lake.data_meta_path, receipt.max_commit,
                         receipt.information_cutoff, max_check_id=receipt.max_check_id):
    reader = DataLake.open(data_meta_path=lake.data_meta_path,
                           lake_path=lake.lake_path, read_only=True)
```

冻结原子记录依赖、定义、schema、恢复批次、信息截止日、commit 与 check 上界。
读冻结输入校验本地不可变证据，后续修订、注销或当前文件损坏不改变重现。
receipt ID 用 `inputs.get` 重开；嵌套边界可收窄，明确扩大时拒绝。读取不触发 Provider。
