# DataItem 与外部 producer

DataItem 的 `(time, asset_id)` 唯一且排序确定，value 声明 float/integer/boolean/string/
date/datetime/categorical 标量类型。定义、依赖和 producer revision 保存在 Data SQLite。

```python
from bagelquant_data import DataItemSpec, RawInput, Select, Cast

lake.items.register(DataItemSpec(
    "close", (RawInput("tushare", "daily"),), value_dtype="float64",
    time_column="source_time", value_column="close",
    transforms=(Select(("source_time", "asset_id", "close")), Cast({"close": "float64"})),
))
lake.items.initialize("close", end="2020-12-31")
lake.items.update("close", end="2021-01-15")
```

内置 Select/Filter/MapValues/Cast/显式键 Join/Align。重复输出键报错。坐标由调用者提供，
默认缺失 null；向前填充必须声明有限 `forward_fill_sessions`，遵守可用日和显式坐标日期，
后来的 null 事件终止之前的值。Data 不推断中国日历或全体股票分母。

复杂计算通过 `items.register_producer(key, revision, callable)` 注册，DataItemSpec
同时声明 producer_key/revision 和依赖。BuildContext 提供冻结 frames、起止日、信息截止日
及 commit；producer 返回中性 Polars 结果，Data 不导入 Core。重开后定义保留，执行时重新
注册 callable。依赖 receipt 控制失效，未知影响范围保守重建；移除坐标发布 tombstone，
保留历史。外部直接结果用 `items.ingest`，共用同一个提交和恢复协议。

声明依赖的直接结果必须传 `inputs.freeze` 生成的 `input_receipt`；Data 校验依赖身份并将
receipt 写入提交证据。`expected_definition_hash` 可拒绝计算期间更改的定义。未明确声明
可用日期的外部结果在采集时才可见；历史基线必须显式标记，严格读取会排除。
完整范围用 `items.replace_range(name, frame, start=..., end=..., available_date=...,
input_receipt=...)` 发布；缺少的坐标新增 null 事件，已冻结历史保持可重现。

首次直接发布的 Panel 如果坐标唯一、时点证明一致，会跨月分区一次原子提交，并保留
每行可用日期。同坐标修订、已有内容复查和混合基线证明仍按可用日期分别发布。
完整范围的 receipt 在输出为空时也保留调用者声明的起止日。

`items.builds(name)` 按发布顺序提供成功构建的定义、依赖摘要、范围和冻结输入引用，
注销后仍可读取。`inputs.is_current(receipt)` 在原有窗口及信息截止日下检查当前已提交
证据，不创建新 receipt；Data 递归检查最新选定的 Item 上游证明，保留旧证明不会导致
已重建结果永远失效。已验证内容的同值复查保持 currentness。`inputs.verify` 单独校验
保留的数据字节；新鲜度检查不替代完整性验证。

空的派生输入仍保留冻结的上游时点证明。下游数量等标量结果在所选上游证据于该截止日
得到验证前仍属于历史基线。后来的可用性证明不能越过更早的信息日期或已捕获的
commit/check 上界。已验证输入根据不可变时点标记跳过重复的逐日前缀选择。
