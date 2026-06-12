# Retrieved Panels

`RetrievedPanel` 是数据层的中性结果对象。它不是 core 适配器，也不会导入或构造 `bagelquant-core` 对象。

它包含：

- `kind`: `numeric_panel` 或 `category_panel`
- `data`: pandas DataFrame
- `universe`: 静态资产序列或动态成员 DataFrame
- `calendar`: 有序 pandas DatetimeIndex
- `dataset_name`: 稳定输入名
- `metadata`: provider、request、lineage、field 和 calendar 元数据

下游代码可以显式使用这些普通对象：

```python
from bagelquant_core import Domain, Panel

domain = Domain(calendar=retrieved.calendar, universe=retrieved.universe)
panel = Panel.from_domain(
    retrieved.data,
    domain,
    name=retrieved.dataset_name,
    metadata=retrieved.metadata,
)
```

这种方式保持单向依赖，让 core 继续负责 Panel 语义。
