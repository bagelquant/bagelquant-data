# 数据源

Provider 实现 `name/configure/test_connection/fetch(dataset, request)`，每次 fetch
返回 Polars DataFrame。通过 `lake.catalog.sources.register/configure` 接入。
读已保存数据不会触发网络请求。凭据只由运行配置提供，持久化配置对其做脱敏。

```python
from bagelquant_data import TushareSource

lake.catalog.sources.register(TushareSource())
lake.catalog.sources.configure("tushare", token="runtime-secret")
```

数据源支持 list/get/register/configure/enable/disable/remove/test，有活动数据集或分类时
不可删除。注册 adapter 是本地操作，test 和 update 才执行显式外部调用。测试使用假 provider。
中国市场字段、日历和可用性约定由 Workbench 声明，Data 默认中性 UTC，不推断交易所语义。
