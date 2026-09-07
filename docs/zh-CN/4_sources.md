# 数据源

数据源是为单次 provider 请求返回 Polars DataFrame 的 adapter。通过 `lake.admin.sources` 注册与配置。凭据应保存在环境变量或运行时配置中，不应写入数据集声明或提交文件。

Adapter 可选实现 `wait_for_request(dataset, cancel_requested=...) -> bool` 请求准入钩子。通用摄取 worker 在每次请求前调用；返回 false 时停止该请求。Tushare 通过它协调共享接口的 workers：收到每分钟配额错误后，共享冷却等待，并按低于返回配额的间隔继续请求。等待以短间隔检查取消；其他接口及已提交的 success/empty scopes 不受影响。

参见[英文接口示例](../en/4_sources.md)。
