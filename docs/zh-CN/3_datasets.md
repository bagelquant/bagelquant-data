# 数据集声明

声明统一描述 Provider API、日期参数、源日期字段、业务主键、交易日或自然日、固定参数、参数展开与分页规则。

`date_kind="trading"` 必须声明 General 日历；`date_kind="calendar"` 包括周末。多个 `date_params` 会形成独立的日期 × 参数 scope。`source_time_fields` 按顺序选择首个非空源日期，并保留供应商原始字段。业务键由 `source_time`、`asset_id` 和 `primary_key_extra` 组成；版本顺序另由入库时间和提交序号确定。

`source_api_param_sets` 对列表值做笛卡尔展开；`parameter_dataset`、`parameter_field` 和 `parameter_name` 可以从已登记的 General 目录生成未来请求参数，但不会创建资产级水位。TOML 和表单生成的定义进入同一个校验器。

旧的按资产更新类型、年/桶分区与修订水位参数均不受支持。
