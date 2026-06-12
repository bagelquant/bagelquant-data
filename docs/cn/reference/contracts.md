# 数据契约

数据契约用于保证数据集可复现。

## DatasetIdentity

通过名称、provider、版本和可选快照识别一个逻辑数据集。

## DatasetSchema

独立于物理存储声明字段和主键。

## DataContract

组合 identity、schema、owner、freshness 和 metadata，形成完整的数据契约。

## LineageRecord

记录来源、操作、参数和创建时间，帮助追踪数据如何产生。
