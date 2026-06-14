"""Unified data access interfaces for BagelQuant."""

from bagelquant_data.datasource.base import DataRequest, DataSource
from bagelquant_data.datasource.registry import DataSourceRegistry, default_registry
from bagelquant_data.lake import (
    DataLakeManager,
    LocalDataLake,
    TushareTableUpdateSpec,
    TushareTradingCalendarRef,
    TushareUniverseRef,
    TushareUpdateJob,
    TushareUpdatePlan,
    TushareUpdateReport,
)
from bagelquant_data.loader.loader import LoadedDataset, Loader, RetrievedPanel
from bagelquant_data.metadata.contract import DataContract
from bagelquant_data.metadata.schema import DatasetSchema, FieldSchema

__all__ = [
    "DataContract",
    "DataLakeManager",
    "DataRequest",
    "DataSource",
    "DataSourceRegistry",
    "DatasetSchema",
    "FieldSchema",
    "LoadedDataset",
    "Loader",
    "LocalDataLake",
    "RetrievedPanel",
    "TushareTableUpdateSpec",
    "TushareTradingCalendarRef",
    "TushareUniverseRef",
    "TushareUpdateJob",
    "TushareUpdatePlan",
    "TushareUpdateReport",
    "default_registry",
]
