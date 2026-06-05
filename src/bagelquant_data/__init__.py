"""Unified data access interfaces for BagelQuant."""

from bagelquant_data.datasource.base import DataRequest, DataSource
from bagelquant_data.datasource.registry import DataSourceRegistry, default_registry
from bagelquant_data.lake import (
    DataLakeManager,
    LocalDataLake,
    TushareTableUpdateSpec,
)
from bagelquant_data.loader.loader import LoadedDataset, Loader, PanelInputAgreement
from bagelquant_data.metadata.contract import DataContract, DomainSpec
from bagelquant_data.metadata.schema import DatasetSchema, FieldSchema
from bagelquant_data.transform.pipeline import Transform

__all__ = [
    "DataContract",
    "DataLakeManager",
    "DataRequest",
    "DataSource",
    "DataSourceRegistry",
    "DatasetSchema",
    "DomainSpec",
    "FieldSchema",
    "LoadedDataset",
    "Loader",
    "LocalDataLake",
    "PanelInputAgreement",
    "Transform",
    "TushareTableUpdateSpec",
    "default_registry",
]
