"""Metadata contracts."""

from bagelquant_data.metadata.catalog import InMemoryMetadataCatalog
from bagelquant_data.metadata.contract import (
    DataContract,
    DatasetIdentity,
    DomainSpec,
    PanelKind,
)
from bagelquant_data.metadata.lineage import LineageRecord
from bagelquant_data.metadata.schema import DatasetSchema, FieldSchema

__all__ = [
    "DataContract",
    "DatasetIdentity",
    "DatasetSchema",
    "DomainSpec",
    "FieldSchema",
    "InMemoryMetadataCatalog",
    "LineageRecord",
    "PanelKind",
]
