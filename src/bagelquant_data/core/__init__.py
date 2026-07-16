"""Core source-agnostic framework primitives."""

from bagelquant_data.core.dataset import ASSET_BUCKET_COUNT, DatasetSpec, dataset_key, incremental_key
from bagelquant_data.core.exceptions import (
    BagelQuantDataError,
    ConfigurationError,
    DatasetNotFoundError,
    DatasetSpecError,
    DestructiveOperationError,
    DuplicateResolutionError,
    SourceNotFoundError,
    StaleUpdatePlanError,
    ValidationError,
)
from bagelquant_data.core.hashing import frame_content_hash, stable_bucket, stable_record_hash
from bagelquant_data.core.registry import FrameworkRegistries, Registry, default_registries
from bagelquant_data.core.request import RequestContext
from bagelquant_data.core.source import DataSource

__all__ = [
    "BagelQuantDataError",
    "ASSET_BUCKET_COUNT",
    "ConfigurationError",
    "DataSource",
    "DatasetNotFoundError",
    "DatasetSpec",
    "DatasetSpecError",
    "DestructiveOperationError",
    "DuplicateResolutionError",
    "FrameworkRegistries",
    "Registry",
    "RequestContext",
    "SourceNotFoundError",
    "StaleUpdatePlanError",
    "ValidationError",
    "default_registries",
    "dataset_key",
    "frame_content_hash",
    "incremental_key",
    "stable_bucket",
    "stable_record_hash",
]
