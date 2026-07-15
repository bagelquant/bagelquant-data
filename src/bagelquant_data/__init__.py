"""Source-agnostic data lake framework for BagelQuant research."""

from bagelquant_data.core import (
    BagelQuantDataError,
    DataSource,
    DatasetNotFoundError,
    DatasetSpec,
    DatasetSpecError,
    DuplicateResolutionError,
    SourceNotFoundError,
    ValidationError,
    stable_bucket,
)
from bagelquant_data.management import DataLake, LakeAdmin, LakeUpdater
from bagelquant_data.pipeline import UpdateProgress
from bagelquant_data.query import LakeQuery
from bagelquant_data.sources.tushare import TushareSource

__all__ = [
    "BagelQuantDataError",
    "DataLake",
    "DataSource",
    "DatasetNotFoundError",
    "DatasetSpec",
    "DatasetSpecError",
    "DuplicateResolutionError",
    "LakeAdmin",
    "LakeQuery",
    "LakeUpdater",
    "SourceNotFoundError",
    "TushareSource",
    "UpdateProgress",
    "ValidationError",
    "stable_bucket",
]
