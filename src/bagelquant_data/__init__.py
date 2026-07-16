"""Source-agnostic data lake framework for BagelQuant research."""

from bagelquant_data.core import (
    BagelQuantDataError,
    DataSource,
    DatasetNotFoundError,
    DatasetSpec,
    DatasetSpecError,
    DuplicateResolutionError,
    SourceNotFoundError,
    StaleUpdatePlanError,
    ValidationError,
    stable_bucket,
)
from bagelquant_data.management import DataLake, LakeAdmin, LakeUpdater
from bagelquant_data.pipeline import (
    CoverageSummary,
    CoverageYearSummary,
    PartitionChange,
    UpdatePlan,
    UpdateProgress,
)
from bagelquant_data.query import LakeQuery
from bagelquant_data.sources.tushare import TushareSource

__all__ = [
    "BagelQuantDataError",
    "CoverageSummary",
    "CoverageYearSummary",
    "DataLake",
    "DataSource",
    "DatasetNotFoundError",
    "DatasetSpec",
    "DatasetSpecError",
    "DuplicateResolutionError",
    "LakeAdmin",
    "LakeQuery",
    "LakeUpdater",
    "PartitionChange",
    "SourceNotFoundError",
    "StaleUpdatePlanError",
    "TushareSource",
    "UpdatePlan",
    "UpdateProgress",
    "ValidationError",
    "stable_bucket",
]
