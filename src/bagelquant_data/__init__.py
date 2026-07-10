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
from bagelquant_data.finance import FinancialFieldKind, FinancialFieldSpec
from bagelquant_data.management import DataLake, LakeAdmin, LakeUpdater
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
    "FinancialFieldKind",
    "FinancialFieldSpec",
    "LakeAdmin",
    "LakeQuery",
    "LakeUpdater",
    "SourceNotFoundError",
    "TushareSource",
    "ValidationError",
    "stable_bucket",
]
