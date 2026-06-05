"""Data lake interfaces."""

from bagelquant_data.lake.base import LakeStore
from bagelquant_data.lake.catalog import LakeCatalog
from bagelquant_data.lake.local import LocalDataLake
from bagelquant_data.lake.manager import DataLakeManager
from bagelquant_data.lake.partition import PartitionSpec
from bagelquant_data.lake.reader import LakeReader
from bagelquant_data.lake.snapshot import SnapshotRef
from bagelquant_data.lake.tushare_update import (
    TushareTableUpdateSpec,
    TushareTradingCalendarRef,
    TushareUniverseRef,
    TushareUpdateJob,
    TushareUpdatePlan,
    TushareUpdateReport,
)
from bagelquant_data.lake.writer import LakeWriter

__all__ = [
    "DataLakeManager",
    "LakeCatalog",
    "LakeReader",
    "LakeStore",
    "LakeWriter",
    "LocalDataLake",
    "PartitionSpec",
    "SnapshotRef",
    "TushareTableUpdateSpec",
    "TushareTradingCalendarRef",
    "TushareUniverseRef",
    "TushareUpdateJob",
    "TushareUpdatePlan",
    "TushareUpdateReport",
]
