"""Local lake and Tushare update interfaces."""

from bagelquant_data.lake.local import LocalDataLake
from bagelquant_data.lake.manager import DataLakeManager
from bagelquant_data.lake.snapshot import SnapshotRef
from bagelquant_data.lake.tushare_update import (
    TushareTableUpdateSpec,
    TushareTradingCalendarRef,
    TushareUniverseRef,
    TushareUpdateJob,
    TushareUpdatePlan,
    TushareUpdateReport,
)

__all__ = [
    "DataLakeManager",
    "LocalDataLake",
    "SnapshotRef",
    "TushareTableUpdateSpec",
    "TushareTradingCalendarRef",
    "TushareUniverseRef",
    "TushareUpdateJob",
    "TushareUpdatePlan",
    "TushareUpdateReport",
]
