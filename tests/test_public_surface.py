from __future__ import annotations

import importlib

import pytest

import bagelquant_data
import bagelquant_data.lake as lake
from bagelquant_data.datasource import (
    DataRequest,
    DataSourceRegistry,
    TushareDataSource,
)
from bagelquant_data.lake import (
    DataLakeManager,
    LocalDataLake,
    TushareTableUpdateSpec,
)
from bagelquant_data.loader import Loader, RetrievedPanel


def test_core_public_imports_remain_available() -> None:
    assert bagelquant_data.LocalDataLake is LocalDataLake
    assert bagelquant_data.DataLakeManager is DataLakeManager
    assert bagelquant_data.TushareTableUpdateSpec is TushareTableUpdateSpec
    assert DataRequest
    assert DataSourceRegistry
    assert TushareDataSource
    assert Loader
    assert RetrievedPanel


def test_placeholder_lake_exports_are_removed() -> None:
    removed = {"LakeStore", "LakeCatalog", "LakeReader", "LakeWriter", "PartitionSpec"}

    assert removed.isdisjoint(set(lake.__all__))
    for name in removed:
        assert not hasattr(lake, name)


@pytest.mark.parametrize(
    "module",
    [
        "bagelquant_data.cache",
        "bagelquant_data.transform",
        "bagelquant_data.lake.reader",
        "bagelquant_data.lake.writer",
        "bagelquant_data.datasource.database",
    ],
)
def test_placeholder_modules_are_removed(module: str) -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(module)
