from __future__ import annotations

import bagelquant_data
from bagelquant_data import DataLake, DatasetSpec, TushareSource


def test_core_public_imports_are_new_facade() -> None:
    assert bagelquant_data.DataLake is DataLake
    assert bagelquant_data.DatasetSpec is DatasetSpec
    assert bagelquant_data.TushareSource is TushareSource


def test_legacy_top_level_exports_are_removed() -> None:
    removed = {
        "LocalDataLake",
        "DataLakeManager",
        "TushareTableUpdateSpec",
        "Loader",
        "RetrievedPanel",
    }

    for name in removed:
        assert not hasattr(bagelquant_data, name)


def test_update_backfill_api_is_removed(tmp_path) -> None:
    lake = DataLake.open(tmp_path)

    assert not hasattr(lake.update, "backfill")
