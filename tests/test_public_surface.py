from __future__ import annotations

import bagelquant_data
from bagelquant_data import DataLake, DatasetSpec, LakeAdmin, LakeQuery, LakeUpdater, TushareSource


def test_core_public_imports_are_new_facade() -> None:
    assert bagelquant_data.DataLake is DataLake
    assert bagelquant_data.DatasetSpec is DatasetSpec
    assert bagelquant_data.LakeAdmin is LakeAdmin
    assert bagelquant_data.LakeQuery is LakeQuery
    assert bagelquant_data.LakeUpdater is LakeUpdater
    assert bagelquant_data.TushareSource is TushareSource
