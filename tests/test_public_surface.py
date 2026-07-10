import bagelquant_data

from bagelquant_data import DataLake, DatasetSpec, LakeAdmin, LakeQuery, LakeUpdater, TushareSource


def test_public_surface_is_limited_to_three_facades(tmp_path) -> None:
    lake = DataLake.open(tmp_path)

    assert isinstance(lake.admin, LakeAdmin)
    assert isinstance(lake.update, LakeUpdater)
    assert isinstance(lake.query, LakeQuery)
    assert not hasattr(lake, "sources")
    assert not hasattr(lake, "datasets")
    assert not hasattr(lake, "status")
    assert hasattr(lake.query, "query_general")
    assert hasattr(lake.query, "query")
    for removed in ("raw", "field", "fields", "records", "price", "fundamental", "reference"):
        assert not hasattr(lake.query, removed)
    assert not hasattr(bagelquant_data, "FinancialFieldSpec")
    assert DatasetSpec and TushareSource
