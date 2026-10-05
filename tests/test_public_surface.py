import bagelquant_data

from bagelquant_data import DataLake, DatasetSpec, TushareSource


def test_public_surface_has_one_facade_per_concern(tmp_path) -> None:
    lake = DataLake.open(data_meta_path=tmp_path / "data_meta.sqlite", lake_path=tmp_path / "lake")
    for facade in ("catalog", "raw", "items", "integrity", "inputs"):
        assert hasattr(lake, facade)
    for removed in ("admin", "update", "query", "metadata", "paths", "parquet", "ingest"):
        assert not hasattr(lake, removed)
    for removed in ("LakeAdmin", "LakeQuery", "LakeUpdater", "MetadataStore"):
        assert not hasattr(bagelquant_data, removed)
    assert DatasetSpec and TushareSource
