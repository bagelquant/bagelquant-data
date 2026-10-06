"""Actual disk inventory and conservative owner-local temporary cleanup."""

from datetime import date

import polars as pl
import pytest

from bagelquant_data import ConfigurationError, DataLake, DatasetSpec


def setup(lake):
    spec = DatasetSpec("daily", "by_date", source="fake", date_kind="calendar",
                       field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2026, 1, 2)], "asset_id": ["A"], "value": [1.]})
    lake.raw.ingest(spec, frame)
    lake.raw.ingest(spec, frame.with_columns(pl.lit(2.).alias("value")))
    root = lake.lake_path / "raw/fake/daily/year=2026/month=01"
    temporary = root / (".data-" + "a" * 32 + ".parquet." + "b" * 32 + ".tmp")
    temporary.write_bytes(b"abandoned")
    (root / "unknown.tmp").write_bytes(b"unknown")
    return temporary


def test_usage_history_unknown_and_cleanup_idempotence(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    temporary = setup(lake)
    usage = lake.integrity.storage_usage()
    assert usage["raw_datasets"] == usage["available_raw_datasets"] == 1
    assert usage["history_bytes"] > 0 and usage["current_bytes"] > 0
    assert usage["temporary_bytes"] == len(b"abandoned")
    assert usage["unknown_bytes"] == len(b"unknown")
    assert usage["bytes"] == usage["metadata_bytes"] + usage["lake_bytes"]
    manifest = lake.raw.manifest("daily", source="fake")
    plan = lake.integrity.temporary_cleanup_plan()
    result = lake.integrity.cleanup_temporary(plan)
    assert result["bytes"] == len(b"abandoned") and not temporary.exists()
    repeated = lake.integrity.cleanup_temporary(plan)
    assert repeated["bytes"] == 0 and repeated["already_missing"] == result["deleted"]
    assert lake.raw.manifest("daily", source="fake") == manifest
    assert (temporary.parent / "unknown.tmp").read_bytes() == b"unknown"
    assert lake.integrity.scan("daily", source="fake")["valid"]


def test_active_writer_changed_file_foreign_plan_and_links_protected(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    temporary = setup(lake)
    store = lake._data_meta
    store.acquire_update_leases([("fake", "daily", "running")])
    try:
        plan = lake.integrity.temporary_cleanup_plan()
        assert plan["candidates"] == [] and plan["protected_temporary_bytes"] > 0
    finally:
        store.release_update_leases(["running"])
    plan = lake.integrity.temporary_cleanup_plan()
    temporary.write_bytes(b"changed")
    with pytest.raises(ConfigurationError, match="stale"):
        lake.integrity.cleanup_temporary(plan)
    foreign = DataLake.open(data_meta_path=tmp_path / "other.sqlite", lake_path=tmp_path / "other")
    with pytest.raises(ConfigurationError, match="another lake"):
        foreign.integrity.cleanup_temporary(plan)
    temporary.unlink()
    outside = tmp_path / "outside"
    outside.write_bytes(b"protected")
    temporary.symlink_to(outside)
    assert not lake.integrity.temporary_cleanup_plan()["candidates"]
    assert outside.read_bytes() == b"protected"


def test_inventory_deduplicates_hardlinks_and_readonly_cleanup_rejected(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    temporary = setup(lake)
    import os
    os.link(temporary, temporary.parent / "hardlink")
    assert lake.integrity.temporary_cleanup_plan()["candidates"] == []
    usage = lake.integrity.storage_usage()
    physical = {}
    for path in tmp_path.rglob("*"):
        if path.is_file():
            facts = path.stat()
            physical[(facts.st_dev, facts.st_ino)] = facts.st_size
    assert usage["bytes"] == sum(physical.values())
    readonly = DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path, read_only=True)
    with pytest.raises(PermissionError):
        readonly.integrity.cleanup_temporary(readonly.integrity.temporary_cleanup_plan())


def test_late_writer_and_forged_candidate_cannot_delete_retained_bytes(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    temporary = setup(lake)
    plan = lake.integrity.temporary_cleanup_plan()
    lake._data_meta.acquire_update_leases([("fake", "daily", "late")])
    try:
        with pytest.raises(Exception, match="owned|lease|active"):
            lake.integrity.cleanup_temporary(plan)
        assert temporary.exists()
    finally:
        lake._data_meta.release_update_leases(["late"])
    # A caller can recompute a JSON hash; ownership checks remain authoritative.
    from bagelquant_data.management.maintenance import _hash, _identity
    generation = lake.raw.manifest("daily", source="fake")[0]["generation_path"]
    relative = "raw/fake/daily/" + generation
    path = lake.lake_path / relative
    plan["candidates"] = [{"path": relative, "source": "fake", "dataset": "daily", **_identity(path, content=True)}]
    plan["plan_hash"] = _hash({key: value for key, value in plan.items() if key != "plan_hash"})
    with pytest.raises(ConfigurationError, match="stale"):
        lake.integrity.cleanup_temporary(plan)
    assert path.exists() and temporary.exists()
