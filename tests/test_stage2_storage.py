"""Stage 2 storage guarantees, independent of provider and application state."""

from datetime import UTC, date, datetime
import sqlite3

import polars as pl
import pytest

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.exceptions import ConfigurationError
from bagelquant_data.pipeline.versions import commit_versions
from bagelquant_data.query.raw import RawQueryService
from bagelquant_data.storage.data_meta import DataMetaStore
from bagelquant_data.storage.parquet import ParquetStore
from bagelquant_data.storage.paths import LakePaths
from bagelquant_data.storage.recovery import inspect_partition, repair_partition


@pytest.fixture
def storage(tmp_path):
    paths = LakePaths.open(data_meta_path=tmp_path / "meta" / "data_meta.sqlite", lake_path=tmp_path / "lake")
    paths.ensure()
    data_meta = DataMetaStore(data_meta_path=paths.data_meta_path)
    data_meta.bind_lake(paths.lake)
    spec = DatasetSpec("daily", "by_date")
    data_meta.upsert_dataset(spec)
    return spec, paths, data_meta, ParquetStore(paths, data_meta)


def frame(value=1.0):
    return pl.DataFrame({"source_time": [date(2025, 1, 2)], "time": [date(2025, 1, 2)], "asset_id": ["A"], "value": [value]})


def commit(storage, value=1.0, day=2, **kwargs):
    spec, _, _, parquet = storage
    return commit_versions(spec, frame(value), parquet, run_id=f"run-{day}", ingested_at=datetime(2025, 1, day, tzinfo=UTC), **kwargs)


def test_immutable_generation_pins_lazy_reads(storage):
    spec, paths, data_meta, parquet = storage
    commit(storage)
    first = data_meta.manifest(spec.source, spec.name)[0]
    lazy = RawQueryService(parquet, data_meta).query(spec.name, source=spec.source)
    commit(storage, 2.0, 3)
    second = data_meta.manifest(spec.source, spec.name)[0]
    assert first["partition_path"] == second["partition_path"]
    assert first["generation_path"] != second["generation_path"]
    assert (paths.dataset_root(spec.source, spec.name) / first["generation_path"]).is_file()
    assert lazy.collect()["value"].to_list() == [1.0]
    assert RawQueryService(parquet, data_meta).query(spec.name, source=spec.source).collect()["value"].to_list() == [2.0]
    assert not list(paths.lake.rglob("recovery.sqlite"))


def test_failed_publication_keeps_prior_visibility(storage, monkeypatch):
    spec, paths, data_meta, parquet = storage
    commit(storage)
    original = data_meta.manifest(spec.source, spec.name)
    def fail(*args, **kwargs):
        raise RuntimeError("injected publication failure")
    monkeypatch.setattr(data_meta, "commit_dataset_metadata", fail)
    with pytest.raises(RuntimeError, match="injected"):
        commit(storage, 2.0, 3)
    assert data_meta.manifest(spec.source, spec.name) == original
    assert len(data_meta.dataset_snapshot(spec.source, spec.name)["commits"]) == 1
    assert RawQueryService(parquet, data_meta).query(spec.name, source=spec.source).collect()["value"].to_list() == [1.0]
    assert len(list(paths.dataset_root(spec.source, spec.name).rglob("*.parquet"))) == 1


def test_same_value_new_availability_is_new_evidence(storage):
    spec, _, data_meta, parquet = storage
    commit(storage, mode="initialize")
    assert commit(storage, day=3).rows_committed == 0
    assert commit(storage, day=4).rows_committed == 0
    versions = RawQueryService(parquet, data_meta).query(spec.name, source=spec.source, view="versions").collect()
    assert versions["time"].to_list() == [date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 4)]
    assert versions["_baseline"].to_list() == [True, False, False]
    assert versions["_commit_seq"].n_unique() == 1
    assert len(data_meta.dataset_snapshot(spec.source, spec.name)["commits"]) == 1


def test_exact_local_repair_from_central_evidence(storage):
    spec, paths, data_meta, parquet = storage
    commit(storage)
    row = data_meta.manifest(spec.source, spec.name)[0]
    path = paths.dataset_root(spec.source, spec.name) / row["generation_path"]
    path.write_bytes(b"corrupt parquet")
    assert inspect_partition(parquet, spec.source, spec.name, row["partition_path"], deep=True)["recovery_state"] == "ready"
    result = repair_partition(parquet, spec.source, spec.name, row["partition_path"])
    assert result["content_changed"] is False
    assert data_meta.manifest(spec.source, spec.name)[0] == row
    assert pl.read_parquet(path)["value"].to_list() == [1.0]


def test_inadequate_evidence_blocks_repair(storage):
    spec, paths, data_meta, parquet = storage
    commit(storage)
    row = data_meta.manifest(spec.source, spec.name)[0]
    path = paths.dataset_root(spec.source, spec.name) / row["generation_path"]
    path.write_bytes(b"corrupt parquet")
    with data_meta.connect() as db:
        db.execute("update version_batches set payload=?", (b"corrupt batch",))
    with pytest.raises(Exception):
        repair_partition(parquet, spec.source, spec.name, row["partition_path"])
    assert path.read_bytes() == b"corrupt parquet"


def test_read_only_and_lake_identity(storage, tmp_path):
    spec, paths, _, parquet = storage
    commit(storage)
    readonly_paths = LakePaths.open(data_meta_path=paths.data_meta_path, lake_path=paths.lake, read_only=True)
    readonly_paths.ensure()
    data_meta = DataMetaStore(data_meta_path=paths.data_meta_path, read_only=True)
    data_meta.bind_lake(paths.lake)
    readonly = ParquetStore(readonly_paths, data_meta)
    assert RawQueryService(readonly, data_meta).query(spec.name, source=spec.source).collect().height == 1
    with pytest.raises(PermissionError, match="read-only"):
        commit_versions(spec, frame(), readonly, run_id="forbidden")
    with pytest.raises(PermissionError, match="read-only"):
        data_meta.upsert_dataset(spec)
    other = tmp_path / "different-lake"
    other.mkdir()
    with pytest.raises(ConfigurationError, match="different lakes"):
        data_meta.bind_lake(other)
    assert not list(other.iterdir())
    assert parquet.paths.lake == paths.lake


def test_archiving_preserves_version_evidence(storage):
    spec, _, data_meta, parquet = storage
    commit(storage)
    data_meta.remove_dataset(spec.source, spec.name)
    assert data_meta.get_dataset(spec.source, spec.name) is None
    assert data_meta.list_datasets() == []
    assert len(data_meta.dataset_snapshot(spec.source, spec.name)["commits"]) == 1
    assert RawQueryService(parquet, data_meta).query(spec.name, source=spec.source).collect().height == 1


def test_old_schema_rejected_without_modification(tmp_path):
    data_meta_path = tmp_path / "old.sqlite"
    with sqlite3.connect(data_meta_path) as db:
        db.execute("create table metadata_state(key text primary key,value text)")
        db.execute("insert into metadata_state values('schema_version','4')")
    before = data_meta_path.read_bytes()
    with pytest.raises(ConfigurationError, match="Incompatible"):
        DataMetaStore(data_meta_path=data_meta_path)
    assert data_meta_path.read_bytes() == before


def test_scope_failure_rolls_back_visibility_with_files(storage):
    spec, paths, data_meta, _ = storage
    commit(storage)
    old = data_meta.manifest(spec.source, spec.name)
    with pytest.raises(RuntimeError, match="not claimed"):
        commit(storage, 2.0, 3, scope_transitions=[{"scope_id": 999, "status": "success"}])
    assert data_meta.manifest(spec.source, spec.name) == old
    assert len(data_meta.dataset_snapshot(spec.source, spec.name)["commits"]) == 1
    assert len(list(paths.dataset_root(spec.source, spec.name).rglob("*.parquet"))) == 1


def test_strict_reads_exclude_historical_baseline(storage):
    spec, _, data_meta, parquet = storage
    commit(storage, mode="initialize")
    query = RawQueryService(parquet, data_meta)
    assert query.query(spec.name, source=spec.source, strict=True).collect().is_empty()
    commit(storage, day=3)
    assert query.query(spec.name, source=spec.source, strict=True).collect()["time"].to_list() == [date(2025, 1, 3)]
    assert query.query(spec.name, source=spec.source, strict=True, as_of_date=date(2025, 1, 2)).collect().is_empty()
    assert query.query(spec.name, source=spec.source, strict=True, ingested_before=datetime(2025, 1, 2, 23, tzinfo=UTC)).collect().is_empty()
    assert query.query(spec.name, source=spec.source, strict=True, max_check_id=0).collect().is_empty()


def test_general_unchanged_checks_attest_at_exact_cutoff(storage):
    _, _, data_meta, parquet = storage
    spec = DatasetSpec("general", "general")
    data_meta.upsert_dataset(spec)
    raw = pl.DataFrame({"asset_id": ["A"], "name": ["Alpha"]})
    commit_versions(spec, raw, parquet, run_id="baseline", mode="initialize", ingested_at=datetime(2025, 1, 2, tzinfo=UTC))
    query = RawQueryService(parquet, data_meta)
    assert query.query_general(spec.name, source=spec.source, strict=True).collect().is_empty()
    result = commit_versions(spec, raw, parquet, run_id="attested", ingested_at=datetime(2025, 1, 3, tzinfo=UTC))
    assert result.rows_committed == 0
    assert len(data_meta.dataset_snapshot(spec.source, spec.name)["commits"]) == 1
    assert query.query_general(spec.name, source=spec.source, strict=True, as_of_date=date(2025, 1, 2)).collect().is_empty()
    assert query.query_general(spec.name, source=spec.source, strict=True, ingested_before=datetime(2025, 1, 2, 23, tzinfo=UTC)).collect().is_empty()
    witnessed = query.query_general(spec.name, source=spec.source, strict=True, as_of_date=date(2025, 1, 3)).collect()
    assert witnessed["name"].to_list() == ["Alpha"]
    assert witnessed["snapshot_date"].to_list() == [date(2025, 1, 3)]
    assert witnessed["_baseline"].to_list() == [False]
    commit_versions(spec, raw, parquet, run_id="same-day", ingested_at=datetime(2025, 1, 3, 12, tzinfo=UTC))
    assert query.query_general(spec.name, source=spec.source, strict=True).collect().height == 1


def test_reverting_payload_is_a_new_observation(storage):
    spec, _, data_meta, parquet = storage
    commit(storage, 1.0, 2)
    commit(storage, 2.0, 3)
    assert commit(storage, 1.0, 4).rows_committed == 1
    versions = RawQueryService(parquet, data_meta).query(spec.name, source=spec.source, view="versions").collect()
    assert versions["value"].to_list() == [1.0, 2.0, 1.0]
