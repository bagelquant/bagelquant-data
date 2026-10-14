"""Recover exact workflow-owned native leases only after publisher exit."""
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from datetime import UTC, date, datetime
import json
import subprocess
import sys
from threading import Event

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, RawInput
from bagelquant_data.pipeline import versions


def opened(root):
    return DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")


def panel():
    return pl.DataFrame({"time": [date(2020, 1, value) for value in range(1, 4)],
                          "asset_id": ["A"] * 3, "value": [-0.0, None, 3.0]})


def raw_spec():
    return DatasetSpec("prices", "by_date", date_kind="calendar",
                        field_mappings={"time": "time", "asset_id": "asset_id"})


def test_forced_dead_native_writer_exact_owner_recovery_preserves_flags_and_originals(tmp_path, monkeypatch):
    lake = opened(tmp_path)
    lake.raw.ingest(raw_spec(), panel(), mode="initialize",
                    ingested_at=datetime(2020, 1, 5, tzinfo=UTC))
    for name in ("complete", "interrupted"):
        lake.items.register(DataItemSpec(name, (RawInput("custom", "prices"),),
                                         time_column="source_time"))
    lake.items.initialize("complete", start="2020-01-01", end="2020-01-05")
    flag_before = lake.items.completion_statuses(names=["complete"])
    assert flag_before[0]["publication_current"]
    worker = subprocess.Popen([
        sys.executable, "-c", """
from pathlib import Path
import json
import sys
from bagelquant_data import DataLake
root = Path(sys.argv[1])
lake = DataLake.open(data_meta_path=root / 'meta.sqlite', lake_path=root / 'lake')
def progress(report):
    if report.rows_committed:
        print(json.dumps(lake.integrity.active_update_leases()), flush=True)
        sys.stdin.read()
with lake.integrity.update_owner('workflow:dead'):
    lake.items.initialize('interrupted', start='2020-01-01', end='2020-01-05', progress=progress)
""", str(tmp_path)], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True)
    try:
        assert worker.stdout is not None
        ready = worker.stdout.readline()
        if not ready:
            assert worker.stderr is not None
            pytest.fail(worker.stderr.read())
        leases = json.loads(ready)
        assert len(leases) == 1 and leases[0]["owner_id"] == "workflow:dead"
        assert leases[0]["source"] == "items" and leases[0]["dataset"] == "interrupted"
        assert worker.poll() is None
        # Even the now-expired heartbeat cannot transfer a living writer's lease.
        lake._data_meta.refresh_update_lease(run_id=leases[0]["run_id"], ttl_seconds=-1)
        with lake.integrity.update_owner("workflow:retry"):
            with pytest.raises(RuntimeError, match="already active"):
                lake.items.ingest("interrupted", panel(), available_date="2020-01-05")
        assert lake.integrity.abandon_update_owner("workflow:unrelated", reason="unrelated exited job") == {
            "runs": 0, "scopes": 0, "leases": 0}
        assert lake.integrity.active_update_leases()[0]["owner_id"] == "workflow:dead"
        assert not lake.items.completion_statuses(names=["interrupted"])[0]["publication_current"]
        originals = {path: path.read_bytes() for path in lake.lake_path.rglob("*.parquet")}
        frozen = lake.inputs.freeze({"partial": ItemInput("interrupted", view="snapshot")},
                                    information_cutoff="2020-01-05")
        retained = lake.inputs.read(frozen, "partial").collect()
        assert retained.height == 3 and retained["value"].to_list() == [-0.0, None, 3.0]
        worker.kill()
        worker.wait(timeout=10)
        assert worker.returncode is not None and worker.returncode != 0
        assert lake.integrity.active_update_leases()[0]["owner_id"] == "workflow:dead"
        with lake.integrity.update_owner("workflow:retry"):
            with pytest.raises(RuntimeError, match="already active"):
                lake.items.ingest("interrupted", panel(), available_date="2020-01-05")
        recovered = lake.integrity.abandon_update_owner("workflow:dead", reason="verified process exited after forced termination")
        assert recovered == {"runs": 1, "scopes": 0, "leases": 1}
        assert not lake.integrity.active_update_leases()
        assert lake.items.completion_statuses(names=["complete"]) == flag_before
        assert lake.inputs.read(frozen, "partial").collect().equals(retained)
        assert all(path.read_bytes() == content for path, content in originals.items())
        assert not lake.items.completion_statuses(names=["interrupted"])[0]["publication_current"]
        with lake.integrity.update_owner("workflow:retry"):
            result = lake.items.initialize("interrupted", start="2020-01-01", end="2020-01-05")
        assert result.status == "success" and result.rows_committed == 0
        current = lake.items.completion_statuses(names=["interrupted"])[0]
        assert current["publication_current"] and current["definition_current"]
        assert (current["completed_start"], current["completed_end"], current["checked_target"]) == (
            "2020-01-01", "2020-01-05", "2020-01-05")
        assert not lake.integrity.active_update_leases()
        assert lake.items.completion_statuses(names=["complete"]) == flag_before
        assert {path: path.read_bytes() for path in lake.lake_path.rglob("*.parquet")} == originals
        assert lake.inputs.read(frozen, "partial").collect().equals(retained)
        assert lake.inputs.verify(frozen)["valid"]
        with monkeypatch.context() as patch:
            from bagelquant_data import input_index, inputs
            patch.setattr(inputs, "read_batch", lambda *a, **k: pytest.fail("complete node decoded original"))
            patch.setattr(input_index, "project_original", lambda *a, **k: pytest.fail("complete node projected original"))
            patch.setattr(lake.items, "_build", lambda *a, **k: pytest.fail("complete node reran producer"))
            assert lake.items.initialize("complete", start="2020-01-01", end="2020-01-05").status == "unchanged"
    finally:
        if worker.poll() is None:
            worker.kill()
        worker.communicate(timeout=10)


def test_update_owner_shares_same_path_and_restores_nested_context(tmp_path):
    lake = opened(tmp_path)
    same_lake = opened(tmp_path)
    other_lake = opened(tmp_path / "other")
    with lake.integrity.update_owner("workflow:first"):
        with same_lake.integrity.update_owner("workflow:first"):
            with same_lake._data_meta.dataset_writer("items", "node", "nested"):
                assert lake.integrity.active_update_leases()[0]["owner_id"] == "workflow:first"
        with pytest.raises(RuntimeError, match="enclosing"):
            with same_lake.integrity.update_owner("workflow:foreign"):
                pytest.fail("replaced workflow identity")
        with other_lake.integrity.update_owner("workflow:other"):
            with other_lake._data_meta.dataset_writer("items", "node", "independent"):
                assert other_lake.integrity.active_update_leases()[0]["owner_id"] == "workflow:other"
        with pytest.raises(RuntimeError, match="enclosing"):
            same_lake._data_meta.acquire_update_leases([("items", "node", "foreign")], owner_id="workflow:foreign")
        with pytest.raises(RuntimeError, match="enclosing"):
            same_lake._data_meta.begin_run(run_id="foreign", source="items", dataset="node", mode="initialize", owner_id="workflow:foreign")
        assert not lake.integrity.active_update_leases()
        with pytest.raises(ValueError, match="fixture"):
            with same_lake.integrity.update_owner("workflow:first"):
                raise ValueError("fixture")
        with same_lake._data_meta.dataset_writer("items", "node", "after-nested-error"):
            assert lake.integrity.active_update_leases()[0]["owner_id"] == "workflow:first"
    with same_lake._data_meta.dataset_writer("items", "node", "standalone"):
        assert lake.integrity.active_update_leases()[0]["owner_id"] == "standalone"


def test_native_partition_workers_keep_enclosing_workflow_owner(tmp_path, monkeypatch):
    lake = opened(tmp_path)
    frame = pl.DataFrame({"time": [date(2020, month, 1) for month in (1, 2, 3)],
                           "asset_id": ["A"] * 3, "value": [1.0, 2.0, 3.0]})
    observed = []
    write = versions._write_version_partition

    def checked_write(*args, **kwargs):
        observed.append(lake.integrity.active_update_leases()[0]["owner_id"])
        return write(*args, **kwargs)

    monkeypatch.setattr(versions, "_write_version_partition", checked_write)
    with lake.integrity.update_owner("workflow:native"):
        from bagelquant_data import ExecutionOptions
        lake.items.register(DataItemSpec("output"))
        with lake.items.publication() as publication:
            from bagelquant_data import ItemPublication
            publication.publish([ItemPublication("output", frame=frame)])
        lake.raw.ingest(raw_spec(), frame, mode="initialize",
                        ingested_at=datetime(2020, 3, 1, tzinfo=UTC))
        lake.items.register(DataItemSpec("parallel", (RawInput("custom", "prices"),),
                                         time_column="source_time"))
        lake.items.initialize("parallel", start="2020-01-01", end="2020-03-31",
                               config=ExecutionOptions(workers=3))
    assert observed and set(observed) == {"workflow:native"}
    assert not lake.integrity.active_update_leases()


def test_live_foreign_owner_is_fenced_even_when_heartbeat_expired(tmp_path, monkeypatch):
    lake = opened(tmp_path)
    lake.items.register(DataItemSpec("output"))
    entered, release = Event(), Event()
    write = versions._write_version_partition

    def held_write(*args, **kwargs):
        entered.set()
        assert release.wait(timeout=10)
        return write(*args, **kwargs)

    monkeypatch.setattr(versions, "_write_version_partition", held_write)

    def publisher():
        with lake.integrity.update_owner("workflow:live"):
            return lake.items.ingest("output", panel(), available_date="2020-01-05")

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(publisher)
        try:
            assert entered.wait(timeout=10)
            lease = lake.integrity.active_update_leases()[0]
            lake._data_meta.refresh_update_lease(run_id=lease["run_id"], ttl_seconds=-1)
            assert lake.integrity.active_update_leases()[0]["heartbeat_expired"]
            with lake.integrity.update_owner("workflow:other"):
                with pytest.raises(RuntimeError, match="already active"):
                    lake.items.ingest("output", panel(), available_date="2020-01-05")
            assert lake.integrity.abandon_update_owner("workflow:other", reason="other exited") == {
                "runs": 0, "scopes": 0, "leases": 0}
            assert lake.integrity.active_update_leases()[0]["owner_id"] == "workflow:live"
        finally:
            release.set()
        assert future.result(timeout=10).rows_committed == 3
    assert not lake.integrity.active_update_leases()


def test_explicit_context_copy_and_readonly_owner_admission(tmp_path):
    lake = opened(tmp_path)

    def new_writer():
        with lake._data_meta.dataset_writer("items", "node", "thread"):
            return lake.integrity.active_update_leases()[0]["owner_id"]

    with lake.integrity.update_owner("workflow:copied"), ThreadPoolExecutor(max_workers=1) as executor:
        assert executor.submit(copy_context().run, new_writer).result(timeout=10) == "workflow:copied"
    for owner in ("", " "):
        with pytest.raises(ValueError, match="nonempty"):
            with lake.integrity.update_owner(owner):
                pytest.fail("invalid owner")
    readonly = DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path, read_only=True)
    with pytest.raises(PermissionError, match="read-only"):
        with readonly.integrity.update_owner("workflow:readonly"):
            pytest.fail("read-only write owner")


def test_raw_scheduler_inherits_owner_and_retains_completed_scope_flags(tmp_path):
    from test_ledger_updates import LedgerSource, _daily_lake
    source = LedgerSource()
    lake = _daily_lake(tmp_path, source)
    owners = []
    fetch = source.fetch

    def checked_fetch(dataset, request):
        owners.extend(lease["owner_id"] for lease in lake.integrity.active_update_leases())
        return fetch(dataset, request)

    source.fetch = checked_fetch
    with lake.integrity.update_owner("workflow:raw"):
        report = lake.raw.initialize("daily", source="custom", start="2025-01-02", end="2025-01-03")
    assert report.status == "success"
    assert owners and set(owners) == {"workflow:raw"}
    with lake._data_meta.connect() as db:
        assert db.execute("select owner_id from ingestion_runs where run_id=?", (report.run_id,)).fetchone()[0] == "workflow:raw"
    before = lake.raw.completion_statuses(source="custom", datasets=["daily"])
    assert before[0]["publication_current"]
    scopes = lake.integrity.update_scopes(source="custom", dataset="daily")
    assert lake.integrity.abandon_update_owner("workflow:raw", reason="completed owner exited") == {
        "runs": 0, "scopes": 0, "leases": 0}
    assert lake.integrity.update_scopes(source="custom", dataset="daily") == scopes
    assert lake.raw.completion_statuses(source="custom", datasets=["daily"]) == before
    assert not lake.raw.plan_updates(["daily"], source="custom", start="2025-01-02", end="2025-01-03")
