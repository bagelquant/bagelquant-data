"""Exact currentness parent selection has bounded, disposable projected frames."""
from concurrent.futures import CancelledError, ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, date, datetime
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, RawInput
from bagelquant_data.execution import ExecutionOptions
from bagelquant_data.storage import full_commit_checks
import bagelquant_data.inputs as implementation
import bagelquant_data.storage.recovery as recovery


def prepared(root, *, baseline=False, revisions=True):
    lake = DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")
    frame = pl.DataFrame({"time": [date(2020, 1, 1)] * 24,
                          "asset_id": [f"asset-{i}" for i in range(24)],
                          "value": [float(i) for i in range(24)]})
    spec = DatasetSpec("raw", "by_date", date_kind="calendar",
                       field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    raw = RawInput("custom", "raw", view="versions")
    lake.items.register(DataItemSpec("item", (raw,), producer_key="external", producer_revision="1"))
    old = lake.inputs.freeze({"raw": raw}, information_cutoff="2020-03-05")
    lake.items.ingest("item", frame, available_date="2020-01-02", input_receipt=old,
                      historical_baseline=baseline)
    lake.raw.ingest(spec, frame.with_columns(pl.col("value") + 1),
                    ingested_at=datetime(2020, 2, 1, tzinfo=UTC))
    current = lake.inputs.freeze({"raw": raw}, information_cutoff="2020-03-05")
    if not revisions:
        return lake
    lake.items.ingest("item", frame.head(12).with_columns(pl.col("value") + 2),
                      available_date="2020-02-02", input_receipt=current)
    # A witnessed copy of unchanged baseline content carries the new proof.
    lake.items.ingest("item", frame.tail(12), available_date="2020-03-02",
                      input_receipt=current)
    return lake


def reference(lake, frozen):
    evidence = frozen.evidence["item"]
    frame = lake.inputs._read_frame(frozen, "item")
    cutoff = frozen.information_cutoff
    if cutoff is None and frame.height:
        cutoff = frame["time"].max()
    selected = lake.inputs._select(frame, replace(frozen.requests["item"], view="snapshot"),
                                   cutoff, evidence=evidence) if cutoff is not None else frame
    commits = {row["commit_seq"]: row["input_receipt_id"] for row in evidence["batches"]}
    checks = {row["id"]: row["input_receipt_id"] for row in evidence["checks"]}
    parents = set()
    for commit, check in selected.select("_commit_seq", "_attestation_id").unique().iter_rows():
        parent = checks.get(check) if check is not None else commits.get(commit)
        if parent is not None:
            parents.add(parent)
    return bool(selected.height), parents


@pytest.mark.parametrize("baseline", [False, True])
@pytest.mark.parametrize("cutoff", ["2020-01-03", "2020-02-03", "2020-03-03", None])
@pytest.mark.parametrize("strict", [False, True])
def test_cross_month_and_witness_selection_equals_original_frames(tmp_path, baseline, cutoff, strict, monkeypatch):
    lake = prepared(tmp_path, baseline=baseline)
    frozen = lake.inputs.freeze({"item": ItemInput("item", strict=strict, start="2020-01-01", end="2020-01-01")},
                                information_cutoff=cutoff)
    expected = reference(lake, frozen)
    # A low budget forces multiple actual record shards and the disk transport.
    options = ExecutionOptions(max_buffer_bytes=32 * 1024)
    converted = []
    shards = set()
    spilled = []
    original_temp = recovery.tempfile.TemporaryFile
    def spill(*args, **kwargs):
        spilled.append(True)
        return original_temp(*args, **kwargs)
    monkeypatch.setattr(recovery.tempfile, "TemporaryFile", spill)
    original_writer = implementation.pq.ParquetWriter
    class Writer:
        def __init__(self, *args, **kwargs):
            self.writer = original_writer(*args, **kwargs)
        def write_table(self, table):
            shards.update(table["_lineage_shard"].to_pylist())
            self.writer.write_table(table)
        def close(self):
            self.writer.close()
    monkeypatch.setattr(implementation.pq, "ParquetWriter", Writer)
    original = recovery.pl.from_arrow
    def convert(value, **kwargs):
        if value.num_rows:
            converted.append(tuple(value.schema.names))
            assert "value" not in value.schema.names
        return original(value, **kwargs)
    monkeypatch.setattr(recovery.pl, "from_arrow", convert)
    assert lake.inputs._selected_item_parents(frozen, "item", options) == expected
    assert converted
    assert len(shards) > 1
    assert spilled


def test_compact_attestation_matches_inline_selection(tmp_path, monkeypatch):
    lake = prepared(tmp_path, baseline=True, revisions=False)
    parent_id = lake.items.builds("item")[0]["frozen_receipt_id"]
    parent = lake.inputs.get(parent_id)
    # Reattest the whole original baseline, not a partial/mixed publication.
    original = pl.DataFrame({"time": [date(2020, 1, 1)] * 24,
                            "asset_id": [f"asset-{i}" for i in range(24)],
                            "value": [float(i) for i in range(24)]})
    lake.items.ingest("item", original, available_date="2020-03-03", input_receipt=parent)
    monkeypatch.setattr(full_commit_checks, "COMPACT_MIN_ROWS", 1)
    frozen = lake.inputs.freeze({"item": ItemInput("item", strict=True)}, information_cutoff="2020-03-03")
    assert frozen.evidence["item"].get("full_commit_checks")
    assert lake.inputs._selected_item_parents(frozen, "item", ExecutionOptions(max_buffer_bytes=128 * 1024)) == reference(lake, frozen)


def test_low_budget_and_cancel_remove_transient_spool(tmp_path, monkeypatch):
    lake = prepared(tmp_path)
    frozen = lake.inputs.freeze({"item": ItemInput("item")}, information_cutoff="2020-03-03")
    temporary = tmp_path / "temporary"
    temporary.mkdir()
    original = implementation.tempfile.TemporaryDirectory
    monkeypatch.setattr(implementation.tempfile, "TemporaryDirectory",
                        lambda **kwargs: original(dir=temporary, **kwargs))
    with pytest.raises(MemoryError, match="max_buffer_bytes"):
        lake.inputs._selected_item_parents(frozen, "item", ExecutionOptions(max_buffer_bytes=8))
    assert list(temporary.iterdir()) == []
    calls = 0
    error = CancelledError("original cancellation")
    def cancel():
        nonlocal calls
        calls += 1
        if calls > 10:
            raise error
    with pytest.raises(CancelledError) as caught:
        with lake.inputs.read_context(frozen, verify=False, check_canceled=cancel,
                                     config=ExecutionOptions(max_buffer_bytes=128 * 1024)):
            lake.inputs._selected_item_parents(frozen, "item", ExecutionOptions(max_buffer_bytes=128 * 1024))
    assert caught.value is error
    assert list(temporary.iterdir()) == []
    assert lake.inputs._selected_item_parents(frozen, "item", ExecutionOptions()) == reference(lake, frozen)


def test_original_corruption_is_rejected_even_after_context_byte_proof(tmp_path):
    lake = prepared(tmp_path)
    frozen = lake.inputs.freeze({"item": ItemInput("item")}, information_cutoff="2020-03-03")
    with lake.inputs.read_context(frozen):
        assert lake.inputs._selected_item_parents(frozen, "item", ExecutionOptions()) == reference(lake, frozen)
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update version_batches set payload=x'00' where commit_seq=?",
                   (frozen.evidence["item"]["batches"][0]["commit_seq"],))
    with pytest.raises(RuntimeError, match="compression"):
        lake.inputs._selected_item_parents(frozen, "item", ExecutionOptions())


@pytest.mark.parametrize("budget", [32 * 1024, 1024 * 1024])
@pytest.mark.parametrize("error_type", [TypeError, ValueError, RuntimeError, CancelledError])
def test_project_consumer_exception_preserves_identity_and_closes_transport(tmp_path, monkeypatch, budget, error_type):
    lake = prepared(tmp_path)
    frozen = lake.inputs.freeze({"item": ItemInput("item")}, information_cutoff="2020-03-03")
    batch = frozen.evidence["item"]["batches"][0]
    files = []
    original = recovery.tempfile.TemporaryFile
    def temporary(*args, **kwargs):
        file = original(*args, **kwargs)
        files.append(file)
        return file
    monkeypatch.setattr(recovery.tempfile, "TemporaryFile", temporary)
    error = error_type("original consumer failure")
    def consume(frame):
        assert "value" not in frame.columns
        raise error
    with pytest.raises(error_type) as caught:
        recovery.project_batch(lake._data_meta, batch["partition_path"], batch["commit_seq"],
            batch["content_hash"], columns=("_record_id",), consume=consume, buffer_bytes=budget)
    assert caught.value is error
    assert all(file.closed for file in files)
    assert bool(files) is (budget == 32 * 1024)


def test_tied_witness_sort_keys_and_cutoff_none_keep_original_selected_parent(tmp_path):
    lake = prepared(tmp_path, baseline=True, revisions=False)
    first = lake.inputs.get(lake.items.builds("item")[0]["frozen_receipt_id"])
    # Two original IDs bind identical dependencies and exact witness sort keys.
    with sqlite3.connect(lake.data_meta_path) as db:
        payload, digest, created = db.execute("select payload_json,digest,created_at from frozen_inputs where receipt_id=?",
                                            (first.receipt_id,)).fetchone()
        db.execute("insert into frozen_inputs values('second-original',?,?,?)", (payload, digest, created))
    frame = pl.DataFrame({"time": [date(2020, 1, 1)] * 24,
                          "asset_id": [f"asset-{i}" for i in range(24)],
                          "value": [float(i) for i in range(24)]})
    for receipt in (first, lake.inputs.get("second-original")):
        lake.items.ingest("item", frame, available_date="2020-03-03", input_receipt=receipt,
                          ingested_at=datetime(2020, 3, 3, tzinfo=UTC))
    frozen = lake.inputs.freeze({"item": ItemInput("item", strict=True)})
    assert len(frozen.evidence["item"]["checks"]) == 2
    assert lake.inputs._selected_item_parents(frozen, "item", ExecutionOptions(max_buffer_bytes=32 * 1024)) == reference(lake, frozen)


def test_sealed_witness_expansion_fails_before_selector_allocation(tmp_path, monkeypatch):
    lake = prepared(tmp_path, baseline=True, revisions=False)
    parent = lake.inputs.get(lake.items.builds("item")[0]["frozen_receipt_id"])
    frame = pl.DataFrame({"time": [date(2020, 1, 1)] * 24,
                          "asset_id": [f"asset-{i}" for i in range(24)],
                          "value": [float(i) for i in range(24)]})
    # Every retained unchanged collection produces a complete original witness.
    for second in range(60):
        lake.items.ingest("item", frame, available_date="2020-03-03", input_receipt=parent,
                          ingested_at=datetime(2020, 3, 3, 0, 0, second, tzinfo=UTC))
    monkeypatch.setattr(full_commit_checks, "COMPACT_MIN_ROWS", 1)
    frozen = lake.inputs.freeze({"item": ItemInput("item", strict=True)}, information_cutoff="2020-03-03")
    assert len(frozen.evidence["item"]["full_commit_checks"]) == 60
    import bagelquant_data.query.raw as raw_query
    monkeypatch.setattr(raw_query, "_attested_versions",
                        lambda *args, **kwargs: pytest.fail("oversized witness expansion reached allocation"))
    with pytest.raises(MemoryError, match="witnessed lineage"):
        lake.inputs._selected_item_parents(frozen, "item", ExecutionOptions(max_buffer_bytes=32 * 1024))


def test_public_currentness_projects_in_captured_wal_view_and_inherits_budget(tmp_path, monkeypatch):
    lake = prepared(tmp_path)
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("pragma journal_mode=WAL")
    writer = DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path)
    frozen = lake.inputs.freeze({"item": ItemInput("item")}, information_cutoff="2020-03-03")
    original = implementation.project_batch
    admitted = []
    def project(*args, **kwargs):
        admitted.append(kwargs["buffer_bytes"])
        if len(admitted) == 1:
            spec = writer.raw.get("raw", source="custom")
            frame = pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["asset-0"], "value": [999.]})
            with ThreadPoolExecutor(max_workers=1) as executor:
                executor.submit(writer.raw.ingest, spec, frame,
                                ingested_at=datetime(2020, 3, 3, tzinfo=UTC)).result()
        return original(*args, **kwargs)
    monkeypatch.setattr(implementation, "project_batch", project)
    with lake.inputs.read_context(frozen, verify=False, config=ExecutionOptions(max_buffer_bytes=64 * 1024)):
        assert lake.inputs.is_current(frozen)
        assert set(admitted) == {64 * 1024}
    assert not lake.inputs.is_current(frozen, config=ExecutionOptions(max_buffer_bytes=128 * 1024))


def test_many_tiny_projected_fragments_use_bounded_owned_row_groups(tmp_path, monkeypatch):
    lake = prepared(tmp_path)
    frozen = lake.inputs.freeze({"item": ItemInput("item")}, information_cutoff="2020-03-03")
    expected = reference(lake, frozen)
    original_project = implementation.project_batch
    fragments = []
    def project(*args, **kwargs):
        consumer = kwargs["consume"]
        def tiny(frame):
            for offset in range(frame.height):
                part = frame.slice(offset, 1)
                fragments.extend(part["_record_id"].to_list())
                consumer(part)
        return original_project(*args, **{**kwargs, "consume": tiny})
    monkeypatch.setattr(implementation, "project_batch", project)
    original_writer = implementation.pq.ParquetWriter
    written = []
    options = ExecutionOptions(max_buffer_bytes=2 * 1024 * 1024)
    class Writer:
        def __init__(self, *args, **kwargs):
            self.writer = original_writer(*args, **kwargs)
        def write_table(self, table):
            assert table.nbytes <= options.max_buffer_bytes // 4
            written.append(table["_record_id"].to_pylist())
            self.writer.write_table(table)
        def close(self):
            self.writer.close()
    monkeypatch.setattr(implementation.pq, "ParquetWriter", Writer)
    assert lake.inputs._selected_item_parents(frozen, "item", options) == expected
    assert len(fragments) > 40
    assert len(written) == 1
    assert written[0] == fragments


@pytest.mark.parametrize("budget", [32 * 1024, 1024 * 1024])
def test_staging_failure_releases_owned_pending_fragments_and_spool(tmp_path, monkeypatch, budget):
    lake = prepared(tmp_path)
    frozen = lake.inputs.freeze({"item": ItemInput("item")}, information_cutoff="2020-03-03")
    temporary = tmp_path / "temporary"
    temporary.mkdir()
    original_directory = implementation.tempfile.TemporaryDirectory
    monkeypatch.setattr(implementation.tempfile, "TemporaryDirectory",
                        lambda **kwargs: original_directory(dir=temporary, **kwargs))
    original_project = implementation.project_batch
    error = TypeError("failure after copied staging fragment")
    def project(*args, **kwargs):
        consumer = kwargs["consume"]
        def fail(frame):
            consumer(frame)
            raise error
        return original_project(*args, **{**kwargs, "consume": fail})
    monkeypatch.setattr(implementation, "project_batch", project)
    with pytest.raises(TypeError) as caught:
        lake.inputs._selected_item_parents(frozen, "item", ExecutionOptions(max_buffer_bytes=budget))
    assert caught.value is error
    assert list(temporary.iterdir()) == []
