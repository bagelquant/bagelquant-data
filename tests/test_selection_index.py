"""Optional metadata proofs never silently fall back to original numerical IPC."""
from concurrent.futures import CancelledError, ThreadPoolExecutor
from datetime import date
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, ItemInput, RawInput
from bagelquant_data import inputs, input_index
from test_currentness_lineage import prepared


def test_missing_index_is_unknown_and_explicit_build_restores_lineage(tmp_path, monkeypatch):
    lake = prepared(tmp_path)
    frozen = lake.inputs.freeze({"item": ItemInput("item")}, information_cutoff="2020-03-03")
    assert lake.inputs.is_current(frozen)
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("DELETE FROM input_batch_index")
        db.execute("DELETE FROM input_selection_index")
    monkeypatch.setattr(inputs, "read_batch", lambda *a, **k: pytest.fail("implicit IPC fallback"))
    assert lake.inputs.selection_identity(frozen, "item") is None
    assert not lake.inputs.is_current(frozen)
    assert lake.inputs.build_index(lake.inputs.index_plan())["status"] == "complete"
    assert lake.inputs.selection_identity(frozen, "item") is not None
    assert lake.inputs.is_current(frozen)
    assert lake.inputs.verify(frozen)["valid"]


def test_selection_ignores_unrelated_alias_and_matches_consumed_view(tmp_path):
    lake = prepared(tmp_path)
    item = ItemInput("item", view="versions")
    first = lake.inputs.freeze({"item": item}, information_cutoff="2020-03-03")
    second = lake.inputs.freeze({"item": item, "raw": RawInput("custom", "raw")}, information_cutoff="2020-03-03")
    assert first.digest != second.digest
    assert lake.inputs.selection_identity(first, "item", view="snapshot") == lake.inputs.selection_identity(second, "item", view="snapshot")
    assert lake.inputs.selection_identity(first, "item", view="snapshot") != lake.inputs.selection_identity(first, "item", view="versions")


def test_request_identity_is_original_metadata_and_index_independent(tmp_path, monkeypatch):
    lake = prepared(tmp_path)
    request = ItemInput("item", view="versions")
    first = lake.inputs.freeze({"item": request}, information_cutoff="2020-03-03")
    second = lake.inputs.freeze({"item": request, "raw": RawInput("custom", "raw")},
                                information_cutoff="2020-03-03")
    token = lake.inputs.request_identity(first, "item")
    assert token == lake.inputs.request_identity(second, "item")
    with lake._data_meta.connect() as db:
        db.execute("DELETE FROM input_batch_index")
        db.execute("DELETE FROM input_selection_index")
    monkeypatch.setattr(inputs, "read_batch", lambda *a, **k: pytest.fail("identity read original values"))
    assert lake.inputs.selection_identity(first, "item") is None
    assert lake.inputs.request_identity(first, "item") == token
    with lake.inputs.read_context(first):
        assert lake.inputs.request_identity(first, "item") == token
        # Supplied evidence never becomes authority, including memo hits.
        from dataclasses import replace
        assert lake.inputs.request_identity(replace(first, evidence={}), "item") == token
        with pytest.raises(RuntimeError, match="checksum"):
            lake.inputs.request_identity(replace(first, digest="invented"), "item")
    lake.inputs.build_index(lake.inputs.index_plan())
    assert lake.inputs.selection_identity(first, "item") is not None
    assert lake.inputs.request_identity(first, "item") == token
    narrower = lake.inputs.freeze({"item": ItemInput("item", start="2020-03-01")},
                                  information_cutoff="2020-03-03")
    assert lake.inputs.request_identity(narrower, "item") != token
    with pytest.raises(KeyError, match="alias"):
        lake.inputs.request_identity(first, "missing")


@pytest.mark.parametrize("target", ["bounds", "payload", "summary"])
def test_corrupt_index_rejected_without_original_reads(tmp_path, target):
    lake = prepared(tmp_path)
    frozen = lake.inputs.freeze({"item": ItemInput("item")}, information_cutoff="2020-03-03")
    with sqlite3.connect(lake.data_meta_path) as db:
        if target == "summary":
            db.execute("UPDATE input_selection_index SET identity='invented' WHERE receipt_digest=?", (frozen.digest,))
        else:
            db.execute("DELETE FROM input_selection_index")
            batch = frozen.evidence["item"]["batches"][0]
            if target == "payload":
                db.execute("UPDATE input_batch_index SET payload=x'00' WHERE commit_seq=?", (batch["commit_seq"],))
            else:
                db.execute("UPDATE input_batch_index SET bounds_json=json_set(bounds_json,'$.upper','1900-01-01') WHERE commit_seq=?", (batch["commit_seq"],))
    with pytest.raises(RuntimeError, match="damaged"):
        lake.inputs.selection_identity(frozen, "item")
    with pytest.raises(RuntimeError, match="damaged"):
        lake.inputs.verify(frozen)


def test_audit_compares_self_consistent_derived_summary_to_original(tmp_path):
    lake = prepared(tmp_path)
    frozen = lake.inputs.freeze({"item": ItemInput("item")}, information_cutoff="2020-03-03")
    with lake._data_meta.connect() as db:
        row = db.execute("SELECT request_key FROM input_selection_index WHERE receipt_digest=?", (frozen.digest,)).fetchone()
        saved = input_index.selection(db, frozen.digest, "item", row[0])
        assert saved is not None
        input_index.save_selection(db, frozen.digest, "item", row[0], {**saved, "identity": "invented"})
    with pytest.raises(RuntimeError, match="summary differs"):
        lake.inputs.verify(frozen)


def test_build_cancellation_keeps_only_atomic_completed_batches(tmp_path):
    lake = prepared(tmp_path)
    plan = lake.inputs.index_plan()
    with lake._data_meta.connect() as db:
        db.execute("DELETE FROM input_batch_index")
        db.execute("DELETE FROM input_selection_index")
    canceled = False
    def check():
        if canceled:
            raise CancelledError()
    def progress(_):
        nonlocal canceled
        canceled = True
    with pytest.raises(CancelledError):
        lake.inputs.build_index(plan, check_canceled=check, progress=progress)
    with lake._data_meta.connect() as db:
        assert db.execute("SELECT count(*) FROM input_batch_index").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM input_selection_index").fetchone()[0] == 0
    assert lake.inputs.build_index(plan)["status"] == "complete"


def test_worker_metadata_borrowing_expires_with_source(tmp_path, monkeypatch):
    lake = prepared(tmp_path)
    frozen = lake.inputs.freeze({"item": ItemInput("item")}, information_cutoff="2020-03-03")
    original = inputs.json.loads
    loaded = []
    def decode(value, *args, **kwargs):
        if '"requests"' in value:
            loaded.append(value)
        return original(value, *args, **kwargs)
    monkeypatch.setattr(inputs.json, "loads", decode)
    child = DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path, read_only=True)
    with lake.inputs.read_context(frozen):
        before = len(loaded)
        def work():
            with child.inputs.read_context(frozen, source=lake.inputs):
                return child.inputs.get(frozen).digest
        with ThreadPoolExecutor(max_workers=1) as pool:
            assert pool.submit(work).result() == frozen.digest
        assert len(loaded) == before
    with pytest.raises(ValueError, match="source"):
        with child.inputs.read_context(frozen, source=lake.inputs):
            pass


def test_unselected_historical_revision_preserves_snapshot_selection_identity(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("published"))
    rows = pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.]})
    lake.items.ingest("published", rows, available_date="2020-01-02")
    lake.items.ingest("published", rows.with_columns(pl.lit(3.).alias("value")),
                      available_date="2020-01-03")
    requests = {"item": ItemInput("published", view="versions")}
    first = lake.inputs.freeze(requests, information_cutoff="2020-01-04")
    token = lake.inputs.selection_identity(first, "item", view="snapshot")
    assert token is not None
    assert lake.inputs.read(first, "item", view="snapshot").collect()["value"].to_list() == [3.]

    # This new immutable version is present in the forest, but the consumer
    # still selects January 3. Its scoped numerical identity must stay equal.
    lake.items.ingest("published", rows.with_columns(pl.lit(2.).alias("value")),
                      available_date="2020-01-02")
    second = lake.inputs.freeze(requests, information_cutoff="2020-01-04")
    assert first.digest != second.digest
    assert token == lake.inputs.selection_identity(second, "item", view="snapshot")
    assert lake.inputs.read(second, "item", view="snapshot").collect()["value"].to_list() == [3.]

    # A genuinely selected revision must invalidate the same consumer token.
    lake.items.ingest("published", rows.with_columns(pl.lit(4.).alias("value")),
                      available_date="2020-01-04")
    third = lake.inputs.freeze(requests, information_cutoff="2020-01-04")
    assert token != lake.inputs.selection_identity(third, "item", view="snapshot")
    assert lake.inputs.read(third, "item", view="snapshot").collect()["value"].to_list() == [4.]


def test_new_publication_indexes_only_new_batch_in_index_free_store(tmp_path):
    lake = prepared(tmp_path)
    with lake._data_meta.connect() as db:
        db.execute("DROP TABLE input_selection_index")
        db.execute("DROP TABLE input_batch_index")
    reopened = DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path)
    with reopened._data_meta.connect() as db:
        assert not input_index.available(db)
    reopened.items.register(DataItemSpec("new_external"))
    reopened.items.ingest("new_external", pl.DataFrame({"time": [date(2020, 1, 1)],
        "asset_id": ["A"], "value": [1.]}), available_date="2020-01-02")
    with reopened._data_meta.connect() as db:
        assert db.execute("SELECT count(*) FROM input_batch_index").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM version_batches").fetchone()[0] > 1


def test_damaged_index_ipc_closes_large_mapping(tmp_path, monkeypatch):
    import mmap
    import tempfile
    mapped, original = [], mmap.mmap
    def track(*args, **kwargs):
        mapping = original(*args, **kwargs)
        mapped.append(mapping)
        return mapping
    monkeypatch.setattr(mmap, "mmap", track)
    with tempfile.TemporaryFile() as transport:
        transport.write(b"invalid IPC" * 100)
        transport.seek(0)
        with pytest.raises(RuntimeError, match="IPC is damaged"):
            with input_index._arrow_index_reader(transport, 1100, 64):
                pytest.fail("damaged IPC was admitted")
    assert mapped and all(mapping.closed for mapping in mapped)


def test_index_consumer_failure_keeps_exception_and_closes_mappings(tmp_path, monkeypatch):
    from datetime import UTC, datetime
    import mmap
    from bagelquant_data import DatasetSpec
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.raw.ingest(DatasetSpec("general", "general", source="fake"),
        pl.DataFrame({"asset_id": ["A"], "value": [1.]}), ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    frozen = lake.inputs.freeze({"general": RawInput("fake", "general", view="snapshot")}, information_cutoff="2020-01-03")
    mapped, original_map, original_decode = [], mmap.mmap, pl.from_arrow
    error = TypeError("original indexed consumer failure")
    def track(*args, **kwargs):
        mapping = original_map(*args, **kwargs)
        mapped.append(mapping)
        return mapping
    def decode(value, **kwargs):
        if "_index_content" in value.schema.names:
            raise error
        return original_decode(value, **kwargs)
    monkeypatch.setattr(mmap, "mmap", track)
    monkeypatch.setattr(pl, "from_arrow", decode)
    from bagelquant_data.execution import ExecutionOptions
    with pytest.raises(TypeError) as caught:
        lake.inputs.verify(frozen, config=ExecutionOptions(max_buffer_bytes=8192))
    assert caught.value is error
    assert mapped and all(mapping.closed for mapping in mapped)


@pytest.mark.parametrize("context_budget", [None, 1024 * 1024])
def test_full_summary_audit_respects_explicit_budget(tmp_path, monkeypatch, context_budget):
    from contextlib import nullcontext
    from bagelquant_data import ExecutionOptions
    lake = prepared(tmp_path)
    frozen = lake.inputs.freeze({"item": ItemInput("item")}, information_cutoff="2020-03-03")
    original, seen = input_index.read, []
    def tracked(*args, **kwargs):
        seen.append(kwargs["max_bytes"])
        return original(*args, **kwargs)
    monkeypatch.setattr(input_index, "read", tracked)
    scope = nullcontext() if context_budget is None else lake.inputs.read_context(
        frozen, config=ExecutionOptions(max_buffer_bytes=context_budget))
    with scope, pytest.raises(MemoryError, match="Selection summary audit"):
        lake.inputs.verify(frozen, config=ExecutionOptions(max_buffer_bytes=256))
    assert seen and max(seen) <= 256 // 4


def test_summary_audit_cancellation_does_not_report_valid(tmp_path, monkeypatch):
    lake = prepared(tmp_path)
    frozen = lake.inputs.freeze({"item": ItemInput("item")}, information_cutoff="2020-03-03")
    original, canceled = lake.inputs._indexed_selection, False
    def tracked(*args, **kwargs):
        nonlocal canceled
        result = original(*args, **kwargs)
        canceled = True
        return result
    def check():
        if canceled:
            raise CancelledError()
    monkeypatch.setattr(lake.inputs, "_indexed_selection", tracked)
    with pytest.raises(CancelledError):
        lake.inputs.verify(frozen, check_canceled=check)
