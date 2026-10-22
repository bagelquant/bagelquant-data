"""Finite metadata reuse never substitutes for fresh owner authority."""

from collections import Counter
from contextlib import closing, contextmanager, ExitStack
from datetime import UTC, date, datetime
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataLake, ExecutionOptions, ItemInput
from bagelquant_data.storage import immutable_metadata
from test_input_read_context import frozen_months


def trace_blocks(monkeypatch, store):
    queries = Counter()
    connect = type(store).connect

    @contextmanager
    def traced(self, *args, **kwargs):
        with connect(self, *args, **kwargs) as db:
            db.set_trace_callback(lambda sql: queries.update([sql])
                if "FROM immutable_metadata WHERE digest=" in sql else None)
            yield db

    monkeypatch.setattr(type(store), "connect", traced)
    return queries


def entered_memo(reader):
    state = reader._read_context()
    assert state is not None
    memo = state.metadata_memo
    assert memo is not None
    return memo


def test_nested_readers_decode_each_original_once_and_release_on_exit(tmp_path, monkeypatch):
    lake, frozen = frozen_months(tmp_path)
    queries = trace_blocks(monkeypatch, lake._data_meta)
    with lake.inputs.read_context(frozen) as reader:
        original = reader.get(frozen)
        expected = original.evidence["raw"]["batches"][0]["content_hash"]
        for _ in range(40):
            assert original.evidence["raw"]["batches"][0]["content_hash"] == expected
        assert queries and max(queries.values()) == 1
        before = queries.copy()
        nested = DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path,
            read_only=True, runtime=True)
        memo = entered_memo(reader)
        with nested.inputs.read_context(frozen) as second:
            assert entered_memo(second) is memo
            assert second.get(frozen).evidence["raw"]["batches"][0]["content_hash"] == expected
        assert queries == before
        assert memo.hits > 40 and 0 < memo.resident_bytes <= memo.maximum_bytes
    assert memo.resident_bytes == 0 and not memo.entries
    with lake.inputs.read_context(frozen) as reader:
        assert reader.get(frozen).evidence["raw"]["batches"][0]["content_hash"] == expected
    assert max(queries.values()) == 2


@pytest.mark.parametrize("buffer_bytes", [1, 1024, 1024 * 1024])
def test_small_budget_falls_back_without_changing_evidence(tmp_path, buffer_bytes):
    lake, frozen = frozen_months(tmp_path)
    expected = immutable_metadata.canonical(frozen.evidence)
    with lake.inputs.read_context(frozen, config=ExecutionOptions(max_buffer_bytes=buffer_bytes)) as reader:
        actual = immutable_metadata.canonical(reader.get(frozen).evidence)
        memo = entered_memo(reader)
        assert memo.resident_bytes <= buffer_bytes // 16
        assert memo.peak_bytes <= memo.maximum_bytes
    assert actual == expected and memo.resident_bytes == 0


def test_writer_change_disables_warmed_blocks_including_savepoint_rollback(tmp_path):
    lake, frozen = frozen_months(tmp_path)
    assert isinstance(frozen.evidence, immutable_metadata.FrozenEvidenceMapping)
    reference = frozen.evidence.references["raw"]
    with lake._data_meta.publication_read_view() as db:
        with lake.inputs.read_context(frozen) as reader:
            retained = reader.get(frozen).evidence["raw"]
            assert retained["definition_hash"]
            memo = entered_memo(reader)
            assert memo.entries
            db.execute("SAVEPOINT damage")
            db.execute("UPDATE immutable_metadata SET payload_json='{}' WHERE digest=?", (reference["ref"],))
            with pytest.raises(RuntimeError, match="checksum mismatch"):
                retained["definition_hash"]
            assert not memo.entries
            db.execute("ROLLBACK TO damage")
            assert retained["definition_hash"]
            assert reader._metadata_memo() is memo and not memo.enabled
            assert not memo.entries


def test_later_context_rejects_corrupt_original_and_failure_releases_cache(tmp_path):
    lake, frozen = frozen_months(tmp_path)
    assert isinstance(frozen.evidence, immutable_metadata.FrozenEvidenceMapping)
    with lake.inputs.read_context(frozen) as reader:
        retained = reader.get(frozen).evidence["raw"]
        assert retained["definition_hash"]
        memo = entered_memo(reader)
    with lake._data_meta.connect() as db:
        db.execute("UPDATE immutable_metadata SET payload_json='{}' WHERE digest=?",
            (frozen.evidence.references["raw"]["ref"],))
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        with lake.inputs.read_context(frozen) as reader:
            reader.get(frozen).evidence["raw"]["definition_hash"]
    assert reader._read_context() is None and memo.resident_bytes == 0


def test_unscoped_mapping_keeps_one_local_decode(tmp_path, monkeypatch):
    lake, frozen = frozen_months(tmp_path)
    queries = trace_blocks(monkeypatch, lake._data_meta)
    view = lake.inputs.get(frozen).evidence["raw"]
    assert isinstance(view, immutable_metadata.LazyMetadataMapping)
    assert dict(view)
    root = view.reference["ref"]
    assert sum(count for sql, count in queries.items() if root in sql) == 1


def test_external_write_preserves_entered_snapshot_and_is_rechecked_after_exit(tmp_path):
    lake, frozen = frozen_months(tmp_path)
    assert isinstance(frozen.evidence, immutable_metadata.FrozenEvidenceMapping)
    reference = frozen.evidence.references["raw"]
    with lake.inputs.read_context(frozen) as reader:
        view = reader.get(frozen).evidence["raw"]
        original = view["definition_hash"]
        memo = entered_memo(reader)
        with closing(sqlite3.connect(lake.data_meta_path)) as writer:
            writer.execute("UPDATE immutable_metadata SET payload_json='{}' WHERE digest=?",
                (reference["ref"],))
            writer.commit()
        assert view["definition_hash"] == original
        assert memo.enabled
    assert memo.resident_bytes == 0
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        view["definition_hash"]


def test_alternating_nested_budgets_share_one_shrinking_reserve(tmp_path):
    lake, frozen = frozen_months(tmp_path)
    memos = []
    with ExitStack() as stack:
        for index in range(100):
            options = ExecutionOptions(max_buffer_bytes=65536 + 16 * (index % 2))
            reader = stack.enter_context(lake.inputs.read_context(frozen, config=options))
            assert reader.get(frozen).evidence["raw"]["batches"][0]["content_hash"]
            memo = entered_memo(reader)
            memos.append(memo)
            assert memo.maximum_bytes <= 65536 // 16
            assert memo.resident_bytes <= memo.maximum_bytes
    assert len({id(memo) for memo in memos}) == 1
    assert memo.resident_bytes == 0


def test_omitted_nested_budget_inherits_parent_and_explicit_budget_shrinks(tmp_path):
    lake, frozen = frozen_months(tmp_path)
    options = ExecutionOptions(max_buffer_bytes=128 * 1024 * 1024)
    with lake.inputs.read_context(frozen, config=options) as reader:
        memo = entered_memo(reader)
        with reader.read_context(frozen):
            state = reader._read_context()
            assert state is not None and state.options is options
            assert entered_memo(reader) is memo
            assert memo.maximum_bytes == options.max_buffer_bytes // 16
        with reader.read_context(frozen, config=ExecutionOptions(max_buffer_bytes=1024)):
            assert entered_memo(reader) is memo and memo.maximum_bytes == 64


def test_schema_reuse_authenticates_again_after_write_and_exit(tmp_path, monkeypatch):
    lake, frozen = frozen_months(tmp_path)
    reads = []
    original = immutable_metadata.schema_bytes
    def counted(db, digest):
        reads.append(digest)
        return original(db, digest)
    monkeypatch.setattr(immutable_metadata, "schema_bytes", counted)
    with lake._data_meta.publication_read_view() as db:
        with lake.inputs.read_context(frozen) as reader:
            evidence = reader.get(frozen).evidence["raw"]
            encoded = evidence["schema_ipc"]
            for _ in range(10):
                assert evidence["schema_ipc"] == encoded
            assert len(reads) == 1
            db.execute("SAVEPOINT damage_schema")
            db.execute("UPDATE immutable_schemas SET schema_ipc=x'00' WHERE digest=?", (reads[0],))
            with pytest.raises(RuntimeError, match="schema checksum mismatch"):
                evidence["schema_ipc"]
            db.execute("ROLLBACK TO damage_schema")
    with lake.inputs.read_context(frozen) as reader:
        assert reader.get(frozen).evidence["raw"]["schema_ipc"] == encoded
    assert len(reads) == 3


@pytest.mark.parametrize("buffer_bytes", [1, 8 * 1024 * 1024])
def test_currentness_shares_exact_capture_across_registered_sources(tmp_path, monkeypatch, buffer_bytes):
    lake, frozen = frozen_months(tmp_path)
    # A different dataset advances the database, preserving this Raw selection.
    lake.items.update("derived", start="2020-01-01", end="2020-03-31")
    later = lake.inputs.freeze(frozen.requests, information_cutoff=frozen.information_cutoff)
    assert frozen.receipt_id != later.receipt_id
    captures = []
    original = type(lake.inputs)._capture
    def counted(self, *args, **kwargs):
        captures.append(kwargs["cutoff"])
        return original(self, *args, **kwargs)
    monkeypatch.setattr(type(lake.inputs), "_capture", counted)
    monkeypatch.setattr(type(lake.inputs), "_completed_source_current", lambda *_args: None)
    monkeypatch.setattr(type(lake.inputs), "_unchanged_alias_state", lambda *_args: False)
    with lake.inputs.read_context([frozen, later], config=ExecutionOptions(max_buffer_bytes=buffer_bytes)) as reader:
        assert reader.is_current([frozen, later])
        assert entered_memo(reader).resident_bytes <= buffer_bytes // 16
    assert len(captures) == (1 if buffer_bytes > 1 else 2)
    with lake.inputs.read_context(frozen) as reader:
        assert reader.is_current(frozen)
    assert len(captures) == (2 if buffer_bytes > 1 else 3)


def test_current_capture_shares_shrinking_reserve_and_expires_on_write(tmp_path):
    lake, frozen = frozen_months(tmp_path)
    options = ExecutionOptions(max_buffer_bytes=8 * 1024 * 1024)
    with lake._data_meta.publication_read_view() as db:
        with lake.inputs.read_context(frozen, config=options) as reader:
            original = reader.get(frozen)
            reader._current_selection_receipt(original, cutoff=original.information_cutoff)
            memo = entered_memo(reader)
            assert any(key[0] == "current_capture" for key in memo.entries)
            with reader.read_context(frozen, config=ExecutionOptions(max_buffer_bytes=1024)):
                assert entered_memo(reader) is memo
                assert memo.resident_bytes <= 64 and not memo.entries
            assert memo.maximum_bytes == 64
            db.execute("SAVEPOINT generation")
            db.execute("UPDATE datasets SET active=0 WHERE source='custom' AND name='values'")
            with pytest.raises(KeyError, match="Unknown active input"):
                reader._current_selection_receipt(original, cutoff=original.information_cutoff)
            db.execute("ROLLBACK TO generation")
            reader._current_selection_receipt(original, cutoff=original.information_cutoff)
            assert not memo.enabled and not memo.entries
    assert memo.resident_bytes == 0


def test_equal_item_content_still_checks_original_creator_freshness(tmp_path, monkeypatch):
    lake, _ = frozen_months(tmp_path)
    lake.items.update("derived", start="2020-01-01", end="2020-03-31")
    frozen = lake.inputs.freeze({"item": ItemInput("derived", start="2020-01-01", end="2020-03-31", view="versions")},
        information_cutoff="2020-04-01")
    captures = []
    capture = type(lake.inputs)._capture
    def counted(self, *args, **kwargs):
        captures.append(tuple(args[1]))
        return capture(self, *args, **kwargs)
    monkeypatch.setattr(type(lake.inputs), "_capture", counted)
    with lake.inputs.read_context(frozen):
        assert lake.inputs.is_current(frozen)
    assert not captures
    # The Item itself has no new publication, but its original creator is stale.
    lake.raw.ingest(lake.raw.get("values", source="custom"),
        pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [9.]}),
        ingested_at=datetime(2020, 3, 15, tzinfo=UTC))
    with lake.inputs.read_context(frozen):
        assert not lake.inputs.is_current(frozen)
    assert captures
