"""Shared metadata preserves typed/PIT results and bounded ordinary reads."""
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
import json

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, InputSelectionScope, ItemInput, RawInput
from bagelquant_data.storage import immutable_metadata
from test_selection_descriptors import setup


def _raw(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("prices", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    for month in (1, 2, 3):
        day = date(2024, month, 1)
        lake.raw.ingest(spec, pl.DataFrame({"time": [day], "asset_id": ["A"], "close": [float(month)]}),
            ingested_at=datetime(2024, month, 1, tzinfo=UTC))
    return lake, spec


def test_repeated_freeze_shares_headers_and_schema_and_noop_never_expands(tmp_path, monkeypatch):
    lake, spec = _raw(tmp_path)
    requests = {"prices": RawInput("custom", "prices", view="snapshot")}
    first = lake.inputs.freeze(requests, information_cutoff="2024-03-01")
    with lake._data_meta.connect() as db:
        before = db.execute("SELECT count(*) FROM immutable_metadata").fetchone()[0]
        header = json.loads(db.execute("SELECT payload_json FROM frozen_inputs WHERE receipt_id=?", (first.receipt_id,)).fetchone()[0])
        assert "batches" not in header["evidence"]["prices"]
        assert len(json.dumps(header)) < 4096
        assert db.execute("SELECT count(*) FROM immutable_schemas").fetchone()[0] == 1
        assert "schema_ipc" not in {row[1] for row in db.execute("PRAGMA table_info(version_segments)")}
    second = lake.inputs.freeze(requests, information_cutoff="2024-03-01")
    assert second.receipt_id == first.receipt_id
    with lake._data_meta.connect() as db:
        assert db.execute("SELECT count(*) FROM immutable_metadata").fetchone()[0] == before
    token = lake.inputs.request_identity(first, "prices")
    with monkeypatch.context() as guard:
        guard.setattr(immutable_metadata.MetadataReader, "block", lambda *_: pytest.fail("no-op expanded historical metadata"))
        guard.setattr(lake.inputs, "_capture", lambda *_a, **_k: pytest.fail("no-op recaptured historical metadata"))
        assert lake.inputs.get(first.receipt_id).digest == first.digest
        assert lake.inputs.describe(first)["aliases"]["prices"]["batch_count"] == 3
        assert lake.inputs.request_identity(second, "prices") == token
        assert lake.inputs.is_current(first)
    assert lake.inputs.read(first, "prices").collect()["close"].to_list() == [1., 2., 3.]
    visited = []
    value = immutable_metadata.MetadataReader.value
    def record(reader, reference):
        resolved = value(reader, reference)
        if isinstance(reference, dict) and reference.get("entity") == "segment":
            visited.append((resolved["commit_seq"], resolved["partition_path"]))
        return resolved
    with monkeypatch.context() as guard:
        guard.setattr(immutable_metadata.MetadataReader, "value", record)
        bounded = lake.inputs.read(first, "prices", start="2024-02-01", end="2024-02-01").collect()
    assert bounded["close"].to_list() == [2.]
    assert {coordinate[0] for coordinate in visited} == {2}
    original_token = lake.inputs.range_identity(first, "prices", end="2024-02-01", fields=("close",))
    lake.raw.ingest(spec, pl.DataFrame({"time": [date(2024, 4, 1)], "asset_id": ["A"], "close": [4.]}),
        ingested_at=datetime(2024, 4, 1, tzinfo=UTC))
    third = lake.inputs.freeze(requests, information_cutoff="2024-04-01")
    assert lake.inputs.range_identity(third, "prices", end="2024-02-01", fields=("close",)) == original_token
    assert lake.inputs.read(first, "prices").collect()["close"].to_list() == [1., 2., 3.]
    assert lake.inputs.read(third, "prices").collect()["close"].to_list() == [1., 2., 3., 4.]


def test_selection_set_headers_are_constant_work_and_selected_keys_are_exact(tmp_path, monkeypatch):
    lake, _, source, scope, _ = setup(tmp_path)
    assert scope.coordinates is not None
    lake.inputs.prepare_coordinate_selector(scope.coordinates)
    scopes = [replace(scope, fields=fields) for fields in (("value",), ("value", "available_date"))]
    descriptors = lake.inputs.prepare_range_selections(source, scopes)
    reference = lake.inputs.register_selection_set(source, descriptors)
    assert lake.inputs.register_selection_set(source, descriptors) == reference
    with monkeypatch.context() as guard:
        guard.setattr(immutable_metadata.MetadataReader, "block", lambda *_: pytest.fail("set header expanded historical blocks"))
        guard.setattr(lake.inputs, "resolve_selection_descriptors", lambda *_a, **_k: pytest.fail("header re-resolved historical proofs"))
        assert lake.inputs.describe_selection_set(reference, receipt=source) == reference
    key = lake.inputs.selection_descriptor_identity(descriptors[1])
    assert lake.inputs.read_selection_set(reference, keys=[key]) == (descriptors[1],)
    assert lake.inputs.read_selection_set(reference, keys=[]) == ()
    assert lake.inputs.read_selection_set(reference) == descriptors
    assert lake.inputs.verify(source)["valid"]
    with pytest.raises(KeyError):
        lake.inputs.read_selection_set(reference, keys=["unknown"])
    with pytest.raises(RuntimeError, match="identity"):
        lake.inputs.describe_selection_set({**reference, "count": reference["count"] + 1})


def test_completed_item_currentness_uses_only_its_live_completion_authority(tmp_path, monkeypatch):
    lake, spec = _raw(tmp_path)
    lake.items.register_producer("close", "1", lambda context: context.read("raw").select(
        pl.col("source_time").alias("time"), "asset_id", pl.col("close").alias("value")))
    lake.items.register(DataItemSpec("close", (RawInput("custom", "prices", alias="raw"),), producer_key="close", producer_revision="1"))
    lake.items.initialize("close", start="2024-01-01", end="2024-03-01")
    source = lake.inputs.freeze({"close": ItemInput("close", view="history")}, information_cutoff="2024-03-01")
    with monkeypatch.context() as guard:
        guard.setattr(immutable_metadata.MetadataReader, "block", lambda *_: pytest.fail("complete Item expanded historical Source metadata"))
        guard.setattr(lake.inputs, "_capture", lambda *_a, **_k: pytest.fail("complete Item recaptured history"))
        assert lake.inputs.is_current(source)
    lake.raw.ingest(spec, pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "close": [99.]}),
        ingested_at=datetime(2024, 2, 1, tzinfo=UTC))
    assert not lake.inputs.is_current(source)


def test_request_templates_are_shared_and_commit_records_replay_logically(tmp_path):
    lake, _ = _raw(tmp_path)
    requests = [{"scope_id": index, "params": {"fields": [f"field_{i}" for i in range(120)],
        "asset": f"A{index}"}} for index in range(100)]
    with lake._data_meta.connect() as db:
        encoded = immutable_metadata.encode_requests(db, requests)
        # The fields list exists once; scopes only bind their distinct deltas.
        raw_bytes = len(json.dumps(requests).encode())
        stored = db.execute("SELECT sum(length(payload_json)) FROM immutable_metadata").fetchone()[0]
        assert len(encoded) < 200 and stored < raw_bytes // 2
        assert json.loads(immutable_metadata.decode_requests(db, encoded)) == requests


def test_explicit_audit_detects_mutated_shared_descendant(tmp_path):
    lake, _ = _raw(tmp_path)
    source = lake.inputs.freeze({"prices": RawInput("custom", "prices")}, information_cutoff="2024-03-01")
    with lake._data_meta.connect() as db:
        row = db.execute("SELECT digest FROM immutable_metadata WHERE kind='mapping' LIMIT 1").fetchone()
        db.execute("UPDATE immutable_metadata SET payload_json='{}' WHERE digest=?", (row["digest"],))
    with pytest.raises(RuntimeError, match="checksum"):
        lake.inputs.verify(source)


def _selection_chunks(lake, reference):
    reader = immutable_metadata.MetadataReader(lake._data_meta)
    with lake._data_meta.connect() as db:
        row = db.execute("SELECT m.digest,m.kind,m.item_count FROM input_selection_sets s "
            "JOIN immutable_metadata m ON m.digest=s.root_ref WHERE s.id=?", (reference["id"],)).fetchone()
        body = reader.block({"ref": row["digest"], "kind": row["kind"], "count": row["item_count"]})
        chunks, start = [], 0
        for value in reader.value(body["blocks"]):
            chunks.append({**dict(value), "start": start})
            start += value["count"]
        chunks = tuple(chunks)
        entries = tuple(db.execute("SELECT e.descriptor_key,e.reference_json FROM input_selection_chunk_entries e "
            "JOIN json_each(?) b ON e.block_ref=json_extract(b.value,'$.ref') "
            "ORDER BY json_extract(b.value,'$.start'),e.ordinal", (json.dumps(chunks),)))
        count = db.execute("SELECT count(*) FROM input_selection_chunk_entries").fetchone()[0]
    return chunks, entries, count


def test_append_shares_original_selection_bodies_and_complete_chunk_indexes(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("x"))
    days = [date(2024, 1, 1) + timedelta(days=index) for index in range(130)]
    def frame(selected):
        return pl.DataFrame({"time": selected, "asset_id": ["A"] * len(selected), "value": [1.] * len(selected),
            "available_date": selected, "observation_date": selected, "base_available_date": selected})
    def prepare(source, selected):
        scopes = [InputSelectionScope("x", start=day, end=day, view="snapshot", fields=("value",)) for day in selected]
        return tuple(value for first in range(0, len(scopes), 16)
            for value in lake.inputs.prepare_range_selections(source, scopes[first:first + 16]))
    lake.items.ingest("x", frame(days[:-1]))
    first = lake.inputs.freeze({"x": ItemInput("x", view="history")}, information_cutoff=days[-2], retain_batch_bounds=True)
    before = prepare(first, days[:-1])
    set1 = lake.inputs.register_selection_set(first, before)
    chunks1, entries1, count1 = _selection_chunks(lake, set1)
    lake.items.ingest("x", frame(days[-1:]))
    second = lake.inputs.freeze(first.requests, information_cutoff=days[-1], retain_batch_bounds=True)
    after = prepare(second, days)
    set2 = lake.inputs.register_selection_set(second, after)
    chunks2, entries2, count2 = _selection_chunks(lake, set2)
    assert first.receipt_id != second.receipt_id
    assert all(old["requested_cutoff"] != new["requested_cutoff"] for old, new in zip(before, after, strict=False))
    assert all(lake.inputs.selection_descriptor_identity(old) == lake.inputs.selection_descriptor_identity(new)
        for old, new in zip(before, after, strict=False))
    assert all(tuple(old) == tuple(new) for old, new in zip(entries1, entries2, strict=False))
    assert chunks1[:2] == chunks2[:2] and count1 == 129 and count2 == 131
    key = lake.inputs.selection_descriptor_identity(before[0])
    assert lake.inputs.read_selection_set(set1, keys=[key]) == before[:1]
    assert lake.inputs.read_selection_set(set2, keys=[key]) == after[:1]
    assert lake.inputs.read(first, "x").collect().height == 129
    assert lake.inputs.read(second, "x").collect().height == 130
    assert lake.inputs.verify(second)["valid"]
    revision = frame(days[:1]).with_columns(pl.lit(9.).alias("value"),
        pl.lit(days[-1]).alias("available_date"), pl.lit(days[-1]).alias("base_available_date"))
    lake.items.ingest("x", revision)
    third = lake.inputs.freeze(first.requests, information_cutoff=days[-1], retain_batch_bounds=True)
    changed = prepare(third, days[:1])[0]
    assert changed["content_token"] != before[0]["content_token"]
    assert lake.inputs.selection_descriptor_identity(changed) != key
    assert lake.inputs.read_selection_set(set1, keys=[key]) == before[:1]
    assert lake.inputs.read_selection_set(set2, keys=[key]) == after[:1]
    assert lake.inputs.read(first, "x", start=days[0], end=days[0], view="snapshot").collect()["value"].to_list() == [1.]
    assert lake.inputs.read(third, "x", start=days[0], end=days[0], view="snapshot").collect()["value"].to_list() == [9.]


def test_selection_requested_boundary_is_reconstructed_and_explicit_asof_is_bound(tmp_path):
    lake, _, source, scope, _ = setup(tmp_path)
    assert scope.coordinates is not None
    lake.inputs.prepare_coordinate_selector(scope.coordinates)
    scopes = [scope, replace(scope, as_of="2024-01-04"), replace(scope, as_of="2024-01-05")]
    descriptors = lake.inputs.prepare_range_selections(source, scopes)
    assert len({lake.inputs.selection_descriptor_identity(value) for value in descriptors}) == 3
    assert descriptors[1]["canonical_cutoff"] == descriptors[2]["canonical_cutoff"]
    reference = lake.inputs.register_selection_set(source, descriptors)
    assert lake.inputs.read_selection_set(reference) == descriptors
    with pytest.raises((RuntimeError, ValueError), match="original|registered|source|Source"):
        lake.inputs.register_selection_set(source, [dict(descriptors[0], requested_cutoff="2024-01-01")])
    with pytest.raises(ValueError, match="cutoff"):
        lake.inputs.prepare_range_selections(source, [replace(scope, as_of="2025-01-01")])
    with lake._data_meta.connect() as db:
        db.execute("UPDATE input_selection_chunk_entries SET ordinal=-1 WHERE descriptor_key=?",
            (lake.inputs.selection_descriptor_identity(descriptors[0]),))
    with pytest.raises(RuntimeError, match="ordinal"):
        lake.inputs.read_selection_set(reference)


def test_selection_body_and_original_source_tampering_are_rejected(tmp_path):
    lake, _, source, scope, _ = setup(tmp_path)
    assert scope.coordinates is not None
    lake.inputs.prepare_coordinate_selector(scope.coordinates)
    descriptor, = lake.inputs.prepare_range_selections(source, [scope])
    reference = lake.inputs.register_selection_set(source, [descriptor])
    with lake._data_meta.connect() as db:
        row = db.execute("SELECT reference_json FROM input_selection_chunk_entries").fetchone()
        body_ref = json.loads(row[0])["ref"]
        original = db.execute("SELECT payload_json FROM immutable_metadata WHERE digest=?", (body_ref,)).fetchone()[0]
        db.execute("UPDATE immutable_metadata SET payload_json='{}' WHERE digest=?", (body_ref,))
    with pytest.raises(RuntimeError, match="checksum"):
        lake.inputs.read_selection_set(reference)
    with pytest.raises(RuntimeError, match="checksum"):
        lake.inputs.verify(source)
    with lake._data_meta.connect() as db:
        db.execute("UPDATE immutable_metadata SET payload_json=? WHERE digest=?", (original, body_ref))
        db.execute("UPDATE frozen_inputs SET digest='tampered' WHERE receipt_id=?", (source.receipt_id,))
    with pytest.raises(RuntimeError, match="checksum|Source|identity"):
        lake.inputs.read_selection_set(reference)


def test_earlier_alias_append_preserves_later_alias_member_chunks_and_indexes(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    days = [date(2024, 1, 1) + timedelta(days=index) for index in range(130)]
    frame = pl.DataFrame({"time": days[:-1], "asset_id": ["A"] * 129, "value": [1.] * 129,
        "available_date": days[:-1], "observation_date": days[:-1], "base_available_date": days[:-1]})
    for name in ("x", "y"):
        lake.items.register(DataItemSpec(name))
        lake.items.ingest(name, frame)
    first = lake.inputs.freeze({name: ItemInput(name, view="history") for name in ("x", "y")},
        information_cutoff=days[-2], retain_batch_bounds=True)
    def prepare(source, sizes):
        descriptors = []
        for name, count in sizes:
            scopes = [InputSelectionScope(name, start=day, end=day, view="snapshot", fields=("value",)) for day in days[:count]]
            for index in range(0, count, 16):
                descriptors.extend(lake.inputs.prepare_range_selections(source, scopes[index:index + 16]))
        return tuple(descriptors)
    before = prepare(first, [("x", 129), ("y", 129)])
    set1 = lake.inputs.register_selection_set(first, before)
    chunks1, entries1, count1 = _selection_chunks(lake, set1)
    tail = frame.tail(1).with_columns(pl.lit(days[-1]).alias("time"), pl.lit(days[-1]).alias("available_date"),
        pl.lit(days[-1]).alias("observation_date"), pl.lit(days[-1]).alias("base_available_date"))
    lake.items.ingest("x", tail)
    second = lake.inputs.freeze(first.requests, information_cutoff=days[-1], retain_batch_bounds=True)
    after = prepare(second, [("x", 130), ("y", 129)])
    set2 = lake.inputs.register_selection_set(second, after)
    chunks2, entries2, count2 = _selection_chunks(lake, set2)
    assert len(chunks1) == len(chunks2) == 6
    assert [(chunk["ref"], chunk["count"]) for chunk in chunks1[3:]] == [
        (chunk["ref"], chunk["count"]) for chunk in chunks2[3:]]
    assert all(tuple(old) == tuple(new) for old, new in zip(entries1[129:], entries2[130:], strict=True))
    assert count1 == 258 and count2 == 260
    key = lake.inputs.selection_descriptor_identity(before[129])
    assert key == lake.inputs.selection_descriptor_identity(after[130])
    assert lake.inputs.read_selection_set(set1, keys=[key]) == before[129:130]
    assert lake.inputs.read_selection_set(set2, keys=[key]) == after[130:131]
