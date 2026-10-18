"""Whole-set audits reuse inventory while rechecking every bounded member."""

from collections import Counter
from copy import deepcopy
from datetime import UTC, date, datetime, timedelta
import hashlib
import json

import polars as pl
import pytest

from bagelquant_data import (
    DataItemSpec, DataLake, DatasetSpec, ExecutionOptions, InputSelectionScope, ItemInput, RawInput,
)
from bagelquant_data import input_index, inputs
from bagelquant_data.storage import immutable_metadata


def prepared(root):
    lake = DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")
    days = [date(2024, 1, 1) + timedelta(days=index) for index in range(145)]
    frame = pl.DataFrame({"time": days, "asset_id": ["A"] * len(days),
        "value": [float(index) for index in range(len(days))]})
    raw = RawInput("custom", "quotes", view="snapshot")
    lake.raw.ingest(DatasetSpec("quotes", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"}), frame,
        ingested_at=datetime(2024, 5, 25, tzinfo=UTC))
    parent = lake.inputs.freeze({"raw": raw}, information_cutoff=days[-1])
    lake.items.register(DataItemSpec("x", (raw,)))
    lake.items.ingest("x", frame.with_columns(pl.col("time").alias("available_date"),
        pl.col("time").alias("observation_date"), pl.col("time").alias("base_available_date")),
        input_receipt=parent)
    source = lake.inputs.freeze({"x": ItemInput("x", view="history")},
        information_cutoff=days[-1], retain_batch_bounds=True)
    descriptors = []
    for first in range(0, len(days), 16):
        scopes = [InputSelectionScope("x", start=day, end=day, view="snapshot", fields=("value",))
            for day in days[first:first + 16]]
        descriptors.extend(lake.inputs.prepare_range_selections(source, scopes))
    references = [lake.inputs.register_selection_set(source, group)
        for group in (descriptors, descriptors[:129])]
    assert all(len(lake.inputs._selection_membership(reference)[3]) >= 3 for reference in references)
    return lake, source, tuple(descriptors), references


def test_audit_prepares_each_inventory_once_and_matches_unoptimized_reference(tmp_path, monkeypatch):
    lake, source, descriptors, references = prepared(tmp_path)
    original_member_read = lake.inputs._read_selection_members
    original_resolve = lake.inputs.resolve_selection_descriptors
    original_membership = lake.inputs._selection_membership
    originals = {path.relative_to(lake.lake_path): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in lake.lake_path.rglob("*.parquet")}
    source_identity = lake.inputs.request_identity(source, "x")
    prepared_sets, resolved = [], []
    def tracked_membership(reference, **kwargs):
        prepared_sets.append(reference["id"])
        return original_membership(reference, **kwargs)
    def tracked_resolve(receipt, group, **kwargs):
        resolved.append((receipt, deepcopy(group)))
        return original_resolve(receipt, group, **kwargs)
    def unoptimized(header, _reader, _members, _blocks, **kwargs):
        membership = lake.inputs._selection_membership(header, config=kwargs.get("config"),
            check_canceled=kwargs.get("check_canceled", lambda: None))
        return original_member_read(*membership, **kwargs)
    monkeypatch.setattr(lake.inputs, "_selection_membership", tracked_membership)
    monkeypatch.setattr(lake.inputs, "resolve_selection_descriptors", tracked_resolve)
    with monkeypatch.context() as patch:
        patch.setattr(lake.inputs, "_read_selection_members", unoptimized)
        expected_progress = []
        expected = lake.inputs.verify(source, progress=expected_progress.append)
    reference_resolved = deepcopy(resolved)
    assert len(prepared_sets) == 2 + (145 + 15) // 16 + (129 + 15) // 16
    prepared_sets.clear()
    resolved.clear()
    progress = []
    assert lake.inputs.verify(source, progress=progress.append) == expected
    assert progress == expected_progress
    assert Counter(prepared_sets) == Counter({reference["id"]: 1 for reference in references})
    assert resolved == reference_resolved
    assert sum(len(group) for _, group in resolved) == 145 + 129
    assert expected["upstream_receipt_count"] == 1
    assert lake.inputs.request_identity(source, "x") == source_identity
    assert {path.relative_to(lake.lake_path): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in lake.lake_path.rglob("*.parquet")} == originals
    assert lake.inputs.read_selection_set(references[0]) == descriptors


def test_public_keyed_reads_prepare_fresh_and_keep_budget_and_key_checks(tmp_path, monkeypatch):
    lake, _, descriptors, references = prepared(tmp_path)
    membership = lake.inputs._selection_membership
    calls = []
    def tracked(reference, **kwargs):
        calls.append(reference["id"])
        return membership(reference, **kwargs)
    monkeypatch.setattr(lake.inputs, "_selection_membership", tracked)
    key = lake.inputs.selection_descriptor_identity(descriptors[-1])
    for _ in range(2):
        assert lake.inputs.read_selection_set(references[0], keys=[key]) == descriptors[-1:]
    assert calls == [references[0]["id"]] * 2
    assert lake.inputs.read_selection_set(references[0], keys=[]) == ()
    with pytest.raises(KeyError, match="unknown"):
        lake.inputs.read_selection_set(references[0], keys=["unknown"])
    with pytest.raises(ValueError, match="finite sequence"):
        lake.inputs.read_selection_set(references[0], keys=key)
    keys = [lake.inputs.selection_descriptor_identity(value) for value in descriptors[:16]]
    with pytest.raises(MemoryError, match="Selected descriptors"):
        lake.inputs.read_selection_set(references[0], keys=keys,
            config=ExecutionOptions(max_buffer_bytes=64 * 1024))


@pytest.mark.parametrize("damage", ["chunk", "ordinal", "descriptor", "selection_source", "proof_source"])
def test_later_audit_rejects_corrupt_late_members_and_original_sources(tmp_path, damage):
    lake, source, descriptors, references = prepared(tmp_path)
    assert lake.inputs.verify(source)["valid"]
    late = descriptors[-1]
    key = lake.inputs.selection_descriptor_identity(late)
    with lake._data_meta.connect() as db:
        if damage == "chunk":
            blocks = lake.inputs._selection_membership(references[0])[3]
            db.execute("UPDATE immutable_metadata SET payload_json='[]' WHERE digest=?", (blocks[-1]["ref"],))
        elif damage == "ordinal":
            db.execute("UPDATE input_selection_chunk_entries SET ordinal=-1 WHERE descriptor_key=?", (key,))
        elif damage == "descriptor":
            replacement = {name: value for name, value in late.items() if name not in {"source", "requested_cutoff"}}
            replacement["content_token"] = "forged"
            reference = immutable_metadata.save(db, replacement)
            db.execute("UPDATE input_selection_chunk_entries SET reference_json=? WHERE descriptor_key=?",
                (json.dumps(reference), key))
        elif damage == "selection_source":
            db.execute("UPDATE input_selection_sets SET source_digest='forged' WHERE id=?", (references[0]["id"],))
        else:
            row = db.execute("SELECT * FROM input_projection_index WHERE evidence_key=? AND request_key=?",
                (late["evidence_key"], late["request_key"])).fetchone()
            body = {name: row[name] for name in row.keys() if name != "digest"}
            body["source_receipt_digest"] = "forged"
            db.execute("UPDATE input_projection_index SET source_receipt_digest=?,digest=? WHERE evidence_key=? AND request_key=?",
                ("forged", input_index._digest(body), late["evidence_key"], late["request_key"]))
    with pytest.raises(RuntimeError, match="checksum|ordinal|membership|identity|[Ss]ource"):
        lake.inputs.verify(source)
    assert lake.inputs._read_context() is None


def test_cancel_after_member_group_discards_inventory_and_rechecks_all_originals(tmp_path, monkeypatch):
    lake, source, _, _ = prepared(tmp_path)
    read_members = lake.inputs._read_selection_members
    verify_batch = inputs.verify_batch
    canceled = False
    batches = []
    class Canceled(Exception):
        pass
    def check():
        if canceled:
            raise Canceled()
    def after_group(*args, **kwargs):
        nonlocal canceled
        result = read_members(*args, **kwargs)
        canceled = True
        return result
    def tracked_batch(*args, **kwargs):
        batches.append((args[1], args[2], args[3]))
        return verify_batch(*args, **kwargs)
    monkeypatch.setattr(inputs, "verify_batch", tracked_batch)
    with monkeypatch.context() as patch:
        patch.setattr(lake.inputs, "_read_selection_members", after_group)
        with pytest.raises(Canceled):
            lake.inputs.verify(source, check_canceled=check)
    assert lake.inputs._read_context() is None and not batches
    canceled = False
    assert lake.inputs.verify(source)["valid"]
    assert batches and len(batches) == len(set(batches))


def test_writing_shared_view_revalidates_membership_instead_of_reusing_stale_header(tmp_path, monkeypatch):
    lake, source, _, _ = prepared(tmp_path)
    read_members = lake.inputs._read_selection_members
    def mutate_after_group(header, *args, **kwargs):
        result = read_members(header, *args, **kwargs)
        with lake._data_meta.connect() as db:
            db.execute("UPDATE input_selection_sets SET source_digest='forged' WHERE id=?", (header["id"],))
        return result
    with monkeypatch.context() as patch:
        patch.setattr(lake.inputs, "_read_selection_members", mutate_after_group)
        with pytest.raises(RuntimeError, match="identity|[Ss]ource"):
            with lake.inputs.publication_guard(source) as reader:
                reader.verify(source)
    assert lake.inputs._read_context() is None
    assert lake.inputs.verify(source)["valid"]
