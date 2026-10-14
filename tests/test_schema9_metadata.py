"""Shared metadata preserves typed/PIT results and bounded ordinary reads."""
from dataclasses import replace
from datetime import UTC, date, datetime
import json

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, RawInput
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
    assert json.loads(immutable_metadata.decode_requests(lake._data_meta, encoded)) == requests


def test_explicit_audit_detects_mutated_shared_descendant(tmp_path):
    lake, _ = _raw(tmp_path)
    source = lake.inputs.freeze({"prices": RawInput("custom", "prices")}, information_cutoff="2024-03-01")
    with lake._data_meta.connect() as db:
        row = db.execute("SELECT digest FROM immutable_metadata WHERE kind='mapping' LIMIT 1").fetchone()
        db.execute("UPDATE immutable_metadata SET payload_json='{}' WHERE digest=?", (row["digest"],))
    with pytest.raises(RuntimeError, match="checksum"):
        lake.inputs.verify(source)
