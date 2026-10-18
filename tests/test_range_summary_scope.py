"""Range publication expands only the aliases owned by its original proofs."""
from collections import Counter
from datetime import UTC, date, datetime, timedelta
import hashlib
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, InputSelectionScope, ItemInput, RawInput
from bagelquant_data import input_index, inputs
from bagelquant_data.storage import immutable_metadata


DAY = date(2024, 1, 1)


def setup(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    raw = RawInput("custom", "quotes", view="snapshot")
    spec = DatasetSpec("quotes", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [DAY + timedelta(days=i) for i in range(16)],
        "asset_id": ["A"] * 16, "close": [float(i + 1) for i in range(16)]})
    lake.raw.ingest(spec, frame, ingested_at=datetime(2024, 1, 16, tzinfo=UTC))
    parent = lake.inputs.freeze({"raw": raw}, information_cutoff="2024-01-16")
    lake.items.register(DataItemSpec("x", inputs=(raw,)))
    lake.items.ingest("x", frame.rename({"close": "value"}),
        input_receipt=parent, available_date="2024-01-16")
    root = lake.inputs.freeze({"selected": ItemInput("x", view="snapshot"),
        **{f"unused_{i}": ItemInput("x", view="snapshot") for i in range(31)}},
        information_cutoff="2024-01-16")
    scopes = tuple(InputSelectionScope("selected", start=DAY + timedelta(days=i),
        end=DAY + timedelta(days=i), view="snapshot", fields=("value",)) for i in range(16))
    return lake, root, parent, scopes, spec, raw


def test_preparation_does_not_expand_unconsumed_aliases(tmp_path, monkeypatch):
    lake, root, parent, scopes, _, _ = setup(tmp_path)
    expected = lake.inputs.selection_descriptors(root, scopes)
    getitem = immutable_metadata.FrozenEvidenceMapping.__getitem__
    save = input_index.save_projection
    accessed, published = Counter(), []

    def selected_only(self, alias):
        assert not alias.startswith("unused_"), "Unconsumed alias evidence was expanded"
        accessed[alias] += 1
        return getitem(self, alias)

    def checked_save(db, evidence, request, value, **source):
        assert db.in_transaction
        assert value["parents"] == (parent.receipt_id,)
        assert source == {"receipt_id": root.receipt_id, "receipt_digest": root.digest, "alias": "selected"}
        published.append(value["identity"])
        return save(db, evidence, request, value, **source)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("Range summary preparation read original numerical values")

    monkeypatch.setattr(immutable_metadata.FrozenEvidenceMapping, "__getitem__", selected_only)
    monkeypatch.setattr(input_index, "save_projection", checked_save)
    monkeypatch.setattr(inputs, "read_batch", forbidden)
    monkeypatch.setattr(input_index, "project_original", forbidden)
    assert lake.inputs.prepare_range_selections(root, scopes) == expected
    assert lake.inputs.prepare_range_selections(root, scopes) == expected
    assert set(accessed) == {"selected"}
    assert published == [row["content_token"] for row in expected] * 2
    assert lake.inputs.get(root).digest == root.digest


def test_renamed_consumer_keeps_original_proof_alias_and_parents(tmp_path, monkeypatch):
    lake, original, parent, scopes, spec, raw = setup(tmp_path)
    first, = lake.inputs.prepare_range_selections(original, scopes[:1])
    tail_day = DAY + timedelta(days=16)
    frame = pl.DataFrame({"time": [tail_day], "asset_id": ["A"], "close": [17.]})
    lake.raw.ingest(spec, frame, ingested_at=datetime(2024, 1, 17, tzinfo=UTC))
    tail_parent = lake.inputs.freeze({"raw": raw}, information_cutoff=tail_day)
    lake.items.ingest("x", frame.rename({"close": "value"}), input_receipt=tail_parent,
        available_date=tail_day)
    current = lake.inputs.freeze({"renamed": ItemInput("x", view="snapshot")}, information_cutoff=tail_day)
    scope = InputSelectionScope("renamed", start=DAY, end=DAY, view="snapshot", fields=("value",))
    expected, = lake.inputs.selection_descriptors(current, [scope])
    assert expected["alias"] == "renamed"
    assert expected["proof_source"] == {"id": original.receipt_id, "digest": original.digest, "alias": "selected"}
    assert expected["content_token"] == first["content_token"]
    assert set(current.evidence["renamed"]["parent_receipts"]) == {parent.receipt_id, tail_parent.receipt_id}
    save = input_index.save_projection
    published = []

    def checked_save(db, evidence, request, value, **source):
        assert value["parents"] == (parent.receipt_id,)
        assert source == {"receipt_id": original.receipt_id, "receipt_digest": original.digest, "alias": "selected"}
        published.append(value["identity"])
        return save(db, evidence, request, value, **source)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("Unchanged history was numerically read")

    monkeypatch.setattr(input_index, "save_projection", checked_save)
    monkeypatch.setattr(inputs, "read_batch", forbidden)
    monkeypatch.setattr(input_index, "project_original", forbidden)
    assert lake.inputs.prepare_range_selections(current, [scope]) == (expected,)
    assert published == [first["content_token"]]
    assert lake.inputs.get(original).digest == original.digest


@pytest.mark.parametrize("view", ["snapshot", "history", "versions"])
def test_empty_item_range_summary_audits_original_and_independent_snapshot(tmp_path, monkeypatch, view):
    lake, original, _, _, _, _ = setup(tmp_path / "original")
    first, last = DAY - timedelta(days=31), DAY - timedelta(days=1)
    scope = InputSelectionScope("selected", start=first, end=last, view=view,
        causal_versions=view == "versions")
    identity = lake.inputs.selection_identity(original, "selected", start=first, end=last,
        view=view, values_only=True, causal_versions=view == "versions")
    originals = {path.relative_to(lake.lake_path): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in lake.lake_path.rglob("*.parquet")}
    def forbidden(*_args, **_kwargs):
        pytest.fail("Empty range metadata preparation decoded Original values")
    with monkeypatch.context() as patch:
        patch.setattr(inputs, "read_batch", forbidden)
        patch.setattr(input_index, "project_original", forbidden)
        descriptor, = lake.inputs.prepare_range_selections(original, [scope])
        assert descriptor["content_token"] == identity
        assert descriptor["has_rows"] is False
        assert lake.inputs.prepare_range_selections(original, [scope]) == (descriptor,)
        assert lake.inputs.selection_identity(original, "selected", start=first, end=last,
            view=view, values_only=True, causal_versions=view == "versions") == identity
    assert lake.inputs.verify(original)["valid"]
    copy = tmp_path / "copy"
    lake.integrity.snapshot(data_meta_path=copy / "meta.sqlite", lake_path=copy / "lake")
    with DataLake.open(data_meta_path=copy / "meta.sqlite", lake_path=copy / "lake", read_only=True) as copied:
        assert copied.inputs.get(original.receipt_id).digest == original.digest
        assert copied.inputs.verify(original.receipt_id)["valid"]
        assert {path.relative_to(copied.lake_path): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in copied.lake_path.rglob("*.parquet")} == originals
    assert lake.inputs.get(original).digest == original.digest
    assert {path.relative_to(lake.lake_path): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in lake.lake_path.rglob("*.parquet")} == originals


def test_empty_item_range_audit_rejects_self_consistent_wrong_summary(tmp_path):
    lake, original, _, _, _, _ = setup(tmp_path)
    scope = InputSelectionScope("selected", start=DAY - timedelta(days=1),
        end=DAY - timedelta(days=1), view="snapshot")
    lake.inputs.prepare_range_selections(original, [scope])
    with sqlite3.connect(lake.data_meta_path) as db:
        db.row_factory = sqlite3.Row
        row = dict(db.execute("select * from input_projection_index").fetchone())
        assert row["version"] == "segment_range.v1" and not row["has_rows"]
        row["identity"] = "0" * 64
        digest = input_index._digest({key: value for key, value in row.items() if key != "digest"})
        db.execute("update input_projection_index set identity=?,digest=?", (row["identity"], digest))
    with pytest.raises(RuntimeError, match="Scalar selection summary differs from original evidence"):
        lake.inputs.verify(original)


def test_empty_item_range_audit_still_checks_out_of_window_original_bytes(tmp_path):
    lake, original, _, _, _, _ = setup(tmp_path)
    scope = InputSelectionScope("selected", start=DAY - timedelta(days=1),
        end=DAY - timedelta(days=1), view="snapshot")
    lake.inputs.prepare_range_selections(original, [scope])
    assert lake.inputs.verify(original)["valid"]
    path = next(lake.lake_path.rglob("*.parquet"))
    path.write_bytes(path.read_bytes() + b"corruption")
    with pytest.raises(RuntimeError, match="Original Parquet segment .*checksum"):
        lake.inputs.verify(original)
