"""Range publication expands only the aliases owned by its original proofs."""
from collections import Counter
from datetime import UTC, date, datetime, timedelta

import polars as pl

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
