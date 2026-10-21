"""Scoped proofs skip disjoint headers without changing selection semantics."""
from datetime import date, timedelta

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, ItemInput
from bagelquant_data.storage import immutable_metadata


@pytest.fixture
def history(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("value"))
    first = date(2023, 1, 1)
    for index in range(130):
        day = first + timedelta(days=index)
        lake.items.ingest("value", pl.DataFrame({"time": [day], "asset_id": ["A"], "value": [float(index)]}),
                          available_date=day)
    frozen = lake.inputs.freeze({"value": ItemInput("value", view="versions", start=first,
        end=first + timedelta(days=129))}, information_cutoff="2023-06-01")
    return lake, frozen, first


@pytest.mark.parametrize("start,end,cutoff", [
    (0, 0, 0), (63, 64, 64), (127, 129, 128), (65, 65, 60),
    (None, 2, 129), (128, None, 129), (None, None, 129),
])
def test_scoped_proof_matches_full_traversal_without_original_reads(history, monkeypatch, start, end, cutoff):
    from bagelquant_data import inputs

    lake, frozen, first = history
    def forbidden(*args, **kwargs):
        pytest.fail("A record proof must not decode original bytes, index, or freeze inputs")
    monkeypatch.setattr(inputs, "read_batch", forbidden)
    monkeypatch.setattr(lake.inputs, "_indexed_selection", forbidden)
    monkeypatch.setattr(lake.inputs, "freeze", forbidden)
    options = dict(start=None if start is None else first + timedelta(days=start),
        end=None if end is None else first + timedelta(days=end),
        as_of=first + timedelta(days=cutoff), current_as_of="2023-06-01",
        values_only=True, causal_versions=True, records_only=True)
    optimized = lake.inputs.selection_equivalent(frozen, frozen, "value", **options)
    monkeypatch.setattr(immutable_metadata.LazyMetadataSequence, "selected", lambda self, *args: iter(self))
    reference = lake.inputs.selection_equivalent(frozen, frozen, "value", **options)
    assert optimized is reference
    assert lake.inputs.get(frozen).digest == frozen.digest


def test_small_scope_decodes_only_intersecting_headers(history, monkeypatch):
    lake, frozen, first = history
    original, reads = immutable_metadata.MetadataReader.block, []
    def block(self, reference):
        reads.append(reference)
        return original(self, reference)
    monkeypatch.setattr(immutable_metadata.MetadataReader, "block", block)
    options = dict(start=first + timedelta(days=64), end=first + timedelta(days=64),
        values_only=True, causal_versions=True, records_only=True)
    assert lake.inputs.selection_equivalent(frozen, frozen, "value", **options) is True
    optimized = len(reads)
    reads.clear()
    monkeypatch.setattr(immutable_metadata.LazyMetadataSequence, "selected", lambda self, *args: iter(self))
    assert lake.inputs.selection_equivalent(frozen, frozen, "value", **options) is True
    assert optimized < len(reads) / 10


def test_disjoint_corruption_is_still_rejected_by_explicit_audit(history):
    import sqlite3
    lake, frozen, first = history
    batches = frozen.evidence["value"]["batches"]
    reference = batches[-1].reference
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("UPDATE immutable_metadata SET payload_json='{}' WHERE digest=?", (reference["ref"],))
    assert lake.inputs.selection_equivalent(frozen, frozen, "value", start=first, end=first,
        values_only=True, causal_versions=True, records_only=True) is True
    with pytest.raises(RuntimeError, match="checksum"):
        lake.inputs.verify(frozen)


def test_missing_bounds_remain_conservative(tmp_path):
    import sqlite3
    with sqlite3.connect(tmp_path / "blocks.sqlite") as db:
        db.executescript(immutable_metadata.SCHEMA)
        reference = immutable_metadata.save(db, [
            {"value": "unknown"}, {"value": "outside", "min_observation": "2025-01-01"},
        ])
        sequence = immutable_metadata.MetadataReader(database=db).value(reference)
        # Ordinary mappings without complete segment bounds cannot be excluded.
        assert [dict(item) for item in sequence.selected("2023-01-01", "2023-01-02", "2023-01-02")] == [
            {"value": "unknown"}, {"value": "outside", "min_observation": "2025-01-01"},
        ]


def test_optional_raw_summary_bounds_do_not_leak_to_next_batch(tmp_path, monkeypatch):
    from dataclasses import replace
    from datetime import UTC, datetime
    from bagelquant_data import DatasetSpec, RawInput, input_index
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("raw", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    for day in (1, 3):
        lake.raw.ingest(spec, pl.DataFrame({"time": [date(2024, 1, day)], "asset_id": ["A"], "value": [float(day)]}),
                        mode="initialize" if day == 1 else "incremental", ingested_at=datetime(2024, 1, day, tzinfo=UTC))
        frozen = lake.inputs.freeze({"raw": RawInput("custom", "raw", view="versions")}, information_cutoff="2024-01-31")
        # Exercise retained older evidence lacking receipt-level bound authority.
        evidence = {**frozen.evidence, "raw": {**frozen.evidence["raw"],
            "scoped_batches": False, "retained_batch_bounds": False}}
        if day == 1:
            before = replace(frozen, evidence=evidence)
        else:
            after = replace(frozen, evidence=evidence)
    first = before.evidence["raw"]["batches"][0]["content_hash"]
    def summary(db, batch):
        return {"record": True, "lower": "2024-01-01", "upper": "2024-01-01",
                "available": "2024-01-01"} if batch["content_hash"] == first else None
    monkeypatch.setattr(input_index, "describe_bounds", summary)
    # The second batch lacks authorized summary bounds, even though the first
    # has them. Retaining it must leave the comparison unknown, not prove empty.
    assert lake.inputs._selection_equivalent(before, after, "raw", start="2024-01-10",
        end="2024-01-11", records_only=True) is None
