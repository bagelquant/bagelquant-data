"""Explicit original completion attestations can continue without numerical work."""
from datetime import UTC, date, datetime
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, ItemCompletionAttestation, ItemInput, ItemPublication


def prepared(tmp_path, *, pending=(), later_check=False, bind_output=True):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    rows = pl.DataFrame({"time": [date(2024, 1, 1), date(2024, 1, 2)],
                         "asset_id": ["A", "A"], "value": [1., 2.]})
    lake.items.register(DataItemSpec("input"))
    lake.items.ingest("input", rows, available_date="2024-01-02")
    request = ItemInput("input")
    original = lake.inputs.freeze({"input": request}, information_cutoff="2024-01-02")
    lake.items.register(DataItemSpec("output", (request,)))
    lake.items.ingest("output", rows, available_date="2024-01-02", input_receipt=original)
    build = lake.items.builds("output")[-1]
    assert not lake.items.completed_builds("output")
    original_output = lake.inputs.freeze({"output": ItemInput("output", start="2024-01-01", end="2024-01-02", view="snapshot")},
                                        information_cutoff="2024-01-02")
    if later_check:
        lake.items.ingest("output", rows, available_date="2024-01-04", input_receipt=original,
                          ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    lake.items.index_completion(ItemCompletionAttestation(
        build_id=build["id"], name="output", definition_hash=build["definition_hash"],
        start=date(2024, 1, 1), end=date(2024, 1, 2), input_commit=build["input_commit"],
        result_commit=build["result_commit"], dependency_digest=build["dependency_digest"],
        frozen_receipt_id=original.receipt_id, frozen_receipt_digest=original.digest,
        information_cutoff=original.information_cutoff, report_id="original-complete-producer-report",
        report_digest="a" * 64, complete_output_range=True, pending_coordinates=pending,
        output_receipt_id=original_output.receipt_id if bind_output else None,
        output_receipt_digest=original_output.digest if bind_output else None))
    current = lake.inputs.freeze({"input": request}, information_cutoff="2024-01-04")
    publication = ItemPublication("output", rows.head(0), expected_definition_hash=build["definition_hash"],
        start="2024-01-01", end="2024-01-02", complete_at=current.information_cutoff,
        reuse_build_id=build["id"], affected_ranges=())
    return lake, original, current, build, publication


def test_attested_noop_preserves_values_receipts_and_is_native_idempotent(tmp_path, monkeypatch):
    lake, original, current, build, publication = prepared(tmp_path)
    before = lake.items.manifest("output")
    original_output = lake.inputs.freeze({"output": ItemInput("output")}, information_cutoff="2024-01-02")
    retained = lake.inputs.read(original_output, "output").collect()
    with monkeypatch.context() as patched:
        def forbidden(*args, **kwargs):
            pytest.fail("No-op must not read/replace historical numerical values")
        patched.setattr(lake.items, "_replace_range", forbidden)
        patched.setattr(lake.inputs, "read", forbidden)
        with lake.items.publication(input_receipt=current) as publisher:
            first = publisher.publish([publication])["output"]
        count = len(lake.items.completed_builds("output"))
        with lake.items.publication(input_receipt=current) as publisher:
            repeated = publisher.publish([publication])["output"]
    assert first is not None and repeated is not None
    assert first.rows_committed == repeated.rows_committed == 0
    assert len(lake.items.completed_builds("output")) == count == 2
    assert build in lake.items.completed_builds("output")
    assert lake.items.manifest("output") == before
    assert lake.inputs.get(original).digest == original.digest
    assert lake.inputs.read(original_output, "output").collect().equals(retained)
    output = lake.inputs.freeze({"output": ItemInput("output", start="2024-01-01", end="2024-01-02")},
                               information_cutoff=current.information_cutoff)
    assert lake.inputs.is_current(output)


def test_attested_noop_rejects_newly_mature_pending_coordinates(tmp_path):
    lake, _, current, _, publication = prepared(tmp_path, pending=((date(2024, 1, 1), date(2024, 1, 3)),))
    with pytest.raises(ValueError, match="newly mature"):
        with lake.items.publication(input_receipt=current) as publisher:
            publisher.publish([publication])


def test_attested_noop_rejects_damaged_original_attestation(tmp_path):
    lake, _, current, build, publication = prepared(tmp_path)
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update item_completion_attestations set payload_json='{}' where build_id=?", (build["id"],))
    with pytest.raises(RuntimeError, match="damaged"):
        with lake.items.publication(input_receipt=current) as publisher:
            publisher.publish([publication])


def test_attested_sparse_changed_ranges_still_require_native_authority(tmp_path):
    lake, _, current, build, _ = prepared(tmp_path)
    changed = pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "value": [3.]})
    publication = ItemPublication("output", changed, expected_definition_hash=build["definition_hash"],
        start="2024-01-01", end="2024-01-02", complete_at=current.information_cutoff,
        reuse_build_id=build["id"], affected_ranges=(("2024-01-01", "2024-01-01"),))
    with pytest.raises(ValueError, match="native whole-range"):
        with lake.items.publication(input_receipt=current) as publisher:
            publisher.publish([publication])


def test_later_backdated_timing_check_is_not_absorbed_by_completion_maintenance(tmp_path):
    lake, _, current, _, publication = prepared(tmp_path, later_check=True)
    with pytest.raises(ValueError, match="later output timing"):
        with lake.items.publication(input_receipt=current) as publisher:
            publisher.publish([publication])


def test_attestation_without_original_output_binding_cannot_sparse_reuse(tmp_path):
    lake, _, current, _, publication = prepared(tmp_path, bind_output=False)
    with pytest.raises(ValueError, match="native whole-range"):
        with lake.items.publication(input_receipt=current) as publisher:
            publisher.publish([publication])
