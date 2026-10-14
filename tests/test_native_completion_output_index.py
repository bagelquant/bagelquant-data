"""Explicit old-native output boundary maintenance preserves immutable evidence."""
import hashlib
import json
import sqlite3
from datetime import date

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, ItemInput, ItemPublication


def legacy_native(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    rows = pl.DataFrame({"time": [date(2024, 1, 3)], "asset_id": ["A"], "value": [1.],
        "version_available_date": [date(2024, 1, 4)]})
    lake.items.register(DataItemSpec("input"))
    lake.items.ingest("input", rows, available_date="2024-01-04")
    source = lake.inputs.freeze({"input": ItemInput("input")}, information_cutoff="2024-01-04")
    lake.items.register(DataItemSpec("output", (ItemInput("input"),)))
    with lake.items.publication(input_receipt=source) as publisher:
        publisher.publish([ItemPublication("output", rows, start="2024-01-03",
            end="2024-01-04", complete_at="2024-01-04")])
    build = lake.items.completed_builds("output")[-1]
    output = lake.inputs.freeze({"output": ItemInput("output", start="2024-01-01",
        end="2024-01-04", view="snapshot")}, information_cutoff="2024-01-04", retain_batch_bounds=True)
    assert output.evidence["output"]["empty_item_build"] is None
    # Emulate a native authority persisted before check ceilings were recorded.
    with sqlite3.connect(lake.data_meta_path) as db:
        payload = json.loads(db.execute("select payload_json from item_completion_authorities where build_id=?",
            (build["id"],)).fetchone()[0])
        del payload["output_check_bound"]
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        db.execute("update item_completion_authorities set payload_json=?,digest=? where build_id=?",
            (raw, hashlib.sha256(raw.encode()).hexdigest(), build["id"]))
    return lake, source, build, output, rows


def admit(lake, build, output, **changes):
    return lake.items.index_completion_output(build["id"], **{
        "output_receipt_id": output.receipt_id, "output_receipt_digest": output.digest,
        "report_id": "original-app-publication", "report_digest": "a" * 64, **changes})


def test_original_native_output_index_is_immutable_and_value_free(tmp_path, monkeypatch):
    lake, source, build, output, rows = legacy_native(tmp_path)
    before = lake.items.manifest("output")
    with sqlite3.connect(lake.data_meta_path) as db:
        original_authority = db.execute("select * from item_completion_authorities").fetchall()
        original_pending = db.execute("select * from item_pending_outputs").fetchall()
    def forbidden(*args, **kwargs):
        raise AssertionError("Original native boundary maintenance must only read metadata")
    from bagelquant_data import inputs
    monkeypatch.setattr(inputs, "read_batch", forbidden)
    first = admit(lake, build, output)
    assert admit(lake, build, output) == first
    assert first["output_check_bound"] == output.max_check_id
    with pytest.raises(ValueError, match="immutable"):
        admit(lake, build, output, report_id="different-report")
    with lake.items.publication(input_receipt=source) as publisher:
        result = publisher.publish([ItemPublication("output", rows.clear(), start="2024-01-03",
            end="2024-01-04", complete_at="2024-01-04", reuse_build_id=build["id"], affected_ranges=())])
    assert result["output"] is not None and result["output"].rows_committed == 0
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("select * from item_completion_authorities where build_id=?", (build["id"],)).fetchall() == original_authority
        assert db.execute("select * from item_pending_outputs where build_id=?", (build["id"],)).fetchall() == original_pending
    assert lake.items.manifest("output") == before and build in lake.items.completed_builds("output")


def test_new_backdated_check_is_not_absorbed(tmp_path):
    lake, source, build, output, rows = legacy_native(tmp_path)
    lake.items.ingest("output", rows, available_date="2024-01-04", input_receipt=source)
    admit(lake, build, output)
    with pytest.raises(ValueError, match="later output timing"):
        with lake.items.publication(input_receipt=source) as publisher:
            publisher.publish([ItemPublication("output", rows.clear(), start="2024-01-03",
                end="2024-01-04", complete_at="2024-01-04", reuse_build_id=build["id"], affected_ranges=())])


@pytest.mark.parametrize("change", ["digest", "definition", "foreign_batch", "damaged_index"])
def test_native_output_binding_rejects_foreign_or_damaged_proofs(tmp_path, change):
    from dataclasses import replace
    lake, source, build, output, rows = legacy_native(tmp_path)
    if change == "digest":
        with pytest.raises(ValueError, match="identity"):
            admit(lake, build, output, output_receipt_digest="f" * 64)
    elif change == "definition":
        lake.items.register(replace(lake.items.get("output"), metadata={"revision": 2}))
        foreign = lake.inputs.freeze(output.requests, information_cutoff="2024-01-04", retain_batch_bounds=True)
        with pytest.raises(ValueError, match="definition"):
            admit(lake, build, foreign)
    elif change == "foreign_batch":
        current = lake.inputs.freeze(source.requests, information_cutoff="2024-01-04")
        lake.items.ingest("output", rows.with_columns(pl.lit(2.).alias("value")),
            available_date="2024-01-04", input_receipt=current)
        foreign = lake.inputs.freeze(output.requests, information_cutoff="2024-01-04", retain_batch_bounds=True)
        with pytest.raises(ValueError, match="batch source/range"):
            admit(lake, build, foreign)
    else:
        admit(lake, build, output)
        with sqlite3.connect(lake.data_meta_path) as db:
            db.execute("update item_completion_output_receipts set payload_json='{}'")
        with pytest.raises(RuntimeError, match="damaged"):
            lake.items.completed_builds("output")
