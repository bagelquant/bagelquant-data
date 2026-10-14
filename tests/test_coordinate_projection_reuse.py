"""General axes inherit exact original atomic evidence, not enclosing receipts."""
from dataclasses import replace
from datetime import UTC, date, datetime
import json
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, ExecutionOptions, RawInput, input_index
from bagelquant_data import inputs


def setup(tmp_path, *, baseline=False, empty=False):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("calendar", "general")
    frame = pl.DataFrame({"time": [date(2024, 1, 1), date(2024, 1, 2)], "is_open": [1, 1], "unused": [1, 2]})
    if empty:
        frame = frame.clear()
    lake.raw.ingest(spec, frame, mode="initialize" if baseline else "incremental",
        ingested_at=datetime(2024, 1, 1, tzinfo=UTC))
    def freeze(cutoff="2024-01-03", **kwargs):
        return lake.inputs.freeze({"calendar": RawInput("custom", "calendar", view="versions", **kwargs)},
            information_cutoff=cutoff)
    root = freeze()
    return lake, spec, frame, root, freeze


def forbidden(*_args, **_kwargs):
    raise AssertionError("Ordinary coordinate proof consumed values or wrote metadata")


def test_calendar_authority_inherits_new_source_and_explicit_audit_original_closure(tmp_path, monkeypatch):
    lake, _, _, original, freeze = setup(tmp_path)
    lake.inputs.index_coordinates(original, "calendar", selector=("is_open", 1))
    current = freeze("2024-01-04")
    assert current.digest != original.digest
    with monkeypatch.context() as patch:
        patch.setattr(lake.inputs, "read", forbidden)
        patch.setattr(input_index, "initialize", forbidden)
        old = lake.inputs.coordinate_identity(original, "calendar", selector=("is_open", 1))
        assert old == lake.inputs.coordinate_identity(current, "calendar", selector=("is_open", 1))
        lake.inputs.index_coordinates(current, "calendar", selector=("is_open", 1))
    with sqlite3.connect(lake.data_meta_path) as db:
        roots = db.execute("select distinct receipt_digest from input_coordinate_index").fetchall()
        assert roots == [(original.digest,)]
        assert db.execute("select source_receipt_id from input_projection_index").fetchone()[0] == original.receipt_id
    assert lake.inputs.verify(current)["valid"]
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update frozen_inputs set payload_json=payload_json||' ' where receipt_id=?", (original.receipt_id,))
    with pytest.raises(RuntimeError, match="checksum"):
        lake.inputs.coordinate_identity(current, "calendar", selector=("is_open", 1))
    with pytest.raises(RuntimeError, match="checksum"):
        lake.inputs.verify(current)


def test_rebound_changed_atomic_prefix_uses_registered_previous_source_proof(tmp_path, monkeypatch):
    lake, spec, frame, old, freeze = setup(tmp_path)
    lake.inputs.index_coordinates(old, "calendar", selector=("is_open", 1))
    lake.raw.ingest(spec, pl.concat([frame, frame.head(1).with_columns(pl.lit(date(2024, 1, 5)).alias("time"))]),
        ingested_at=datetime(2024, 1, 5, tzinfo=UTC))
    indexed = freeze("2024-01-06")
    assert lake.inputs.coordinate_equivalent(old, indexed, "calendar", selector=("is_open", 1), end="2024-01-02") is None
    lake.inputs.index_coordinates(indexed, "calendar", selector=("is_open", 1))
    current = freeze("2024-01-07")
    monkeypatch.setattr(lake.inputs, "read", forbidden)
    assert lake.inputs.coordinate_equivalent(old, current, "calendar", selector=("is_open", 1), end="2024-01-02") is True
    assert lake.inputs.coordinate_equivalent(old, current, "calendar", selector=("is_open", 1)) is False
    assert lake.inputs.coordinate_identity(current, "calendar", selector=("is_open", 0)) is None


@pytest.mark.parametrize("strict,baseline,empty", [(False, False, False), (False, True, False), (False, False, True)])
def test_typed_empty_and_baseline_remain_bound_and_cutoff_cannot_select_future(tmp_path, strict, baseline, empty):
    lake, _, _, original, freeze = setup(tmp_path, baseline=baseline, empty=empty)
    lake.inputs.index_coordinates(original, "calendar", selector=("is_open", 1))
    same = freeze("2024-01-04")
    assert lake.inputs.coordinate_identity(original, "calendar", selector=("is_open", 1)) == (
        lake.inputs.coordinate_identity(same, "calendar", selector=("is_open", 1)))
    before = freeze("2023-12-31", include_historical_baseline=False, strict=strict)
    assert lake.inputs.coordinate_identity(before, "calendar", selector=("is_open", 1)) is None
    if baseline:
        excluded = freeze("2024-01-04", strict=True)
        assert lake.inputs.coordinate_identity(excluded, "calendar", selector=("is_open", 1)) is None
    assert lake.inputs.verify(same)["valid"]


@pytest.mark.parametrize("mutation", ["payload", "locator", "missing_payload"])
def test_damaged_or_missing_canonical_coordinate_authority_is_not_positive(tmp_path, mutation):
    lake, _, _, original, freeze = setup(tmp_path)
    lake.inputs.index_coordinates(original, "calendar", selector=("is_open", 1))
    current = freeze("2024-01-04")
    with sqlite3.connect(lake.data_meta_path) as db:
        if mutation == "payload":
            db.execute("update input_coordinate_index set dates_json='[]'")
        elif mutation == "locator":
            db.execute("update input_projection_index set identity=?", ("f" * 64,))
        else:
            db.execute("delete from input_coordinate_index")
    if mutation == "missing_payload":
        assert lake.inputs.coordinate_identity(current, "calendar", selector=("is_open", 1)) is None
        with pytest.raises(RuntimeError, match="payload"):
            lake.inputs.verify(current)
    else:
        with pytest.raises(RuntimeError, match="damaged"):
            lake.inputs.coordinate_identity(current, "calendar", selector=("is_open", 1))


def test_repaired_checksums_cannot_forge_coordinate_payload_audit(tmp_path):
    lake, _, _, original, freeze = setup(tmp_path)
    lake.inputs.index_coordinates(original, "calendar", selector=("is_open", 1))
    current = freeze("2024-01-04")
    with sqlite3.connect(lake.data_meta_path) as db:
        selection, = db.execute("select selection from input_coordinate_index").fetchone()
        dates = ["2024-01-01"]
        digest = input_index._digest({"receipt": original.digest, "alias": "calendar", "selection": selection, "dates": dates})
        db.execute("update input_coordinate_index set dates_json=?,digest=?", (json.dumps(dates), digest))
        db.row_factory = sqlite3.Row
        row = dict(db.execute("select * from input_projection_index").fetchone())
        row["identity"] = input_index.coordinate_content_identity(dates)
        row["digest"] = input_index._digest({key: value for key, value in row.items() if key != "digest"})
        db.execute("update input_projection_index set identity=?,digest=?", (row["identity"], row["digest"]))
    with pytest.raises(RuntimeError, match="Coordinate index differs"):
        lake.inputs.verify(current)


@pytest.mark.parametrize("field", ["is_open"])
def test_consumed_schema_conflict_blocks_cached_locator_before_original_reads(tmp_path, monkeypatch, field):
    lake, spec, frame, original, freeze = setup(tmp_path)
    lake.inputs.index_coordinates(original, "calendar", selector=("is_open", 1))
    lake.raw.ingest(spec, frame.with_columns(pl.col(field).cast(pl.String)),
        ingested_at=datetime(2024, 1, 8, tzinfo=UTC))
    captured_future = freeze("2024-01-03")
    monkeypatch.setattr(lake.inputs, "read", forbidden)
    with pytest.raises(ValueError, match="conflicting captured"):
        lake.inputs.coordinate_identity(captured_future, "calendar", selector=("is_open", 1))
    with pytest.raises(ValueError, match="conflicting captured"):
        lake.inputs.coordinate_equivalent(original, captured_future, "calendar", selector=("is_open", 1))
    assert lake.inputs.get(original).digest == original.digest


def test_unconsumed_future_schema_change_preserves_selected_atomic_locator(tmp_path, monkeypatch):
    lake, spec, frame, original, freeze = setup(tmp_path)
    lake.inputs.index_coordinates(original, "calendar", selector=("is_open", 1))
    lake.raw.ingest(spec, frame.with_columns(pl.lit(1.5).alias("unused")),
        ingested_at=datetime(2024, 1, 8, tzinfo=UTC))
    future = freeze("2024-01-03")
    monkeypatch.setattr(lake.inputs, "read", forbidden)
    assert lake.inputs.coordinate_identity(original, "calendar", selector=("is_open", 1)) == (
        lake.inputs.coordinate_identity(future, "calendar", selector=("is_open", 1)))


def test_missing_optional_tables_do_not_initialize_and_tiny_budget_is_admitted(tmp_path, monkeypatch):
    lake, _, _, original, _ = setup(tmp_path)
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("drop table input_projection_index")
        db.execute("drop table input_coordinate_index")
    monkeypatch.setattr(input_index, "initialize", forbidden)
    monkeypatch.setattr(inputs, "read_batch", forbidden)
    assert lake.inputs.coordinate_identity(original, "calendar", selector=("is_open", 1)) is None
    with lake.inputs.read_context(original, config=ExecutionOptions(max_buffer_bytes=64)):
        with pytest.raises(MemoryError, match="source metadata"):
            lake.inputs.coordinate_identity(original, "calendar", selector=("is_open", 1))
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("select 1 from sqlite_master where name='input_coordinate_index'").fetchone() is None
    with pytest.raises(MemoryError, match="source metadata"):
        lake.inputs.index_coordinates(original, "calendar", config=ExecutionOptions(max_buffer_bytes=64))


def test_forged_source_and_window_widening_cannot_use_registered_locator(tmp_path):
    lake, _, _, original, _ = setup(tmp_path)
    lake.inputs.index_coordinates(original, "calendar", selector=("is_open", 1))
    with pytest.raises(RuntimeError, match="checksum"):
        lake.inputs.coordinate_identity(replace(original, digest="f" * 64), "calendar", selector=("is_open", 1))
    with pytest.raises(ValueError, match="precedes"):
        lake.inputs.coordinate_identity(original, "calendar", start="2024-02-01", end="2024-01-01")
