"""Public producer continuations retain untouched output versions and coverage."""
from datetime import UTC, date, datetime

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, ItemPublication, RawInput


def prepared(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    days = [date(2024, 1, day) for day in range(1, 8)]
    source = DatasetSpec("raw", "by_date", date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": days[:5], "asset_id": ["A"] * 5, "value": [1., 2., 3., 4., 5.]})
    lake.raw.ingest(source, frame, ingested_at=datetime(2024, 1, 5, tzinfo=UTC))
    request = RawInput("custom", "raw", alias="raw")
    original = lake.inputs.freeze({"raw": request}, information_cutoff=days[4])
    lake.items.register(DataItemSpec("output", (request,), producer_key="external", producer_revision="1"))
    with lake.items.publication(input_receipt=original) as publisher:
        publisher.publish([ItemPublication("output", frame.with_columns(pl.lit(days[4]).alias("version_available_date")),
            start=days[0], end=days[4], complete_at=days[4])])
    completion = lake.items.completed_builds("output")[-1]
    retained = lake.inputs.freeze({"output": ItemInput("output", start=days[0], end=days[4], view="snapshot")}, information_cutoff=days[4])
    current = lake.inputs.freeze({"raw": request}, information_cutoff=days[6])
    changed = pl.DataFrame({"time": [days[0], days[5], days[6]], "asset_id": ["A"] * 3, "value": [10., 6., 7.]})
    publication = ItemPublication("output", changed, start=days[0], end=days[6], complete_at=days[6],
        reuse_build_id=completion["id"], affected_ranges=((days[0], days[0]), (days[5], days[6])))
    return lake, days, completion, retained, current, publication


def test_discrete_repair_and_tail_keep_intervening_versions_and_full_current_coverage(tmp_path, monkeypatch):
    lake, days, completion, retained, current, publication = prepared(tmp_path)
    read = lake.items.read
    windows = []
    def bounded_read(name, **kwargs):
        windows.append((kwargs.get("start"), kwargs.get("end")))
        return read(name, **kwargs)
    monkeypatch.setattr(lake.items, "read", bounded_read)
    with lake.items.publication(input_receipt=current) as publisher:
        report = publisher.publish([publication])["output"]
    assert report is not None and report.rows_committed == 3
    assert windows == [(days[0], days[0]), (days[5], days[6])]
    versions = read("output", view="versions", as_of=days[6]).collect()
    assert sorted(versions.filter(pl.col("version_available_date") == days[6])["time"].to_list()) == [days[0], days[5], days[6]]
    expected = pl.DataFrame({"time": days, "asset_id": ["A"] * 7, "value": [10., 2., 3., 4., 5., 6., 7.]})
    assert_frame_equal(read("output", view="snapshot", as_of=days[6]).collect().select("time", "asset_id", "value"), expected)
    assert lake.inputs.read(retained, "output").collect()["value"].to_list() == [1., 2., 3., 4., 5.]
    final = lake.inputs.freeze({"output": ItemInput("output", start=days[0], end=days[6], view="versions")}, information_cutoff=days[6])
    assert lake.inputs.is_current(final)
    assert completion in lake.items.completed_builds("output")
    assert lake.items.completed_builds("output")[-1]["start_date"] == str(days[0])


@pytest.mark.parametrize("damage", ["hole", "overlap", "outside", "foreign", "row_only", "definition", "later_outside"])
def test_sparse_rejects_unproved_coverage_before_output_publication(tmp_path, damage):
    from dataclasses import replace
    lake, days, completion, _, current, publication = prepared(tmp_path)
    if damage == "hole":
        publication = replace(publication, affected_ranges=((days[0], days[0]), (days[6], days[6])))
    elif damage == "overlap":
        publication = replace(publication, affected_ranges=((days[0], days[1]), (days[1], days[6])))
    elif damage == "outside":
        publication = replace(publication, affected_ranges=((days[5], days[6]),))
    elif damage == "foreign":
        publication = replace(publication, reuse_build_id=999999)
    elif damage == "row_only":
        lake.items.ingest("output", publication.frame.head(1), available_date=days[6], input_receipt=current)
        publication = replace(publication, reuse_build_id=lake.items.builds("output")[-1]["id"])
    elif damage == "definition":
        lake.items.register(replace(lake.items.get("output"), producer_revision="2"))
    else:
        lake.items.ingest("output", pl.DataFrame({"time": [days[2]], "asset_id": ["A"], "value": [99.]}),
            available_date=days[6], input_receipt=current)
    before = lake.items.manifest("output")
    with pytest.raises(ValueError):
        with lake.items.publication(input_receipt=current) as publisher:
            publisher.publish([publication])
    assert lake.items.manifest("output") == before
    assert completion in lake.items.completed_builds("output")


def test_sparse_cancel_after_unit_preserves_old_whole_range_for_retry(tmp_path, monkeypatch):
    from concurrent.futures import CancelledError
    lake, _, completion, _, current, publication = prepared(tmp_path)
    replace = lake.items._replace_range
    state = {"cancel": False}
    def cancel_after_unit(*args, **kwargs):
        report = replace(*args, **kwargs)
        state["cancel"] = True
        return report
    with monkeypatch.context() as patch:
        patch.setattr(lake.items, "_replace_range", cancel_after_unit)
        with pytest.raises(CancelledError):
            with lake.items.publication(input_receipt=current, cancelled=lambda: state["cancel"]) as publisher:
                publisher.publish([publication])
    assert max(row["end_date"] for row in lake.items.completed_builds("output")) == completion["end_date"]
    state["cancel"] = False
    with lake.items.publication(input_receipt=current) as publisher:
        publisher.publish([publication])
    assert lake.items.completed_builds("output")[-1]["end_date"] == str(publication.end)


def test_repeat_noop_reuses_native_completion_and_rejects_later_output(tmp_path, monkeypatch):
    from dataclasses import replace
    lake, days, completion, retained, current, publication = prepared(tmp_path)
    noop = replace(publication, frame=publication.frame.clear(), end=days[4], affected_ranges=())
    before = lake.items.manifest("output")
    original_count = len(lake.items.completed_builds("output"))
    monkeypatch.setattr(lake.inputs, "read", lambda *a, **k: pytest.fail("No-op decoded values"))
    with lake.items.publication(input_receipt=current) as publisher:
        first = publisher.publish([noop])["output"]
    assert first is not None and first.rows_committed == 0
    with lake.items.publication(input_receipt=current) as publisher:
        second = publisher.publish([noop])["output"]
    assert second is not None and second.status == "unchanged" and second.rows_committed == 0
    assert len(lake.items.completed_builds("output")) == original_count + 1
    assert lake.items.manifest("output") == before
    assert lake.inputs.get(retained).digest == retained.digest
    assert completion in lake.items.completed_builds("output")
    lake.items.ingest("output", pl.DataFrame({"time": [days[2]], "asset_id": ["A"], "value": [99.]}),
        available_date=days[6], input_receipt=current)
    with pytest.raises(ValueError, match="later output"):
        with lake.items.publication(input_receipt=current) as publisher:
            publisher.publish([noop])
    assert len(lake.items.completed_builds("output")) == original_count + 1


def test_sparse_noop_later_output_fence_reads_only_registered_bounds(tmp_path, monkeypatch):
    from contextlib import contextmanager
    from dataclasses import replace
    import sqlite3
    lake, days, _, _, current, publication = prepared(tmp_path)
    lake.items.ingest("output", pl.DataFrame({"time": [days[2]], "asset_id": ["A"], "value": [99.]}),
                      available_date=days[6], input_receipt=current)
    noop = replace(publication, frame=publication.frame.clear(), end=days[4], affected_ranges=())
    before = lake.items.manifest("output")
    connect = lake._data_meta.connect
    reads = []
    def authorize(operation, table, column, *_):
        if operation == sqlite3.SQLITE_READ and table == "version_segments":
            reads.append(column)
            if column == "payload":
                return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK
    @contextmanager
    def metadata_only():
        with connect() as db:
            db.set_authorizer(authorize)
            try:
                yield db
            finally:
                db.set_authorizer(None)
    with monkeypatch.context() as patch:
        patch.setattr(lake._data_meta, "connect", metadata_only)
        with pytest.raises(ValueError, match="later output"):
            with lake.items.publication(input_receipt=current) as publisher:
                publisher.publish([noop])
    assert "payload" not in reads
    assert {"row_count", "min_observation", "max_observation"} <= set(reads)
    assert lake.items.manifest("output") == before
