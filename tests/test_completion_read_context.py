"""Completion planning uses fixed metadata views without publication admission."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import UTC, date, datetime
import sqlite3

import polars as pl
import pytest

from bagelquant_data import (
    ConfigurationError,
    DataItemSpec,
    DataLake,
    DatasetSpec,
    ItemPublication,
    RawInput,
)


@pytest.fixture
def completion_lakes(tmp_path):
    paths = {"data_meta_path": tmp_path / "meta.sqlite", "lake_path": tmp_path / "lake"}
    with DataLake.open(**paths) as writer:
        spec = DatasetSpec("input", "by_date", date_kind="calendar",
            field_mappings={"time": "time", "asset_id": "asset_id"})
        rows = pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "value": [1.]})
        writer.raw.ingest(spec, rows, ingested_at=datetime(2024, 1, 1, tzinfo=UTC))
        request = RawInput("custom", "input", view="versions")
        writer.items.register(DataItemSpec("value", (request,)))
        source = writer.inputs.freeze({"input": request}, information_cutoff="2024-01-02")
        with writer.items.publication(input_receipt=source) as publisher:
            publisher.publish([ItemPublication("value", rows, start="2024-01-01",
                end="2024-01-02", complete_at="2024-01-02")])
        with DataLake.open(**paths, read_only=True, runtime=True) as reader:
            yield writer, reader, source, spec, rows


def _metadata_contents(lake):
    with closing(sqlite3.connect(lake.data_meta_path.resolve().as_uri() + "?mode=ro", uri=True)) as db:
        return tuple(db.iterdump())


def test_readonly_completion_context_reads_only_metadata(completion_lakes, monkeypatch):
    writer, reader, source, _, _ = completion_lakes
    before = _metadata_contents(writer)
    expected = reader.items.completion_statuses(names=["value"])
    builds = reader.items.completed_builds("value")

    def forbidden(*args, **kwargs):
        pytest.fail("completion planning must not admit writes or consume original values")

    monkeypatch.setattr(reader._data_meta, "ensure_writable", forbidden)
    for owner, method in ((reader.items, "read"), (reader.raw, "read"),
            (reader.inputs, "read"), (reader.inputs, "verify"), (reader.inputs, "freeze"),
            (pl, "read_parquet"), (pl, "scan_parquet")):
        monkeypatch.setattr(owner, method, forbidden)
    with reader.items.completion_read_context(names=["value"]) as flags:
        assert flags == expected
        assert reader.items.completed_builds("value") == builds
        with reader.inputs.read_context(source, verify=False):
            assert reader.inputs.is_current(source)
        with reader._data_meta.connect() as db:
            assert db.total_changes == 0
            with pytest.raises(sqlite3.OperationalError, match="readonly|read-only"):
                db.execute("update datasets set update_incomplete=1")
            assert db.total_changes == 0
    assert _metadata_contents(writer) == before


@pytest.mark.parametrize("raise_inside", [False, True])
def test_nested_completion_snapshot_survives_concurrent_commit_and_inner_failure(
        completion_lakes, raise_inside):
    writer, reader, source, spec, rows = completion_lakes
    with reader.items.completion_read_context(names=["value"]) as original:
        builds = reader.items.completed_builds("value")
        assert original[0]["dependency_current"] is True
        try:
            with reader.items.completion_read_context(names=["value"]) as nested:
                with reader.inputs.read_context(source, verify=False):
                    assert nested == original and reader.inputs.is_current(source)
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        executor.submit(writer.raw.ingest, spec,
                            rows.with_columns(pl.lit(2.).alias("value")),
                            ingested_at=datetime(2024, 1, 2, tzinfo=UTC)).result(timeout=5)
                    assert reader.items.completion_statuses(names=["value"]) == original
                    assert reader.items.completed_builds("value") == builds
                    assert reader.inputs.is_current(source)
                    if raise_inside:
                        raise ValueError("consumer read failed")
        except ValueError:
            assert raise_inside
        assert reader.items.completion_statuses(names=["value"]) == original
        assert reader.inputs._read_context() is None
    with reader.items.completion_read_context(names=["value"]) as current:
        assert current[0]["dependency_current"] is False
        with reader.inputs.read_context(source, verify=False):
            assert not reader.inputs.is_current(source)


def test_completion_context_outer_failure_releases_snapshot(completion_lakes):
    writer, reader, _, spec, rows = completion_lakes
    with pytest.raises(ValueError, match="consumer read failed"):
        with reader.items.completion_read_context(names=["value"]):
            raise ValueError("consumer read failed")
    writer.raw.ingest(spec, rows.with_columns(pl.lit(2.).alias("value")),
        ingested_at=datetime(2024, 1, 2, tzinfo=UTC))
    with reader.items.completion_read_context(names=["value"]) as current:
        assert current[0]["dependency_current"] is False


def test_completion_read_context_preserves_writer_and_publication_gates(completion_lakes):
    writer, reader, source, _, _ = completion_lakes
    with reader.items.completion_read_context(names=["value"]):
        with pytest.raises(PermissionError, match="read-only"):
            reader.items.register(DataItemSpec("new"))
    for guard in (reader.items.completion_guard(names=["value"]),
            reader.inputs.publication_guard(source)):
        with pytest.raises(PermissionError, match="read-only"):
            with guard:
                pytest.fail("read-only handles cannot admit consumer publication")
    with writer.items.completion_read_context(names=["value"]):
        for guard in (writer.items.completion_guard(names=["value"]),
                writer.inputs.publication_guard(source)):
            with pytest.raises(ConfigurationError, match="cannot upgrade"):
                with guard:
                    pytest.fail("a read snapshot cannot upgrade to a publication fence")


@pytest.mark.parametrize("names,expected", [
    (None, ["value"]), ([], []), (["missing"], []),
    (["value", "value"], ["value"]),
])
def test_completion_read_context_keeps_selected_batch_semantics(completion_lakes, names, expected):
    _, reader, _, _, _ = completion_lakes
    with reader.items.completion_read_context(names=names) as flags:
        assert [row["dataset"] for row in flags] == expected
