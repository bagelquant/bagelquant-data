"""Frozen completion cannot borrow newer flags or uncertified output."""
from datetime import UTC, date, datetime

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, ItemPublication, RawInput


def fixture(tmp_path):
    lake = DataLake(data_meta_path=tmp_path / "metadata.sqlite", lake_path=tmp_path / "lake")
    lake.raw.ingest(DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"}),
        pl.DataFrame({"time": [date(2024, 1, 2)], "asset_id": ["A"], "close": [10.]}),
        ingested_at=datetime(2000, 1, 1, tzinfo=UTC))
    lake.items.register(DataItemSpec("value", (RawInput("custom", "daily"),),
        time_column="source_time", value_column="close"))
    return lake


def source(lake, *, end="2024-01-03"):
    return lake.inputs.freeze({"x": ItemInput("value", view="versions")},
        information_cutoff=end, retain_batch_bounds=True)


def test_original_sparse_completion_survives_later_definition_and_output(tmp_path, monkeypatch):
    lake = fixture(tmp_path)
    lake.items.initialize("value", start="2024-01-02", end="2024-01-03")
    original = source(lake)
    assert lake.inputs.completed_range(original, "x", start="2024-01-02", end="2024-01-03")
    lake.items.ingest("value", pl.DataFrame({"time": [date(2024, 1, 2)],
        "asset_id": ["A"], "value": [99.]}), available_date="2024-01-03",
        input_receipt=next(iter(original.evidence["x"]["parent_receipts"])))
    later = source(lake)
    assert not lake.inputs.completed_range(later, "x", start="2024-01-02", end="2024-01-03")
    lake.items.register(DataItemSpec("value", description="changed current definition"))
    def forbidden(*_a, **_k):
        pytest.fail("Frozen completion read values or mutable completion flags")
    for owner, name in ((lake.items, "read"), (lake.items, "completion_statuses"),
            (lake.inputs, "read"), (lake.inputs, "index_selection")):
        monkeypatch.setattr(owner, name, forbidden)
    assert lake.inputs.completed_range(original, "x", start="2024-01-02", end="2024-01-03")


def test_later_completion_cannot_extend_original_range(tmp_path):
    lake = fixture(tmp_path)
    lake.items.initialize("value", start="2024-01-02", end="2024-01-02")
    original = source(lake)
    lake.items.update("value", start="2024-01-03", end="2024-01-03")
    assert not lake.inputs.completed_range(original, "x", start="2024-01-02", end="2024-01-03")
    assert lake.inputs.completed_range(source(lake), "x", start="2024-01-02", end="2024-01-03")


def test_row_scoped_same_value_check_requires_a_new_complete_certificate(tmp_path):
    lake = fixture(tmp_path)
    lake.items.initialize("value", start="2024-01-02", end="2024-01-03")
    original = source(lake)
    lake.items.ingest("value", pl.DataFrame({"time": [date(2024, 1, 2)],
        "asset_id": ["A"], "value": [10.]}), available_date="2024-01-03",
        input_receipt=next(iter(original.evidence["x"]["parent_receipts"])))
    checked = source(lake)
    assert not lake.inputs.completed_range(checked, "x", start="2024-01-02", end="2024-01-03")
    parent = lake.inputs.get(next(iter(original.evidence["x"]["parent_receipts"])))
    current_parent = lake.inputs.freeze(parent.requests, information_cutoff="2024-01-03")
    with lake.items.publication(input_receipt=current_parent) as publisher:
        publisher.publish([ItemPublication("value", pl.DataFrame({"source_time": [date(2024, 1, 2)],
            "asset_id": ["A"], "close": [10.], "version_available_date": [date(2024, 1, 3)]}),
            start="2024-01-02", end="2024-01-03", complete_at="2024-01-03")])
    certified = source(lake)
    assert lake.inputs.completed_range(certified, "x", start="2024-01-02", end="2024-01-03")
    assert lake.inputs.completed_range(original, "x", start="2024-01-02", end="2024-01-03")


def test_complete_point_publication_keeps_disjoint_nonempty_and_empty_certificates(tmp_path, monkeypatch):
    lake = fixture(tmp_path)
    january, september, october = date(2024, 1, 2), date(2024, 9, 17), date(2024, 10, 1)
    def publish(day, assets):
        rows = pl.DataFrame({"source_time": [day] * len(assets), "asset_id": assets,
            "close": [10.] * len(assets), "version_available_date": [day] * len(assets)},
            schema={"source_time": pl.Date, "asset_id": pl.String, "close": pl.Float64, "version_available_date": pl.Date})
        receipt = lake.inputs.freeze({"daily": RawInput("custom", "daily")}, information_cutoff=day)
        with lake.items.publication(input_receipt=receipt) as publisher:
            publisher.publish([ItemPublication("value", rows, start=day, end=day, complete_at=day)])
    publish(january, ["A"])
    publish(october, ["A"])
    original = source(lake, end=october.isoformat())
    publish(september, [])
    # Mixed same-value and new coordinates must scope both the initial witness
    # and final completion to October, retaining January and empty September.
    publish(october, ["A", "B"])
    current = source(lake, end=october.isoformat())
    monkeypatch.setattr(lake.inputs, "read", lambda *_a, **_k:
        pytest.fail("completion metadata decoded values"))
    for day in (january, september, october):
        assert lake.inputs.completed_range(current, "x", start=day, end=day)
    assert lake.inputs.completed_range(original, "x", start=january, end=january)


@pytest.mark.parametrize("partial_rows", [False, True])
def test_earlier_asof_cannot_borrow_later_completion_even_with_earlier_parent_or_rows(tmp_path, monkeypatch, partial_rows):
    lake = fixture(tmp_path)
    day, later = date(2024, 1, 2), date(2024, 1, 21)
    rows = pl.DataFrame({"source_time": [day], "asset_id": ["A"], "close": [10.],
        "version_available_date": [day]})
    if not partial_rows:
        rows = rows.head(0)
    parent = lake.inputs.freeze({"daily": RawInput("custom", "daily")}, information_cutoff=day)
    if partial_rows:
        with monkeypatch.context() as interrupted:
            interrupted.setattr(lake.items, "_replace_range", lambda *_a, **_k:
                (_ for _ in ()).throw(RuntimeError("before complete certificate")))
            with pytest.raises(RuntimeError, match="before complete certificate"):
                with lake.items.publication(input_receipt=parent) as publisher:
                    publisher.publish([ItemPublication("value", rows, start=day, end=day, complete_at=day)])
    with lake.items.publication(input_receipt=parent) as publisher:
        publisher.publish([ItemPublication("value", rows, start=day, end=day, complete_at=later)])
    original = source(lake, end="2024-02-01")
    monkeypatch.setattr(lake.inputs, "read", lambda *_a, **_k: pytest.fail("completion proof decoded values"))
    assert lake.inputs.completed_range(original, "x", start=day, end=day)
    assert not lake.inputs.completed_range(original, "x", start=day, end=day, as_of="2024-01-16")
    assert lake.inputs.completed_range(original, "x", start=day, end=day, as_of=later)
