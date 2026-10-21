"""Snapshot reuse proves the consumed range without optional exact summaries."""
from dataclasses import replace
from datetime import UTC, date, datetime
from typing import Any

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ExecutionOptions, InputSelectionScope, ItemInput, RawInput, input_read_boundary
from bagelquant_data import input_index, inputs


def forbid_payloads_and_preparation(monkeypatch, lake):
    def forbidden(*args, **kwargs):
        pytest.fail("metadata equivalence read values or prepared proof")
    for owner, names in ((input_index, ("project_original",)), (inputs, ("read_batch",)),
            (pl, ("read_parquet", "scan_parquet")),
            (lake.inputs, ("read", "freeze", "index_selection", "index_selections", "prepare_range_selections"))):
        for name in names:
            monkeypatch.setattr(owner, name, forbidden)


def item_pair(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("price"))
    frame = pl.DataFrame({"time": [date(2000, 1, 4)], "asset_id": ["A"], "value": [10.], "creator": [1]})
    lake.items.ingest("price", frame, available_date="2000-01-04")
    request = ItemInput("price", start="2000-01-01", end="2000-01-31", view="versions")
    old = lake.inputs.freeze({"execution": request}, information_cutoff="2000-01-31", retain_batch_bounds=True)
    lake.items.ingest("price", frame.with_columns(pl.lit(2).alias("creator")), available_date="2000-01-04")
    current = lake.inputs.freeze({"execution": replace(request, end="2000-02-01")},
        information_cutoff="2000-02-01", retain_batch_bounds=True)
    return lake, old, current, frame


@pytest.mark.parametrize("live", [False, True])
def test_unchanged_scalar_with_new_provenance_needs_no_exact_summary(tmp_path, monkeypatch, live):
    lake, old, current, _ = item_pair(tmp_path)
    options: dict[str, Any] = dict(start="2000-01-01", end="2000-01-31", view="snapshot", values_only=True,
        current_as_of="2000-02-01")
    identities = [lake.inputs.selection_identity(source, "execution", **{
        key: value for key, value in options.items() if key != "current_as_of"}) for source in (old, current)]
    assert identities[0] == identities[1]
    assert lake.inputs.selection_equivalent(old, current, "execution", summaries_only=True, **options) is None
    with monkeypatch.context() as guarded:
        forbid_payloads_and_preparation(guarded, lake)
        assert lake.inputs.selection_equivalent(old, None if live else current, "execution",
            records_only=True, **options) is True
    assert [lake.inputs.get(source).digest for source in (old, current)] == [old.digest, current.digest]


@pytest.mark.parametrize("bound", ["start", "end", "both"])
def test_raw_enclosing_window_does_not_change_consumed_prefix(tmp_path, monkeypatch, bound):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("index", "by_date", source="fake", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2000, 1, 4), date(2000, 2, 1)],
        "asset_id": ["A", "A"], "close": [100., 110.]})
    lake.raw.ingest(spec, frame, ingested_at=datetime(2000, 1, 1, tzinfo=UTC))
    request = RawInput("fake", "index", observation_start="2000-01-01",
        observation_end="2000-01-31", view="versions")
    old = lake.inputs.freeze({"index_daily": request}, information_cutoff="2000-01-31", retain_batch_bounds=True)
    current_request = replace(request,
        observation_start="1999-12-01" if bound in {"start", "both"} else request.observation_start,
        observation_end="2000-02-01" if bound in {"end", "both"} else request.observation_end)
    current = lake.inputs.freeze({"index_daily": current_request}, information_cutoff="2000-02-01", retain_batch_bounds=True)
    options: dict[str, Any] = dict(start="2000-01-01", end="2000-01-31", view="snapshot", values_only=True)
    identities = [lake.inputs.selection_identity(source, "index_daily", **options) for source in (old, current)]
    scope = InputSelectionScope("index_daily", start="2000-01-01", end="2000-01-31", view="snapshot")
    descriptors = lake.inputs.prepare_range_selections(old, [scope])
    selection_set = lake.inputs.register_selection_set(old, descriptors)
    with monkeypatch.context() as guarded:
        forbid_payloads_and_preparation(guarded, lake)
        assert lake.inputs.selection_equivalent(old, current, "index_daily", records_only=True, **options) is True
        assert lake.inputs.selection_equivalent(old, current, "index_daily", **options) is True
    assert [lake.inputs.selection_identity(source, "index_daily", **options) for source in (old, current)] == identities
    assert lake.inputs.get(old).requests == old.requests
    assert lake.inputs.get(current).requests == current.requests
    assert lake.inputs.selection_descriptors(old, [scope]) == descriptors
    assert lake.inputs.read_selection_set(selection_set) == descriptors


@pytest.mark.parametrize("current_cutoff,expected", [("2024-01-02", True), ("2024-01-03", None)])
def test_partial_batch_cannot_prove_unchanged_values_at_a_later_cutoff(tmp_path, monkeypatch, current_cutoff, expected):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("price"))
    frame = pl.DataFrame({"time": [date(2024, 1, 1)] * 2, "asset_id": ["A", "B"],
        "value": [1., 2.], "version_available_date": [date(2024, 1, 1), date(2024, 1, 3)]})
    lake.items.ingest("price", frame)
    request = {"value": ItemInput("price", view="versions")}
    old = lake.inputs.freeze(request, information_cutoff="2024-01-02", retain_batch_bounds=True)
    current = lake.inputs.freeze(request, information_cutoff=current_cutoff, retain_batch_bounds=True)
    assert len(old.evidence["value"]["batches"]) == 1
    assert lake.inputs.read(old, "value", view="snapshot").collect()["asset_id"].to_list() == ["A"]
    assert lake.inputs.read(current, "value", view="snapshot").collect().height == (1 if expected else 2)
    options: dict[str, Any] = dict(start="2024-01-01", end="2024-01-01", view="snapshot", values_only=True)
    with monkeypatch.context() as guarded:
        forbid_payloads_and_preparation(guarded, lake)
        for records, summaries in ((False, False), (True, False), (False, True)):
            assert lake.inputs.selection_equivalent(old, current, "value", **options,
                records_only=records, summaries_only=summaries) is expected
    if expected is None:
        for source in (old, current):
            assert lake.inputs.index_selection(source, "value", start="2024-01-01", end="2024-01-01", view="snapshot")
        assert lake.inputs.selection_equivalent(old, current, "value", summaries_only=True, **options) is False


@pytest.mark.parametrize("replacement", [20., None])
def test_changed_or_withdrawn_value_keeps_historical_receipt(tmp_path, monkeypatch, replacement):
    lake, old, current, frame = item_pair(tmp_path)
    lake.items.ingest("price", frame.with_columns(pl.lit(replacement, dtype=pl.Float64).alias("value")),
        available_date="2000-02-01")
    revised = lake.inputs.freeze(current.requests, information_cutoff="2000-02-01", retain_batch_bounds=True)
    options: dict[str, Any] = dict(start="2000-01-01", end="2000-01-31", view="snapshot", values_only=True)
    with monkeypatch.context() as guarded:
        forbid_payloads_and_preparation(guarded, lake)
        assert lake.inputs.selection_equivalent(old, revised, "execution", records_only=True, **options) is None
        assert lake.inputs.selection_equivalent(old, revised, "execution", **options) is not True
    assert lake.inputs.read(old, "execution", view="snapshot").collect()["value"].to_list() == [10.]
    assert lake.inputs.get(old).digest == old.digest


def test_snapshot_range_admission_is_bounded_and_cancelable(tmp_path, monkeypatch):
    lake, old, current, _ = item_pair(tmp_path)
    options: dict[str, Any] = dict(start="2000-01-01", end="2000-01-31", view="snapshot", values_only=True, records_only=True)
    forbid_payloads_and_preparation(monkeypatch, lake)
    with lake.inputs.read_context([old, current], config=ExecutionOptions(max_buffer_bytes=1)):
        assert lake.inputs.selection_equivalent(old, current, "execution", **options) is None
    with lake.inputs.read_context([old, current], config=ExecutionOptions(max_buffer_bytes=2**20)):
        assert lake.inputs.selection_equivalent(old, current, "execution", **options) is True
    canceled = False
    def check():
        if canceled:
            raise RuntimeError("canceled")
    with lake.inputs.read_context([old, current], check_canceled=check):
        canceled = True
        with pytest.raises(RuntimeError, match="canceled"):
            lake.inputs.selection_equivalent(old, current, "execution", **options)
        canceled = False


def test_same_cutoff_does_not_hide_new_visibility(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("price"))
    frame = pl.DataFrame({"time": [date(2024, 1, 1)] * 2, "asset_id": ["A", "B"],
        "value": [1., 2.], "version_available_date": [date(2024, 1, 1), date(2024, 1, 3)]})
    lake.items.ingest("price", frame)
    request = {"value": ItemInput("price", view="versions")}
    old = lake.inputs.freeze(request, information_cutoff="2024-01-02", retain_batch_bounds=True)
    lake.items.ingest("price", frame.filter(pl.col("asset_id") == "B"), available_date="2024-01-02")
    current = lake.inputs.freeze(request, information_cutoff="2024-01-02", retain_batch_bounds=True)
    assert lake.inputs.read(old, "value", view="snapshot").collect().height == 1
    assert lake.inputs.read(current, "value", view="snapshot").collect().height == 2
    forbid_payloads_and_preparation(monkeypatch, lake)
    for records, summaries in ((False, False), (True, False), (False, True)):
        assert lake.inputs.selection_equivalent(old, current, "value", view="snapshot", values_only=True,
            records_only=records, summaries_only=summaries) is not True


def test_strict_baseline_attestation_still_requires_exact_visibility(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("price"))
    frame = pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "value": [1.]})
    lake.items.ingest("price", frame, available_date="2024-01-01", historical_baseline=True)
    request = {"value": ItemInput("price", view="versions", strict=True)}
    old = lake.inputs.freeze(request, information_cutoff="2024-01-03", retain_batch_bounds=True)
    lake.items.ingest("price", frame, available_date="2024-01-02")
    current = lake.inputs.freeze(request, information_cutoff="2024-01-03", retain_batch_bounds=True)
    assert lake.inputs.read(old, "value", view="snapshot").collect().is_empty()
    assert lake.inputs.read(current, "value", view="snapshot").collect().height == 1
    with monkeypatch.context() as guarded:
        forbid_payloads_and_preparation(guarded, lake)
        for records, summaries in ((False, False), (True, False), (False, True)):
            assert lake.inputs.selection_equivalent(old, current, "value", view="snapshot", values_only=True,
                records_only=records, summaries_only=summaries) is not True
    for source in (old, current):
        lake.inputs.index_selection(source, "value", view="snapshot")
    assert lake.inputs.selection_equivalent(old, current, "value", view="snapshot", values_only=True, summaries_only=True) is False


def test_new_attestation_can_change_precedence_of_fully_visible_values(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("price", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "close": [20.]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2024, 1, 1, tzinfo=UTC))
    request = {"value": RawInput("custom", "price", view="versions")}
    initial = lake.inputs.freeze(request, information_cutoff="2024-01-06", retain_batch_bounds=True)
    lake.raw.ingest(spec, frame, ingested_at=datetime(2024, 1, 5, tzinfo=UTC))
    lake.raw.ingest(spec, frame.with_columns(pl.lit(10.).alias("close")), ingested_at=datetime(2024, 1, 2, tzinfo=UTC))
    with input_read_boundary(lake.data_meta_path, lake.inputs.max_commit(), max_check_id=initial.max_check_id):
        bounded = DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path)
        old = bounded.inputs.freeze(request, information_cutoff="2024-01-06", retain_batch_bounds=True)
    current = lake.inputs.freeze(request, information_cutoff="2024-01-06", retain_batch_bounds=True)
    assert lake.inputs.read(old, "value", view="snapshot").collect()["close"].to_list() == [10.]
    assert lake.inputs.read(current, "value", view="snapshot").collect()["close"].to_list() == [20.]
    options: dict[str, Any] = dict(start="2024-01-01", end="2024-01-01", view="snapshot", values_only=True)
    with monkeypatch.context() as guarded:
        forbid_payloads_and_preparation(guarded, lake)
        for records, summaries in ((False, False), (True, False), (False, True)):
            assert lake.inputs.selection_equivalent(old, current, "value", **options,
                records_only=records, summaries_only=summaries) is None
    for source in (old, current):
        lake.inputs.index_selection(source, "value", start="2024-01-01", end="2024-01-01", view="snapshot")
    assert lake.inputs.selection_equivalent(old, current, "value", summaries_only=True, **options) is False
