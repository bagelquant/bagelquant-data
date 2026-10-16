"""Monthly Item reads do not replay disjoint first-visibility witnesses."""
from datetime import date
import sqlite3

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from bagelquant_data import DataItemSpec, DataLake, ItemInput


@pytest.fixture
def monthly_witnesses(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("industry", value_dtype="string"))
    days = [date(2020, month, 1) for month in range(1, 13)]
    frame = pl.DataFrame({"time": days, "asset_id": ["A"] * 12, "value": ["one"] * 12})
    known = date(2021, 1, 4)
    lake.items.ingest("industry", frame, available_date=days[-1], historical_baseline=True)
    original = lake.inputs.freeze({"item": ItemInput("industry", view="versions")}, information_cutoff=known)
    for day in days:
        lake.items.ingest("industry", frame.filter(pl.col("time") == day), available_date=known)
    observed = lake.inputs.freeze({"item": ItemInput("industry", view="versions")}, information_cutoff=known)
    return lake, days, known, original, observed


@pytest.mark.parametrize("lower,upper,months", [(6, 6, [6]), (5, 7, [5, 6, 7]), (None, 2, [1, 2]), (11, None, [11, 12])])
def test_direct_item_windows_scan_only_overlapping_visibility_events(monthly_witnesses, monkeypatch, lower, upper, months):
    lake, days, known, _, observed = monthly_witnesses
    start, end = None if lower is None else days[lower - 1], None if upper is None else days[upper - 1]
    # A full frozen read is an independent reference for the bounded direct path.
    expected = lake.inputs.read(observed, "item", view="snapshot", as_of=known, strict=True).collect()
    if start is not None:
        expected = expected.filter(pl.col("time") >= start)
    if end is not None:
        expected = expected.filter(pl.col("time") <= end)
    events = {row["generation_path"]: row for row in observed.evidence["item"]["record_checks"]}
    scanned = []
    scan = pl.scan_parquet

    def tracked(path, **options):
        if not isinstance(path, list) and ".visibility" in str(path):
            scanned.append(next(row for suffix, row in events.items() if str(path).endswith(suffix)))
        return scan(path, **options)

    monkeypatch.setattr(pl, "scan_parquet", tracked)
    actual = lake.items.read("industry", start=start, end=end, view="snapshot", as_of=known, strict=True).collect()
    assert sorted(date.fromisoformat(row["min_observation"]).month for row in scanned) == months
    fields = ["time", "asset_id", "value", "observation_date", "available_date", "version_available_date", "_baseline"]
    assert_frame_equal(actual.select(fields), expected.select(fields))


@pytest.mark.parametrize("cutoff,strict", [(date(2020, 12, 1), False), (date(2020, 12, 1), True), (date(2021, 1, 4), False), (date(2021, 1, 4), True)])
def test_bounded_visibility_keeps_original_frozen_timing(monthly_witnesses, cutoff, strict):
    lake, days, _, original, observed = monthly_witnesses
    fields = ["time", "asset_id", "value", "version_available_date", "_baseline"]
    for receipt in (original, observed):
        expected = lake.inputs.read(receipt, "item", view="versions", as_of=cutoff, strict=strict).collect().filter(pl.col("time") == days[5])
        actual = lake.inputs.read(receipt, "item", start=days[5], end=days[5], view="versions", as_of=cutoff, strict=strict).collect()
        assert_frame_equal(actual.select(fields), expected.select(fields))


def test_visibility_with_unknown_bounds_is_still_consumed(monthly_witnesses, monkeypatch):
    lake, days, known, _, _ = monthly_witnesses
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("UPDATE visibility_segments SET min_observation=NULL,max_observation=NULL")
    scanned = []
    scan = pl.scan_parquet

    def tracked(path, **options):
        if not isinstance(path, list) and ".visibility" in str(path):
            scanned.append(path)
        return scan(path, **options)

    monkeypatch.setattr(pl, "scan_parquet", tracked)
    actual = lake.items.read("industry", start=days[5], end=days[5], view="snapshot", as_of=known, strict=True).collect()
    assert len(scanned) == 12
    assert actual["value"].to_list() == ["one"]
