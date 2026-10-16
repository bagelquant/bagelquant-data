"""One value collection shares bounded original-key work across witnesses."""
from datetime import date

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from bagelquant_data import DataItemSpec, DataLake, ItemInput
from bagelquant_data.pipeline import versions


@pytest.mark.parametrize("bounded", [False, True])
@pytest.mark.parametrize("view", ["history", "snapshot", "versions"])
@pytest.mark.parametrize("strict", [False, True])
def test_visibility_overlay_encodes_selected_originals_once(
    tmp_path, monkeypatch, bounded, view, strict,
):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("industry", value_dtype="string"))
    days = [date(2020, month, 1) for month in range(1, 13)]
    members = [f"A{index:03}" for index in range(16)]
    frame = pl.DataFrame({"time": days}).join(
        pl.DataFrame({"asset_id": members}), how="cross",
    ).with_columns(pl.lit("one").alias("value"))
    known = date(2021, 1, 4)
    lake.items.ingest("industry", frame, available_date=days[-1], historical_baseline=True)
    for day in days:
        lake.items.ingest("industry", frame.filter(pl.col("time") == day), available_date=known)
    receipt = lake.inputs.freeze({"item": ItemInput("industry", view="versions")}, information_cutoff=known)
    # Frozen consumption reads each original eagerly before applying witnesses,
    # independently of the direct query's lazy fork and natural-key UDF.
    expected = lake.inputs.read(receipt, "item", view=view, strict=strict).collect()
    if bounded:
        expected = expected.filter(pl.col("time").is_between(days[4], days[6]))
    calls = []
    original = versions._payload_hashes

    def counted(selected, fields):
        calls.append((selected.height, selected["source_time"].min(),
                      selected["source_time"].max(), "_attested_date" in selected.columns))
        return original(selected, fields)

    monkeypatch.setattr(versions, "_payload_hashes", counted)
    actual = lake.items.read("industry", as_of=known, view=view, strict=strict,
        start=days[4] if bounded else None, end=days[6] if bounded else None).collect()
    selected_rows = len(members) * (3 if bounded else 12)
    assert sum(rows for rows, _, _, event in calls if not event) == selected_rows
    assert sum(rows for rows, _, _, event in calls if event) == selected_rows
    if bounded:
        assert all(days[4] <= first <= last <= days[6] for _, first, last, _ in calls)
    fields = ["time", "asset_id", "value", "observation_date", "available_date",
              "version_available_date", "_baseline", "_record_id"]
    assert_frame_equal(actual.select(fields).sort("time", "asset_id", "version_available_date"),
                       expected.select(fields).sort("time", "asset_id", "version_available_date"))
