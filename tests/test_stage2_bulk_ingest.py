from datetime import UTC, date, datetime, timedelta

import polars as pl

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, RawInput


def _lake(root, *, read_only=False):
    return DataLake.open(data_meta_path=root / "data_meta.sqlite",
                         lake_path=root / "lake", read_only=read_only)


def _frame(days, values=None, *, versions=None):
    frame = pl.DataFrame({"time": days, "asset_id": ["A"] * len(days),
                          "value": values or [1.0] * len(days)},
                         schema={"time": pl.Date, "asset_id": pl.String, "value": pl.Float64})
    if versions is not None:
        frame = frame.with_columns(pl.Series("version_available_date", versions, dtype=pl.Date))
    return frame


def test_first_bulk_publication_preserves_row_dates_pit_and_frozen_replay(tmp_path):
    lake = _lake(tmp_path)
    lake.items.register(DataItemSpec("item"))
    days = [date(2020, 1, 1), date(2020, 1, 2), date(2020, 2, 1)]
    available = [date(2020, 1, 1), date(2020, 1, 3), date(2020, 2, 2)]
    report = lake.items.ingest("item", _frame(days, [1.0, 2.0, 3.0], versions=available))
    versions = lake.items.read("item", view="versions").collect()
    assert report.rows_committed == 3
    assert versions["_commit_seq"].n_unique() == 1
    assert versions["version_available_date"].to_list() == available
    assert lake.items.read("item", view="history").collect()["value"].to_list() == [1.0]
    assert lake.items.read("item", view="snapshot", as_of="2020-01-02", strict=True).collect()["value"].to_list() == [1.0]
    assert lake.items.read("item", view="snapshot", as_of="2020-01-03").collect()["value"].to_list() == [1.0, 2.0]
    early = lake.inputs.freeze({"item": ItemInput("item", view="snapshot", strict=True)},
                               information_cutoff="2020-01-02")
    assert lake.inputs.read(early, "item").collect()["value"].to_list() == [1.0]
    receipt = lake.inputs.freeze({"item": ItemInput("item", view="snapshot", strict=True)},
                                 information_cutoff="2020-02-02")
    assert len(receipt.evidence["item"]["batches"]) == 2
    assert {batch["commit_seq"] for batch in receipt.evidence["item"]["batches"]} == {report.commit_seq}
    delayed = lake.items.read("item", view="snapshot", as_of="2020-02-02", max_commit=receipt.max_commit)
    lake.items.ingest("item", _frame([days[0]], [9.0], versions=[date(2020, 1, 4)]))
    lake.items.remove("item")
    reader = _lake(tmp_path, read_only=True).inputs
    assert reader.verify(receipt)["batch_count"] == 2
    assert reader.read(receipt, "item").collect()["value"].to_list() == [1.0, 2.0, 3.0]
    assert delayed.collect()["value"].to_list() == [1.0, 2.0, 3.0]


def test_large_first_panel_publishes_one_commit_across_months(tmp_path):
    lake = _lake(tmp_path)
    lake.items.register(DataItemSpec("panel"))
    days = [date(2020, 1, 1) + timedelta(days=i) for i in range(210)]
    frame = pl.DataFrame({"time": days}).join(pl.DataFrame({"asset_id": [f"A{i}" for i in range(48)]}), how="cross")
    frame = frame.with_columns(pl.col("time").dt.ordinal_day().cast(pl.Float64).alias("value"))
    report = lake.items.ingest("panel", frame, available_date=days[0])
    result = lake.items.read("panel", strict=True).collect()
    assert report.rows_committed == result.height == 210 * 48
    assert result["_commit_seq"].n_unique() == 1
    assert result["version_available_date"].to_list() == result["time"].to_list()
    assert result["value"].sum() == 48 * 210 * 211 / 2


def test_baseline_bulk_does_not_coalesce_later_availability_attestations(tmp_path):
    lake = _lake(tmp_path)
    lake.items.register(DataItemSpec("item"))
    days = [date(2020, 1, 1), date(2020, 1, 2)]
    lake.items.ingest("item", _frame(days), historical_baseline=True)
    original = lake.items.read("item", view="versions").collect()
    assert original["_commit_seq"].n_unique() == 1
    assert original["_baseline"].to_list() == [True, True]
    receipt = lake.inputs.freeze({"item": ItemInput("item", view="snapshot", strict=True)},
                                 information_cutoff="2020-01-04")
    assert lake.inputs.read(receipt, "item").collect().is_empty()
    lake.items.ingest("item", _frame(days, versions=[date(2020, 1, 3), date(2020, 1, 4)]))
    strict = lake.items.read("item", view="versions", strict=True).collect()
    assert strict["version_available_date"].to_list() == [date(2020, 1, 3), date(2020, 1, 4)]
    assert strict["_attestation_id"].n_unique() == 2
    assert lake.items.read("item", view="snapshot", as_of="2020-01-02", strict=True).collect().is_empty()
    assert lake.items.read("item", view="snapshot", as_of="2020-01-03", strict=True).collect().height == 1
    assert lake.inputs.read(receipt, "item").collect().is_empty()


def test_same_coordinate_revisions_keep_separate_publications(tmp_path):
    lake = _lake(tmp_path)
    lake.items.register(DataItemSpec("item"))
    day = date(2020, 1, 1)
    lake.items.ingest("item", _frame([day, day], [1.0, 2.0], versions=[day, date(2020, 1, 3)]))
    versions = lake.items.read("item", view="versions").collect()
    assert versions["_commit_seq"].n_unique() == 2
    assert lake.items.read("item", view="snapshot", as_of="2020-01-02").collect()["value"].to_list() == [1.0]
    assert lake.items.read("item", view="snapshot", as_of="2020-01-03").collect()["value"].to_list() == [2.0]


def test_distinct_coordinates_with_mixed_input_baselines_keep_separate_commits(tmp_path):
    lake = _lake(tmp_path)
    raw = DatasetSpec("raw", "by_date", date_kind="calendar",
                      field_mappings={"time": "time", "asset_id": "asset_id"})
    day = date(2020, 1, 1)
    source = _frame([day])
    lake.raw.ingest(raw, source, mode="initialize", ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    lake.raw.ingest(raw, source, ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    request = RawInput("custom", "raw", view="versions", include_historical_baseline=True)
    receipt = lake.inputs.freeze({"raw": request}, information_cutoff="2020-01-03")
    lake.items.register(DataItemSpec("item", (request,)))
    lake.items.ingest("item", _frame([day, date(2020, 1, 2)], versions=[day, date(2020, 1, 3)]), input_receipt=receipt)
    versions = lake.items.read("item", view="versions").collect()
    assert versions["_commit_seq"].n_unique() == 2
    assert versions["_baseline"].to_list() == [True, False]
    assert lake.items.read("item", view="snapshot", as_of="2020-01-03", strict=True).collect()["time"].to_list() == [date(2020, 1, 2)]


def test_empty_replacement_records_declared_range_and_upstream_proof(tmp_path):
    lake = _lake(tmp_path)
    raw = DatasetSpec("raw", "by_date", date_kind="calendar",
                      field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(raw, _frame([]), ingested_at=datetime(2020, 1, 31, tzinfo=UTC))
    request = RawInput("custom", "raw", view="versions")
    inputs = lake.inputs.freeze({"raw": request}, information_cutoff="2020-01-31")
    lake.items.register(DataItemSpec("item", (request,)))
    report = lake.items.replace_range("item", _frame([]), start="2020-01-02", end="2020-01-31",
                                      available_date="2020-01-31", input_receipt=inputs)
    assert (report.start, report.end) == (date(2020, 1, 2), date(2020, 1, 31))
    assert lake.items.builds("item")[-1]["start_date"] == "2020-01-02"
    output = lake.inputs.freeze({"item": ItemInput("item", start="2020-01-02", end="2020-01-31", view="snapshot")},
                                information_cutoff="2020-01-31")
    assert output.evidence["item"]["empty_item_build"]["end_date"] == "2020-01-31"
    assert lake.inputs.is_current(output)
    lake.raw.ingest(raw, _frame([date(2020, 1, 4)]), ingested_at=datetime(2020, 1, 31, tzinfo=UTC))
    assert not lake.inputs.is_current(output)
