from datetime import UTC, date, datetime

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, ExecutionOptions, DataItemSpec, RawInput


def test_raw_history_default_keeps_observation_window_separate_from_cutoff(tmp_path):
    lake = DataLake.open(
        data_meta_path=tmp_path / "data_meta.sqlite", lake_path=tmp_path / "lake"
    )
    spec = DatasetSpec(
        "prices",
        "by_date",
        date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"},
    )
    frame = pl.DataFrame(
        {"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.0]}
    )
    lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    lake.raw.ingest(
        spec,
        frame.with_columns(pl.lit(2.0).alias("value")),
        ingested_at=datetime(2020, 1, 3, tzinfo=UTC),
    )
    assert lake.raw.read("prices", source="custom").collect()["value"].to_list() == [
        1.0
    ]
    assert lake.raw.read(
        "prices", source="custom", observation_end="2020-01-01", as_of="2020-01-03"
    ).collect()["value"].to_list() == [2.0]


def test_oversized_response_fails_without_publishing_data(tmp_path):
    class Provider:
        name = "fake"

        def fetch(self, api, params):
            return pl.DataFrame(
                {"time": ["2020-01-01"], "asset_id": ["A"], "value": ["x" * 1024]}
            )

    lake = DataLake.open(
        data_meta_path=tmp_path / "data_meta.sqlite", lake_path=tmp_path / "lake"
    )
    lake.catalog.sources.register(Provider())
    lake.raw.register(
        DatasetSpec(
            "events",
            "by_date",
            source="fake",
            date_kind="calendar",
            field_mappings={"time": "time", "asset_id": "asset_id"},
        )
    )
    report = lake.raw.initialize(
        "events",
        source="fake",
        start="2020-01-01",
        end="2020-01-01",
        config=ExecutionOptions(workers=1, max_buffer_bytes=128),
        max_retries=1,
        retry_backoff_seconds=0,
    )
    assert report.rows_committed == 0
    assert lake.raw.manifest("events", source="fake") == []
    scopes = lake.integrity.update_scopes("events", source="fake")
    assert scopes[0]["status"] == "invalid"
    assert "max_buffer_bytes" in scopes[0]["last_error"]


def test_item_input_buffers_respect_explicit_budget(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "data_meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("values", "by_date", date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(spec, pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": ["x" * 2048]}), ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    lake.items.register(DataItemSpec("derived", (RawInput("custom", "values"),), time_column="source_time", value_dtype="string"))
    with pytest.raises(MemoryError, match="max_buffer_bytes"):
        lake.items.update("derived", end="2020-01-01", config=ExecutionOptions(max_buffer_bytes=128))
    assert lake.items.manifest("derived") == []


def test_single_revision_receives_actual_admission_buffer_with_many_requested_workers(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "data_meta.sqlite", lake_path=tmp_path / "lake")
    lake.raw.ingest(DatasetSpec("seed", "by_date", date_kind="calendar",
                              field_mappings={"time": "time", "asset_id": "asset_id"}),
                    pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.0]}),
                    mode="initialize", ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    def producer(context):
        return pl.DataFrame({"time": [date(2020, 1, 1)] * 128,
            "asset_id": [f"A{index}" for index in range(128)], "value": [1.0] * 128})
    lake.items.register_producer("fixture", "1", producer)
    lake.items.register(DataItemSpec("expanded", (RawInput("custom", "seed"),),
                                   producer_key="fixture", producer_revision="1"))
    report = lake.items.initialize("expanded", start="2020-01-01", end="2020-01-01",
        config=ExecutionOptions(workers=8, max_in_flight=8, max_buffer_bytes=8192))
    assert report.status == "success" and report.rows_committed == 128
    assert lake.items.read("expanded").collect().height == 128
