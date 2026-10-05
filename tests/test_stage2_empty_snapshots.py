from datetime import UTC, date, datetime

import polars as pl

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, RawInput, input_read_boundary


def _lake(root, *, read_only=False):
    return DataLake.open(data_meta_path=root / "data_meta.sqlite",
                         lake_path=root / "lake", read_only=read_only)


def _general():
    return DatasetSpec("members", "general", date_kind="calendar",
                       field_mappings={"time": "time", "asset_id": "asset_id"})


def _rows():
    return pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.0]})


def _received(day):
    return datetime(2020, 1, day, tzinfo=UTC)


def test_frozen_empty_general_keeps_exact_snapshot_after_reopen_and_archive(tmp_path):
    lake = _lake(tmp_path)
    lake.raw.ingest(_general(), _rows(), ingested_at=_received(1))
    first_commit = lake.inputs.max_commit()
    lake.raw.ingest(_general(), _rows().head(0), ingested_at=_received(3))
    requests = {"members": RawInput("custom", "members", view="snapshot", strict=True)}
    frozen = lake.inputs.freeze(requests, information_cutoff="2020-01-03")
    assert lake.raw.read("members", source="custom", as_of="2020-01-03").collect().is_empty()
    assert lake.inputs.read(frozen, "members").collect().is_empty()
    early = lake.inputs.freeze(requests, information_cutoff="2020-01-02")
    bounded = lake.inputs.freeze(requests, information_cutoff="2020-01-03", max_commit=first_commit)
    assert lake.inputs.read(early, "members").collect()["value"].to_list() == [1.0]
    assert lake.inputs.read(bounded, "members").collect()["value"].to_list() == [1.0]
    lake.raw.ingest(_general(), _rows().with_columns(pl.lit(2.0).alias("value")), ingested_at=_received(4))
    lake.raw.remove("members", source="custom")
    reopened = _lake(tmp_path, read_only=True)
    assert reopened.inputs.read(frozen.receipt_id, "members").collect().is_empty()
    assert reopened.inputs.verify(frozen)["valid"]


def test_general_empty_revision_changes_producer_at_its_information_date(tmp_path):
    lake = _lake(tmp_path)
    lake.raw.ingest(_general(), _rows(), ingested_at=_received(1))
    lake.raw.ingest(_general(), _rows().head(0), ingested_at=_received(3))
    observed = []

    def count_members(context):
        count = context.read("members").height
        observed.append((context.information_cutoff, count))
        return pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["market"], "value": [count]})

    lake.items.register(DataItemSpec(
        "count", (RawInput("custom", "members", alias="members"),),
        value_dtype="int64", producer_key="count", producer_revision="1",
    ))
    lake.items.register_producer("count", "1", count_members)
    lake.items.initialize("count", start="2020-01-01", end="2020-01-04")
    assert observed == [(date(2020, 1, 2), 1), (date(2020, 1, 4), 0)]
    assert lake.items.read("count", view="snapshot", as_of="2020-01-02").collect()["value"].to_list() == [1]
    assert lake.items.read("count", view="snapshot", as_of="2020-01-03").collect()["value"].to_list() == [0]


def test_empty_general_snapshot_preserves_its_schema(tmp_path):
    lake = _lake(tmp_path)
    lake.raw.ingest(_general(), _rows(), ingested_at=_received(1))
    empty = _rows().head(0).drop("value").with_columns(pl.lit(None, dtype=pl.Boolean).alias("eligible"))
    lake.raw.ingest(_general(), empty, ingested_at=_received(3))
    frozen = lake.inputs.freeze({"members": RawInput("custom", "members", view="snapshot")},
                                information_cutoff="2020-01-03")
    restored = lake.inputs.read(frozen, "members").collect()
    assert restored.is_empty()
    assert "value" not in restored.columns
    assert restored.schema["eligible"] == pl.Boolean


def test_empty_general_baseline_and_later_attestation_keep_check_boundary(tmp_path):
    lake = _lake(tmp_path)
    lake.raw.ingest(_general(), _rows().head(0), mode="initialize", ingested_at=_received(1))
    maximum = lake.inputs.max_commit()
    before = lake.inputs.freeze({"members": RawInput("custom", "members", view="versions",
                                                     include_historical_baseline=True)},
                                information_cutoff="2020-01-04")
    with input_read_boundary(lake.data_meta_path, maximum, "2020-01-04", max_check_id=before.max_check_id):
        captured = _lake(tmp_path)
    lake.raw.ingest(_general(), _rows().head(0), ingested_at=_received(3))
    statuses = []

    def empty_count(context):
        statuses.append((context.information_cutoff, context.historical_baseline))
        return pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["market"], "value": [0]})

    spec = DataItemSpec("count", (RawInput("custom", "members", alias="members"),),
                        value_dtype="int64", producer_key="empty", producer_revision="1")
    captured.items.register(spec)
    captured.items.register_producer("empty", "1", empty_count)
    captured.items.initialize("count", start="2020-01-01", end="2020-01-04")
    assert statuses == [(date(2020, 1, 4), True)]
    assert captured.items.read("count", view="snapshot", as_of="2020-01-04", strict=True).collect().is_empty()
    statuses.clear()
    lake.items.register_producer("empty", "1", empty_count)
    lake.items.update("count", start="2020-01-01", end="2020-01-04")
    assert statuses == [(date(2020, 1, 2), True), (date(2020, 1, 4), False)]
    assert lake.items.read("count", view="snapshot", as_of="2020-01-02", strict=True).collect().is_empty()
    assert lake.items.read("count", view="snapshot", as_of="2020-01-03", strict=True).collect()["value"].to_list() == [0]
