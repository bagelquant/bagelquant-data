"""External publication must retain complete, usable input and timing evidence."""
from dataclasses import replace
from datetime import UTC, date, datetime

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemPublication, RawInput


DAY = date(2020, 1, 2)


def fixture(root):
    lake = DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")
    lake.raw.ingest(DatasetSpec("prices", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"}),
        pl.DataFrame({"time": [DAY], "asset_id": ["A"], "close": [10.], "volume": [500.]}),
        ingested_at=datetime(2020, 1, 2, tzinfo=UTC))
    return lake


def output():
    return pl.DataFrame({"time": [DAY], "asset_id": ["A"], "value": [10.]})


def assert_no_output(lake, names):
    for name in names:
        assert lake.items.read(name, view="versions").collect().is_empty()
        assert lake.items.builds(name) == []
    assert lake._data_meta._rows("select 1 from version_commits where source='items'") == []
    assert lake._data_meta._rows("select 1 from node_completions where node_key like 'items/%'") == []


@pytest.mark.parametrize("mixed", [False, True], ids=["all-null", "mixed"])
@pytest.mark.parametrize("batch", [False, True], ids=["ingest", "publication-group"])
def test_null_version_dates_fail_before_any_output(tmp_path, mixed, batch):
    lake = fixture(tmp_path)
    lake.items.register(DataItemSpec("first"))
    lake.items.register(DataItemSpec("invalid"))
    rows = output().with_columns(pl.lit(None, dtype=pl.Date).alias("version_available_date"))
    if mixed:
        rows = pl.concat([rows, output().with_columns(pl.lit("B").alias("asset_id"),
            pl.lit(DAY).alias("version_available_date"))])
    with pytest.raises(ValueError, match="version.*null"):
        if batch:
            with lake.items.publication() as publisher:
                publisher.publish([ItemPublication("first", output()), ItemPublication("invalid", rows)])
        else:
            lake.items.ingest("invalid", rows)
    assert_no_output(lake, ["first", "invalid"])


@pytest.mark.parametrize("batch", [False, True], ids=["replace-range", "publication-group"])
@pytest.mark.parametrize("mismatch", ["fields", "strict", "view", "window", "observation-window", "baseline", "repeated"])
def test_wrong_input_selections_fail_before_any_output(tmp_path, batch, mismatch):
    lake = fixture(tmp_path)
    declared = RawInput("custom", "prices", alias="prices", fields=("close",),
        start=DAY, end=DAY, view="versions", strict=True)
    actual = declared
    inputs = (declared,)
    if mismatch == "fields":
        actual = replace(actual, fields=("volume",))
    elif mismatch == "strict":
        actual = replace(actual, strict=False)
    elif mismatch == "view":
        actual = replace(actual, view="history")
    elif mismatch == "window":
        actual = replace(actual, start=date(2020, 1, 3), end=date(2020, 1, 3))
    elif mismatch == "observation-window":
        declared = replace(declared, observation_start=DAY, observation_end=DAY)
        inputs = (declared,)
        actual = replace(actual, observation_start=date(2020, 1, 3), observation_end=date(2020, 1, 3))
    elif mismatch == "baseline":
        declared = replace(declared, include_historical_baseline=True)
        inputs = (declared,)
    else:
        inputs += (replace(declared, alias="volume", fields=("volume",)),)
    receipt = lake.inputs.freeze({"renamed": actual}, information_cutoff=DAY)
    lake.items.register(DataItemSpec("first", (actual,)))
    lake.items.register(DataItemSpec("invalid", inputs))
    with pytest.raises(ValueError, match="declared DataItem.*selection"):
        if batch:
            with lake.items.publication(input_receipt=receipt) as publisher:
                publisher.publish([ItemPublication("first", output()), ItemPublication("invalid", output())])
        else:
            lake.items.replace_range("invalid", output(), start=DAY, end=DAY,
                available_date=DAY, input_receipt=receipt)
    assert_no_output(lake, ["first", "invalid"])


def test_bounded_normalized_captures_and_repeated_selections_are_valid(tmp_path):
    lake = fixture(tmp_path)
    first = RawInput("custom", "prices", alias="close", fields=("close",))
    second = replace(first, alias="volume", fields=("volume",))
    lake.items.register(DataItemSpec("valid", (first, second)))
    # Native preparation widens the retained view but bounds its read window.
    broad = replace(first, alias="broad", fields=("close", "volume"), view="versions",
        include_historical_baseline=True, start=DAY, end=DAY)
    narrow = replace(first, alias="narrow", view="versions",
        include_historical_baseline=True, start=DAY, end=DAY)
    receipt = lake.inputs.freeze({"broad": broad, "narrow": narrow}, information_cutoff=DAY)
    report = lake.items.replace_range("valid", output(), start=DAY, end=DAY,
        available_date=DAY, input_receipt=receipt)
    assert report.rows_committed == 1
    assert lake.items.read("valid", view="snapshot", as_of=DAY).collect()["value"].to_list() == [10.]
    assert lake._data_meta._rows("select dataset from item_dependencies where item_name='valid'") == [{"dataset": "prices"}]


def test_native_explicit_window_narrowing_retains_noop_history(tmp_path, monkeypatch):
    lake = fixture(tmp_path)
    request = RawInput("custom", "prices", alias="prices", fields=("source_time", "asset_id", "close"),
        start="2020-01-01", end="2020-01-31")
    lake.items.register(DataItemSpec("native", (request,), time_column="source_time", value_column="close"))
    options = {"start": DAY, "end": DAY, "input_windows": {"prices": (DAY, DAY)}}
    first = lake.items.update("native", **options)
    assert first.rows_committed == 1
    assert lake.items.read("native").collect()["value"].to_list() == [10.]
    monkeypatch.setattr(lake.inputs, "read", lambda *_a, **_k: pytest.fail("no-op read historical values"))
    second = lake.items.update("native", **options)
    assert second.status == "unchanged"
    assert second.commit_seq == first.commit_seq
