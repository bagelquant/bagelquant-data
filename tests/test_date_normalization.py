"""Retain accepted date forms through repeated canonical normalization."""
from datetime import date, datetime, timezone

import polars as pl
import pytest

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.normalization import NormalizeContext, StandardNormalizer


@pytest.mark.filterwarnings("error:Casting from String to Date:DeprecationWarning")
@pytest.mark.parametrize("values,dtype,expected", [
    (["20200102", "2020-01-02", "2020-1-2", "2020-01-02 ", "2020-01-02\n",
      None, "invalid", "20200230"], pl.String,
     [date(2020, 1, 2)] * 2 + [None] + [date(2020, 1, 2)] * 2 + [None] * 3),
    ([date(2020, 1, 2), None], pl.Date, [date(2020, 1, 2), None]),
    ([datetime(2020, 1, 2, 3, tzinfo=timezone.utc), None],
     pl.Datetime("us", "UTC"), [date(2020, 1, 2), None]),
    ([20200102, 18263, None], pl.Int64, [date(2020, 1, 2), date(2020, 1, 2), None]),
    ([None, None], pl.Null, [None, None]),
], ids=["strings", "dates", "zoned-datetimes", "integers", "nulls"])
def test_dates_preserve_values_and_types_after_source_time_normalization(values, dtype, expected):
    frame = pl.Series("quoted", values, dtype=dtype).to_frame().lazy()
    spec = DatasetSpec("dates", "by_date", date_kind="calendar",
        field_mappings={"quoted": "time"}, source_time_fields=("time",))
    result = StandardNormalizer().normalize(frame, spec, NormalizeContext("custom", "dates"))
    accepted = result.accepted.collect()
    assert accepted.schema["time"] == accepted.schema["source_time"] == pl.Date
    assert accepted["time"].to_list() == accepted["source_time"].to_list() == expected


@pytest.mark.filterwarnings("error:Casting from String to Date:DeprecationWarning")
def test_iso_dates_keep_whitespace_and_reject_timestamp_text():
    from bagelquant_data.core.normalization import _date_expr

    values = [" 2020-01-02", "2020-01-02\t", "2020-01-02\r\n", "2020-1-02",
              "2020-01-2", "2020-01-02T00:00:00", "2020-01-02junk"]
    frame = pl.DataFrame({"time": values})
    actual = frame.select(_date_expr("time", frame.schema["time"]))
    assert actual["time"].to_list() == [date(2020, 1, 2)] * 5 + [None, None]


@pytest.mark.filterwarnings("error:Casting from String to Date:DeprecationWarning")
def test_item_publication_and_alignment_parse_iso_strings(tmp_path):
    from bagelquant_data import (AvailabilityAlignment, AvailabilityPolicy, DataItemSpec,
                                DataLake, align_panel, materialize_daily_pit)

    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("dates"))
    row = pl.DataFrame({"time": ["2020-01-02\t"], "asset_id": ["A"], "value": [7.],
        "available_date": ["2020-01-02"], "observation_date": ["2020-01-02"],
        "version_available_date": ["2020-01-02"]})
    lake.items.ingest("dates", row)
    assert lake.items.read("dates").collect()["value"].to_list() == [7.]
    coordinates = row.select("time", "asset_id")
    assert align_panel(row, coordinates)["value"].to_list() == [7.]
    policy = AvailabilityPolicy("observation_date", "available_date", 0,
        AvailabilityAlignment.EXACT_DAILY, None, "custom/calendar")
    panel = materialize_daily_pit(row, coordinates.select("time"), policy,
        start=date(2020, 1, 2), end=date(2020, 1, 2))
    assert panel["value"].to_list() == [7.]
