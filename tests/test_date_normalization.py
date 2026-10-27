"""Retain accepted date forms through repeated canonical normalization."""
from datetime import date, datetime, timezone

import polars as pl
import pytest

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.normalization import NormalizeContext, StandardNormalizer


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
