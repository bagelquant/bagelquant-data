from datetime import date

import polars as pl
import pytest

from bagelquant_data.exploration import coverage, cross_section, distribution, overview, profile_item, quantiles, time_series


def test_coordinates_and_finite_statistics_are_independent():
    dates = [date(2020, 1, day) for day in range(1, 7)]
    frame = pl.DataFrame({"time": dates[:5], "asset_id": ["A"] * 5, "value": [1.0, 3.0, None, float("nan"), float("inf")]})
    coordinates = pl.DataFrame({"time": dates, "asset_id": ["A"] * 6})
    summary = overview(frame, coordinates=coordinates)
    assert (summary["expected_count"], summary["valid_count"], summary["missing_count"]) == (6, 2, 4)
    assert (summary["null_count"], summary["nan_count"], summary["infinite_count"]) == (2, 1, 1)
    assert summary["coverage_rate"] == pytest.approx(1 / 3)
    stats = dict(distribution(frame).iter_rows())
    assert stats["mean"] == 2.0
    assert quantiles(frame, (.5,))["value"].to_list() == [2.0]
    assert coverage(frame, coordinates, by="asset_id")["missing_count"].to_list() == [4]
    assert time_series(frame, "A").height == 5
    assert cross_section(frame, dates[0])["value"].to_list() == [1.0]
    assert profile_item(frame, coordinates)["values"].height == 2
    assert overview(frame)["coverage_rate"] is None


def test_categorical_empty_and_duplicate_inputs():
    frame = pl.DataFrame({"time": [date(2020, 1, 1), date(2020, 1, 2)], "asset_id": ["A", "A"], "value": ["x", None]})
    assert distribution(frame).rows() == [("x", 1)]
    with pytest.raises(ValueError, match="numerical"):
        quantiles(frame)
    empty = frame.head(0)
    assert overview(empty, coordinates=empty)["coverage_rate"] is None
    assert coverage(empty, empty).is_empty()
    with pytest.raises(ValueError, match="duplicate"):
        overview(pl.concat([frame, frame]))
