"""Raw fields and actual observation coordinates retain the original PIT axis."""
from datetime import UTC, date, datetime, timedelta
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, InputSelectionScope, RawInput, input_index


DAY = date(2024, 1, 1)
FIELDS = ("time", "asset_id", "open", "close")


def fixture(tmp_path, *, view="history", extra=False):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("prices", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"},
        primary_key_extra=("venue",) if extra else ())
    frame = pl.DataFrame({"time": [DAY, DAY + timedelta(days=1), DAY],
        "asset_id": ["A", "A", "B"], "open": [10., 11., 20.],
        "close": [11., 12., 21.], "tag": ["original"] * 3})
    if extra:
        frame = pl.concat([frame.with_columns(pl.lit("first").alias("venue")),
            frame.head(1).with_columns(pl.lit("second").alias("venue"))])
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2024, 1, 1, tzinfo=UTC))
    request = RawInput("custom", "prices", start=DAY, end=DAY + timedelta(days=5),
        fields=FIELDS, view=view, include_historical_baseline=True)
    def freeze():
        return lake.inputs.freeze({"x": request}, information_cutoff=DAY + timedelta(days=6), retain_batch_bounds=True)
    return lake, spec, frame, freeze, freeze()


def prohibit_reads(monkeypatch):
    from bagelquant_data import inputs
    def fail(*_a, **_k):
        pytest.fail("ordinary Raw proof decoded original bytes or historical row indexes")
    monkeypatch.setattr(inputs, "read_batch", fail)
    monkeypatch.setattr(input_index, "project_original", fail)
    monkeypatch.setattr(input_index, "project_original", fail)


def remove_indexes(lake):
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_value_index'").fetchone() is None
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_index'").fetchone() is None


@pytest.mark.parametrize("view", ["history", "snapshot"])
def test_raw_nonmember_and_unconsumed_changes_have_metadata_only_equivalence(tmp_path, monkeypatch, view):
    lake, spec, frame, freeze, original = fixture(tmp_path, view=view)
    coords = frame.filter(pl.col("asset_id") == "A").select("time", "asset_id")
    lake.raw.ingest(spec, frame.head(1).with_columns(pl.lit("revised").alias("tag")),
        ingested_at=datetime(2024, 1, 1, 12, tzinfo=UTC))
    lake.raw.ingest(spec, frame.tail(1).with_columns(pl.lit(99.).alias("close")),
        ingested_at=datetime(2024, 1, 1, 12, tzinfo=UTC))
    current = freeze()
    remove_indexes(lake)
    tokens = [lake.inputs.index_selection(root, "x", fields=FIELDS, coordinates=coords)
        for root in (original, current)]
    assert tokens[0] == tokens[1] and tokens[0]
    assert lake.inputs.verify([original, current])["valid"]
    assert lake.inputs.get(original).digest == original.digest
    prohibit_reads(monkeypatch)
    assert lake.inputs.selection_equivalent(original, current, "x", fields=FIELDS,
        coordinates=coords.select("asset_id", "time").reverse(), summaries_only=True) is True
    assert lake.inputs.index_selection(current, "x", fields=FIELDS, coordinates=coords) == tokens[0]


@pytest.mark.parametrize("value", [None, float("nan"), float("inf"), -float("inf"), 99.])
def test_changed_consumed_raw_value_is_not_equivalent(tmp_path, monkeypatch, value):
    lake, spec, frame, freeze, original = fixture(tmp_path)
    coords = frame.head(1).select("time", "asset_id")
    lake.raw.ingest(spec, frame.head(1).with_columns(pl.lit(value, dtype=pl.Float64).alias("open")),
        ingested_at=datetime(2024, 1, 1, 12, tzinfo=UTC))
    current = freeze()
    first = lake.inputs.index_selection(original, "x", fields=FIELDS, coordinates=coords)
    second = lake.inputs.index_selection(current, "x", fields=FIELDS, coordinates=coords)
    assert first and second and first != second
    assert lake.inputs.verify([original, current])["valid"]
    prohibit_reads(monkeypatch)
    assert lake.inputs.selection_equivalent(original, current, "x", fields=FIELDS, coordinates=coords) is False


def test_raw_coordinates_select_observations_without_replacing_version_time(tmp_path):
    lake, spec, frame, freeze, original = fixture(tmp_path, view="snapshot")
    coords = frame.head(1).select("time", "asset_id")
    # A later physical availability is still the same observed coordinate.
    lake.raw.ingest(spec, frame.head(1).with_columns(pl.lit("late").alias("tag")),
        ingested_at=datetime(2024, 1, 4, tzinfo=UTC))
    current = freeze()
    selected = lake.inputs.read(current, "x", fields=("source_time", *FIELDS)).collect()
    revised = selected.filter(pl.col("asset_id") == "A", pl.col("source_time") == DAY)
    assert revised["time"].item() == DAY + timedelta(days=3)
    untimed = ("source_time", "asset_id", "open", "close")
    assert lake.inputs.index_selection(original, "x", fields=untimed, coordinates=coords) == (
        lake.inputs.index_selection(current, "x", fields=untimed, coordinates=coords))
    assert lake.inputs.index_selection(original, "x", fields=FIELDS, coordinates=coords) != (
        lake.inputs.index_selection(current, "x", fields=FIELDS, coordinates=coords))
    assert lake.inputs.verify([original, current])["valid"]


def test_history_price_time_matches_observation_axis_and_strict_baseline_is_preserved(tmp_path):
    lake, spec, frame, freeze, original = fixture(tmp_path)
    coords = frame.head(1).select("time", "asset_id")
    lake.raw.ingest(spec, frame.head(1).with_columns(pl.lit(99.).alias("close")),
        ingested_at=datetime(2024, 1, 4, tzinfo=UTC))
    current = freeze()
    history = lake.inputs.read(current, "x", fields=("source_time", *FIELDS)).collect()
    assert history["time"].equals(history["source_time"])
    assert history.filter(pl.col("asset_id") == "A", pl.col("source_time") == DAY)["close"].item() == 11.
    normal = lake.inputs.index_selection(current, "x", fields=FIELDS, coordinates=coords)
    empty = lake.inputs.index_selection(current, "x", fields=FIELDS, coordinates=coords, strict=True)
    assert normal and empty and normal != empty
    assert lake.inputs.index_selection(original, "x", fields=FIELDS, coordinates=coords) == normal
    assert lake.inputs.verify([original, current])["valid"]


def test_raw_extra_record_keys_keep_every_selected_row(tmp_path):
    lake, spec, frame, freeze, original = fixture(tmp_path, extra=True)
    coords = frame.head(1).select("time", "asset_id")
    scopes = [InputSelectionScope("x", fields=FIELDS, coordinates=coords),
        InputSelectionScope("x", fields=FIELDS, coordinate_selector=lake.inputs.coordinate_selector_identity(coords))]
    first = lake.inputs.index_selection(original, "x", fields=FIELDS, coordinates=coords)
    lake.raw.ingest(spec, frame.filter(pl.col("venue") == "second").with_columns(pl.lit(99.).alias("close")),
        ingested_at=datetime(2024, 1, 1, 12, tzinfo=UTC))
    current = freeze()
    second = lake.inputs.index_selections(current, scopes)
    assert first and all(second) and second[0] == second[1] and first != second[0]
    assert lake.inputs.read(current, "x", fields=("source_time", *FIELDS)).collect().filter(
        pl.col("source_time") == DAY, pl.col("asset_id") == "A").height == 2
    assert lake.inputs.verify([original, current])["valid"]


def test_missing_raw_coordinates_and_empty_types_remain_distinct(tmp_path, monkeypatch):
    lake, spec, frame, freeze, original = fixture(tmp_path)
    empty = pl.DataFrame(schema={"time": pl.Date, "asset_id": pl.String})
    absent = pl.DataFrame({"time": [DAY], "asset_id": ["ABSENT"]})
    missing = lake.inputs.index_selection(original, "x", fields=FIELDS, coordinates=absent)
    assert missing and missing != lake.inputs.index_selection(original, "x", fields=FIELDS, coordinates=empty)
    lake.raw.ingest(spec, frame.head(1).with_columns(pl.lit("ABSENT").alias("asset_id")),
        ingested_at=datetime(2024, 1, 1, 12, tzinfo=UTC))
    current = freeze()
    assert lake.inputs.index_selection(current, "x", fields=FIELDS, coordinates=absent) != missing
    assert lake.inputs.selection_equivalent(original, current, "x", fields=FIELDS, coordinates=empty) is None
    assert lake.inputs.index_selection(current, "x", fields=FIELDS, coordinates=empty)
    assert lake.inputs.verify([original, current])["valid"]
    prohibit_reads(monkeypatch)
    assert lake.inputs.selection_equivalent(original, current, "x", fields=FIELDS, coordinates=empty) is True
    assert lake.inputs.selection_equivalent(original, current, "x", fields=FIELDS, coordinates=absent) is False


def test_raw_coordinate_scope_cannot_widen_observation_bounds_or_old_scalar_api(tmp_path, monkeypatch):
    lake, _, frame, _, original = fixture(tmp_path)
    clipped = lake.inputs.freeze({"x": RawInput("custom", "prices", start=DAY, end=DAY + timedelta(days=5),
        observation_start=DAY + timedelta(days=1), observation_end=DAY + timedelta(days=2))},
        information_cutoff=DAY + timedelta(days=6))
    coords = frame.head(1).select("time", "asset_id")
    prohibit_reads(monkeypatch)
    with pytest.raises(ValueError, match="observation window"):
        lake.inputs.selection_identity(clipped, "x", fields=FIELDS, coordinates=coords)
    with pytest.raises(ValueError, match="coordinates require|Raw payload projections"):
        lake.inputs.selection_identity(original, "x", values_only=True, coordinates=coords)
    duplicate = pl.concat([coords, coords])
    with pytest.raises(ValueError, match="unique non-null"):
        lake.inputs.selection_identity(original, "x", fields=FIELDS, coordinates=duplicate)
    assert lake.inputs.selection_identity(original, "x", fields=FIELDS, coordinates=coords) is not None
