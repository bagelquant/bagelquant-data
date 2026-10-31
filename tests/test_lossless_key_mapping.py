"""Public PIT and Item regressions for lossless typed comparisons."""
from datetime import UTC, date, datetime
import hashlib

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, MapValues, RawInput
from bagelquant_data.core.exceptions import ValidationError
from bagelquant_data.transforms import apply_transforms


def lake_at(root):
    return DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")


def raw_spec():
    return DatasetSpec("numbers", "by_date", date_kind="calendar",
        primary_key_extra=("sub_id",), nullable_primary_key_extra=("sub_id",),
        field_mappings={"time": "time", "asset_id": "asset_id"})


def rows(day, keys, dtype, values=None):
    return pl.DataFrame({"time": [date(2020, 1, day)] * len(keys),
        "asset_id": ["A"] * len(keys), "sub_id": keys,
        "value": values if values is not None else list(range(1, len(keys) + 1))},
        schema_overrides={"sub_id": dtype})


def clock(day):
    return datetime(2020, 1, day, tzinfo=UTC)


def projection(frame):
    return frame.select("source_time", "asset_id", "sub_id", "value").sort(
        "source_time", "asset_id", "sub_id", nulls_last=True)


@pytest.mark.parametrize("verification_dtype", [pl.Int32, pl.Int64])
def test_key_widening_preserves_strict_snapshots_and_frozen_originals(tmp_path, verification_dtype):
    lake = lake_at(tmp_path)
    spec = raw_spec()
    first = rows(1, [1, None], pl.Int32)
    lake.raw.ingest(spec, first, mode="initialize", ingested_at=clock(1))
    lake.raw.ingest(spec, first.cast({"sub_id": verification_dtype}), ingested_at=clock(2))
    request = RawInput("custom", "numbers", view="snapshot", strict=True)
    original = lake.inputs.freeze({"x": request}, information_cutoff="2020-01-02")
    expected = projection(lake.inputs.read(original, "x").collect())
    retained = {path: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in lake.lake_path.rglob("*.parquet")}

    lake.raw.ingest(spec, rows(3, [2], pl.Int64), ingested_at=clock(3))
    live = lake.raw.read("numbers", source="custom", as_of="2020-01-02", strict=True).collect()
    assert projection(live).rows() == expected.rows()
    later = lake.inputs.freeze({"x": request}, information_cutoff="2020-01-03")
    assert projection(lake.inputs.read(later, "x").collect()).rows() == projection(
        lake.raw.read("numbers", source="custom", as_of="2020-01-03", strict=True).collect()).rows()

    # A same-key correction under the wider dtype must resolve one version.
    lake.raw.ingest(spec, rows(1, [1], pl.Int64, [9]), ingested_at=clock(4))
    current = lake.inputs.freeze({"x": request}, information_cutoff="2020-01-04")
    frozen_current = projection(lake.inputs.read(current, "x").collect())
    assert frozen_current.rows() == projection(lake.raw.read(
        "numbers", source="custom", as_of="2020-01-04", strict=True).collect()).rows()
    assert frozen_current.filter(pl.col("sub_id") == 1)["value"].to_list() == [9]
    assert projection(lake.inputs.read(original, "x").collect()).equals(expected)
    assert lake.inputs.get(original.receipt_id).digest == original.digest
    assert all(hashlib.sha256(path.read_bytes()).hexdigest() == digest
        for path, digest in retained.items())
    assert lake.inputs.verify(original)["valid"]


def test_unsafe_integer_float_key_evolution_fails_before_publication(tmp_path):
    lake = lake_at(tmp_path)
    spec = raw_spec()
    first = rows(1, [2**53 + 1], pl.Int64)
    lake.raw.ingest(spec, first, mode="initialize", ingested_at=clock(1))
    receipt = lake.inputs.freeze({"x": RawInput("custom", "numbers")})
    boundary = lake.inputs.max_commit()
    manifests = lake.raw.manifest("numbers", source="custom")
    with pytest.raises(ValidationError, match="[Ll]ossless.*key|key.*[Ll]ossless"):
        lake.raw.ingest(spec, rows(3, [float(2**53)], pl.Float64), ingested_at=clock(3))
    assert lake.inputs.max_commit() == boundary
    assert lake.raw.manifest("numbers", source="custom") == manifests
    assert lake.inputs.read(receipt, "x").collect()["sub_id"].to_list() == [2**53 + 1]


def test_projected_scalar_selection_keeps_extra_keys_for_visibility(tmp_path):
    lake = lake_at(tmp_path)
    spec = raw_spec()
    lake.raw.ingest(spec, rows(1, [1], pl.Int32), mode="initialize", ingested_at=clock(1))
    lake.raw.ingest(spec, rows(1, [1], pl.Int64), ingested_at=clock(2))
    frozen = lake.inputs.freeze({"x": RawInput("custom", "numbers", view="snapshot", strict=True)},
        information_cutoff="2020-01-02")
    assert lake.inputs.read(frozen, "x").collect().height == 1
    lake.inputs.index_selection(frozen, "x", fields=("source_time", "asset_id", "value"))
    saved = lake._data_meta._rows("select has_rows from input_projection_index where source_receipt_id=? and alias='x'",
        (frozen.receipt_id,))
    assert saved and all(row["has_rows"] for row in saved)
    assert lake.inputs.verify(frozen)["valid"]


def test_native_map_preserves_fractional_output_and_metadata_only_repeat(tmp_path, monkeypatch):
    lake = lake_at(tmp_path)
    raw = DatasetSpec("item_numbers", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(raw, pl.DataFrame({"time": [date(2020, 1, 1)] * 3,
        "asset_id": ["A", "B", "C"], "value": [1, 2, None]}),
        mode="initialize", ingested_at=clock(1))
    spec = DataItemSpec("mapped", (RawInput("custom", "item_numbers"),),
        time_column="source_time", value_dtype="float64",
        transforms=(MapValues("value", {1: 1.5}),))
    lake.items.register(spec)
    assert lake.items.initialize("mapped", start="2020-01-01", end="2020-01-01").status == "success"
    assert lake.items.read("mapped").collect()["value"].to_list() == [1.5, 2.0, None]
    import bagelquant_data.items.api as item_api
    monkeypatch.setattr(item_api, "apply_transforms", lambda *_: pytest.fail("unchanged history recomputed"))
    assert lake.items.update("mapped", end="2020-01-01").status == "unchanged"


def test_map_output_values_are_lossless_and_unmapped_values_survive():
    source = pl.DataFrame({"value": [1, 2, None]})
    mapped = apply_transforms(source, (MapValues("value", {1: 1.5, None: 0.25}, "mapped"),), {})
    assert mapped["value"].to_list() == [1, 2, None]
    assert mapped["mapped"].to_list() == [1.5, 2.0, 0.25]
    large = pl.DataFrame({"value": [1, 2**53 + 1]})
    with pytest.raises(ValueError, match="[Ll]ossless"):
        apply_transforms(large, (MapValues("value", {1: 1.5}),), {})
    assert apply_transforms(large, (MapValues("value", {1: None}),), {})["value"].to_list() == [None, 2**53 + 1]


def test_native_map_declared_integer_dtype_cannot_truncate_output(tmp_path):
    lake = lake_at(tmp_path)
    source = DatasetSpec("source", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(source, pl.DataFrame({"time": [date(2020, 1, 1)],
        "asset_id": ["A"], "value": [1]}), mode="initialize", ingested_at=clock(1))
    lake.items.register(DataItemSpec("mapped", (RawInput("custom", "source"),),
        value_dtype="int64", time_column="source_time", transforms=(MapValues("value", {1: 1.5}),)))
    before = lake.inputs.max_commit()
    with pytest.raises(ValueError, match="Lossless"):
        lake.items.initialize("mapped", start="2020-01-01", end="2020-01-01")
    assert lake.inputs.max_commit() == before
    assert lake.items.read("mapped").collect().is_empty()


def test_float_key_widening_preserves_signed_zero_and_null_witnesses(tmp_path):
    import math
    lake = lake_at(tmp_path)
    spec = raw_spec()
    first = rows(1, [-0.0, 0.0, None], pl.Float32)
    lake.raw.ingest(spec, first, mode="initialize", ingested_at=clock(1))
    lake.raw.ingest(spec, first, ingested_at=clock(2))
    lake.raw.ingest(spec, rows(3, [2.0], pl.Float64), ingested_at=clock(3))
    actual = lake.raw.read("numbers", source="custom", as_of="2020-01-02", strict=True).collect()
    assert actual.height == 3
    assert {math.copysign(1, key): value for key, value in actual.select("sub_id", "value").iter_rows()
        if key is not None} == {-1: 1, 1: 2}
    frozen = lake.inputs.freeze({"x": RawInput("custom", "numbers", view="snapshot", strict=True)},
        information_cutoff="2020-01-02")
    assert lake.inputs.read(frozen, "x").collect().height == 3


def test_stale_map_implementation_requires_explicit_reregistration(tmp_path, monkeypatch):
    import bagelquant_data.items.api as item_api
    lake = lake_at(tmp_path)
    spec = DataItemSpec("mapped", transforms=(MapValues("value", {1: 1.5}),))
    lake.items.register(spec)
    lake.items.ingest("mapped", pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.0]}), historical_baseline=True)
    from bagelquant_data.items.types import spec_payload
    legacy = {key: value for key, value in spec_payload(spec).items() if key != "description"}
    legacy_hash = hashlib.sha256(item_api._json(legacy).encode()).hexdigest()
    with lake._data_meta.connect() as db:
        db.execute("update item_definitions set spec_hash=? where name='mapped'", (legacy_hash,))
    before = lake.inputs.max_commit()
    monkeypatch.setattr(lake.inputs, "freeze", lambda *_args, **_kwargs: pytest.fail("stale implementation froze inputs"))
    with pytest.raises(ValueError, match="re-register"):
        lake.items.plan_update("mapped", end="2020-01-02")
    with pytest.raises(ValueError, match="re-register"):
        lake.items.update("mapped", end="2020-01-02")
    assert lake.inputs.max_commit() == before
    assert lake.items.read("mapped").collect()["value"].to_list() == [1.0]
    assert item_api._definition_hash(spec) != legacy_hash
