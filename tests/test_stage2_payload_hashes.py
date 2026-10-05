from datetime import UTC, date, datetime
from math import isinf, isnan

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.pipeline.versions import _payload_hashes


def _lake(root):
    return DataLake.open(data_meta_path=root / "data_meta.sqlite", lake_path=root / "lake")


def _spec():
    return DatasetSpec("values", "by_date", date_kind="calendar",
                       field_mappings={"time": "time", "asset_id": "asset_id"})


def _frame(value):
    return pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [value]},
                        schema={"time": pl.Date, "asset_id": pl.String, "value": pl.Float64})


def test_null_nan_and_signed_infinities_are_separate_revisions(tmp_path):
    lake = _lake(tmp_path)
    for day, value in enumerate((None, float("nan"), float("inf"), float("-inf")), start=1):
        lake.raw.ingest(_spec(), _frame(value), ingested_at=datetime(2020, 1, day, tzinfo=UTC))
    versions = lake.raw.read_versions("values", source="custom").collect().sort("time")
    assert versions.height == 4
    assert versions["_payload_hash"].n_unique() == 4
    values = versions["value"].to_list()
    assert values[0] is None and isnan(values[1])
    assert isinf(values[2]) and values[2] > 0
    assert isinf(values[3]) and values[3] < 0
    maximum = lake.inputs.max_commit()
    lake.raw.ingest(_spec(), _frame(float("-inf")), ingested_at=datetime(2020, 1, 5, tzinfo=UTC))
    assert lake.inputs.max_commit() == maximum


def test_conflicting_nonfinite_output_keys_are_rejected(tmp_path):
    lake = _lake(tmp_path)
    conflicting = pl.concat([_frame(float("nan")), _frame(float("inf"))])
    with pytest.raises(ValueError, match="Conflicting rows"):
        lake.raw.ingest(_spec(), conflicting, ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    assert lake.inputs.max_commit() == 0


def test_nested_and_schema_typed_payload_hashes(tmp_path):
    nested = pl.DataFrame({"payload": [
        {"label": "x", "values": [None, float("inf")]},
        {"label": "x", "values": [float("nan"), float("inf")]},
        {"label": "x", "values": [None, float("-inf")]},
    ]})
    hashes = _payload_hashes(nested, ["payload"])
    assert hashes.n_unique() == 3
    assert _payload_hashes(nested.reverse(), ["payload"]).to_list() == hashes.reverse().to_list()
    assert _payload_hashes(pl.DataFrame({"value": [1]}, schema={"value": pl.Int64}), ["value"])[0] != \
        _payload_hashes(pl.DataFrame({"value": [1.0]}, schema={"value": pl.Float64}), ["value"])[0]
    lake = _lake(tmp_path)
    for day, value in enumerate(nested["payload"].to_list(), start=1):
        frame = _frame(1.0).with_columns(pl.Series("payload", [value], dtype=nested.schema["payload"]))
        lake.raw.ingest(_spec(), frame, ingested_at=datetime(2020, 1, day, tzinfo=UTC))
    assert lake.raw.read_versions("values", source="custom").collect().height == 3


def test_nanosecond_temporal_values_keep_native_precision_in_nested_hashes():
    frame = pl.DataFrame({"stamp": pl.Series([1, 2], dtype=pl.Int64).cast(pl.Datetime("ns"))})
    assert _payload_hashes(frame, ["stamp"]).n_unique() == 2
    nested = frame.select(pl.struct("stamp").alias("nested"))
    assert _payload_hashes(nested, ["nested"]).n_unique() == 2


def test_nullable_float_record_keys_remain_distinct(tmp_path):
    lake = _lake(tmp_path)
    spec = DatasetSpec("keys", "by_date", date_kind="calendar",
                       field_mappings={"time": "time", "asset_id": "asset_id"},
                       primary_key_extra=("variant",), nullable_primary_key_extra=("variant",))
    frame = pl.DataFrame({"time": [date(2020, 1, 1)] * 4, "asset_id": ["A"] * 4,
                          "variant": [None, float("nan"), float("inf"), float("-inf")],
                          "value": [1.0, 2.0, 3.0, 4.0]})
    lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    restored = lake.raw.read("keys", source="custom").collect()
    assert restored.height == 4
    assert restored["_record_id"].n_unique() == 4
    assert sorted(restored["value"].to_list()) == [1.0, 2.0, 3.0, 4.0]
