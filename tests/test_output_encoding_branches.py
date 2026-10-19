"""Precision and publication branches for typed-row encoding reuse."""
from datetime import UTC, date, datetime

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec
from bagelquant_data.items.preparation import prepared_window
from bagelquant_data.pipeline import versions


@pytest.mark.parametrize("dtype", [pl.Datetime("ns"), pl.Datetime("ns", "UTC"), pl.Duration("ns"), pl.Time])
def test_same_schema_native_temporal_key_differences_never_reuse_hashes(tmp_path, dtype):
    path = tmp_path / "data.sqlite"
    first = pl.DataFrame({"key": pl.Series([1, None, 2], dtype=pl.Int64).cast(dtype)})
    changed = pl.DataFrame({"key": pl.Series([2, None, 1], dtype=pl.Int64).cast(dtype)})
    with prepared_window(path, 100_000) as work:
        old = versions._record_hashes(first, ["key"], path)
        new = versions._record_hashes(changed, ["key"], path)
        assert new.equals(versions._payload_hashes(changed, ["key"]).rename("_record_id"))
        assert old[0] != new[0] and old[2] != new[2]
        assert work.encoding_counts["output_key_rows_encoded"] == 6
        assert "output_key_cache_hits" not in work.encoding_counts
        assert versions._record_hashes(changed, ["key"], path).equals(new)
        assert work.encoding_counts["output_key_cache_hits"] == 1


@pytest.mark.parametrize("incoming_dtype,expected_calls", [(pl.Float64, 2), (pl.Int64, 3)])
def test_payload_rehash_skips_only_unchanged_schema_and_values(tmp_path, monkeypatch, incoming_dtype, expected_calls):
    lake = DataLake.open(data_meta_path=tmp_path / "data.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    def frame(dtype):
        return pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"],
            "value": pl.Series([1], dtype=dtype)})
    lake.raw.ingest(spec, frame(pl.Float64), ingested_at=datetime(2024, 1, 1, tzinfo=UTC))
    commit = lake.inputs.max_commit()
    calls, original = [], versions._payload_hashes
    def encode(values, fields):
        if "value" in fields:
            calls.append(values.select(fields).schema)
        return original(values, fields)
    monkeypatch.setattr(versions, "_payload_hashes", encode)
    lake.raw.ingest(spec, frame(incoming_dtype), ingested_at=datetime(2024, 1, 2, tzinfo=UTC))
    assert len(calls) == expected_calls
    assert calls[-1]["value"] == pl.Float64
    assert lake.inputs.max_commit() == commit


def test_explicit_semantic_version_dates_keep_required_payload_rehash_and_history(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "data.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("external", ()))
    def frame(day):
        return pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "value": [-0.],
            "version_available_date": [date(2024, 1, day)]})
    lake.items.ingest("external", frame(2), ingested_at=datetime(2024, 1, 2, tzinfo=UTC))
    original, calls = versions._payload_hashes, []
    def encode(values, fields):
        if "value" in fields and "time" in fields:
            calls.append(values.select(fields).clone())
        return original(values, fields)
    monkeypatch.setattr(versions, "_payload_hashes", encode)
    lake.items.ingest("external", frame(3), ingested_at=datetime(2024, 1, 3, tzinfo=UTC))
    assert len(calls) == 3
    retained = lake.items.read("external", view="versions").collect()
    assert retained.height == 2
    assert retained["version_available_date"].sort().to_list() == [date(2024, 1, 2), date(2024, 1, 3)]
