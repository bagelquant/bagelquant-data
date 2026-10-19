"""Output-key reuse preserves the persisted typed-byte contract and budget."""
from datetime import date

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ExecutionOptions, RawInput
from bagelquant_data.items.preparation import current_preparation, prepared_window
from bagelquant_data.pipeline.versions import _payload_hashes, _record_hashes


def keys():
    return pl.DataFrame({"source_time": [date(2024, 1, 1), date(2024, 1, 2)],
        "asset_id": ["Ω😀", "A"], "value": [1., 2.]})


def test_exact_keys_share_hashes_but_values_order_schema_and_lake_are_independent(tmp_path):
    path = tmp_path / "data.sqlite"
    frame = keys()
    fields = ["source_time", "asset_id"]
    with prepared_window(path, 100_000) as work:
        first = _record_hashes(frame, fields, path)
        assert _record_hashes(frame.with_columns(pl.lit(-0.).alias("value")), fields, path).equals(first)
        assert work.encoding_counts == {"output_key_rows_encoded": 2, "output_key_cache_hits": 1}
        for changed in (frame.reverse(), frame.with_columns(pl.lit("B").alias("asset_id")),
                        frame.with_columns(pl.col("source_time").cast(pl.Datetime("ns")))):
            assert _record_hashes(changed, fields, path).equals(_payload_hashes(changed, fields).rename("_record_id"))
        assert _record_hashes(frame, fields, tmp_path / "other.sqlite").equals(first)
        assert sum(charge for _, charge in work.frames.values()) == work.retained_bytes
        assert work.retained_bytes <= work.byte_limit
    assert work.retained_bytes == 0 and not work.frames
    assert current_preparation(path) is None
    with prepared_window(path, 100_000) as later:
        _record_hashes(frame, fields, path)
        assert later.encoding_counts == {"output_key_rows_encoded": 2}


@pytest.mark.parametrize("dtype,values", [
    (pl.Float64, [-0., 0.]),
    (pl.List(pl.Float64), [[-0.], [0.]]),
    (pl.Struct({"x": pl.Float64}), [{"x": -0.}, {"x": 0.}]),
])
def test_floating_and_nested_keys_keep_the_scalar_path(tmp_path, dtype, values):
    path = tmp_path / "data.sqlite"
    frame = pl.DataFrame({"key": pl.Series(values, dtype=dtype)})
    with prepared_window(path, 100_000) as work:
        for _ in range(2):
            hashes = _record_hashes(frame, ["key"], path)
            assert hashes.equals(_payload_hashes(frame, ["key"]).rename("_record_id"))
            assert hashes.n_unique() == 2
        assert work.encoding_counts == {"output_key_rows_encoded": 4}
        assert not work.frames


def test_zero_budget_and_eviction_do_not_admit_uncharged_encodings(tmp_path):
    path = tmp_path / "data.sqlite"
    frame, fields = keys(), ["source_time", "asset_id"]
    for budget in (0, 5000):
        with prepared_window(path, budget) as work:
            _record_hashes(frame, fields, path)
            # Replacing a same-shape entry must release the previous charge.
            _record_hashes(frame.reverse(), fields, path)
            assert work.retained_bytes == sum(charge for _, charge in work.frames.values())
            work.put(("other",), pl.DataFrame({"x": list(range(50))}))
            assert work.retained_bytes <= budget
            _record_hashes(frame, fields, path)
            assert work.retained_bytes <= budget


def test_public_item_group_shares_output_keys_and_keeps_individual_payloads(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "data.sqlite", lake_path=tmp_path / "lake")
    frame = keys().rename({"source_time": "time"}).with_columns((pl.col("value") + 10).alias("other"))
    lake.raw.ingest(DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"}), frame, mode="initialize")
    for name in ("value", "other"):
        lake.items.register(DataItemSpec(name, (RawInput("custom", "daily", fields=(name,)),),
            time_column="source_time", value_column=name))
    reports = lake.items.initialize_many(["value", "other"], start="2024-01-01", end="2024-01-02",
        config=ExecutionOptions(observation_window_days=1))
    for report in reports.values():
        assert report.work["output_key_rows_encoded"] == 2
        assert report.work["output_key_cache_hits"] == 2
    for name in reports:
        actual = lake.items.read(name).collect().sort("time", "asset_id")
        expected = frame.sort("time", "asset_id")[name]
        assert actual["value"].equals(expected.rename("value"))
