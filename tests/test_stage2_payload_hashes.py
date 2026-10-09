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


def _previous_typed_row_hashes(frame, fields):
    """Pre-optimization persisted typed-row-v1 reference."""
    import hashlib
    import json
    from bagelquant_data.pipeline.versions import _payload_expression, _payload_value
    selected = frame.select(fields)
    header = json.dumps([(name, str(dtype)) for name, dtype in selected.schema.items()],
                        separators=(",", ":")).encode()
    selected = selected.select(_payload_expression(pl.col(name), dtype).alias(name)
                               for name, dtype in selected.schema.items())
    prefix = hashlib.sha256(b"typed-row-v1\0" + header + b"\0")
    hashes = []
    for row in selected.iter_rows():
        digest = prefix.copy()
        digest.update(json.dumps([_payload_value(value) for value in row],
                                 sort_keys=True, separators=(",", ":")).encode())
        hashes.append(digest.hexdigest())
    return pl.Series("_payload_hash", hashes, dtype=pl.String)


def test_scalar_token_optimization_preserves_persisted_typed_bytes():
    from decimal import Decimal
    frame = pl.DataFrame({
        "text": ['重复', '重复', None, '"\\\n', 'Ω😀', '', 'long' * 1000],
        "float": [-0.0, 0.0, float("nan"), float("inf"), -float("inf"), None, 1.25],
        "flag": [True, False, None, True, False, True, None],
        "null": [None] * 7,
        "decimal": pl.Series([Decimal("1.20"), None, Decimal("-3.4"), Decimal("0"),
                              Decimal("1.2"), Decimal("99999.123"), Decimal("-0")],
                             dtype=pl.Decimal(20, 6)),
        "binary": [b"\x00", b"\xff", None, b"", b"abc", b"\n", b"abc"],
        "nested": [{"values": [1.0, None]}, {"values": [float("nan")]}, None,
                   {"values": []}, {"values": [-0.0]}, {"values": [float("inf")]},
                   {"values": [float("-inf")]}],
    })
    for dtype in (pl.Int8, pl.Int64, pl.UInt8, pl.UInt64, pl.Int128):
        values = [0, 1, None, 2, 3, 4, 5]
        if dtype == pl.Int128:
            values[-1] = 2**96
        frame = frame.with_columns(pl.Series(str(dtype), values, dtype=dtype))
    for dtype in (pl.Date, pl.Datetime("ns"), pl.Datetime("ns", "UTC"),
                  pl.Duration("ns"), pl.Time):
        frame = frame.with_columns(pl.Series(str(dtype), [0, 1, 2, None, 3, 4, 5],
                                             dtype=pl.Int64).cast(dtype))
    frame = frame.with_columns(pl.Series("array", [[1, 2], None, [0, 0], [3, 4],
                                                    [5, 6], [7, 8], [9, 10]],
                                         dtype=pl.Array(pl.Int64, 2)))
    for selected in (frame, frame.reverse(), frame.head(0)):
        fields = list(reversed(frame.columns))
        assert _payload_hashes(selected, fields).equals(_previous_typed_row_hashes(selected, fields))


def test_string_token_cache_is_bounded_and_keeps_large_or_uncached_values():
    import json
    from bagelquant_data.pipeline.versions import _payload_encoder
    reserve = [2048]
    encode = _payload_encoder(pl.String, reserve)
    for value in [None, '重复', '"\\\n', *[str(i) for i in range(200)], 'Ω' * 10000, '重复']:
        assert encode(value) == json.dumps(value, separators=(",", ":")).encode()
        assert 0 <= reserve[0] <= 2048


def test_old_hash_publication_and_frozen_receipt_survive_new_encoder(tmp_path, monkeypatch):
    from bagelquant_data import RawInput
    from bagelquant_data.pipeline import versions
    lake = _lake(tmp_path)
    frame = _frame(-0.0).with_columns(pl.lit('重复').alias('label'))
    optimized = versions._payload_hashes
    monkeypatch.setattr(versions, "_payload_hashes", _previous_typed_row_hashes)
    lake.raw.ingest(_spec(), frame, ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    frozen = lake.inputs.freeze({"raw": RawInput("custom", "values", view="versions")},
                               information_cutoff=date(2020, 1, 1))
    commit = lake.inputs.max_commit()
    monkeypatch.setattr(versions, "_payload_hashes", optimized)
    lake.raw.ingest(_spec(), frame, ingested_at=datetime(2020, 1, 2, tzinfo=UTC))
    assert lake.inputs.max_commit() == commit
    lake.inputs.verify(frozen)
    assert lake.raw.read_versions("values", source="custom").collect().height == 1
