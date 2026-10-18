"""Empty byte audits retain validation without deriving discarded primitive IDs."""

from datetime import date, datetime, time, timedelta
from decimal import Decimal
import hashlib
import json
from typing import cast

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from bagelquant_data import input_index
from bagelquant_data.pipeline import versions
from bagelquant_data.storage import recovery
from bagelquant_data.storage.data_meta import DataMetaStore
from test_input_read_context import frozen_months


UNUSED_META = cast(DataMetaStore, object())  # _registered is the isolated fixture boundary


def segment(tmp_path, monkeypatch, frame, keys=None):
    """Real original Parquet with isolated registered-header authority."""
    path = tmp_path / "original.parquet"
    frame.write_parquet(path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    row = {"schema_ipc": frame.to_arrow().schema.serialize().to_pybytes(),
           "row_count": frame.height,
           "bounds_json": json.dumps({**input_index.bounds(frame),
                                       "keys": ["source_time", "asset_id"] if keys is None else keys}),
           "field_changes_json": json.dumps({"$initial": {"identity": digest}})}
    monkeypatch.setattr(recovery, "_registered", lambda *args: (row, path))
    return row, path, digest


def panel(extra):
    count = len(extra)
    return pl.DataFrame({"source_time": [date(2020, 1, 1)] * count,
                         "time": [date(2020, 1, 2)] * count,
                         "asset_id": ["A"] * count}).with_columns(extra.rename("extra"))


def audit(digest, **kwargs):
    recovery.verify_batch(UNUSED_META, "2020/01", 1, digest, buffer_bytes=512, **kwargs)


def reference(monkeypatch, function):
    # Disallow every normalized dtype: execute the unchanged with_record_key
    # implementation, including its complete original typed-row encoder.
    with monkeypatch.context() as patch:
        patch.setattr(recovery, "_primitive_audit_key", lambda dtype: False)
        return function()


@pytest.mark.parametrize("extra", [
    pl.Series([None, None], dtype=pl.Null),
    pl.Series([True, None], dtype=pl.Boolean),
    pl.Series(["\u0000é中", None], dtype=pl.String),
    pl.Series([b"\x00\xff", None], dtype=pl.Binary),
    *[pl.Series([-(2**(bits-1)), 2**(bits-1)-1, None], dtype=dtype)
      for bits, dtype in ((8, pl.Int8), (16, pl.Int16), (32, pl.Int32), (64, pl.Int64))],
    *[pl.Series([0, 2**bits-1, None], dtype=dtype)
      for bits, dtype in ((8, pl.UInt8), (16, pl.UInt16), (32, pl.UInt32), (64, pl.UInt64))],
    *[pl.Series([0., -0., float("nan"), float("inf"), -float("inf"), None], dtype=dtype)
      for dtype in (pl.Float32, pl.Float64)],
    pl.Series([date.min, None], dtype=pl.Date),
    pl.Series([datetime(2020, 1, 1), None], dtype=pl.Datetime("ns")),
    pl.Series([time(23, 59, 59), None], dtype=pl.Time),
    pl.Series([timedelta(microseconds=-1), None], dtype=pl.Duration("ns")),
], ids=lambda value: str(value.dtype))
def test_primitive_empty_audit_matches_original_and_does_no_row_hashing(tmp_path, monkeypatch, extra):
    _, path, digest = segment(tmp_path, monkeypatch, panel(extra), ["source_time", "asset_id", "extra"])
    original_bytes = path.read_bytes()
    original_hash = versions._payload_hashes
    hashed = []
    def tracked(frame, fields):
        hashed.append(frame.height)
        return original_hash(frame, fields)
    monkeypatch.setattr(versions, "_payload_hashes", tracked)
    reference(monkeypatch, lambda: audit(digest))
    assert sum(hashed) == len(extra)
    hashed.clear()
    audit(digest)
    assert hashed == []
    assert path.read_bytes() == original_bytes


@pytest.mark.parametrize("extra", [
    pl.Series([Decimal("1.20"), None], dtype=pl.Decimal(8, 2)),
    pl.Series(["A", None], dtype=pl.Categorical),
    pl.Series(["A", None], dtype=pl.Enum(["A", "B"])),
    pl.Series([[date(2020, 1, 1)], None], dtype=pl.List(pl.Date)),
    pl.Series([[1, 2], [3, 4]], dtype=pl.Array(pl.Int64, 2)),
    pl.Series([{"x": date(2020, 1, 1)}, None], dtype=pl.Struct({"x": pl.Date})),
], ids=lambda value: str(value.dtype))
def test_complex_empty_audit_retains_original_encoder(tmp_path, monkeypatch, extra):
    _, _, digest = segment(tmp_path, monkeypatch, panel(extra), ["source_time", "extra"])
    original = versions._payload_hashes
    hashes = []
    def tracked(frame, fields):
        result = original(frame, fields)
        hashes.extend(result.to_list())
        return result
    monkeypatch.setattr(versions, "_payload_hashes", tracked)
    audit(digest)
    actual = list(hashes)
    hashes.clear()
    reference(monkeypatch, lambda: audit(digest))
    assert actual == hashes and len(actual) == len(extra)


@pytest.mark.parametrize("columns", [("_record_id",), ("_payload_hash",), ("time", "source_time"), ("absent",)])
def test_nonempty_projection_keeps_exact_original_hashes(tmp_path, monkeypatch, columns):
    frame = panel(pl.Series([0., -0., float("nan"), None]))
    _, _, digest = segment(tmp_path, monkeypatch, frame)
    original = versions._payload_hashes
    calls = []
    def tracked(*args):
        calls.append(1)
        return original(*args)
    monkeypatch.setattr(versions, "_payload_hashes", tracked)
    def run():
        pieces = []
        recovery.project_batch(UNUSED_META, "p", 1, digest, columns=columns,
                               consume=pieces.append, buffer_bytes=512)
        return pl.concat(pieces) if pieces else pl.DataFrame()
    expected = reference(monkeypatch, run)
    calls.clear()
    assert_frame_equal(run(), expected)
    assert len(calls) == (4 if "_payload_hash" in columns else 2)
    if columns == ("_record_id",):
        assert expected["_record_id"].to_list() == recovery.with_record_key(frame)["_record_id"].to_list()


@pytest.mark.parametrize("keys", [None, [], ["source_time", "source_time"], ["missing"],
                                  1, [1], "source_time", {"x": 1},
                                  ["^does_not_exist$"], ["^source_.*$"], ["*"]])
def test_key_metadata_validation_matches_original(tmp_path, monkeypatch, keys):
    row, _, digest = segment(tmp_path, monkeypatch, panel(pl.Series([1, 1])), keys)
    bounds = json.loads(row["bounds_json"])
    bounds["keys"] = keys
    row["bounds_json"] = json.dumps(bounds)
    def outcome():
        try:
            audit(digest)
        except Exception as error:
            return type(error), str(error)
        return None
    assert outcome() == reference(monkeypatch, outcome)
    if keys == []:
        assert outcome() is None  # duplicate key values are permitted


@pytest.mark.parametrize("kind", ["general", "physical_id", "empty"])
def test_original_early_returns_and_empty_files_ignore_invalid_keys(tmp_path, monkeypatch, kind):
    frame = panel(pl.Series([1]))
    if kind == "general":
        frame = frame.drop("source_time")
    elif kind == "physical_id":
        frame = frame.with_columns(pl.lit("retained").alias("_record_id"))
    else:
        frame = frame.head(0)
    _, _, digest = segment(tmp_path, monkeypatch, frame, ["missing", "missing"])
    audit(digest)
    reference(monkeypatch, lambda: audit(digest))


@pytest.mark.parametrize("damage", ["bytes", "schema", "count", "bounds", "field_change", "field_projection"])
def test_empty_audit_preserves_original_corruption_errors(tmp_path, monkeypatch, damage):
    row, path, digest = segment(tmp_path, monkeypatch, panel(pl.Series([1, 2, 3])))
    if damage == "bytes":
        path.write_bytes(b"damaged")
    elif damage == "schema":
        row["schema_ipc"] = pl.DataFrame({"wrong": [1]}).to_arrow().schema.serialize().to_pybytes()
    elif damage == "count":
        row["row_count"] += 1
    elif damage == "bounds":
        bounds = json.loads(row["bounds_json"])
        bounds["lower"] = "1999-01-01"
        row["bounds_json"] = json.dumps(bounds)
    else:
        row["field_changes_json"] = json.dumps({"extra": {"identity": "bad",
            "projection": ["extra"] if damage == "field_projection" else None}})
    def error():
        with pytest.raises((RuntimeError, MemoryError)) as caught:
            audit(digest)
        return type(caught.value), str(caught.value)
    assert error() == reference(monkeypatch, error)


def test_arrow_full_validation_and_chunk_cancellation_remain(tmp_path, monkeypatch):
    _, _, digest = segment(tmp_path, monkeypatch, panel(pl.Series(range(9))))
    real_reader = recovery.pq.ParquetFile
    validations = []
    class InvalidBatch:
        def validate(self, *, full):
            validations.append(full)
            raise ValueError("malformed Arrow body")
    class Reader:
        def __init__(self, *args):
            self.reader = real_reader(*args)
            self.schema_arrow, self.metadata = self.reader.schema_arrow, self.reader.metadata
        def __enter__(self):
            return self
        def __exit__(self, *args):
            self.reader.close()
        def iter_batches(self, **kwargs):
            yield InvalidBatch()
    with monkeypatch.context() as patch:
        patch.setattr(recovery.pq, "ParquetFile", Reader)
        with pytest.raises(ValueError, match="malformed Arrow body"):
            audit(digest)
        reference(monkeypatch, lambda: pytest.raises(ValueError, audit, digest))
    assert validations == [True, True]
    class Canceled(Exception):
        pass
    def run():
        visits = []
        def cancel():
            visits.append(1)
            if len(visits) == 4:
                raise Canceled()
        with pytest.raises(Canceled):
            audit(digest, check_canceled=cancel)
        return visits
    assert run() == reference(monkeypatch, run) == [1] * 4


@pytest.mark.parametrize("buffer_bytes", [512, 16384])
def test_general_field_projection_preserves_typed_hash_and_buffer_limit(tmp_path, monkeypatch, buffer_bytes):
    from bagelquant_data.core.hashing import frame_content_hash
    frame = panel(pl.Series([0., -0., float("nan"), None] * 8)).drop("source_time")
    row, _, digest = segment(tmp_path, monkeypatch, frame)
    projected = frame.select("time", "asset_id", "extra")
    row["field_changes_json"] = json.dumps({"extra": {"identity": frame_content_hash(projected),
                                                     "projection": projected.columns}})
    def result():
        try:
            recovery.verify_batch(UNUSED_META, "p", 1, digest, buffer_bytes=buffer_bytes)
        except MemoryError as error:
            return type(error), str(error)
        return None
    expected = reference(monkeypatch, result)
    assert result() == expected
    assert (expected is None) == (buffer_bytes == 16384)


def test_complex_encoder_failure_is_not_hidden(tmp_path, monkeypatch):
    _, _, digest = segment(tmp_path, monkeypatch, panel(pl.Series([[1], [2]])), ["extra"])
    def unsupported(*args):
        raise TypeError("Unsupported persisted payload scalar: sentinel")
    monkeypatch.setattr(versions, "_payload_hashes", unsupported)
    with pytest.raises(TypeError, match="Unsupported persisted payload scalar: sentinel"):
        audit(digest)


@pytest.mark.parametrize("columns", [[], None])
def test_non_tuple_falsy_projection_keeps_original_key_path(tmp_path, monkeypatch, columns):
    _, _, digest = segment(tmp_path, monkeypatch, panel(pl.Series([1])))
    original = versions._payload_hashes
    calls = []
    def tracked(*args):
        calls.append(1)
        return original(*args)
    monkeypatch.setattr(versions, "_payload_hashes", tracked)
    def run():
        try:
            recovery.project_batch(UNUSED_META, "p", 1, digest, columns=columns,
                                   consume=lambda _: None, buffer_bytes=512)
        except TypeError as error:
            return type(error), str(error)
        return None
    expected = reference(monkeypatch, run)
    calls.clear()
    assert run() == expected
    assert calls == [1]


def test_regex_named_field_normalization_preserves_original_shape_error(tmp_path, monkeypatch):
    frame = panel(pl.Series([1, 2])).rename({"extra": "^ghost$"})
    _, _, digest = segment(tmp_path, monkeypatch, frame, [r"^\^ghost\$$"])
    def result():
        try:
            audit(digest)
        except pl.exceptions.PolarsError as error:
            return type(error), str(error)
        return None
    expected = reference(monkeypatch, result)
    assert expected is not None
    assert result() == expected


def test_public_audit_result_progress_and_original_identities_match_reference(tmp_path, monkeypatch):
    lake, frozen = frozen_months(tmp_path)
    before = {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
              for path in lake.lake_path.rglob("*.parquet")}
    progress = []
    expected = reference(monkeypatch, lambda: lake.inputs.verify(frozen, progress=progress.append))
    actual_progress = []
    assert lake.inputs.verify(frozen, progress=actual_progress.append) == expected
    assert actual_progress == progress
    assert lake.inputs.get(frozen).digest == frozen.digest
    assert {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in lake.lake_path.rglob("*.parquet")} == before
