"""Only captured-bound audits may omit discarded primitive date-projection IDs."""

from contextlib import contextmanager
from datetime import date, datetime, time, timedelta
from decimal import Decimal
import hashlib
import inspect
import json

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from bagelquant_data import input_index
from bagelquant_data.pipeline import versions
from bagelquant_data.storage import recovery
from test_audit_projection_keys import UNUSED_META, panel, segment
from test_input_read_context import frozen_months


@contextmanager
def ordinary_bounds(monkeypatch):
    """Reference uses the unchanged ordinary date-projection entry and engine."""

    def original(
        store, partition, seq, digest, *, consume, buffer_bytes, check_canceled=None
    ):
        return recovery.project_batch(
            store,
            partition,
            seq,
            digest,
            columns=("time", "source_time"),
            consume=consume,
            buffer_bytes=buffer_bytes,
            check_canceled=check_canceled,
        )

    with monkeypatch.context() as patch:
        patch.setattr(recovery, "_project_audit_bounds", original)
        yield


def project(digest, *, private, buffer_bytes=512, check_canceled=None):
    pieces = []
    if private:
        recovery._project_audit_bounds(
            UNUSED_META,
            "p",
            1,
            digest,
            consume=pieces.append,
            buffer_bytes=buffer_bytes,
            check_canceled=check_canceled,
        )
    else:
        recovery.project_batch(
            UNUSED_META,
            "p",
            1,
            digest,
            columns=("time", "source_time"),
            consume=pieces.append,
            buffer_bytes=buffer_bytes,
            check_canceled=check_canceled,
        )
    return pl.concat(pieces) if pieces else pl.DataFrame()


def captured(frame, digest):
    return {
        "commit_seq": 1,
        "partition_path": "p",
        "content_hash": digest,
        **{
            key: None
            if axis not in frame.columns
            or frame.is_empty()
            or getattr(frame[axis], op)() is None
            else str(getattr(frame[axis], op)())
            for key, axis, op in (
                ("min_available", "time", "min"),
                ("max_available", "time", "max"),
                ("min_observation", "source_time", "min"),
                ("max_observation", "source_time", "max"),
            )
        },
    }


SCALARS = [
    pl.Series([None, None], dtype=pl.Null),
    pl.Series([True, None], dtype=pl.Boolean),
    pl.Series(["\u0000é中", None], dtype=pl.String),
    pl.Series([b"\x00\xff", None], dtype=pl.Binary),
    *[
        pl.Series([-(2 ** (bits - 1)), 2 ** (bits - 1) - 1, None], dtype=dtype)
        for bits, dtype in (
            (8, pl.Int8),
            (16, pl.Int16),
            (32, pl.Int32),
            (64, pl.Int64),
        )
    ],
    *[
        pl.Series([0, 2**bits - 1, None], dtype=dtype)
        for bits, dtype in (
            (8, pl.UInt8),
            (16, pl.UInt16),
            (32, pl.UInt32),
            (64, pl.UInt64),
        )
    ],
    *[
        pl.Series(
            [0.0, -0.0, float("nan"), float("inf"), -float("inf"), None], dtype=dtype
        )
        for dtype in (pl.Float32, pl.Float64)
    ],
    pl.Series([date.min, None], dtype=pl.Date),
    pl.Series([datetime(2020, 1, 1), None], dtype=pl.Datetime("ns")),
    pl.Series([time(23, 59, 59), None], dtype=pl.Time),
    pl.Series([timedelta(microseconds=-1), None], dtype=pl.Duration("ns")),
]


@pytest.mark.parametrize("extra", SCALARS, ids=lambda value: str(value.dtype))
def test_private_date_projection_matches_ordinary_without_key_hashes(
    tmp_path, monkeypatch, extra
):
    frame = panel(extra)
    _, path, digest = segment(
        tmp_path, monkeypatch, frame, ["source_time", "asset_id", "extra"]
    )
    original = versions._payload_hashes
    hashed = []

    def tracked(value, fields):
        hashed.append(value.height)
        return original(value, fields)

    monkeypatch.setattr(versions, "_payload_hashes", tracked)
    before = path.read_bytes()
    expected = project(digest, private=False)
    assert sum(hashed) == frame.height
    hashed.clear()
    assert_frame_equal(project(digest, private=True), expected)
    input_index.audit_captured_bounds(
        UNUSED_META, captured(frame, digest), max_bytes=1024
    )
    assert hashed == []
    assert_frame_equal(expected, frame.select("time", "source_time"))
    assert path.read_bytes() == before


@pytest.mark.parametrize(
    "keys",
    [
        None,
        [],
        ["missing"],
        ["source_time", "source_time"],
        1,
        [1],
        {},
        "source_time",
        ["^does_not_exist$"],
        ["^source_.*$"],
        ["*"],
        "absent_keys",
    ],
)
def test_date_audit_preserves_exact_key_metadata_results_and_errors(
    tmp_path, monkeypatch, keys
):
    row, _, digest = segment(tmp_path, monkeypatch, panel(pl.Series([1, 1, 1])))
    bounds = json.loads(row["bounds_json"])
    if keys == "absent_keys":
        del bounds["keys"]
    else:
        bounds["keys"] = keys
    row["bounds_json"] = json.dumps(bounds)

    def result(private):
        try:
            return project(digest, private=private).to_dict(as_series=False)
        except Exception as error:
            return type(error), str(error)

    assert result(True) == result(False)


def test_regex_named_key_retains_normalization_shape_failure(tmp_path, monkeypatch):
    frame = panel(pl.Series([1, 2])).rename({"extra": "^ghost$"})
    _, _, digest = segment(tmp_path, monkeypatch, frame, [r"^\^ghost\$$"])
    errors = []
    for private in (False, True):
        with pytest.raises(pl.exceptions.ShapeError) as caught:
            project(digest, private=private)
        errors.append((type(caught.value), str(caught.value)))
    assert errors[0] == errors[1]


COMPLEX = [
    pl.Series([Decimal("1.20"), None], dtype=pl.Decimal(8, 2)),
    pl.Series(["A", None], dtype=pl.Categorical),
    pl.Series(["A", None], dtype=pl.Enum(["A", "B"])),
    pl.Series([[date(2020, 1, 1)], None], dtype=pl.List(pl.Date)),
    pl.Series([[1, 2], [3, 4]], dtype=pl.Array(pl.Int64, 2)),
    pl.Series([{"x": date(2020, 1, 1)}, None], dtype=pl.Struct({"x": pl.Date})),
]


@pytest.mark.parametrize("key_extra", [False, True])
@pytest.mark.parametrize("extra", COMPLEX, ids=lambda value: str(value.dtype))
def test_complex_full_payload_falls_back_even_outside_keys(
    tmp_path, monkeypatch, key_extra, extra
):
    _, _, digest = segment(
        tmp_path,
        monkeypatch,
        panel(extra),
        ["source_time", "extra"] if key_extra else ["source_time", "asset_id"],
    )
    original = versions._payload_hashes
    hashes = []

    def tracked(*args):
        value = original(*args)
        hashes.extend(value.to_list())
        return value

    monkeypatch.setattr(versions, "_payload_hashes", tracked)
    expected = project(digest, private=False)
    expected_hashes = list(hashes)
    hashes.clear()
    assert_frame_equal(project(digest, private=True), expected)
    assert hashes == expected_hashes and len(hashes) == len(extra)

    def unsupported(*args):
        raise TypeError("Unsupported persisted payload scalar: sentinel")

    monkeypatch.setattr(versions, "_payload_hashes", unsupported)
    with pytest.raises(
        TypeError, match="Unsupported persisted payload scalar: sentinel"
    ):
        project(digest, private=True)


@pytest.mark.parametrize(
    "kind", ["missing_time", "missing_source_time", "physical_id", "empty"]
)
def test_date_audit_retains_early_and_missing_axis_behavior(
    tmp_path, monkeypatch, kind
):
    frame = panel(pl.Series([1, 2]))
    if kind == "missing_time":
        frame = frame.drop("time")
    elif kind == "missing_source_time":
        frame = frame.drop("source_time")
    elif kind == "physical_id":
        frame = frame.with_columns(pl.lit("original-id").alias("_record_id"))
    else:
        frame = frame.head(0)
    _, _, digest = segment(tmp_path, monkeypatch, frame, ["missing", "missing"])

    def result(private):
        try:
            return project(digest, private=private).to_dict(as_series=False)
        except Exception as error:
            return type(error), str(error)

    assert result(True) == result(False)


@pytest.mark.parametrize("buffer_bytes", [0, 1, 128, 512, 1024 * 1024])
def test_date_projection_chunks_and_buffer_behavior_remain_exact(
    tmp_path, monkeypatch, buffer_bytes
):
    frame = panel(pl.Series(range(17)))
    _, _, digest = segment(tmp_path, monkeypatch, frame)
    assert_frame_equal(
        project(digest, private=True, buffer_bytes=buffer_bytes),
        project(digest, private=False, buffer_bytes=buffer_bytes),
    )


@pytest.mark.parametrize(
    "damage",
    [
        "bytes",
        "missing_file",
        "schema",
        "count",
        "registered_bounds",
        "captured_bounds",
        "json",
    ],
)
def test_private_date_audit_preserves_all_original_and_bound_errors(
    tmp_path, monkeypatch, damage
):
    frame = panel(pl.Series([1, 2, 3]))
    row, path, digest = segment(tmp_path, monkeypatch, frame)
    proof = captured(frame, digest)
    if damage == "bytes":
        path.write_bytes(b"damaged")
    elif damage == "missing_file":
        path.unlink()
    elif damage == "schema":
        row["schema_ipc"] = (
            pl.DataFrame({"wrong": [1]}).to_arrow().schema.serialize().to_pybytes()
        )
    elif damage == "count":
        row["row_count"] += 1
    elif damage == "registered_bounds":
        bounds = json.loads(row["bounds_json"])
        bounds["lower"] = "1999-01-01"
        row["bounds_json"] = json.dumps(bounds)
    elif damage == "captured_bounds":
        proof["min_available"] = "1999-01-01"
    else:
        row["bounds_json"] = "{"

    def result():
        try:
            input_index.audit_captured_bounds(UNUSED_META, proof, max_bytes=1024)
        except Exception as error:
            return type(error), str(error)
        pytest.fail("damaged original was accepted")

    actual = result()
    with ordinary_bounds(monkeypatch):
        assert result() == actual


def test_private_date_audit_keeps_full_arrow_validation_and_cancellation(
    tmp_path, monkeypatch
):
    _, _, digest = segment(tmp_path, monkeypatch, panel(pl.Series(range(9))))
    actual_reader = recovery.pq.ParquetFile
    seen = []

    class Batch:
        def validate(self, *, full):
            seen.append(full)
            raise ValueError("malformed Arrow body")

    class Reader:
        def __init__(self, *args):
            self.reader = actual_reader(*args)
            self.schema_arrow, self.metadata = (
                self.reader.schema_arrow,
                self.reader.metadata,
            )

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.reader.close()

        def iter_batches(self, **kwargs):
            yield Batch()

    with monkeypatch.context() as patch:
        patch.setattr(recovery.pq, "ParquetFile", Reader)
        for private in (False, True):
            with pytest.raises(ValueError, match="malformed Arrow body"):
                project(digest, private=private)
    assert seen == [True, True]

    class Canceled(Exception):
        pass

    def result(private):
        visits = []

        def cancel():
            visits.append(1)
            if len(visits) == 4:
                raise Canceled()

        with pytest.raises(Canceled):
            project(digest, private=private, check_canceled=cancel)
        return visits

    assert result(True) == result(False) == [1] * 4


def test_ordinary_projection_signature_and_record_identity_stay_unchanged(
    tmp_path, monkeypatch
):
    assert tuple(inspect.signature(recovery.project_batch).parameters) == (
        "data_meta",
        "partition",
        "seq",
        "digest",
        "columns",
        "consume",
        "buffer_bytes",
        "check_canceled",
    )
    frame = panel(pl.Series([0.0, -0.0, float("nan"), None]))
    _, _, digest = segment(tmp_path, monkeypatch, frame)
    original = versions._payload_hashes
    calls = []

    def tracked(*args):
        calls.append(1)
        return original(*args)

    monkeypatch.setattr(versions, "_payload_hashes", tracked)
    pieces = []
    recovery.project_batch(
        UNUSED_META,
        "p",
        1,
        digest,
        columns=("time", "source_time", "_record_id"),
        consume=pieces.append,
        buffer_bytes=512,
    )
    assert calls == [1, 1]
    assert_frame_equal(
        pl.concat(pieces),
        recovery.with_record_key(frame).select("time", "source_time", "_record_id"),
    )
    with pytest.raises(TypeError, match="_audit_bounds"):
        inspect.signature(recovery.project_batch).bind(
            UNUSED_META,
            "p",
            1,
            digest,
            columns=("time", "source_time"),
            consume=lambda _: None,
            buffer_bytes=512,
            _audit_bounds=True,
        )


def test_public_full_audit_matches_reference_progress_identity_and_fresh_byte_checks(
    tmp_path, monkeypatch
):
    lake, frozen = frozen_months(tmp_path)
    originals = {
        str(path): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in lake.lake_path.rglob("*.parquet")
    }
    expected_progress = []
    with ordinary_bounds(monkeypatch):
        expected = lake.inputs.verify(frozen, progress=expected_progress.append)
    progress = []
    assert lake.inputs.verify(frozen, progress=progress.append) == expected
    assert progress == expected_progress
    assert lake.inputs.get(frozen).digest == frozen.digest
    assert {
        str(path): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in lake.lake_path.rglob("*.parquet")
    } == originals
    next(lake.lake_path.rglob("*.parquet")).write_bytes(b"later damage")
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        lake.inputs.verify(frozen)
