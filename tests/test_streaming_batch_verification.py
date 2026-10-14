"""Explicit bounded audits inspect the sole retained Parquet originals."""

from datetime import UTC, date, datetime
import hashlib
import sqlite3
import threading
import time
import polars as pl
import pytest
from bagelquant_data import DataLake, DatasetSpec, ExecutionOptions, RawInput
from bagelquant_data.storage.recovery import read_batch, verify_batch, project_batch
from storage_fixtures import original_segments


def fixture(root):
    lake = DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")
    lake.raw.ingest(
        DatasetSpec(
            "values",
            "by_date",
            date_kind="calendar",
            field_mappings={"time": "time", "asset_id": "asset_id"},
        ),
        pl.DataFrame(
            {
                "time": [date(2020, month, 1) for month in (1, 2, 3, 4)],
                "asset_id": ["A"] * 4,
                "value": [1.0, 2.0, 3.0, 4.0],
            }
        ),
        ingested_at=datetime(2020, 1, 2, tzinfo=UTC),
    )
    receipt = lake.inputs.freeze(
        {"values": RawInput("custom", "values", view="versions")},
        information_cutoff="2020-04-30",
    )
    return lake, receipt


@pytest.mark.parametrize("buffer_bytes", [256, 1024 * 1024])
@pytest.mark.parametrize(
    "damage",
    ["truncated", "changed_bytes", "valid_hash_bad_parquet", "missing", "uncommitted"],
)
def test_streamed_verifier_rejects_original_damage_cases(
    tmp_path, damage, buffer_bytes
):
    lake, receipt = fixture(tmp_path)
    batch = receipt.evidence["values"]["batches"][0]
    seq, partition, digest = (
        batch["commit_seq"],
        batch["partition_path"],
        batch["content_hash"],
    )
    verify_batch(lake._data_meta, partition, seq, digest, buffer_bytes=buffer_bytes)
    with sqlite3.connect(lake.data_meta_path) as db:
        (path,) = original_segments(
            db, "s.commit_seq=? and s.partition_path=?", (seq, partition)
        )
        if damage == "missing":
            path.unlink()
        elif damage == "uncommitted":
            db.execute(
                "update version_commits set status='prepared' where seq=?", (seq,)
            )
        else:
            payload = (
                path.read_bytes()[:-1]
                if damage == "truncated"
                else b"invalid original Parquet"
            )
            path.write_bytes(payload)
            if damage == "valid_hash_bad_parquet":
                digest = hashlib.sha256(payload).hexdigest()
                db.execute(
                    "update version_segments set content_hash=? where commit_seq=? and partition_path=?",
                    (digest, seq, partition),
                )
    with pytest.raises(Exception):
        read_batch(lake._data_meta, partition, seq, digest, verify=True)
    with pytest.raises(Exception):
        verify_batch(lake._data_meta, partition, seq, digest, buffer_bytes=buffer_bytes)


def test_parallel_verification_respects_inflight_and_rechecks_next_call(
    tmp_path, monkeypatch
):
    from bagelquant_data import inputs

    lake, receipt = fixture(tmp_path)
    original = inputs.verify_batch
    active, peak = 0, 0
    lock = threading.Lock()
    seen = []

    def tracked(*args, **kwargs):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            seen.append((args[2], args[1], kwargs["buffer_bytes"]))
        try:
            time.sleep(0.005)
            return original(*args, **kwargs)
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(inputs, "verify_batch", tracked)
    config = ExecutionOptions(workers=8, max_in_flight=2, max_buffer_bytes=4 * 1024**2)
    expected = lake.inputs.verify(receipt, config=config)
    assert expected["batch_count"] == len(seen) == 4 and peak == 2
    assert all(value[2] == config.max_buffer_bytes // 2 for value in seen)
    seen.clear()
    assert lake.inputs.verify(receipt, config=config) == expected and len(seen) == 4
    with sqlite3.connect(lake.data_meta_path) as db:
        path = original_segments(
            db,
            "s.commit_seq=?",
            (receipt.evidence["values"]["batches"][0]["commit_seq"],),
        )[0]
        path.write_bytes(b"corrupt")
    with pytest.raises(RuntimeError):
        lake.inputs.verify(receipt, config=config)


def test_original_projection_is_bounded_and_cancellation_stops_next_chunk(tmp_path):
    lake, receipt = fixture(tmp_path)
    spec = lake.raw.get("values", source="custom")
    rows = pl.DataFrame(
        {
            "time": [date(2020, 5, 1)] * 1000,
            "asset_id": [f"A{i}" for i in range(1000)],
            "value": [float(i) for i in range(1000)],
        }
    )
    lake.raw.ingest(spec, rows, ingested_at=datetime(2020, 5, 2, tzinfo=UTC))
    receipt = lake.inputs.freeze(
        {"values": RawInput("custom", "values", view="versions")}
    )
    batch = receipt.evidence["values"]["batches"][-1]
    chunks = []
    project_batch(
        lake._data_meta,
        batch["partition_path"],
        batch["commit_seq"],
        batch["content_hash"],
        columns=("source_time", "asset_id", "value"),
        consume=lambda frame: chunks.append(frame.height),
        buffer_bytes=256,
    )
    assert sum(chunks) == 1000 and max(chunks) == 1
    chunks.clear()

    def canceled():
        if chunks:
            raise RuntimeError("requested cancellation")

    with pytest.raises(RuntimeError, match="requested cancellation"):
        project_batch(
            lake._data_meta,
            batch["partition_path"],
            batch["commit_seq"],
            batch["content_hash"],
            columns=("value",),
            consume=lambda frame: chunks.append(frame.height),
            buffer_bytes=256,
            check_canceled=canceled,
        )
    assert chunks == [1]
    assert lake.inputs.verify(receipt)["valid"]
