from datetime import UTC, date, datetime
import hashlib
import sqlite3
import threading
import time
import zlib

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, ExecutionOptions, RawInput
from bagelquant_data.storage.recovery import read_batch, verify_batch


def fixture(root):
    lake = DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")
    lake.raw.ingest(DatasetSpec("values", "by_date", date_kind="calendar",
                               field_mappings={"time": "time", "asset_id": "asset_id"}),
                    pl.DataFrame({"time": [date(2020, month, 1) for month in (1, 2, 3, 4)],
                                  "asset_id": ["A"] * 4, "value": [1., 2., 3., 4.]}),
                    ingested_at=datetime(2020, 1, 2, tzinfo=UTC))
    receipt = lake.inputs.freeze({"values": RawInput("custom", "values", view="versions")},
                                 information_cutoff="2020-04-30")
    return lake, receipt


@pytest.mark.parametrize("buffer_bytes", [256, 1024 * 1024])
@pytest.mark.parametrize("damage", ["truncated", "bad_compression", "changed_bytes", "valid_hash_bad_ipc", "missing", "uncommitted"])
def test_streamed_verifier_rejects_original_damage_cases(tmp_path, damage, buffer_bytes):
    lake, receipt = fixture(tmp_path)
    batch = receipt.evidence["values"]["batches"][0]
    seq, path, digest = batch["commit_seq"], batch["partition_path"], batch["content_hash"]
    verify_batch(lake._data_meta, path, seq, digest, buffer_bytes=buffer_bytes)
    with sqlite3.connect(lake.data_meta_path) as db:
        payload = db.execute("select payload from version_batches where commit_seq=? and partition_path=?", (seq, path)).fetchone()[0]
        if damage == "missing":
            db.execute("delete from version_batches where commit_seq=? and partition_path=?", (seq, path))
        elif damage == "uncommitted":
            db.execute("update version_commits set status='prepared' where seq=?", (seq,))
        else:
            if damage == "truncated":
                payload = payload[:-1]
            elif damage == "bad_compression":
                payload = b"not compressed"
            elif damage == "changed_bytes":
                payload = zlib.compress(b"changed retained bytes")
            else:
                raw = b"valid SHA but invalid Arrow IPC" * 100
                payload = zlib.compress(raw)
                digest = hashlib.sha256(raw).hexdigest()
                db.execute("update version_batches set content_hash=? where commit_seq=? and partition_path=?", (digest, seq, path))
            db.execute("update version_batches set payload=? where commit_seq=? and partition_path=?", (payload, seq, path))
    with pytest.raises(Exception):
        read_batch(lake._data_meta, path, seq, digest)
    with pytest.raises(RuntimeError):
        verify_batch(lake._data_meta, path, seq, digest, buffer_bytes=buffer_bytes)


def test_parallel_verification_respects_inflight_and_rechecks_next_call(tmp_path, monkeypatch):
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
            time.sleep(.005)
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
        db.execute("update version_batches set payload=x'00' where commit_seq=?", (receipt.evidence["values"]["batches"][0]["commit_seq"],))
    with pytest.raises(RuntimeError):
        lake.inputs.verify(receipt, config=config)


def test_small_ipc_verification_avoids_temporary_file_and_mapping(tmp_path, monkeypatch):
    from bagelquant_data.storage import recovery
    lake, receipt = fixture(tmp_path)
    def disk_io(*args, **kwargs):
        pytest.fail("small IPC used temporary file or mmap")
    monkeypatch.setattr(recovery.tempfile, "TemporaryFile", disk_io)
    monkeypatch.setattr(recovery.mmap, "mmap", disk_io)
    assert lake.inputs.verify(receipt, config=ExecutionOptions(max_buffer_bytes=1024 * 1024))["valid"]


@pytest.mark.parametrize("large", [False, True])
def test_verification_spills_with_bounded_memory_and_closes_transport(tmp_path, monkeypatch, large):
    from bagelquant_data.storage import recovery
    lake, receipt = fixture(tmp_path)
    if large:
        frame = pl.DataFrame({"time": [date(2020, 5, 1)] * 10000,
                              "asset_id": [f"A{index}" for index in range(10000)],
                              "value": [float(index) for index in range(10000)]})
        spec = DatasetSpec("values", "by_date", date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"})
        lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 5, 2, tzinfo=UTC))
        receipt = lake.inputs.freeze({"values": RawInput("custom", "values", view="versions")})
    budget = 64 * 1024 if large else 256
    files, buffers = [], []
    original_file, original_buffer = recovery.tempfile.TemporaryFile, recovery.io.BytesIO
    def temporary(*args, **kwargs):
        result = original_file(*args, **kwargs)
        files.append(result)
        return result
    class TrackedBuffer(original_buffer):
        def __init__(self):
            super().__init__()
            self.peak = 0
            buffers.append(self)
        def write(self, value):
            result = super().write(value)
            self.peak = max(self.peak, self.tell())
            return result
    monkeypatch.setattr(recovery.tempfile, "TemporaryFile", temporary)
    from types import SimpleNamespace
    monkeypatch.setattr(recovery, "io", SimpleNamespace(BytesIO=TrackedBuffer))
    # This test covers original-byte transport. Full receipt verification also
    # audits selection summaries, which may need more than this tiny budget.
    for batch in receipt.evidence["values"]["batches"]:
        verify_batch(lake._data_meta, batch["partition_path"], batch["commit_seq"],
                     batch["content_hash"], buffer_bytes=budget)
    assert files and all(file.closed for file in files)
    assert buffers and all(buffer.closed for buffer in buffers)
    assert max(buffer.peak for buffer in buffers) <= budget // 4
