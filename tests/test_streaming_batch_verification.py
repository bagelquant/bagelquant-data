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


@pytest.mark.parametrize("damage", ["truncated", "bad_compression", "changed_bytes", "valid_hash_bad_ipc", "missing", "uncommitted"])
def test_streamed_verifier_rejects_original_damage_cases(tmp_path, damage):
    lake, receipt = fixture(tmp_path)
    batch = receipt.evidence["values"]["batches"][0]
    seq, path, digest = batch["commit_seq"], batch["partition_path"], batch["content_hash"]
    verify_batch(lake._data_meta, path, seq, digest, buffer_bytes=256)
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
                raw = b"valid SHA but invalid Arrow IPC"
                payload = zlib.compress(raw)
                digest = hashlib.sha256(raw).hexdigest()
                db.execute("update version_batches set content_hash=? where commit_seq=? and partition_path=?", (digest, seq, path))
            db.execute("update version_batches set payload=? where commit_seq=? and partition_path=?", (payload, seq, path))
    with pytest.raises(Exception):
        read_batch(lake._data_meta, path, seq, digest)
    with pytest.raises(RuntimeError):
        verify_batch(lake._data_meta, path, seq, digest, buffer_bytes=256)


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
