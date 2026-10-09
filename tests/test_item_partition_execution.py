"""Public builds reuse bounded writers and retain atomic recovery evidence."""
from datetime import UTC, date, datetime
import sqlite3
from threading import Barrier, Lock, get_ident

import polars as pl
import pytest

from bagelquant_data import DataLake, DataItemSpec, DatasetSpec, ExecutionOptions, ItemInput, RawInput
from bagelquant_data.pipeline import versions
from bagelquant_data.storage import recovery


def fixture_lake(root):
    lake = DataLake.open(data_meta_path=root / "data.sqlite", lake_path=root / "lake")
    spec = DatasetSpec("prices", "by_date", date_kind="calendar",
                       field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2020, month, 1) for month in range(1, 5)],
                          "asset_id": ["A"] * 4, "value": [1.0, 2.0, 3.0, 4.0]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2020, 5, 1, tzinfo=UTC))
    lake.items.register(DataItemSpec("derived", (RawInput("custom", "prices"),),
                                    time_column="source_time"))
    return lake


def test_parallel_month_writes_preserve_serial_values_and_frozen_recovery(tmp_path, monkeypatch):
    serial = fixture_lake(tmp_path / "serial")
    parallel = fixture_lake(tmp_path / "parallel")
    serial.items.initialize("derived", start="2020-01-01", end="2020-04-30")
    original = versions._write_version_partition
    gate = Barrier(3, timeout=10)
    lock = Lock()
    threads = set()
    started = 0

    def write(*args):
        nonlocal started
        with lock:
            started += 1
            number = started
            threads.add(get_ident())
        if number <= 3:
            gate.wait()
        return original(*args)

    monkeypatch.setattr(versions, "_write_version_partition", write)
    report = parallel.items.initialize("derived", start="2020-01-01", end="2020-04-30",
                                       config=ExecutionOptions(workers=3, max_in_flight=3))
    assert report.status == "success" and report.rows_committed == 4
    assert len(threads) == 3
    for view in ("history", "snapshot"):
        cutoff = "2020-04-30" if view == "snapshot" else None
        assert parallel.items.read("derived", view=view, as_of=cutoff).collect().equals(
            serial.items.read("derived", view=view, as_of=cutoff).collect())
    assert parallel.items.read("derived", strict=True).collect().is_empty()
    receipt = parallel.inputs.freeze({"derived": ItemInput("derived")}, information_cutoff="2020-04-30")
    assert parallel.inputs.verify(receipt)
    for part in parallel.items.manifest("derived"):
        state = recovery.inspect_partition(parallel._parquet, "items", "derived", part["partition_path"], deep=True)
        assert state["recovery_state"] == "ready", state


def test_parallel_writer_failure_drains_before_rollback(tmp_path, monkeypatch):
    lake = fixture_lake(tmp_path)
    original = versions._write_version_partition
    gate = Barrier(3, timeout=10)
    lock = Lock()
    settled = []

    def write(*args):
        partition = args[4]
        if "month=04" not in partition:
            gate.wait()
        if "month=02" in partition:
            with lock:
                settled.append(partition)
            raise RuntimeError("fixture writer failure")
        result = original(*args)
        with lock:
            settled.append(partition)
        return result

    monkeypatch.setattr(versions, "_write_version_partition", write)
    with pytest.raises(RuntimeError, match="fixture writer failure"):
        lake.items.initialize("derived", start="2020-01-01", end="2020-04-30",
                              config=ExecutionOptions(workers=3))
    assert len(settled) == 3
    assert lake.items.manifest("derived") == []
    assert lake.integrity.active_update_leases() == []
    assert not list((tmp_path / "lake" / "items").rglob("*.parquet"))
    monkeypatch.setattr(versions, "_write_version_partition", original)
    report = lake.items.initialize("derived", start="2020-01-01", end="2020-04-30",
                                   config=ExecutionOptions(workers=3))
    assert report.status == "success" and lake.items.read("derived").collect().height == 4


def test_recovery_compression_does_not_hold_sqlite_writer_lock(tmp_path, monkeypatch):
    lake = fixture_lake(tmp_path)
    original = recovery.zlib.compress
    calls = []

    def compress(payload, level):
        with sqlite3.connect(tmp_path / "data.sqlite", timeout=0) as connection:
            connection.execute("begin immediate")
            connection.rollback()
        calls.append(len(payload))
        return original(payload, level)

    monkeypatch.setattr(recovery.zlib, "compress", compress)
    report = lake.items.initialize("derived", start="2020-01-01", end="2020-04-30",
                                   config=ExecutionOptions(workers=1))
    assert report.status == "success" and len(calls) == 4


def test_small_writer_reserve_falls_back_to_serial_preparation(tmp_path, monkeypatch):
    lake = fixture_lake(tmp_path)
    original = versions._write_version_partition
    threads = set()

    def write(*args):
        threads.add(get_ident())
        return original(*args)

    monkeypatch.setattr(versions, "_write_version_partition", write)
    report = lake.items.initialize("derived", start="2020-01-01", end="2020-04-30",
                                   config=ExecutionOptions(workers=3, max_buffer_bytes=4096))
    assert report.status == "success" and len(threads) == 1


def test_revision_calculation_and_writes_share_pool_and_cancel_retains_commit(tmp_path, monkeypatch):
    lake = fixture_lake(tmp_path)
    lake.raw.ingest(lake.raw.get("prices", source="custom"),
                    pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [5.0]}),
                    ingested_at=datetime(2020, 3, 15, tzinfo=UTC))
    threads = set()
    lock = Lock()
    original = versions._write_version_partition
    gate = Barrier(3, timeout=10)
    writes = 0
    stopped = False

    def producer(context):
        with lock:
            threads.add(get_ident())
        return context.frames["custom/prices"].select(pl.col("source_time").alias("time"), "asset_id", "value")

    def write(*args):
        nonlocal writes
        with lock:
            threads.add(get_ident())
            writes += 1
            number = writes
        if number <= 3:
            gate.wait()
        return original(*args)

    def progress(report):
        nonlocal stopped
        stopped = True

    lake.items.register_producer("derived", "1", producer)
    lake.items.register(DataItemSpec("derived", (RawInput("custom", "prices"),),
                                   producer_key="derived", producer_revision="1"))
    monkeypatch.setattr(versions, "_write_version_partition", write)
    report = lake.items.initialize("derived", start="2020-01-01", end="2020-04-30",
        config=ExecutionOptions(workers=3), progress=progress, cancelled=lambda: stopped)
    assert report.status == "cancelled" and report.rows_committed == 3
    assert writes == 3 and len(threads) <= 3
    assert lake.integrity.active_update_leases() == []
    assert lake.items.read("derived", view="snapshot", as_of="2020-03-14").collect()["value"].to_list() == [1.0, 2.0, 3.0]
    monkeypatch.setattr(versions, "_write_version_partition", original)
    completed = lake.items.initialize("derived", start="2020-01-01", end="2020-04-30",
                                      config=ExecutionOptions(workers=3))
    assert completed.status == "success"
    assert lake.items.read("derived", view="snapshot", as_of="2020-04-30").collect()["value"].to_list() == [5.0, 2.0, 3.0, 4.0]


def test_general_recovery_retains_columns_omitted_by_later_snapshot(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "data.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("membership", "general", field_mappings={"asset_id": "asset_id"})
    lake.raw.ingest(spec, pl.DataFrame({"asset_id": ["A"], "value": [1.0], "old": ["retained"]}),
                    ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    lake.raw.ingest(spec, pl.DataFrame({"asset_id": ["A"], "value": [2.0]}),
                    ingested_at=datetime(2020, 1, 2, tzinfo=UTC))
    partition = lake.raw.manifest("membership", source="custom")[0]["partition_path"]
    state = recovery.inspect_partition(lake._parquet, "custom", "membership", partition, deep=True)
    assert state["recovery_state"] == "ready", state
    frame = recovery.reconstruct(lake._parquet, "custom", "membership", partition)
    assert frame.sort("snapshot_date")["old"].to_list() == ["retained", None]


def test_existing_baseline_batches_match_chronological_noop_and_retain_frozen(tmp_path, monkeypatch):
    from bagelquant_data.items import api as item_api
    serial = fixture_lake(tmp_path / "serial")
    batched = fixture_lake(tmp_path / "batched")
    for lake in (serial, batched):
        lake.items.initialize("derived", start="2020-01-01", end="2020-04-30")
    receipt = batched.inputs.freeze({"item": ItemInput("derived")}, information_cutoff="2020-04-30")
    frozen = batched.inputs.read(receipt, "item").collect()
    original_publish = serial.items._publish_proven

    def chronological(*args, **kwargs):
        kwargs["commit_batch_rows"] = None
        return original_publish(*args, **kwargs)

    monkeypatch.setattr(serial.items, "_publish_proven", chronological)
    original_commit = item_api.commit_versions
    counts = []

    def commit(spec, frame, *args, **kwargs):
        counts.append(frame.height)
        return original_commit(spec, frame, *args, **kwargs)

    monkeypatch.setattr(item_api, "commit_versions", commit)
    serial.items.update("derived", start="2020-01-01", end="2020-04-30", force=True)
    assert counts == [1, 1, 1, 1]
    counts.clear()
    report = batched.items.update("derived", start="2020-01-01", end="2020-04-30", force=True,
                                  config=ExecutionOptions(workers=3, commit_batch_rows=2))
    assert report.status == "success" and report.rows_committed == 0
    assert counts == [2, 2]
    for cutoff in ("2020-01-01", "2020-02-01", "2020-03-01", "2020-04-30"):
        assert batched.items.read("derived", view="snapshot", as_of=cutoff).collect().equals(
            serial.items.read("derived", view="snapshot", as_of=cutoff).collect())
    assert batched.items.read("derived", strict=True).collect().is_empty()
    assert batched.inputs.read(receipt, "item").collect().equals(frozen)
    assert batched.inputs.verify(receipt)["valid"]
    assert batched.inputs.is_current(receipt)


def test_existing_baseline_publication_bytes_bound_admission(tmp_path, monkeypatch):
    from bagelquant_data.items import api as item_api
    lake = fixture_lake(tmp_path)
    lake.items.initialize("derived", start="2020-01-01", end="2020-04-30")
    original = item_api.commit_versions
    counts = []

    def commit(spec, frame, *args, **kwargs):
        counts.append(frame.height)
        return original(spec, frame, *args, **kwargs)

    monkeypatch.setattr(item_api, "commit_versions", commit)
    report = lake.items.update("derived", start="2020-01-01", end="2020-04-30", force=True,
                               config=ExecutionOptions(workers=1, commit_batch_rows=4, max_buffer_bytes=4096))
    assert report.status == "success" and counts == [1, 1, 1, 1]


def test_verified_rechecks_remain_chronological_for_frozen_cutoffs(tmp_path, monkeypatch):
    from bagelquant_data.items import api as item_api
    lake = DataLake.open(data_meta_path=tmp_path / "data.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("prices", "by_date", date_kind="calendar",
                       field_mappings={"time": "time", "asset_id": "asset_id"})
    for day in (1, 2):
        lake.raw.ingest(spec, pl.DataFrame({"time": [date(2020, 1, day)], "asset_id": ["A"], "value": [float(day)]}),
                        ingested_at=datetime(2020, 1, day, tzinfo=UTC))
    lake.items.register(DataItemSpec("derived", (RawInput("custom", "prices"),), time_column="source_time"))
    lake.items.initialize("derived", start="2020-01-01", end="2020-01-02")
    original = item_api.commit_versions
    counts = []

    def commit(spec, frame, *args, **kwargs):
        counts.append(frame.height)
        return original(spec, frame, *args, **kwargs)

    monkeypatch.setattr(item_api, "commit_versions", commit)
    report = lake.items.update("derived", start="2020-01-01", end="2020-01-02", force=True,
                               config=ExecutionOptions(workers=3, commit_batch_rows=2))
    assert report.status == "success" and counts == [1, 1]
    receipt = lake.inputs.freeze({"item": ItemInput("derived", view="snapshot", strict=True)}, information_cutoff="2020-01-01")
    frozen = lake.inputs.read(receipt, "item").collect()
    assert frozen.height == 1 and frozen["value"].to_list() == [1.0]


def test_verified_noop_witnesses_do_not_delay_old_baseline_frozen_visibility(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "data.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("prices", "by_date", date_kind="calendar",
                       field_mappings={"time": "time", "asset_id": "asset_id"})
    for observation, received in ((1, 3), (2, 4)):
        lake.raw.ingest(spec, pl.DataFrame({"time": [date(2020, 1, observation)], "asset_id": ["A"], "value": [float(observation)]}),
                        ingested_at=datetime(2020, 1, received, tzinfo=UTC))

    def producer(context):
        return context.frames["custom/prices"].select(pl.col("source_time").alias("time"), "asset_id", "value", pl.col("time").alias("available_date"))

    lake.items.register_producer("fixture", "1", producer)
    lake.items.register(DataItemSpec("derived", (RawInput("custom", "prices"),),
                                   producer_key="fixture", producer_revision="1"))
    inputs = lake.inputs.freeze({"custom/prices": RawInput("custom", "prices", view="versions", include_historical_baseline=True)}, information_cutoff="2020-01-04")
    frame = pl.DataFrame({"time": [date(2020, 1, 1), date(2020, 1, 2)], "asset_id": ["A", "A"], "value": [1.0, 2.0],
                          "available_date": [date(2020, 1, 3), date(2020, 1, 4)]})
    lake.items.ingest("derived", frame, historical_baseline=True, input_receipt=inputs)
    original = lake.inputs.freeze({"item": ItemInput("derived", view="snapshot", strict=True)}, information_cutoff="2020-01-03")
    assert lake.inputs.read(original, "item").collect().is_empty()
    report = lake.items.update("derived", start="2020-01-01", end="2020-01-04", force=True,
                               config=ExecutionOptions(workers=3, commit_batch_rows=2))
    assert report.rows_committed == 0
    current = lake.inputs.freeze({"item": ItemInput("derived", view="snapshot", strict=True)}, information_cutoff="2020-01-03")
    assert lake.inputs.read(current, "item").collect()["value"].to_list() == [1.0]
    assert lake.inputs.read(original, "item").collect().is_empty()
    assert lake.inputs.verify(original)["valid"]


def test_cancel_between_baseline_batches_reports_retained_rows_without_success_build(tmp_path):
    lake = fixture_lake(tmp_path)
    lake.items.initialize("derived", start="2020-01-01", end="2020-04-30")
    old = lake.inputs.freeze({"item": ItemInput("derived")}, information_cutoff="2020-04-30")
    prior_build_count = len(lake.items.builds("derived"))
    lake.raw.ingest(lake.raw.get("prices", source="custom"),
                    pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [5.0]}),
                    ingested_at=datetime(2020, 4, 30, tzinfo=UTC))
    updates = []

    def progress(report):
        updates.append(report)

    report = lake.items.update("derived", start="2020-01-01", end="2020-04-30",
        config=ExecutionOptions(workers=3, commit_batch_rows=1),
        progress=progress, cancelled=lambda: bool(updates))
    assert report.status == "cancelled" and report.rows_committed == 1
    assert report.commit_seq is not None and updates[-1].rows_committed == 1
    assert len(lake.items.builds("derived")) == prior_build_count
    assert lake.integrity.active_update_leases() == []
    assert lake.inputs.verify(old)["valid"]
    assert lake.items.read("derived", view="snapshot", as_of="2020-04-29").collect()["value"].to_list() == [1.0, 2.0, 3.0, 4.0]
    completed = lake.items.update("derived", start="2020-01-01", end="2020-04-30",
                                   config=ExecutionOptions(workers=3, commit_batch_rows=1))
    assert completed.status == "success"
    assert lake.items.read("derived", view="snapshot", as_of="2020-04-30").collect()["value"].to_list() == [5.0, 2.0, 3.0, 4.0]
