"""Transport keeps moving while the same bounded pool prepares publication."""
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Barrier, Event, Lock, get_ident, current_thread

import polars as pl
import pytest

from bagelquant_data.pipeline import versions
from bagelquant_data.core import ConfigurationError
from test_update_performance import DailySource, _daily_lake, _daily_spec


def test_raw_native_admission_is_a_public_execution_only_option(tmp_path):
    from contextlib import contextmanager
    lake = _daily_lake(tmp_path, ["20270101"])
    calls = []
    @contextmanager
    def admission(kind):
        calls.append(kind)
        yield
    with pytest.raises(ConfigurationError, match="native_admission must be callable"):
        lake.raw.update("daily", source="custom", end="2027-01-01", native_admission=3)
    report = lake.raw.update("daily", source="custom", end="2027-01-01", native_admission=admission)
    assert report.status == "success"
    assert calls and set(calls) == {"columnar"}


@pytest.mark.parametrize("cancel", [False, True])
def test_fetch_admission_overlaps_commit_and_publication_stays_serial(tmp_path, monkeypatch, cancel):
    lake = _daily_lake(tmp_path, [f"202701{day:02d}" for day in range(1, 9)])
    writing, fetched = Event(), Event()
    cancel_requested = Event()
    pool_threads, publisher_threads = set(), []
    lock = Lock()
    initial = Barrier(2, timeout=5)
    fetch_count = 0
    original = versions._write_version_partition
    first_write = True
    def write(*args):
        nonlocal first_write
        pool_threads.add(current_thread().name)
        if first_write:
            first_write = False
            writing.set()
            assert fetched.wait(5), "no fetch was admitted during a blocked commit"
        return original(*args)
    class Source(DailySource):
        def fetch(self, dataset, request):
            nonlocal fetch_count
            with lock:
                pool_threads.add(current_thread().name)
                fetch_count += 1
                number = fetch_count
            if number <= 2:
                initial.wait()
            if number > 2 and writing.is_set():
                if cancel:
                    cancel_requested.set()
                fetched.set()
            return super().fetch(dataset, request)
    publish = lake._parquet.commit_metadata
    def publish_serial(*args, **kwargs):
        publisher_threads.append(get_ident())
        return publish(*args, **kwargs)
    lake.catalog.sources.register(Source())
    monkeypatch.setattr(versions, "_write_version_partition", write)
    monkeypatch.setattr(lake._parquet, "commit_metadata", publish_serial)
    report = lake.raw.update("daily", source="custom", end="2027-01-08", workers=3,
        max_in_flight=2, commit_batch_rows=1, cancel_requested=cancel_requested.is_set)
    assert fetched.is_set()
    assert publisher_threads and set(publisher_threads) == {get_ident()}
    assert len(pool_threads) <= 3 and all(name.startswith("bagelquant-data") for name in pool_threads)
    assert report.peak_in_flight <= 2
    assert report.status == ("cancelled" if cancel else "success")
    if cancel:
        assert not lake.integrity.active_update_leases()
        assert not lake._data_meta.update_scopes(source="custom", dataset="daily", status="running")
        report = lake.raw.update("daily", source="custom", end="2027-01-08", workers=2,
            max_in_flight=2, commit_batch_rows=1)
        assert report.status == "success"
    assert lake.raw.read("daily", source="custom", view="latest").collect().height == 8


def test_prefetch_callback_failure_drains_every_partition_before_rollback(tmp_path, monkeypatch):
    lake = _daily_lake(tmp_path, ["20270102", "20270202"])
    frame = pl.DataFrame({"trade_date": ["20270102", "20270202"], "ts_code": ["A", "A"], "value": [1., 1.]})
    lake.raw.ingest(_daily_spec(), frame)
    manifest = lake._data_meta.manifest("custom", "daily")
    old = {path: path.read_bytes() for path in tmp_path.rglob("data-*.parquet")}
    release = Event()
    finished = []
    original = versions._write_version_partition
    def write(*args):
        assert release.wait(5)
        result = original(*args)
        finished.append(args[4])
        return result
    def fail():
        release.set()
        raise RuntimeError("prefetch admission failed")
    monkeypatch.setattr(versions, "_write_version_partition", write)
    with ThreadPoolExecutor(max_workers=2) as executor:
        with pytest.raises(RuntimeError, match="prefetch admission failed"):
            lake._pipeline.commit_frame(_daily_spec(), frame.with_columns(pl.lit(2.).alias("value")),
                run_id="prefetch-failure", writer_executor=executor, partition_workers=2,
                ingested_at=datetime(2026, 1, 1, tzinfo=UTC), on_wait=fail)
    assert len(finished) == 2
    assert lake._data_meta.manifest("custom", "daily") == manifest
    assert {path: path.read_bytes() for path in old} == old
    assert not list(tmp_path.rglob("*.rollback"))
