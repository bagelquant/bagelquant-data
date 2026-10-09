from datetime import UTC, date, datetime

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ExecutionOptions, ItemPublication, RawInput


def setup(root, *, attested=False):
    lake = DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")
    rows = pl.DataFrame({"time": [date(2020,1,1)], "asset_id": ["A"], "value": [1.]})
    requests = {}
    for name in ("one", "two"):
        spec = DatasetSpec(name, "by_date", date_kind="calendar", field_mappings={"time":"time","asset_id":"asset_id"})
        lake.raw.ingest(spec, rows, mode="initialize", ingested_at=datetime(2020,1,1,tzinfo=UTC))
        if attested:
            lake.raw.ingest(spec, rows, ingested_at=datetime(2020,1,15,tzinfo=UTC))
        requests[name] = RawInput("custom", name, alias=name, view="versions", include_historical_baseline=True)
    receipt = lake.inputs.freeze(requests, information_cutoff="2020-01-31")
    for name in ("first", "second"):
        lake.items.register(DataItemSpec(name, tuple(requests.values())))
    return lake, receipt, rows


def output(name, rows, day):
    return ItemPublication(name, rows.with_columns(pl.lit(day).alias("version_available_date")))


def test_positive_timing_short_circuits_other_aliases_and_repeated_groups(tmp_path, monkeypatch):
    lake, receipt, rows = setup(tmp_path)
    original = lake.inputs._read_frame
    calls = []
    def read(value, alias, **kwargs):
        calls.append(alias)
        return original(value, alias, **kwargs)
    monkeypatch.setattr(lake.inputs, "_read_frame", read)
    with lake.items.publication(input_receipt=receipt) as operation:
        operation.publish([output("first", rows, date(2020,1,10))])
        operation.publish([output("second", rows, date(2020,1,10))])
        assert calls == ["one"]
    assert not operation.active
    assert lake.items.read("first", view="versions").collect()["_baseline"].to_list() == [True]
    with lake.items.publication(input_receipt=receipt) as new:
        new.publish([output("second", rows, date(2020,1,10))])
    assert calls == ["one", "one"]


def test_later_attestation_reverses_timing_and_negative_memo_requires_all_aliases(tmp_path, monkeypatch):
    lake, receipt, rows = setup(tmp_path, attested=True)
    original = lake.inputs._baseline_at
    calls = []
    def baseline(value, alias, cutoff, **kwargs):
        result = original(value, alias, cutoff, **kwargs)
        calls.append((alias, cutoff, result))
        return result
    monkeypatch.setattr(lake.inputs, "_baseline_at", baseline)
    with lake.items.publication(input_receipt=receipt) as operation:
        for day in (date(2020,1,10), date(2020,1,20)):
            operation.publish([output("first", rows, day)])
            operation.publish([output("second", rows, day)])
    assert calls == [("one",date(2020,1,10),True), ("one",date(2020,1,20),False),
                     ("two",date(2020,1,20),False)]
    for name in ("first", "second"):
        versions = lake.items.read(name, view="versions").collect()
        assert versions["_baseline"].to_list() == [True, False]
        assert lake.items.read(name, view="snapshot", as_of="2020-01-10", strict=True).collect().is_empty()
        assert lake.items.read(name, view="snapshot", as_of="2020-01-20", strict=True).collect()["value"].to_list() == [1.]


def test_timing_memo_respects_explicit_small_entry_reserve_and_eviction(tmp_path, monkeypatch):
    lake, receipt, rows = setup(tmp_path)
    original = lake.inputs._baseline_at
    calls = []
    def baseline(*args, **kwargs):
        calls.append(args[2])
        return original(*args, **kwargs)
    monkeypatch.setattr(lake.inputs, "_baseline_at", baseline)
    # 8KiB /4 /1KiB admits only two timing entries.
    with lake.items.publication(input_receipt=receipt, config=ExecutionOptions(max_buffer_bytes=8192)) as operation:
        for day in (10,11,12,10):
            operation.publish([output("first", rows, date(2020,1,day))])
    assert calls == [date(2020,1,day) for day in (10,11,12,10)]


def test_failed_group_clears_timing_memo_and_cannot_seed_next_operation(tmp_path, monkeypatch):
    lake, receipt, rows = setup(tmp_path)
    original = lake.inputs._baseline_at
    calls = []
    def baseline(*args, **kwargs):
        calls.append(args[2])
        return original(*args, **kwargs)
    monkeypatch.setattr(lake.inputs, "_baseline_at", baseline)
    with pytest.raises(RuntimeError, match="operation failed"):
        with lake.items.publication(input_receipt=receipt) as operation:
            operation.publish([output("first", rows, date(2020,1,10))])
            lake.items.register(DataItemSpec("second"))
            with pytest.raises(ValueError):
                operation.publish([output("second", rows, date(2020,1,10))])
    assert not operation.active and not lake.integrity.active_update_leases()
    with lake.items.publication(input_receipt=receipt) as new:
        new.publish([output("first", rows, date(2020,1,10))])
    assert calls == [date(2020,1,10)] * 2


def test_reused_build_rechecks_bytes_using_explicit_budget(tmp_path, monkeypatch):
    lake, _, _ = setup(tmp_path)
    lake.items.register(DataItemSpec("copy", (RawInput("custom", "one"),), time_column="source_time"))
    lake.items.initialize("copy", start="2020-01-01", end="2020-01-31")
    original = lake.inputs.verify
    calls = []
    def verify(*args, **kwargs):
        calls.append(kwargs.get("config"))
        return original(*args, **kwargs)
    monkeypatch.setattr(lake.inputs, "verify", verify)
    config = ExecutionOptions(workers=3, max_buffer_bytes=8*1024**2)
    assert lake.items.update("copy", start="2020-01-01", end="2020-01-31", config=config).status == "unchanged"
    assert calls == [config]
    assert lake.items.update("copy", start="2020-01-01", end="2020-01-31").status == "unchanged"
    assert calls == [config, None]
    import sqlite3
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update version_batches set payload=x'00' where commit_seq=1")
    with pytest.raises(RuntimeError):
        lake.items.update("copy", start="2020-01-01", end="2020-01-31", config=config)
    assert calls == [config, None, config]


def test_one_group_uses_latest_pending_cutoff_without_losing_earlier_timing(tmp_path, monkeypatch):
    lake, receipt, rows = setup(tmp_path, attested=True)
    original = lake.inputs._read_frame
    cutoffs = []
    def read(*args, **kwargs):
        cutoffs.append(kwargs.get("timing_cutoff"))
        return original(*args, **kwargs)
    monkeypatch.setattr(lake.inputs, "_read_frame", read)
    frames = [rows.with_columns(pl.lit(date(2020,1,day)).alias("version_available_date")) for day in (10,20)]
    with lake.items.publication(input_receipt=receipt) as operation:
        operation.publish([ItemPublication("first", pl.concat(frames))])
    assert cutoffs == [date(2020,1,20)] * 2
    assert lake.items.read("first", view="versions").collect()["_baseline"].to_list() == [True, False]
