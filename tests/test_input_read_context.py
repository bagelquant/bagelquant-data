from dataclasses import replace
from datetime import UTC, date, datetime
from concurrent.futures import ThreadPoolExecutor
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, RawInput, input_read_boundary
from bagelquant_data import inputs
from bagelquant_data.execution import ExecutionOptions
from test_item_input_windows import fixture


def frozen_months(tmp_path):
    lake, _, _ = fixture(tmp_path)
    frozen = lake.inputs.freeze({"raw": RawInput("custom", "values", start="2020-01-01", end="2020-03-31", view="snapshot")},
                               information_cutoff="2020-04-01")
    return lake, frozen


def test_context_reuses_original_metadata_across_public_readers_and_expires(tmp_path, monkeypatch):
    lake, frozen = frozen_months(tmp_path)
    original = inputs.json.loads
    loads = []
    def tracked(value, *args, **kwargs):
        if '"requests"' in value:
            loads.append(value)
        return original(value, *args, **kwargs)
    monkeypatch.setattr(inputs.json, "loads", tracked)
    with lake.inputs.read_context(frozen) as reader:
        nested = DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path,
                               read_only=True, runtime=True)
        assert nested.inputs.get(frozen).digest == frozen.digest
        assert reader.read(frozen, "raw").collect().height == 3
        assert nested.inputs.is_current(frozen)
        assert len(loads) == 1
        with pytest.raises(TypeError):
            reader.get(frozen).evidence["raw"]["batches"][0]["content_hash"] = "bad"
    lake.inputs.get(frozen)
    assert len(loads) == 2


def test_external_object_evidence_is_never_authority_and_every_root_digest_checked(tmp_path):
    lake, frozen = frozen_months(tmp_path)
    invented = replace(frozen, evidence={}, requests={})
    with lake.inputs.read_context(invented, verify=False) as reader:
        assert reader.read(invented, "raw").collect().height == 3
        with pytest.raises(RuntimeError, match="checksum"):
            reader.verify([frozen, replace(frozen, digest="bad")])
    with pytest.raises(RuntimeError, match="checksum"):
        with lake.inputs.read_context([frozen, replace(frozen, digest="bad")]):
            pass


def test_later_context_rechecks_original_batch_bytes_and_failed_context_cleans_up(tmp_path):
    lake, frozen = frozen_months(tmp_path)
    with lake.inputs.read_context(frozen):
        pass
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update version_batches set payload=x'00' where commit_seq=1")
    with pytest.raises(RuntimeError, match="compression"):
        with lake.inputs.read_context(frozen):
            pass
    assert lake.inputs._read_context() is None
    assert lake.inputs.get(frozen).digest == frozen.digest


def test_verification_proofs_reuse_only_within_entered_context(tmp_path, monkeypatch):
    lake, frozen = frozen_months(tmp_path)
    original = inputs.verify_batch
    batches = []
    def tracked(*args, **kwargs):
        batches.append((args[1], args[2], args[3]))
        return original(*args, **kwargs)
    monkeypatch.setattr(inputs, "verify_batch", tracked)
    expected = lake.inputs.verify(frozen)
    batches.clear()
    with lake.inputs.read_context(frozen) as reader:
        assert len(batches) == 3
        assert reader.verify(frozen) == expected
        assert reader.verify([frozen, frozen])["batch_count"] == expected["batch_count"]
        assert len(batches) == 3
        with pytest.raises(RuntimeError, match="checksum"):
            reader.verify(replace(frozen, digest="bad"))
    with lake.inputs.read_context(frozen):
        assert len(batches) == 6
    lake.inputs.verify(frozen)
    assert len(batches) == 9


@pytest.mark.parametrize("workers", [1, 2])
def test_progress_deduplicates_batch_work_and_cancel_propagates_with_cleanup(tmp_path, workers):
    lake, frozen = frozen_months(tmp_path)
    options = ExecutionOptions(workers=workers)
    events = []
    with lake.inputs.read_context([frozen, frozen], config=options, progress=events.append):
        pass
    assert events[0] == {"stage": "verify_inputs", "completed": 0, "total": 3}
    assert events[-1]["completed"] == 3
    class Canceled(Exception):
        pass
    calls = 0
    def cancel():
        nonlocal calls
        calls += 1
        if calls >= 10:
            raise Canceled()
    with pytest.raises(Canceled):
        with lake.inputs.read_context(frozen, config=options, check_canceled=cancel):
            pass
    assert lake.inputs._read_context() is None
    assert lake.inputs.verify(frozen, config=options)["valid"]


def test_narrow_window_matches_full_sparse_pit_result_and_reads_fewer_batches(tmp_path, monkeypatch):
    lake, frozen = frozen_months(tmp_path)
    expected = lake.inputs.read(frozen, "raw").collect().filter(pl.col("source_time").is_between(date(2020, 2, 1), date(2020, 2, 29)))
    original = inputs.read_batch
    batches = []
    def tracked(*args, **kwargs):
        batches.append(args[1])
        return original(*args, **kwargs)
    monkeypatch.setattr(inputs, "read_batch", tracked)
    with lake.inputs.read_context(frozen, verify=False) as reader:
        actual = reader.read(frozen, "raw", start="2020-02-01", end="2020-02-29").collect()
    assert actual.equals(expected)
    assert len(batches) == 1
    with pytest.raises(ValueError, match="widen"):
        lake.inputs.read(frozen, "raw", start="2019-12-31")
    with pytest.raises(ValueError, match="precedes"):
        lake.inputs.read(frozen, "raw", start="2020-03-01", end="2020-02-01")
    assert lake.inputs.get(frozen).digest == frozen.digest


def test_legacy_window_remains_conservative(tmp_path, monkeypatch):
    lake, _, _ = fixture(tmp_path)
    original = lake.inputs._capture
    def legacy(*args, **kwargs):
        return original(*args, **{**kwargs, "scoped_aliases": set()})
    with monkeypatch.context() as patch:
        patch.setattr(lake.inputs, "_capture", legacy)
        frozen = lake.inputs.freeze({"raw": RawInput("custom", "values", view="snapshot")}, information_cutoff="2020-04-01")
    original_read = inputs.read_batch
    batches = []
    def tracked(*args, **kwargs):
        batches.append(args[1])
        return original_read(*args, **kwargs)
    monkeypatch.setattr(inputs, "read_batch", tracked)
    assert lake.inputs.read(frozen, "raw", start="2020-02-01", end="2020-02-29").collect()["value"].to_list() == [2.]
    assert len(batches) == 3
    assert not lake.inputs.window_read_supported(frozen, "raw")


@pytest.mark.parametrize("canceled_type", [RuntimeError, TypeError, ValueError])
@pytest.mark.parametrize("buffer_bytes", [256, 1024 * 1024])
def test_ipc_decode_cancellation_closes_mapping_and_allows_retry(tmp_path, monkeypatch, canceled_type, buffer_bytes):
    from bagelquant_data.storage import recovery
    lake, frozen = frozen_months(tmp_path)
    batch = frozen.evidence["raw"]["batches"][0]
    opened = False
    original = recovery.pa.ipc.open_file
    def tracked(*args, **kwargs):
        nonlocal opened
        result = original(*args, **kwargs)
        opened = True
        return result
    def cancel():
        if opened:
            raise canceled_type("IPC canceled")
    with monkeypatch.context() as patch:
        patch.setattr(recovery.pa.ipc, "open_file", tracked)
        with pytest.raises(canceled_type, match="IPC canceled"):
            recovery.verify_batch(lake._data_meta, batch["partition_path"], batch["commit_seq"],
                                  batch["content_hash"], check_canceled=cancel, buffer_bytes=buffer_bytes)
    assert lake.inputs.verify(frozen)["valid"]


def test_read_context_keeps_full_commit_seals_and_shared_parent_graph_exact(tmp_path, monkeypatch):
    from test_full_commit_checks import prepared, request
    from bagelquant_data.storage import full_commit_checks
    lake = prepared(tmp_path)
    monkeypatch.setattr(full_commit_checks, "COMPACT_MIN_ROWS", 1)
    frozen = lake.inputs.freeze(request(), information_cutoff="2020-02-03")
    expected = lake.inputs.read(frozen, "raw").collect()
    with lake.inputs.read_context(frozen) as reader:
        assert reader.window_read_supported(frozen, "raw")
        assert reader.read(frozen, "raw").collect().equals(expected)
        assert reader.read(frozen, "raw", start="2020-02-01", end="2020-02-01").collect()["value"].to_list() == [2.]
        assert reader.is_current(frozen)


def test_window_read_support_is_original_metadata_only_and_unknown_general_stay_broad(tmp_path):
    lake, frozen = frozen_months(tmp_path)
    assert lake.inputs.window_read_supported(frozen, "raw")
    with pytest.raises(KeyError):
        lake.inputs.window_read_supported(frozen, "missing")
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update version_batches set min_observation=null,max_observation=null")
    assert lake.inputs.window_read_supported(frozen, "raw")
    unknown = lake.inputs.freeze(frozen.requests, information_cutoff="2020-04-01")
    assert not lake.inputs.window_read_supported(unknown, "raw")
    lake.raw.ingest(DatasetSpec("general", "general", source="custom"),
                    pl.DataFrame({"asset_id": ["A"], "value": [1.]}),
                    ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    general = lake.inputs.freeze({"raw": RawInput("custom", "general", view="snapshot")}, information_cutoff="2020-04-01")
    assert not lake.inputs.window_read_supported(general, "raw")


def test_raw_dataset_snapshots_reuse_captured_read_view_without_transaction_control(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    general = DatasetSpec("calendar", "general", source="custom")
    prices = DatasetSpec("prices", "by_date", date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"})
    calendar = pl.DataFrame({"time": [date(2020, 1, 1)], "is_open": [1]})
    price = pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.]})
    lake.raw.ingest(general, calendar, ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    lake.raw.ingest(prices, price, ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    frozen = lake.inputs.freeze({"prices": RawInput("custom", "prices", view="snapshot")}, information_cutoff="2020-01-03")
    reader = DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path, read_only=True, runtime=True)
    def update():
        lake.raw.ingest(general, calendar.with_columns(pl.lit(0).alias("is_open")), ingested_at=datetime(2020, 1, 2, tzinfo=UTC))
        lake.raw.ingest(prices, price.with_columns(pl.lit(2.).alias("value")), ingested_at=datetime(2020, 1, 2, tzinfo=UTC))
    with reader.inputs.read_context(frozen, verify=False):
        with reader._data_meta.connect() as connection:
            statements = []
            connection.set_trace_callback(statements.append)
            snapshot = reader._data_meta.dataset_snapshot("custom", "calendar")
            assert len(snapshot["commits"]) == 1
            assert reader.raw.read("calendar", source="custom").collect()["is_open"].to_list() == [1]
            assert reader.raw.read("prices", source="custom", as_of="2020-01-03").collect()["value"].to_list() == [1.]
            assert reader.inputs.is_current(frozen)
            with ThreadPoolExecutor(max_workers=1) as executor:
                executor.submit(update).result()
            assert len(reader._data_meta.dataset_snapshot("custom", "calendar")["commits"]) == 1
            assert reader.raw.read("calendar", source="custom").collect()["is_open"].to_list() == [1]
            assert reader.raw.read("prices", source="custom", as_of="2020-01-03").collect()["value"].to_list() == [1.]
            assert reader.inputs.read(frozen, "prices").collect()["value"].to_list() == [1.]
            assert reader.inputs.is_current(frozen)
            assert connection.in_transaction
            assert not any(statement.strip().lower().startswith(("begin", "commit", "rollback")) for statement in statements)
            connection.set_trace_callback(None)
    assert len(reader._data_meta.dataset_snapshot("custom", "calendar")["commits"]) == 2
    assert reader.raw.read("calendar", source="custom").collect()["is_open"].to_list() == [0]
    assert reader.raw.read("prices", source="custom", as_of="2020-01-03").collect()["value"].to_list() == [2.]
    assert reader.inputs.read(frozen, "prices").collect()["value"].to_list() == [1.]
    assert not reader.inputs.is_current(frozen)


def test_currentness_reuses_completed_boolean_in_context_but_rechecks_after_exit(tmp_path, monkeypatch):
    lake, frozen = frozen_months(tmp_path)
    original = lake.inputs._capture
    captures = []
    def tracked(*args, **kwargs):
        captures.append(kwargs)
        return original(*args, **kwargs)
    monkeypatch.setattr(lake.inputs, "_capture", tracked)
    with lake.inputs.read_context(frozen, verify=False):
        assert lake.inputs.is_current(frozen)
        assert lake.inputs.is_current([frozen, frozen])
        assert len(captures) == 1
        nested = DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path, read_only=True, runtime=True)
        with monkeypatch.context() as patch:
            patch.setattr(nested.inputs, "_capture", lambda *args, **kwargs: pytest.fail("compatible reader recaptured currentness"))
            assert nested.inputs.is_current(frozen)
        with pytest.raises(RuntimeError, match="checksum"):
            lake.inputs.is_current(replace(frozen, digest="forged"))
        assert len(captures) == 1
    spec = lake.raw.get("values", source="custom")
    lake.raw.ingest(spec, pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [9.]}),
                    ingested_at=datetime(2020, 4, 1, tzinfo=UTC))
    assert not lake.inputs.is_current(frozen)
    assert not lake.inputs.is_current(frozen)
    assert len(captures) == 3
    with lake.inputs.read_context(frozen, verify=False):
        assert not lake.inputs.is_current(frozen)
        assert not lake.inputs.is_current(frozen)
        assert len(captures) == 4
    lake.raw.register(replace(spec, description="changed"))
    with lake.inputs.read_context(frozen, verify=False):
        assert not lake.inputs.is_current(frozen)
        assert len(captures) == 5


def test_currentness_context_memo_distinguishes_every_reader_boundary(tmp_path, monkeypatch):
    lake, spec, _ = fixture(tmp_path)
    lake.raw.ingest(spec, pl.DataFrame({"time": [date(2020, month, 1) for month in (1, 2, 3)],
                                      "asset_id": ["A"] * 3, "value": [1., 2., 3.]}),
                    ingested_at=datetime(2020, 3, 3, tzinfo=UTC))
    frozen = lake.inputs.freeze({"raw": RawInput("custom", "values", start="2020-01-01", end="2020-03-31", view="snapshot")}, information_cutoff="2020-04-01")
    lake.raw.ingest(spec, pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [9.]}),
                    ingested_at=datetime(2020, 4, 1, tzinfo=UTC))
    original = inputs.InputsAPI._capture
    captures = []
    def tracked(self, *args, **kwargs):
        captures.append(self._boundary)
        return original(self, *args, **kwargs)
    monkeypatch.setattr(inputs.InputsAPI, "_capture", tracked)
    with lake.inputs.read_context(frozen, verify=False):
        assert not lake.inputs.is_current(frozen)
        for cutoff, check in [("2020-04-01", frozen.max_check_id),
                              ("2020-03-01", frozen.max_check_id),
                              ("2020-04-01", 0)]:
            with input_read_boundary(lake.data_meta_path, frozen.max_commit, cutoff, max_check_id=check):
                bounded = DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path, read_only=True, runtime=True)
                assert bounded.inputs.is_current(frozen) == (check == frozen.max_check_id)
                assert bounded.inputs.is_current(frozen) == (check == frozen.max_check_id)
        assert len(captures) == 4
        assert not lake.inputs.is_current(frozen)
        assert len(captures) == 4


def test_currentness_context_does_not_cache_failed_capture_or_partial_roots(tmp_path, monkeypatch):
    lake, frozen = frozen_months(tmp_path)
    second = lake.inputs.freeze(frozen.requests, information_cutoff="2020-04-01")
    original = lake.inputs._capture
    captures = 0
    def fail_second(*args, **kwargs):
        nonlocal captures
        captures += 1
        if captures == 2:
            raise RuntimeError("transient capture failure")
        return original(*args, **kwargs)
    with lake.inputs.read_context([frozen, second], verify=False):
        with monkeypatch.context() as patch:
            patch.setattr(lake.inputs, "_capture", fail_second)
            with pytest.raises(RuntimeError, match="transient capture"):
                lake.inputs.is_current([frozen, second])
            assert lake.inputs.is_current([frozen, second])
            assert captures == 4
            assert lake.inputs.is_current([frozen, second])
            assert captures == 4
