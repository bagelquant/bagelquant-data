from concurrent.futures import CancelledError, ThreadPoolExecutor
from datetime import UTC, date, datetime
from dataclasses import replace
import hashlib
import json
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, ItemPublication, RawInput


def publish(lake, publications, **kwargs):
    with lake.items.publication(**kwargs) as publisher:
        return publisher.publish(publications)


def fixture(root):
    lake = DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")
    lake.raw.ingest(DatasetSpec("source", "by_date", date_kind="calendar",
                               field_mappings={"time": "time", "asset_id": "asset_id"}),
                    pl.DataFrame({"time": [date(2020, 1, 2)], "asset_id": ["A"], "value": [1.]}),
                    ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    request = RawInput("custom", "source")
    receipt = lake.inputs.freeze({"source": request}, information_cutoff="2020-01-31")
    for name in ("first", "second"):
        lake.items.register(DataItemSpec(name, (request,)))
    frame = pl.DataFrame({"time": [date(2020, 1, 2)] * 2, "asset_id": ["A"] * 2,
                          "value": [1., 9.], "version_available_date": [date(2020, 1, 2), date(2020, 1, 15)]})
    return lake, receipt, frame


def publication(lake, name, frame):
    return ItemPublication(name, frame, expected_definition_hash=lake.items.status(name)["definition_hash"],
                           start="2020-01-01", end="2020-01-31", complete_at="2020-01-31")


def test_multi_publication_preserves_versions_empty_certificates_and_rechecks(tmp_path, monkeypatch):
    lake, receipt, frame = fixture(tmp_path)
    original = lake.inputs.verify
    calls = []
    def verify(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)
    monkeypatch.setattr(lake.inputs, "verify", verify)
    publish(lake, [publication(lake, "first", frame), publication(lake, "second", frame.head(0))], input_receipt=receipt)
    assert len(calls) == 1
    assert lake.items.read("first", view="snapshot", as_of="2020-01-10").collect()["value"].to_list() == [1.]
    assert lake.items.read("first", view="snapshot", as_of="2020-01-20").collect()["value"].to_list() == [9.]
    old = lake.inputs.freeze({"first": ItemInput("first", start="2020-01-01", end="2020-01-31")}, information_cutoff="2020-01-31")
    empty = lake.inputs.freeze({"second": ItemInput("second", start="2020-01-01", end="2020-01-31")}, information_cutoff="2020-01-31")
    assert lake.inputs.is_current(empty)
    publish(lake, [publication(lake, "first", frame.head(0))], input_receipt=receipt)
    assert len(calls) == 2
    assert lake.inputs.read(old, "first", view="snapshot").collect()["value"].to_list() == [9.]
    assert lake.items.read("first", view="snapshot", as_of="2020-01-31").collect()["value"].to_list() == [None]
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update version_batches set payload=x'00' where commit_seq=1")
    with pytest.raises(RuntimeError):
        publish(lake, [publication(lake, "second", frame)], input_receipt=receipt)
    assert len(calls) == 3 and not lake.integrity.active_update_leases()


def test_multi_publication_keeps_committed_first_output_on_cancellation(tmp_path):
    lake, receipt, frame = fixture(tmp_path)
    with pytest.raises(CancelledError):
        publish(lake, [publication(lake, "first", frame), publication(lake, "second", frame)],
            input_receipt=receipt, cancelled=lambda: len(lake.items.builds("first")) >= 2)
    assert lake.items.status("first")["row_count"] > 0
    assert not lake.items.builds("second") and not lake.integrity.active_update_leases()


def test_multi_publication_validates_dependency_identity_and_definition_cas(tmp_path):
    lake, receipt, frame = fixture(tmp_path)
    lake.items.register(DataItemSpec("second"))
    with pytest.raises(ValueError, match="does not match"):
        publish(lake, [publication(lake, "first", frame), publication(lake, "second", frame)], input_receipt=receipt)
    assert not lake.items.builds("first")
    stale = publication(lake, "first", frame)
    lake.items.register(DataItemSpec("first", (RawInput("custom", "source"),), description="changed"))
    with pytest.raises(RuntimeError, match="definition changed"):
        publish(lake, [stale], input_receipt=receipt)
    assert not lake.items.builds("first") and not lake.integrity.active_update_leases()


def test_cancel_after_historical_output_prevents_complete_range_certificate(tmp_path, monkeypatch):
    lake, receipt, frame = fixture(tmp_path)
    original = lake.items._ingest
    cancel = [False]
    def ingest(*args, **kwargs):
        result = original(*args, **kwargs)
        cancel[0] = True
        return result
    monkeypatch.setattr(lake.items, "_ingest", ingest)
    with pytest.raises(CancelledError):
        publish(lake, [publication(lake, "first", frame)], input_receipt=receipt,
                           cancelled=lambda: cancel[0])
    builds = lake.items.builds("first")
    assert len(builds) == 1 and builds[0]["start_date"] == builds[0]["end_date"] == "2020-01-02"
    assert not lake.integrity.active_update_leases()


def test_operation_verifies_once_across_groups_expires_and_rechecks_next_entry(tmp_path, monkeypatch):
    lake, receipt, frame = fixture(tmp_path)
    original = lake.inputs.verify
    calls = []
    def verify(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)
    monkeypatch.setattr(lake.inputs, "verify", verify)
    with lake.items.publication(input_receipt=receipt) as operation:
        assert operation.lake is lake and operation.input_receipt == receipt
        operation.publish([publication(lake, "first", frame)])
        operation.publish([publication(lake, "second", frame)])
        assert len(calls) == 1
    assert not operation.active
    with pytest.raises(RuntimeError, match="closed"):
        operation.publish([publication(lake, "first", frame.head(0))])
    assert lake.items.read("first", view="snapshot", as_of="2020-01-31").collect()["value"].to_list() == [9.]
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update version_batches set payload=x'00' where commit_seq=1")
    with pytest.raises(RuntimeError):
        with lake.items.publication(input_receipt=receipt):
            pytest.fail("Corrupt input admitted")
    assert len(calls) == 2 and not lake.integrity.active_update_leases()


def test_caught_group_failure_invalidates_operation_and_rejects_success_exit(tmp_path):
    lake, receipt, frame = fixture(tmp_path)
    with pytest.raises(RuntimeError, match="operation failed"):
        with lake.items.publication(input_receipt=receipt) as operation:
            operation.publish([publication(lake, "first", frame)])
            lake.items.register(DataItemSpec("second"))
            with pytest.raises(ValueError, match="does not match"):
                operation.publish([publication(lake, "second", frame)])
            with pytest.raises(RuntimeError, match="closed"):
                operation.publish([publication(lake, "first", frame)])
    assert lake.items.builds("first") and not lake.items.builds("second")
    assert not lake.integrity.active_update_leases()


def test_cancellation_at_context_exit_retains_commits_and_rejects_success(tmp_path):
    lake, receipt, frame = fixture(tmp_path)
    cancel = [False]
    with pytest.raises(CancelledError):
        with lake.items.publication(input_receipt=receipt, cancelled=lambda: cancel[0]) as operation:
            operation.publish([publication(lake, "first", frame)])
            cancel[0] = True
    assert not operation.active and lake.items.builds("first")
    assert not lake.integrity.active_update_leases()


def test_operation_rejects_other_thread_before_writer_can_outlive_context(tmp_path):
    lake, receipt, frame = fixture(tmp_path)
    with lake.items.publication(input_receipt=receipt) as operation:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(operation.publish, [publication(lake, "first", frame)])
            with pytest.raises(RuntimeError, match="owning thread"):
                future.result()
        assert not lake.items.builds("first")
        operation.publish([publication(lake, "second", frame)])
    assert not operation.active and lake.items.builds("second")
    assert not lake.integrity.active_update_leases()


def test_forest_verifies_every_original_output_and_shared_parent_once(tmp_path, monkeypatch):
    from bagelquant_data import inputs
    lake, receipt, frame = fixture(tmp_path)
    with lake.items.publication(input_receipt=receipt) as operation:
        operation.publish([publication(lake, "first", frame), publication(lake, "second", frame)])
    roots = [lake.inputs.freeze({name: ItemInput(name)}, information_cutoff="2020-01-31")
             for name in ("first", "second")]
    original = inputs.verify_batch
    batches = []
    def verify(*args, **kwargs):
        batches.append((args[1], args[2], args[3]))
        return original(*args, **kwargs)
    monkeypatch.setattr(inputs, "verify_batch", verify)
    report = lake.inputs.verify(roots)
    assert report["valid"] and report["receipts"] == [
        {"receipt_id": value.receipt_id, "digest": value.digest} for value in roots]
    assert len(batches) == len(set(batches))
    source = receipt.evidence["source"]["batches"][0]
    assert batches.count((source["partition_path"], source["commit_seq"], source["content_hash"])) == 1
    assert report["upstream_receipt_count"] == 1
    batches.clear()
    assert lake.inputs.verify(roots) == report and batches
    output = roots[1].evidence["second"]["batches"][0]
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update version_batches set payload=x'00' where commit_seq=? and partition_path=?",
                   (output["commit_seq"], output["partition_path"]))
    with pytest.raises(RuntimeError):
        lake.inputs.verify(roots)


def test_forest_validates_each_explicit_root_digest_and_rejects_empty(tmp_path):
    lake, receipt, _ = fixture(tmp_path)
    with pytest.raises(RuntimeError, match="checksum"):
        lake.inputs.verify([receipt, replace(receipt, digest="0" * 64)])
    with pytest.raises(ValueError, match="at least one"):
        lake.inputs.verify([])


def test_forest_checks_every_shared_parent_edge_digest(tmp_path):
    lake, receipt, frame = fixture(tmp_path)
    with lake.items.publication(input_receipt=receipt) as operation:
        operation.publish([publication(lake, "first", frame), publication(lake, "second", frame)])
    roots = [lake.inputs.freeze({name: ItemInput(name)}, information_cutoff="2020-01-31")
             for name in ("first", "second")]
    with sqlite3.connect(lake.data_meta_path) as db:
        payload = json.loads(db.execute("select payload_json from frozen_inputs where receipt_id=?",
                                        (roots[1].receipt_id,)).fetchone()[0])
        payload["evidence"]["second"]["parent_receipts"][receipt.receipt_id] = "0" * 64
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        db.execute("update frozen_inputs set payload_json=?,digest=? where receipt_id=?",
                   (serialized, hashlib.sha256(serialized.encode()).hexdigest(), roots[1].receipt_id))
    with pytest.raises(RuntimeError, match="upstream.*checksum"):
        lake.inputs.verify([value.receipt_id for value in roots])
