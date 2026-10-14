"""Shared Source checks retain original no-op authorities and writer fences."""
from concurrent.futures import CancelledError
from dataclasses import replace
from datetime import date
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, ExecutionOptions, ItemInput, ItemPublication


def prepared(tmp_path, count=3):
    lake = DataLake.open(data_meta_path=tmp_path / 'meta.sqlite', lake_path=tmp_path / 'lake')
    rows = pl.DataFrame({'time': [date(2024, 1, 1)], 'asset_id': ['A'], 'value': [1.]})
    lake.items.register(DataItemSpec('leaf'))
    lake.items.ingest('leaf', rows, available_date='2024-01-02')
    requests = {'leaf': ItemInput('leaf')}
    old = lake.inputs.freeze(requests, information_cutoff='2024-01-02')
    values = []
    for index in range(count):
        name = f'output{index}'
        lake.items.register(DataItemSpec(name, tuple(requests.values())))
        with lake.items.publication(input_receipt=old) as publisher:
            publisher.publish([ItemPublication(name, rows, start='2024-01-01', end='2024-01-02',
                complete_at='2024-01-02')])
        build = lake.items.completed_builds(name)[-1]
        values.append(ItemPublication(name, rows.clear(), start='2024-01-01', end='2024-01-02',
            complete_at='2024-01-04', reuse_build_id=build['id'], affected_ranges=()))
    current = lake.inputs.freeze(requests, information_cutoff='2024-01-04')
    return lake, current, values, rows


def records(lake):
    with sqlite3.connect(lake.data_meta_path) as db:
        return {table: db.execute(f'select * from {table} order by rowid').fetchall() for table in (
            'item_builds', 'item_completion_authorities', 'item_pending_outputs', 'version_commits', 'version_checks')}


def test_shared_source_once_and_idempotent_without_historical_reads(tmp_path, monkeypatch):
    lake, current, values, _ = prepared(tmp_path, 8)
    original = records(lake)
    manifests = {value.name: lake.items.manifest(value.name) for value in values}
    actual = lake.inputs.is_current
    checks = []
    def currentness(receipt, **kwargs):
        checks.append(receipt.receipt_id)
        return actual(receipt, **kwargs)
    def forbidden(*a, **k):
        pytest.fail('No-op must not consume numerical bytes')
    monkeypatch.setattr(lake.inputs, 'is_current', currentness)
    monkeypatch.setattr(lake.inputs, 'read', forbidden)
    monkeypatch.setattr(lake.inputs, 'verify', forbidden)
    monkeypatch.setattr(lake.items, '_replace_range', forbidden)
    with lake.items.publication(input_receipt=current) as publisher:
        first = publisher.publish(values)
        assert publisher._verification.sparse_group is None
    assert checks == [current.receipt_id]
    assert all(report is not None and report.rows_committed == 0 for report in first.values())
    after = records(lake)
    assert len(after['item_builds']) == len(original['item_builds']) + len(values)
    for table in ('version_commits', 'version_checks'):
        assert after[table] == original[table]
    for table in ('item_builds', 'item_completion_authorities', 'item_pending_outputs'):
        assert after[table][:len(original[table])] == original[table]
    with lake.items.publication(input_receipt=current) as publisher:
        repeated = publisher.publish(values)
    assert checks == [current.receipt_id] * 2
    assert all(report is not None and report.status == 'unchanged' for report in repeated.values())
    assert records(lake) == after
    assert {value.name: lake.items.manifest(value.name) for value in values} == manifests
    with sqlite3.connect(lake.data_meta_path) as db:
        assert not db.execute('select * from update_leases').fetchall()


@pytest.mark.parametrize('damage', ['authority', 'later_output', 'definition', 'check', 'cancel_mid', 'cancel_end'])
def test_failed_last_item_or_cancel_rolls_back_whole_group(tmp_path, monkeypatch, damage):
    lake, current, values, rows = prepared(tmp_path)
    last = values[-1]
    if damage == 'authority':
        with sqlite3.connect(lake.data_meta_path) as db:
            db.execute("update item_completion_authorities set payload_json='{}' where build_id=?", (last.reuse_build_id,))
    elif damage == 'later_output':
        lake.items.ingest(last.name, rows.with_columns(pl.lit(99.).alias('value')), available_date='2024-01-04', input_receipt=current)
    elif damage == 'definition':
        lake.items.register(replace(lake.items.get(last.name), metadata={'revision': 'changed'}))
    elif damage == 'check':
        with sqlite3.connect(lake.data_meta_path) as db:
            db.execute('insert into version_checks(source,dataset,run_id,checked_at,pit_date,baseline,visible_commit,request_json,row_count,input_receipt_id) '
                'select source,dataset,run_id,checked_at,pit_date,baseline,visible_commit,request_json,row_count,input_receipt_id from version_checks '
                "where source='items' and dataset=? limit 1", (last.name,))
    before = records(lake)
    cancelled = {'value': False}
    actual = lake.items._publish_sparse
    def sparse(value, **kwargs):
        result = actual(value, **kwargs)
        if damage == 'cancel_mid' and value == values[0] or damage == 'cancel_end' and value == last:
            cancelled['value'] = True
        return result
    monkeypatch.setattr(lake.items, '_publish_sparse', sparse)
    with pytest.raises((RuntimeError, ValueError, CancelledError)):
        with lake.items.publication(input_receipt=current, cancelled=lambda: cancelled['value']) as publisher:
            publisher.publish(values)
    assert records(lake) == before
    with sqlite3.connect(lake.data_meta_path) as db:
        assert not db.execute('select * from update_leases').fetchall()
    if damage.startswith('cancel'):
        cancelled['value'] = False
        monkeypatch.setattr(lake.items, '_publish_sparse', actual)
        with lake.items.publication(input_receipt=current) as publisher:
            publisher.publish(values)


def test_foreign_lease_conflict_has_no_partial_leases_or_writes(tmp_path):
    lake, current, values, _ = prepared(tmp_path)
    before = records(lake)
    lake._data_meta.acquire_update_leases([('items', values[-1].name, 'foreign')])
    with pytest.raises(RuntimeError, match='already active'):
        with lake.items.publication(input_receipt=current) as publisher:
            publisher.publish(values)
    assert records(lake) == before
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute('select dataset,run_id from update_leases').fetchall() == [(values[-1].name, 'foreign')]


def test_existing_read_context_is_not_upgraded(tmp_path):
    lake, current, values, _ = prepared(tmp_path)
    before = records(lake)
    with lake.inputs.read_context(current, verify=False):
        with pytest.raises(Exception, match='existing read view|transaction within a transaction'):
            with lake.items.publication(input_receipt=current) as publisher:
                publisher.publish(values)
    assert records(lake) == before
    with sqlite3.connect(lake.data_meta_path) as db:
        assert not db.execute('select * from update_leases').fetchall()


def test_group_budget_admission_precedes_leases(tmp_path):
    lake, current, values, _ = prepared(tmp_path)
    before = records(lake)
    with pytest.raises(ValueError, match='buffer budget'):
        with lake.items.publication(input_receipt=current, config=ExecutionOptions(max_buffer_bytes=32_000)) as publisher:
            publisher.publish(values)
    assert records(lake) == before


@pytest.mark.parametrize('field', ['active', 'owner', 'thread_id', 'receipt_id', 'connection', 'leases'])
def test_group_capability_cannot_cross_its_owned_scope(tmp_path, monkeypatch, field):
    lake, current, values, _ = prepared(tmp_path)
    before = records(lake)
    sparse = lake.items._publish_sparse
    def corrupt(value, **kwargs):
        group = kwargs['verification'].sparse_group
        assert group is not None
        setattr(group, field, {'active': False, 'owner': None, 'thread_id': -1,
            'receipt_id': 'foreign', 'connection': None, 'leases': {}}[field])
        return sparse(value, **kwargs)
    monkeypatch.setattr(lake.items, '_publish_sparse', corrupt)
    with pytest.raises(RuntimeError, match='active owned'):
        with lake.items.publication(input_receipt=current) as publisher:
            publisher.publish(values)
    assert records(lake) == before
    with sqlite3.connect(lake.data_meta_path) as db:
        assert not db.execute('select * from update_leases').fetchall()


def test_original_output_index_large_receipt_is_admitted_before_decode(tmp_path, monkeypatch):
    import hashlib
    import json
    from bagelquant_data.items import completion
    lake, current, values, _ = prepared(tmp_path, count=2)
    # A valid public output receipt can contain unrelated aliases. The old
    # native format below deliberately lacks the newer inline check bound.
    requests = {value.name: ItemInput(value.name, start="2024-01-01", end="2024-01-02", view="snapshot")
                for value in values}
    requests.update({f"unrelated{index}": ItemInput("leaf", start="2024-01-01", end="2024-01-02", view="snapshot")
                     for index in range(256)})
    original_output = lake.inputs.freeze(requests, information_cutoff=date(2024, 1, 4), retain_batch_bounds=True)
    with sqlite3.connect(lake.data_meta_path) as db:
        for value in values:
            body = json.loads(db.execute("select payload_json from item_completion_authorities where build_id=?",
                                         (value.reuse_build_id,)).fetchone()[0])
            del body["output_check_bound"]
            serialized = json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)
            db.execute("update item_completion_authorities set payload_json=?,digest=? where build_id=?",
                       (serialized, hashlib.sha256(serialized.encode()).hexdigest(), value.reuse_build_id))
    for value in values:
        lake.items.index_completion_output(value.reuse_build_id, output_receipt_id=original_output.receipt_id,
            output_receipt_digest=original_output.digest, report_id="isolated-original-publication",
            report_digest="a" * 64)
    budget = 128 * 1024
    with sqlite3.connect(lake.data_meta_path) as db:
        size = db.execute("select length(cast(payload_json as blob)) from frozen_inputs where receipt_id=?",
                          (original_output.receipt_id,)).fetchone()[0]
    assert size > budget
    before = records(lake)
    actual = completion.json.loads
    def bounded_decode(body, *args, **kwargs):
        if isinstance(body, str) and len(body.encode()) > budget:
            pytest.fail(f"Decoded original-output binding {len(body.encode())} bytes before admission to {budget}-byte budget")
        return actual(body, *args, **kwargs)
    monkeypatch.setattr(completion.json, "loads", bounded_decode)
    with pytest.raises((MemoryError, ValueError), match="budget|buffer"):
        with lake.items.publication(input_receipt=current, config=ExecutionOptions(max_buffer_bytes=budget)) as publisher:
            publisher.publish(values)
    assert records(lake) == before
    with sqlite3.connect(lake.data_meta_path) as db:
        assert not db.execute("select * from update_leases").fetchall()


def test_idempotence_candidate_original_output_is_admitted_before_decode(tmp_path, monkeypatch):
    import hashlib
    import json
    from bagelquant_data.items import completion
    lake, _, values, _ = prepared(tmp_path, count=2)
    original_source = lake.inputs.get(lake.items.completed_builds(values[0].name)[0]["frozen_receipt_id"])
    values = [replace(value, complete_at=original_source.information_cutoff) for value in values]
    with lake.items.publication(input_receipt=original_source) as publisher:
        publisher.publish(values)
    candidates = {value.name: lake.items.completed_builds(value.name)[-1]["id"] for value in values}
    requests = {value.name: ItemInput(value.name, start="2024-01-01", end="2024-01-02", view="snapshot")
                for value in values}
    requests.update({f"unrelated{index}": ItemInput("leaf", start="2024-01-01", end="2024-01-02", view="snapshot")
                     for index in range(256)})
    original_output = lake.inputs.freeze(requests, information_cutoff=original_source.information_cutoff,
                                        retain_batch_bounds=True)
    with sqlite3.connect(lake.data_meta_path) as db:
        for build_id in candidates.values():
            body = json.loads(db.execute("select payload_json from item_completion_authorities where build_id=?",
                                         (build_id,)).fetchone()[0])
            del body["output_check_bound"]
            serialized = json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)
            db.execute("update item_completion_authorities set payload_json=?,digest=? where build_id=?",
                       (serialized, hashlib.sha256(serialized.encode()).hexdigest(), build_id))
    for build_id in candidates.values():
        lake.items.index_completion_output(build_id, output_receipt_id=original_output.receipt_id,
            output_receipt_digest=original_output.digest, report_id="isolated-original-candidate-publication",
            report_digest="b" * 64)
    budget = 128 * 1024
    before = records(lake)
    actual = completion.json.loads
    def bounded_decode(body, *args, **kwargs):
        if isinstance(body, str) and len(body.encode()) > budget:
            pytest.fail(f"Decoded candidate original-output binding {len(body.encode())} bytes before admission")
        return actual(body, *args, **kwargs)
    monkeypatch.setattr(completion.json, "loads", bounded_decode)
    with pytest.raises((MemoryError, ValueError), match="budget|buffer"):
        with lake.items.publication(input_receipt=original_source,
                                    config=ExecutionOptions(max_buffer_bytes=budget)) as publisher:
            publisher.publish(values)
    assert records(lake) == before
    with sqlite3.connect(lake.data_meta_path) as db:
        assert not db.execute("select * from update_leases").fetchall()
