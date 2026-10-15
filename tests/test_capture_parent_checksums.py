"""Successful original metadata checks expire with their actual fixed view."""
import pytest

from tests.test_capture_header_context import lake_roots as lake_roots


def test_same_parent_checked_once_then_next_view_rechecks(lake_roots):
    lake, roots = lake_roots
    statements = []
    for _ in range(2):
        with lake.inputs.read_context(roots):
            with lake.inputs._store.connect() as db:
                db.set_trace_callback(statements.append)
                for _ in range(5):
                    assert lake.inputs._capture_parent_digest(db, roots[0].receipt_id) == roots[0].digest
            cache = lake.inputs._read_context().capture_parents
            assert cache
        assert cache == {}
    assert sum(query.startswith('select payload_json,digest from frozen_inputs') for query in statements) == 2


@pytest.mark.parametrize('change', ['digest', 'payload', 'delete'])
def test_guard_own_write_invalidates_parent_checksum(lake_roots, change):
    lake, roots = lake_roots
    with lake.inputs.publication_guard(roots):
        with lake.inputs._store.connect() as db:
            assert lake.inputs._capture_parent_digest(db, roots[0].receipt_id) == roots[0].digest
            if change == 'delete':
                db.execute('delete from frozen_inputs where receipt_id=?', (roots[0].receipt_id,))
            else:
                field = 'digest' if change == 'digest' else 'payload_json'
                value = 'damaged' if change == 'digest' else '{"damaged": true}'
                db.execute(f'update frozen_inputs set {field}=? where receipt_id=?', (value, roots[0].receipt_id))
            with pytest.raises(RuntimeError, match='checksum|missing'):
                lake.inputs._capture_parent_digest(db, roots[0].receipt_id)


def test_missing_parent_is_not_cached_and_can_become_available(lake_roots):
    lake, roots = lake_roots
    restored = lake.inputs.freeze(roots[0].requests, information_cutoff="2024-03-02")
    with lake.inputs._store.connect() as db:
        original = tuple(db.execute("select * from frozen_inputs where receipt_id=?", (restored.receipt_id,)).fetchone())
        db.execute("delete from frozen_inputs where receipt_id=?", (restored.receipt_id,))
    with lake.inputs.publication_guard(roots):
        with lake.inputs._store.connect() as db:
            with pytest.raises(RuntimeError, match='missing'):
                lake.inputs._capture_parent_digest(db, restored.receipt_id)
            assert not lake.inputs._read_context().capture_parents
            db.execute('insert into frozen_inputs values(?,?,?,?)', original)
            assert lake.inputs._capture_parent_digest(db, restored.receipt_id) == restored.digest


def test_changed_budget_does_not_borrow_checksum_memo(lake_roots):
    lake, roots = lake_roots
    statements = []
    with lake.inputs.read_context(roots):
        with lake.inputs._store.connect() as db:
            db.set_trace_callback(statements.append)
            assert lake.inputs._capture_parent_digest(db, roots[0].receipt_id) == roots[0].digest
            assert lake.inputs._capture_parent_digest(db, roots[0].receipt_id, max_buffer_bytes=1024) == roots[0].digest
    assert sum(query.startswith('select payload_json,digest from frozen_inputs') for query in statements) == 2


def test_checksum_hit_checks_cancellation(lake_roots):
    lake, roots = lake_roots
    cancel = {'requested': False}
    def check():
        if cancel['requested']:
            raise RuntimeError('requested cancellation')
    with pytest.raises(RuntimeError, match='requested cancellation'):
        with lake.inputs.read_context(roots, check_canceled=check):
            with lake.inputs._store.connect() as db:
                lake.inputs._capture_parent_digest(db, roots[0].receipt_id)
                cancel['requested'] = True
                lake.inputs._capture_parent_digest(db, roots[0].receipt_id)
