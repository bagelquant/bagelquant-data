"""Header reuse preserves original requests and the actual SQLite snapshot."""
from datetime import UTC, date, datetime
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, ExecutionOptions, RawInput


@pytest.fixture
def lake_roots(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / 'meta.sqlite', lake_path=tmp_path / 'lake')
    spec = DatasetSpec('raw', 'by_date', date_kind='calendar',
        field_mappings={'time': 'time', 'asset_id': 'asset_id'})
    values = pl.DataFrame({'time': [date(2024, 1, 2), date(2024, 2, 2)],
        'asset_id': ['A', 'A'], 'value': [1., 2.]})
    lake.raw.ingest(spec, values, ingested_at=datetime(2024, 3, 1, tzinfo=UTC))
    roots = [lake.inputs.freeze({'x': RawInput('custom', 'raw', start=f'2024-0{month}-01',
        end=f'2024-0{month}-28', view='versions')}, information_cutoff='2024-03-01') for month in (1, 2)]
    return lake, roots


def statements_for_batch_headers(statements):
    return [query for query in statements if query.startswith('select b.commit_seq,b.partition_path')]


def test_distinct_windows_share_headers_only_in_one_view(lake_roots):
    lake, roots = lake_roots
    statements = []
    with lake.inputs.read_context(roots):
        with lake.inputs._store.connect() as db:
            db.set_trace_callback(statements.append)
            assert lake.inputs.is_current(roots)
            assert len(statements_for_batch_headers(statements)) == 1
        cache = lake.inputs._read_context().capture_batches
        assert cache
    assert cache == {}
    with lake.inputs.read_context(roots):
        with lake.inputs._store.connect() as db:
            db.set_trace_callback(statements.append)
            assert lake.inputs.is_current(roots)
    assert len(statements_for_batch_headers(statements)) == 2


def test_writable_guard_own_changes_invalidate_same_boundary_headers(lake_roots):
    lake, roots = lake_roots
    with lake.inputs.publication_guard(roots):
        with lake.inputs._store.connect() as db:
            boundary = roots[0].max_commit
            first = lake.inputs._capture_batch_headers(db, 'custom', 'raw', boundary)
            db.execute("update version_segments set content_hash=? where commit_seq=?", ('changed', boundary))
            second = lake.inputs._capture_batch_headers(db, 'custom', 'raw', boundary)
            assert first[0]['content_hash'] != second[0]['content_hash']
            # Restore this isolated fixture; neither lookup changes data.
            db.execute('update version_segments set content_hash=? where commit_seq=?',
                (first[0]['content_hash'], boundary))


def test_foreign_connection_cannot_borrow_view_headers(lake_roots):
    lake, roots = lake_roots
    with lake.inputs.read_context(roots):
        with lake.inputs._store.connect() as db:
            first = lake.inputs._capture_batch_headers(db, 'custom', 'raw', roots[0].max_commit)
        with sqlite3.connect(':memory:') as foreign:
            foreign.row_factory = sqlite3.Row
            with pytest.raises(sqlite3.OperationalError, match='no such table'):
                lake.inputs._capture_batch_headers(foreign, 'custom', 'raw', roots[0].max_commit)
        assert first


def test_different_budget_does_not_reuse_header_cache(lake_roots):
    lake, roots = lake_roots
    statements = []
    with lake.inputs.read_context(roots):
        with lake.inputs._store.connect() as db:
            db.set_trace_callback(statements.append)
            lake.inputs._capture_batch_headers(db, 'custom', 'raw', roots[0].max_commit)
            lake.inputs._capture_batch_headers(db, 'custom', 'raw', roots[0].max_commit,
                max_buffer_bytes=1024)
        assert len(statements_for_batch_headers(statements)) == 2


def test_tiny_reserve_does_not_store_headers(lake_roots):
    lake, roots = lake_roots
    options = ExecutionOptions(max_buffer_bytes=32768)
    with lake.inputs.read_context(roots, config=options):
        with lake.inputs._store.connect() as db:
            assert lake.inputs._capture_batch_headers(db, 'custom', 'raw', roots[0].max_commit)
        assert not lake.inputs._read_context().capture_batches


def test_cache_hit_checks_cancellation(lake_roots):
    lake, roots = lake_roots
    cancel = {'requested': False}
    def check():
        if cancel['requested']:
            raise RuntimeError('requested cancellation')
    with pytest.raises(RuntimeError, match='requested cancellation'):
        with lake.inputs.read_context(roots, check_canceled=check):
            with lake.inputs._store.connect() as db:
                lake.inputs._capture_batch_headers(db, 'custom', 'raw', roots[0].max_commit)
                cancel['requested'] = True
                lake.inputs._capture_batch_headers(db, 'custom', 'raw', roots[0].max_commit)
