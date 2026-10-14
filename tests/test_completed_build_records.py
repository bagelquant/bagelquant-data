"""Complete-range authority and consumer publication fences in isolated lakes."""
from datetime import date
import sqlite3
from threading import Event, Thread

import polars as pl
import pytest

from bagelquant_data import ConfigurationError, DataItemSpec, DataLake, ItemInput


def _lake(root):
    return DataLake.open(data_meta_path=root / 'meta.sqlite', lake_path=root / 'lake')


def _rows(day=1):
    return pl.DataFrame({'time': [date(2024, 1, day)], 'asset_id': ['A'], 'value': [float(day)]})


def test_completed_builds_excludes_row_only_and_verifies_original_whole_authority(tmp_path):
    data = _lake(tmp_path)
    data.items.register(DataItemSpec('input'))
    data.items.ingest('input', _rows(), available_date='2024-01-01')
    request = ItemInput('input')
    original = data.inputs.freeze({'input': request}, information_cutoff='2024-01-02')
    data.items.register(DataItemSpec('value', (request,)))
    data.items.ingest('value', _rows(), available_date='2024-01-01', input_receipt=original)
    assert data.items.builds('value') and data.items.completed_builds('value') == []
    data.items.replace_range('value', _rows(), start='2024-01-01', end='2024-01-02',
        available_date='2024-01-02', input_receipt=original)
    completed = data.items.completed_builds('value')
    assert len(completed) == 1 and completed[0]['start_date'] == '2024-01-01'
    assert completed[0]['end_date'] == '2024-01-02'
    assert completed[0]['id'] in {row['id'] for row in data.items.builds('value')}
    with sqlite3.connect(data.data_meta_path) as db:
        db.execute('delete from item_pending_outputs where build_id=?', (completed[0]['id'],))
    with pytest.raises(RuntimeError, match='pending visibility proof is missing'):
        data.items.completed_builds('value')


@pytest.mark.parametrize('raise_inside', [False, True])
def test_publication_guard_fences_other_writers_until_consumer_publication_or_failure(tmp_path, raise_inside):
    data = _lake(tmp_path)
    foreign = _lake(tmp_path)
    data.items.register(DataItemSpec('value'))
    data.items.ingest('value', _rows(), available_date='2024-01-01')
    original = data.inputs.freeze({'value': ItemInput('value')}, information_cutoff='2024-01-03')
    started, completed = Event(), Event()
    failures = []
    def publish():
        started.set()
        try:
            foreign.items.ingest('value', _rows(2), available_date='2024-01-02')
        except Exception as error:
            failures.append(error)
        finally:
            completed.set()
    thread = Thread(target=publish)
    try:
        with data.inputs.publication_guard(original):
            thread.start()
            assert started.wait(1) and not completed.wait(.05)
            assert data.items.status('value')['row_count'] == 1
            assert foreign.inputs.get(original).digest == original.digest
            if raise_inside:
                raise ValueError('consumer failed before publication')
    except ValueError:
        assert raise_inside
    thread.join(3)
    assert completed.is_set() and not failures
    assert data.items.status('value')['row_count'] == 2
    assert data.inputs.get(original).digest == original.digest


def test_publication_guard_cannot_upgrade_readonly_context_or_readonly_lake(tmp_path):
    data = _lake(tmp_path)
    data.items.register(DataItemSpec('value'))
    data.items.ingest('value', _rows(), available_date='2024-01-01')
    original = data.inputs.freeze({'value': ItemInput('value')})
    with data.inputs.read_context(original):
        with pytest.raises(ConfigurationError, match='cannot upgrade'):
            with data.inputs.publication_guard(original):
                pytest.fail('must reject lock upgrade')
        assert data.inputs.get(original).digest == original.digest
    readonly = DataLake.open(data_meta_path=data.data_meta_path, lake_path=data.lake_path, read_only=True)
    with pytest.raises(PermissionError, match='read-only'):
        with readonly.inputs.publication_guard(original):
            pytest.fail('must require writable admission')
