"""A visible terminal Item has no upstream creator to resolve through values."""
from datetime import date

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, ItemInput


@pytest.mark.parametrize('view', ['snapshot', 'versions'])
def test_visible_terminal_item_needs_no_selected_value_index(tmp_path, monkeypatch, view):
    lake = DataLake.open(data_meta_path=tmp_path / 'meta.sqlite', lake_path=tmp_path / 'lake')
    lake.items.register(DataItemSpec('terminal'))
    frame = pl.DataFrame({'time': [date(2024, 1, 2)], 'asset_id': ['A'], 'value': [1.]})
    lake.items.ingest('terminal', frame, available_date='2024-01-03')
    receipt = lake.inputs.freeze({'x': ItemInput('terminal', start='2024-01-01',
        end='2024-01-04', view=view)}, information_cutoff='2024-01-04')
    assert not receipt.evidence['x']['parent_receipts']
    assert receipt.evidence['x']['empty_item_build'] is None
    def forbidden(*a, **k):
        pytest.fail('Terminal currentness resolved numerical selections')
    monkeypatch.setattr(lake.inputs, '_selected_item_parents', forbidden)
    from bagelquant_data import inputs
    monkeypatch.setattr(inputs, 'read_batch', forbidden)
    assert lake.inputs.is_current(receipt)
    lake.items.ingest('terminal', frame.with_columns(pl.lit(2.).alias('value')),
        available_date='2024-01-03')
    assert not lake.inputs.is_current(receipt)


@pytest.mark.parametrize('case', ['uninitialized', 'future', 'outside', 'strict_baseline'])
def test_terminal_unknown_or_invisible_selection_retains_fallback(tmp_path, monkeypatch, case):
    lake = DataLake.open(data_meta_path=tmp_path / 'meta.sqlite', lake_path=tmp_path / 'lake')
    lake.items.register(DataItemSpec('terminal'))
    frame = pl.DataFrame({'time': [date(2024, 1, 2)], 'asset_id': ['A'], 'value': [1.]})
    if case != 'uninitialized':
        lake.items.ingest('terminal', frame,
            available_date='2024-01-05' if case == 'future' else '2024-01-03',
            historical_baseline=case == 'strict_baseline')
    request = ItemInput('terminal', start='2024-02-01' if case == 'outside' else '2024-01-01',
        end='2024-02-04' if case == 'outside' else '2024-01-04', view='snapshot',
        strict=case == 'strict_baseline')
    receipt = lake.inputs.freeze({'x': request}, information_cutoff='2024-01-04')
    calls = []
    def unknown(*args, **kwargs):
        calls.append(args[1])
        return None
    monkeypatch.setattr(lake.inputs, '_selected_item_parents', unknown)
    assert not lake.inputs.is_current(receipt)
    assert calls == ['x']
