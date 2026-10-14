"""Conditional declaration replacement preserves evidence and concurrent authors."""

from dataclasses import replace
from datetime import date

import polars as pl
import pytest
from bagelquant_data import DataItemSpec, DataLake, ItemInput


def test_original_bound_restore_preserves_all_batches_and_frozen_values(tmp_path):
    with DataLake.open(data_meta_path=tmp_path/'meta.sqlite', lake_path=tmp_path/'lake') as lake:
        original = DataItemSpec('output', metadata={'revision': 1})
        lake.items.register(original)
        old_hash = lake.items.status('output')['definition_hash']
        lake.items.ingest('output', pl.DataFrame({'time': [date(2024, 1, 1)], 'asset_id': ['A'], 'value': [7.]}),
            available_date='2024-01-01')
        frozen = lake.inputs.freeze({'output': ItemInput('output')}, information_cutoff='2024-01-02')
        values = lake.inputs.read(frozen, 'output').collect()
        manifest = lake.items.manifest('output')
        drift = replace(original, metadata={'revision': 2})
        lake.items.register(drift)
        drift_hash = lake.items.status('output')['definition_hash']
        lake.items.register(original, expected_definition_hash=drift_hash)
        assert lake.items.get('output') == original
        assert lake.items.status('output')['definition_hash'] == old_hash
        assert lake.items.manifest('output') == manifest
        assert lake.inputs.get(frozen).digest == frozen.digest
        assert lake.inputs.read(frozen, 'output').collect().equals(values)
        assert not lake.integrity.active_update_leases()


@pytest.mark.parametrize('missing', [False, True])
def test_conditional_register_rejects_stale_or_missing_definition(tmp_path, missing):
    with DataLake.open(data_meta_path=tmp_path/'meta.sqlite', lake_path=tmp_path/'lake') as lake:
        original = DataItemSpec('output', metadata={'revision': 1})
        if not missing:
            lake.items.register(original)
            old_hash = lake.items.status('output')['definition_hash']
            concurrent = replace(original, metadata={'author': 'new'})
            lake.items.register(concurrent)
        else:
            old_hash = 'missing'
        with pytest.raises(RuntimeError, match='definition changed'):
            lake.items.register(original, expected_definition_hash=old_hash)
        if missing:
            with pytest.raises(KeyError):
                lake.items.get('output')
        else:
            assert lake.items.get('output') == concurrent
        assert not lake.integrity.active_update_leases()


def test_conditional_invalid_replacement_rolls_back_original_declaration(tmp_path):
    with DataLake.open(data_meta_path=tmp_path/'meta.sqlite', lake_path=tmp_path/'lake') as lake:
        original = DataItemSpec('output')
        lake.items.register(original)
        expected = lake.items.status('output')['definition_hash']
        with pytest.raises(KeyError, match='missing'):
            lake.items.register(replace(original, inputs=(ItemInput('missing'),)), expected_definition_hash=expected)
        assert lake.items.get('output') == original
        assert lake.items.status('output')['definition_hash'] == expected
        assert not lake.integrity.active_update_leases()
