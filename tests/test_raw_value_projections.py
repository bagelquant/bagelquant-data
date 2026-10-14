from datetime import UTC, date, datetime
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, RawInput, input_index


def test_selected_raw_payload_equality_ignores_attestation_only_after_selection(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / 'meta.sqlite', lake_path=tmp_path / 'lake')
    spec = DatasetSpec('prices', 'by_date', field_mappings={'time': 'time', 'asset_id': 'asset_id'}, date_kind='calendar')
    frame = pl.DataFrame({'time': [date(2024, 1, 1)], 'asset_id': ['A'], 'close': [2.]})
    lake.raw.ingest(spec, frame, mode='initialize', ingested_at=datetime(2024, 1, 2, tzinfo=UTC))
    old = lake.inputs.freeze({'prices': RawInput('custom', 'prices', view='versions')}, information_cutoff='2024-01-02')
    lake.raw.ingest(spec, frame, ingested_at=datetime(2024, 1, 3, tzinfo=UTC))
    new = lake.inputs.freeze(old.requests, information_cutoff='2024-01-03')
    from bagelquant_data import inputs
    monkeypatch.setattr(inputs, 'read_batch', lambda *a, **k: pytest.fail('Proof read original bytes'))
    left = lake.inputs.selection_identity(old, 'prices', view='snapshot', values_only=True)
    right = lake.inputs.selection_identity(new, 'prices', view='snapshot', values_only=True)
    assert left is not None and left == right
    assert lake.inputs.index_selection(old, 'prices', view='snapshot') == lake.inputs.index_selection(new, 'prices', view='snapshot')
    assert lake.inputs.selection_identity(old, 'prices', view='snapshot', values_only=True) == left
    assert lake.inputs.selection_identity(new, 'prices', view='snapshot', values_only=True) == right
    monkeypatch.setattr(input_index, 'project_original', lambda *a, **k: pytest.fail('Saved summary decoded row indexes'))
    assert lake.inputs.selection_equivalent(old, new, 'prices', view='snapshot', values_only=True, summaries_only=True)
    assert lake.inputs.get(old).digest == old.digest


@pytest.mark.parametrize('revision', [False, True])
def test_raw_payload_projection_keeps_actual_visibility_and_changed_values(tmp_path, revision):
    lake = DataLake.open(data_meta_path=tmp_path / 'meta.sqlite', lake_path=tmp_path / 'lake')
    spec = DatasetSpec('prices', 'by_date', field_mappings={'time': 'time', 'asset_id': 'asset_id'}, date_kind='calendar')
    frame = pl.DataFrame({'time': [date(2024, 1, 1)], 'asset_id': ['A'], 'close': [2.]})
    lake.raw.ingest(spec, frame, ingested_at=datetime(2024, 1, 2, tzinfo=UTC))
    old = lake.inputs.freeze({'prices': RawInput('custom', 'prices', view='versions')}, information_cutoff='2024-01-02')
    changed = frame.with_columns(pl.lit(3.).alias('close')) if revision else frame.with_columns(pl.lit(date(2024, 1, 3)).alias('time'))
    lake.raw.ingest(spec, changed, ingested_at=datetime(2024, 1, 3, tzinfo=UTC))
    new = lake.inputs.freeze(old.requests, information_cutoff='2024-01-03')
    left = lake.inputs.selection_identity(old, 'prices', end='2024-01-01', view='snapshot', values_only=True)
    right = lake.inputs.selection_identity(new, 'prices', end='2024-01-01', view='snapshot', values_only=True)
    assert left is not None and right is not None and (left == right) is (not revision)
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_index'").fetchone() is None
    assert lake.inputs.selection_identity(old, 'prices', view='snapshot', values_only=True) is not None


def test_raw_payload_projection_cannot_drop_baseline_admission_or_accept_versions(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / 'meta.sqlite', lake_path=tmp_path / 'lake')
    spec = DatasetSpec('prices', 'by_date', field_mappings={'time': 'time', 'asset_id': 'asset_id'}, date_kind='calendar')
    frame = pl.DataFrame({'time': [date(2024, 1, 1)], 'asset_id': ['A'], 'close': [2.]})
    lake.raw.ingest(spec, frame, mode='initialize', ingested_at=datetime(2024, 1, 2, tzinfo=UTC))
    old = lake.inputs.freeze({'prices': RawInput('custom', 'prices', view='versions')}, information_cutoff='2024-01-02')
    assert lake.inputs.selection_identity(old, 'prices', view='snapshot', values_only=True) != lake.inputs.selection_identity(
        old, 'prices', view='snapshot', values_only=True, strict=True)
    with pytest.raises(ValueError, match='snapshot'):
        lake.inputs.selection_identity(old, 'prices', view='versions', values_only=True)


def test_raw_value_window_canonicalizes_only_the_effective_observation_intersection(tmp_path):
    from dataclasses import replace
    lake = DataLake.open(data_meta_path=tmp_path / 'meta.sqlite', lake_path=tmp_path / 'lake')
    spec = DatasetSpec('prices', 'by_date', field_mappings={'time': 'time', 'asset_id': 'asset_id'}, date_kind='calendar')
    frame = pl.DataFrame({'time': [date(2024, 1, 1), date(2024, 1, 3)], 'asset_id': ['A', 'A'], 'close': [2., 3.]})
    lake.raw.ingest(spec, frame, ingested_at=datetime(2024, 1, 3, tzinfo=UTC))
    request = RawInput('custom', 'prices', view='versions', observation_end='2024-01-02')
    old = lake.inputs.freeze({'prices': request}, information_cutoff='2024-01-03')
    new = lake.inputs.freeze({'prices': replace(request, observation_end='2024-01-03')}, information_cutoff='2024-01-03')
    assert lake.inputs.index_selection(old, 'prices', end='2024-01-01', view='snapshot') == lake.inputs.index_selection(new, 'prices', end='2024-01-01', view='snapshot')
    assert lake.inputs.selection_equivalent(old, new, 'prices', end='2024-01-01', view='snapshot', values_only=True, summaries_only=True)
    assert not lake.inputs.selection_equivalent(old, new, 'prices', view='snapshot', values_only=True)


def test_raw_value_summary_audits_original_source_and_rejects_item_coordinates(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / 'meta.sqlite', lake_path=tmp_path / 'lake')
    spec = DatasetSpec('prices', 'by_date', field_mappings={'time': 'time', 'asset_id': 'asset_id'}, date_kind='calendar')
    frame = pl.DataFrame({'time': [date(2024, 1, 1)], 'asset_id': ['A'], 'close': [2.]})
    lake.raw.ingest(spec, frame, ingested_at=datetime(2024, 1, 2, tzinfo=UTC))
    old = lake.inputs.freeze({'prices': RawInput('custom', 'prices', view='versions')}, information_cutoff='2024-01-02')
    with pytest.raises(ValueError, match='Item'):
        lake.inputs.selection_identity(old, 'prices', view='snapshot', values_only=True,
            coordinates=frame.select('time', 'asset_id'))
    assert lake.inputs.index_selection(old, 'prices', view='snapshot') is not None
    assert lake.inputs.verify(old)['valid']
    newer = lake.inputs.freeze(old.requests, information_cutoff='2024-01-02')
    assert lake.inputs.selection_equivalent(old, newer, 'prices', view='snapshot', values_only=True, summaries_only=True)
    # A summary reused by a later source keeps its original audit closure.
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update input_projection_index set source_receipt_digest='corrupt'")
    with pytest.raises(RuntimeError, match='original source'):
        lake.inputs.verify(newer)


@pytest.mark.parametrize('field', ['time', 'year', 'month', 'ingested_at', '_baseline'])
def test_raw_payload_channel_rejects_partition_and_version_fields(tmp_path, field):
    lake = DataLake.open(data_meta_path=tmp_path / 'meta.sqlite', lake_path=tmp_path / 'lake')
    spec = DatasetSpec('prices', 'by_date', field_mappings={'time': 'time', 'asset_id': 'asset_id'}, date_kind='calendar')
    lake.raw.ingest(spec, pl.DataFrame({'time': [date(2024, 1, 1)], 'asset_id': ['A'], 'close': [2.]}),
        ingested_at=datetime(2024, 1, 2, tzinfo=UTC))
    receipt = lake.inputs.freeze({'prices': RawInput('custom', 'prices', fields=(field,))}, information_cutoff='2024-01-02')
    with pytest.raises(ValueError, match='provenance'):
        lake.inputs.selection_identity(receipt, 'prices', values_only=True)
    with pytest.raises(ValueError, match='provenance'):
        lake.inputs.index_selection(receipt, 'prices')
    with pytest.raises(ValueError, match='provenance'):
        lake.inputs.selection_equivalent(receipt, receipt, 'prices', values_only=True)
