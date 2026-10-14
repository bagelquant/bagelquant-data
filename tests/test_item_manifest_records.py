"""Partition manifests preserve original metadata without per-partition queries."""
from datetime import date

import polars as pl

from bagelquant_data import DataItemSpec, DataLake


def test_manifest_batches_partition_metadata_without_reading_original_values(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / 'meta.sqlite', lake_path=tmp_path / 'lake')
    lake.items.register(DataItemSpec('sample'))
    lake.items.register(DataItemSpec('other'))
    dates = [date(2024, month, 2) for month in range(1, 13)]
    rows = pl.DataFrame({'time': dates, 'asset_id': ['A'] * 12, 'value': [float(month) for month in range(12)]})
    for day in dates:
        lake.items.ingest('sample', rows.filter(pl.col('time') == day), available_date=day)
    lake.items.ingest('sample', rows.head(1).with_columns(pl.lit(None, dtype=pl.Float64).alias('value')),
        available_date='2024-12-04')
    lake.items.ingest('other', rows, available_date='2024-12-05')
    expected = []
    for manifest in lake._data_meta.manifest('items', 'sample'):
        with lake._data_meta.connect() as connection:
            commit = dict(connection.execute(
                "select max(b.commit_seq) as commit_seq,min(b.min_observation) as minimum_time,"
                "max(b.max_observation) as maximum_time from version_segments b "
                "join version_commits c on c.seq=b.commit_seq where c.source='items' and c.dataset=? "
                "and c.status='committed' and b.partition_path=?", ('sample', manifest['partition_path'])).fetchone())
            definition = connection.execute("select spec_hash from item_definitions where name='sample'").fetchone()[0]
        expected.append({**manifest, **commit, 'definition_hash': definition})
    calls = []
    read_rows = lake._data_meta._rows
    def counted(sql, params=()):
        calls.append(sql)
        assert 'b.payload' not in sql and 'select b.*' not in sql
        return read_rows(sql, params)
    monkeypatch.setattr(lake._data_meta, '_rows', counted)
    assert lake.items.manifest('sample') == expected
    assert len(calls) == 3  # declaration, definition identity, one joined manifest
    assert len(expected) == 13
    calls.clear()
    lake.items.register(DataItemSpec('empty'))
    calls.clear()
    assert lake.items.manifest('empty') == []
    assert len(calls) == 3
