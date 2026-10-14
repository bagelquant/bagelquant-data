from datetime import date
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataLake, DataItemSpec, ItemInput


def test_causal_versions_ignore_redundant_creators_and_preserve_pit_value_events(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("value"))
    frame = pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "value": [1.], "creator_tag": [1]})
    lake.items.ingest("value", frame, available_date="2024-01-01")
    request = {"value": ItemInput("value", view="versions")}
    old = lake.inputs.freeze(request, information_cutoff="2024-01-03")
    def proof(receipt):
        return lake.inputs.selection_identity(receipt, "value", values_only=True, causal_versions=True)
    original = proof(old)
    assert original is not None
    lake.items.ingest("value", frame.with_columns(pl.lit(2).alias("creator_tag")), available_date="2024-01-01")
    duplicate = lake.inputs.freeze(request, information_cutoff="2024-01-03")
    assert not lake.inputs.read(old, "value").collect().equals(lake.inputs.read(duplicate, "value").collect())
    from bagelquant_data import inputs
    def forbidden(*args, **kwargs):
        raise AssertionError("Causal scalar proof consumed original bytes")
    with monkeypatch.context() as patch:
        patch.setattr(inputs, "read_batch", forbidden)
        assert proof(duplicate) == original
    # A new availability event remains visible, even for an equal scalar: it
    # carries information timing that a causal consumer may use.
    lake.items.ingest("value", frame.with_columns(pl.lit(3).alias("creator_tag")), available_date="2024-01-02")
    timed = lake.inputs.freeze(request, information_cutoff="2024-01-03")
    assert proof(timed) != original
    lake.items.ingest("value", frame.with_columns(pl.lit(9.).alias("value")), available_date="2024-01-02")
    revised = lake.inputs.freeze(request, information_cutoff="2024-01-03")
    assert proof(revised) != proof(timed)
    lake.inputs.verify((old, duplicate, timed, revised))


def test_causal_range_identity_requires_no_row_index_and_rejects_wrong_views(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("value"))
    lake.items.ingest("value", pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "value": [1.]}))
    frozen = lake.inputs.freeze({"value": ItemInput("value", view="versions")}, information_cutoff="2024-01-03")
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_value_index'").fetchone() is None
    def forbidden(*args, **kwargs):
        raise AssertionError("Missing causal index triggered implicit maintenance or byte reads")
    from bagelquant_data import inputs
    monkeypatch.setattr(inputs, "read_batch", forbidden)
    assert not hasattr(lake.inputs, "index_values")
    assert lake.inputs.selection_identity(frozen, "value", values_only=True, causal_versions=True) is not None
    with pytest.raises(ValueError, match="scalar versions"):
        lake.inputs.selection_identity(frozen, "value", values_only=True, causal_versions=True, view="snapshot")
