"""Optional metadata proofs never silently fall back to original numerical IPC."""
from concurrent.futures import ThreadPoolExecutor
from datetime import date

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, ItemInput, RawInput
from bagelquant_data import inputs
from test_currentness_lineage import prepared






def test_selection_ignores_unrelated_alias_and_matches_consumed_view(tmp_path):
    lake = prepared(tmp_path)
    item = ItemInput("item", view="versions")
    first = lake.inputs.freeze({"item": item}, information_cutoff="2020-03-03")
    second = lake.inputs.freeze({"item": item, "raw": RawInput("custom", "raw")}, information_cutoff="2020-03-03")
    assert first.digest != second.digest
    assert lake.inputs.selection_identity(first, "item", view="snapshot") == lake.inputs.selection_identity(second, "item", view="snapshot")
    assert lake.inputs.selection_identity(first, "item", view="snapshot") != lake.inputs.selection_identity(first, "item", view="versions")


def test_request_identity_is_original_metadata_and_index_independent(tmp_path, monkeypatch):
    lake = prepared(tmp_path)
    request = ItemInput("item", view="versions")
    first = lake.inputs.freeze({"item": request}, information_cutoff="2020-03-03")
    second = lake.inputs.freeze({"item": request, "raw": RawInput("custom", "raw")},
                                information_cutoff="2020-03-03")
    token = lake.inputs.request_identity(first, "item")
    assert token == lake.inputs.request_identity(second, "item")
    with lake._data_meta.connect() as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_index'").fetchone() is None
        db.execute("DELETE FROM input_selection_index")
    monkeypatch.setattr(inputs, "read_batch", lambda *a, **k: pytest.fail("identity read original values"))
    assert lake.inputs.selection_identity(first, "item") is not None
    assert lake.inputs.request_identity(first, "item") == token
    with lake.inputs.read_context(first):
        assert lake.inputs.request_identity(first, "item") == token
        # Supplied evidence never becomes authority, including memo hits.
        from dataclasses import replace
        assert lake.inputs.request_identity(replace(first, evidence={}), "item") == token
        with pytest.raises(RuntimeError, match="checksum"):
            lake.inputs.request_identity(replace(first, digest="invented"), "item")
    assert lake.inputs.selection_identity(first, "item") is not None
    assert lake.inputs.request_identity(first, "item") == token
    narrower = lake.inputs.freeze({"item": ItemInput("item", start="2020-03-01")},
                                  information_cutoff="2020-03-03")
    assert lake.inputs.request_identity(narrower, "item") != token
    with pytest.raises(KeyError, match="alias"):
        lake.inputs.request_identity(first, "missing")








def test_worker_metadata_borrowing_expires_with_source(tmp_path, monkeypatch):
    lake = prepared(tmp_path)
    frozen = lake.inputs.freeze({"item": ItemInput("item")}, information_cutoff="2020-03-03")
    original = inputs.json.loads
    loaded = []
    def decode(value, *args, **kwargs):
        if '"requests"' in value:
            loaded.append(value)
        return original(value, *args, **kwargs)
    monkeypatch.setattr(inputs.json, "loads", decode)
    child = DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path, read_only=True)
    with lake.inputs.read_context(frozen):
        before = len(loaded)
        def work():
            with child.inputs.read_context(frozen, source=lake.inputs):
                return child.inputs.get(frozen).digest
        with ThreadPoolExecutor(max_workers=1) as pool:
            assert pool.submit(work).result() == frozen.digest
        assert len(loaded) == before
    with pytest.raises(ValueError, match="source"):
        with child.inputs.read_context(frozen, source=lake.inputs):
            pass


def test_unselected_historical_revision_preserves_snapshot_selection_identity(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("published"))
    rows = pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.]})
    lake.items.ingest("published", rows, available_date="2020-01-02")
    lake.items.ingest("published", rows.with_columns(pl.lit(3.).alias("value")),
                      available_date="2020-01-03")
    requests = {"item": ItemInput("published", view="versions")}
    first = lake.inputs.freeze(requests, information_cutoff="2020-01-04")
    token = lake.inputs.selection_identity(first, "item", view="snapshot")
    assert token is not None
    assert lake.inputs.read(first, "item", view="snapshot").collect()["value"].to_list() == [3.]

    # This new immutable version is present in the forest, but the consumer
    # still selects January 3. Its scoped numerical identity must stay equal.
    lake.items.ingest("published", rows.with_columns(pl.lit(2.).alias("value")),
                      available_date="2020-01-02")
    second = lake.inputs.freeze(requests, information_cutoff="2020-01-04")
    assert first.digest != second.digest
    # Range tokens conservatively capture any in-range historical change.
    # Explicit exact projection can still prove the selected scalar unchanged.
    assert lake.inputs.index_selection(first, "item", view="snapshot") == lake.inputs.index_selection(second, "item", view="snapshot")
    assert lake.inputs.selection_equivalent(first, second, "item", view="snapshot", values_only=True, summaries_only=True) is True
    assert lake.inputs.read(second, "item", view="snapshot").collect()["value"].to_list() == [3.]

    # A genuinely selected revision must invalidate the same consumer token.
    lake.items.ingest("published", rows.with_columns(pl.lit(4.).alias("value")),
                      available_date="2020-01-04")
    third = lake.inputs.freeze(requests, information_cutoff="2020-01-04")
    assert token != lake.inputs.selection_identity(third, "item", view="snapshot")
    assert lake.inputs.read(third, "item", view="snapshot").collect()["value"].to_list() == [4.]
