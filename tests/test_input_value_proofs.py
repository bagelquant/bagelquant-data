"""Consumed scalar proofs preserve provenance without invalidating equal values."""
from datetime import UTC, date, datetime

import polars as pl
import pytest

from bagelquant_data import DataLake, DataItemSpec, DatasetSpec, ItemInput, RawInput
from bagelquant_data import input_index, inputs


def test_value_proof_reuses_changed_creator_receipt_and_detects_scalar_revision(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    raw = DatasetSpec("quotes", "by_date", date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"})
    raw_input = RawInput("custom", "quotes", alias="quotes", view="versions")
    frame = pl.DataFrame({"time": [date(2024, 1, 2)], "asset_id": ["A"], "value": [10.]})
    lake.raw.ingest(raw, frame, mode="initialize", ingested_at=datetime(2024, 1, 2, tzinfo=UTC))
    lake.items.register(DataItemSpec("price", inputs=(raw_input,)))
    first_inputs = lake.inputs.freeze({"quotes": raw_input}, information_cutoff="2024-01-02")
    lake.items.ingest("price", frame, input_receipt=first_inputs, available_date="2024-01-02")
    request = {"price": ItemInput("price", view="snapshot")}
    old = lake.inputs.freeze(request, information_cutoff="2024-01-02")
    original = lake.inputs.selection_identity(old, "price", values_only=True)
    assert original is not None
    old_provenance = lake.inputs.request_identity(old, "price")
    lake.raw.ingest(raw, frame.with_columns(pl.lit(date(2024, 1, 3)).alias("time")), ingested_at=datetime(2024, 1, 3, tzinfo=UTC))
    second_inputs = lake.inputs.freeze({"quotes": raw_input}, information_cutoff="2024-01-03")
    assert first_inputs.dependency_digest != second_inputs.dependency_digest
    lake.items.ingest("price", frame, input_receipt=second_inputs, available_date="2024-01-03")
    new = lake.inputs.freeze(request, information_cutoff="2024-01-03")
    def forbidden(*_args, **_kwargs):
        raise AssertionError("ordinary value proof consumed original batches")
    with monkeypatch.context() as patch:
        patch.setattr(inputs, "read_batch", forbidden)
        assert lake.inputs.selection_identity(new, "price", values_only=True) == original
        assert lake.inputs.request_identity(new, "price") != old_provenance
    lake.items.ingest("price", frame.with_columns(pl.lit(11.).alias("value")), input_receipt=second_inputs, available_date="2024-01-03")
    revised = lake.inputs.freeze(request, information_cutoff="2024-01-03")
    assert lake.inputs.selection_identity(revised, "price", values_only=True) != original
    assert lake.inputs.read(old, "price").collect()["value"].to_list() == [10.]
    assert lake.inputs.read(revised, "price").collect()["value"].to_list() == [11.]
    lake.inputs.verify((old, new, revised))


def test_optional_exact_preparation_preserves_range_token_and_audits_original(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("value"))
    lake.items.ingest("value", pl.DataFrame({"time": [date(2024, 1, 2)], "asset_id": ["A"], "value": [-0.]}), available_date="2024-01-02")
    frozen = lake.inputs.freeze({"item": ItemInput("value", view="snapshot")}, information_cutoff="2024-01-02")
    original = lake.inputs.selection_identity(frozen, "item", values_only=True)
    def forbidden(*_args, **_kwargs):
        raise AssertionError("ordinary metadata proof triggered original transport")
    with monkeypatch.context() as patch:
        patch.setattr(input_index, "project_original", forbidden)
        assert lake.inputs.selection_identity(frozen, "item", values_only=True) == original
    assert lake.inputs.index_selection(frozen, "item") is not None
    assert lake.inputs.selection_identity(frozen, "item", values_only=True) == original
    assert lake.inputs.get(frozen).digest == frozen.digest
    assert lake.inputs.verify(frozen)["valid"]
    from storage_fixtures import damage_original_segments
    with lake.inputs._store.connect() as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_value_index'").fetchone() is None
        damage_original_segments(db)
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        lake.inputs.verify(frozen)


@pytest.mark.parametrize("before,after", [(0., -0.), (float("nan"), None), (1., None)])
def test_value_proofs_preserve_typed_float_and_withdrawal_differences(tmp_path, before, after):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("value"))
    def publish(value, cutoff):
        frame = pl.DataFrame({"time": [date(2024, 1, 2)], "asset_id": ["A"], "value": pl.Series([value], dtype=pl.Float64)})
        lake.items.ingest("value", frame, available_date=cutoff)
        frozen = lake.inputs.freeze({"item": ItemInput("value", view="snapshot")}, information_cutoff=cutoff)
        return lake.inputs.selection_identity(frozen, "item", values_only=True)
    assert publish(before, "2024-01-02") != publish(after, "2024-01-03")


def test_consumed_coordinates_ignore_other_assets_and_preserve_missing_null_and_withdrawal(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("value"))
    d1, d2 = date(2024, 1, 2), date(2024, 1, 3)
    frame = pl.DataFrame({"time": [d1, d2, d1], "asset_id": ["A", "B", "C"], "value": [1., 2., 3.]})
    lake.items.ingest("value", frame, available_date=d2)
    request = {"item": ItemInput("value", start=d1, end=d2, view="snapshot")}
    coordinates = pl.DataFrame({"time": [d1, d2, d1], "asset_id": ["A", "B", "missing"]})
    old = lake.inputs.freeze(request, information_cutoff=d2)
    original = lake.inputs.selection_identity(old, "item", values_only=True, coordinates=coordinates)
    assert original is not None
    exact = lake.inputs.index_selection(old, "item", coordinates=coordinates)
    lake.items.ingest("value", frame.filter(pl.col("asset_id") == "C").with_columns(pl.lit(30.).alias("value")),
        available_date="2024-01-04")
    newer = lake.inputs.freeze(request, information_cutoff="2024-01-04")
    def forbidden(*args, **kwargs):
        pytest.fail("coordinate proof read original numerical batches")
    with monkeypatch.context() as patch:
        patch.setattr(inputs, "read_batch", forbidden)
        assert lake.inputs.selection_identity(newer, "item", values_only=True, coordinates=coordinates.reverse()) == original
        assert lake.inputs.selection_identity(newer, "item", values_only=True) != lake.inputs.selection_identity(old, "item", values_only=True)
    # The dynamic domain excludes A on d2 and B on d1; those revisions are irrelevant.
    lake.items.ingest("value", pl.DataFrame({"time": [d2, d1], "asset_id": ["A", "B"], "value": [8., 9.]}),
        available_date="2024-01-05")
    irrelevant = lake.inputs.freeze(request, information_cutoff="2024-01-05")
    # Compact change boxes are conservative for a sparse dynamic Domain.
    # An explicit projection proves the skipped diagonal keys numerically equal.
    assert lake.inputs.index_selection(irrelevant, "item", coordinates=coordinates) == exact
    assert lake.inputs.selection_equivalent(old, irrelevant, "item", coordinates=coordinates, values_only=True, summaries_only=True) is True
    # A previously absent consumed key, including a typed null, changes proof.
    lake.items.ingest("value", pl.DataFrame({"time": [d1], "asset_id": ["missing"],
        "value": pl.Series([None], dtype=pl.Float64)}), available_date="2024-01-06")
    nullable = lake.inputs.freeze(request, information_cutoff="2024-01-06")
    assert lake.inputs.selection_identity(nullable, "item", values_only=True, coordinates=coordinates) != original
    lake.items.ingest("value", frame.filter(pl.col("asset_id") == "A").with_columns(pl.lit(None, dtype=pl.Float64).alias("value")),
        available_date="2024-01-07")
    withdrawn = lake.inputs.freeze(request, information_cutoff="2024-01-07")
    assert lake.inputs.selection_identity(withdrawn, "item", values_only=True, coordinates=coordinates) != original
    with pytest.raises(ValueError, match="unique non-null"):
        lake.inputs.selection_identity(withdrawn, "item", values_only=True, coordinates=pl.concat([coordinates, coordinates]))
    with pytest.raises(ValueError, match="cannot widen"):
        lake.inputs.selection_identity(withdrawn, "item", values_only=True,
            coordinates=coordinates.with_columns(pl.lit(date(2024, 1, 1)).alias("time")))
    with pytest.raises(ValueError, match="scalar projection"):
        lake.inputs.selection_identity(withdrawn, "item", coordinates=coordinates)
