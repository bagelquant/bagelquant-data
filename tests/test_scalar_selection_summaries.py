from datetime import date
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataLake, DataItemSpec, ItemInput, ExecutionOptions
from bagelquant_data import input_index


def setup(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("value"))
    frame = pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "value": [1.], "creator": [1]})
    lake.items.ingest("value", frame, available_date="2024-01-01")
    request = {"value": ItemInput("value", view="versions")}
    old = lake.inputs.freeze(request, information_cutoff="2024-01-03", retain_batch_bounds=True)
    return lake, frame, request, old


def test_scalar_summary_survives_unrelated_commits_without_row_index_decode(tmp_path, monkeypatch):
    lake, frame, request, old = setup(tmp_path)
    old_proof = lake.inputs.index_selection(old, "value", causal_versions=True)
    lake.items.ingest("value", frame.with_columns(pl.lit(2).alias("creator")), available_date="2024-01-01")
    duplicate = lake.inputs.freeze(request, information_cutoff="2024-01-03", retain_batch_bounds=True)
    assert lake.inputs.index_selection(duplicate, "value", causal_versions=True) == old_proof
    lake.items.register(DataItemSpec("unrelated"))
    lake.items.ingest("unrelated", frame.drop("creator"), available_date="2024-01-01")
    def forbidden(*args, **kwargs):
        raise AssertionError("Normal proof decoded per-row indexes or built missing summaries")
    monkeypatch.setattr(input_index, "project_original", forbidden)
    monkeypatch.setattr(lake.inputs, "index_selection", forbidden)
    monkeypatch.setattr(lake.inputs, "freeze", forbidden)
    assert lake.inputs.selection_equivalent(old, None, "value", values_only=True,
        causal_versions=True, summaries_only=True) is True


def test_new_scalar_event_requires_new_summary_and_then_proves_changed(tmp_path, monkeypatch):
    lake, frame, request, old = setup(tmp_path)
    lake.inputs.index_selection(old, "value", causal_versions=True)
    lake.items.ingest("value", frame.with_columns(pl.lit(9.).alias("value")), available_date="2024-01-02")
    revised = lake.inputs.freeze(request, information_cutoff="2024-01-03", retain_batch_bounds=True)
    def forbidden(*args, **kwargs):
        raise AssertionError("Unknown canonical summary decoded per-row indexes")
    with monkeypatch.context() as patch:
        patch.setattr(input_index, "project_original", forbidden)
        assert lake.inputs.selection_equivalent(old, revised, "value", values_only=True,
            causal_versions=True, summaries_only=True) is None
    assert lake.inputs.index_selection(revised, "value", causal_versions=True) is not None
    with monkeypatch.context() as patch:
        patch.setattr(input_index, "project_original", forbidden)
        assert lake.inputs.selection_equivalent(old, revised, "value", values_only=True,
            causal_versions=True, summaries_only=True) is False
    # Both cutoffs see the complete event stream, so its recorded summary can
    # already prove this selected historical change at the earlier date.
    assert lake.inputs.selection_equivalent(old, revised, "value", as_of="2024-01-02",
        current_as_of="2024-01-02", values_only=True, causal_versions=True, summaries_only=True) is False
    # Captured bounds separately prove the later batch invisible at Jan 1.
    assert lake.inputs.selection_equivalent(old, revised, "value", as_of="2024-01-01",
        current_as_of="2024-01-01", values_only=True, causal_versions=True, summaries_only=True) is True
    lake.inputs.verify((old, revised))


def test_complete_scalar_summary_survives_cutoff_advance_and_audits_original(tmp_path, monkeypatch):
    lake, _, request, old = setup(tmp_path)
    lake.inputs.index_selection(old, "value", causal_versions=True)
    later = lake.inputs.freeze(request, information_cutoff="2024-01-05", retain_batch_bounds=True)
    with monkeypatch.context() as patch:
        patch.setattr(input_index, "project_original", lambda *a, **k: pytest.fail("cutoff-only change decoded row indexes"))
        assert lake.inputs.selection_equivalent(old, later, "value", values_only=True,
            causal_versions=True, summaries_only=True) is True
        assert lake.inputs.selection_equivalent(old, None, "value", current_as_of="2024-01-07", values_only=True,
            causal_versions=True, summaries_only=True) is True
    assert lake.inputs.verify(later)["valid"]


def test_future_event_is_not_reused_by_complete_cutoff_proof(tmp_path, monkeypatch):
    lake, frame, request, old = setup(tmp_path)
    lake.inputs.index_selection(old, "value", causal_versions=True)
    lake.items.ingest("value", frame.with_columns(pl.lit(9.).alias("value")), available_date="2024-01-06")
    current = lake.inputs.freeze(request, information_cutoff="2024-01-07", retain_batch_bounds=True)
    lake.inputs.index_selection(current, "value", causal_versions=True)
    with monkeypatch.context() as patch:
        patch.setattr(input_index, "project_original", lambda *a, **k: pytest.fail("proof decoded row indexes"))
        assert lake.inputs.selection_equivalent(old, current, "value", current_as_of="2024-01-05",
            values_only=True, causal_versions=True, summaries_only=True) is True
        assert lake.inputs.selection_equivalent(old, current, "value", current_as_of="2024-01-07",
            values_only=True, causal_versions=True, summaries_only=True) is False
    assert lake.inputs.verify(current)["valid"]


def test_empty_scalar_window_uses_original_bounds_without_summary_or_row_decode(tmp_path, monkeypatch):
    lake, _, request, _ = setup(tmp_path)
    old = lake.inputs.freeze(request, information_cutoff="2024-02-03", retain_batch_bounds=True)
    current = lake.inputs.freeze(request, information_cutoff="2024-02-05", retain_batch_bounds=True)
    def forbidden(*args, **kwargs):
        pytest.fail("empty record-only proof consumed or built a row index")
    with monkeypatch.context() as patch:
        patch.setattr(input_index, "project_original", forbidden)
        patch.setattr(lake.inputs, "index_selection", forbidden)
        assert lake.inputs.selection_equivalent(old, current, "value", start="2024-02-01", end="2024-02-02",
            values_only=True, causal_versions=True, summaries_only=True) is True
    # All schema 8 receipts retain physical range bounds.
    legacy = lake.inputs.freeze(request, information_cutoff="2024-02-03")
    with monkeypatch.context() as patch:
        patch.setattr(input_index, "project_original", forbidden)
        assert lake.inputs.selection_equivalent(legacy, current, "value", start="2024-02-01", end="2024-02-02",
            values_only=True, causal_versions=True, summaries_only=True) is True


def test_summary_audit_rejects_self_consistent_wrong_identity(tmp_path):
    lake, _, _, old = setup(tmp_path)
    lake.inputs.index_selection(old, "value", causal_versions=True)
    with sqlite3.connect(lake.data_meta_path) as db:
        db.row_factory = sqlite3.Row
        row = dict(db.execute("select * from input_projection_index").fetchone())
        row["identity"] = "0" * 64
        digest = input_index._digest({key: value for key, value in row.items() if key != "digest"})
        db.execute("update input_projection_index set identity=?,digest=?", (row["identity"], digest))
    with pytest.raises(RuntimeError, match="Scalar selection summary differs from original evidence"):
        lake.inputs.verify(old)


def test_missing_scalar_rows_remain_unknown_without_index_maintenance(tmp_path):
    lake, _, _, old = setup(tmp_path)
    expected = lake.inputs.selection_identity(old, "value", values_only=True, causal_versions=True)
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_value_index'").fetchone() is None
    assert lake.inputs.selection_identity(old, "value", values_only=True, causal_versions=True) == expected
    assert lake.inputs.index_selection(old, "value", causal_versions=True) is not None
    assert lake.inputs.selection_identity(old, "value", values_only=True, causal_versions=True) == expected
    with pytest.raises(ValueError, match="causal_versions"):
        lake.inputs.index_selection(old, "value")


def test_projection_lookup_binds_original_source_and_checks_digest(tmp_path):
    lake, _, _, old = setup(tmp_path)
    lake.inputs.index_selection(old, "value", causal_versions=True)
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update input_projection_index set identity=?", ("0" * 64,))
    with pytest.raises(RuntimeError, match="summary is damaged"):
        lake.inputs.verify(old)


def test_summary_cannot_rescue_damaged_original_source_metadata(tmp_path):
    lake, frame, request, old = setup(tmp_path)
    lake.inputs.index_selection(old, "value", causal_versions=True)
    # An equivalent current root may find the old source-bound summary, but
    # its original registration must still have its full recorded checksum.
    lake.items.register(DataItemSpec("unrelated"))
    lake.items.ingest("unrelated", frame.drop("creator"), available_date="2024-01-01")
    current = lake.inputs.freeze(request, information_cutoff="2024-01-03", retain_batch_bounds=True)
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update frozen_inputs set payload_json='{}' where receipt_id=?", (old.receipt_id,))
    with pytest.raises(RuntimeError, match="receipt checksum mismatch"):
        lake.inputs.index_selection(current, "value", causal_versions=True)


def test_equivalent_later_root_audits_reused_summary_against_original_source(tmp_path):
    lake, frame, request, old = setup(tmp_path)
    lake.inputs.index_selection(old, "value", causal_versions=True)
    lake.items.register(DataItemSpec("unrelated"))
    lake.items.ingest("unrelated", frame.drop("creator"), available_date="2024-01-01")
    later = lake.inputs.freeze(request, information_cutoff="2024-01-03", retain_batch_bounds=True)
    lake.inputs.verify(later)
    with sqlite3.connect(lake.data_meta_path) as db:
        db.row_factory = sqlite3.Row
        row = dict(db.execute("select * from input_projection_index").fetchone())
        row["identity"] = "0" * 64
        digest = input_index._digest({key: value for key, value in row.items() if key != "digest"})
        db.execute("update input_projection_index set identity=?,digest=?", (row["identity"], digest))
    with pytest.raises(RuntimeError, match="Scalar selection summary differs from original evidence"):
        lake.inputs.verify(later)


def test_explicit_summary_fast_lookup_obeys_small_budget(tmp_path):
    lake, _, _, old = setup(tmp_path)
    assert lake.inputs.index_selection(old, "value", causal_versions=True) is not None
    with pytest.raises(MemoryError):
        lake.inputs.index_selection(old, "value", causal_versions=True,
            config=ExecutionOptions(max_buffer_bytes=128))


def test_freeze_records_only_without_decoding_optional_row_indexes(tmp_path, monkeypatch):
    lake, _, request, _ = setup(tmp_path)
    def forbidden(*args, **kwargs):
        raise AssertionError("Freeze computed a whole-history optional selection summary")
    monkeypatch.setattr(input_index, "project_original", forbidden)
    frozen = lake.inputs.freeze(request, information_cutoff="2024-01-03")
    assert frozen.evidence["value"]["batches"]
