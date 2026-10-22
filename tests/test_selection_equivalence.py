from datetime import date
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataLake, DataItemSpec, ItemInput, ExecutionOptions, input_read_boundary


def test_equivalence_reuses_fixed_scope_from_records_without_optional_index(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("value"))
    lake.items.ingest("value", pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "value": [1.]}),
        available_date="2024-01-01")
    old = lake.inputs.freeze({"value": ItemInput("value", view="versions")}, information_cutoff="2024-01-02",
        retain_batch_bounds=True)
    lake.items.ingest("value", pl.DataFrame({"time": [date(2024, 1, 3)], "asset_id": ["A"], "value": [3.]}),
        available_date="2024-01-03")
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_value_index'").fetchone() is None
    from bagelquant_data import inputs
    def forbidden(*args, **kwargs):
        raise AssertionError("Record equivalence consumed originals or implicitly indexed/froze inputs")
    monkeypatch.setattr(inputs, "read_batch", forbidden)
    monkeypatch.setattr(lake.inputs, "_indexed_selection", forbidden)
    monkeypatch.setattr(lake.inputs, "freeze", forbidden)
    assert lake.inputs.selection_equivalent(old, None, "value", end="2024-01-02",
        current_as_of="2024-01-03", view="snapshot", values_only=True) is True


def test_equivalence_uses_canonical_events_and_preserves_unknown(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("value"))
    frame = pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "value": [1.], "creator": [1]})
    lake.items.ingest("value", frame, available_date="2024-01-01")
    old = lake.inputs.freeze({"value": ItemInput("value", view="versions")}, information_cutoff="2024-01-03")
    lake.items.ingest("value", frame.with_columns(pl.lit(2).alias("creator")), available_date="2024-01-01")
    assert lake.inputs.selection_equivalent(old, None, "value", values_only=True, causal_versions=True) is True
    lake.items.ingest("value", frame.with_columns(pl.lit(5.).alias("value")), available_date="2024-01-02")
    assert lake.inputs.selection_equivalent(old, None, "value", values_only=True, causal_versions=True) is False
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_value_index'").fetchone() is None
    assert lake.inputs.selection_equivalent(old, None, "value", values_only=True, causal_versions=True) is False
    with pytest.raises(ValueError, match="scalar versions"):
        lake.inputs.selection_equivalent(old, None, "value", view="snapshot", values_only=True, causal_versions=True)
    with pytest.raises(ValueError, match="exceeds"):
        lake.inputs.selection_equivalent(old, None, "value", as_of="2024-01-04")


def test_same_content_different_snapshot_and_history_selectors_are_not_equivalent(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("value"))
    frame = pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "value": [1.]})
    lake.items.ingest("value", frame, available_date="2024-01-01")
    lake.items.ingest("value", frame.with_columns(pl.lit(9.).alias("value")), available_date="2024-01-02")
    snapshot = lake.inputs.freeze({"value": ItemInput("value", view="snapshot")}, information_cutoff="2024-01-03",
        retain_batch_bounds=True)
    history = lake.inputs.freeze({"value": ItemInput("value", view="history")}, information_cutoff="2024-01-03",
        retain_batch_bounds=True)
    assert lake.inputs.read(snapshot, "value").collect()["value"].to_list() == [9.]
    assert lake.inputs.read(history, "value").collect()["value"].to_list() == [1.]
    assert lake.inputs.selection_equivalent(snapshot, history, "value", values_only=True) is False


def test_snapshot_equivalence_requires_same_valid_cutoff_as_actual_consumption(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("value"))
    lake.items.ingest("value", pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "value": [1.]}),
        available_date="2024-01-01")
    frozen = lake.inputs.freeze({"value": ItemInput("value", view="snapshot")})
    with pytest.raises(ValueError, match="information cutoff"):
        lake.inputs.selection_equivalent(frozen, None, "value", values_only=True)


def test_current_metadata_reused_only_in_same_fixed_operation_and_boundary(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("value"))
    frame = pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "value": [1.]})
    lake.items.ingest("value", frame, available_date="2024-01-01")
    frozen = lake.inputs.freeze({"value": ItemInput("value", view="versions")}, information_cutoff="2024-01-03")
    owner = type(lake.inputs)
    original_capture, captures = owner._capture, []
    def capture(self, *args, **kwargs):
        captures.append(kwargs["cutoff"])
        return original_capture(self, *args, **kwargs)
    monkeypatch.setattr(owner, "_capture", capture)
    # Captures and decoded blocks now share one conservatively charged reserve.
    options = ExecutionOptions(max_buffer_bytes=4 * 2**20)
    with lake.inputs.read_context(frozen, config=options):
        assert lake.inputs.selection_equivalent(frozen, None, "value", values_only=True, causal_versions=True)
        with lake.inputs.read_context(frozen, config=options):
            assert lake.inputs.selection_equivalent(frozen, None, "value", values_only=True, causal_versions=True)
        assert lake.inputs.selection_equivalent(frozen, None, "value", values_only=True, causal_versions=True)
        assert len(captures) == 1
        assert lake.inputs.selection_equivalent(frozen, None, "value", values_only=True, causal_versions=True,
            current_as_of="2024-01-04")
        assert len(captures) == 2
        with input_read_boundary(lake.data_meta_path, frozen.max_commit, as_of="2024-01-03",
            max_check_id=frozen.max_check_id):
            bounded = DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path, read_only=True)
            assert bounded.inputs.selection_equivalent(frozen, None, "value", values_only=True, causal_versions=True)
        assert len(captures) == 3
    lake.items.ingest("value", frame.with_columns(pl.lit(9.).alias("value")), available_date="2024-01-02")
    with lake.inputs.read_context(frozen, config=options):
        assert lake.inputs.selection_equivalent(frozen, None, "value", values_only=True, causal_versions=True) is False
    assert len(captures) == 4


def test_small_current_metadata_reserve_does_not_retain_oversized_capture(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("value"))
    lake.items.ingest("value", pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "value": [1.]}),
        available_date="2024-01-01")
    frozen = lake.inputs.freeze({"value": ItemInput("value", view="versions")}, information_cutoff="2024-01-03")
    original_capture, captures = lake.inputs._capture, []
    def capture(*args, **kwargs):
        captures.append(kwargs["cutoff"])
        return original_capture(*args, **kwargs)
    monkeypatch.setattr(lake.inputs, "_capture", capture)
    with lake.inputs.read_context(frozen, config=ExecutionOptions(max_buffer_bytes=128)):
        for _ in range(2):
            assert lake.inputs.selection_equivalent(frozen, None, "value", values_only=True, causal_versions=True) is True
    assert len(captures) == 2


def test_nested_larger_budget_cannot_enlarge_parent_metadata_memo(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("value"))
    lake.items.ingest("value", pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "value": [1.]}),
        available_date="2024-01-01")
    frozen = lake.inputs.freeze({"value": ItemInput("value", view="versions")}, information_cutoff="2024-01-03")
    original_capture, captures = lake.inputs._capture, []
    def capture(*args, **kwargs):
        captures.append(kwargs["cutoff"])
        return original_capture(*args, **kwargs)
    monkeypatch.setattr(lake.inputs, "_capture", capture)
    with lake.inputs.read_context(frozen, config=ExecutionOptions(max_buffer_bytes=128)):
        with lake.inputs.read_context(frozen, config=ExecutionOptions(max_buffer_bytes=2**20)):
            assert lake.inputs.selection_equivalent(frozen, None, "value", values_only=True, causal_versions=True)
        for _ in range(2):
            assert lake.inputs.selection_equivalent(frozen, None, "value", values_only=True, causal_versions=True) is True
    assert len(captures) == 3


@pytest.mark.parametrize("late_old_observation", [False, True])
def test_legacy_raw_proof_uses_optional_bounds_without_decoding_or_changing_replay(tmp_path, monkeypatch, late_old_observation):
    from datetime import UTC, datetime
    from bagelquant_data import DatasetSpec, RawInput, input_index
    from bagelquant_data import inputs

    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("raw", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "value": [1.]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2024, 1, 10, tzinfo=UTC))
    original = lake.inputs.freeze({"raw": RawInput("custom", "raw", view="versions")}, information_cutoff="2024-01-10")
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_index'").fetchone() is None
    next_day = date(2024, 1, 1) if late_old_observation else date(2024, 1, 3)
    lake.raw.ingest(spec, frame.with_columns(pl.lit(next_day).alias("time"), pl.lit(9.).alias("value")),
                    ingested_at=datetime(2024, 1, 2 if late_old_observation else 11, tzinfo=UTC))
    before = lake.inputs.read(original, "raw").collect()
    def forbidden(*args, **kwargs):
        raise AssertionError("Record proof decoded bytes/row index, froze receipts or warmed indexes")
    monkeypatch.setattr(input_index, "project_original", forbidden)
    monkeypatch.setattr(inputs, "read_batch", forbidden)
    monkeypatch.setattr(lake.inputs, "freeze", forbidden)
    token = lake.inputs.selection_equivalent(original, None, "raw", end="2024-01-02",
        as_of="2024-01-02", current_as_of="2024-01-02", view="versions", records_only=True)
    assert token is (None if late_old_observation else True)
    # Whole producer currentness uses the same exact record proof at its saved
    # cutoff; future physical batches cannot demand a historical Timing replay.
    if late_old_observation:
        # Unknown evidence may consult the absent optional row proof, but never
        # original bytes or maintenance; it must remain a conservative miss.
        assert not hasattr(input_index, "read")
    assert lake.inputs.is_current(original) is (not late_old_observation)
    assert original.evidence["raw"]["retained_batch_bounds"]
    assert original.evidence["raw"]["batches"][0]["min_observation"] == "2024-01-01"
    monkeypatch.undo()
    assert lake.inputs.read(original, "raw").collect().equals(before)
    assert lake.inputs.get(original).digest == original.digest


@pytest.mark.parametrize("future_baseline", [False, True])
def test_general_versions_header_proof_keeps_unselected_known_and_unknown_baselines_distinct(tmp_path, monkeypatch, future_baseline):
    from datetime import UTC, datetime
    from bagelquant_data import DatasetSpec, RawInput, input_index

    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("general", "general")
    frame = pl.DataFrame({"asset_id": ["A"], "value": [1.]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2024, 1, 1, tzinfo=UTC))
    original = lake.inputs.freeze({"general": RawInput("custom", "general", view="versions",
        include_historical_baseline=True)}, information_cutoff="2024-01-10")
    changed = frame.with_columns(pl.lit(9.).alias("value"))
    if future_baseline:
        from bagelquant_data.pipeline.versions import commit_versions
        commit_versions(spec, changed.with_columns(pl.lit("custom").alias("source")),
            lake._parquet, run_id="future-baseline", historical_baseline=True,
            ingested_at=datetime(2024, 1, 11, tzinfo=UTC))
    else:
        lake.raw.ingest(spec, changed, ingested_at=datetime(2024, 1, 11, tzinfo=UTC))
    monkeypatch.setattr(input_index, "project_original", lambda *a, **k: pytest.fail("General headers need no index decode"))
    result = lake.inputs.selection_equivalent(original, None, "general", as_of="2024-01-02",
        current_as_of="2024-01-02", view="versions", records_only=True)
    assert result is (None if future_baseline else True)
    assert lake.inputs.get(original).digest == original.digest
