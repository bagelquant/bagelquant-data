from datetime import UTC, datetime

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, RawInput


@pytest.mark.parametrize("view", ["snapshot", "history", "versions"])
@pytest.mark.parametrize("strict", [False, True])
def test_future_general_attestations_and_content_use_records_at_original_cutoff(tmp_path, monkeypatch, view, strict):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("settings", "general")
    original = pl.DataFrame({"asset_id": ["A"], "value": [1.]})
    lake.raw.ingest(spec, original, mode="initialize", ingested_at=datetime(2024, 1, 1, tzinfo=UTC))
    request = {"settings": RawInput("custom", "settings", view=view, strict=strict, include_historical_baseline=True)}
    frozen = lake.inputs.freeze(request, information_cutoff="2024-01-03")
    before = lake.inputs.read(frozen, "settings").collect()
    lake.raw.ingest(spec, original, ingested_at=datetime(2024, 1, 4, tzinfo=UTC))
    lake.raw.ingest(spec, original.with_columns(pl.lit(2.).alias("value")),
        ingested_at=datetime(2024, 1, 5, tzinfo=UTC))
    current = lake.inputs.freeze(request, information_cutoff="2024-01-03")
    assert before.equals(lake.inputs.read(current, "settings").collect())
    def forbidden(*args, **kwargs):
        raise AssertionError("General currentness read originals or decoded an optional row index")
    from bagelquant_data import inputs
    monkeypatch.setattr(inputs, "read_batch", forbidden)
    monkeypatch.setattr(lake.inputs, "_indexed_selection", forbidden)
    assert lake.inputs.is_current(frozen)


@pytest.mark.parametrize("view", ["snapshot", "history", "versions"])
def test_known_general_check_inside_cutoff_changes_original_visible_timing(tmp_path, view):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("settings", "general")
    frame = pl.DataFrame({"asset_id": ["A"], "value": [1.]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2024, 1, 1, tzinfo=UTC))
    request = {"settings": RawInput("custom", "settings", view=view, include_historical_baseline=True)}
    frozen = lake.inputs.freeze(request, information_cutoff="2024-01-03")
    old = lake.inputs.read(frozen, "settings").collect()
    lake.raw.ingest(spec, frame, ingested_at=datetime(2024, 1, 3, tzinfo=UTC))
    current = lake.inputs.freeze(request, information_cutoff="2024-01-03")
    assert not old.equals(lake.inputs.read(current, "settings").collect())
    assert not lake.inputs.is_current(frozen)


def test_future_historical_baseline_general_event_remains_visible_and_invalidates(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("settings", "general")
    lake.raw.ingest(spec, pl.DataFrame({"asset_id": ["A"], "value": [1.]}),
        mode="initialize", ingested_at=datetime(2024, 1, 1, tzinfo=UTC))
    request = {"settings": RawInput("custom", "settings", view="versions", include_historical_baseline=True)}
    frozen = lake.inputs.freeze(request, information_cutoff="2024-01-03")
    from bagelquant_data.pipeline.versions import commit_versions
    commit_versions(spec, pl.DataFrame({"asset_id": ["A"], "value": [2.], "source": ["custom"]}),
        lake._parquet, run_id="future-baseline", historical_baseline=True,
        ingested_at=datetime(2024, 1, 5, tzinfo=UTC))
    assert not lake.inputs.is_current(frozen)
