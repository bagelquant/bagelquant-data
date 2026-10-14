"""Native monthly completion recertifies covered provenance without row reads."""
from dataclasses import replace
from datetime import UTC, date, datetime

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, ItemPublication, RawInput


def frame(day, value=1.):
    return pl.DataFrame({"time": [day], "asset_id": ["A"], "value": [value],
        "available_date": [day], "version_available_date": [day]})


def publish(lake, source, output):
    with lake.items.publication(input_receipt=source) as operation:
        return operation.publish([output])["derived"]


def prepared(tmp_path, *, outside=False):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    jan, feb = date(2024, 1, 1), date(2024, 2, 1)
    spec = DatasetSpec("raw", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(spec, frame(jan), ingested_at=datetime(2024, 1, 1, tzinfo=UTC))
    request = RawInput("custom", "raw", view="versions")
    lake.items.register(DataItemSpec("derived", (request,), producer_key="external", producer_revision="1"))
    stable = lake.inputs.freeze({"raw": replace(request, observation_start=feb, observation_end=feb)},
        information_cutoff=feb)
    publish(lake, stable, ItemPublication("derived", frame(feb, 7.), start=feb, end=feb, complete_at=feb))
    old = lake.inputs.freeze({"raw": request}, information_cutoff=feb)
    if outside:
        # The same old creator also owns a batch outside the recertified scope.
        lake.items.ingest("derived", frame(feb, 7.), available_date=feb, input_receipt=old)
    publish(lake, old, ItemPublication("derived", frame(jan), start=jan, end=jan, complete_at=feb))
    original = lake.items.completed_builds("derived")[-1]
    retained = lake.inputs.freeze({"derived": ItemInput("derived", view="versions")},
        information_cutoff=feb, retain_batch_bounds=True)
    manifest = lake.items.manifest("derived")
    lake.raw.ingest(spec, frame(jan, 2.), ingested_at=datetime(2024, 2, 1, tzinfo=UTC))
    current = lake.inputs.freeze({"raw": request}, information_cutoff=feb)
    assert not lake.inputs.is_current(old)
    assert lake.inputs.is_current(stable)
    report = publish(lake, current, ItemPublication("derived", frame(jan).head(0), start=jan, end=jan,
        complete_at=feb, reuse_build_id=original["id"], affected_ranges=()))
    assert report is not None and report.rows_committed == 0
    assert lake.items.manifest("derived") == manifest
    assert original in lake.items.completed_builds("derived")
    assert lake.inputs.get(retained).digest == retained.digest
    return lake, old, jan, feb


def freeze(lake, cutoff, *, bounds=True):
    return lake.inputs.freeze({"derived": ItemInput("derived", view="versions")},
        information_cutoff=cutoff, retain_batch_bounds=bounds)


def no_values(lake, monkeypatch):
    from bagelquant_data import inputs
    def forbidden(*args, **kwargs):
        raise AssertionError("currentness decoded historical values or original bytes")
    monkeypatch.setattr(lake.inputs, "read", forbidden)
    monkeypatch.setattr(lake.inputs, "verify", forbidden)
    monkeypatch.setattr(inputs, "read_batch", forbidden)


def test_partial_native_completion_preserves_old_results_without_selected_lineage(tmp_path, monkeypatch):
    lake, _, _, feb = prepared(tmp_path)
    frozen = freeze(lake, feb)
    no_values(lake, monkeypatch)
    monkeypatch.setattr(lake.inputs, "_selected_item_parents",
        lambda *a, **k: pytest.fail("monthly certificate required selected row lineage"))
    assert lake.inputs.is_current(frozen)


@pytest.mark.parametrize("outside", [False, True])
def test_partial_completion_keeps_uncovered_or_unknown_creator_requirements(tmp_path, monkeypatch, outside):
    lake, _, _, feb = prepared(tmp_path, outside=outside)
    frozen = freeze(lake, feb, bounds=outside)
    no_values(lake, monkeypatch)
    # Schema 8 always captures segment bounds. Only the uncovered February
    # creator prevents recertification of the complete selection.
    assert lake.inputs.is_current(frozen) is (not outside)


def test_later_same_value_check_requires_its_own_completion(tmp_path, monkeypatch):
    from bagelquant_data.items import api
    lake, old, jan, feb = prepared(tmp_path)
    with monkeypatch.context() as patch:
        def fail_completion(*args, **kwargs):
            raise RuntimeError("completion failed")
        patch.setattr(api, "_save_completion", fail_completion)
        with pytest.raises(RuntimeError, match="completion failed"):
            lake.items.replace_range("derived", frame(jan), start=jan, end=jan,
                available_date=feb, input_receipt=old)
    frozen = freeze(lake, feb)
    no_values(lake, monkeypatch)
    assert not lake.inputs.is_current(frozen)
