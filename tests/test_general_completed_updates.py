from __future__ import annotations

from dataclasses import replace

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.core.dataset import RequestDiscoverySpec
from bagelquant_data.pipeline.scopes import general_snapshot_completed


class DiscoveredSource:
    name = "fake"

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.empty_left = 0
        self.groups = ["A", "B"]

    def fetch(self, dataset: str, params: dict[str, object]) -> pl.DataFrame:
        self.calls.append((dataset, dict(params)))
        if dataset == "categories":
            return pl.DataFrame({"category": self.groups})
        if params["group"] == "A" and params["state"] == "old" and self.empty_left:
            self.empty_left -= 1
            return pl.DataFrame()
        return pl.DataFrame({"asset_id": [str(params["group"]) + str(params["state"])]})


@pytest.fixture
def completed(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    source = DiscoveredSource()
    lake.catalog.sources.register(source)
    spec = DatasetSpec(
        "inventory", "general", source="fake",
        source_api_param_sets=({"state": ["old", "current"]},),
        request_discovery=RequestDiscoverySpec(
            api="categories", params={"level": "one"}, result_field="category", target_param="group",
        ),
    )
    lake.raw.register(spec)
    report = lake.raw.update("inventory", source="fake", end="2025-01-03")
    assert report.status == "success"
    assert len(source.calls) == 5
    source.calls.clear()
    return lake, source, spec, report


def test_complete_same_target_skips_discovery_provider_and_original_values(completed, monkeypatch):
    lake, source, spec, _ = completed
    manifest = lake.raw.manifest("inventory", source="fake")
    with monkeypatch.context() as patch:
        patch.setattr(source, "fetch", lambda *args: pytest.fail("provider called"))
        patch.setattr(lake.raw, "read", lambda *args, **kwargs: pytest.fail("values read"))
        assert general_snapshot_completed(spec, lake._data_meta, end="2025-01-03")
        assert lake.raw.update("inventory", source="fake", end="2025-01-03").request_count == 0
        assert lake.raw.plan_updates(["inventory"], source="fake", end="2025-01-03") == []
    assert source.calls == []
    assert lake.raw.manifest("inventory", source="fake") == manifest


@pytest.mark.parametrize("mode", ["advance", "refresh", "reset", "definition"])
def test_new_or_unfinished_general_rechecks_whole_inventory(completed, mode):
    lake, source, spec, _ = completed
    end = "2025-01-03"
    if mode == "advance":
        end = "2025-01-04"
    elif mode == "reset":
        rows = lake.integrity.update_scopes(source="fake", dataset="inventory")
        lake.integrity.reset_update_scopes([rows[0]["id"]])
    elif mode == "definition":
        lake.raw.register(replace(spec, source_api_params={"version": "two"}))
    method = lake.raw.refresh if mode == "refresh" else lake.raw.update
    report = method("inventory", source="fake", end=end)
    assert report.status == "success"
    assert report.commit_count == 1
    assert len(source.calls) == 5
    assert lake.raw.read("inventory", source="fake").collect()["asset_id"].sort().to_list() == [
        "Acurrent", "Aold", "Bcurrent", "Bold",
    ]


@pytest.mark.parametrize("damage", ["definition", "publication", "unfinished"])
def test_general_gate_rejects_stale_or_incomplete_completion_flag(completed, damage):
    lake, _, spec, _ = completed
    with lake._data_meta.connect() as db:
        if damage == "definition":
            db.execute("UPDATE node_completions SET definition_hash='stale' WHERE node_key='fake/inventory'")
        elif damage == "publication":
            db.execute("UPDATE node_completions SET publication_revision=publication_revision+1 WHERE node_key='fake/inventory'")
        else:
            db.execute("UPDATE datasets SET update_incomplete=1 WHERE source='fake' AND name='inventory'")
    assert not general_snapshot_completed(spec, lake._data_meta, end="2025-01-03")


def test_general_subset_gate_does_not_certify_default_inventory(completed):
    lake, _, spec, _ = completed
    assert not general_snapshot_completed(spec, lake._data_meta, end="2025-01-03", params={"group": "A"})
    assert not general_snapshot_completed(spec, lake._data_meta, end="2025-01-03", params={})
    assert not general_snapshot_completed(spec, lake._data_meta, end="2025-01-03", ids=[])


def test_unverified_general_initialization_needs_a_real_witness(completed):
    lake, source, spec, _ = completed
    spec = replace(spec, name="unverified")
    lake.raw.register(spec)
    lake.raw.initialize("unverified", source="fake", end="2025-01-03")
    assert not general_snapshot_completed(spec, lake._data_meta, end="2025-01-03")
    source.calls.clear()
    assert lake.raw.update("unverified", source="fake", end="2025-01-03").request_count == 4
    assert len(source.calls) == 5
    assert general_snapshot_completed(spec, lake._data_meta, end="2025-01-03")
    assert lake.raw.update("unverified", source="fake", end="2025-01-03").request_count == 0


@pytest.mark.parametrize("persistent", [False, True])
def test_declared_general_history_retries_transient_empty_and_rejects_unproved_withdrawal(completed, persistent):
    lake, source, _, _ = completed
    before = lake.raw.manifest("inventory", source="fake")
    original = lake.raw.read("inventory", source="fake").collect()
    source.empty_left = 2 if persistent else 1
    report = lake.raw.update(
        "inventory", source="fake", end="2025-01-04", max_retries=2,
        retry_backoff_seconds=0, source_options={"preserve_nonempty_history": True},
    )
    assert len(source.calls) == 6
    if persistent:
        assert report.status == "failed"
        assert report.commit_count == 0
        assert "previously nonempty General" in str(report.error_message)
        assert lake.raw.manifest("inventory", source="fake") == before
        assert lake.raw.read("inventory", source="fake").collect().equals(original)
    else:
        assert report.status == "success"
        assert report.commit_count == 1
        assert lake.raw.read("inventory", source="fake").collect()["asset_id"].sort().to_list() == [
            "Acurrent", "Aold", "Bcurrent", "Bold",
        ]


@pytest.mark.parametrize("same_target", [False, True])
def test_enabling_history_guard_reopens_an_already_admitted_empty(completed, same_target):
    lake, source, spec, _ = completed
    source.empty_left = 1
    end = "2025-01-03" if same_target else "2025-01-04"
    admitted = lake.raw.refresh("inventory", source="fake", end=end)
    assert admitted.status == "success" and admitted.empty_count == 1
    assert general_snapshot_completed(spec, lake._data_meta, end=end)
    assert not general_snapshot_completed(
        spec, lake._data_meta, end=end, source_options={"preserve_nonempty_history": True},
    )
    source.calls.clear()
    repaired = lake.raw.update(
        "inventory", source="fake", end=end,
        source_options={"preserve_nonempty_history": True},
    )
    assert repaired.status == "success" and repaired.commit_count == 1
    assert len(source.calls) == 5
    assert general_snapshot_completed(
        spec, lake._data_meta, end=end, source_options={"preserve_nonempty_history": True},
    )


def test_unclaimed_general_cohort_cannot_publish_a_subset_and_retry_keeps_all_cohorts(completed, monkeypatch):
    lake, source, _, _ = completed
    before = lake.raw.manifest("inventory", source="fake")
    original = lake.raw.read("inventory", source="fake").collect()
    claim = lake._data_meta.claim_update_scopes
    claims = 0
    def omit_second(scope_ids, *, run_id):
        nonlocal claims
        claims += 1
        return [] if claims == 2 else claim(scope_ids, run_id=run_id)
    with monkeypatch.context() as patch:
        patch.setattr(lake._data_meta, "claim_update_scopes", omit_second)
        with pytest.raises(RuntimeError, match="complete request inventory"):
            lake.raw.update("inventory", source="fake", end="2025-01-04", workers=1, max_in_flight=1)
    assert lake.raw.manifest("inventory", source="fake") == before
    assert lake.raw.read("inventory", source="fake").collect().equals(original)
    source.calls.clear()
    retried = lake.raw.update("inventory", source="fake", end="2025-01-04", workers=1, max_in_flight=1)
    assert retried.status == "success" and retried.commit_count == 1
    assert len(source.calls) == 5
    assert lake.raw.read("inventory", source="fake").collect()["asset_id"].sort().to_list() == [
        "Acurrent", "Aold", "Bcurrent", "Bold",
    ]


@pytest.mark.parametrize("singleton", [False, True])
def test_prior_override_of_discovery_target_cannot_certify_default_inventory(completed, singleton):
    lake, source, spec, _ = completed
    if singleton:
        source.groups = ["A"]
    overridden = lake.raw.refresh(
        "inventory", source="fake", end="2025-01-03", params={"group": "FOREIGN"},
    )
    assert overridden.status == "success"
    assert not general_snapshot_completed(spec, lake._data_meta, end="2025-01-03")
    source.calls.clear()
    restored = lake.raw.update("inventory", source="fake", end="2025-01-03")
    assert restored.status == "success" and restored.commit_count == 1
    assert len(source.calls) == (3 if singleton else 5)
    assert general_snapshot_completed(spec, lake._data_meta, end="2025-01-03")
