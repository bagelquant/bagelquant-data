from __future__ import annotations

from datetime import UTC, datetime

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.core.request import RequestContext
from bagelquant_data.pipeline.scopes import LedgerRequest
from bagelquant_data.pipeline.update import update_dataset


class InventorySource:
    name = "fake"

    def __init__(self, failure: str | None = None) -> None:
        self.failure = failure

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        group = str(request["group"])
        if group == self.failure:
            raise RuntimeError("unavailable cohort")
        return pl.DataFrame({"asset_id": [group], "category": [group]})


@pytest.mark.parametrize("failure", [None, "forward", "retry"])
def test_general_retry_and_forward_cohorts_share_one_atomic_snapshot(
    tmp_path, failure: str | None,
) -> None:
    lake = DataLake.open(
        data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake",
    )
    spec = DatasetSpec("inventory", "general", source="fake")
    lake.raw.ingest(spec, pl.DataFrame({"asset_id": ["retained"], "category": ["old"]}))
    original = lake.raw.read("inventory", source="fake").collect()
    original_commits = len(lake._data_meta.dataset_snapshot("fake", "inventory")["commits"])
    report = update_dataset(
        spec=spec,
        source_adapter=InventorySource(failure),
        pipeline=lake._pipeline,
        context=RequestContext(
            source="fake", dataset="inventory",
            options={
                "workers": 1, "max_retries": 1, "retry_backoff_seconds": 0,
                "ingested_at": datetime(2025, 1, 3, tzinfo=UTC),
            },
        ),
        requests=tuple(
            LedgerRequest({"group": kind}, request_kind=kind, target_end="2025-01-03")
            for kind in ("retry", "forward")
        ),
    )
    current = lake.raw.read("inventory", source="fake").collect()
    commits = lake._data_meta.dataset_snapshot("fake", "inventory")["commits"]
    if failure is None:
        assert report.status == "success"
        assert report.commit_count == 1
        assert report.rows_downloaded == 2
        assert current["asset_id"].sort().to_list() == ["forward", "retry"]
        assert len(commits) == original_commits + 1
    else:
        assert report.status == "failed"
        assert report.commit_count == 0
        assert current.equals(original)
        assert len(commits) == original_commits
