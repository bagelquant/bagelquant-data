"""Premature baseline completion can resume without backdating incremental history."""
from dataclasses import replace

import polars as pl
import pytest

from bagelquant_data import RawInput
from bagelquant_data.core.exceptions import ConfigurationError
from test_initialization_transport import FinancialSource, lake_for

START, END = "2024-02-28", "2024-02-29"
OPTIONS = {"pagination": "offset", "page_size": 2,
    "daily_range_backfill": {"start_param": "start_date", "end_param": "end_date",
        "row_limit": 2, "window": "calendar_month"}}


def initialized(tmp_path):
    source = FinancialSource()
    lake = lake_for(tmp_path, source)
    report = lake.raw.initialize("reports", source="fake", start=START, end=END,
        source_options=OPTIONS, retry_backoff_seconds=0)
    assert report.status == "success"
    return lake, source


def reopen(lake, **options):
    return lake.integrity.reopen_raw_initialization("reports", source="fake",
        **{"start": START, "end": END, "reason": "provider cap truncated the initial inventory", **options})


def test_reopen_restores_historical_query_without_changing_frozen_inputs_or_verified_pit(tmp_path):
    lake, source = initialized(tmp_path)
    request = RawInput("fake", "reports", view="versions", include_historical_baseline=True)
    frozen = lake.inputs.freeze({"history": request}, information_cutoff=END)
    original = lake.inputs.read(frozen, "history").collect()
    source.frame = pl.concat([source.frame, pl.DataFrame({
        "ts_code": ["A"], "ann_date": ["20240229"], "f_ann_date": ["20240229"],
        "end_date": ["20230331"], "value": [99.0]})])
    receipt = reopen(lake)
    assert receipt["scope_count"] == 2 and receipt["status"] == "running"
    assert not lake.integrity.coverage("reports", source="fake", start=START, end=END)["complete"]
    assert lake.raw.plan_updates(["reports"], source="fake", start=START, end=END)[0]["mode"] == "initialize"
    source.calls.clear()
    resumed = lake.raw.initialize("reports", source="fake", start=START, end=END,
        source_options=OPTIONS, retry_backoff_seconds=0)
    assert resumed.status == "success" and resumed.request_count == 3
    assert all("start_date" in params for params in source.calls)
    history = lake.raw.read("reports", source="fake", start=START, end=END, view="latest", as_of=END).collect()
    assert history.height == 5
    assert history.filter(pl.col("value") == 99.0)["time"].item().isoformat() == END
    assert lake.raw.read("reports", source="fake", view="latest", as_of=END, strict=True).collect().is_empty()
    assert lake.inputs.read(frozen, "history").collect().equals(original)
    assert lake.inputs.verify(frozen)["valid"]
    assert lake.integrity.coverage("reports", source="fake", start=START, end=END)["complete"]


@pytest.mark.parametrize("failure", ["start", "end", "definition", "incremental_check", "incremental_commit", "lease", "expired_lease", "prepared", "reason", "outside_scope", "unfinished_run"])
def test_reopen_refuses_unsafe_history_and_does_not_change_completed_state(tmp_path, failure):
    lake, source = initialized(tmp_path)
    options = {}
    if failure == "start":
        options["start"] = "2024-02-27"
    elif failure == "end":
        options["end"] = "2024-03-01"
    elif failure == "reason":
        options["reason"] = " "
    elif failure == "definition":
        lake.raw.register(replace(lake.raw.get("reports", source="fake"), description="changed"))
    elif failure.startswith("incremental"):
        if failure == "incremental_commit":
            source.frame = source.frame.with_columns(pl.lit(42.0).alias("value"))
        lake.raw.update("reports", source="fake", start=START, end=END, today=END, retry_backoff_seconds=0)
    elif failure in {"lease", "expired_lease"}:
        lake._data_meta.acquire_update_leases([("fake", "reports", "writer")])
        if failure == "expired_lease":
            with lake._data_meta.connect() as db:
                db.execute("update update_leases set lease_expires_at='2000-01-01T00:00:00+00:00'")
    elif failure == "prepared":
        with lake._data_meta.connect() as db:
            db.execute("update version_commits set status='prepared' where source='fake' and dataset='reports'")
    elif failure == "outside_scope":
        with lake._data_meta.connect() as db:
            db.execute("update update_scopes set scope_key='2024-03-01',status='running' where scope_key='2024-02-28'")
    elif failure == "unfinished_run":
        with lake._data_meta.connect() as db:
            db.execute("update ingestion_runs set status='running' where dataset='reports'")
    with pytest.raises(ConfigurationError):
        reopen(lake, **options)
    with lake._data_meta.connect() as db:
        assert db.execute("select status from dataset_initializations where dataset='reports'").fetchone()[0] == "complete"
