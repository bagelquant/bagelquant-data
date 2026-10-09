"""Bulk transport preserves the daily PIT/coverage contract without Workbench."""
from contextlib import closing
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, ExecutionOptions


class FinancialSource:
    name = "fake"

    def __init__(self):
        self.calls = []
        self.cancelled = False
        self.frame = pl.DataFrame({
            "ts_code": ["A", "A", "B", "B", "A"],
            "ann_date": ["19990101", None, "20240301", "20240229", "20240301"],
            "f_ann_date": ["20240228", "20240229", "20240229", "20240303", "20240228"],
            "end_date": ["20231231", "20231231", "20231231", "20231231", "20230630"],
            "value": [1., 2., 3., 4., 5.],
        })

    def fetch(self, api, params):
        self.calls.append(dict(params))
        frame = self.frame
        if "ts_code" in params:
            frame = frame.filter(pl.col("ts_code").is_in(params["ts_code"].split(",")))
        if "f_ann_date" in params:
            frame = frame.filter(pl.col("f_ann_date") == params["f_ann_date"].replace("-", ""))
        if "start_date" in params:
            frame = frame.filter(pl.col("f_ann_date").is_between(pl.lit(params["start_date"].replace("-", "")), pl.lit(params["end_date"].replace("-", ""))))
        return frame.slice(params.get("offset", 0), params.get("limit", frame.height))


def lake_for(root, provider):
    lake = DataLake.open(data_meta_path=root / "data.sqlite", lake_path=root / "lake")
    lake.catalog.sources.register(provider)
    lake.raw.register(DatasetSpec("reports", "by_date", source="fake", date_kind="calendar",
        date_param="f_ann_date", field_mappings={"f_ann_date": "time", "ts_code": "asset_id"},
        primary_key_extra=("end_date",)))
    return lake


SCAN = {"initialization_scan": {"parameter_values": ["B", "A"], "cohort_size": 1,
    "target_param": "ts_code", "page_size": 2, "max_pages": 10, "require_nonempty": True}}


def test_explicit_monthly_refresh_repairs_history_without_losing_previous_versions(tmp_path):
    source = FinancialSource()
    lake = lake_for(tmp_path, source)
    options = {"pagination": "offset", "page_size": 2,
        "daily_range_backfill": {"start_param": "start_date", "end_param": "end_date",
            "row_limit": 2, "window": "calendar_month"}}
    first = lake.raw.initialize("reports", source="fake", start="2024-02-28", end="2024-02-29",
        source_options=options, retry_backoff_seconds=0)
    assert first.status == "success" and first.rows_committed == 4
    original = lake.raw.read("reports", source="fake", view="versions").collect()
    source.frame = pl.concat([source.frame, pl.DataFrame({
        "ts_code": ["A"], "ann_date": ["20240229"], "f_ann_date": ["20240229"],
        "end_date": ["20230331"], "value": [99.0]})])
    source.calls.clear()
    refreshed = lake.raw.refresh("reports", source="fake", start="2024-02-28", end="2024-02-29",
        source_options=options, retry_backoff_seconds=0)
    assert refreshed.status == "success" and refreshed.request_count == 3
    assert all("start_date" in params and "f_ann_date" not in params for params in source.calls)
    assert lake.raw.read("reports", source="fake").collect().height == 5
    retained = lake.raw.read("reports", source="fake", view="versions").collect()
    assert original.join(retained, on=original.columns, how="anti", nulls_equal=True).is_empty()
    assert lake.integrity.coverage("reports", source="fake", start="2024-02-28", end="2024-02-29")["complete"]


def test_full_history_matches_daily_with_early_null_and_reverse_ann_dates(tmp_path):
    source = FinancialSource()
    lake = lake_for(tmp_path / "bulk", source)
    report = lake.raw.initialize("reports", source="fake", start="2024-02-28", end="2024-03-01", max_retries=1, retry_backoff_seconds=0,
                                source_options=SCAN, config=ExecutionOptions(workers=4))
    reference = lake_for(tmp_path / "daily", FinancialSource())
    reference.raw.initialize("reports", source="fake", start="2024-02-28", end="2024-03-01", max_retries=1, retry_backoff_seconds=0)
    keys = ["source_time", "asset_id", "end_date", "ann_date", "value"]
    result = lake.raw.read("reports", source="fake", view="latest").collect().select(keys).sort(keys[:3])
    expected = reference.raw.read("reports", source="fake", view="latest").collect().select(keys).sort(keys[:3])
    assert result.equals(expected)
    assert result.height == 4
    assert result["ann_date"].null_count() == 1
    assert report.status == "success", report.error_message
    assert report.request_count == len(source.calls) == 4
    assert report.rows_downloaded == 5  # Future f_ann_date fetched, excluded from publication.
    assert [(row["scope_key"], row["status"]) for row in lake.integrity.update_scopes("reports", source="fake")] == [
        ("2024-02-28", "success"), ("2024-02-29", "success"), ("2024-03-01", "empty")]
    metrics = lake.integrity.runs()[0]["metrics"]
    assert metrics["provider_seconds"] >= 0
    assert metrics["commit_count"] == 1


def test_cancellation_before_inventory_complete_never_declares_daily_coverage(tmp_path):
    class CancelSource(FinancialSource):
        def fetch(self, api, params):
            result = super().fetch(api, params)
            self.cancelled = True
            return result
    provider = CancelSource()
    lake = lake_for(tmp_path, provider)
    first = lake.raw.initialize("reports", source="fake", start="2024-02-28", end="2024-03-01", max_retries=1, retry_backoff_seconds=0,
        source_options=SCAN, cancel_requested=lambda: provider.cancelled)
    assert first.status == "cancelled"
    assert first.rows_committed == 0
    assert not lake.raw.manifest("reports", source="fake")
    assert not any(row["status"] in {"success", "empty"}
                   for row in lake.integrity.update_scopes("reports", source="fake"))
    provider.cancelled = False
    # The same declaration and frozen bounds resume through the ordinary planner.
    provider.fetch = FinancialSource.fetch.__get__(provider)
    resumed = lake.raw.initialize("reports", source="fake", start="2024-02-28", end="2024-03-01", max_retries=1, retry_backoff_seconds=0, source_options=SCAN)
    assert resumed.status == "success"
    assert resumed.rows_committed == 4


@pytest.mark.parametrize("failure", ["repeat_page", "ignore_cohort", "memory", "empty"])
def test_invalid_bulk_transport_does_not_publish_complete_coverage(tmp_path, failure):
    class BrokenSource(FinancialSource):
        def fetch(self, api, params):
            if failure == "repeat_page":
                return super().fetch(api, {**params, "offset": 0})
            if failure == "ignore_cohort":
                return self.frame
            if failure == "empty":
                return self.frame.head(0)
            return super().fetch(api, params)
    lake = lake_for(tmp_path, BrokenSource())
    options = ExecutionOptions(max_buffer_bytes=2 if failure == "memory" else 1000000)
    report = lake.raw.initialize("reports", source="fake", start="2024-02-28", end="2024-03-01", max_retries=1, retry_backoff_seconds=0,
                                source_options=SCAN, config=options)
    assert report.status == "failed"
    assert not lake.raw.manifest("reports", source="fake")
    assert report.rows_committed == 0


def test_calendar_month_windows_are_complete_offset_pages_and_keep_leap_day(tmp_path):
    provider = FinancialSource()
    lake = lake_for(tmp_path, provider)
    report = lake.raw.initialize("reports", source="fake", start="2024-02-28", end="2024-03-01", max_retries=1, retry_backoff_seconds=0,
        source_options={"daily_range_backfill": {"window": "calendar_month", "start_param": "start_date",
            "end_param": "end_date", "row_limit": 2}})
    assert report.status == "success", report.error_message
    assert report.rows_committed == 4
    ranged = [call for call in provider.calls if "start_date" in call]
    assert all(call["start_date"][:7] == call["end_date"][:7] for call in ranged)
    assert any(call["offset"] > 0 for call in ranged)


def test_general_coverage_uses_committed_generation_not_logical_path(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "data.sqlite", lake_path=tmp_path / "lake")
    lake.raw.ingest(DatasetSpec("calendar", "general", source="fake"),
                    pl.DataFrame({"time": ["20240229"], "is_open": [1]}))
    coverage = lake.integrity.coverage("calendar", source="fake", start="2024-02-29", end="2024-02-29")
    assert coverage["complete"]


def test_runtime_inspection_reads_committed_wal_without_copying(tmp_path, monkeypatch):
    from bagelquant_data.storage import data_meta
    metadata, root = tmp_path / "data.sqlite", tmp_path / "lake"
    DataLake.open(data_meta_path=metadata, lake_path=root).close()
    with closing(sqlite3.connect(metadata)) as writer:
        writer.execute("PRAGMA wal_autocheckpoint=0")
        writer.execute("UPDATE data_meta_state SET updated_at='live' WHERE key='schema_version'")
        writer.commit()
        monkeypatch.setattr(data_meta, "copy_database", lambda *args: pytest.fail("runtime copied SQLite"))
        assert DataLake.inspect(data_meta_path=metadata, lake_path=root, runtime=True)["status"] == "ready"
        DataLake.open(data_meta_path=metadata, lake_path=root, read_only=True, runtime=True).close()


def test_row_commit_limit_counts_rows_instead_of_daily_tasks(tmp_path):
    provider = FinancialSource()
    lake = lake_for(tmp_path, provider)
    report = lake.raw.initialize("reports", source="fake", start="2024-02-28", end="2024-03-01", max_retries=1, retry_backoff_seconds=0,
                                config=ExecutionOptions(commit_batch_rows=1))
    assert report.rows_committed == 4
    assert report.commit_count == 2


def test_initialization_prerequisite_is_transient_not_a_declaration_change(tmp_path):
    lake = lake_for(tmp_path, FinancialSource())
    lake.raw.register(DatasetSpec("stocks", "general", source="fake"))
    before = lake.catalog.export_declarations()
    plan = lake.raw.plan_updates(["reports", "stocks"], source="fake", start="2024-02-28", end="2024-03-01",
        source_options={"reports": {"initialization_scan": {"parameter_dataset": "stocks"}}})
    assert [action["dataset"] for action in plan] == ["stocks", "reports"]
    assert before == lake.catalog.export_declarations()


def test_single_day_dense_inventory_allows_a_cohort_without_history(tmp_path):
    provider = FinancialSource()
    provider.frame = provider.frame.head(1)  # A has the requested day's report; B has no history.
    lake = lake_for(tmp_path, provider)
    report = lake.raw.initialize('reports', source='fake', start='2024-02-28', end='2024-02-28',
        max_retries=3, retry_backoff_seconds=0,
        source_options={**SCAN, 'require_nonempty_scopes': True})
    assert report.status == 'success', report.error_message
    assert report.rows_committed == 1
    assert report.request_count == len(provider.calls) == 2
    assert lake.integrity.coverage('reports', source='fake', start='2024-02-28', end='2024-02-28')['complete']
