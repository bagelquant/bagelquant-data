from datetime import date, timedelta

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, ExecutionOptions


class RangeProvider:
    name = "fake"

    def __init__(self, payload_size=600, *, truncate=False, assets_per_day=1):
        self.payload_size = payload_size
        self.truncate = truncate
        self.assets_per_day = assets_per_day
        self.calls = []

    def fetch(self, api, params):
        self.calls.append(dict(params))
        lower = date.fromisoformat(params.get("date", params.get("start_date")))
        upper = date.fromisoformat(params.get("date", params.get("end_date")))
        days = [
            lower + timedelta(days=index) for index in range((upper - lower).days + 1)
        ]
        rows = [(day, asset) for day in days for asset in range(self.assets_per_day)]
        offset = params.get("offset", 0)
        limit = params.get("limit", 2 if self.truncate else len(rows))
        rows = rows[offset : offset + limit]
        return pl.DataFrame(
            {
                "trade_date": [day.strftime("%Y%m%d") for day, _ in rows],
                "ts_code": ["A" * self.payload_size + str(asset) for _, asset in rows],
                "price": [float(day.day) for day, _ in rows],
            },
            schema={"trade_date": pl.String, "ts_code": pl.String, "price": pl.Float64},
        )


@pytest.mark.parametrize("truncate", [False, True])
def test_oversized_initial_range_splits_claimed_scopes_without_losing_coverage(
    tmp_path, truncate
):
    provider = RangeProvider(truncate=truncate)
    lake = DataLake.open(
        data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake"
    )
    lake.catalog.sources.register(provider)
    lake.raw.register(
        DatasetSpec(
            "daily",
            "by_date",
            source="fake",
            date_kind="calendar",
            date_param="date",
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
            request_options={
                "pagination": "offset",
                "page_size": 2 if truncate else 1000,
                "daily_range_backfill": {
                    "start_param": "start_date",
                    "end_param": "end_date",
                    "row_limit": 2 if truncate else 1000,
                    "max_scopes": 1024,
                },
            },
        )
    )
    progress = []
    result = lake.raw.update_many(
        datasets=["daily"],
        source="fake",
        start="2025-01-01",
        end="2025-01-04",
        mode="initialize",
        config=ExecutionOptions(workers=2, max_buffer_bytes=4096),
        progress_callback=progress.append,
    )
    report = result.runs[0]
    assert report.status == "success"
    assert report.success_count == report.rows_committed == 4
    assert report.failure_count == result.remaining_scope_count == 0
    assert report.request_count == len(provider.calls)
    frame = (
        lake.raw.read("daily", source="fake", view="latest")
        .collect()
        .sort("source_time")
    )
    assert frame["price"].to_list() == [1.0, 2.0, 3.0, 4.0]
    assert frame.height == 4
    assert all(
        scope["status"] == "success"
        for scope in lake.integrity.update_scopes("daily", source="fake")
    )
    assert max(event.completed for event in progress) == 4
    assert all(
        event.completed <= event.total == 4
        for event in progress
        if event.phase not in {"planning", "discovery"}
    )
    assert any(call["start_date"] == call["end_date"] for call in provider.calls)


def test_memory_split_single_day_retains_nested_range_pagination(tmp_path):
    provider = RangeProvider(payload_size=250, truncate=True, assets_per_day=3)
    lake = DataLake.open(
        data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake"
    )
    lake.catalog.sources.register(provider)
    lake.raw.register(
        DatasetSpec(
            "daily",
            "by_date",
            source="fake",
            date_kind="calendar",
            date_param="date",
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
            request_options={
                "daily_range_backfill": {
                    "start_param": "start_date",
                    "end_param": "end_date",
                    "row_limit": 2,
                }
            },
        )
    )
    result = lake.raw.update_many(
        datasets=["daily"],
        source="fake",
        start="2025-01-01",
        end="2025-01-04",
        mode="initialize",
        config=ExecutionOptions(workers=2, max_buffer_bytes=4096),
    )
    report = result.runs[0]
    assert report.status == "success"
    assert report.success_count == 4
    assert report.rows_committed == 12
    assert report.request_count == len(provider.calls)
    frame = lake.raw.read("daily", source="fake", view="latest").collect()
    assert frame.height == 12
    assert frame.group_by("source_time").len()["len"].to_list() == [3] * 4
    assert all(
        scope["status"] == "success"
        for scope in lake.integrity.update_scopes("daily", source="fake")
    )
    assert any(call.get("offset", 0) > 0 for call in provider.calls)


def test_irreducible_single_day_still_fails_without_publishing(tmp_path):
    provider = RangeProvider(payload_size=3000)
    lake = DataLake.open(
        data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake"
    )
    lake.catalog.sources.register(provider)
    lake.raw.register(
        DatasetSpec(
            "daily",
            "by_date",
            source="fake",
            date_kind="calendar",
            date_param="date",
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
            request_options={
                "daily_range_backfill": {
                    "start_param": "start_date",
                    "end_param": "end_date",
                    "row_limit": 1000,
                }
            },
        )
    )
    result = lake.raw.update_many(
        datasets=["daily"],
        source="fake",
        start="2025-01-01",
        end="2025-01-02",
        mode="initialize",
        config=ExecutionOptions(workers=2, max_buffer_bytes=4096),
    )
    assert result.runs[0].status == "failed"
    assert result.runs[0].rows_committed == 0
    assert lake.raw.manifest("daily", source="fake") == []
    assert all(
        scope["status"] == "invalid"
        for scope in lake.integrity.update_scopes("daily", source="fake")
    )


def test_cancel_after_memory_split_preserves_commits_and_resumes(tmp_path):
    class CancelProvider(RangeProvider):
        cancelled = False
        stopped_once = False

        def fetch(self, api, params):
            frame = super().fetch(api, params)
            if "start_date" in params and frame.height == 2 and not self.stopped_once:
                self.stopped_once = self.cancelled = True
            return frame

    provider = CancelProvider()
    lake = DataLake.open(
        data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake"
    )
    lake.catalog.sources.register(provider)
    lake.raw.register(
        DatasetSpec(
            "daily",
            "by_date",
            source="fake",
            date_kind="calendar",
            date_param="date",
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
            request_options={
                "daily_range_backfill": {
                    "start_param": "start_date",
                    "end_param": "end_date",
                    "row_limit": 1000,
                }
            },
        )
    )
    first = lake.raw.update_many(
        datasets=["daily"],
        source="fake",
        start="2025-01-01",
        end="2025-01-04",
        mode="initialize",
        config=ExecutionOptions(workers=1, max_buffer_bytes=4096),
        cancel_requested=lambda: provider.cancelled,
    )
    assert first.runs[0].status == "cancelled"
    assert first.runs[0].rows_committed == 2
    provider.cancelled = False
    resumed = lake.raw.update_many(
        datasets=["daily"],
        source="fake",
        start="2025-01-01",
        end="2025-01-04",
        mode="initialize",
        config=ExecutionOptions(workers=1, max_buffer_bytes=4096),
    )
    assert resumed.runs[0].status == "success"
    assert resumed.runs[0].rows_committed == 2
    assert lake.raw.read("daily", source="fake", view="latest").collect().height == 4
