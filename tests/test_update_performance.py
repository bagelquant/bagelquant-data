from __future__ import annotations

import threading
from collections import Counter
from datetime import date
from types import SimpleNamespace

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.core import ValidationError
from bagelquant_data.core.hashing import stable_bucket
from bagelquant_data.core.request import RequestContext
from bagelquant_data.pipeline import commit as commit_module
from bagelquant_data.pipeline import update as update_module
from bagelquant_data.pipeline.scopes import LedgerRequest
from bagelquant_data.pipeline.update import (
    DatasetUpdateWork,
    _initial_asset_task_groups,
    _partition_affinity_order,
    _validate_response,
)
from bagelquant_data.storage import parquet as parquet_module
from bagelquant_data.storage.atomic import atomic_write_parquet


class DailySource:
    name = "custom"

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        value = str(request["date"])
        return pl.DataFrame(
            {"trade_date": [value.replace("-", "")], "ts_code": ["000001.SZ"]}
        )


class AssetSource:
    name = "custom"

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        asset = str(request["id"])
        return pl.DataFrame(
            {
                "ann_date": ["20240630", "20250630"],
                "ts_code": [asset, asset],
                "value": [1.0, 2.0],
            }
        )


class WideAssetSource:
    name = "custom"

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        asset = str(request["id"])
        payload = f"{asset}:" + ("x" * 20_000)
        return pl.DataFrame(
            {
                "ann_date": ["20240630", "20250630"],
                "ts_code": [asset, asset],
                "payload": [payload, payload],
            }
        )


class RevisionAssetSource:
    name = "custom"

    def __init__(self) -> None:
        self.dates = ["20240630", "20250630"]

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        asset = str(request["id"])
        return pl.DataFrame(
            {
                "ann_date": self.dates,
                "ts_code": [asset] * len(self.dates),
                "value": list(range(len(self.dates))),
            }
        )


def _daily_spec() -> DatasetSpec:
    return DatasetSpec(
        "daily",
        "by_daily",
        calendar="trade_cal",
        field_mappings={"trade_date": "time", "ts_code": "asset_id"},
    )


def _daily_lake(tmp_path, dates: list[str]) -> DataLake:
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(DailySource())
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame({"time": dates, "is_open": [1] * len(dates)}),
    )
    lake.admin.datasets.register(_daily_spec())
    return lake


def test_scheduler_bounds_in_flight_and_reports_timings(tmp_path) -> None:
    lake = _daily_lake(tmp_path, [f"202501{day:02d}" for day in range(1, 11)])

    report = lake.update.dataset(
        "daily",
        source="custom",
        end="2025-01-10",
        workers=2,
        max_in_flight=3,
        progress=False,
    )

    assert report.peak_in_flight <= 3
    assert report.elapsed_seconds > 0
    assert report.fetch_seconds >= 0
    assert report.metadata_seconds >= 0


def test_one_batch_rewrites_one_shared_partition(tmp_path) -> None:
    lake = _daily_lake(tmp_path, [f"202501{day:02d}" for day in range(1, 5)])

    report = lake.update.dataset(
        "daily",
        source="custom",
        end="2025-01-04",
        batch_size=100,
        progress=False,
    )

    assert report.commit_count == 1
    assert report.partitions_rewritten == 1
    assert lake.query.query("daily", source="custom").collect().height == 4


def test_single_page_response_avoids_request_level_concat(
    tmp_path, monkeypatch
) -> None:
    lake = _daily_lake(tmp_path, ["20250102"])
    from bagelquant_data.pipeline import update as update_module

    original = update_module.concat_compatible_frames
    calls = 0

    def tracked_concat(frames):
        nonlocal calls
        calls += 1
        return original(frames)

    monkeypatch.setattr(update_module, "concat_compatible_frames", tracked_concat)

    lake.update.dataset(
        "daily",
        source="custom",
        end="2025-01-02",
        progress=False,
    )

    assert calls == 1


@pytest.mark.parametrize(
    ("frame", "message"),
    [
        (
            pl.DataFrame(
                {
                    "trade_date": ["20250102", "20250102"],
                    "ts_code": ["A", "B"],
                    "value": [1.0, 2.0],
                }
            ),
            None,
        ),
        (
            pl.DataFrame(
                {
                    "trade_date": ["20250102", "20250102"],
                    "ts_code": ["A", None],
                    "value": [1.0, 2.0],
                }
            ),
            "response contains null primary keys",
        ),
        (
            pl.DataFrame(
                {
                    "trade_date": ["bad-date", "20250102"],
                    "ts_code": ["A", "B"],
                    "value": [1.0, 2.0],
                }
            ),
            "response contains invalid dates",
        ),
        (
            pl.DataFrame(
                {
                    "trade_date": ["20250102", "20250103"],
                    "ts_code": ["A", "B"],
                    "value": [1.0, 2.0],
                }
            ),
            "response contains dates outside requested date 2025-01-02",
        ),
        (
            pl.DataFrame(
                {
                    "trade_date": ["20250102", "20250102"],
                    "ts_code": ["A", "B"],
                    "value": pl.Series([None, None], dtype=pl.Float64),
                }
            ),
            "response payload is entirely null",
        ),
    ],
)
def test_daily_response_validation_uses_equivalent_vector_checks(
    frame: pl.DataFrame, message: str | None
) -> None:
    request = LedgerRequest({"date": "2025-01-02"}, target_end="2025-01-02")

    assert _validate_response(_daily_spec(), request, frame) == message


@pytest.mark.parametrize(
    ("frame", "message"),
    [
        (
            pl.DataFrame(
                {
                    "f_ann_date": ["20240630", "20250630"],
                    "ann_date": ["20240601", "20250601"],
                    "ts_code": ["A", "A"],
                    "value": [1.0, 2.0],
                }
            ),
            None,
        ),
        (
            pl.DataFrame(
                {
                    "f_ann_date": ["20240630", "20250630"],
                    "ann_date": ["20240601", "20250601"],
                    "ts_code": ["A", "B"],
                    "value": [1.0, 2.0],
                }
            ),
            "response contains assets other than A",
        ),
        (
            pl.DataFrame(
                {
                    "f_ann_date": ["20240630", "20250630"],
                    "ann_date": ["invalid", "20250601"],
                    "ts_code": ["A", "A"],
                    "value": [1.0, 2.0],
                }
            ),
            "response contains invalid request dates",
        ),
        (
            pl.DataFrame(
                {
                    "f_ann_date": ["20240630", "20250630"],
                    "ann_date": ["20230601", "20250601"],
                    "ts_code": ["A", "A"],
                    "value": [1.0, 2.0],
                }
            ),
            "response contains dates outside requested range",
        ),
    ],
)
def test_asset_response_validation_uses_equivalent_vector_checks(
    frame: pl.DataFrame, message: str | None
) -> None:
    spec = DatasetSpec(
        "income",
        "by_asset",
        asset_list="stock_basic",
        request_date_field="ann_date",
        field_mappings={"f_ann_date": "time", "ts_code": "asset_id"},
    )
    request = LedgerRequest(
        {"id": "A", "start": "2024-01-01", "end": "2025-12-31"},
        target_end="2025-12-31",
    )

    assert _validate_response(spec, request, frame) == message


def test_default_buffer_does_not_commit_every_hundred_requests(tmp_path) -> None:
    dates = (
        pl.date_range(
            pl.date(2025, 1, 1),
            pl.date(2025, 5, 30),
            interval="1d",
            eager=True,
        )
        .dt.strftime("%Y%m%d")
        .to_list()
    )
    lake = _daily_lake(tmp_path, dates)

    report = lake.update.dataset(
        "daily",
        source="custom",
        end="2025-05-30",
        progress=False,
    )

    assert report.request_count == 150
    assert report.commit_count == 1
    assert report.partitions_rewritten == 5


def test_by_asset_default_rewrites_each_touched_partition_once(tmp_path) -> None:
    assets = [f"{index:06d}.SZ" for index in range(150)]
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(AssetSource())
    lake.ingest(
        DatasetSpec(
            "stock_basic",
            "general",
            field_mappings={"ts_code": "asset_id"},
        ),
        pl.DataFrame(
            {
                "ts_code": assets,
                "list_date": ["20240101"] * len(assets),
            }
        ),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "income",
            "by_asset",
            asset_list="stock_basic",
            field_mappings={"ann_date": "time", "ts_code": "asset_id"},
        )
    )

    report = lake.update.dataset(
        "income",
        source="custom",
        start="2024-01-01",
        end="2025-12-31",
        workers=8,
        progress=False,
    )

    files = lake.admin.status.files("income", source="custom")
    assert report.request_count == 150
    assert report.commit_count == len(
        {stable_bucket(asset, 32) for asset in assets}
    )
    assert report.partitions_rewritten == len(files)


def test_partition_affinity_keeps_retries_first_and_sorts_each_band() -> None:
    spec = DatasetSpec(
        "income",
        "by_asset",
        asset_list="stock_basic",
        asset_bucket_count=4,
        field_mappings={"ann_date": "time", "ts_code": "asset_id"},
    )
    work = DatasetUpdateWork(
        spec,
        RequestContext(source="custom", dataset="income"),
        (),
    )
    assets = ["000003.SZ", "000001.SZ", "000004.SZ", "000002.SZ"]
    tasks = [
        (
            work,
            LedgerRequest(
                {"id": asset},
                request_kind="retry" if index == 2 else "forward",
            ),
        )
        for index, asset in enumerate(assets)
    ]

    ordered = _partition_affinity_order(tasks)

    assert ordered[0][1].request_kind == "retry"
    forward_assets = [str(task[1].params["id"]) for task in ordered[1:]]
    assert forward_assets == sorted(
        forward_assets,
        key=lambda asset: (stable_bucket(asset, 4), asset),
    )
    groups = _initial_asset_task_groups(ordered)
    assert all(
        len(
            {
                (
                    request.request_kind != "retry",
                    stable_bucket(str(request.params["id"]), 4),
                )
                for _, request in group
            }
        )
        == 1
        for group in groups
    )
    assert [task for group in groups for task in group] == ordered


def test_clean_asset_build_writes_each_physical_partition_once(
    tmp_path, monkeypatch
) -> None:
    assets = [f"{index:06d}.SZ" for index in range(320)]
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(WideAssetSource())
    lake.ingest(
        DatasetSpec(
            "stock_basic",
            "general",
            field_mappings={"ts_code": "asset_id"},
        ),
        pl.DataFrame(
            {
                "ts_code": assets,
                "list_date": ["20240101"] * len(assets),
            }
        ),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "income",
            "by_asset",
            asset_list="stock_basic",
            field_mappings={"ann_date": "time", "ts_code": "asset_id"},
        )
    )
    writes: Counter[str] = Counter()
    deduplicate_calls = 0
    coverage_calls = 0
    original = parquet_module.ParquetStore.write_partition_file_result
    original_deduplicate = commit_module._deduplicate
    original_coverage = commit_module._coverage

    def tracked_write(self, spec, frame, relative_path, *args, **kwargs):
        writes[relative_path.as_posix()] += 1
        return original(self, spec, frame, relative_path, *args, **kwargs)

    def tracked_deduplicate(frame, spec):
        nonlocal deduplicate_calls
        deduplicate_calls += 1
        return original_deduplicate(frame, spec)

    def tracked_coverage(frames, spec):
        nonlocal coverage_calls
        coverage_calls += 1
        return original_coverage(frames, spec)

    def forbidden_read(*args, **kwargs):
        raise AssertionError("clean partitions must not read parquet")

    monkeypatch.setattr(
        parquet_module.ParquetStore,
        "write_partition_file_result",
        tracked_write,
    )
    monkeypatch.setattr(commit_module, "_deduplicate", tracked_deduplicate)
    monkeypatch.setattr(commit_module, "_coverage", tracked_coverage)
    monkeypatch.setattr(commit_module, "_read_existing", forbidden_read)

    report = lake.update.dataset(
        "income",
        source="custom",
        start="2024-01-01",
        end="2025-12-31",
        workers=1,
        max_buffer_mb=1,
        progress=False,
    )
    files = lake.admin.status.files("income", source="custom")

    assert report.commit_count == len(
        {stable_bucket(asset, 32) for asset in assets}
    )
    assert writes
    assert max(writes.values()) == 1
    assert sum(writes.values()) == len(files)
    assert report.partitions_rewritten == len(files)
    assert deduplicate_calls == report.commit_count
    assert coverage_calls == report.commit_count


def test_update_reuses_one_internal_writer_pool_across_bucket_commits(
    tmp_path, monkeypatch
) -> None:
    assets = [f"{index:06d}.SZ" for index in range(80)]
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(AssetSource())
    lake.ingest(
        DatasetSpec(
            "stock_basic",
            "general",
            field_mappings={"ts_code": "asset_id"},
        ),
        pl.DataFrame(
            {
                "ts_code": assets,
                "list_date": ["20240101"] * len(assets),
            }
        ),
    )
    for dataset in ("income", "balancesheet"):
        lake.admin.datasets.register(
            DatasetSpec(
                dataset,
                "by_asset",
                asset_list="stock_basic",
                field_mappings={"ann_date": "time", "ts_code": "asset_id"},
            )
        )
    real_executor = update_module.ThreadPoolExecutor
    writer_pool_count = 0

    def tracked_executor(*args, **kwargs):
        nonlocal writer_pool_count
        if kwargs.get("thread_name_prefix") == "bagelquant-parquet":
            writer_pool_count += 1
        return real_executor(*args, **kwargs)

    monkeypatch.setattr(
        update_module,
        "ThreadPoolExecutor",
        tracked_executor,
    )

    report = lake.update.datasets(
        ["income", "balancesheet"],
        source="custom",
        start="2024-01-01",
        end="2025-12-31",
        confirm=False,
        progress=False,
    )

    assert len(report.runs) == 2
    assert report.commit_count > 2
    assert writer_pool_count == 1


def test_response_validation_runs_in_fetch_worker_but_sqlite_stays_on_scheduler(
    tmp_path, monkeypatch
) -> None:
    lake = _daily_lake(tmp_path, ["20250102", "20250103"])
    main_thread = threading.get_ident()
    validation_threads: list[int] = []
    transition_threads: list[int] = []
    original_validation = update_module._validate_response
    original_transition = lake.metadata.transition_update_scopes

    def tracked_validation(spec, request, frame):
        validation_threads.append(threading.get_ident())
        return original_validation(spec, request, frame)

    def tracked_transition(*args, **kwargs):
        transition_threads.append(threading.get_ident())
        return original_transition(*args, **kwargs)

    monkeypatch.setattr(
        update_module,
        "_validate_response",
        tracked_validation,
    )
    monkeypatch.setattr(
        lake.metadata,
        "transition_update_scopes",
        tracked_transition,
    )

    lake.update.dataset(
        "daily",
        source="custom",
        start="2025-01-02",
        end="2025-01-03",
        workers=2,
        progress=False,
    )

    assert validation_threads
    assert all(thread != main_thread for thread in validation_threads)
    assert transition_threads
    assert set(transition_threads) == {main_thread}


def test_response_processing_failure_settles_fetches_without_committing(
    tmp_path, monkeypatch
) -> None:
    lake = _daily_lake(
        tmp_path,
        ["20250102", "20250103", "20250104", "20250105"],
    )
    calls = 0
    lock = threading.Lock()

    def fail_one_validation(*args, **kwargs):
        nonlocal calls
        with lock:
            calls += 1
            call = calls
        if call == 2:
            raise RuntimeError("response processing fault")
        return None

    monkeypatch.setattr(
        update_module,
        "_validate_response",
        fail_one_validation,
    )

    with pytest.raises(RuntimeError, match="response processing fault"):
        lake.update.dataset(
            "daily",
            source="custom",
            start="2025-01-02",
            end="2025-01-05",
            workers=4,
            max_in_flight=4,
            progress=False,
        )

    scopes = lake.admin.status.update_scopes(
        dataset="daily", source="custom"
    )
    assert 2 <= calls <= 4
    assert {str(row["status"]) for row in scopes} <= {"failed", "pending"}
    assert lake.admin.status.files("daily", source="custom") == []


def test_clean_asset_build_still_splits_an_oversized_bucket(tmp_path) -> None:
    assets = [f"{index:06d}.SZ" for index in range(80)]
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(WideAssetSource())
    lake.ingest(
        DatasetSpec(
            "stock_basic",
            "general",
            field_mappings={"ts_code": "asset_id"},
        ),
        pl.DataFrame(
            {
                "ts_code": assets,
                "list_date": ["20240101"] * len(assets),
            }
        ),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "income",
            "by_asset",
            asset_list="stock_basic",
            asset_bucket_count=1,
            field_mappings={"ann_date": "time", "ts_code": "asset_id"},
        )
    )

    report = lake.update.dataset(
        "income",
        source="custom",
        start="2024-01-01",
        end="2025-12-31",
        workers=1,
        max_buffer_mb=1,
        progress=False,
    )
    files = lake.admin.status.files("income", source="custom")

    assert report.commit_count > 1
    assert report.partitions_rewritten > len(files)


def test_committed_coverage_does_not_rescan_canonical_parquet(
    tmp_path, monkeypatch
) -> None:
    lake = _daily_lake(tmp_path, ["20250102"])

    def forbidden_query(*args, **kwargs):
        raise AssertionError("commit coverage must not query canonical parquet")

    monkeypatch.setattr(
        "bagelquant_data.query.raw.RawQueryService.query",
        forbidden_query,
    )

    first = lake.update.dataset(
        "daily",
        source="custom",
        start="2025-01-02",
        end="2025-01-02",
        today="2025-01-02",
        progress=False,
    )
    second = lake.update.dataset(
        "daily",
        source="custom",
        start="2025-01-02",
        end="2025-01-02",
        today="2025-01-03",
        progress=False,
    )
    scope = lake.admin.status.update_scopes(
        dataset="daily", source="custom"
    )[0]

    assert first.partitions_rewritten == 1
    assert second.partitions_skipped == 1
    assert scope["data_max_time"] == "2025-01-02"


def test_asset_commit_coverage_uses_maximum_across_years(
    tmp_path, monkeypatch
) -> None:
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(AssetSource())
    lake.ingest(
        DatasetSpec(
            "stock_basic",
            "general",
            field_mappings={"ts_code": "asset_id"},
        ),
        pl.DataFrame({"ts_code": ["A"], "list_date": ["20240101"]}),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "income",
            "by_asset",
            asset_list="stock_basic",
            field_mappings={"ann_date": "time", "ts_code": "asset_id"},
        )
    )

    def forbidden_query(*args, **kwargs):
        raise AssertionError("commit coverage must not query canonical parquet")

    monkeypatch.setattr(
        "bagelquant_data.query.raw.RawQueryService.query",
        forbidden_query,
    )

    lake.update.dataset(
        "income",
        source="custom",
        start="2024-01-01",
        end="2025-12-31",
        today="2025-12-31",
        progress=False,
    )
    scope = lake.admin.status.update_scopes(
        dataset="income", source="custom"
    )[0]

    assert scope["data_max_time"] == "2025-06-30"


def test_asset_revision_of_older_year_does_not_regress_watermark(
    tmp_path,
) -> None:
    source = RevisionAssetSource()
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    lake.ingest(
        DatasetSpec(
            "stock_basic",
            "general",
            field_mappings={"ts_code": "asset_id"},
        ),
        pl.DataFrame({"ts_code": ["A"], "list_date": ["20240101"]}),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "income",
            "by_asset",
            asset_list="stock_basic",
            revision_refresh_days=1,
            revision_lookback_days=730,
            field_mappings={"ann_date": "time", "ts_code": "asset_id"},
        )
    )
    lake.update.dataset(
        "income",
        source="custom",
        start="2024-01-01",
        end="2025-12-31",
        today="2025-12-31",
        progress=False,
    )
    source.dates = ["20240630"]

    lake.update.dataset(
        "income",
        source="custom",
        start="2024-01-01",
        end="2025-12-31",
        today="2026-01-02",
        progress=False,
    )
    scope = lake.admin.status.update_scopes(
        dataset="income", source="custom"
    )[0]

    assert scope["data_max_time"] == "2025-06-30"
    assert (
        lake.query.query("income", source="custom").collect()["time"].max()
        == date(2025, 6, 30)
    )


def test_atomic_validation_does_not_replace_existing_file(
    tmp_path, monkeypatch
) -> None:
    path = tmp_path / "data.parquet"
    original = pl.DataFrame({"value": [1]})
    atomic_write_parquet(original, path)
    monkeypatch.setattr(
        "bagelquant_data.storage.atomic.pq.ParquetFile",
        lambda _: SimpleNamespace(
            metadata=SimpleNamespace(num_rows=999, num_columns=1),
            schema_arrow=original.to_arrow().schema,
            close=lambda: None,
        ),
    )

    with pytest.raises(ValidationError):
        atomic_write_parquet(pl.DataFrame({"value": [2]}), path)

    assert pl.read_parquet(path).equals(original)
