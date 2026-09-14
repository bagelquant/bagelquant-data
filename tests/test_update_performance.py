from __future__ import annotations

import threading
from types import SimpleNamespace

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.core import ValidationError
from bagelquant_data.pipeline import update as update_module
from bagelquant_data.pipeline.scopes import LedgerRequest
from bagelquant_data.pipeline.update import (
    _validate_response,
)
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
            "response contains dates outside requested date 2025-01-02 to 2025-01-02",
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


def test_daily_response_validation_can_allow_all_null_optional_payload() -> None:
    spec = DatasetSpec(
        "suspend_d",
        "by_daily",
        calendar="trade_cal",
        primary_key_extra=("suspend_type",),
        field_mappings={"trade_date": "time", "ts_code": "asset_id"},
    )
    request = LedgerRequest({"date": "2025-01-02"}, target_end="2025-01-02")
    valid = pl.DataFrame(
        {
            "trade_date": ["20250102", "20250102"],
            "ts_code": ["A", "B"],
            "suspend_type": ["S", "S"],
            "suspend_timing": pl.Series([None, None], dtype=pl.String),
        }
    )
    null_key = valid.with_columns(pl.lit(None).alias("suspend_type"))
    wrong_date = valid.with_columns(pl.lit("20250103").alias("trade_date"))

    assert _validate_response(spec, request, valid) == (
        "response payload is entirely null"
    )
    assert (
        _validate_response(
            spec,
            request,
            valid,
            allow_all_null_payload=True,
        )
        is None
    )
    assert _validate_response(
        spec,
        request,
        null_key,
        allow_all_null_payload=True,
    ) == "response contains null primary keys"
    assert _validate_response(
        spec,
        request,
        wrong_date,
        allow_all_null_payload=True,
    ) == "response contains dates outside requested date 2025-01-02 to 2025-01-02"


def test_daily_response_validation_allows_declared_nullable_extra_keys() -> None:
    spec = DatasetSpec(
        "financial",
        "by_daily",
        date_kind="calendar",
        primary_key_extra=("period", "company_type"),
        nullable_primary_key_extra=("company_type",),
        field_mappings={"announcement_date": "time", "code": "asset_id"},
    )
    request = LedgerRequest(
        {"announcement_date": "2025-01-02"}, target_end="2025-01-02"
    )
    frame = pl.DataFrame(
        {
            "announcement_date": ["20250102"],
            "code": ["A"],
            "period": ["20241231"],
            "company_type": pl.Series([None], dtype=pl.String),
            "value": [1.0],
        }
    )

    assert _validate_response(spec, request, frame) is None




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
    )

    assert report.request_count == 150
    assert report.commit_count == 1
    assert report.partitions_rewritten == 5










def test_response_validation_runs_in_fetch_worker_but_sqlite_stays_on_scheduler(
    tmp_path, monkeypatch
) -> None:
    lake = _daily_lake(tmp_path, ["20250102", "20250103"])
    main_thread = threading.get_ident()
    validation_threads: list[int] = []
    transition_threads: list[int] = []
    original_validation = update_module._validate_response
    original_transition = lake.metadata.transition_update_scopes

    def tracked_validation(spec, request, frame, **kwargs):
        validation_threads.append(threading.get_ident())
        return original_validation(spec, request, frame, **kwargs)

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
        )

    scopes = lake.admin.status.update_scopes(
        dataset="daily", source="custom"
    )
    assert 2 <= calls <= 4
    assert {str(row["status"]) for row in scopes} <= {"failed", "pending"}
    assert lake.admin.status.files("daily", source="custom") == []




def test_coverage_inspection_does_not_read_numerical_rows(tmp_path, monkeypatch) -> None:
    lake = _daily_lake(tmp_path, ["20250102"])
    lake.update.dataset("daily", source="custom", start="2025-01-02", end="2025-01-02")
    def forbidden_query(*args, **kwargs):
        raise AssertionError("coverage must not read daily Parquet")
    monkeypatch.setattr("bagelquant_data.query.raw.RawQueryService.query", forbidden_query)
    assert lake.admin.coverage("daily", source="custom", start="2025-01-02", end="2025-01-02")["complete"]


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
