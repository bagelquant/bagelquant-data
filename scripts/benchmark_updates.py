"""Deterministic JSON benchmark for current PIT update and query paths.

Run with: ``python scripts/benchmark_updates.py --requests 2000 --workers 8``.
Every case uses a temporary lake and an in-memory provider.
"""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl

from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.core.hashing import frame_content_hash
from bagelquant_data.query.scanner import manifest_rows


class DelayedSource:
    name = "benchmark"

    def __init__(self, delay: float) -> None:
        self.delay = delay

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        time.sleep(self.delay)
        value = str(request["trade_date"])
        return pl.DataFrame(
            {"trade_date": [value.replace("-", "")], "ts_code": ["000001.SZ"]}
        )


class BulkDailySource:
    name = "bulk_daily"

    def __init__(self, rows_per_request: int) -> None:
        self.rows_per_request = rows_per_request
        self.assets = [f"{index:06d}.SZ" for index in range(rows_per_request)]
        self.values = {
            f"value_{column:02d}": [
                float((row * (column + 1)) % 100_003)
                for row in range(rows_per_request)
            ]
            for column in range(20)
        }

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        value = str(request["trade_date"]).replace("-", "")
        return pl.DataFrame(
            {
                "trade_date": [value] * self.rows_per_request,
                "ts_code": self.assets,
                **self.values,
            }
        )


class ParameterizedDailySource:
    name = "parameterized_daily"

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        asset = str(request["ts_code"])
        announcement = str(request["ann_date"]).replace("-", "")
        return pl.DataFrame(
            {
                "ann_date": [announcement],
                "ts_code": [asset],
                "end_date": [announcement],
                **{
                    f"value_{column:02d}": [
                        float((int(asset[:6]) * (column + 1)) % 100_003)
                    ]
                    for column in range(32)
                },
            }
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--delay", type=float, default=0.0)
    parser.add_argument("--hash-rows", type=int, default=125_000)
    parser.add_argument("--bulk-daily-requests", type=int, default=120)
    parser.add_argument("--bulk-daily-rows", type=int, default=1_000)
    parser.add_argument("--parameter-assets", type=int, default=320)
    parser.add_argument("--parameter-days", type=int, default=34)
    args = parser.parse_args()
    positive = {
        "requests": args.requests,
        "hash rows": args.hash_rows,
        "bulk daily requests": args.bulk_daily_requests,
        "bulk daily rows": args.bulk_daily_rows,
        "parameter assets": args.parameter_assets,
        "parameter days": args.parameter_days,
    }
    if any(value <= 0 for value in positive.values()):
        parser.error("all benchmark sizes must be positive")

    results = {"hash": _hash_benchmark(args.hash_rows)}
    with tempfile.TemporaryDirectory(
        dir=Path.cwd(), ignore_cleanup_errors=True
    ) as root:
        lake = DataLake.open(root)
        results["update"] = _update_benchmark(
            lake,
            requests=args.requests,
            workers=args.workers,
            delay=args.delay,
        )
        results["bulk_daily"] = _bulk_daily_benchmark(
            lake,
            requests=args.bulk_daily_requests,
            rows_per_request=args.bulk_daily_rows,
            workers=args.workers,
        )
        results["parameterized_daily"] = _parameterized_daily_benchmark(
            lake,
            asset_count=args.parameter_assets,
            day_count=args.parameter_days,
            workers=args.workers,
        )
        results["query"] = _query_benchmark(lake)
    print(json.dumps(results, indent=2, sort_keys=True))


def _hash_benchmark(row_count: int) -> dict[str, object]:
    frame = pl.DataFrame(
        {
            "asset_id": [f"{index % 5000:06d}.SZ" for index in range(row_count)],
            **{
                f"value_{column:02d}": [
                    float((index * (column + 1)) % 100_003)
                    for index in range(row_count)
                ]
                for column in range(21)
            },
        }
    )
    started = time.perf_counter()
    content_hash = frame_content_hash(frame)
    return {
        "columns": frame.width,
        "content_hash": content_hash,
        "rows": row_count,
        "seconds": time.perf_counter() - started,
    }


def _update_benchmark(
    lake: DataLake, *, requests: int, workers: int, delay: float
) -> dict[str, object]:
    first_day = date(2020, 1, 1)
    days = [first_day + timedelta(days=index) for index in range(requests)]
    dates = [value.strftime("%Y%m%d") for value in days]
    lake.admin.sources.register(DelayedSource(delay))
    lake.ingest(
        DatasetSpec("trade_cal", "general", source="benchmark"),
        pl.DataFrame({"time": dates, "is_open": [1] * len(dates)}),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "daily",
            "by_daily",
            source="benchmark",
            calendar="trade_cal",
            date_kind="trading",
            date_param="trade_date",
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
        )
    )
    report = lake.update.dataset(
        "daily",
        source="benchmark",
        start=days[0],
        end=days[-1],
        today=days[-1],
        mode="initialize",
        ingested_at=datetime(2026, 1, 1, tzinfo=UTC),
        workers=workers,
        batch_size=requests,
    )
    noop = lake.update.dataset(
        "daily",
        source="benchmark",
        start=days[-1],
        end=days[-1],
        today=days[-1] + timedelta(days=1),
        workers=workers,
    )
    ideal = delay * ((requests + workers - 1) // workers)
    return {
        "commit_seconds": report.commit_seconds,
        "elapsed_seconds": report.elapsed_seconds,
        "ideal_fetch_seconds": ideal,
        "no_op_partitions_rewritten": noop.partitions_rewritten,
        "no_op_partitions_skipped": noop.partitions_skipped,
        "no_op_seconds": noop.elapsed_seconds,
        "partitions_rewritten": report.partitions_rewritten,
        "peak_in_flight": report.peak_in_flight,
        "planning_seconds": report.planning_seconds,
        "requests": requests,
        "workers": workers,
    }


def _bulk_daily_benchmark(
    lake: DataLake,
    *,
    requests: int,
    rows_per_request: int,
    workers: int,
) -> dict[str, object]:
    first_day = date(2020, 1, 1)
    days = [first_day + timedelta(days=index) for index in range(requests)]
    lake.admin.sources.register(BulkDailySource(rows_per_request))
    lake.admin.datasets.register(
        DatasetSpec(
            "bulk_daily",
            "by_daily",
            source="bulk_daily",
            date_kind="calendar",
            date_param="trade_date",
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
        )
    )
    report = lake.update.dataset(
        "bulk_daily",
        source="bulk_daily",
        start=days[0],
        end=days[-1],
        today=days[-1],
        mode="initialize",
        ingested_at=datetime(2026, 1, 1, tzinfo=UTC),
        workers=workers,
    )
    files = lake.metadata.manifest("bulk_daily", "bulk_daily")
    return {
        "bytes_written": report.bytes_written,
        "columns": 22,
        "commit_seconds": report.commit_seconds,
        "elapsed_seconds": report.elapsed_seconds,
        "partitions_rewritten": report.partitions_rewritten,
        "requests": requests,
        "rows": requests * rows_per_request,
        "rows_per_request": rows_per_request,
        "unique_partitions": len(files),
    }


def _parameterized_daily_benchmark(
    lake: DataLake, *, asset_count: int, day_count: int, workers: int
) -> dict[str, object]:
    assets = [f"{index:06d}.SZ" for index in range(asset_count)]
    first_day = date(2020, 1, 1)
    last_day = first_day + timedelta(days=day_count - 1)
    lake.admin.sources.register(ParameterizedDailySource())
    lake.admin.datasets.register(
        DatasetSpec(
            "parameterized_income",
            "by_daily",
            source="parameterized_daily",
            date_kind="calendar",
            date_param="ann_date",
            source_api_param_sets=({"ts_code": assets},),
            primary_key_extra=("end_date",),
            field_mappings={"ann_date": "time", "ts_code": "asset_id"},
        )
    )
    report = lake.update.dataset(
        "parameterized_income",
        source="parameterized_daily",
        start=first_day,
        end=last_day,
        today=last_day,
        mode="initialize",
        ingested_at=datetime(2026, 1, 1, tzinfo=UTC),
        workers=workers,
    )
    files = lake.metadata.manifest("parameterized_daily", "parameterized_income")
    return {
        "assets": asset_count,
        "bytes_written": report.bytes_written,
        "commit_count": report.commit_count,
        "commit_seconds": report.commit_seconds,
        "days": day_count,
        "elapsed_seconds": report.elapsed_seconds,
        "partitions_rewritten": report.partitions_rewritten,
        "rows": asset_count * day_count,
        "unique_partitions": len(files),
    }


def _query_benchmark(lake: DataLake) -> dict[str, object]:
    monthly_dates = [
        date(1999 + index // 12, index % 12 + 1, 1) for index in range(319)
    ]
    spec = DatasetSpec(
        "monthly_query",
        "by_daily",
        source="benchmark",
        date_kind="calendar",
        date_param="trade_date",
        field_mappings={"trade_date": "time", "ts_code": "asset_id"},
    )
    lake.ingest(
        spec,
        pl.DataFrame(
            {
                "trade_date": [value.strftime("%Y%m%d") for value in monthly_dates],
                "ts_code": ["000001.SZ"] * len(monthly_dates),
                "value": list(range(len(monthly_dates))),
            }
        ),
        mode="initialize",
        ingested_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    month = monthly_dates[len(monthly_dates) // 2]
    month_end = (month.replace(day=28) + timedelta(days=4)).replace(
        day=1
    ) - timedelta(days=1)
    selected_rows = manifest_rows(
        lake.metadata,
        "benchmark",
        "monthly_query",
        start=month,
        end=month_end,
    )
    started = time.perf_counter()
    result = lake.query.query(
        "monthly_query",
        source="benchmark",
        start=month,
        end=month_end,
    ).collect()
    return {
        "query_files": len(selected_rows),
        "query_rows": result.height,
        "query_seconds": time.perf_counter() - started,
        "total_files": len(lake.metadata.manifest("benchmark", "monthly_query")),
    }


if __name__ == "__main__":
    main()
