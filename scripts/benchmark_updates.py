"""Deterministic JSON benchmark for update planning, hashing, and lake I/O.

Run with: ``python scripts/benchmark_updates.py --requests 2000 --workers 8``.
The benchmark includes scaled daily and wide by-asset full rebuilds and uses
only a temporary lake with in-memory fake providers.
"""

from __future__ import annotations

import argparse
import io
import json
import tempfile
import time
from contextlib import redirect_stdout
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.core.hashing import frame_content_hash, stable_bucket
from bagelquant_data.query.scanner import manifest_rows


class DelayedSource:
    name = "benchmark"

    def __init__(self, delay: float) -> None:
        self.delay = delay

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        time.sleep(self.delay)
        value = str(request["date"])
        return pl.DataFrame(
            {"trade_date": [value.replace("-", "")], "ts_code": ["000001.SZ"]}
        )


class BulkDailySource:
    name = "bulk_daily"

    def __init__(self, rows_per_request: int) -> None:
        self.rows_per_request = rows_per_request
        self.assets = [
            f"{index:06d}.SZ" for index in range(self.rows_per_request)
        ]
        self.values = {
            f"value_{column:02d}": [
                float((row * (column + 1)) % 100_003)
                for row in range(self.rows_per_request)
            ]
            for column in range(20)
        }

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        value = str(request["date"]).replace("-", "")
        return pl.DataFrame(
            {
                "trade_date": [value] * self.rows_per_request,
                "ts_code": self.assets,
                **self.values,
            }
        )


class BulkAssetSource:
    name = "bulk_asset"

    def __init__(self, rows_per_asset: int) -> None:
        self.rows_per_asset = rows_per_asset
        self.dates = [
            f"{2009 + index // 2}{'0630' if index % 2 == 0 else '1231'}"
            for index in range(self.rows_per_asset)
        ]
        self.values = {
            f"value_{column:02d}": [
                float((row * (column + 1)) % 100_003)
                for row in range(self.rows_per_asset)
            ]
            for column in range(96)
        }

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        asset = str(request["id"])
        return pl.DataFrame(
            {
                "f_ann_date": self.dates,
                "ann_date": self.dates,
                "ts_code": [asset] * self.rows_per_asset,
                "end_date": self.dates,
                **self.values,
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
    parser.add_argument("--bulk-assets", type=int, default=320)
    parser.add_argument("--bulk-asset-rows", type=int, default=34)
    args = parser.parse_args()
    if args.bulk_daily_requests <= 0 or args.bulk_daily_rows <= 0:
        parser.error("bulk daily requests and rows must be positive")
    if args.bulk_assets <= 0:
        parser.error("bulk assets must be positive")
    if not 1 <= args.bulk_asset_rows <= 34:
        parser.error("bulk asset rows must be between 1 and 34")

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
        results["bulk_asset"] = _bulk_asset_benchmark(
            lake,
            asset_count=args.bulk_assets,
            rows_per_asset=args.bulk_asset_rows,
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
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
        )
    )
    with redirect_stdout(io.StringIO()):
        report = lake.update.dataset(
            "daily",
            source="benchmark",
            start=days[0],
            end=days[-1],
            today=days[-1],
            workers=workers,
            batch_size=requests,
            progress=False,
        )
        noop = lake.update.dataset(
            "daily",
            source="benchmark",
            start=days[-1],
            end=days[-1],
            today=days[-1] + timedelta(days=1),
            workers=workers,
            progress=False,
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


def _query_benchmark(lake: DataLake) -> dict[str, object]:
    monthly_dates = [
        date(1999 + index // 12, index % 12 + 1, 1) for index in range(319)
    ]
    daily_spec = DatasetSpec(
        "daily_query",
        "by_daily",
        source="benchmark",
        calendar="trade_cal",
        field_mappings={"trade_date": "time", "ts_code": "asset_id"},
    )
    lake.ingest(
        daily_spec,
        pl.DataFrame(
            {
                "trade_date": [value.strftime("%Y%m%d") for value in monthly_dates],
                "ts_code": ["000001.SZ"] * len(monthly_dates),
                "value": list(range(len(monthly_dates))),
            }
        ),
    )
    month = monthly_dates[len(monthly_dates) // 2]
    month_end = (month.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(
        days=1
    )
    date_rows = manifest_rows(
        lake.metadata,
        "benchmark",
        "daily_query",
        start=month,
        end=month_end,
    )
    started = time.perf_counter()
    lake.query.query(
        "daily_query",
        source="benchmark",
        start=month,
        end=month_end,
    ).collect()
    date_seconds = time.perf_counter() - started

    target = "000001.SZ"
    other = next(
        f"{value:06d}.SZ"
        for value in range(2, 10_000)
        if stable_bucket(f"{value:06d}.SZ", 32) != stable_bucket(target, 32)
    )
    years = range(2009, 2026)
    asset_spec = DatasetSpec(
        "income_query",
        "by_asset",
        source="benchmark",
        asset_list="stock_basic",
        field_mappings={"ann_date": "time", "ts_code": "asset_id"},
    )
    lake.ingest(
        asset_spec,
        pl.DataFrame(
            {
                "ann_date": [f"{year}0630" for year in years for _ in (target, other)],
                "ts_code": [asset for _ in years for asset in (target, other)],
                "value": [float(index) for index in range(len(years) * 2)],
            }
        ),
    )
    asset_rows = manifest_rows(
        lake.metadata,
        "benchmark",
        "income_query",
        buckets={stable_bucket(target, 32)},
    )
    started = time.perf_counter()
    lake.query.query("income_query", source="benchmark", assets=[target]).collect()
    asset_seconds = time.perf_counter() - started
    return {
        "asset_query_files": len(asset_rows),
        "asset_query_seconds": asset_seconds,
        "date_query_files": len(date_rows),
        "date_query_seconds": date_seconds,
        "total_asset_files": len(lake.metadata.manifest("benchmark", "income_query")),
        "total_daily_files": len(lake.metadata.manifest("benchmark", "daily_query")),
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
    dates = [value.strftime("%Y%m%d") for value in days]
    lake.admin.sources.register(BulkDailySource(rows_per_request))
    lake.ingest(
        DatasetSpec(
            "bulk_trade_cal",
            "general",
            source="bulk_daily",
        ),
        pl.DataFrame({"time": dates, "is_open": [1] * len(dates)}),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "bulk_daily",
            "by_daily",
            source="bulk_daily",
            calendar="bulk_trade_cal",
            field_mappings={"trade_date": "time", "ts_code": "asset_id"},
        )
    )
    with redirect_stdout(io.StringIO()):
        report = lake.update.dataset(
            "bulk_daily",
            source="bulk_daily",
            start=days[0],
            end=days[-1],
            today=days[-1],
            workers=workers,
            progress=False,
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


def _bulk_asset_benchmark(
    lake: DataLake,
    *,
    asset_count: int,
    rows_per_asset: int,
    workers: int,
) -> dict[str, object]:
    assets = [f"{index:06d}.SZ" for index in range(asset_count)]
    lake.admin.sources.register(BulkAssetSource(rows_per_asset))
    lake.ingest(
        DatasetSpec(
            "bulk_stock_basic",
            "general",
            source="bulk_asset",
            field_mappings={"ts_code": "asset_id"},
        ),
        pl.DataFrame(
            {
                "ts_code": assets,
                "list_date": ["20090101"] * asset_count,
            }
        ),
    )
    lake.admin.datasets.register(
        DatasetSpec(
            "bulk_income",
            "by_asset",
            source="bulk_asset",
            asset_list="bulk_stock_basic",
            request_date_field="ann_date",
            primary_key_extra=("end_date",),
            field_mappings={"f_ann_date": "time", "ts_code": "asset_id"},
        )
    )
    with redirect_stdout(io.StringIO()):
        report = lake.update.dataset(
            "bulk_income",
            source="bulk_asset",
            start="2009-01-01",
            end="2025-12-31",
            today="2025-12-31",
            workers=workers,
            max_buffer_mb=1,
            progress=False,
        )
    files = lake.metadata.manifest("bulk_asset", "bulk_income")
    unique_partitions = len(files)
    return {
        "assets": asset_count,
        "bytes_written": report.bytes_written,
        "columns": 100,
        "commit_count": report.commit_count,
        "commit_seconds": report.commit_seconds,
        "elapsed_seconds": report.elapsed_seconds,
        "partitions_rewritten": report.partitions_rewritten,
        "rewrite_amplification": (
            0.0
            if unique_partitions == 0
            else report.partitions_rewritten / unique_partitions
        ),
        "rows": asset_count * rows_per_asset,
        "rows_per_asset": rows_per_asset,
        "unique_partitions": unique_partitions,
    }


if __name__ == "__main__":
    main()
