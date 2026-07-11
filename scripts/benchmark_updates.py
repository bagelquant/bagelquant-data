"""Deterministic full-update scheduler benchmark.

Run with: ``python scripts/benchmark_updates.py --requests 100 --workers 8``.
"""

from __future__ import annotations

import argparse
import tempfile
import time
from pathlib import Path

import polars as pl

from bagelquant_data import DataLake, DatasetSpec


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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--delay", type=float, default=0.02)
    args = parser.parse_args()
    dates = (
        pl.date_range(
            pl.date(2025, 1, 1),
            pl.date(2025, 1, 1) + pl.duration(days=args.requests - 1),
            interval="1d",
            eager=True,
        )
        .dt.strftime("%Y%m%d")
        .to_list()
    )
    with tempfile.TemporaryDirectory(
        dir=Path.cwd(), ignore_cleanup_errors=True
    ) as root:
        lake = DataLake.open(root)
        lake.admin.sources.register(DelayedSource(args.delay))
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
        report = lake.update.dataset(
            "daily",
            source="benchmark",
            workers=args.workers,
            batch_size=args.requests,
            progress=False,
        )
    ideal = args.delay * ((args.requests + args.workers - 1) // args.workers)
    print(
        f"elapsed={report.elapsed_seconds:.3f}s ideal_fetch={ideal:.3f}s peak_in_flight={report.peak_in_flight} commits={report.commit_count} partitions={report.partitions_rewritten}"
    )


if __name__ == "__main__":
    main()
