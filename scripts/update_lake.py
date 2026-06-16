from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path
from time import perf_counter

from bagelquant_data import DataLake, TushareSource


DEFAULT_ROOT = Path("/Users/eric/data")
DEFAULT_START = "2000-01-01"
INCREMENTAL_CATEGORIES = {"market", "financial_statement", "financial_event"}
DEFAULT_ORDER = (
    "stock_basic",
    "trade_cal",
    "daily",
    "daily_basic",
    "adj_factor",
    "income",
    "balancesheet",
    "cashflow",
    "forecast",
    "express",
)
MARKET_CATEGORIES = {"market"}
FINANCIAL_CATEGORIES = {"financial_statement", "financial_event"}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Update Tushare datasets in a BagelQuant data lake.")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="Data lake root directory.")
    parser.add_argument(
        "--datasets",
        nargs="+",
        help="Dataset names to update. Defaults to all enabled Tushare datasets in the recommended order.",
    )
    parser.add_argument(
        "--start",
        default=DEFAULT_START,
        help=f"Start date. Defaults to {DEFAULT_START}.",
    )
    parser.add_argument(
        "--end",
        default=date.today().isoformat(),
        help="End date. Defaults to today.",
    )
    parser.add_argument("--assets", nargs="+", help="Asset IDs for by-asset datasets, for example 000001.SZ.")
    parser.add_argument("--workers", type=int, default=8, help="Parallel API fetch workers.")
    parser.add_argument("--max-retries", type=int, default=3, help="Retries per API call/page.")
    parser.add_argument(
        "--retry-backoff-seconds",
        type=float,
        default=2.0,
        help="Base retry backoff in seconds.",
    )
    parser.add_argument("--no-progress", action="store_true", help="Disable per-dataset progress bars.")
    args = parser.parse_args(argv)

    lake = DataLake.open(args.root)
    lake.sources.register(TushareSource())
    datasets = args.datasets or _prompt_for_datasets(lake)
    start = perf_counter()
    reports = []
    for dataset in datasets:
        effective_start = _effective_start(lake, dataset, source="tushare", fallback_start=args.start)
        if _date_after(effective_start, args.end):
            print(f"{dataset}: skipped, local maximum_time={effective_start} is after end={args.end}")
            continue
        print(f"{dataset}: date range {effective_start} to {args.end}")
        report = lake.update.dataset(
            dataset,
            source="tushare",
            start=effective_start,
            end=args.end,
            assets=args.assets,
            workers=args.workers,
            max_retries=args.max_retries,
            retry_backoff_seconds=args.retry_backoff_seconds,
            progress=not args.no_progress,
        )
        reports.append(report)
        print(
            f"{report.dataset}: status={report.status} "
            f"rows_downloaded={report.rows_downloaded} rows_committed={report.rows_committed} "
            f"requests={report.request_count} failures={report.failure_count}"
        )
        if report.error_message:
            print(f"  error: {report.error_message}")
    elapsed = perf_counter() - start
    print(f"Updated {len(reports)} dataset(s) in {elapsed:.2f}s ({elapsed / 60:.2f}m)")
    return 0


def _enabled_tushare_datasets(lake: DataLake) -> list[str]:
    enabled = {row["name"] for row in lake.datasets.list("tushare") if row["enabled"]}
    ordered = [dataset for dataset in DEFAULT_ORDER if dataset in enabled]
    extras = sorted(enabled - set(ordered))
    return ordered + extras


def _enabled_tushare_datasets_by_category(lake: DataLake, categories: set[str]) -> list[str]:
    enabled = {
        row["name"]
        for row in lake.datasets.list("tushare")
        if row["enabled"] and row.get("category") in categories
    }
    ordered = [dataset for dataset in DEFAULT_ORDER if dataset in enabled]
    extras = sorted(enabled - set(ordered))
    return ordered + extras


def _prompt_for_datasets(lake: DataLake) -> list[str]:
    all_datasets = _enabled_tushare_datasets(lake)
    market_datasets = _enabled_tushare_datasets_by_category(lake, MARKET_CATEGORIES)
    financial_datasets = _enabled_tushare_datasets_by_category(lake, FINANCIAL_CATEGORIES)
    choices = {
        "1": ("update all", all_datasets),
        "2": ("update market datasets", market_datasets),
        "3": ("update financial datasets", financial_datasets),
    }

    print("Select datasets to update:")
    print("1. update all")
    print("2. update market datasets:")
    _print_dataset_list(market_datasets)
    print("3. update financial datasets:")
    _print_dataset_list(financial_datasets)

    while True:
        choice = input("Enter choice [1-3]: ").strip()
        if choice in choices:
            label, datasets = choices[choice]
            if not datasets:
                print(f"No enabled datasets found for: {label}")
                continue
            print(f"Selected {label}: {', '.join(datasets)}")
            return datasets
        print("Invalid choice. Enter 1, 2, or 3.", file=sys.stderr)


def _print_dataset_list(datasets: list[str]) -> None:
    if not datasets:
        print("  (none)")
        return
    for dataset in datasets:
        print(f"  - {dataset}")


def _effective_start(lake: DataLake, dataset: str, *, source: str, fallback_start: str) -> str:
    spec = lake.datasets.get(dataset, source=source)
    if spec.category not in INCREMENTAL_CATEGORIES:
        return fallback_start
    status = lake.status.dataset(dataset, source=source)
    maximum_time = status.get("maximum_time")
    if maximum_time:
        return str(maximum_time)
    return fallback_start


def _date_after(left: str, right: str) -> bool:
    return _parse_date(left) > _parse_date(right)


def _parse_date(value: str) -> date:
    text = str(value)
    if len(text) >= 10:
        text = text[:10]
    return datetime.strptime(text, "%Y-%m-%d").date()


if __name__ == "__main__":
    raise SystemExit(main())
