from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any

import polars as pl


FINANCIAL_DATASETS = {
    "income",
    "balancesheet",
    "cashflow",
}

EVENT_DATASETS = {
    "forecast",
    "express",
}

MARKET_DATASETS = {
    "daily",
    "daily_basic",
    "adj_factor",
}


@dataclass
class DatasetSummary:
    dataset: str
    path: str
    file_count: int
    total_size_gb: float
    average_file_mb: float
    median_file_mb: float
    minimum_file_mb: float
    maximum_file_mb: float
    small_file_count: int
    row_count: int | None
    column_count: int | None
    asset_count: int | None
    minimum_time: str | None
    maximum_time: str | None
    minimum_period: str | None
    maximum_period: str | None
    exact_duplicate_count: int | None
    time_asset_duplicate_count: int | None
    business_key_duplicate_count: int | None
    schema_consistent: bool
    warnings: list[str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect a BagelQuant Parquet data lake.",
    )

    parser.add_argument(
        "--root",
        type=Path,
        default=Path("data/lake/tushare"),
        help="Root directory containing dataset directories.",
    )

    parser.add_argument(
        "--datasets",
        nargs="*",
        default=None,
        help="Optional dataset names to inspect.",
    )

    parser.add_argument(
        "--small-file-mb",
        type=float,
        default=1.0,
        help="Files smaller than this value are counted as small files.",
    )

    parser.add_argument(
        "--sample-files",
        type=int,
        default=100,
        help=(
            "Maximum number of files used for schema consistency checks. "
            "Use 0 to inspect every file."
        ),
    )

    parser.add_argument(
        "--deep-duplicates",
        action="store_true",
        help=(
            "Perform expensive full-row exact duplicate checks. "
            "This may require substantial memory and time."
        ),
    )

    parser.add_argument(
        "--partition-details",
        action="store_true",
        help="Print per-file and per-partition row and asset counts.",
    )

    parser.add_argument(
        "--json-output",
        type=Path,
        default=None,
        help="Optional path for JSON summary output.",
    )

    return parser.parse_args()


def human_size(num_bytes: int) -> str:
    value = float(num_bytes)

    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:,.2f} {unit}"
        value /= 1024

    return f"{value:,.2f} TB"


def discover_datasets(
    root: Path,
    requested: list[str] | None,
) -> list[Path]:
    if not root.exists():
        raise FileNotFoundError(f"Data lake root does not exist: {root}")

    if requested:
        directories = [root / name for name in requested]
        missing = [path for path in directories if not path.exists()]

        if missing:
            missing_text = ", ".join(str(path) for path in missing)
            raise FileNotFoundError(
                f"Requested dataset directories do not exist: {missing_text}"
            )

        return directories

    return sorted(
        path
        for path in root.iterdir()
        if path.is_dir()
        and any(path.rglob("*.parquet"))
    )


def list_parquet_files(dataset_path: Path) -> list[Path]:
    return sorted(dataset_path.rglob("*.parquet"))


def calculate_file_statistics(
    files: list[Path],
    small_file_mb: float,
) -> dict[str, Any]:
    sizes = sorted(path.stat().st_size for path in files)

    if not sizes:
        return {
            "total_bytes": 0,
            "average_bytes": 0.0,
            "median_bytes": 0.0,
            "minimum_bytes": 0,
            "maximum_bytes": 0,
            "small_file_count": 0,
        }

    count = len(sizes)
    midpoint = count // 2

    if count % 2 == 0:
        median_bytes = (
            sizes[midpoint - 1] + sizes[midpoint]
        ) / 2
    else:
        median_bytes = float(sizes[midpoint])

    small_threshold = small_file_mb * 1024**2

    return {
        "total_bytes": sum(sizes),
        "average_bytes": sum(sizes) / count,
        "median_bytes": median_bytes,
        "minimum_bytes": sizes[0],
        "maximum_bytes": sizes[-1],
        "small_file_count": sum(
            size < small_threshold
            for size in sizes
        ),
    }


def scan_files(files: list[Path]) -> pl.LazyFrame:
    paths = [str(path) for path in files]

    return pl.scan_parquet(
        paths,
        missing_columns="insert",
        extra_columns="ignore",
    )


def collect_scalar(frame: pl.DataFrame, column: str) -> Any:
    if column not in frame.columns or frame.height == 0:
        return None

    return frame[column][0]


def stringify_value(value: Any) -> str | None:
    if value is None:
        return None

    return str(value)


def schema_signature(schema: pl.Schema) -> tuple[tuple[str, str], ...]:
    return tuple(
        (name, str(dtype))
        for name, dtype in schema.items()
    )


def check_schema_consistency(
    files: list[Path],
    sample_files: int,
) -> tuple[bool, Counter[tuple[tuple[str, str], ...]]]:
    if sample_files <= 0:
        selected = files
    else:
        selected = files[:sample_files]

    signatures: Counter[tuple[tuple[str, str], ...]] = Counter()

    for path in selected:
        try:
            schema = pl.scan_parquet(path).collect_schema()
            signatures[schema_signature(schema)] += 1
        except Exception as exc:
            signatures[(("__READ_ERROR__", str(exc)),)] += 1

    return len(signatures) <= 1, signatures


def build_basic_summary(
    lf: pl.LazyFrame,
    schema: pl.Schema,
) -> dict[str, Any]:
    expressions: list[pl.Expr] = [
        pl.len().alias("row_count"),
    ]

    if "asset_id" in schema:
        expressions.append(
            pl.col("asset_id")
            .n_unique()
            .alias("asset_count")
        )

    if "time" in schema:
        expressions.extend(
            [
                pl.col("time").min().alias("minimum_time"),
                pl.col("time").max().alias("maximum_time"),
            ]
        )

    if "period" in schema:
        expressions.extend(
            [
                pl.col("period").min().alias("minimum_period"),
                pl.col("period").max().alias("maximum_period"),
            ]
        )

    result = lf.select(expressions).collect(
        engine="streaming"
    )

    return {
        "row_count": collect_scalar(result, "row_count"),
        "asset_count": collect_scalar(result, "asset_count"),
        "minimum_time": stringify_value(
            collect_scalar(result, "minimum_time")
        ),
        "maximum_time": stringify_value(
            collect_scalar(result, "maximum_time")
        ),
        "minimum_period": stringify_value(
            collect_scalar(result, "minimum_period")
        ),
        "maximum_period": stringify_value(
            collect_scalar(result, "maximum_period")
        ),
    }


def count_key_duplicates(
    lf: pl.LazyFrame,
    keys: list[str],
) -> int:
    result = (
        lf.group_by(keys)
        .len()
        .filter(pl.col("len") > 1)
        .select(
            (pl.col("len") - 1)
            .sum()
            .fill_null(0)
            .alias("duplicate_count")
        )
        .collect(engine="streaming")
    )

    value = collect_scalar(result, "duplicate_count")
    return int(value or 0)


def count_exact_duplicates(
    lf: pl.LazyFrame,
    schema: pl.Schema,
) -> int:
    columns = list(schema.keys())

    result = (
        lf.group_by(columns)
        .len()
        .filter(pl.col("len") > 1)
        .select(
            (pl.col("len") - 1)
            .sum()
            .fill_null(0)
            .alias("duplicate_count")
        )
        .collect(engine="streaming")
    )

    value = collect_scalar(result, "duplicate_count")
    return int(value or 0)


def detect_business_key(
    dataset: str,
    schema: pl.Schema,
) -> list[str] | None:
    candidates: list[list[str]]

    if dataset in FINANCIAL_DATASETS:
        candidates = [
            [
                "asset_id",
                "period",
                "report_type",
                "comp_type",
                "time",
            ],
            [
                "asset_id",
                "period",
                "report_type",
                "time",
            ],
            [
                "asset_id",
                "period",
                "time",
            ],
        ]
    elif dataset == "forecast":
        candidates = [
            [
                "asset_id",
                "period",
                "time",
                "type",
            ],
            [
                "asset_id",
                "period",
                "time",
            ],
        ]
    elif dataset == "express":
        candidates = [
            [
                "asset_id",
                "period",
                "time",
            ],
        ]
    elif dataset in MARKET_DATASETS:
        candidates = [
            [
                "asset_id",
                "time",
            ],
        ]
    else:
        candidates = []

    for candidate in candidates:
        if all(column in schema for column in candidate):
            return candidate

    return None


def inspect_asset_history(
    lf: pl.LazyFrame,
    schema: pl.Schema,
) -> pl.DataFrame | None:
    if "asset_id" not in schema:
        return None

    aggregations: list[pl.Expr] = [
        pl.len().alias("rows"),
    ]

    if "period" in schema:
        aggregations.append(
            pl.col("period")
            .n_unique()
            .alias("periods")
        )

    if "time" in schema:
        aggregations.extend(
            [
                pl.col("time").min().alias("min_time"),
                pl.col("time").max().alias("max_time"),
            ]
        )

    grouped = lf.group_by("asset_id").agg(aggregations)

    summary_expressions: list[pl.Expr] = [
        pl.len().alias("assets"),
        pl.col("rows").min().alias("min_rows_per_asset"),
        pl.col("rows").median().alias("median_rows_per_asset"),
        pl.col("rows").mean().alias("mean_rows_per_asset"),
        pl.col("rows").max().alias("max_rows_per_asset"),
    ]

    if "period" in schema:
        summary_expressions.extend(
            [
                pl.col("periods")
                .min()
                .alias("min_periods_per_asset"),
                pl.col("periods")
                .median()
                .alias("median_periods_per_asset"),
                pl.col("periods")
                .mean()
                .alias("mean_periods_per_asset"),
                pl.col("periods")
                .max()
                .alias("max_periods_per_asset"),
            ]
        )

    return grouped.select(summary_expressions).collect(
        engine="streaming"
    )


def inspect_partition_details(
    files: list[Path],
) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []

    for path in files:
        try:
            lf = pl.scan_parquet(path)
            schema = lf.collect_schema()

            expressions: list[pl.Expr] = [
                pl.len().alias("rows"),
            ]

            if "asset_id" in schema:
                expressions.append(
                    pl.col("asset_id")
                    .n_unique()
                    .alias("assets")
                )

            if "time" in schema:
                expressions.extend(
                    [
                        pl.col("time")
                        .min()
                        .alias("minimum_time"),
                        pl.col("time")
                        .max()
                        .alias("maximum_time"),
                    ]
                )

            result = lf.select(expressions).collect(
                engine="streaming"
            )

            details.append(
                {
                    "path": str(path),
                    "size_mb": path.stat().st_size / 1024**2,
                    "rows": collect_scalar(result, "rows"),
                    "assets": collect_scalar(result, "assets"),
                    "minimum_time": stringify_value(
                        collect_scalar(result, "minimum_time")
                    ),
                    "maximum_time": stringify_value(
                        collect_scalar(result, "maximum_time")
                    ),
                }
            )
        except Exception as exc:
            details.append(
                {
                    "path": str(path),
                    "error": str(exc),
                }
            )

    return details


def generate_warnings(
    dataset: str,
    summary: dict[str, Any],
    file_stats: dict[str, Any],
    asset_history: pl.DataFrame | None,
    schema_consistent: bool,
    time_asset_duplicates: int | None,
    business_key_duplicates: int | None,
) -> list[str]:
    warnings: list[str] = []

    row_count = summary.get("row_count") or 0
    asset_count = summary.get("asset_count") or 0

    if row_count == 0:
        warnings.append("Dataset contains zero rows.")

    if asset_count == 0 and dataset not in {"trade_cal"}:
        warnings.append("No asset_id values were found.")

    if not schema_consistent:
        warnings.append(
            "Parquet files do not have a consistent schema."
        )

    if file_stats["small_file_count"] > 0:
        warnings.append(
            f"{file_stats['small_file_count']:,} files are below "
            "the configured small-file threshold."
        )

    if dataset in MARKET_DATASETS and time_asset_duplicates:
        warnings.append(
            f"Found {time_asset_duplicates:,} duplicate rows "
            "under the expected (time, asset_id) key."
        )

    if dataset in FINANCIAL_DATASETS:
        if summary.get("minimum_period") is None:
            warnings.append(
                "Financial dataset does not contain a usable period column."
            )

        if asset_history is not None and asset_history.height:
            median_rows = collect_scalar(
                asset_history,
                "median_rows_per_asset",
            )

            median_periods = collect_scalar(
                asset_history,
                "median_periods_per_asset",
            )

            if median_rows is not None and median_rows <= 2:
                warnings.append(
                    "Median rows per asset is extremely low; "
                    "the dataset may contain only the latest records."
                )

            if median_periods is not None and median_periods <= 2:
                warnings.append(
                    "Median report periods per asset is extremely low; "
                    "historical reports may be missing."
                )

    if business_key_duplicates:
        warnings.append(
            f"Found {business_key_duplicates:,} duplicate rows "
            "under the detected business key. These may be revisions "
            "or unresolved duplicate records."
        )

    if summary.get("minimum_time") == summary.get("maximum_time"):
        warnings.append(
            "The dataset contains only one distinct time range endpoint; "
            "historical coverage may be incomplete."
        )

    return warnings


def inspect_dataset(
    dataset_path: Path,
    args: argparse.Namespace,
) -> tuple[DatasetSummary, dict[str, Any]]:
    dataset = dataset_path.name
    files = list_parquet_files(dataset_path)

    if not files:
        raise ValueError(f"No Parquet files found in {dataset_path}")

    file_stats = calculate_file_statistics(
        files,
        args.small_file_mb,
    )

    schema_consistent, schema_signatures = (
        check_schema_consistency(
            files,
            args.sample_files,
        )
    )

    lf = scan_files(files)
    schema = lf.collect_schema()

    basic = build_basic_summary(lf, schema)

    time_asset_duplicate_count: int | None = None

    if "time" in schema and "asset_id" in schema:
        time_asset_duplicate_count = count_key_duplicates(
            lf,
            ["time", "asset_id"],
        )

    business_key = detect_business_key(
        dataset,
        schema,
    )

    business_key_duplicate_count: int | None = None

    if business_key is not None:
        business_key_duplicate_count = count_key_duplicates(
            lf,
            business_key,
        )

    exact_duplicate_count: int | None = None

    if args.deep_duplicates:
        exact_duplicate_count = count_exact_duplicates(
            lf,
            schema,
        )

    asset_history = inspect_asset_history(
        lf,
        schema,
    )

    warnings = generate_warnings(
        dataset=dataset,
        summary=basic,
        file_stats=file_stats,
        asset_history=asset_history,
        schema_consistent=schema_consistent,
        time_asset_duplicates=time_asset_duplicate_count,
        business_key_duplicates=business_key_duplicate_count,
    )

    summary = DatasetSummary(
        dataset=dataset,
        path=str(dataset_path),
        file_count=len(files),
        total_size_gb=round(
            file_stats["total_bytes"] / 1024**3,
            6,
        ),
        average_file_mb=round(
            file_stats["average_bytes"] / 1024**2,
            6,
        ),
        median_file_mb=round(
            file_stats["median_bytes"] / 1024**2,
            6,
        ),
        minimum_file_mb=round(
            file_stats["minimum_bytes"] / 1024**2,
            6,
        ),
        maximum_file_mb=round(
            file_stats["maximum_bytes"] / 1024**2,
            6,
        ),
        small_file_count=file_stats["small_file_count"],
        row_count=basic["row_count"],
        column_count=len(schema),
        asset_count=basic["asset_count"],
        minimum_time=basic["minimum_time"],
        maximum_time=basic["maximum_time"],
        minimum_period=basic["minimum_period"],
        maximum_period=basic["maximum_period"],
        exact_duplicate_count=exact_duplicate_count,
        time_asset_duplicate_count=time_asset_duplicate_count,
        business_key_duplicate_count=business_key_duplicate_count,
        schema_consistent=schema_consistent,
        warnings=warnings,
    )

    details: dict[str, Any] = {
        "schema": {
            name: str(dtype)
            for name, dtype in schema.items()
        },
        "schema_signatures": [
            {
                "count": count,
                "schema": dict(signature),
            }
            for signature, count in schema_signatures.items()
        ],
        "business_key": business_key,
        "asset_history": (
            asset_history.to_dicts()
            if asset_history is not None
            else None
        ),
    }

    if args.partition_details:
        details["partitions"] = inspect_partition_details(files)

    return summary, details


def print_dataset_report(
    summary: DatasetSummary,
    details: dict[str, Any],
) -> None:
    print()
    print("=" * 88)
    print(f"DATASET: {summary.dataset}")
    print("=" * 88)

    print(f"Path:                {summary.path}")
    print(f"Files:               {summary.file_count:,}")
    print(f"Total size:          {summary.total_size_gb:,.6f} GB")
    print(f"Average file size:   {summary.average_file_mb:,.3f} MB")
    print(f"Median file size:    {summary.median_file_mb:,.3f} MB")
    print(f"Minimum file size:   {summary.minimum_file_mb:,.3f} MB")
    print(f"Maximum file size:   {summary.maximum_file_mb:,.3f} MB")
    print(f"Small files:         {summary.small_file_count:,}")
    print(f"Rows:                {summary.row_count or 0:,}")
    print(f"Columns:             {summary.column_count or 0:,}")
    print(f"Assets:              {summary.asset_count or 0:,}")
    print(f"Minimum time:        {summary.minimum_time}")
    print(f"Maximum time:        {summary.maximum_time}")
    print(f"Minimum period:      {summary.minimum_period}")
    print(f"Maximum period:      {summary.maximum_period}")
    print(f"Schema consistent:   {summary.schema_consistent}")

    if summary.exact_duplicate_count is None:
        print("Exact duplicates:    not checked")
    else:
        print(
            f"Exact duplicates:    "
            f"{summary.exact_duplicate_count:,}"
        )

    if summary.time_asset_duplicate_count is not None:
        print(
            f"(time, asset) dupes: "
            f"{summary.time_asset_duplicate_count:,}"
        )

    business_key = details.get("business_key")

    if business_key:
        print(
            "Detected key:         "
            + ", ".join(business_key)
        )
        print(
            f"Business-key dupes:  "
            f"{summary.business_key_duplicate_count or 0:,}"
        )

    asset_history = details.get("asset_history")

    if asset_history:
        print()
        print("Asset history summary:")

        for key, value in asset_history[0].items():
            if isinstance(value, float):
                print(f"  {key:28s} {value:,.2f}")
            elif isinstance(value, int):
                print(f"  {key:28s} {value:,}")
            else:
                print(f"  {key:28s} {value}")

    print()
    print("Schema:")

    for name, dtype in details["schema"].items():
        print(f"  {name:40s} {dtype}")

    if summary.warnings:
        print()
        print("Warnings:")

        for warning in summary.warnings:
            print(f"  - {warning}")
    else:
        print()
        print("Warnings: none")

    partitions = details.get("partitions")

    if partitions:
        print()
        print("Partition details:")

        for item in partitions:
            if "error" in item:
                print(
                    f"  ERROR {item['path']}: {item['error']}"
                )
                continue

            print(
                f"  {item['path']} | "
                f"{item['size_mb']:.2f} MB | "
                f"rows={item.get('rows')} | "
                f"assets={item.get('assets')} | "
                f"time={item.get('minimum_time')} "
                f"to {item.get('maximum_time')}"
            )


def print_overall_summary(
    summaries: list[DatasetSummary],
) -> None:
    print()
    print("#" * 88)
    print("OVERALL DATA LAKE SUMMARY")
    print("#" * 88)

    total_size_gb = sum(
        item.total_size_gb
        for item in summaries
    )

    total_files = sum(
        item.file_count
        for item in summaries
    )

    total_rows = sum(
        item.row_count or 0
        for item in summaries
    )

    print(f"Datasets:    {len(summaries):,}")
    print(f"Files:       {total_files:,}")
    print(f"Total size:  {total_size_gb:,.6f} GB")
    print(f"Total rows:  {total_rows:,}")

    print()
    print(
        f"{'Dataset':22s}"
        f"{'Files':>10s}"
        f"{'Size GB':>12s}"
        f"{'Rows':>16s}"
        f"{'Assets':>12s}"
        f"{'Columns':>10s}"
    )

    print("-" * 82)

    for item in sorted(
        summaries,
        key=lambda value: value.total_size_gb,
        reverse=True,
    ):
        print(
            f"{item.dataset:22s}"
            f"{item.file_count:10,d}"
            f"{item.total_size_gb:12.4f}"
            f"{(item.row_count or 0):16,d}"
            f"{(item.asset_count or 0):12,d}"
            f"{(item.column_count or 0):10,d}"
        )

    warning_count = sum(
        len(item.warnings)
        for item in summaries
    )

    print()
    print(f"Total warnings: {warning_count:,}")

    for item in summaries:
        for warning in item.warnings:
            print(f"  [{item.dataset}] {warning}")


def write_json_report(
    path: Path,
    summaries: list[DatasetSummary],
    all_details: dict[str, Any],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    payload = {
        "summaries": [
            asdict(summary)
            for summary in summaries
        ],
        "details": all_details,
    }

    path.write_text(
        json.dumps(
            payload,
            indent=2,
            ensure_ascii=False,
            default=str,
        ),
        encoding="utf-8",
    )


def main() -> int:
    args = parse_args()

    try:
        dataset_paths = discover_datasets(
            args.root,
            args.datasets,
        )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    if not dataset_paths:
        print(
            f"No Parquet datasets found under {args.root}",
            file=sys.stderr,
        )
        return 1

    summaries: list[DatasetSummary] = []
    all_details: dict[str, Any] = {}

    for dataset_path in dataset_paths:
        try:
            summary, details = inspect_dataset(
                dataset_path,
                args,
            )
        except Exception as exc:
            print()
            print("=" * 88)
            print(f"DATASET FAILED: {dataset_path.name}")
            print("=" * 88)
            print(f"{type(exc).__name__}: {exc}")
            continue

        summaries.append(summary)
        all_details[summary.dataset] = details

        print_dataset_report(
            summary,
            details,
        )

    if not summaries:
        print(
            "All dataset inspections failed.",
            file=sys.stderr,
        )
        return 1

    print_overall_summary(summaries)

    if args.json_output is not None:
        write_json_report(
            args.json_output,
            summaries,
            all_details,
        )

        print()
        print(
            f"JSON report written to: "
            f"{args.json_output}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())