from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.core.exceptions import DatasetNotFoundError


FLOAT64_COLUMNS = {
    "daily": (
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "change",
        "pct_chg",
        "vol",
        "amount",
    ),
    "daily_basic": (
        "close",
        "turnover_rate",
        "turnover_rate_f",
        "volume_ratio",
        "pe",
        "pe_ttm",
        "pb",
        "ps",
        "ps_ttm",
        "dv_ratio",
        "dv_ttm",
        "total_share",
        "float_share",
        "free_share",
        "total_mv",
        "circ_mv",
    ),
    "adj_factor": ("adj_factor",),
}


class RepairResult:
    def __init__(self, *, bad: bool, repaired: bool = False, skipped: bool = False) -> None:
        self.bad = bad
        self.repaired = repaired
        self.skipped = skipped


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Repair Tushare market partitions whose numeric columns were widened to strings "
            "by empty update responses."
        )
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("data"),
        help="Data lake root directory, the directory containing lake/ and metadata/. Defaults to ./data.",
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        choices=sorted(FLOAT64_COLUMNS),
        default=sorted(FLOAT64_COLUMNS),
        help="Tushare market datasets to scan.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Rewrite bad partitions. Without this flag, only reports what would change.",
    )
    parser.add_argument(
        "--allow-new-nulls",
        action="store_true",
        help="Allow casts that introduce nulls. By default those partitions are skipped.",
    )
    args = parser.parse_args(argv)

    lake = DataLake.open(args.root)
    total_bad = 0
    total_repaired = 0
    total_skipped = 0

    for dataset in args.datasets:
        spec = _dataset_spec(lake, dataset)
        dataset_root = lake.paths.dataset_root("tushare", dataset)
        files = sorted(dataset_root.glob("year=*/month=*/data.parquet"))
        if not files:
            print(f"{dataset}: no partition files found at {dataset_root}")
            continue

        print(f"{dataset}: scanning {len(files)} partition(s)")
        for path in files:
            result = _repair_partition(
                lake=lake,
                spec=spec,
                path=path,
                columns=FLOAT64_COLUMNS[dataset],
                apply=args.apply,
                allow_new_nulls=args.allow_new_nulls,
            )
            total_bad += int(result.bad)
            total_repaired += int(result.repaired)
            total_skipped += int(result.skipped)

    mode = "applied" if args.apply else "dry-run"
    print(
        f"{mode}: bad_partitions={total_bad} repaired={total_repaired} skipped={total_skipped}"
    )
    if not args.apply and total_bad:
        print("Run again with --apply to rewrite the bad partitions.")
    return 0 if total_skipped == 0 else 1


def _repair_partition(
    *,
    lake: DataLake,
    spec: DatasetSpec,
    path: Path,
    columns: tuple[str, ...],
    apply: bool,
    allow_new_nulls: bool,
) -> RepairResult:
    schema = pl.scan_parquet(path).collect_schema()
    present = [column for column in columns if column in schema.names()]
    mismatched = [column for column in present if schema[column] != pl.Float64]
    if not mismatched:
        return RepairResult(bad=False)

    relative_path = path.relative_to(lake.paths.dataset_root(spec.source, spec.name))
    before = {column: str(schema[column]) for column in mismatched}
    print(f"  bad: {spec.name}/{relative_path} {before}")
    if not apply:
        return RepairResult(bad=True)

    frame = pl.read_parquet(path)
    repaired = frame.with_columns(
        [pl.col(column).cast(pl.Float64, strict=False) for column in present]
    )
    introduced_nulls = _introduced_nulls(frame, repaired, present)
    if introduced_nulls and not allow_new_nulls:
        print(f"  skipped: cast would introduce nulls: {introduced_nulls}")
        return RepairResult(bad=True, skipped=True)

    lake.parquet.write_partition(
        spec,
        repaired,
        relative_path,
        _partition_values(relative_path),
    )
    after = {column: str(repaired.schema[column]) for column in mismatched}
    print(f"  repaired: {spec.name}/{relative_path} {after}")
    return RepairResult(bad=True, repaired=True)


def _introduced_nulls(
    original: pl.DataFrame,
    repaired: pl.DataFrame,
    columns: list[str],
) -> dict[str, int]:
    original_nulls = original.select(
        [pl.col(column).is_null().sum().alias(column) for column in columns]
    ).row(0, named=True)
    repaired_nulls = repaired.select(
        [pl.col(column).is_null().sum().alias(column) for column in columns]
    ).row(0, named=True)
    return {
        column: repaired_nulls[column] - original_nulls[column]
        for column in columns
        if repaired_nulls[column] > original_nulls[column]
    }


def _partition_values(relative_path: Path) -> dict[str, int | str]:
    values: dict[str, int | str] = {}
    for part in relative_path.parts:
        if "=" not in part:
            continue
        key, value = part.split("=", maxsplit=1)
        try:
            values[key] = int(value)
        except ValueError:
            values[key] = value
    return values


def _dataset_spec(lake: DataLake, dataset: str) -> DatasetSpec:
    try:
        return lake.datasets.get(dataset, source="tushare")
    except DatasetNotFoundError:
        bundled = Path(__file__).resolve().parents[1] / "datasets" / "tushare" / f"{dataset}.yaml"
        return DatasetSpec.from_yaml(bundled)


if __name__ == "__main__":
    raise SystemExit(main())
