from __future__ import annotations

import argparse
from pathlib import Path

from bagelquant_data import DataLake

try:
    from scripts.lake_paths import default_lake_root
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from lake_paths import default_lake_root


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Example point-in-time financial factor query.")
    parser.add_argument("--root", type=Path, default=default_lake_root(), help="Data lake root directory.")
    parser.add_argument("--asset", default="000001.SZ")
    parser.add_argument("--start", default="2025-01-01")
    parser.add_argument("--end", default="2025-12-31")
    args = parser.parse_args(argv)

    lake = DataLake.open(args.root)
    earnings_ytd = lake.finance.field("income", "n_income_attr_p", source="tushare")
    earnings_quarter = lake.finance.ytd_to_period(earnings_ytd)
    earnings_ttm = lake.finance.trailing(
        earnings_quarter,
        periods=4,
        operation="sum",
        output_name="earnings_ttm",
    )

    observations = lake.query.observations(
        start=args.start,
        end=args.end,
        frequency="month_end",
        assets=[args.asset],
    )
    aligned = lake.finance.asof(
        earnings_ttm,
        observations,
        value_column="earnings_ttm",
        output_name="earnings_ttm",
        collect=True,
    )
    print(aligned)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
