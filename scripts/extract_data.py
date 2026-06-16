from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl
from bagelquant_data import DataLake


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Example Tushare extraction queries.")
    parser.add_argument("--root", type=Path, default=Path("data"), help="Data lake root directory.")
    parser.add_argument("--start", default="2000-01-01")
    parser.add_argument("--end", default="2026-06-17")
    parser.add_argument("--asset", default="000001.SZ")
    args = parser.parse_args(argv)

    lake = DataLake.open("C:/Users/ericy/data")
    close = lake.query.field(
        "daily",
        "close",
        source="tushare",
        start=args.start,
        assets=[args.asset],
        end=args.end,
        collect=True,
    )
    print(close.tail())

    index_basic = lake.query.reference("index_basic", source="tushare", collect=True)
    # select is_open = 1 and sort by date
    print(index_basic.head())

    # ohlcv = lake.query.fields(
    #     "daily",
    #     ["open", "high", "low", "close", "vol"],
    #     source="tushare",
    #     start=args.start,
    #     end=args.end,
    #     collect=True,
    # )
    # print(ohlcv)

    # income_records = lake.query.raw(
    #     "income",
    #     source="tushare",
    #     start=args.start,
    #     end=args.end,
    #     assets=[args.asset],
    #     columns=["asset_id", "time", "period", "report_type", "n_income_attr_p"],
    # ).collect()
    # print(income_records)

    # stock_basic = lake.query.reference("stock_basic", source="tushare", collect=True)
    # print(stock_basic)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
