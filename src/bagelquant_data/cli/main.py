"""Thin CLI for the Python-first data lake API."""

from __future__ import annotations

import argparse

from bagelquant_data import DataLake


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="bagelquant-data")
    parser.add_argument("--root", default="data")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status")
    datasets = sub.add_parser("dataset-list")
    datasets.add_argument("--source")
    sub.add_parser("source-list")
    args = parser.parse_args(argv)
    lake = DataLake.open(args.root)
    if args.command == "status":
        print(lake.admin.summary())
    elif args.command == "dataset-list":
        print(lake.admin.datasets.list(args.source))
    elif args.command == "source-list":
        print(lake.admin.sources.list())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
