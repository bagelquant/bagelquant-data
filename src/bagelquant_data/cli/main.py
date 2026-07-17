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
    bootstrap = sub.add_parser("bootstrap-update-state")
    bootstrap.add_argument("--start", default="1999-12-31")
    bootstrap.add_argument("--end")
    bootstrap.add_argument("--apply", action="store_true")
    args = parser.parse_args(argv)
    lake = DataLake.open(args.root)
    if args.command == "status":
        print(lake.admin.summary())
    elif args.command == "dataset-list":
        print(lake.admin.datasets.list(args.source))
    elif args.command == "source-list":
        print(lake.admin.sources.list())
    elif args.command == "bootstrap-update-state":
        print(
            lake.update.bootstrap_update_state(
                start=args.start, end=args.end, apply=args.apply
            )
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
