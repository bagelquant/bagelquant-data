from __future__ import annotations

import argparse
from pathlib import Path

from bagelquant_data import DataLake, TushareSource


DEFAULT_ROOT = Path("/Users/eric/data")
DEFAULT_DATASET_DIR = Path(__file__).resolve().parents[1] / "datasets" / "tushare"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Configure a BagelQuant data lake for Tushare.")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT, help="Data lake root directory.")
    parser.add_argument(
        "--dataset-dir",
        type=Path,
        default=DEFAULT_DATASET_DIR,
        help="Directory containing Tushare dataset YAML specs.",
    )
    parser.add_argument("--token", help="Tushare token to save into the local lake metadata DB.")
    parser.add_argument("--test-source", action="store_true", help="Test the Tushare connection after configuration.")
    args = parser.parse_args(argv)

    lake = DataLake.open(args.root)
    lake.sources.register(TushareSource())
    if args.token:
        lake.sources.configure_tushare(args.token)
    if args.test_source:
        lake.sources.test("tushare")

    specs = []
    for yaml_file in sorted(args.dataset_dir.glob("*.yaml")):
        specs.append(lake.datasets.add_from_yaml(yaml_file))

    print(f"Lake root: {args.root}")
    print("Sources:")
    for source in lake.sources.list():
        print(f"  - {source['name']} configured={bool(source['configured'])}")
    print("Datasets:")
    for spec in specs:
        print(f"  - {spec.source}/{spec.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
