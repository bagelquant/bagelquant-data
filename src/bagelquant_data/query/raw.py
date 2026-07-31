"""Canonical raw record queries."""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path

import polars as pl

from bagelquant_data.core.dataset import ASSET_BUCKET_COUNT
from bagelquant_data.core.exceptions import DatasetNotFoundError
from bagelquant_data.core.hashing import stable_bucket
from bagelquant_data.core.schema import (
    align_lazy_frame,
    compatible_schema,
)
from bagelquant_data.core.types import DateLike
from bagelquant_data.query.scanner import manifest_rows
from bagelquant_data.storage.metadata import MetadataStore
from bagelquant_data.storage.parquet import ParquetStore


class RawQueryService:
    """Read row-oriented canonical records."""

    def __init__(self, parquet: ParquetStore, metadata: MetadataStore) -> None:
        self.parquet = parquet
        self.metadata = metadata

    def query_general(
        self,
        dataset: str,
        *,
        source: str,
        fields: Sequence[str] | None = None,
    ) -> pl.LazyFrame:
        return self._scan(dataset, source=source, fields=fields)

    def query(
        self,
        dataset: str,
        *,
        source: str,
        start: DateLike | None = None,
        end: DateLike | None = None,
        assets: Sequence[str] | None = None,
        fields: Sequence[str] | None = None,
    ) -> pl.LazyFrame:
        return self._scan(
            dataset,
            source=source,
            start=start,
            end=end,
            assets=assets,
            fields=fields,
        )

    def _scan(
        self,
        dataset: str,
        *,
        source: str,
        start: DateLike | None = None,
        end: DateLike | None = None,
        assets: Sequence[str] | None = None,
        fields: Sequence[str] | None,
    ) -> pl.LazyFrame:
        bucket_count = self._asset_bucket_count(source, dataset)
        buckets = (
            None
            if not assets
            else {stable_bucket(str(asset), bucket_count) for asset in assets}
        )
        all_rows = self.metadata.manifest(source, dataset)
        if not all_rows:
            raise DatasetNotFoundError(f"No canonical data for {source}/{dataset}")
        rows = manifest_rows(
            self.metadata,
            source,
            dataset,
            start=start,
            end=end,
            buckets=buckets,
        )
        if not rows:
            canonical = self.parquet.canonical_schema(source, dataset)
            if canonical is None:
                raise DatasetNotFoundError(
                    f"No canonical schema for {source}/{dataset}"
                )
            selected = (
                list(canonical)
                if fields is None
                else [field for field in fields if field in canonical]
            )
            return pl.DataFrame(
                schema={name: canonical[name] for name in selected}
            ).lazy()
        root = self.parquet.paths.dataset_root(source, dataset)
        missing = [
            root / str(row["partition_path"])
            for row in rows
            if not (root / str(row["partition_path"])).is_file()
        ]
        if missing:
            raise DatasetNotFoundError(
                f"Canonical manifest for {source}/{dataset} references missing files: "
                f"{[str(path) for path in missing[:5]]}"
            )
        grouped: dict[str, list[Path]] = {}
        for row in rows:
            grouped.setdefault(str(row["schema_hash"]), []).append(
                root / str(row["partition_path"])
            )
        scans = [
            pl.scan_parquet([str(path) for path in sorted(paths)])
            for paths in grouped.values()
        ]
        canonical = self.parquet.canonical_schema(source, dataset)
        schemas = [scan.collect_schema() for scan in scans]
        resolved = compatible_schema(
            [*([] if canonical is None else [canonical]), *schemas]
        )
        selected = (
            list(resolved)
            if fields is None
            else [field for field in fields if field in resolved]
        )
        aligned = [
            align_lazy_frame(
                _apply_filters(scan, start, end, assets), resolved, selected
            )
            for scan in scans
        ]
        return aligned[0] if len(aligned) == 1 else pl.concat(aligned, how="vertical")

    def _asset_bucket_count(self, source: str, dataset: str) -> int:
        row = self.metadata.get_dataset(source, dataset)
        if row is None:
            return ASSET_BUCKET_COUNT
        spec = json.loads(str(row["spec_json"]))
        return int(spec.get("asset_bucket_count", ASSET_BUCKET_COUNT))


def _date_literal(value: DateLike) -> pl.Expr:
    return pl.lit(_date_value(value), dtype=pl.Date)


def _apply_filters(
    frame: pl.LazyFrame,
    start: DateLike | None,
    end: DateLike | None,
    assets: Sequence[str] | None,
) -> pl.LazyFrame:
    if start is not None:
        frame = frame.filter(pl.col("time") >= _date_literal(start))
    if end is not None:
        frame = frame.filter(pl.col("time") <= _date_literal(end))
    if assets is not None:
        frame = frame.filter(pl.col("asset_id").is_in(list(assets)))
    return frame


def _date_value(value: DateLike) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).split("T", maxsplit=1)[0]
    if len(text) == 8 and text.isdigit():
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    return date.fromisoformat(text)
