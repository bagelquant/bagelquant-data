"""Ingestion pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast
from uuid import uuid4

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.exceptions import ConfigurationError
from bagelquant_data.core.normalization import NormalizeContext, Normalizer
from bagelquant_data.core.registry import FrameworkRegistries
from bagelquant_data.core.types import DateLike
from bagelquant_data.pipeline.commit import commit_frame
from bagelquant_data.storage.metadata import MetadataStore
from bagelquant_data.storage.parquet import ParquetStore
from bagelquant_data.storage.rejected import RejectedStore
from bagelquant_data.storage.staging import StagingStore


@dataclass(frozen=True, slots=True)
class IngestionReport:
    """Update result."""

    run_id: str
    source: str
    dataset: str
    status: str
    rows_downloaded: int
    rows_committed: int
    request_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    error_message: str | None = None


class IngestionPipeline:
    """Fetch source data and commit canonical records."""

    def __init__(
        self,
        *,
        registries: FrameworkRegistries,
        parquet: ParquetStore,
        metadata: MetadataStore,
        staging: StagingStore,
        rejected: RejectedStore,
    ) -> None:
        self.registries = registries
        self.parquet = parquet
        self.metadata = metadata
        self.staging = staging
        self.rejected = rejected

    def ingest_frame(
        self,
        spec: DatasetSpec,
        frame: pl.DataFrame,
        *,
        mode: str = "upsert",
        run_id: str | None = None,
        status: str = "success",
        request_count: int = 0,
        success_count: int = 0,
        failure_count: int = 0,
        error_message: str | None = None,
        update_start: DateLike | None = None,
        update_end: DateLike | None = None,
        replace_assets: set[str] | None = None,
    ) -> IngestionReport:
        run_id = run_id or uuid4().hex
        committed = self.commit_frame(
            spec,
            frame,
            run_id=run_id,
            mode=mode,
            update_start=update_start,
            update_end=update_end,
            replace_assets=replace_assets,
        )
        self.metadata.record_run(
            run_id=run_id,
            source=spec.source,
            dataset=spec.name,
            mode=mode,
            status=status,
            request_count=request_count,
            success_count=success_count,
            failure_count=failure_count,
            rows_downloaded=frame.height,
            rows_committed=committed,
            error_message=error_message,
        )
        return IngestionReport(
            run_id=run_id,
            source=spec.source,
            dataset=spec.name,
            status=status,
            rows_downloaded=frame.height,
            rows_committed=committed,
            request_count=request_count,
            success_count=success_count,
            failure_count=failure_count,
            error_message=error_message,
        )

    def commit_frame(
        self,
        spec: DatasetSpec,
        frame: pl.DataFrame,
        *,
        run_id: str,
        mode: str = "upsert",
        update_start: DateLike | None = None,
        update_end: DateLike | None = None,
        replace_assets: set[str] | None = None,
    ) -> int:
        """Commit a frame as part of an existing logical run."""

        frame = _apply_row_filter(spec, frame)
        self.staging.write(spec.source, spec.name, frame, run_id)
        normalizer = cast(Normalizer, self.registries.normalizers.get(spec.normalizer))
        result = normalizer.normalize(
            frame.lazy(),
            spec,
            NormalizeContext(source=spec.source, dataset=spec.name, run_id=run_id),
        )
        rejected = result.rejected.collect()
        if rejected.height:
            self.rejected.write(spec.source, spec.name, run_id, "normalization", rejected)
        committed = commit_frame(
            spec=spec,
            frame=result.accepted,
            registries=self.registries,
            parquet=self.parquet,
            mode=mode,
            update_start=update_start,
            update_end=update_end,
            replace_assets=replace_assets,
        )
        self.staging.cleanup(spec.source, spec.name, run_id)
        return committed


def _apply_row_filter(spec: DatasetSpec, frame: pl.DataFrame) -> pl.DataFrame:
    row_filter = spec.request_options.get("row_filter")
    if row_filter is None:
        return frame
    if not isinstance(row_filter, dict):
        raise ConfigurationError(f"{spec.source}/{spec.name} request_options.row_filter must be a mapping")
    column = row_filter.get("column")
    values = row_filter.get("in")
    if column is None or values is None:
        raise ConfigurationError(
            f"{spec.source}/{spec.name} request_options.row_filter requires column and in"
        )
    column = str(column)
    if column not in frame.columns:
        raise ConfigurationError(f"{spec.source}/{spec.name} row_filter column is missing: {column}")
    if isinstance(values, str) or not isinstance(values, list):
        raise ConfigurationError(f"{spec.source}/{spec.name} request_options.row_filter.in must be a list")
    return frame.filter(pl.col(column).cast(pl.String).is_in([str(value) for value in values]))
