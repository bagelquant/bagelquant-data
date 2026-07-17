"""Ingestion pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.normalization import NormalizeContext, StandardNormalizer
from bagelquant_data.core.registry import FrameworkRegistries
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
    remaining_scope_count: int = 0
    elapsed_seconds: float = 0.0
    fetch_seconds: float = 0.0
    commit_seconds: float = 0.0
    metadata_seconds: float = 0.0
    commit_count: int = 0
    partitions_rewritten: int = 0
    peak_in_flight: int = 0
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
    ) -> IngestionReport:
        run_id = run_id or uuid4().hex
        committed = self.commit_frame(
            spec,
            frame,
            run_id=run_id,
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
    ) -> int:
        """Commit a frame as part of an existing logical run."""

        self.staging.write(spec.source, spec.name, frame, run_id)
        try:
            result = StandardNormalizer().normalize(
                frame.lazy(),
                spec,
                NormalizeContext(source=spec.source, dataset=spec.name, run_id=run_id),
            )
            rejected = result.rejected.collect()
            if rejected.height:
                self.rejected.write(
                    spec.source, spec.name, run_id, "normalization", rejected
                )
                self.metadata.record_rejected(
                    run_id=run_id,
                    source=spec.source,
                    dataset=spec.name,
                    reason="normalization",
                    row_count=rejected.height,
                )
            return commit_frame(
                spec=spec,
                frame=result.accepted,
                registries=self.registries,
                parquet=self.parquet,
            )
        finally:
            self.staging.cleanup(spec.source, spec.name, run_id)
