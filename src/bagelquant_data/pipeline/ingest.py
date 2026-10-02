"""Ingestion pipeline."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from uuid import uuid4

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.normalization import NormalizeContext, StandardNormalizer
from bagelquant_data.core.registry import FrameworkRegistries
from bagelquant_data.pipeline.commit import CommitResult
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
    empty_count: int = 0
    partitions_skipped: int = 0
    planning_seconds: float = 0.0
    bytes_written: int = 0


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
        mode: str = "incremental",
        run_id: str | None = None,
        status: str = "success",
        request_count: int = 0,
        success_count: int = 0,
        failure_count: int = 0,
        error_message: str | None = None,
        ingested_at: datetime | None = None,
    ) -> IngestionReport:
        run_id = run_id or uuid4().hex
        commit = self.commit_frame(
            spec,
            frame,
            run_id=run_id,
            mode=mode,
            ingested_at=ingested_at,
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
            rows_committed=commit.rows_committed,
            error_message=error_message,
        )
        return IngestionReport(
            run_id=run_id,
            source=spec.source,
            dataset=spec.name,
            status=status,
            rows_downloaded=frame.height,
            rows_committed=commit.rows_committed,
            request_count=request_count,
            success_count=success_count,
            failure_count=failure_count,
            commit_count=1,
            partitions_rewritten=commit.partitions_rewritten,
            partitions_skipped=commit.partitions_skipped,
            bytes_written=commit.bytes_written,
            error_message=error_message,
        )

    def commit_frame(
        self,
        spec: DatasetSpec,
        frame: pl.DataFrame,
        *,
        run_id: str,
        writer_executor: ThreadPoolExecutor | None = None,
        mode: str = "incremental",
        ingested_at: datetime | None = None,
        requests: list[dict] | None = None,
    ) -> CommitResult:
        """Commit a frame as part of an existing logical run."""

        if frame.width == 0:
            frame = pl.DataFrame(
                schema={field: pl.String for field in spec.field_mappings}
            )
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
        from bagelquant_data.pipeline.versions import commit_versions
        from bagelquant_data.core.validation import FrameworkValidator

        FrameworkValidator().validate(result.accepted, spec)
        return commit_versions(
            spec,
            result.accepted.collect(),
            self.parquet,
            run_id=run_id,
            mode=mode,
            ingested_at=ingested_at,
            requests=requests,
        )
