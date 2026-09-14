"""Canonical commit result and versioned ingestion entry point."""
from __future__ import annotations
from dataclasses import dataclass
from uuid import uuid4

MAX_PARQUET_WRITE_WORKERS = 4

@dataclass(frozen=True, slots=True)
class CommitResult:
    rows_committed: int
    partitions_rewritten: int
    partitions_skipped: int
    bytes_written: int
    present_times: frozenset[str] = frozenset()


def commit_frame(*, spec, frame, registries, parquet, writer_executor=None,
                 run_id=None, mode="incremental", ingested_at=None, requests=None):
    from bagelquant_data.pipeline.versions import commit_versions
    registries.validators.get("framework").validate(frame, spec)
    return commit_versions(spec, frame.collect(), parquet, run_id=run_id or uuid4().hex,
                           mode=mode, ingested_at=ingested_at, requests=requests)
