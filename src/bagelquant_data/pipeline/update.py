"""Dataset update orchestration."""

from __future__ import annotations

import time
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable
from uuid import uuid4

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.request import RequestContext
from bagelquant_data.pipeline.ingest import IngestionPipeline, IngestionReport


@dataclass(frozen=True, slots=True)
class UpdateReport:
    """Update report for one or more datasets."""

    source: str
    datasets: tuple[str, ...]
    runs: tuple[IngestionReport, ...]


@dataclass(frozen=True, slots=True)
class FetchPage:
    """Result of one physical source API call."""

    request_key: str
    request_params: dict[str, Any]
    frame: object | None
    status: str
    row_count: int
    retry_count: int
    error_message: str | None = None
    asset_id: str | None = None


def update_dataset(
    *,
    spec: DatasetSpec,
    source_adapter: object,
    pipeline: IngestionPipeline,
    context: RequestContext,
    requests: Sequence[dict[str, object]],
) -> IngestionReport:
    """Fetch requests for a dataset and commit bounded batches."""

    import polars as pl

    run_id = uuid4().hex
    workers = max(1, int(context.options.get("workers", 4)))
    progress_enabled = bool(context.options.get("progress", True))
    max_retries = max(1, int(context.options.get("max_retries", 3)))
    retry_backoff_seconds = float(context.options.get("retry_backoff_seconds", 60.0))
    request_options = _request_options(context)
    planned_requests = [dict(request) for request in requests]
    batches = [list(enumerate(planned_requests))] if spec.update_type == "general" else _request_batches(planned_requests, context)
    errors: list[str] = []
    request_count = 0
    success_count = 0
    failure_count = 0
    rows_downloaded = 0
    rows_committed = 0
    any_committed_frame = False

    progress = _progress_bar(spec.name, len(planned_requests), enabled=progress_enabled)
    progress_lock = None
    if progress is not None:
        from threading import Lock

        progress_lock = Lock()

    def on_extra_page() -> None:
        if progress is None or progress_lock is None:
            return
        with progress_lock:
            progress.total = (progress.total or 0) + 1
            progress.refresh()

    def on_page_done() -> None:
        if progress is None or progress_lock is None:
            return
        with progress_lock:
            progress.update(1)

    try:
        for batch in batches:
            pages: list[FetchPage] = []
            frames: list[pl.DataFrame] = []
            if workers == 1 or len(batch) <= 1:
                for index, request in batch:
                    request_pages = _fetch_request_pages(
                        spec=spec,
                        source_adapter=source_adapter,
                        request=request,
                        request_index=index,
                        request_options=request_options,
                        max_retries=max_retries,
                        retry_backoff_seconds=retry_backoff_seconds,
                        on_extra_page=on_extra_page,
                        on_page_done=on_page_done,
                    )
                    pages.extend(request_pages)
            else:
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = [
                        executor.submit(
                            _fetch_request_pages,
                            spec=spec,
                            source_adapter=source_adapter,
                            request=request,
                            request_index=index,
                            request_options=request_options,
                            max_retries=max_retries,
                            retry_backoff_seconds=retry_backoff_seconds,
                            on_extra_page=on_extra_page,
                            on_page_done=on_page_done,
                        )
                        for index, request in batch
                    ]
                    for future in as_completed(futures):
                        pages.extend(future.result())

            pipeline.metadata.record_api_calls(
                {
                    "run_id": run_id,
                    "source": spec.source,
                    "dataset": spec.name,
                    "request_key": page.request_key,
                    "request_params": page.request_params,
                    "status": page.status,
                    "row_count": page.row_count,
                    "retry_count": page.retry_count,
                    "error_message": page.error_message,
                    "asset_id": page.asset_id,
                }
                for page in pages
            )
            for page in pages:
                if page.status == "success" and isinstance(page.frame, pl.DataFrame) and page.frame.height > 0:
                    frames.append(page.frame)
                elif page.status == "success" and spec.update_type == "general" and isinstance(page.frame, pl.DataFrame):
                    frames.append(page.frame)
                elif page.error_message:
                    errors.append(f"{page.request_key}: {page.error_message}")

            request_count += len(pages)
            batch_success_count = sum(1 for page in pages if page.status == "success")
            success_count += batch_success_count
            failure_count += len(pages) - batch_success_count
            rows_downloaded += sum(page.row_count for page in pages if page.status == "success")
            batch_has_failures = any(page.status != "success" for page in pages)
            if frames and not (spec.update_type == "general" and batch_has_failures):
                frame = pl.concat(frames, how="diagonal_relaxed")
                rows_committed += pipeline.commit_frame(
                    spec,
                    frame,
                    run_id=run_id,
                )
                any_committed_frame = True
            elif pages and all(page.status == "success" for page in pages):
                # Empty successful responses should be observed but must not rewrite existing data.
                any_committed_frame = True
    finally:
        if progress is not None:
            progress.close()

    error_message = "; ".join(errors[:5]) if errors else None
    if len(errors) > 5:
        error_message = f"{error_message}; ... {len(errors) - 5} more"

    status = "failed" if failure_count and not any_committed_frame else "partial" if failure_count else "success"
    pipeline.metadata.record_run(
        run_id=run_id,
        source=spec.source,
        dataset=spec.name,
        mode=spec.update_type,
        status=status,
        request_count=request_count,
        success_count=success_count,
        failure_count=failure_count,
        rows_downloaded=rows_downloaded,
        rows_committed=rows_committed,
        error_message=error_message,
    )
    return IngestionReport(
        run_id=run_id,
        source=spec.source,
        dataset=spec.name,
        status=status,
        rows_downloaded=rows_downloaded,
        rows_committed=rows_committed,
        request_count=request_count,
        success_count=success_count,
        failure_count=failure_count,
        error_message=error_message,
    )


def combine_reports(source: str, reports: Sequence[IngestionReport]) -> UpdateReport:
    return UpdateReport(source=source, datasets=tuple(report.dataset for report in reports), runs=tuple(reports))


def _request_batches(
    requests: list[dict[str, Any]],
    context: RequestContext,
) -> list[list[tuple[int, dict[str, Any]]]]:
    indexed = list(enumerate(requests))
    batch_size = max(1, int(context.options.get("batch_size", len(indexed) or 1)))
    if len(indexed) <= batch_size:
        return [indexed]
    return [indexed[index : index + batch_size] for index in range(0, len(indexed), batch_size)]


def _fetch_request_pages(
    *,
    spec: DatasetSpec,
    source_adapter: object,
    request: dict[str, Any],
    request_index: int,
    request_options: dict[str, Any],
    max_retries: int,
    retry_backoff_seconds: float,
    on_extra_page: Callable[[], None],
    on_page_done: Callable[[], None],
) -> list[FetchPage]:
    if request_options.get("pagination") != "offset":
        page = _fetch_one(
            spec=spec,
            source_adapter=source_adapter,
            request=request,
            request_key=str(request_index),
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
        )
        on_page_done()
        return [page]

    page_size = int(request_options.get("page_size", 5000))
    limit_param = str(request_options.get("limit_param", "limit"))
    offset_param = str(request_options.get("offset_param", "offset"))
    offset = int(request_options.get("offset_start", 0))
    pages: list[FetchPage] = []
    page_index = 0
    while True:
        if page_index > 0:
            on_extra_page()
        paged_request = dict(request)
        paged_request[limit_param] = page_size
        paged_request[offset_param] = offset
        page = _fetch_one(
            spec=spec,
            source_adapter=source_adapter,
            request=paged_request,
            request_key=f"{request_index}:{page_index}",
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
        )
        pages.append(page)
        on_page_done()
        if page.status != "success" or page.row_count < page_size:
            break
        offset += page_size
        page_index += 1
    return pages


def _fetch_one(
    *,
    spec: DatasetSpec,
    source_adapter: object,
    request: dict[str, Any],
    request_key: str,
    max_retries: int,
    retry_backoff_seconds: float,
) -> FetchPage:
    last_error: Exception | None = None
    for attempt in range(max_retries):
        try:
            frame = source_adapter.fetch(spec.name, request)  # type: ignore[attr-defined]
            return FetchPage(
                request_key=request_key,
                request_params=request,
                frame=frame,
                status="success",
                row_count=frame.height,
                retry_count=attempt,
                asset_id=_request_asset(request),
            )
        except Exception as exc:  # noqa: BLE001 - source SDKs raise provider-specific exceptions.
            last_error = exc
            if attempt + 1 < max_retries:
                time.sleep(retry_backoff_seconds * (attempt + 1))
    return FetchPage(
        request_key=request_key,
        request_params=request,
        frame=None,
        status="failed",
        row_count=0,
        retry_count=max_retries - 1,
        error_message=str(last_error) if last_error else "Unknown source error",
        asset_id=_request_asset(request),
    )


def _request_options(context: RequestContext) -> dict[str, Any]:
    options: dict[str, Any] = {}
    source_options = context.options.get("source_options")
    if isinstance(source_options, dict):
        options.update(source_options)
    for key in ("pagination", "page_size", "limit_param", "offset_param", "offset_start"):
        if key in context.options:
            options[key] = context.options[key]
    return options


def _request_asset(request: dict[str, Any]) -> str | None:
    value = request.get("id") or request.get("ts_code") or request.get("index_code") or request.get("asset_id")
    return None if value is None else str(value)


def _progress_bar(dataset: str, total: int, *, enabled: bool) -> Any | None:
    if not enabled:
        return None
    try:
        from tqdm import tqdm
    except ImportError:
        return None
    return tqdm(total=total, desc=dataset, unit="call")  # type: ignore[no-any-return]
