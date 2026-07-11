"""Dataset update orchestration."""

from __future__ import annotations

import hashlib
import json
import time
from collections import deque
from collections.abc import Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, TypeAlias
from uuid import uuid4

import polars as pl

from bagelquant_data.core.dataset import ASSET_BUCKET_COUNT, DatasetSpec
from bagelquant_data.core.hashing import stable_bucket
from bagelquant_data.core.request import RequestContext
from bagelquant_data.pipeline.ingest import IngestionPipeline, IngestionReport


@dataclass(frozen=True, slots=True)
class UpdateReport:
    """Update report for one or more datasets."""

    source: str
    datasets: tuple[str, ...]
    runs: tuple[IngestionReport, ...]
    pending_job_count: int = 0
    elapsed_seconds: float = 0.0
    fetch_seconds: float = 0.0
    commit_seconds: float = 0.0
    metadata_seconds: float = 0.0
    commit_count: int = 0
    partitions_rewritten: int = 0
    peak_in_flight: int = 0


@dataclass(frozen=True, slots=True)
class DatasetUpdateWork:
    """Planned logical requests for one dataset in a shared update run."""

    spec: DatasetSpec
    context: RequestContext
    requests: tuple[dict[str, object], ...]


UpdateTask: TypeAlias = tuple[DatasetUpdateWork, dict[str, object], str]


@dataclass(slots=True)
class _RunState:
    work: DatasetUpdateWork
    run_id: str
    request_count: int = 0
    success_count: int = 0
    failure_count: int = 0
    rows_downloaded: int = 0
    rows_committed: int = 0
    successful_jobs: int = 0
    errors: list[str] = field(default_factory=list)
    frames: list[pl.DataFrame] = field(default_factory=list)
    buffered_jobs: int = 0
    general_failed: bool = False
    started_at: float = 0.0
    fetch_seconds: float = 0.0
    commit_seconds: float = 0.0
    metadata_seconds: float = 0.0
    commit_count: int = 0
    partitions_rewritten: int = 0
    peak_in_flight: int = 0
    buffered_bytes: int = 0

    def __post_init__(self) -> None:
        self.started_at = time.perf_counter()


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
    """Fetch one dataset through the shared global scheduler."""

    report = update_datasets(
        source_adapter=source_adapter,
        pipeline=pipeline,
        works=(
            DatasetUpdateWork(
                spec, context, tuple(dict(request) for request in requests)
            ),
        ),
    )
    return report.runs[0]


def update_datasets(
    *,
    source_adapter: object,
    pipeline: IngestionPipeline,
    works: Sequence[DatasetUpdateWork],
) -> UpdateReport:
    """Execute logical requests with one worker limit shared across datasets."""

    if not works:
        return UpdateReport(source="", datasets=(), runs=())
    started_at = time.perf_counter()
    workers = max(1, int(works[0].context.options.get("workers", 4)))
    max_in_flight = max(
        workers,
        int(works[0].context.options.get("max_in_flight", workers * 2)),
    )
    states = {work.spec.name: _RunState(work, uuid4().hex) for work in works}
    progresses = {
        work.spec.name: _progress_bar(
            work.spec.name,
            len(work.requests),
            enabled=bool(work.context.options.get("progress", True)),
        )
        for work in works
    }
    selected_names = set(states)
    pending_rows = [
        row
        for row in pipeline.metadata.pending_update_jobs(source=works[0].spec.source)
        if row["dataset"] in selected_names and row["update_type"] != "general"
    ]
    retry_keys = {str(row["job_key"]) for row in pending_rows}
    retry_tasks = [
        (
            states[str(row["dataset"])].work,
            dict(row["request_params"]),
            str(row["job_key"]),
        )
        for row in pending_rows
    ]
    new_tasks: list[UpdateTask] = []
    for work in works:
        for original in work.requests:
            request = _after_pending_asset_ranges(work, dict(original), pending_rows)
            if request is None:
                continue
            key = _job_key(work.spec, request)
            if key not in retry_keys:
                new_tasks.append((work, request, key))
    for name, progress in progresses.items():
        if progress is not None:
            progress.total = sum(
                work.spec.name == name for work, _, _ in (*retry_tasks, *new_tasks)
            )
            progress.refresh()

    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            _run_phase(
                "retry",
                retry_tasks,
                executor,
                source_adapter,
                pipeline,
                states,
                progresses,
                max_in_flight=max_in_flight,
            )
            _flush_incremental_states(pipeline, states)
            _run_phase(
                "new",
                _fair_tasks(new_tasks),
                executor,
                source_adapter,
                pipeline,
                states,
                progresses,
                max_in_flight=max_in_flight,
            )
            _flush_states(pipeline, states)
    finally:
        for progress in progresses.values():
            if progress is not None:
                progress.close()

    reports = tuple(_finish_state(pipeline, state) for state in states.values())
    pending_count = sum(report.pending_job_count for report in reports)
    return UpdateReport(
        source=works[0].spec.source,
        datasets=tuple(report.dataset for report in reports),
        runs=reports,
        pending_job_count=pending_count,
        elapsed_seconds=time.perf_counter() - started_at,
        fetch_seconds=sum(report.fetch_seconds for report in reports),
        commit_seconds=sum(report.commit_seconds for report in reports),
        metadata_seconds=sum(report.metadata_seconds for report in reports),
        commit_count=sum(report.commit_count for report in reports),
        partitions_rewritten=sum(report.partitions_rewritten for report in reports),
        peak_in_flight=max((report.peak_in_flight for report in reports), default=0),
    )


def _run_phase(
    phase: str,
    tasks: Sequence[UpdateTask],
    executor: ThreadPoolExecutor,
    source_adapter: object,
    pipeline: IngestionPipeline,
    states: dict[str, _RunState],
    progresses: dict[str, Any | None],
    *,
    max_in_flight: int,
) -> None:
    task_iter = iter(enumerate(tasks))
    futures: dict[Future[list[FetchPage]], tuple[float, UpdateTask]] = {}

    def submit_next() -> bool:
        try:
            index, (work, request, key) = next(task_iter)
        except StopIteration:
            return False
        progress = progresses[work.spec.name]
        if progress is not None:
            progress.set_description_str(f"{work.spec.name} [{phase}]")
        future = executor.submit(
            _fetch_request_pages,
            spec=work.spec,
            source_adapter=source_adapter,
            request=request,
            request_index=index,
            request_options=_request_options(work.context),
            max_retries=max(1, int(work.context.options.get("max_retries", 3))),
            retry_backoff_seconds=float(
                work.context.options.get("retry_backoff_seconds", 60.0)
            ),
        )
        futures[future] = (time.perf_counter(), (work, request, key))
        states[work.spec.name].peak_in_flight = max(
            states[work.spec.name].peak_in_flight, len(futures)
        )
        return True

    while len(futures) < max_in_flight and submit_next():
        pass
    while futures:
        completed, _ = wait(futures, return_when=FIRST_COMPLETED)
        api_rows: list[dict[str, object]] = []
        resolved_keys: list[str] = []
        failed_jobs: list[dict[str, object]] = []
        completed_results = []
        for future in completed:
            submitted_at, task = futures.pop(future)
            work, request, key = task
            state = states[work.spec.name]
            pages = future.result()
            state.fetch_seconds += time.perf_counter() - submitted_at
            completed_results.append((work, request, key, state, pages))
            api_rows.extend(
                {
                    "run_id": state.run_id,
                    "source": work.spec.source,
                    "dataset": work.spec.name,
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
            failed_pages = [page for page in pages if page.status != "success"]
            if failed_pages:
                message = failed_pages[-1].error_message or "Unknown source error"
                failed_jobs.append(
                    {
                        "job_key": key,
                        "source": work.spec.source,
                        "dataset": work.spec.name,
                        "update_type": work.spec.update_type,
                        "request_params": request,
                        "asset_id": _request_asset(request),
                        "error_message": message,
                    }
                )
            else:
                resolved_keys.append(key)
        metadata_started = time.perf_counter()
        pipeline.metadata.record_update_results(api_rows, resolved_keys, failed_jobs)
        metadata_elapsed = time.perf_counter() - metadata_started
        if completed_results:
            share = metadata_elapsed / len(completed_results)
            for _, _, _, state, _ in completed_results:
                state.metadata_seconds += share

        for work, request, key, state, pages in completed_results:
            state.request_count += len(pages)
            state.success_count += sum(page.status == "success" for page in pages)
            state.failure_count += sum(page.status != "success" for page in pages)
            state.rows_downloaded += sum(
                page.row_count for page in pages if page.status == "success"
            )
            progress = progresses[work.spec.name]
            if progress is not None:
                if len(pages) > 1:
                    progress.total = (progress.total or 0) + len(pages) - 1
                    progress.refresh()
                progress.update(len(pages))

            failed_pages = [page for page in pages if page.status != "success"]
            if failed_pages:
                message = failed_pages[-1].error_message or "Unknown source error"
                state.errors.append(f"{key}: {message}")
                state.general_failed = (
                    state.general_failed or work.spec.update_type == "general"
                )
                continue

            state.successful_jobs += 1
            frames = [
                page.frame
                for page in pages
                if isinstance(page.frame, pl.DataFrame)
                and (page.frame.height > 0 or work.spec.update_type == "general")
            ]
            state.frames.extend(frames)
            state.buffered_bytes += sum(frame.estimated_size() for frame in frames)
            state.buffered_jobs += 1
            batch_size = max(1, int(work.context.options.get("batch_size", 100)))
            max_buffer_bytes = (
                max(1, int(work.context.options.get("max_buffer_mb", 256)))
                * 1024
                * 1024
            )
            if work.spec.update_type != "general" and (
                state.buffered_jobs >= batch_size
                or state.buffered_bytes >= max_buffer_bytes
            ):
                _commit_state(pipeline, state)
        while len(futures) < max_in_flight and submit_next():
            pass


def _commit_state(pipeline: IngestionPipeline, state: _RunState) -> None:
    if state.frames:
        frame = pl.concat(state.frames, how="diagonal_relaxed")
        started_at = time.perf_counter()
        state.rows_committed += pipeline.commit_frame(
            state.work.spec,
            frame,
            run_id=state.run_id,
        )
        state.commit_seconds += time.perf_counter() - started_at
        state.commit_count += 1
        state.partitions_rewritten += _partition_count(frame, state.work.spec)
    state.frames.clear()
    state.buffered_jobs = 0
    state.buffered_bytes = 0


def _flush_incremental_states(
    pipeline: IngestionPipeline,
    states: dict[str, _RunState],
) -> None:
    for state in states.values():
        if state.work.spec.update_type != "general":
            _commit_state(pipeline, state)


def _flush_states(pipeline: IngestionPipeline, states: dict[str, _RunState]) -> None:
    for state in states.values():
        if state.work.spec.update_type == "general" and state.general_failed:
            state.frames.clear()
            continue
        _commit_state(pipeline, state)


def _finish_state(pipeline: IngestionPipeline, state: _RunState) -> IngestionReport:
    error_message = "; ".join(state.errors[:5]) if state.errors else None
    if len(state.errors) > 5:
        error_message = f"{error_message}; ... {len(state.errors) - 5} more"
    status = (
        "failed"
        if state.general_failed or (state.failure_count and not state.successful_jobs)
        else "partial"
        if state.failure_count
        else "success"
    )
    pending_count = len(
        pipeline.metadata.pending_update_jobs(
            source=state.work.spec.source,
            dataset=state.work.spec.name,
        )
    )
    pipeline.metadata.record_run(
        run_id=state.run_id,
        source=state.work.spec.source,
        dataset=state.work.spec.name,
        mode=state.work.spec.update_type,
        status=status,
        request_count=state.request_count,
        success_count=state.success_count,
        failure_count=state.failure_count,
        rows_downloaded=state.rows_downloaded,
        rows_committed=state.rows_committed,
        error_message=error_message,
    )
    return IngestionReport(
        run_id=state.run_id,
        source=state.work.spec.source,
        dataset=state.work.spec.name,
        status=status,
        rows_downloaded=state.rows_downloaded,
        rows_committed=state.rows_committed,
        request_count=state.request_count,
        success_count=state.success_count,
        failure_count=state.failure_count,
        pending_job_count=pending_count,
        elapsed_seconds=time.perf_counter() - state.started_at,
        fetch_seconds=state.fetch_seconds,
        commit_seconds=state.commit_seconds,
        metadata_seconds=state.metadata_seconds,
        commit_count=state.commit_count,
        partitions_rewritten=state.partitions_rewritten,
        peak_in_flight=state.peak_in_flight,
        error_message=error_message,
    )


def _job_key(spec: DatasetSpec, request: dict[str, object]) -> str:
    payload = json.dumps(
        {
            "source": spec.source,
            "dataset": spec.name,
            "update_type": spec.update_type,
            "request": request,
        },
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.blake2b(payload.encode(), digest_size=16).hexdigest()


def _after_pending_asset_ranges(
    work: DatasetUpdateWork,
    request: dict[str, object],
    pending_rows: Sequence[dict[str, Any]],
) -> dict[str, object] | None:
    if work.spec.update_type != "by_asset" or "id" not in request:
        return request
    matching_ends = [
        _date_value(dict(row["request_params"])["end"])
        for row in pending_rows
        if row["dataset"] == work.spec.name
        and dict(row["request_params"]).get("id") == request.get("id")
        and "end" in dict(row["request_params"])
    ]
    if not matching_ends:
        return request
    start = max(_date_value(request["start"]), max(matching_ends) + timedelta(days=1))
    end = _date_value(request["end"])
    if start > end:
        return None
    request["start"] = start.isoformat()
    return request


def _date_value(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    return datetime.strptime(text, "%Y-%m-%d").date()


def combine_reports(source: str, reports: Sequence[IngestionReport]) -> UpdateReport:
    return UpdateReport(
        source=source,
        datasets=tuple(report.dataset for report in reports),
        runs=tuple(reports),
        pending_job_count=sum(report.pending_job_count for report in reports),
        elapsed_seconds=max(
            (report.elapsed_seconds for report in reports), default=0.0
        ),
        fetch_seconds=sum(report.fetch_seconds for report in reports),
        commit_seconds=sum(report.commit_seconds for report in reports),
        metadata_seconds=sum(report.metadata_seconds for report in reports),
        commit_count=sum(report.commit_count for report in reports),
        partitions_rewritten=sum(report.partitions_rewritten for report in reports),
        peak_in_flight=max((report.peak_in_flight for report in reports), default=0),
    )


def _fair_tasks(
    tasks: Sequence[UpdateTask],
) -> list[UpdateTask]:
    """Round-robin datasets while preserving each dataset's request order."""

    grouped: dict[str, deque[UpdateTask]] = {}
    for task in tasks:
        grouped.setdefault(task[0].spec.name, deque()).append(task)
    result = []
    queues = deque(grouped.values())
    while queues:
        queue = queues.popleft()
        result.append(queue.popleft())
        if queue:
            queues.append(queue)
    return result


def _partition_count(frame: pl.DataFrame, spec: DatasetSpec) -> int:
    if spec.update_type == "general" or not frame.height:
        return int(bool(frame.height))
    source_for = {target: source for source, target in spec.field_mappings.items()}
    time_column = source_for.get("time", "time")
    if spec.update_type == "by_daily":
        values = frame[time_column]
        if values.dtype == pl.Date:
            return int(values.dt.strftime("%Y-%m").n_unique())
        return len({str(value).replace("-", "")[:6] for value in values})
    asset_column = source_for.get("asset_id", "asset_id")
    pairs = frame.select(time_column, asset_column).unique().iter_rows()
    return len(
        {
            (
                str(value).replace("-", "")[:4],
                stable_bucket(str(asset), ASSET_BUCKET_COUNT),
            )
            for value, asset in pairs
        }
    )


def _fetch_request_pages(
    *,
    spec: DatasetSpec,
    source_adapter: object,
    request: dict[str, Any],
    request_index: int,
    request_options: dict[str, Any],
    max_retries: int,
    retry_backoff_seconds: float,
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
        return [page]

    page_size = int(request_options.get("page_size", 5000))
    limit_param = str(request_options.get("limit_param", "limit"))
    offset_param = str(request_options.get("offset_param", "offset"))
    offset = int(request_options.get("offset_start", 0))
    pages: list[FetchPage] = []
    page_index = 0
    while True:
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
                time.sleep(retry_backoff_seconds)
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
    for key in (
        "pagination",
        "page_size",
        "limit_param",
        "offset_param",
        "offset_start",
    ):
        if key in context.options:
            options[key] = context.options[key]
    return options


def _request_asset(request: dict[str, Any]) -> str | None:
    value = (
        request.get("id")
        or request.get("ts_code")
        or request.get("index_code")
        or request.get("asset_id")
    )
    return None if value is None else str(value)


def _progress_bar(dataset: str, total: int, *, enabled: bool) -> Any | None:
    if not enabled:
        return None
    try:
        from tqdm import tqdm
    except ImportError:
        return None
    return tqdm(total=total, desc=dataset, unit="call")  # type: ignore[no-any-return]
