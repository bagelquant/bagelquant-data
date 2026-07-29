"""Ledger-driven dataset update orchestration."""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, TypeAlias, cast
from uuid import uuid4

import polars as pl

from bagelquant_data.core.dataset import ASSET_BUCKET_COUNT, DatasetSpec
from bagelquant_data.core.hashing import stable_bucket
from bagelquant_data.core.request import RequestContext
from bagelquant_data.pipeline.ingest import IngestionPipeline, IngestionReport
from bagelquant_data.pipeline.scopes import DiscoveryCall, LedgerRequest
from bagelquant_data.query.raw import RawQueryService


@dataclass(frozen=True, slots=True)
class PartitionChange:
    """Content-hash change for one committed partition."""

    dataset: str
    partition_path: str
    before_hash: str | None
    after_hash: str | None
    min_time: str | None = None
    max_time: str | None = None


@dataclass(frozen=True, slots=True)
class UpdateReport:
    """Update report for one or more datasets."""

    source: str
    datasets: tuple[str, ...]
    runs: tuple[IngestionReport, ...]
    remaining_scope_count: int = 0
    elapsed_seconds: float = 0.0
    fetch_seconds: float = 0.0
    commit_seconds: float = 0.0
    metadata_seconds: float = 0.0
    commit_count: int = 0
    partitions_rewritten: int = 0
    peak_in_flight: int = 0
    changed_partitions: tuple[PartitionChange, ...] = ()


@dataclass(frozen=True, slots=True)
class UpdateProgress:
    """Immutable scheduler progress for one dataset."""

    dataset: str
    phase: str
    completed: int
    total: int
    success_count: int
    failure_count: int
    rows_downloaded: int
    status: str
    rows_committed: int = 0
    empty_count: int = 0
    invalid_count: int = 0
    remaining_count: int = 0


UpdateProgressCallback: TypeAlias = Callable[[UpdateProgress], None]


@dataclass(frozen=True, slots=True)
class DatasetUpdateWork:
    """Eligible ledger requests for one dataset."""

    spec: DatasetSpec
    context: RequestContext
    requests: tuple[LedgerRequest, ...]
    run_id: str = field(default_factory=lambda: uuid4().hex)
    discovery_calls: tuple[DiscoveryCall, ...] = ()


@dataclass(frozen=True, slots=True)
class FetchPage:
    """Result of one physical provider call."""

    request_key: str
    request_params: dict[str, Any]
    frame: pl.DataFrame | None
    status: str
    row_count: int
    retry_count: int
    error_message: str | None = None
    asset_id: str | None = None
    fatal: bool = False


@dataclass(slots=True)
class _RunState:
    work: DatasetUpdateWork
    request_count: int = 0
    success_count: int = 0
    empty_count: int = 0
    failure_count: int = 0
    invalid_count: int = 0
    rows_downloaded: int = 0
    rows_committed: int = 0
    errors: list[str] = field(default_factory=list)
    buffered: list[tuple[pl.DataFrame, LedgerRequest]] = field(default_factory=list)
    buffered_bytes: int = 0
    started_at: float = field(default_factory=time.perf_counter)
    fetch_seconds: float = 0.0
    commit_seconds: float = 0.0
    metadata_seconds: float = 0.0
    commit_count: int = 0
    partitions_rewritten: int = 0
    peak_in_flight: int = 0
    fatal_error: str | None = None
    cancelled: bool = False


UpdateTask: TypeAlias = tuple[DatasetUpdateWork, LedgerRequest]


def update_dataset(
    *,
    spec: DatasetSpec,
    source_adapter: object,
    pipeline: IngestionPipeline,
    context: RequestContext,
    requests: Sequence[LedgerRequest],
) -> IngestionReport:
    """Fetch one dataset through the shared bounded scheduler."""

    return update_datasets(
        source_adapter=source_adapter,
        pipeline=pipeline,
        works=(DatasetUpdateWork(spec, context, tuple(requests)),),
    ).runs[0]


def update_datasets(
    *,
    source_adapter: object,
    pipeline: IngestionPipeline,
    works: Sequence[DatasetUpdateWork],
) -> UpdateReport:
    """Claim centrally selected scopes, fetch concurrently, then commit."""

    if not works:
        return UpdateReport(source="", datasets=(), runs=())
    if len(works) > 1:
        started_at = time.perf_counter()
        reports: list[IngestionReport] = []
        for work in works:
            report = update_datasets(
                source_adapter=source_adapter,
                pipeline=pipeline,
                works=(work,),
            )
            reports.extend(report.runs)
        return _combine(
            works[0].spec.source,
            reports,
            time.perf_counter() - started_at,
        )
    started_at = time.perf_counter()
    workers = max(1, int(works[0].context.options.get("workers", 4)))
    max_in_flight = max(
        workers, int(works[0].context.options.get("max_in_flight", workers * 2))
    )
    states = {work.spec.name: _RunState(work) for work in works}
    callbacks = {work.spec.name: _progress_callback(work.context) for work in works}
    progresses = {
        work.spec.name: _progress_bar(
            work.spec.name,
            len(work.requests),
            enabled=bool(work.context.options.get("progress", True)),
        )
        for work in works
    }
    tasks: list[UpdateTask] = []
    begun: set[str] = set()
    totals = dict.fromkeys(states, 0)
    completed = dict.fromkeys(states, 0)
    caught: BaseException | None = None
    try:
        for work in works:
            pipeline.metadata.begin_run(
                run_id=work.run_id,
                source=work.spec.source,
                dataset=work.spec.name,
                mode=work.spec.update_type,
                owner_id=_owner_id(work.context),
            )
            pipeline.metadata.record_api_calls(_discovery_api_call_rows(work))
            begun.add(work.spec.name)
            _emit_progress(callbacks[work.spec.name], states[work.spec.name], "sync", 0)
            tasks.extend((work, request) for request in work.requests)
        totals = {name: len(states[name].work.requests) for name in states}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            _run_fetches(
                _retry_first(_fair_tasks(tasks)),
                executor=executor,
                source_adapter=source_adapter,
                pipeline=pipeline,
                states=states,
                callbacks=callbacks,
                totals=totals,
                completed=completed,
                max_in_flight=max_in_flight,
            )
        for state in states.values():
            _commit_state(
                pipeline,
                state,
                callbacks[state.work.spec.name],
                completed[state.work.spec.name],
                totals[state.work.spec.name],
            )
    except BaseException as exc:  # finalize durable run state before propagation
        caught = exc
        for name, state in states.items():
            if name not in begun:
                continue
            state.fatal_error = str(exc)
            _fail_buffered(pipeline, state, str(exc))
            _fail_running(pipeline, state, str(exc))
    finally:
        for progress in progresses.values():
            if progress is not None:
                progress.close()
        reports = tuple(
            _finish_state(pipeline, state)
            for name, state in states.items()
            if name in begun
        )
    for report in reports:
        _emit_progress(
            callbacks[report.dataset],
            states[report.dataset],
            "complete",
            completed[report.dataset],
            status=report.status,
            total=totals[report.dataset],
        )
    if caught is not None:
        raise caught
    return _combine(works[0].spec.source, reports, time.perf_counter() - started_at)


def _run_fetches(
    tasks: Sequence[UpdateTask],
    *,
    executor: ThreadPoolExecutor,
    source_adapter: object,
    pipeline: IngestionPipeline,
    states: dict[str, _RunState],
    callbacks: dict[str, UpdateProgressCallback | None],
    totals: dict[str, int],
    completed: dict[str, int],
    max_in_flight: int,
) -> None:
    task_iter = iter(enumerate(tasks))
    ready: deque[tuple[int, UpdateTask]] = deque()
    futures: dict[Future[list[FetchPage]], tuple[float, UpdateTask]] = {}
    stop_submission = False

    def cancellation_requested() -> bool:
        return any(_cancel_requested(state.work.context) for state in states.values())

    def mark_cancelled() -> None:
        for state in states.values():
            state.cancelled = True

    def fill_ready(capacity: int) -> bool:
        nonlocal stop_submission
        if stop_submission or capacity <= 0:
            return False
        if cancellation_requested():
            stop_submission = True
            mark_cancelled()
            return False
        candidates: list[tuple[int, UpdateTask]] = []
        while len(candidates) < capacity:
            try:
                index, task = next(task_iter)
            except StopIteration:
                break
            candidates.append((index, task))
        if not candidates:
            return False
        by_run: dict[str, list[int]] = {}
        for _, (work, request) in candidates:
            if request.scope_id is not None:
                by_run.setdefault(work.run_id, []).append(request.scope_id)
        claimed_by_run = {
            run_id: set(
                pipeline.metadata.claim_update_scopes(scope_ids, run_id=run_id)
            )
            for run_id, scope_ids in by_run.items()
        }
        for candidate in candidates:
            _, (work, request) = candidate
            if (
                request.scope_id is not None
                and request.scope_id not in claimed_by_run.get(work.run_id, set())
            ):
                totals[work.spec.name] = max(0, totals[work.spec.name] - 1)
                continue
            ready.append(candidate)
        return bool(ready)

    def submit_next() -> bool:
        if not ready and not fill_ready(max_in_flight - len(futures)):
            return False
        index, task = ready.popleft()
        work, request = task
        _emit_progress(
            callbacks[work.spec.name],
            states[work.spec.name],
            "claim",
            completed[work.spec.name],
            total=totals[work.spec.name],
        )
        future = executor.submit(
            _fetch_request_pages,
            spec=work.spec,
            source_adapter=source_adapter,
            request=request.params,
            request_index=index,
            request_options=_request_options(work.context),
            max_retries=max(1, int(work.context.options.get("max_retries", 3))),
            retry_backoff_seconds=float(
                work.context.options.get("retry_backoff_seconds", 60.0)
            ),
            cancel_requested=_cancel_callback(work.context),
        )
        futures[future] = (time.perf_counter(), task)
        states[work.spec.name].peak_in_flight = max(
            states[work.spec.name].peak_in_flight, len(futures)
        )
        return True

    while len(futures) < max_in_flight and submit_next():
        pass
    while futures:
        if cancellation_requested():
            stop_submission = True
            mark_cancelled()
        done, _ = wait(futures, timeout=0.25, return_when=FIRST_COMPLETED)
        if not done:
            continue
        for future in done:
            submitted_at, (work, request) = futures.pop(future)
            state = states[work.spec.name]
            pages = future.result()
            state.fetch_seconds += time.perf_counter() - submitted_at
            started = time.perf_counter()
            _harvest_request(pipeline, state, request, pages)
            state.metadata_seconds += time.perf_counter() - started
            completed[work.spec.name] += 1
            _emit_progress(
                callbacks[work.spec.name],
                state,
                "fetch",
                completed[work.spec.name],
                total=totals[work.spec.name],
            )
            configured_batch_size = work.context.options.get("batch_size")
            max_bytes = (
                max(1, int(work.context.options.get("max_buffer_mb", 512)))
                * 1024
                * 1024
            )
            if work.spec.update_type != "general" and (
                (
                    configured_batch_size is not None
                    and len(state.buffered) >= max(1, int(configured_batch_size))
                )
                or state.buffered_bytes >= max_bytes
            ):
                _commit_state(
                    pipeline,
                    state,
                    callbacks[work.spec.name],
                    completed[work.spec.name],
                    totals[work.spec.name],
                )
            pipeline.metadata.refresh_update_lease(run_id=work.run_id)
        if not stop_submission:
            while len(futures) < max_in_flight and submit_next():
                pass


def _harvest_request(
    pipeline: IngestionPipeline,
    state: _RunState,
    request: LedgerRequest,
    pages: Sequence[FetchPage],
) -> None:
    calls = _api_call_rows(state.work, request, pages)
    request_count = len(pages)
    downloaded = sum(page.row_count for page in pages if page.status == "success")
    failures = [page for page in pages if page.status != "success"]
    if failures:
        pipeline.metadata.record_api_calls(calls)
        state.request_count += request_count
        state.rows_downloaded += downloaded
        status = (
            "invalid"
            if any(page.status == "invalid" for page in failures)
            else "failed"
        )
        if any(page.status == "cancelled" for page in failures):
            state.cancelled = True
        message = failures[-1].error_message or "provider request failed"
        _transition(pipeline, state, request, status, message)
        return
    frames = [
        page.frame for page in pages if page.frame is not None and page.frame.height
    ]
    frame = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
    error = _validate_response(state.work.spec, request, frame)
    if error is not None:
        pipeline.metadata.record_api_calls(
            ({**call, "result_kind": "invalid"} for call in calls)
        )
        state.request_count += request_count
        state.rows_downloaded += downloaded
        _transition(pipeline, state, request, "invalid", error)
        return
    if frame.is_empty():
        pipeline.metadata.record_empty_scope_result(
            calls=({**call, "result_kind": "empty"} for call in calls),
            scope_id=request.scope_id,
            run_id=state.work.run_id,
            checked_through=request.target_end,
            recheck_after=None,
        )
        state.request_count += request_count
        state.rows_downloaded += downloaded
        state.empty_count += 1
        return
    pipeline.metadata.record_api_calls(calls)
    state.request_count += request_count
    state.rows_downloaded += downloaded
    state.buffered.append((frame, request))
    state.buffered_bytes += int(frame.estimated_size())


def _api_call_rows(
    work: DatasetUpdateWork,
    request: LedgerRequest,
    pages: Sequence[FetchPage],
) -> list[dict[str, Any]]:
    return [
        {
            "run_id": work.run_id,
            "source": work.spec.source,
            "dataset": work.spec.name,
            "request_key": page.request_key,
            "request_params": page.request_params,
            "status": page.status,
            "row_count": page.row_count,
            "retry_count": page.retry_count,
            "error_message": page.error_message,
            "asset_id": page.asset_id,
            "scope_id": request.scope_id,
            "request_kind": request.request_kind,
        }
        for page in pages
    ]


def _discovery_api_call_rows(work: DatasetUpdateWork) -> list[dict[str, Any]]:
    return [
        {
            "run_id": work.run_id,
            "source": work.spec.source,
            "dataset": work.spec.name,
            "request_key": f"discovery:{index}",
            "request_params": {"api": call.api, **call.params},
            "status": "success",
            "row_count": call.row_count,
            "retry_count": 0,
            "request_kind": "discovery",
        }
        for index, call in enumerate(work.discovery_calls)
    ]


def _commit_state(
    pipeline: IngestionPipeline,
    state: _RunState,
    callback: UpdateProgressCallback | None,
    completed: int,
    total: int,
) -> None:
    if not state.buffered:
        return
    if state.work.spec.update_type == "general" and (
        state.failure_count or state.invalid_count
    ):
        state.buffered.clear()
        state.buffered_bytes = 0
        return
    _emit_progress(callback, state, "commit", completed, total=total)
    buffered = list(state.buffered)
    frame = pl.concat([item[0] for item in buffered], how="diagonal_relaxed")
    try:
        started = time.perf_counter()
        committed = pipeline.commit_frame(
            state.work.spec, frame, run_id=state.work.run_id
        )
        state.commit_seconds += time.perf_counter() - started
        state.rows_committed += committed
        state.commit_count += 1
        state.partitions_rewritten += _partition_count(frame, state.work.spec)
        canonical_maxima = _canonical_data_maxima(pipeline, state.work.spec, buffered)
        transitions = [
            _success_transition(
                state.work.spec,
                request,
                scope_frame,
                data_max_time=canonical_maxima.get(request.scope_id),
            )
            for scope_frame, request in buffered
            if request.scope_id is not None
        ]
        started = time.perf_counter()
        pipeline.metadata.transition_update_scopes(
            transitions,
            run_id=state.work.run_id,
            committed_rows=committed,
        )
        state.metadata_seconds += time.perf_counter() - started
        state.success_count += len(buffered)
    except Exception as exc:
        message = f"commit failed: {exc}"
        for _, request in buffered:
            _transition(pipeline, state, request, "failed", message)
        raise
    finally:
        state.buffered.clear()
        state.buffered_bytes = 0


def _transition(
    pipeline: IngestionPipeline,
    state: _RunState,
    request: LedgerRequest,
    status: str,
    error: str | None,
) -> None:
    if request.scope_id is not None:
        pipeline.metadata.transition_update_scopes(
            [
                {
                    "scope_id": request.scope_id,
                    "status": status,
                    "last_error": error,
                }
            ],
            run_id=state.work.run_id,
        )
    if status == "invalid":
        state.invalid_count += 1
        state.errors.append(error or "invalid response")
    elif status == "failed":
        state.failure_count += 1
        state.errors.append(error or "failed request")


def _success_transition(
    spec: DatasetSpec,
    request: LedgerRequest,
    frame: pl.DataFrame,
    *,
    data_max_time: str | None,
) -> dict[str, object]:
    if data_max_time is None:
        raise RuntimeError(
            f"canonical data maximum missing after commit for {spec.source}/{spec.name}"
        )
    return {
        "scope_id": request.scope_id,
        "status": "success",
        "data_max_time": data_max_time,
        "row_count": frame.height,
        "provider_checked_through": request.target_end,
        "provider_recheck_after": request.recheck_after,
        "last_error": None,
    }


def _canonical_data_maxima(
    pipeline: IngestionPipeline,
    spec: DatasetSpec,
    buffered: Sequence[tuple[pl.DataFrame, LedgerRequest]],
) -> dict[int | None, str]:
    if spec.update_type == "general":
        return {}
    raw = RawQueryService(pipeline.parquet, pipeline.metadata)
    requests = [request for _, request in buffered]
    assets = [
        str(request.params["id"])
        for request in requests
        if spec.update_type == "by_asset" and "id" in request.params
    ] or None
    daily_dates = [
        str(request.target_end)
        for request in requests
        if spec.update_type == "by_daily" and request.target_end is not None
    ]
    frame = raw.query(
        spec.name,
        source=spec.source,
        start=min(daily_dates) if daily_dates else None,
        end=max(daily_dates) if daily_dates else None,
        assets=assets,
        fields=("time", "asset_id"),
    ).collect()
    if frame.is_empty() or "time" not in frame.columns:
        return {}
    if spec.update_type == "by_asset":
        maxima = {
            str(row["asset_id"]): row["time"].isoformat()
            for row in frame.group_by("asset_id").agg(pl.col("time").max()).to_dicts()
            if row["asset_id"] is not None and row["time"] is not None
        }
        return {
            request.scope_id: maxima[str(request.params["id"])]
            for request in requests
            if str(request.params["id"]) in maxima
        }
    present = {value.isoformat() for value in frame["time"].unique().to_list()}
    return {
        request.scope_id: str(request.target_end)
        for request in requests
        if request.target_end in present
    }


def _validate_response(
    spec: DatasetSpec, request: LedgerRequest, frame: pl.DataFrame
) -> str | None:
    if frame.is_empty():
        return None
    if spec.update_type == "general":
        return None
    time_column = _source_column(spec, "time")
    asset_column = _source_column(spec, "asset_id")
    required = [time_column, asset_column]
    required.extend(_source_column(spec, key) for key in spec.primary_key_extra)
    missing = [field for field in required if field not in frame.columns]
    if missing:
        return f"response missing required key columns: {', '.join(missing)}"
    if any(frame[field].null_count() for field in required):
        return "response contains null primary keys"
    dates = frame.select(_date_expr(time_column).alias("value")).get_column("value")
    if dates.null_count():
        return "response contains invalid dates"
    if spec.update_type == "by_daily":
        expected = _date_value(request.target_end)
        if any(value != expected for value in dates):
            return (
                f"response contains dates outside requested date {expected.isoformat()}"
            )
    if spec.update_type == "by_asset":
        expected_asset = str(request.params["id"])
        if any(str(value) != expected_asset for value in frame[asset_column]):
            return f"response contains assets other than {expected_asset}"
        request_date_column = spec.request_date_field or time_column
        if request_date_column not in frame.columns:
            return f"response missing request date column: {request_date_column}"
        request_dates = frame.select(
            _date_expr(request_date_column).alias("value")
        ).get_column("value")
        if request_dates.null_count():
            return "response contains invalid request dates"
        lower = _date_value(request.params["start"])
        upper = _date_value(request.params["end"])
        if any(value < lower or value > upper for value in request_dates):
            return "response contains dates outside requested range"
    payload = [field for field in frame.columns if field not in required]
    if payload and all(frame[field].null_count() == frame.height for field in payload):
        return "response payload is entirely null"
    return None


def _fail_buffered(pipeline: IngestionPipeline, state: _RunState, message: str) -> None:
    for _, request in list(state.buffered):
        _transition(pipeline, state, request, "failed", message)
    state.buffered.clear()
    state.buffered_bytes = 0


def _fail_running(pipeline: IngestionPipeline, state: _RunState, message: str) -> None:
    rows = pipeline.metadata.update_scopes(
        source=state.work.spec.source,
        dataset=state.work.spec.name,
        status="running",
    )
    pipeline.metadata.transition_update_scopes(
        (
            {"scope_id": row["id"], "status": "failed", "last_error": message}
            for row in rows
        ),
        run_id=state.work.run_id,
    )


def _remaining_scope_count(
    pipeline: IngestionPipeline, state: _RunState
) -> int:
    rows = pipeline.metadata.update_scopes(
        source=state.work.spec.source,
        dataset=state.work.spec.name,
        status=("pending", "failed"),
    )
    return len(rows)


def _finish_state(pipeline: IngestionPipeline, state: _RunState) -> IngestionReport:
    remaining = _remaining_scope_count(pipeline, state)
    error = state.fatal_error or ("; ".join(state.errors[:5]) if state.errors else None)
    if state.cancelled and error is None:
        error = "update cancelled by workflow owner"
    attempted = (
        state.success_count
        + state.empty_count
        + state.failure_count
        + state.invalid_count
    )
    if state.cancelled:
        status = "cancelled"
    elif state.fatal_error or (
        attempted and not state.success_count and not state.empty_count
    ):
        status = "failed"
    elif state.failure_count or state.invalid_count:
        status = "partial"
    elif state.empty_count and not state.success_count:
        status = "no_data"
    else:
        status = "success"
    pipeline.metadata.finalize_run(
        run_id=state.work.run_id,
        status=status,
        request_count=state.request_count,
        success_count=state.success_count,
        empty_count=state.empty_count,
        failure_count=state.failure_count + state.invalid_count,
        rows_downloaded=state.rows_downloaded,
        rows_committed=state.rows_committed,
        error_message=error,
    )
    return IngestionReport(
        run_id=state.work.run_id,
        source=state.work.spec.source,
        dataset=state.work.spec.name,
        status=status,
        rows_downloaded=state.rows_downloaded,
        rows_committed=state.rows_committed,
        request_count=state.request_count,
        success_count=state.success_count,
        empty_count=state.empty_count,
        failure_count=state.failure_count + state.invalid_count,
        remaining_scope_count=remaining,
        elapsed_seconds=time.perf_counter() - state.started_at,
        fetch_seconds=state.fetch_seconds,
        commit_seconds=state.commit_seconds,
        metadata_seconds=state.metadata_seconds,
        commit_count=state.commit_count,
        partitions_rewritten=state.partitions_rewritten,
        peak_in_flight=state.peak_in_flight,
        error_message=error,
    )


def _combine(
    source: str, reports: Sequence[IngestionReport], elapsed: float
) -> UpdateReport:
    return UpdateReport(
        source=source,
        datasets=tuple(report.dataset for report in reports),
        runs=tuple(reports),
        remaining_scope_count=sum(report.remaining_scope_count for report in reports),
        elapsed_seconds=elapsed,
        fetch_seconds=sum(report.fetch_seconds for report in reports),
        commit_seconds=sum(report.commit_seconds for report in reports),
        metadata_seconds=sum(report.metadata_seconds for report in reports),
        commit_count=sum(report.commit_count for report in reports),
        partitions_rewritten=sum(report.partitions_rewritten for report in reports),
        peak_in_flight=max((report.peak_in_flight for report in reports), default=0),
    )


def combine_reports(source: str, reports: Sequence[IngestionReport]) -> UpdateReport:
    """Combine already-finished reports for compatibility with empty selections."""

    return _combine(
        source,
        reports,
        max((report.elapsed_seconds for report in reports), default=0.0),
    )


def _fair_tasks(tasks: Sequence[UpdateTask]) -> list[UpdateTask]:
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


def _retry_first(tasks: Sequence[UpdateTask]) -> list[UpdateTask]:
    """Run durable failed scopes before forward or revision work."""

    retries = [task for task in tasks if task[1].request_kind == "retry"]
    incremental = [task for task in tasks if task[1].request_kind != "retry"]
    return [*retries, *incremental]


def _fetch_request_pages(
    *,
    spec: DatasetSpec,
    source_adapter: object,
    request: dict[str, Any],
    request_index: int,
    request_options: dict[str, Any],
    max_retries: int,
    retry_backoff_seconds: float,
    cancel_requested: Callable[[], bool] | None,
) -> list[FetchPage]:
    if request_options.get("pagination") != "offset":
        return [
            _fetch_one(
                spec,
                source_adapter,
                request,
                str(request_index),
                max_retries,
                retry_backoff_seconds,
                cancel_requested,
            )
        ]
    page_size = int(request_options.get("page_size", 5000))
    limit_param = str(request_options.get("limit_param", "limit"))
    offset_param = str(request_options.get("offset_param", "offset"))
    offset = int(request_options.get("offset_start", 0))
    max_pages = int(request_options.get("max_pages", 10_000))
    pages = []
    for page_index in range(max_pages):
        paged = {**request, limit_param: page_size, offset_param: offset}
        page = _fetch_one(
            spec,
            source_adapter,
            paged,
            f"{request_index}:{page_index}",
            max_retries,
            retry_backoff_seconds,
            cancel_requested,
        )
        pages.append(page)
        if page.status != "success" or page.row_count < page_size:
            return pages
        offset += page_size
    pages.append(
        FetchPage(
            f"{request_index}:exhausted",
            request,
            None,
            "invalid",
            0,
            0,
            "pagination exhausted configured max_pages",
            _request_asset(request),
        )
    )
    return pages


def _fetch_one(
    spec: DatasetSpec,
    source_adapter: object,
    request: dict[str, Any],
    request_key: str,
    max_retries: int,
    retry_backoff_seconds: float,
    cancel_requested: Callable[[], bool] | None = None,
) -> FetchPage:
    last_error: Exception | None = None
    for attempt in range(max_retries):
        if cancel_requested is not None and cancel_requested():
            return FetchPage(
                request_key,
                request,
                None,
                "cancelled",
                0,
                attempt,
                "update cancelled before provider retry completed",
                _request_asset(request),
            )
        try:
            frame = source_adapter.fetch(spec.source_api or spec.name, request)  # type: ignore[attr-defined]
            if not isinstance(frame, pl.DataFrame):
                raise TypeError("source adapter must return a Polars DataFrame")
            return FetchPage(
                request_key,
                request,
                frame,
                "success",
                frame.height,
                attempt,
                asset_id=_request_asset(request),
            )
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt + 1 < max_retries:
                if not _cooperative_backoff(
                    retry_backoff_seconds, cancel_requested=cancel_requested
                ):
                    return FetchPage(
                        request_key,
                        request,
                        None,
                        "cancelled",
                        0,
                        attempt,
                        "update cancelled during provider retry backoff",
                        _request_asset(request),
                    )
    return FetchPage(
        request_key,
        request,
        None,
        "failed",
        0,
        max_retries - 1,
        str(last_error) if last_error else "unknown provider error",
        _request_asset(request),
    )


def _request_options(context: RequestContext) -> dict[str, Any]:
    options = (
        dict(context.options.get("source_options", {}))
        if isinstance(context.options.get("source_options"), dict)
        else {}
    )
    for key in (
        "pagination",
        "page_size",
        "limit_param",
        "offset_param",
        "offset_start",
        "max_pages",
    ):
        if key in context.options:
            options[key] = context.options[key]
    return options


def _owner_id(context: RequestContext) -> str | None:
    value = context.options.get("owner_id")
    return None if value is None else str(value)


def _cancel_requested(context: RequestContext) -> bool:
    callback = _cancel_callback(context)
    return bool(callback()) if callback is not None else False


def _cancel_callback(context: RequestContext) -> Callable[[], bool] | None:
    callback = context.options.get("cancel_requested")
    return cast(Callable[[], bool], callback) if callable(callback) else None


def _cooperative_backoff(
    seconds: float,
    *,
    cancel_requested: Callable[[], bool] | None,
) -> bool:
    if cancel_requested is None:
        time.sleep(max(0.0, seconds))
        return True
    deadline = time.monotonic() + max(0.0, seconds)
    while time.monotonic() < deadline:
        if cancel_requested is not None and cancel_requested():
            return False
        time.sleep(min(0.1, max(0.0, deadline - time.monotonic())))
    return cancel_requested is None or not cancel_requested()


def _source_column(spec: DatasetSpec, canonical: str) -> str:
    return next(
        (
            source
            for source, target in spec.field_mappings.items()
            if target == canonical
        ),
        canonical,
    )


def _date_expr(field: str) -> pl.Expr:
    return (
        pl.when(pl.col(field).cast(pl.String).str.len_chars() == 8)
        .then(
            pl.col(field).cast(pl.String).str.strptime(pl.Date, "%Y%m%d", strict=False)
        )
        .otherwise(pl.col(field).cast(pl.Date, strict=False))
    )


def _date_value(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).split("T", maxsplit=1)[0]
    return datetime.strptime(
        text, "%Y%m%d" if len(text) == 8 and text.isdigit() else "%Y-%m-%d"
    ).date()


def _request_asset(request: dict[str, Any]) -> str | None:
    value = (
        request.get("id")
        or request.get("ts_code")
        or request.get("index_code")
        or request.get("asset_id")
    )
    return None if value is None else str(value)


def _partition_count(frame: pl.DataFrame, spec: DatasetSpec) -> int:
    if spec.update_type == "general" or not frame.height:
        return int(bool(frame.height))
    time_column = _source_column(spec, "time")
    if spec.update_type == "by_daily":
        return len({str(value).replace("-", "")[:6] for value in frame[time_column]})
    asset_column = _source_column(spec, "asset_id")
    return len(
        {
            (
                str(value).replace("-", "")[:4],
                stable_bucket(str(asset), ASSET_BUCKET_COUNT),
            )
            for value, asset in frame.select(time_column, asset_column)
            .unique()
            .iter_rows()
        }
    )


def _progress_bar(dataset: str, total: int, *, enabled: bool) -> Any | None:
    if not enabled:
        return None
    try:
        from tqdm import tqdm
    except ImportError:
        return None
    return tqdm(total=total, desc=dataset, unit="scope")  # type: ignore[no-any-return]


def _progress_callback(context: RequestContext) -> UpdateProgressCallback | None:
    callback = context.options.get("progress_callback")
    return cast(UpdateProgressCallback, callback) if callable(callback) else None


def _emit_progress(
    callback: UpdateProgressCallback | None,
    state: _RunState,
    phase: str,
    completed: int,
    *,
    total: int | None = None,
    status: str = "running",
) -> None:
    if callback is None:
        return
    final_total = len(state.work.requests) if total is None else total
    callback(
        UpdateProgress(
            dataset=state.work.spec.name,
            phase=phase,
            completed=completed,
            total=final_total,
            success_count=state.success_count,
            failure_count=state.failure_count,
            rows_downloaded=state.rows_downloaded,
            status=status,
            rows_committed=state.rows_committed,
            empty_count=state.empty_count,
            invalid_count=state.invalid_count,
            remaining_count=max(0, final_total - completed),
        )
    )
