"""Ledger-driven dataset update orchestration."""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from typing import Any, cast
from uuid import uuid4

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.hashing import stable_bucket
from bagelquant_data.core.request import RequestContext
from bagelquant_data.core.schema import concat_compatible_frames
from bagelquant_data.pipeline.commit import (
    MAX_PARQUET_WRITE_WORKERS,
    CommitResult,
)
from bagelquant_data.pipeline.ingest import IngestionPipeline, IngestionReport
from bagelquant_data.pipeline.scopes import DiscoveryCall, LedgerRequest


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
    partitions_skipped: int = 0
    planning_seconds: float = 0.0
    bytes_written: int = 0


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


type UpdateProgressCallback = Callable[[UpdateProgress], None]


@dataclass(frozen=True, slots=True)
class DatasetUpdateWork:
    """Eligible ledger requests for one dataset."""

    spec: DatasetSpec
    context: RequestContext
    requests: tuple[LedgerRequest, ...]
    run_id: str = field(default_factory=lambda: uuid4().hex)
    discovery_calls: tuple[DiscoveryCall, ...] = ()
    planning_seconds: float = 0.0


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


@dataclass(frozen=True, slots=True)
class PreparedFetch:
    """Provider pages plus worker-prepared request-level response state."""

    pages: tuple[FetchPage, ...]
    frame: pl.DataFrame | None
    validation_error: str | None = None


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
    partitions_skipped: int = 0
    bytes_written: int = 0
    peak_in_flight: int = 0
    fatal_error: str | None = None
    cancelled: bool = False
    pending_api_calls: list[dict[str, Any]] = field(default_factory=list)


type UpdateTask = tuple[DatasetUpdateWork, LedgerRequest]


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

    with (
        pipeline.metadata.writer_session(),
        ThreadPoolExecutor(
            max_workers=MAX_PARQUET_WRITE_WORKERS,
            thread_name_prefix="bagelquant-parquet",
        ) as writer_executor,
    ):
        return _update_datasets(
            source_adapter=source_adapter,
            pipeline=pipeline,
            works=works,
            writer_executor=writer_executor,
        )


def _update_datasets(
    *,
    source_adapter: object,
    pipeline: IngestionPipeline,
    works: Sequence[DatasetUpdateWork],
    writer_executor: ThreadPoolExecutor,
) -> UpdateReport:
    if not works:
        return UpdateReport(source="", datasets=(), runs=())
    if len(works) > 1:
        started_at = time.perf_counter()
        sequential_reports: list[IngestionReport] = []
        for work in works:
            report = _update_datasets(
                source_adapter=source_adapter,
                pipeline=pipeline,
                works=(work,),
                writer_executor=writer_executor,
            )
            sequential_reports.extend(report.runs)
        return _combine(
            works[0].spec.source,
            sequential_reports,
            time.perf_counter() - started_at,
        )
    started_at = time.perf_counter()
    workers = max(1, int(works[0].context.options.get("workers", 4)))
    max_in_flight = max(
        workers, int(works[0].context.options.get("max_in_flight", workers * 2))
    )
    states = {work.spec.name: _RunState(work) for work in works}
    initial_asset_build = (
        works[0].spec.update_type == "by_asset"
        and not pipeline.metadata.manifest(
            works[0].spec.source, works[0].spec.name
        )
    )
    callbacks = {work.spec.name: _progress_callback(work.context) for work in works}
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
        ordered_tasks = _partition_affinity_order(_fair_tasks(tasks))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            if initial_asset_build:
                request_index_offset = 0
                for group in _initial_asset_task_groups(ordered_tasks):
                    _run_fetches(
                        group,
                        executor=executor,
                        source_adapter=source_adapter,
                        pipeline=pipeline,
                        states=states,
                        callbacks=callbacks,
                        totals=totals,
                        completed=completed,
                        max_in_flight=max_in_flight,
                        writer_executor=writer_executor,
                        request_index_offset=request_index_offset,
                    )
                    state = states[works[0].spec.name]
                    _commit_state(
                        pipeline,
                        state,
                        callbacks[state.work.spec.name],
                        completed[state.work.spec.name],
                        totals[state.work.spec.name],
                        writer_executor,
                    )
                    request_index_offset += len(group)
                    if state.cancelled:
                        break
            else:
                repair_tasks = [
                    task
                    for task in ordered_tasks
                    if task[1].request_kind in {"retry", "empty_recheck"}
                ]
                incremental_tasks = [
                    task
                    for task in ordered_tasks
                    if task[1].request_kind not in {"retry", "empty_recheck"}
                ]
                request_index_offset = 0
                for phase in (repair_tasks, incremental_tasks):
                    if not phase:
                        continue
                    _run_fetches(
                        phase,
                        executor=executor,
                        source_adapter=source_adapter,
                        pipeline=pipeline,
                        states=states,
                        callbacks=callbacks,
                        totals=totals,
                        completed=completed,
                        max_in_flight=max_in_flight,
                        writer_executor=writer_executor,
                        request_index_offset=request_index_offset,
                    )
                    for state in states.values():
                        _commit_state(
                            pipeline,
                            state,
                            callbacks[state.work.spec.name],
                            completed[state.work.spec.name],
                            totals[state.work.spec.name],
                            writer_executor,
                        )
                    request_index_offset += len(phase)
                    if any(state.cancelled for state in states.values()):
                        break
        for state in states.values():
            _commit_state(
                pipeline,
                state,
                callbacks[state.work.spec.name],
                completed[state.work.spec.name],
                totals[state.work.spec.name],
                writer_executor,
            )
    except BaseException as exc:  # noqa: BLE001 - finalize before propagation
        caught = exc
        for name, state in states.items():
            if name not in begun:
                continue
            state.fatal_error = str(exc)
            _fail_buffered(pipeline, state, str(exc))
            _fail_running(pipeline, state, str(exc))
    finally:
        finished_reports = tuple(
            _finish_state(pipeline, state)
            for name, state in states.items()
            if name in begun
        )
    for report in finished_reports:
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
    return _combine(
        works[0].spec.source,
        finished_reports,
        time.perf_counter() - started_at,
    )


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
    writer_executor: ThreadPoolExecutor,
    request_index_offset: int = 0,
) -> None:
    task_iter = iter(enumerate(tasks, start=request_index_offset))
    ready: deque[tuple[int, UpdateTask]] = deque()
    futures: dict[Future[PreparedFetch], tuple[float, UpdateTask]] = {}
    stop_submission = False
    next_heartbeat = time.monotonic() + 30.0

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
            run_id: set(pipeline.metadata.claim_update_scopes(scope_ids, run_id=run_id))
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
            _fetch_and_prepare_request,
            spec=work.spec,
            source_adapter=source_adapter,
            ledger_request=request,
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
        if time.monotonic() >= next_heartbeat:
            for state in states.values():
                pipeline.metadata.refresh_update_lease(run_id=state.work.run_id)
            next_heartbeat = time.monotonic() + 30.0
        if cancellation_requested():
            stop_submission = True
            mark_cancelled()
        done, _ = wait(futures, timeout=0.25, return_when=FIRST_COMPLETED)
        if not done:
            continue
        for future in done:
            submitted_at, (work, request) = futures.pop(future)
            state = states[work.spec.name]
            prepared = future.result()
            state.fetch_seconds += time.perf_counter() - submitted_at
            started = time.perf_counter()
            _harvest_request(pipeline, state, request, prepared)
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
                    writer_executor,
                )
        if not stop_submission:
            while len(futures) < max_in_flight and submit_next():
                pass


def _harvest_request(
    pipeline: IngestionPipeline,
    state: _RunState,
    request: LedgerRequest,
    prepared: PreparedFetch,
) -> None:
    pages = prepared.pages
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
    frame = prepared.frame
    if frame is None:
        raise RuntimeError("successful provider response was not prepared")
    if prepared.validation_error is not None:
        pipeline.metadata.record_api_calls(
            {**call, "result_kind": "invalid"} for call in calls
        )
        state.request_count += request_count
        state.rows_downloaded += downloaded
        _transition(
            pipeline,
            state,
            request,
            "invalid",
            prepared.validation_error,
        )
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
    state.pending_api_calls.extend(calls)
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
    writer_executor: ThreadPoolExecutor,
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
    frame = concat_compatible_frames(item[0] for item in buffered)
    try:
        _flush_api_calls(pipeline, state)
        started = time.perf_counter()
        commit = pipeline.commit_frame(
            state.work.spec,
            frame,
            run_id=state.work.run_id,
            writer_executor=writer_executor,
        )
        state.commit_seconds += time.perf_counter() - started
        state.rows_committed += commit.rows_committed
        state.commit_count += 1
        state.partitions_rewritten += commit.partitions_rewritten
        state.partitions_skipped += commit.partitions_skipped
        state.bytes_written += commit.bytes_written
        canonical_maxima = _canonical_data_maxima(
            commit, state.work.spec, buffered
        )
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
            committed_rows=commit.rows_committed,
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
    commit: CommitResult,
    spec: DatasetSpec,
    buffered: Sequence[tuple[pl.DataFrame, LedgerRequest]],
) -> dict[int | None, str]:
    if spec.update_type == "general":
        return {}
    requests = [request for _, request in buffered]
    if spec.update_type == "by_asset":
        maxima = dict(commit.asset_max_times)
        resolved: dict[int | None, str] = {}
        for request in requests:
            asset_maximum = maxima.get(str(request.params["id"]))
            candidates = [
                value
                for value in (
                    request.previous_data_max_time,
                    asset_maximum,
                )
                if value is not None
            ]
            if candidates:
                resolved[request.scope_id] = max(candidates)
        return resolved
    return {
        request.scope_id: str(request.target_end)
        for request in requests
        if request.target_end in commit.present_times
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
    if frame.height == 1:
        return _validate_single_row_response(
            spec,
            request,
            frame,
            required=required,
            time_column=time_column,
            asset_column=asset_column,
        )
    null_counts = frame.null_count().row(0, named=True)
    if any(int(null_counts[field]) for field in required):
        return "response contains null primary keys"
    time_values = _date_expr(time_column)
    checks = [
        time_values.is_null().any().alias("invalid_dates"),
    ]
    if spec.update_type == "by_daily":
        expected = _date_value(request.target_end)
        checks.append(
            (time_values != pl.lit(expected, dtype=pl.Date))
            .any()
            .alias("outside_requested_date")
        )
    else:
        expected_asset = str(request.params["id"])
        checks.append(
            (pl.col(asset_column).cast(pl.String) != expected_asset)
            .any()
            .alias("wrong_asset")
        )
        request_date_column = spec.request_date_field or time_column
        if request_date_column not in frame.columns:
            return f"response missing request date column: {request_date_column}"
        request_dates = _date_expr(request_date_column)
        lower = _date_value(request.params["start"])
        upper = _date_value(request.params["end"])
        checks.extend(
            (
                request_dates.is_null().any().alias("invalid_request_dates"),
                (
                    (request_dates < pl.lit(lower, dtype=pl.Date))
                    | (request_dates > pl.lit(upper, dtype=pl.Date))
                )
                .any()
                .alias("outside_requested_range"),
            )
        )
    payload = [field for field in frame.columns if field not in required]
    has_payload = any(
        int(null_counts[field]) < frame.height for field in payload
    )
    summary = frame.select(checks).row(0, named=True)
    if summary["invalid_dates"]:
        return "response contains invalid dates"
    if spec.update_type == "by_daily":
        if summary["outside_requested_date"]:
            return (
                f"response contains dates outside requested date {expected.isoformat()}"
            )
    else:
        if summary["wrong_asset"]:
            return f"response contains assets other than {expected_asset}"
        if summary["invalid_request_dates"]:
            return "response contains invalid request dates"
        if summary["outside_requested_range"]:
            return "response contains dates outside requested range"
    if payload and not has_payload:
        return "response payload is entirely null"
    return None


def _validate_single_row_response(
    spec: DatasetSpec,
    request: LedgerRequest,
    frame: pl.DataFrame,
    *,
    required: list[str],
    time_column: str,
    asset_column: str,
) -> str | None:
    row = frame.row(0, named=True)
    if any(row[field] is None for field in required):
        return "response contains null primary keys"
    try:
        response_date = _date_value(row[time_column])
    except (TypeError, ValueError):
        return "response contains invalid dates"
    if spec.update_type == "by_daily":
        expected = _date_value(request.target_end)
        if response_date != expected:
            return (
                f"response contains dates outside requested date {expected.isoformat()}"
            )
    else:
        expected_asset = str(request.params["id"])
        if str(row[asset_column]) != expected_asset:
            return f"response contains assets other than {expected_asset}"
        request_date_column = spec.request_date_field or time_column
        if request_date_column not in row:
            return f"response missing request date column: {request_date_column}"
        try:
            request_date = _date_value(row[request_date_column])
        except (TypeError, ValueError):
            return "response contains invalid request dates"
        lower = _date_value(request.params["start"])
        upper = _date_value(request.params["end"])
        if request_date < lower or request_date > upper:
            return "response contains dates outside requested range"
    payload = [field for field in frame.columns if field not in required]
    if payload and all(row[field] is None for field in payload):
        return "response payload is entirely null"
    return None


def _fail_buffered(pipeline: IngestionPipeline, state: _RunState, message: str) -> None:
    _flush_api_calls(pipeline, state)
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


def _remaining_scope_count(pipeline: IngestionPipeline, state: _RunState) -> int:
    rows = pipeline.metadata.update_scopes(
        source=state.work.spec.source,
        dataset=state.work.spec.name,
        status=("pending", "failed"),
    )
    return len(rows)


def _finish_state(pipeline: IngestionPipeline, state: _RunState) -> IngestionReport:
    _flush_api_calls(pipeline, state)
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
        planning_seconds=state.work.planning_seconds,
        commit_count=state.commit_count,
        partitions_rewritten=state.partitions_rewritten,
        partitions_skipped=state.partitions_skipped,
        bytes_written=state.bytes_written,
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
        planning_seconds=sum(report.planning_seconds for report in reports),
        commit_count=sum(report.commit_count for report in reports),
        partitions_rewritten=sum(report.partitions_rewritten for report in reports),
        partitions_skipped=sum(report.partitions_skipped for report in reports),
        bytes_written=sum(report.bytes_written for report in reports),
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
    """Run durable repair scopes before forward or revision work."""

    repair_kinds = {"retry", "empty_recheck"}
    retries = [task for task in tasks if task[1].request_kind in repair_kinds]
    incremental = [task for task in tasks if task[1].request_kind not in repair_kinds]
    return [*retries, *incremental]


def _partition_affinity_order(tasks: Sequence[UpdateTask]) -> list[UpdateTask]:
    """Keep retry priority while making physical partition work contiguous."""

    repair_kinds = {"retry", "empty_recheck"}
    retries = [task for task in tasks if task[1].request_kind in repair_kinds]
    incremental = [task for task in tasks if task[1].request_kind not in repair_kinds]
    return [
        *sorted(retries, key=_partition_affinity),
        *sorted(incremental, key=_partition_affinity),
    ]


def _initial_asset_task_groups(
    tasks: Sequence[UpdateTask],
) -> list[list[UpdateTask]]:
    """Group a clean asset build at retry/forward and bucket boundaries."""

    groups: list[list[UpdateTask]] = []
    current: list[UpdateTask] = []
    current_key: tuple[bool, int] | None = None
    for task in tasks:
        work, request = task
        key = (
            request.request_kind != "retry",
            stable_bucket(
                str(request.params["id"]), work.spec.asset_bucket_count
            ),
        )
        if current and key != current_key:
            groups.append(current)
            current = []
        current_key = key
        current.append(task)
    if current:
        groups.append(current)
    return groups


def _partition_affinity(task: UpdateTask) -> tuple[int, str, str]:
    work, request = task
    spec = work.spec
    if spec.update_type == "by_daily":
        value = request.target_end or request.params.get(spec.date_param or "date")
        if value is None:
            return (0, "", "")
        day = _date_value(value)
        return (0, f"{day.year:04d}-{day.month:02d}", day.isoformat())
    if spec.update_type == "by_asset":
        asset_id = str(request.params["id"])
        return (
            1,
            f"{stable_bucket(asset_id, spec.asset_bucket_count):08d}",
            asset_id,
        )
    return (2, "", "")


def _fetch_and_prepare_request(
    *,
    spec: DatasetSpec,
    source_adapter: object,
    ledger_request: LedgerRequest,
    request_index: int,
    request_options: dict[str, Any],
    max_retries: int,
    retry_backoff_seconds: float,
    cancel_requested: Callable[[], bool] | None,
) -> PreparedFetch:
    """Fetch, combine, and validate one logical request in its fetch worker."""

    request = ledger_request.params
    pages = _fetch_request_pages(
        spec=spec,
        source_adapter=source_adapter,
        request=request,
        request_index=request_index,
        request_options=request_options,
        max_retries=max_retries,
        retry_backoff_seconds=retry_backoff_seconds,
        cancel_requested=cancel_requested,
    )
    if any(page.status != "success" for page in pages):
        return PreparedFetch(tuple(pages), None)
    frames = [
        page.frame for page in pages if page.frame is not None and page.frame.height
    ]
    frame = (
        pl.DataFrame()
        if not frames
        else frames[0]
        if len(frames) == 1
        else concat_compatible_frames(frames)
    )
    return PreparedFetch(
        tuple(pages),
        frame,
        _validate_response(spec, ledger_request, frame),
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
    cancel_requested: Callable[[], bool] | None,
) -> list[FetchPage]:
    pagination = request_options.get("pagination")
    if pagination == "adaptive_date_range":
        return _fetch_adaptive_date_range(
            spec=spec,
            source_adapter=source_adapter,
            request=request,
            request_index=request_index,
            request_options=request_options,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
            cancel_requested=cancel_requested,
        )
    if pagination != "offset":
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


def _fetch_adaptive_date_range(
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
    """Bisect saturated date ranges without publishing their parent responses."""

    row_limit = int(request_options.get("row_limit", 0))
    start_param = str(request_options.get("start_param", "start"))
    end_param = str(request_options.get("end_param", "end"))
    minimum_window_days = int(request_options.get("minimum_window_days", 0))
    max_pages = int(request_options.get("max_pages", 10_000))
    if row_limit <= 0 or minimum_window_days < 0 or max_pages <= 0:
        return [
            _invalid_pagination_page(
                request_index,
                request,
                "adaptive date pagination requires positive row_limit/max_pages "
                "and non-negative minimum_window_days",
            )
        ]
    if start_param not in request or end_param not in request:
        return [
            _invalid_pagination_page(
                request_index,
                request,
                f"adaptive date pagination requires {start_param!r} and {end_param!r}",
            )
        ]
    try:
        request_start = _date_value(request[start_param])
        request_end = _date_value(request[end_param])
    except (TypeError, ValueError) as error:
        return [
            _invalid_pagination_page(
                request_index,
                request,
                f"adaptive date pagination received invalid bounds: {error}",
            )
        ]
    if request_start > request_end:
        return [
            _invalid_pagination_page(
                request_index,
                request,
                "adaptive date pagination start is after end",
            )
        ]

    pending: deque[tuple[date, date, str]] = deque(
        [(request_start, request_end, str(request_index))]
    )
    pages: list[FetchPage] = []
    while pending:
        lower, upper, request_key = pending.popleft()
        if len(pages) >= max_pages:
            pages.append(
                _invalid_pagination_page(
                    f"{request_index}:exhausted",
                    request,
                    "adaptive date pagination exhausted configured max_pages",
                )
            )
            return pages
        ranged = {
            **request,
            start_param: lower.isoformat(),
            end_param: upper.isoformat(),
        }
        page = _fetch_one(
            spec,
            source_adapter,
            ranged,
            request_key,
            max_retries,
            retry_backoff_seconds,
            cancel_requested,
        )
        pages.append(page)
        if page.status != "success":
            return pages
        if page.row_count < row_limit:
            continue
        span_days = (upper - lower).days
        if span_days <= minimum_window_days:
            pages.append(
                _invalid_pagination_page(
                    f"{request_key}:saturated",
                    ranged,
                    (
                        f"adaptive date range {lower.isoformat()} to "
                        f"{upper.isoformat()} still returned {page.row_count} rows "
                        f"at configured limit {row_limit}"
                    ),
                )
            )
            return pages
        midpoint = lower + timedelta(days=span_days // 2)
        pages[-1] = replace(page, frame=None)
        pending.appendleft(
            (midpoint + timedelta(days=1), upper, f"{request_key}:1")
        )
        pending.appendleft((lower, midpoint, f"{request_key}:0"))
    return pages


def _invalid_pagination_page(
    request_key: str | int,
    request: dict[str, Any],
    message: str,
) -> FetchPage:
    return FetchPage(
        str(request_key),
        request,
        None,
        "invalid",
        0,
        0,
        message,
        _request_asset(request),
    )


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
            if attempt + 1 < max_retries and not _cooperative_backoff(
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
        "row_limit",
        "limit_param",
        "offset_param",
        "offset_start",
        "start_param",
        "end_param",
        "minimum_window_days",
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
    if len(text) == 8 and text.isdigit():
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    return date.fromisoformat(text)


def _request_asset(request: dict[str, Any]) -> str | None:
    value = (
        request.get("id")
        or request.get("ts_code")
        or request.get("index_code")
        or request.get("asset_id")
    )
    return None if value is None else str(value)


def _flush_api_calls(pipeline: IngestionPipeline, state: _RunState) -> None:
    if not state.pending_api_calls:
        return
    pipeline.metadata.record_api_calls(state.pending_api_calls)
    state.pending_api_calls.clear()


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
