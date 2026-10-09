"""Resolve observation versions before numerical evaluation on ordinary Panels."""
from __future__ import annotations

from datetime import date, timedelta
from collections.abc import Callable, Mapping, Sequence
from typing import Any, cast
from concurrent.futures import CancelledError, ThreadPoolExecutor
from contextlib import nullcontext
import polars as pl

from bagelquant_data.execution import ExecutionOptions

VERSION_DATE = "version_available_date"


def revision_dates(frames: Sequence[pl.DataFrame], start: date, end: date) -> tuple[date, ...]:
    values: set[date] = set()
    for frame in frames:
        if VERSION_DATE in frame.columns:
            dates = frame.filter(pl.col(VERSION_DATE) > pl.col("time"))[VERSION_DATE]
        elif "source_time" in frame.columns:
            dates = frame.filter(pl.col("time") > pl.col("source_time"))["time"]
        elif "snapshot_date" in frame.columns:
            dates = frame.filter(~pl.col("_baseline"))["snapshot_date"]
        else:
            continue
        values.update(value for value in dates.drop_nulls().unique() if start < value <= end)
    return tuple(sorted(values))


def select_versions(frame: pl.DataFrame, *, as_of: date | None = None, history: bool = False) -> pl.DataFrame:
    """Select DataItem values as known at a date, or at each observation's own date."""
    if VERSION_DATE not in frame.columns:
        if frame.is_empty():
            return frame
        raise ValueError("Incompatible DataItem PIT storage; explicitly rebuild the DataItem")
    cutoff = pl.col("time") if history else pl.lit(as_of, dtype=pl.Date)
    if not history and as_of is None:
        raise ValueError("A DataItem snapshot requires an explicit as_of date")
    return (frame.filter(pl.col(VERSION_DATE) <= cutoff)
            .sort([name for name in (VERSION_DATE, "ingested_at", "_commit_seq") if name in frame.columns]).unique(["time", "asset_id"], keep="last", maintain_order=True)
            .sort("time", "asset_id"))


def input_snapshot(frame: pl.DataFrame, cutoff: date) -> pl.DataFrame:
    """Keep causal ordinary observations in bulk; resolve revisions at cutoff."""
    if VERSION_DATE in frame.columns:
        visible = frame.filter(pl.col(VERSION_DATE) <= cutoff)
        return visible.sort([name for name in (VERSION_DATE, "ingested_at", "_commit_seq") if name in visible.columns]).unique(["time", "asset_id"], keep="last", maintain_order=True)
    if "_record_id" in frame.columns:
        visible = frame.filter(pl.col("time") <= cutoff)
        return visible.sort("time", "ingested_at", "_commit_seq").unique("_record_id", keep="last", maintain_order=True)
    if "_snapshot_id" in frame.columns:
        visible = frame.filter(pl.col("snapshot_date") <= cutoff)
        if visible.is_empty():
            return visible
        visible = visible.filter(pl.col("_commit_seq") == visible["_commit_seq"].max())
        visible = visible.filter(pl.col("snapshot_date") == visible["snapshot_date"].max())
        if "ingested_at" in visible.columns:
            visible = visible.filter(pl.col("ingested_at") == visible["ingested_at"].max())
        if "_attestation_id" in visible.columns:
            visible = visible.filter(pl.col("_attestation_id").fill_null(0) == visible["_attestation_id"].fill_null(0).max())
        return visible
    return frame


def _general_snapshot_identity(
    snapshots: Sequence[Mapping[str, Any]], cutoff: date | None, *,
    checks: Sequence[Mapping[str, Any]] = (), strict: bool = False,
    include_historical_baseline: bool = False,
) -> dict[str, Any] | None:
    """Choose a full snapshot from retained events, including empty events."""
    from bagelquant_data.query.raw import _date_value

    candidates = []
    for snapshot in snapshots:
        baseline = bool(snapshot["baseline"])
        day = _date_value(snapshot["pit_date"])
        identity = {**snapshot, "snapshot_date": day,
                    "baseline": baseline, "attestation_id": None}
        if not (strict and baseline) and (
            cutoff is None or day <= cutoff
            or baseline and include_historical_baseline
        ):
            candidates.append(identity)
        if baseline:
            for check in checks:
                if check["baseline"] or check["visible_commit"] != snapshot["seq"]:
                    continue
                available = max(day, _date_value(check["pit_date"]))
                if cutoff is None or available <= cutoff:
                    candidates.append({**identity, "snapshot_date": available,
                                       "ingested_at": check["checked_at"],
                                       "baseline": False, "attestation_id": check["id"]})
    return max(candidates, key=lambda value: (
        int(value["seq"]), value["snapshot_date"], value["ingested_at"],
        value["attestation_id"] or 0,
    )) if candidates else None


def general_input_snapshot(
    frame: pl.DataFrame, cutoff: date | None, *,
    snapshots: Sequence[Mapping[str, Any]] | None = None,
    checks: Sequence[Mapping[str, Any]] = (), strict: bool = False,
    include_historical_baseline: bool = True,
) -> pl.DataFrame:
    """Resolve a general input while retaining its initialization baseline."""

    if snapshots is not None:
        selected = _general_snapshot_identity(
            snapshots, cutoff, checks=checks, strict=strict,
            include_historical_baseline=include_historical_baseline,
        )
        if selected is None:
            return frame.head(0)
        visible = frame.head(0) if not selected["row_count"] else frame.filter(
            (pl.col("_commit_seq") == int(selected["seq"]))
            & (pl.col("snapshot_date") == selected["snapshot_date"])
        )
        if "_attestation_id" in visible.columns:
            visible = visible.filter(
                pl.col("_attestation_id").fill_null(0)
                == (selected["attestation_id"] or 0)
            )
        if selected.get("schema_ipc"):
            import pyarrow as pa
            schema = pl.Schema(pa.ipc.read_schema(pa.BufferReader(
                bytes.fromhex(selected["schema_ipc"]),
            )))
            fields = [*schema.names(), *(["_attestation_id"] if "_attestation_id" in visible.columns else [])]
            visible = visible.select(fields).cast(schema)
        return visible
    if cutoff is None:
        if frame.is_empty():
            return frame
        last = frame["snapshot_date"].max()
        if last is None:
            return frame.head(0)
        cutoff = cast(date, last)
    visible = input_snapshot(frame, cutoff=cutoff)
    if not visible.is_empty() or "_baseline" not in frame.columns or not include_historical_baseline or strict:
        return visible
    baseline = frame.filter(pl.col("_baseline").fill_null(False))
    if baseline.is_empty() or "_commit_seq" not in baseline.columns:
        return visible
    baseline = baseline.filter(pl.col("_commit_seq") == baseline["_commit_seq"].max())
    return baseline.filter(pl.col("snapshot_date") == baseline["snapshot_date"].max())


def _same_ordered_rows(left: pl.DataFrame, right: pl.DataFrame, fields: Sequence[str]) -> bool:
    """Prove exact ordered equality without allocating a hash-join working set."""
    lhs, rhs = left.select(fields), right.select(fields)
    return lhs.schema == rhs.schema and lhs.equals(rhs, null_equal=True)


def iter_computed_versions(
    *, raw: Mapping[str, pl.DataFrame], items: Mapping[str, pl.DataFrame],
    start: date, end: date, evaluate: Callable[[dict, dict], pl.DataFrame],
    raw_resolvers: Mapping[str, Callable[[date], pl.LazyFrame]] | None = None,
    extra_boundaries: Sequence[date] = (),
    evaluate_at: Callable[[dict, dict, date], pl.DataFrame] | None = None,
    config: ExecutionOptions | None = None,
    cancelled: Callable[[], bool] | None = None,
    executor: ThreadPoolExecutor | None = None,
) :
    """Yield causal revisions with one caller-bounded evaluation worker pool.

    Results are reconciled and yielded in cutoff order irrespective of worker
    completion order. Callers can publish each yielded batch before cancellation.
    """
    if end < start:
        raise ValueError("end precedes start")
    options = config or ExecutionOptions()
    boundaries: tuple[date | None, ...] = (None, *sorted(
        set(revision_dates([*raw.values(), *items.values()], start, end))
        | {day for day in extra_boundaries if start < day <= end}
    ))
    previous: pl.DataFrame | None = None
    admission = min(options.max_in_flight or options.workers, options.workers,
                    options.batch_size or options.workers, len(boundaries))
    output_limit = options.max_buffer_bytes // (admission + 1)

    def calculate(index: int) -> tuple[date | None, pl.DataFrame]:
        if cancelled and cancelled():
            raise CancelledError("DataItem build cancelled")
        boundary = boundaries[index]
        next_boundary = boundaries[index + 1] if index + 1 < len(boundaries) else None
        cutoff = next_boundary - timedelta(days=1) if next_boundary is not None else end
        raw_frames = {key: raw_resolvers[key](cutoff) if raw_resolvers and key in raw_resolvers else input_snapshot(value, cutoff).lazy() for key, value in raw.items()}
        item_frames = {key: input_snapshot(value, cutoff).lazy() for key, value in items.items()}
        frame = evaluate_at(raw_frames, item_frames, cutoff) if evaluate_at else evaluate(raw_frames, item_frames)
        if frame.select("time", "asset_id").is_duplicated().any():
            raise ValueError("DataItem producer output has duplicate time-asset keys")
        if frame.estimated_size() > output_limit:
            raise MemoryError("Producer output exceeds max_buffer_bytes; return smaller partitions")
        frame = frame.with_columns(pl.max_horizontal(
            pl.col("time"), pl.col("available_date") if "available_date" in frame.columns else pl.col("time"),
            pl.lit(boundary) if boundary is not None else pl.col("time")).alias(VERSION_DATE))
        return boundary, frame

    def reconcile(boundary: date | None, frame: pl.DataFrame) -> pl.DataFrame:
        nonlocal previous
        changed = frame
        if previous is not None:
            identity = [name for name in ("time", "asset_id", "value", "observation_date", "available_date", "_build_baseline") if name in previous.columns]
            if _same_ordered_rows(frame, previous, identity):
                changed = frame.head(0)
            else:
                changed = frame.join(previous.select(identity), on=identity, how="anti", nulls_equal=True)
                removed = previous.join(frame.select("time", "asset_id"), on=["time", "asset_id"], how="anti")
                if removed.height:
                    removed = removed.with_columns(pl.lit(None, dtype=previous.schema["value"]).alias("value"),
                                                   pl.max_horizontal(pl.col("time"), pl.lit(boundary)).alias(VERSION_DATE))
                    changed = pl.concat([changed, removed], how="diagonal_relaxed")
        previous = frame
        return changed.filter(pl.col("time").is_between(start, end) & (pl.col(VERSION_DATE) <= end))

    if options.workers == 1:
        for index in range(len(boundaries)):
            yield reconcile(*calculate(index))
        return
    # Builds share this pool with partition preparation; yielding a revision
    # must not admit a second, independently sized writer pool.
    with (nullcontext(executor) if executor is not None else
          ThreadPoolExecutor(max_workers=options.workers, thread_name_prefix="data-item")) as pool:
        pending = []
        try:
            for first in range(0, len(boundaries), admission):
                if cancelled and cancelled():
                    raise CancelledError("DataItem build cancelled")
                pending = [pool.submit(calculate, index) for index in range(first, min(first + admission, len(boundaries)))]
                buffered = 0
                for future in pending:
                    boundary, frame = future.result()
                    buffered += frame.estimated_size()
                    if buffered > options.max_buffer_bytes:
                        raise MemoryError("Admitted producer outputs exceed max_buffer_bytes; reduce max_in_flight")
                    yield reconcile(boundary, frame)
        finally:
            for future in pending:
                future.cancel()


def compute_versions(*, raw: Mapping[str, pl.DataFrame], items: Mapping[str, pl.DataFrame], start: date, end: date,
                     evaluate: Callable[[dict, dict], pl.DataFrame], raw_resolvers=None,
                     extra_boundaries=(), config: ExecutionOptions | None = None) -> pl.DataFrame:
    """Materialize causal versions; numerical evaluation remains caller-owned."""
    outputs = []
    buffered = 0
    from dataclasses import replace
    options = config or ExecutionOptions()
    iterator_options = replace(options, max_buffer_bytes=max(1, options.max_buffer_bytes // 2))
    for frame in iter_computed_versions(raw=raw, items=items, start=start, end=end, evaluate=evaluate,
                                       raw_resolvers=raw_resolvers, extra_boundaries=extra_boundaries, config=iterator_options):
        buffered += frame.estimated_size()
        if buffered > options.max_buffer_bytes // 2:
            raise MemoryError("Materialized version outputs exceed max_buffer_bytes; consume iter_computed_versions")
        outputs.append(frame)
    return (pl.concat(outputs, how="diagonal_relaxed")
            .unique(["time", "asset_id", VERSION_DATE], keep="last", maintain_order=True)
            .sort(VERSION_DATE, "time", "asset_id"))

__all__ = ['revision_dates', 'select_versions', 'input_snapshot', 'general_input_snapshot', 'compute_versions', 'iter_computed_versions']
