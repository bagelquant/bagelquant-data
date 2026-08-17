"""Authoritative update-scope synchronization and request selection."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from itertools import product

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.exceptions import ConfigurationError, DataSourceError
from bagelquant_data.core.types import DateLike
from bagelquant_data.query.raw import RawQueryService
from bagelquant_data.storage.metadata import MetadataStore


DAILY_EMPTY_RECHECK_SESSIONS = 20


@dataclass(frozen=True, slots=True)
class DailyScope:
    """One daily ledger outcome covered by a physical provider request."""

    scope_id: int
    scope_key: str
    recheck_after: str | None = None


@dataclass(frozen=True, slots=True)
class LedgerRequest:
    """One claimed ledger scope and the provider request that checks it."""

    params: dict[str, object]
    scope_id: int | None = None
    request_kind: str = "refresh"
    target_end: str | None = None
    revision_check: bool = False
    recheck_after: str | None = None
    overlaps_existing: bool = False
    previous_data_max_time: str | None = None
    daily_scopes: tuple[DailyScope, ...] = ()
    scope_ordinal: int | None = None
    variant_hash: str | None = None
    range_backfill_eligible: bool = False


@dataclass(frozen=True, slots=True)
class DiscoveryCall:
    """One successful provider discovery request retained for API provenance."""

    api: str
    params: dict[str, object]
    row_count: int


def discover_request_param_sets(
    spec: DatasetSpec, source_adapter: object
) -> tuple[tuple[dict[str, object], ...], DiscoveryCall | None]:
    """Fetch and validate dynamic parameter values declared by a dataset."""

    discovery = spec.request_discovery
    if discovery is None:
        return (), None
    try:
        frame = source_adapter.fetch(discovery.api, dict(discovery.params))  # type: ignore[attr-defined]
    except Exception as error:
        raise DataSourceError(
            f"request discovery failed for {spec.source}/{spec.name} "
            f"via {discovery.api}: {error}"
        ) from error
    if not isinstance(frame, pl.DataFrame):
        raise DataSourceError(
            "request discovery source adapter must return a Polars DataFrame"
        )
    if discovery.result_field not in frame.columns:
        raise DataSourceError(
            f"request discovery response for {spec.source}/{spec.name} is missing "
            f"{discovery.result_field!r}"
        )
    values = sorted(
        {
            str(value).strip()
            for value in frame.get_column(discovery.result_field).drop_nulls().to_list()
            if str(value).strip()
        }
    )
    if not values:
        raise DataSourceError(
            f"request discovery returned no usable {discovery.result_field!r} values "
            f"for {spec.source}/{spec.name}"
        )
    return (
        tuple({discovery.target_param: value} for value in values),
        DiscoveryCall(discovery.api, dict(discovery.params), frame.height),
    )


def synchronize_requests(
    *,
    spec: DatasetSpec,
    raw: RawQueryService,
    metadata: MetadataStore,
    start: DateLike | None,
    end: DateLike | None,
    today: DateLike | None = None,
    ids: Sequence[str] | None = None,
    params: dict[str, object] | None = None,
    discovered_param_sets: Sequence[Mapping[str, object]] = (),
) -> tuple[LedgerRequest, ...]:
    """Synchronize declared scopes and return only currently eligible work."""

    current_day = datetime.now(UTC).date()
    final_day = _date_value(end or today or current_day)
    execution_day = _date_value(today or current_day)
    variants = _base_variants(spec, params, discovered_param_sets)
    if spec.update_type == "general":
        requests = []
        for _, request in variants:
            if start is not None:
                request["start"] = _date_value(start).isoformat()
            if end is not None:
                request["end"] = final_day.isoformat()
            requests.append(LedgerRequest(request))
        return tuple(requests)

    spec_hash = metadata.dataset_spec_hash(spec.source, spec.name)
    if spec.update_type == "by_daily":
        return _daily_requests(
            spec,
            raw=raw,
            metadata=metadata,
            variants=variants,
            start=start,
            final_day=final_day,
            execution_day=execution_day,
            spec_hash=spec_hash,
        )
    if spec.update_type == "by_asset":
        return _asset_requests(
            spec,
            raw=raw,
            metadata=metadata,
            variants=variants,
            ids=ids,
            start=start,
            final_day=final_day,
            execution_day=execution_day,
            spec_hash=spec_hash,
        )
    raise ConfigurationError(
        f"{spec.source}/{spec.name} unsupported update_type: {spec.update_type}"
    )


def compact_daily_range_backfill(
    spec: DatasetSpec,
    requests: Sequence[LedgerRequest],
    source_options: Mapping[str, object] | None,
) -> tuple[LedgerRequest, ...]:
    """Compact untouched daily backlog into bounded physical range requests."""

    if spec.update_type != "by_daily" or not source_options:
        return tuple(requests)
    raw_policy = source_options.get("daily_range_backfill")
    if raw_policy is None:
        return tuple(requests)
    if not isinstance(raw_policy, Mapping):
        raise ConfigurationError("daily_range_backfill must be a mapping")
    start_param = _nonempty_option(raw_policy, "start_param", "start")
    end_param = _nonempty_option(raw_policy, "end_param", "end")
    if start_param == end_param:
        raise ConfigurationError(
            "daily_range_backfill start_param and end_param must differ"
        )
    max_scopes = _positive_option(raw_policy, "max_scopes", 1024)
    _positive_option(raw_policy, "row_limit")
    _positive_option(raw_policy, "max_pages", 10_000)

    result = [request for request in requests if not request.range_backfill_eligible]
    eligible_by_variant: dict[str, list[LedgerRequest]] = {}
    for request in requests:
        if request.range_backfill_eligible:
            eligible_by_variant.setdefault(request.variant_hash or "", []).append(request)

    def append_groups(pending: list[LedgerRequest]) -> None:
        cursor = 0
        while cursor < len(pending):
            group = [pending[cursor]]
            cursor += 1
            while (
                cursor < len(pending)
                and len(group) < max_scopes
                and group[-1].scope_ordinal is not None
                and pending[cursor].scope_ordinal == group[-1].scope_ordinal + 1
            ):
                group.append(pending[cursor])
                cursor += 1
            if len(group) == 1:
                result.append(group[0])
                continue
            first = group[0]
            last = group[-1]
            params = dict(first.params)
            params.pop(spec.date_param or "date", None)
            params[start_param] = first.daily_scopes[0].scope_key
            params[end_param] = last.daily_scopes[-1].scope_key
            result.append(
                LedgerRequest(
                    params=params,
                    request_kind="initial_range_backfill",
                    target_end=last.daily_scopes[-1].scope_key,
                    daily_scopes=tuple(
                        scope for request in group for scope in request.daily_scopes
                    ),
                    scope_ordinal=first.scope_ordinal,
                    variant_hash=first.variant_hash,
                    range_backfill_eligible=True,
                )
            )

    for pending in eligible_by_variant.values():
        append_groups(pending)
    return tuple(result)


def _daily_requests(
    spec: DatasetSpec,
    *,
    raw: RawQueryService,
    metadata: MetadataStore,
    variants: list[tuple[str, dict[str, object]]],
    start: DateLike | None,
    final_day: date,
    execution_day: date,
    spec_hash: str,
) -> tuple[LedgerRequest, ...]:
    lower = _date_value(start) if start is not None else None
    dates = [
        value
        for value in _calendar_dates(spec, raw)
        if value <= final_day and (lower is None or value >= lower)
    ]
    selected_dates = set(dates)
    ordinals = {value: index for index, value in enumerate(dates)}
    recent_dates = set(dates[-DAILY_EMPTY_RECHECK_SESSIONS:])
    metadata.synchronize_update_scopes(
        {
            "source": spec.source,
            "dataset": spec.name,
            "scope_kind": "date",
            "scope_key": value.isoformat(),
            "variant_hash": variant_hash,
            "initial_start": value.isoformat(),
            "spec_hash": spec_hash,
        }
        for value in dates
        for variant_hash, _ in variants
    )
    metadata.remove_obsolete_update_scopes(
        source=spec.source, dataset=spec.name, spec_hash=spec_hash
    )
    variant_params = dict(variants)
    rows = metadata.update_scopes_with_checks(
        source=spec.source, dataset=spec.name, scope_kind="date"
    )
    selected = []
    for row in rows:
        if str(row["variant_hash"]) not in variant_params:
            continue
        scope_day = _date_value(row["scope_key"])
        if scope_day not in selected_dates:
            continue
        has_check = row["provider_checked_through"] is not None
        check_due = (
            has_check
            and row["provider_recheck_after"] is not None
            and _date_value(row["provider_recheck_after"]) <= execution_day
        )
        status = str(row["status"])
        eligible = (
            status in {"pending", "failed"}
            or (status == "empty" and scope_day in recent_dates)
            or (status == "success" and check_due)
        )
        if not eligible:
            continue
        request = dict(variant_params[str(row["variant_hash"])])
        request[spec.date_param or "date"] = scope_day.isoformat()
        interrupted_backfill = bool(
            status == "failed"
            and row["provider_checked_through"] is None
            and row["data_max_time"] is None
            and _interrupted_backfill_error(row.get("last_error"))
        )
        daily_scope = DailyScope(
            scope_id=int(row["id"]),
            scope_key=scope_day.isoformat(),
            recheck_after=(scope_day + timedelta(days=1)).isoformat()
            if scope_day >= execution_day
            else None,
        )
        selected.append(
            LedgerRequest(
                request,
                scope_id=int(row["id"]),
                request_kind=(
                    "retry"
                    if status == "failed"
                    else "empty_recheck"
                    if status == "empty"
                    else "historical_recheck"
                    if has_check
                    else "forward"
                ),
                target_end=scope_day.isoformat(),
                recheck_after=daily_scope.recheck_after,
                daily_scopes=(daily_scope,),
                scope_ordinal=ordinals[scope_day],
                variant_hash=str(row["variant_hash"]),
                range_backfill_eligible=(
                    (
                        status == "pending"
                        and int(row["attempt_count"]) == 0
                        and scope_day < execution_day
                    )
                    or interrupted_backfill
                ),
            )
        )
    return tuple(selected)


def _asset_requests(
    spec: DatasetSpec,
    *,
    raw: RawQueryService,
    metadata: MetadataStore,
    variants: list[tuple[str, dict[str, object]]],
    ids: Sequence[str] | None,
    start: DateLike | None,
    final_day: date,
    execution_day: date,
    spec_hash: str,
) -> tuple[LedgerRequest, ...]:
    requested_start = _date_value(start) if start is not None else None
    assets = _asset_bounds(spec, raw, ids)
    bounds: dict[str, tuple[date | None, date]] = {}
    scopes = []
    for asset_id, list_date, delist_date in assets:
        initial_start = (
            max(value for value in (requested_start, list_date) if value is not None)
            if requested_start is not None or list_date is not None
            else None
        )
        target_end = min(final_day, delist_date) if delist_date else final_day
        if initial_start is not None and initial_start > target_end:
            continue
        bounds[asset_id] = (initial_start, target_end)
        for variant_hash, _ in variants:
            scopes.append(
                {
                    "source": spec.source,
                    "dataset": spec.name,
                    "scope_kind": "asset",
                    "scope_key": asset_id,
                    "variant_hash": variant_hash,
                    "initial_start": None
                    if initial_start is None
                    else initial_start.isoformat(),
                    "spec_hash": spec_hash,
                }
            )
    metadata.synchronize_update_scopes(scopes)
    metadata.remove_obsolete_update_scopes(
        source=spec.source, dataset=spec.name, spec_hash=spec_hash
    )
    variant_params = dict(variants)
    rows = metadata.update_scopes_with_checks(
        source=spec.source, dataset=spec.name, scope_kind="asset"
    )
    requests = []
    for row in rows:
        if str(row["variant_hash"]) not in variant_params:
            continue
        asset_id = str(row["scope_key"])
        if asset_id not in bounds or row["status"] not in {
            "pending",
            "failed",
            "success",
        }:
            continue
        initial_start, target_end = bounds[asset_id]
        has_check = row["provider_checked_through"] is not None
        checked = _optional_date(row["provider_checked_through"])
        forward_start = (
            checked + timedelta(days=1) if checked is not None else initial_start
        )
        last_revision = _optional_datetime(row["provider_last_checked_at"])
        recheck_due = bool(
            has_check
            and row["provider_recheck_after"] is not None
            and _date_value(row["provider_recheck_after"]) <= execution_day
        )
        revision_due = recheck_due or (
            last_revision is None
            or (datetime.now(UTC) - last_revision).days >= spec.revision_refresh_days
        )
        forward_due = checked is None or checked < target_end
        status = str(row["status"])
        eligible = status in {"pending", "failed"} or forward_due or revision_due
        if not eligible:
            continue
        request_start = forward_start
        if revision_due:
            revision_start = target_end - timedelta(
                days=spec.revision_lookback_days - 1
            )
            if initial_start is not None:
                revision_start = max(revision_start, initial_start)
            request_start = (
                revision_start
                if request_start is None
                else min(request_start, revision_start)
            )
        if request_start is None:
            raise ConfigurationError(
                f"{spec.source}/{spec.name} needs an update start for {asset_id}"
            )
        request = dict(variant_params[str(row["variant_hash"])])
        request.update(
            id=asset_id,
            start=request_start.isoformat(),
            end=target_end.isoformat(),
        )
        requests.append(
            LedgerRequest(
                request,
                scope_id=int(row["id"]),
                request_kind=(
                    "retry"
                    if status == "failed"
                    else "revision"
                    if revision_due
                    else "forward"
                ),
                target_end=target_end.isoformat(),
                revision_check=revision_due,
                recheck_after=(
                    execution_day + timedelta(days=spec.revision_refresh_days)
                ).isoformat(),
                overlaps_existing=(
                    row["data_max_time"] is not None
                    and request_start <= _date_value(row["data_max_time"])
                ),
                previous_data_max_time=(
                    None
                    if row["data_max_time"] is None
                    else str(row["data_max_time"])
                ),
            )
        )
    return tuple(requests)


def _base_variants(
    spec: DatasetSpec,
    params: dict[str, object] | None,
    discovered_param_sets: Sequence[Mapping[str, object]],
) -> list[tuple[str, dict[str, object]]]:
    defaults = dict(spec.source_api_params)
    overrides = dict(params or {})
    parameter_sets = spec.source_api_param_sets or ({},)
    result = []
    discovered = discovered_param_sets or ({},)
    for parameter_set in parameter_sets:
        keys = tuple(parameter_set)
        values = [
            value if isinstance(value, list) else [value]
            for value in parameter_set.values()
        ]
        for combination in product(*values):
            for dynamic in discovered:
                request = dict(defaults)
                request.update(dict(zip(keys, combination, strict=True)))
                request.update(dynamic)
                request.update(overrides)
                identity = json.dumps(
                    request, sort_keys=True, separators=(",", ":"), default=str
                )
                result.append(
                    (
                        hashlib.blake2b(identity.encode(), digest_size=16).hexdigest(),
                        request,
                    )
                )
    return result


def _calendar_dates(spec: DatasetSpec, raw: RawQueryService) -> list[date]:
    if not spec.calendar:
        raise ConfigurationError(f"{spec.source}/{spec.name} requires calendar")
    frame = raw.query_general(spec.calendar, source=spec.source).collect()
    if frame.is_empty() or "time" not in frame.columns:
        raise ConfigurationError(f"{spec.source}/{spec.calendar} has no calendar dates")
    if "is_open" in frame.columns:
        frame = frame.filter(pl.col("is_open").cast(pl.Int8, strict=False) == 1)
    return (
        frame.select(_date_expr("time").alias("value"))
        .drop_nulls()
        .unique()
        .sort("value")
        .get_column("value")
        .to_list()
    )


def _asset_bounds(
    spec: DatasetSpec, raw: RawQueryService, ids: Sequence[str] | None
) -> list[tuple[str, date | None, date | None]]:
    if not spec.asset_list:
        raise ConfigurationError(f"{spec.source}/{spec.name} requires asset_list")
    frame = raw.query_general(spec.asset_list, source=spec.source).collect()
    if frame.is_empty() or "asset_id" not in frame.columns:
        raise ConfigurationError(f"{spec.source}/{spec.asset_list} has no asset ids")
    selected = {str(value) for value in ids} if ids is not None else None
    result = []
    for row in frame.iter_rows(named=True):
        asset_id = str(row["asset_id"])
        if selected is not None and asset_id not in selected:
            continue
        result.append(
            (
                asset_id,
                _optional_date(row.get("list_date")),
                _optional_date(row.get("delist_date")),
            )
        )
    return sorted(set(result))


def _date_expr(field: str) -> pl.Expr:
    return (
        pl.when(pl.col(field).cast(pl.String).str.len_chars() == 8)
        .then(
            pl.col(field).cast(pl.String).str.strptime(pl.Date, "%Y%m%d", strict=False)
        )
        .otherwise(pl.col(field).cast(pl.Date, strict=False))
    )


def _optional_date(value: object) -> date | None:
    if value is None or str(value).strip() in {"", "None"}:
        return None
    return _date_value(value)


def _date_value(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).split("T", maxsplit=1)[0]
    if len(text) == 8 and text.isdigit():
        return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
    return date.fromisoformat(text)


def _optional_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _positive_option(
    policy: Mapping[str, object], name: str, default: int | None = None
) -> int:
    value = policy.get(name, default)
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ConfigurationError(f"daily_range_backfill {name} must be positive")
    return value


def _nonempty_option(
    policy: Mapping[str, object], name: str, default: str
) -> str:
    value = policy.get(name, default)
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(f"daily_range_backfill {name} cannot be empty")
    return value.strip()


def _interrupted_backfill_error(value: object) -> bool:
    text = "" if value is None else str(value).lower()
    return "cancel" in text or "lease expired" in text or "forced termination" in text
