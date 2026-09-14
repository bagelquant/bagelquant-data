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


ALL_NULL_PAYLOAD_ERROR = "response payload is entirely null"


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
    recheck_after: str | None = None
    overlaps_existing: bool = False
    previous_data_max_time: str | None = None
    daily_scopes: tuple[DailyScope, ...] = ()
    scope_ordinal: int | None = None
    variant_hash: str | None = None
    range_backfill_eligible: bool = False
    request_date_field: str | None = None


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
        wait_for_request = getattr(source_adapter, "wait_for_request", None)
        if callable(wait_for_request) and not wait_for_request(discovery.api):
            raise DataSourceError("request discovery canceled before admission")
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
    source_options: Mapping[str, object] | None = None,
) -> tuple[LedgerRequest, ...]:
    """Synchronize declared scopes and return only currently eligible work."""

    current_day = datetime.now(UTC).date()
    final_day = _date_value(end or today or current_day)
    execution_day = _date_value(today or current_day)
    allow_all_null_payload = _boolean_source_option(
        source_options,
        "allow_all_null_payload",
    )
    variants = _parameter_variants(spec, raw, _base_variants(spec, params, discovered_param_sets), ids)
    if spec.update_type == "general":
        requests = _general_requests(spec, metadata, final_day, variants)
        assert requests is not None
        return requests
    spec_hash = metadata.dataset_spec_hash(spec.source, spec.name)
    if spec.update_type == "by_daily":
        return _daily_requests(
            spec, raw=raw, metadata=metadata, variants=variants, start=start,
            final_day=final_day, execution_day=execution_day, spec_hash=spec_hash,
            allow_all_null_payload=allow_all_null_payload,
            refresh=bool(source_options and source_options.get("refresh")),
        )
    raise ConfigurationError(f"Unsupported update_type: {spec.update_type}")


def _parameter_variants(spec, raw, variants, ids=None):
    if spec.parameter_dataset:
        source, separator, dataset = spec.parameter_dataset.partition("/")
        if not separator:
            source, dataset = spec.source, source
        catalog = raw.query_general(dataset, source=source).collect()
        values = sorted(
            set(catalog[spec.parameter_field].drop_nulls().cast(pl.String).to_list())
        )
        if ids is not None:
            values = [value for value in values if value in set(ids)]
        variants = [
            (
                hashlib.blake2b(
                    json.dumps(
                        {**request, spec.parameter_name: value},
                        sort_keys=True,
                        default=str,
                    ).encode(),
                    digest_size=16,
                ).hexdigest(),
                {**request, spec.parameter_name: value},
            )
            for _, request in variants
            for value in values
        ]
        if not values:
            raise ConfigurationError("Parameter dataset has no registered assets")
    return variants


def _general_requests(spec, metadata, final_day, variants):
    if spec.update_type == "general":
        target = final_day.isoformat()
        spec_hash = metadata.dataset_spec_hash(spec.source, spec.name)
        metadata.synchronize_update_scopes(
            {
                "source": spec.source,
                "dataset": spec.name,
                "scope_kind": "general_snapshot",
                "scope_key": target,
                "variant_hash": variant_hash,
                "initial_start": None,
                "spec_hash": spec_hash,
            }
            for variant_hash, _ in variants
        )
        metadata.remove_obsolete_update_scopes(
            source=spec.source, dataset=spec.name, spec_hash=spec_hash
        )
        params_by_variant = dict(variants)
        rows = metadata.update_scopes_with_checks(
            source=spec.source,
            dataset=spec.name,
            scope_kind="general_snapshot",
        )
        current = [
            row
            for row in rows
            if str(row["scope_key"]) == target
            and str(row["variant_hash"]) in params_by_variant
        ]
        metadata.reset_update_scopes(
            [
                int(row["id"])
                for row in current
                if row["status"] in {"success", "empty"}
            ],
            clear_watermark=True,
        )
        rows = metadata.update_scopes_with_checks(
            source=spec.source, dataset=spec.name, scope_kind="general_snapshot"
        )
        return tuple(
            LedgerRequest(
                dict(params_by_variant[str(row["variant_hash"])]),
                scope_id=int(row["id"]),
                request_kind=(
                    "retry" if row["status"] in {"failed", "invalid"} else "forward"
                ),
                target_end=target,
                variant_hash=str(row["variant_hash"]),
            )
            for row in rows
            if str(row["scope_key"]) == target
            and str(row["variant_hash"]) in params_by_variant
            and row["status"] in {"pending", "failed", "invalid"}
        )



def inspect_coverage(spec, raw, metadata, *, start, end):
    """Read declared daily coverage without synchronizing scopes or calling a provider."""
    first, last = _date_value(start), _date_value(end)
    current_hash = metadata.dataset_spec_hash(spec.source, spec.name)
    scopes = [row for row in metadata.update_scopes(source=spec.source, dataset=spec.name)
              if row["spec_hash"] == current_hash]
    if spec.update_type == "general":
        commits = raw._commits(spec.source, spec.name, None, None)
        manifests = metadata.manifest(spec.source, spec.name)
        complete = bool(commits and commits[-1]["spec_hash"] == current_hash and manifests) and all(
            (raw.parquet.paths.dataset_root(spec.source, spec.name) / m["partition_path"]).is_file()
            for m in manifests
        )
        return {"complete": complete, "missing": [] if complete else ["complete_snapshot"],
                "missing_parameters": {}, "coverage_through": str(last) if complete else None}
    dates = ([value for value in _calendar_dates(spec, raw) if first <= value <= last]
             if spec.date_kind == "trading" else
             [first + timedelta(days=i) for i in range((last-first).days+1)])
    variants = _parameter_variants(spec, raw, _base_variants(spec, None, ()))
    expected = {identity for identity, _ in variants}
    if spec.request_discovery is not None:
        expected = {row["variant_hash"] for row in scopes} or {"undiscovered_parameters"}
    complete = {(row["scope_key"], row["variant_hash"]) for row in scopes
                if row["status"] in {"success", "empty"}}
    missing = {str(day): sorted(identity for identity in expected if (str(day), identity) not in complete)
               for day in dates}
    missing = {day: values for day, values in missing.items() if values}
    coverage = last
    if missing:
        first_missing = _date_value(min(missing))
        coverage = max((day for day in dates if day < first_missing), default=None)
    return {"complete": not missing, "missing": sorted(missing), "missing_parameters": missing,
            "coverage_through": str(coverage) if coverage is not None else None}


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
            eligible_by_variant.setdefault(request.variant_hash or "", []).append(
                request
            )

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
    allow_all_null_payload: bool,
    refresh: bool = False,
) -> tuple[LedgerRequest, ...]:
    lower = _date_value(start) if start is not None else None
    dates = [
        value
        for value in (
            _calendar_dates(spec, raw)
            if spec.date_kind == "trading"
            else [
                final_day - timedelta(days=i)
                for i in range((final_day - (lower or final_day)).days, -1, -1)
            ]
        )
        if value <= final_day and (lower is None or value >= lower)
    ]
    selected_dates = set(dates)
    ordinals = {value: index for index, value in enumerate(dates)}
    recent_dates = {
        d for d in dates if d > final_day - timedelta(days=spec.recent_recheck_days)
    }
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
        retry_all_null_payload = bool(
            allow_all_null_payload
            and status == "invalid"
            and row.get("last_error") == ALL_NULL_PAYLOAD_ERROR
        )
        eligible = (
            status in {"pending", "failed", "invalid"}
            or refresh
            or retry_all_null_payload
            or (status == "empty" and scope_day in recent_dates)
            or (status == "success" and (check_due or scope_day in recent_dates))
        )
        if not eligible:
            continue
        request = dict(variant_params[str(row["variant_hash"])])
        date_param = str(request.pop("__date_param", spec.date_param or "date"))
        request[date_param] = scope_day.isoformat()
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
                    if status in {"failed", "invalid"}
                    else "empty_recheck"
                    if status == "empty"
                    else "historical_recheck"
                    if has_check
                    else "forward"
                ),
                target_end=scope_day.isoformat(),
                request_date_field=(spec.request_date_field or date_param),
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
    if spec.date_params:
        result = [
            (
                hashlib.blake2b(
                    (identity + field).encode(), digest_size=16
                ).hexdigest(),
                {**request, "__date_param": field},
            )
            for identity, request in result
            for field in spec.date_params
        ]
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


def _boolean_source_option(
    source_options: Mapping[str, object] | None,
    name: str,
) -> bool:
    if source_options is None or name not in source_options:
        return False
    value = source_options[name]
    if not isinstance(value, bool):
        raise ConfigurationError(f"source_options.{name} must be boolean")
    return value


def _nonnegative_source_option(
    source_options: Mapping[str, object] | None,
    name: str,
) -> int:
    if source_options is None or name not in source_options:
        return 0
    value = source_options[name]
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ConfigurationError(
            f"source_options.{name} must be a non-negative integer"
        )
    return value


def _nonempty_option(policy: Mapping[str, object], name: str, default: str) -> str:
    value = policy.get(name, default)
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(f"daily_range_backfill {name} cannot be empty")
    return value.strip()


def _interrupted_backfill_error(value: object) -> bool:
    text = "" if value is None else str(value).lower()
    return "cancel" in text or "lease expired" in text or "forced termination" in text
