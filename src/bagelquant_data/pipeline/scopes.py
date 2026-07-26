"""Authoritative update-scope synchronization and request selection."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from itertools import product

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.exceptions import ConfigurationError, DataSourceError
from bagelquant_data.core.types import DateLike
from bagelquant_data.query.raw import RawQueryService
from bagelquant_data.storage.metadata import MetadataStore


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
    except Exception as error:  # noqa: BLE001
        raise DataSourceError(
            f"request discovery failed for {spec.source}/{spec.name} "
            f"via {discovery.api}: {error}"
        ) from error
    if not isinstance(frame, pl.DataFrame):
        raise DataSourceError("request discovery source adapter must return a Polars DataFrame")
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
    discovered_param_sets: Sequence[dict[str, object]] = (),
) -> tuple[LedgerRequest, ...]:
    """Synchronize declared scopes and return only currently eligible work."""

    final_day = _date_value(end or today or date.today())
    execution_day = _date_value(today or date.today())
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
    rows = metadata.update_scopes(
        source=spec.source, dataset=spec.name, scope_kind="date"
    )
    checks = {
        int(row["scope_id"]): row
        for row in metadata.provider_scope_checks(
            source=spec.source, dataset=spec.name
        )
    }
    selected = []
    for row in rows:
        if str(row["variant_hash"]) not in variant_params:
            continue
        scope_day = _date_value(row["scope_key"])
        if scope_day not in dates:
            continue
        check = checks.get(int(row["id"]))
        check_due = (
            check is not None
            and check["recheck_after"] is not None
            and _date_value(check["recheck_after"]) <= execution_day
        )
        status = str(row["status"])
        eligible = status in {"pending", "failed"} or (
            status in {"success", "empty"} and check_due
        )
        if not eligible:
            continue
        request = dict(variant_params[str(row["variant_hash"])])
        request[spec.date_param or "date"] = scope_day.isoformat()
        selected.append(
            LedgerRequest(
                request,
                scope_id=int(row["id"]),
                request_kind="historical_recheck"
                if check is not None
                else "forward",
                target_end=scope_day.isoformat(),
                recheck_after=(scope_day + timedelta(days=1)).isoformat()
                if scope_day >= execution_day
                else None,
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
    rows = metadata.update_scopes(
        source=spec.source, dataset=spec.name, scope_kind="asset"
    )
    checks = {
        int(row["scope_id"]): row
        for row in metadata.provider_scope_checks(
            source=spec.source, dataset=spec.name
        )
    }
    requests = []
    for row in rows:
        if str(row["variant_hash"]) not in variant_params:
            continue
        asset_id = str(row["scope_key"])
        if asset_id not in bounds or row["status"] not in {
            "pending",
            "failed",
            "success",
            "empty",
        }:
            continue
        initial_start, target_end = bounds[asset_id]
        check = checks.get(int(row["id"]))
        checked = _optional_date(None if check is None else check["checked_through"])
        forward_start = (
            checked + timedelta(days=1) if checked is not None else initial_start
        )
        last_revision = _optional_datetime(
            None if check is None else check["last_checked_at"]
        )
        recheck_due = bool(
            check is not None
            and check["recheck_after"] is not None
            and _date_value(check["recheck_after"]) <= execution_day
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
                request_kind="revision" if revision_due else "forward",
                target_end=target_end.isoformat(),
                revision_check=revision_due,
                recheck_after=(
                    execution_day + timedelta(days=spec.revision_refresh_days)
                ).isoformat(),
                overlaps_existing=(
                    row["data_max_time"] is not None
                    and request_start <= _date_value(row["data_max_time"])
                ),
            )
        )
    return tuple(requests)


def _base_variants(
    spec: DatasetSpec,
    params: dict[str, object] | None,
    discovered_param_sets: Sequence[dict[str, object]],
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
    return datetime.strptime(
        text, "%Y%m%d" if len(text) == 8 and text.isdigit() else "%Y-%m-%d"
    ).date()


def _optional_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)
