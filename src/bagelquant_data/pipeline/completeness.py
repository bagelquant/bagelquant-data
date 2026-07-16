"""Completeness-aware, provider-free update planning."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from itertools import product
from typing import Literal, Mapping, Sequence

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.exceptions import ConfigurationError, DatasetNotFoundError
from bagelquant_data.core.types import DateLike
from bagelquant_data.pipeline.planner import plan_update
from bagelquant_data.query.raw import RawQueryService
from bagelquant_data.storage.metadata import MetadataStore

AuditMode = Literal["fast", "full"]


@dataclass(frozen=True, slots=True)
class CoverageYearSummary:
    year: int
    expected: int
    present: int
    verified_empty: int
    provisional: int
    missing: int


@dataclass(frozen=True, slots=True)
class CoverageSummary:
    dataset: str
    update_type: str
    expected: int
    present: int
    verified_empty: int
    provisional: int
    missing: int
    retry: int
    estimated_calls: int
    audit_status: str
    years: tuple[CoverageYearSummary, ...] = ()


@dataclass(frozen=True, slots=True)
class PlannedDataset:
    dataset: str
    update_type: str
    requests: tuple[dict[str, object], ...]
    pending_retries: tuple[dict[str, object], ...]
    summary: CoverageSummary


@dataclass(frozen=True, slots=True)
class UpdatePlan:
    source: str
    audit: AuditMode
    start: str
    end: str
    created_at: str
    state_fingerprint: str
    datasets: tuple[PlannedDataset, ...]

    @property
    def summaries(self) -> tuple[CoverageSummary, ...]:
        return tuple(value.summary for value in self.datasets)

    @property
    def estimated_calls(self) -> int:
        return sum(value.summary.estimated_calls for value in self.datasets)


def build_update_plan(
    *,
    specs: Sequence[DatasetSpec],
    raw: RawQueryService,
    metadata: MetadataStore,
    source: str,
    start: DateLike,
    end: DateLike | None,
    audit: AuditMode = "fast",
    ids: Sequence[str] | None = None,
    params: dict[str, object] | None = None,
    today: DateLike | None = None,
) -> UpdatePlan:
    """Build an immutable update plan without contacting the provider."""

    if audit not in {"fast", "full"}:
        raise ValueError("audit must be 'fast' or 'full'")
    first = _date_value(start)
    last = _date_value(end or today or date.today())
    if first > last:
        raise ValueError("update start must not follow end")
    planned: list[PlannedDataset] = []
    for spec in specs:
        if spec.source != source:
            raise ValueError(f"dataset source mismatch: {spec.source}/{spec.name}")
        planned.append(
            _plan_dataset(
                spec,
                raw=raw,
                metadata=metadata,
                start=first,
                end=last,
                audit=audit,
                ids=ids,
                params=params,
            )
        )
    return UpdatePlan(
        source=source,
        audit=audit,
        start=first.isoformat(),
        end=last.isoformat(),
        created_at=datetime.now(UTC).isoformat(),
        state_fingerprint=planning_state_fingerprint(metadata, source),
        datasets=tuple(planned),
    )


def planning_state_fingerprint(metadata: MetadataStore, source: str) -> str:
    """Hash lake state that can change a completeness plan."""

    payload = {
        "datasets": _planning_rows(
            metadata.list_datasets(source),
            ("source", "name", "enabled", "spec_hash"),
        ),
        "manifest": _planning_rows(
            metadata.manifest(source),
            (
                "source",
                "dataset",
                "partition_path",
                "partition_values",
                "row_count",
                "file_size_bytes",
                "min_time",
                "max_time",
                "content_hash",
                "schema_hash",
            ),
        ),
        "pending": _planning_rows(
            metadata.pending_update_jobs(source=source),
            (
                "job_key",
                "source",
                "dataset",
                "update_type",
                "request_params",
                "asset_id",
            ),
        ),
        "coverage": _planning_rows(
            metadata.coverage(source),
            (
                "source",
                "dataset",
                "scope_kind",
                "scope_key",
                "provisional",
                "row_count",
                "spec_hash",
            ),
        ),
        "watermarks": _planning_rows(
            metadata.audit_watermarks(source),
            ("source", "dataset"),
        ),
    }
    return hashlib.blake2b(
        json.dumps(payload, sort_keys=True, default=str).encode("utf-8"),
        digest_size=20,
    ).hexdigest()


def _planning_rows(
    rows: Sequence[Mapping[str, object]],
    fields: tuple[str, ...],
) -> list[dict[str, object]]:
    """Remove timestamps and other metadata that cannot change a plan."""

    return [{field: row.get(field) for field in fields} for row in rows]


def coverage_scopes(
    spec: DatasetSpec,
    request: dict[str, object],
    *,
    today: date | None = None,
) -> tuple[tuple[str, str, bool], ...]:
    """Return coverage records established by one successful logical request."""

    current = today or date.today()
    if spec.update_type == "by_daily":
        key = spec.date_param or "date"
        if key not in request:
            return ()
        value = _date_value(request[key])
        return (("daily", value.isoformat(), value >= current),)
    if spec.update_type != "by_asset" or "id" not in request:
        return ()
    first = _date_value(request.get("start", current))
    last = _date_value(request.get("end", first))
    asset = str(request["id"])
    return tuple(
        ("asset_year", f"{asset}|{year}", year >= current.year)
        for year in range(first.year, last.year + 1)
    )


def _plan_dataset(
    spec: DatasetSpec,
    *,
    raw: RawQueryService,
    metadata: MetadataStore,
    start: date,
    end: date,
    audit: AuditMode,
    ids: Sequence[str] | None,
    params: dict[str, object] | None,
) -> PlannedDataset:
    pending_rows = metadata.pending_update_jobs(
        source=spec.source, dataset=spec.name
    )
    pending_retries = tuple(
        dict(row["request_params"]) for row in pending_rows
    )
    retry = len(pending_retries)
    watermark = metadata.audit_watermarks(spec.source, spec.name)
    has_partial_state = bool(metadata.coverage(spec.source, spec.name) or pending_rows)
    audit_status = (
        "complete"
        if watermark
        else "incomplete"
        if has_partial_state
        else "never_audited"
    )
    if spec.update_type == "general":
        requests = plan_update(
            spec=spec, raw=raw, start=None, end=None, params=params
        ).requests
        summary = CoverageSummary(
            spec.name, spec.update_type, len(requests), 0, 0, 0,
            len(requests), retry, len(requests) + retry, audit_status,
        )
        return PlannedDataset(
            spec.name, spec.update_type, requests, pending_retries, summary
        )
    if spec.update_type == "by_daily":
        planned = _plan_daily(
            spec, raw=raw, metadata=metadata, start=start, end=end,
            audit=audit, params=params, retry=retry, audit_status=audit_status,
        )
        return PlannedDataset(
            planned.dataset,
            planned.update_type,
            planned.requests,
            pending_retries,
            planned.summary,
        )
    if spec.update_type == "by_asset":
        planned = _plan_assets(
            spec, raw=raw, metadata=metadata, start=start, end=end,
            audit=audit, ids=ids, params=params, retry=retry,
            audit_status=audit_status,
        )
        return PlannedDataset(
            planned.dataset,
            planned.update_type,
            planned.requests,
            pending_retries,
            planned.summary,
        )
    raise ConfigurationError(
        f"{spec.source}/{spec.name} unsupported update_type: {spec.update_type}"
    )


def _plan_daily(
    spec: DatasetSpec,
    *,
    raw: RawQueryService,
    metadata: MetadataStore,
    start: date,
    end: date,
    audit: AuditMode,
    params: dict[str, object] | None,
    retry: int,
    audit_status: str,
) -> PlannedDataset:
    expected_dates = [
        value for value in _calendar_dates(spec, raw) if start <= value <= end
    ]
    if audit == "fast":
        base = list(
            plan_update(
                spec=spec, raw=raw, start=start, end=end, params=params
            ).requests
        )
        verified = _coverage_keys(
            metadata, spec, "daily", provisional=False
        )
        key_name = spec.date_param or "date"
        base = [
            request
            for request in base
            if str(request.get(key_name)) not in verified
        ]
        provisional = _coverage_keys(
            metadata, spec, "daily", provisional=True
        )
        existing = {
            _date_value(request[spec.date_param or "date"])
            for request in base
            if (spec.date_param or "date") in request
        }
        for key in provisional:
            value = date.fromisoformat(key)
            if start <= value <= end and value not in existing:
                base.extend(_requests_for_date(spec, value, params))
        logical = {
            str(request.get(spec.date_param or "date")) for request in base
        }
        summary = CoverageSummary(
            spec.name, spec.update_type, len(logical), 0, 0,
            len(provisional), len(logical), retry, len(base) + retry,
            audit_status,
        )
        return PlannedDataset(
            spec.name, spec.update_type, tuple(base), (), summary
        )

    observed = _observed_dates(raw, spec, start, end)
    verified = _coverage_keys(metadata, spec, "daily", provisional=False)
    verified_empty = _coverage_keys(
        metadata, spec, "daily", provisional=False, empty_only=True
    )
    provisional = _coverage_keys(metadata, spec, "daily", provisional=True)
    expected_keys = {value.isoformat() for value in expected_dates}
    missing_keys = expected_keys - {value.isoformat() for value in observed} - verified
    missing_keys |= expected_keys & provisional
    requests = tuple(
        request
        for key in sorted(missing_keys)
        for request in _requests_for_date(spec, date.fromisoformat(key), params)
    )
    years = _daily_years(
        expected_dates, observed, verified_empty, provisional, missing_keys
    )
    summary = CoverageSummary(
        spec.name, spec.update_type, len(expected_keys), len(observed),
        len(expected_keys & verified_empty), len(expected_keys & provisional),
        len(missing_keys), retry, len(requests) + retry, audit_status, years,
    )
    return PlannedDataset(spec.name, spec.update_type, requests, (), summary)


def _plan_assets(
    spec: DatasetSpec,
    *,
    raw: RawQueryService,
    metadata: MetadataStore,
    start: date,
    end: date,
    audit: AuditMode,
    ids: Sequence[str] | None,
    params: dict[str, object] | None,
    retry: int,
    audit_status: str,
) -> PlannedDataset:
    assets = _assets(spec, raw, ids)
    if audit == "fast":
        requests = plan_update(
            spec=spec,
            raw=raw,
            start=start,
            end=end,
            ids=ids,
            params=params,
        ).requests
        provisional = _coverage_keys(
            metadata, spec, "asset_year", provisional=True
        )
        planned_scopes = {
            f"{request['id']}|{_date_value(request['start']).year}"
            for request in requests
            if "id" in request and "start" in request
        }
        extra: list[dict[str, object]] = []
        active = _expected_asset_years(
            assets, max(start, date(end.year, 1, 1)), end
        )
        for key in sorted(set(active) & provisional - planned_scopes):
            extra.extend(_requests_for_asset_year(spec, key, active[key], params))
        combined = (*requests, *extra)
        summary = CoverageSummary(
            spec.name,
            spec.update_type,
            len({str(row.get("id")) for row in combined}),
            0,
            0,
            len(provisional),
            len({str(row.get("id")) for row in combined}),
            retry,
            len(combined) + retry,
            audit_status,
        )
        return PlannedDataset(
            spec.name, spec.update_type, tuple(combined), (), summary
        )

    scopes = _expected_asset_years(assets, start, end)
    observed = _observed_asset_years(raw, spec, start, end)
    verified = _coverage_keys(metadata, spec, "asset_year", provisional=False)
    verified_empty = _coverage_keys(
        metadata, spec, "asset_year", provisional=False, empty_only=True
    )
    provisional = _coverage_keys(metadata, spec, "asset_year", provisional=True)
    expected_keys = set(scopes)
    missing_keys = expected_keys - observed - verified
    missing_keys |= expected_keys & provisional
    requests = tuple(
        request
        for key in sorted(missing_keys)
        for request in _requests_for_asset_year(
            spec, key, scopes[key], params
        )
    )
    years = _asset_years(
        expected_keys, observed, verified_empty, provisional, missing_keys
    )
    summary = CoverageSummary(
        spec.name, spec.update_type, len(expected_keys),
        len(expected_keys & observed), len(expected_keys & verified_empty),
        len(expected_keys & provisional), len(missing_keys), retry,
        len(requests) + retry, audit_status, years,
    )
    return PlannedDataset(spec.name, spec.update_type, requests, (), summary)


def _calendar_dates(spec: DatasetSpec, raw: RawQueryService) -> list[date]:
    if not spec.calendar:
        raise ConfigurationError(f"{spec.source}/{spec.name} requires calendar")
    frame = raw.query_general(spec.calendar, source=spec.source).collect()
    if frame.is_empty() or "time" not in frame.columns:
        raise ConfigurationError(f"{spec.source}/{spec.calendar} is empty or missing time")
    if "is_open" in frame.columns:
        frame = frame.filter(pl.col("is_open").cast(pl.Int8, strict=False) == 1)
    return (
        frame.select(_date_expr("time").alias("time"))
        .drop_nulls().unique().sort("time").get_column("time").to_list()
    )


def _observed_dates(
    raw: RawQueryService, spec: DatasetSpec, start: date, end: date
) -> set[date]:
    try:
        frame = raw.query(
            spec.name, source=spec.source, start=start, end=end, fields=("time",)
        ).collect()
    except DatasetNotFoundError:
        return set()
    return set(
        frame.select(pl.col("time").cast(pl.Date)).drop_nulls().unique()["time"].to_list()
    )


def _observed_asset_years(
    raw: RawQueryService, spec: DatasetSpec, start: date, end: date
) -> set[str]:
    try:
        frame = raw.query(
            spec.name, source=spec.source, start=start, end=end,
            fields=("time", "asset_id"),
        ).collect()
    except DatasetNotFoundError:
        return set()
    if frame.is_empty():
        return set()
    return {
        f"{row['asset_id']}|{row['year']}"
        for row in frame.select(
            pl.col("asset_id").cast(pl.String),
            pl.col("time").cast(pl.Date).dt.year().alias("year"),
        ).drop_nulls().unique().to_dicts()
    }


def _assets(
    spec: DatasetSpec, raw: RawQueryService, ids: Sequence[str] | None
) -> list[tuple[str, date | None, date | None]]:
    if ids is not None:
        return [(str(value), None, None) for value in ids]
    if not spec.asset_list:
        raise ConfigurationError(f"{spec.source}/{spec.name} requires asset_list")
    frame = raw.query_general(spec.asset_list, source=spec.source).collect()
    if frame.is_empty() or "asset_id" not in frame.columns:
        raise ConfigurationError(
            f"{spec.source}/{spec.asset_list} is empty or missing asset_id"
        )
    expressions = [pl.col("asset_id").cast(pl.String)]
    for column in ("list_date", "delist_date"):
        expressions.append(
            _date_expr(column).alias(column)
            if column in frame.columns
            else pl.lit(None, dtype=pl.Date).alias(column)
        )
    return [
        (str(row["asset_id"]), row["list_date"], row["delist_date"])
        for row in frame.select(*expressions).drop_nulls("asset_id")
        .unique("asset_id", keep="last").sort("asset_id").to_dicts()
    ]


def _expected_asset_years(
    assets: Sequence[tuple[str, date | None, date | None]],
    start: date,
    end: date,
) -> dict[str, tuple[date, date]]:
    result: dict[str, tuple[date, date]] = {}
    for asset, listed, delisted in assets:
        first = max(start, listed or start)
        last = min(end, delisted or end)
        if first > last:
            continue
        for year in range(first.year, last.year + 1):
            scope_start = max(first, date(year, 1, 1))
            scope_end = min(last, date(year, 12, 31))
            result[f"{asset}|{year}"] = (scope_start, scope_end)
    return result


def _coverage_keys(
    metadata: MetadataStore,
    spec: DatasetSpec,
    scope_kind: str,
    *,
    provisional: bool,
    empty_only: bool = False,
) -> set[str]:
    spec_hash = metadata.dataset_spec_hash(spec.source, spec.name)
    return {
        str(row["scope_key"])
        for row in metadata.coverage(spec.source, spec.name)
        if row["scope_kind"] == scope_kind
        and bool(row["provisional"]) is provisional
        and row["spec_hash"] == spec_hash
        and (not empty_only or int(row["row_count"]) == 0)
    }


def _base_requests(
    spec: DatasetSpec, params: dict[str, object] | None
) -> list[dict[str, object]]:
    defaults = dict(spec.source_api_params)
    overrides = dict(params or {})
    parameter_sets = spec.source_api_param_sets or ({},)
    requests: list[dict[str, object]] = []
    for parameter_set in parameter_sets:
        keys = tuple(parameter_set)
        values = [value if isinstance(value, list) else [value] for value in parameter_set.values()]
        for combination in product(*values):
            request = dict(defaults)
            request.update(dict(zip(keys, combination, strict=True)))
            request.update(overrides)
            requests.append(request)
    return requests


def _requests_for_date(
    spec: DatasetSpec, value: date, params: dict[str, object] | None
) -> list[dict[str, object]]:
    result = _base_requests(spec, params)
    for request in result:
        request[spec.date_param or "date"] = value.isoformat()
    return result


def _requests_for_asset_year(
    spec: DatasetSpec,
    key: str,
    bounds: tuple[date, date],
    params: dict[str, object] | None,
) -> list[dict[str, object]]:
    asset, _ = key.rsplit("|", 1)
    result = _base_requests(spec, params)
    for request in result:
        request["id"] = asset
        request["start"] = bounds[0].isoformat()
        request["end"] = bounds[1].isoformat()
    return result


def _daily_years(
    expected: Sequence[date], observed: set[date], verified: set[str],
    provisional: set[str], missing: set[str],
) -> tuple[CoverageYearSummary, ...]:
    result = []
    for year in sorted({value.year for value in expected}):
        keys = {value.isoformat() for value in expected if value.year == year}
        result.append(CoverageYearSummary(
            year, len(keys), len(keys & {value.isoformat() for value in observed}),
            len(keys & verified), len(keys & provisional), len(keys & missing),
        ))
    return tuple(result)


def _asset_years(
    expected: set[str], observed: set[str], verified: set[str],
    provisional: set[str], missing: set[str],
) -> tuple[CoverageYearSummary, ...]:
    result = []
    for year in sorted({int(value.rsplit("|", 1)[1]) for value in expected}):
        keys = {value for value in expected if value.endswith(f"|{year}")}
        result.append(CoverageYearSummary(
            year, len(keys), len(keys & observed), len(keys & verified),
            len(keys & provisional), len(keys & missing),
        ))
    return tuple(result)


def _date_value(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    if "T" in text:
        text = text.split("T", maxsplit=1)[0]
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    return date.fromisoformat(text[:10])


def _date_expr(field: str) -> pl.Expr:
    return pl.coalesce(
        pl.col(field).cast(pl.String).str.strptime(pl.Date, "%Y%m%d", strict=False),
        pl.col(field).cast(pl.Date, strict=False),
    )


__all__ = [
    "AuditMode", "CoverageSummary", "CoverageYearSummary", "PlannedDataset",
    "UpdatePlan", "build_update_plan", "coverage_scopes",
    "planning_state_fingerprint",
]
