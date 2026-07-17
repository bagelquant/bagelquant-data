"""One-time hard-cutover bootstrap for authoritative update state."""

from __future__ import annotations

import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import polars as pl

from bagelquant_data.core.exceptions import ConfigurationError, DatasetNotFoundError
from bagelquant_data.core.types import DateLike
from bagelquant_data.management.lake import DataLake
from bagelquant_data.pipeline.scopes import synchronize_requests
from bagelquant_data.query.raw import RawQueryService


def bootstrap_update_state(
    lake: DataLake,
    *,
    start: DateLike = "1999-12-31",
    end: DateLike | None = None,
    apply: bool = False,
) -> dict[str, Any]:
    """Preview or apply the conservative ledger-v1 migration."""

    datasets = [
        lake.admin.datasets.get(str(row["name"]), source=str(row["source"]))
        for row in lake.admin.datasets.list()
        if row["enabled"]
    ]
    summary = {
        "mode": "apply" if apply else "dry-run",
        "datasets": len(datasets),
        "daily": sum(spec.update_type == "by_daily" for spec in datasets),
        "asset": sum(spec.update_type == "by_asset" for spec in datasets),
        "already_complete": lake.metadata.update_state_ready(),
        "backup": None,
    }
    if not apply:
        return summary
    leases = lake.metadata.active_update_leases()
    if leases:
        names = ", ".join(f"{row['source']}/{row['dataset']}" for row in leases)
        raise ConfigurationError(
            f"cannot bootstrap while update leases are active: {names}"
        )
    if lake.metadata.update_state_ready():
        return summary

    with lake.metadata.connect() as db:
        db.execute("pragma wal_checkpoint(full)")
    backup = _backup_database(lake.paths.database)
    summary["backup"] = str(backup)
    raw = RawQueryService(lake.parquet, lake.metadata)
    seeded_daily = 0
    for spec in datasets:
        if spec.update_type not in {"by_daily", "by_asset"}:
            continue
        synchronize_requests(
            spec=spec,
            raw=raw,
            metadata=lake.metadata,
            start=start,
            end=end,
        )
        if spec.update_type == "by_asset":
            lake.metadata.bootstrap_asset_data_max(
                source=spec.source,
                dataset=spec.name,
                maxima=_asset_max_dates(raw, spec.source, spec.name),
            )
            continue
        if spec.update_type != "by_daily" or spec.source_api_param_sets:
            continue
        present = _present_dates(raw, spec.source, spec.name)
        rows = lake.metadata.update_scopes(
            source=spec.source, dataset=spec.name, scope_kind="date"
        )
        seeded_daily += lake.metadata.bootstrap_daily_success(
            int(row["id"]) for row in rows if str(row["scope_key"]) in present
        )
    lake.metadata.complete_update_state_bootstrap()
    summary["seeded_daily_scopes"] = seeded_daily
    summary["already_complete"] = True
    return summary


def _present_dates(raw: RawQueryService, source: str, dataset: str) -> set[str]:
    try:
        frame = raw.query(dataset, source=source, fields=("time",)).collect()
    except DatasetNotFoundError:
        return set()
    if frame.is_empty() or "time" not in frame.columns:
        return set()
    return {
        value.isoformat()
        for value in frame.select(pl.col("time").cast(pl.Date, strict=False))
        .drop_nulls()
        .get_column("time")
        .to_list()
    }


def _asset_max_dates(raw: RawQueryService, source: str, dataset: str) -> dict[str, str]:
    try:
        frame = raw.query(dataset, source=source, fields=("asset_id", "time")).collect()
    except DatasetNotFoundError:
        return {}
    if frame.is_empty() or not {"asset_id", "time"}.issubset(frame.columns):
        return {}
    rows = frame.group_by("asset_id").agg(pl.col("time").max()).to_dicts()
    return {
        str(row["asset_id"]): row["time"].isoformat()
        for row in rows
        if row["asset_id"] is not None and row["time"] is not None
    }


def _backup_database(database: Path) -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    backup = database.with_name(f"{database.name}.{timestamp}.bak")
    shutil.copy2(database, backup)
    return backup
