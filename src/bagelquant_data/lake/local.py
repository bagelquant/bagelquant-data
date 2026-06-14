"""Polars-native local source-separated data lake."""

from __future__ import annotations

import json
import shutil
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import polars as pl

from bagelquant_data.datasource.base import DataRequest, DataSource
from bagelquant_data.lake.snapshot import SnapshotRef
from bagelquant_data.utils.exceptions import DatasetNotFoundError, LakeError
from bagelquant_data.utils.normalize import (
    as_date,
    date_column,
    normalize_table_columns,
)

WriteMode = Literal["append", "overwrite"]
PartitionGranularity = Literal["day", "month", "quarter", "year"]
FIELD_CATALOG_TABLE = "__fields"
LEGACY_DATA_ITEM_CATALOG_TABLE = "__data_item_ids"


class LocalDataLake:
    """Filesystem-backed lake with Polars table snapshots."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def read(
        self,
        source: str,
        dataset: str,
        *,
        snapshot: str | None = None,
        columns: Sequence[str] | None = None,
        start_date: str | date | datetime | None = None,
        end_date: str | date | datetime | None = None,
        **_: Any,
    ) -> pl.DataFrame:
        catalog = self._table_catalog_metadata(source, dataset)
        if _is_partitioned_catalog(catalog):
            frame = self._read_partitioned(
                source,
                dataset,
                catalog=catalog,
                snapshot=snapshot,
                columns=columns,
                start_date=start_date,
                end_date=end_date,
            )
        else:
            refs = (
                self.snapshots(source, dataset)
                if catalog.get("append_only") and snapshot is None
                else (self._snapshot_ref(source, dataset, snapshot=snapshot),)
            )
            paths = tuple(
                ref.path / "data.parquet"
                for ref in refs
                if ref.path is not None and (ref.path / "data.parquet").exists()
            )
            if not paths:
                raise DatasetNotFoundError(f"No local lake table: {source}/{dataset}")
            frame = _scan_parquet_paths(
                paths,
                columns=columns,
                start_date=start_date,
                end_date=end_date,
            )
        return frame

    def write(
        self,
        source: str,
        dataset: str,
        data: pl.DataFrame,
        *,
        mode: WriteMode = "append",
        metadata: Mapping[str, Any] | None = None,
        partition_column: str | None = None,
        partition_granularity: PartitionGranularity | None = None,
        **_: Any,
    ) -> SnapshotRef:
        if mode not in {"append", "overwrite"}:
            raise LakeError("mode must be 'append' or 'overwrite'")
        if not isinstance(data, pl.DataFrame):
            raise LakeError("lake data must be a polars DataFrame")
        if (partition_column is None) != (partition_granularity is None):
            raise LakeError(
                "partition_column and partition_granularity must be provided together"
            )
        created_at = datetime.now(UTC)
        snapshot_id = created_at.strftime("%Y%m%dT%H%M%S%fZ")
        frame = normalize_table_columns(data)
        if partition_column is not None:
            return self._write_partitioned(
                source,
                dataset,
                frame,
                mode=mode,
                metadata=metadata,
                partition_column=partition_column,
                partition_granularity=partition_granularity,
                snapshot_id=snapshot_id,
                created_at=created_at,
            )
        if mode == "append":
            catalog = self._table_catalog_metadata(source, dataset)
            if not _append_only_dataset(dataset, catalog):
                try:
                    previous = self.read(source, dataset)
                except DatasetNotFoundError:
                    previous = None
                if previous is not None:
                    frame = _deduplicate(
                        pl.concat([previous, frame], how="diagonal_relaxed")
                    )

        snapshot_dir = self._dataset_dir(source, dataset) / "snapshots" / snapshot_id
        snapshot_dir.mkdir(parents=True, exist_ok=False)
        frame.write_parquet(snapshot_dir / "data.parquet")
        payload = {
            "source": source,
            "dataset": dataset,
            "snapshot_id": snapshot_id,
            "format": "parquet",
            "created_at": created_at.isoformat(),
            "mode": mode,
            "rows": frame.height,
            "columns": frame.columns,
            "metadata": dict(metadata or {}),
            **_table_metadata(frame),
        }
        (snapshot_dir / "metadata.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        append_only = mode == "append" and _append_only_dataset(
            dataset, self._table_catalog_metadata(source, dataset)
        )
        catalog = {
            "source": source,
            "dataset": dataset,
            "latest_snapshot": snapshot_id,
            "append_only": append_only,
            "partition_column": None,
            "partition_granularity": None,
            "partitions": {},
            "updated_at": created_at.isoformat(),
            **_table_metadata(frame),
        }
        self._catalog_path(source, dataset).write_text(
            json.dumps(catalog, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        return SnapshotRef(
            source=source,
            dataset=dataset,
            snapshot_id=snapshot_id,
            path=snapshot_dir,
            created_at=created_at,
            metadata=payload,
        )

    def add(
        self,
        source: str,
        dataset: str,
        data: pl.DataFrame,
        *,
        metadata: Mapping[str, Any] | None = None,
    ) -> SnapshotRef:
        return self.write(source, dataset, data, mode="overwrite", metadata=metadata)

    def edit(
        self,
        source: str,
        dataset: str,
        data: pl.DataFrame,
        *,
        metadata: Mapping[str, Any] | None = None,
    ) -> SnapshotRef:
        return self.write(source, dataset, data, mode="overwrite", metadata=metadata)

    def ingest(
        self, source: DataSource, request: DataRequest, *, mode: WriteMode = "overwrite"
    ) -> SnapshotRef:
        return self.write(source.name, request.dataset, source.read(request), mode=mode)

    def delete(
        self, source: str, dataset: str | None = None, *, snapshot: str | None = None
    ) -> None:
        if dataset is None:
            shutil.rmtree(self.root / source, ignore_errors=True)
            return
        if snapshot is None:
            shutil.rmtree(self._dataset_dir(source, dataset), ignore_errors=True)
            return
        table_root = self._dataset_dir(source, dataset)
        shutil.rmtree(table_root / "snapshots" / snapshot, ignore_errors=True)
        for snapshot_dir in table_root.glob(f"**/snapshots/{snapshot}"):
            shutil.rmtree(snapshot_dir, ignore_errors=True)

    def list_sources(self) -> tuple[str, ...]:
        if not self.root.exists():
            return ()
        return tuple(sorted(path.name for path in self.root.iterdir() if path.is_dir()))

    def list_datasets(self, source: str | None = None) -> tuple[tuple[str, str], ...]:
        sources = (source,) if source is not None else self.list_sources()
        rows: list[tuple[str, str]] = []
        for source_name in sources:
            source_dir = self.root / source_name
            if source_dir.exists():
                rows.extend(
                    (source_name, path.name)
                    for path in source_dir.iterdir()
                    if path.is_dir() and not path.name.startswith("__")
                )
        return tuple(sorted(rows))

    def list_tables(self, source: str | None = None) -> tuple[tuple[str, str], ...]:
        return self.list_datasets(source)

    def latest(self, source: str, dataset: str) -> SnapshotRef | None:
        try:
            catalog = self._table_catalog_metadata(source, dataset)
            if _is_partitioned_catalog(catalog):
                refs = self._partition_snapshot_refs(source, dataset, catalog)
                return max(refs, key=lambda ref: ref.created_at) if refs else None
            return self._snapshot_ref(source, dataset, snapshot=None)
        except DatasetNotFoundError:
            return None

    def snapshots(self, source: str, dataset: str) -> tuple[SnapshotRef, ...]:
        catalog = self._table_catalog_metadata(source, dataset)
        if _is_partitioned_catalog(catalog):
            return tuple(
                sorted(
                    (
                        *self._root_snapshot_refs(source, dataset),
                        *self._all_partition_snapshot_refs(source, dataset, catalog),
                    ),
                    key=lambda ref: (str(ref.path), ref.snapshot_id),
                )
            )
        return self._root_snapshot_refs(source, dataset)

    def fields(self, source: str | None = None) -> pl.DataFrame:
        rows: list[dict[str, str]] = []
        for source_name, dataset in self.list_datasets(source):
            metadata = self._table_catalog_metadata(source_name, dataset)
            for field in metadata.get("panel_fields", []):
                rows.append(
                    {
                        "source": source_name,
                        "table": dataset,
                        "field": str(field),
                        "field_id": f"{source_name}_{dataset}_{field}",
                    }
                )
        return pl.DataFrame(rows, schema=["source", "table", "field", "field_id"])

    def field_ids(self, source: str) -> tuple[str, ...]:
        data = self.fields(source)
        return tuple(data["field_id"].to_list()) if data.height else ()

    def data_item_ids(self, source: str) -> tuple[str, ...]:
        return self.field_ids(source)

    def panel_field_ids(self, source: str | None = None) -> tuple[str, ...]:
        data = self.fields(source)
        return tuple(data["field_id"].to_list()) if data.height else ()

    def asset_ids(self, source: str) -> tuple[str, ...]:
        ids: set[str] = set()
        for _, dataset in self.list_datasets(source):
            data = self.read(source, dataset)
            if "asset_id" in data.columns:
                ids.update(str(value) for value in data["asset_id"].unique().to_list())
        return tuple(sorted(ids))

    def resolve_panel_field(
        self, qualified_id: str, *, validate_with_read: bool = True
    ) -> tuple[str, str, str] | None:
        del validate_with_read
        for row in self.fields().iter_rows(named=True):
            if row["field_id"] == qualified_id:
                return str(row["source"]), str(row["table"]), str(row["field"])
        return None

    def read_panel_field(
        self,
        qualified_id: str,
        *,
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None,
    ) -> pl.DataFrame:
        resolved = self.resolve_panel_field(qualified_id)
        if resolved is None:
            raise DatasetNotFoundError(f"No panel field: {qualified_id}")
        source, dataset, field = resolved
        data = self.read(source, dataset, start_date=start_date, end_date=end_date)
        return shape_panel_field(data, field=field)

    def update_catalog_entries(self, source: str, dataset: str, **_: Any) -> None:
        if not self._catalog_path(source, dataset).exists():
            raise DatasetNotFoundError(f"No local lake table: {source}/{dataset}")

    def _dataset_dir(self, source: str, dataset: str) -> Path:
        return self.root / source / dataset

    def _catalog_path(self, source: str, dataset: str) -> Path:
        return self._dataset_dir(source, dataset) / "_catalog.json"

    def _table_catalog_metadata(self, source: str, dataset: str) -> dict[str, Any]:
        path = self._catalog_path(source, dataset)
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def _snapshot_ref(
        self, source: str, dataset: str, *, snapshot: str | None
    ) -> SnapshotRef:
        if snapshot is None:
            catalog = self._table_catalog_metadata(source, dataset)
            snapshot = catalog.get("latest_snapshot")
        if not isinstance(snapshot, str):
            raise DatasetNotFoundError(f"No local lake table: {source}/{dataset}")
        path = self._dataset_dir(source, dataset) / "snapshots" / snapshot
        if not path.exists():
            raise DatasetNotFoundError(
                f"No local lake snapshot: {source}/{dataset}/{snapshot}"
            )
        return SnapshotRef(
            source=source, dataset=dataset, snapshot_id=snapshot, path=path
        )

    def _read_partitioned(
        self,
        source: str,
        dataset: str,
        *,
        catalog: Mapping[str, Any],
        snapshot: str | None,
        columns: Sequence[str] | None,
        start_date: str | date | datetime | None,
        end_date: str | date | datetime | None,
    ) -> pl.DataFrame:
        refs = (
            (
                *self._root_snapshot_refs(source, dataset, snapshot=snapshot),
                *self._all_partition_snapshot_refs(
                    source, dataset, catalog, snapshot=snapshot
                ),
            )
            if snapshot is not None
            else (
                *self._root_snapshot_refs(source, dataset),
                *self._partition_snapshot_refs(source, dataset, catalog),
            )
        )
        start = _as_date_or_none(start_date)
        end = _as_date_or_none(end_date)
        selected = [
            ref
            for ref in refs
            if _partition_overlaps(
                ref.metadata or {},
                granularity=str(catalog.get("partition_granularity")),
                start_date=start,
                end_date=end,
            )
        ]
        if not selected:
            raise DatasetNotFoundError(f"No local lake table: {source}/{dataset}")
        paths = tuple(
            ref.path / "data.parquet"
            for ref in selected
            if ref.path is not None and (ref.path / "data.parquet").exists()
        )
        if not paths:
            raise DatasetNotFoundError(f"No local lake table: {source}/{dataset}")
        return _scan_parquet_paths(
            paths,
            columns=columns,
            start_date=start_date,
            end_date=end_date,
        )

    def _write_partitioned(
        self,
        source: str,
        dataset: str,
        frame: pl.DataFrame,
        *,
        mode: WriteMode,
        metadata: Mapping[str, Any] | None,
        partition_column: str,
        partition_granularity: PartitionGranularity | None,
        snapshot_id: str,
        created_at: datetime,
    ) -> SnapshotRef:
        if partition_granularity is None:
            raise LakeError("partition_granularity is required")
        if partition_column not in frame.columns:
            raise LakeError(
                f"partition column is missing from data: {partition_column}"
            )
        frame = frame.with_columns(date_column(partition_column))
        if frame.filter(pl.col(partition_column).is_null()).height:
            raise LakeError(
                f"partition column has null or invalid dates: {partition_column}"
            )

        catalog = self._table_catalog_metadata(source, dataset)
        partitions = dict(catalog.get("partitions") or {})
        refs: list[SnapshotRef] = []
        table_metadata = _table_metadata(frame)
        for values, partition_frame in _partition_frames(
            frame,
            column=partition_column,
            granularity=partition_granularity,
        ):
            partition_path = _partition_path(values, granularity=partition_granularity)
            if mode == "append":
                previous = self._read_latest_partition(
                    source, dataset, partitions.get(partition_path)
                )
                if previous is not None:
                    partition_frame = _deduplicate(
                        pl.concat([previous, partition_frame], how="diagonal_relaxed")
                    )
            else:
                partition_frame = _deduplicate(partition_frame)

            snapshot_dir = (
                self._dataset_dir(source, dataset)
                / partition_path
                / "snapshots"
                / snapshot_id
            )
            snapshot_dir.mkdir(parents=True, exist_ok=False)
            partition_frame.write_parquet(snapshot_dir / "data.parquet")
            payload = {
                "source": source,
                "dataset": dataset,
                "snapshot_id": snapshot_id,
                "format": "parquet",
                "created_at": created_at.isoformat(),
                "mode": mode,
                "rows": partition_frame.height,
                "columns": partition_frame.columns,
                "metadata": dict(metadata or {}),
                "partition_column": partition_column,
                "partition_granularity": partition_granularity,
                "partition": values,
                **_table_metadata(partition_frame),
            }
            (snapshot_dir / "metadata.json").write_text(
                json.dumps(payload, indent=2, sort_keys=True, default=str),
                encoding="utf-8",
            )
            partitions[partition_path] = {
                "latest_snapshot": snapshot_id,
                "updated_at": created_at.isoformat(),
                "path": partition_path,
                "partition": values,
                "rows": partition_frame.height,
            }
            refs.append(
                SnapshotRef(
                    source=source,
                    dataset=dataset,
                    snapshot_id=snapshot_id,
                    path=snapshot_dir,
                    created_at=created_at,
                    metadata=payload,
                    **_snapshot_partition_kwargs(values),
                )
            )

        if not refs:
            raise LakeError("partitioned write produced no partitions")
        latest_ref = max(refs, key=lambda ref: ref.created_at)
        catalog_payload = {
            "source": source,
            "dataset": dataset,
            "latest_snapshot": latest_ref.snapshot_id,
            "partition_column": partition_column,
            "partition_granularity": partition_granularity,
            "partitions": partitions,
            "updated_at": created_at.isoformat(),
            **table_metadata,
        }
        self._catalog_path(source, dataset).write_text(
            json.dumps(catalog_payload, indent=2, sort_keys=True, default=str),
            encoding="utf-8",
        )
        return latest_ref

    def _read_latest_partition(
        self, source: str, dataset: str, entry: Any
    ) -> pl.DataFrame | None:
        if not isinstance(entry, Mapping):
            return None
        snapshot = entry.get("latest_snapshot")
        path = entry.get("path")
        if not isinstance(snapshot, str) or not isinstance(path, str):
            return None
        data_path = (
            self._dataset_dir(source, dataset)
            / path
            / "snapshots"
            / snapshot
            / "data.parquet"
        )
        if not data_path.exists():
            return None
        return normalize_table_columns(pl.read_parquet(data_path))

    def _root_snapshot_refs(
        self, source: str, dataset: str, *, snapshot: str | None = None
    ) -> tuple[SnapshotRef, ...]:
        root = self._dataset_dir(source, dataset) / "snapshots"
        if not root.exists():
            return ()
        return tuple(
            SnapshotRef(
                source=source, dataset=dataset, snapshot_id=path.name, path=path
            )
            for path in sorted(root.iterdir())
            if path.is_dir() and (snapshot is None or path.name == snapshot)
        )

    def _partition_snapshot_refs(
        self, source: str, dataset: str, catalog: Mapping[str, Any]
    ) -> tuple[SnapshotRef, ...]:
        refs = []
        for entry in (catalog.get("partitions") or {}).values():
            if not isinstance(entry, Mapping):
                continue
            snapshot = entry.get("latest_snapshot")
            path = entry.get("path")
            values = entry.get("partition") or {}
            if not isinstance(snapshot, str) or not isinstance(path, str):
                continue
            snapshot_dir = (
                self._dataset_dir(source, dataset) / path / "snapshots" / snapshot
            )
            if snapshot_dir.exists():
                refs.append(
                    SnapshotRef(
                        source=source,
                        dataset=dataset,
                        snapshot_id=snapshot,
                        path=snapshot_dir,
                        created_at=_parse_created_at(snapshot_dir / "metadata.json"),
                        metadata=dict(values) if isinstance(values, Mapping) else None,
                        **_snapshot_partition_kwargs(values),
                    )
                )
        return tuple(refs)

    def _all_partition_snapshot_refs(
        self,
        source: str,
        dataset: str,
        catalog: Mapping[str, Any],
        *,
        snapshot: str | None = None,
    ) -> tuple[SnapshotRef, ...]:
        refs = []
        for entry in (catalog.get("partitions") or {}).values():
            if not isinstance(entry, Mapping):
                continue
            path = entry.get("path")
            values = entry.get("partition") or {}
            if not isinstance(path, str):
                continue
            root = self._dataset_dir(source, dataset) / path / "snapshots"
            if not root.exists():
                continue
            for snapshot_dir in root.iterdir():
                if not snapshot_dir.is_dir():
                    continue
                if snapshot is not None and snapshot_dir.name != snapshot:
                    continue
                refs.append(
                    SnapshotRef(
                        source=source,
                        dataset=dataset,
                        snapshot_id=snapshot_dir.name,
                        path=snapshot_dir,
                        created_at=_parse_created_at(snapshot_dir / "metadata.json"),
                        metadata=dict(values) if isinstance(values, Mapping) else None,
                        **_snapshot_partition_kwargs(values),
                    )
                )
        return tuple(refs)


def shape_panel_field(data: pl.DataFrame, *, field: str) -> pl.DataFrame:
    frame = normalize_table_columns(data)
    if field not in frame.columns:
        raise LakeError(f"Panel field is missing from data: {field}")
    if "time" not in frame.columns or "asset_id" not in frame.columns:
        raise LakeError("Panel data requires time and asset_id columns")
    return (
        frame.select("time", "asset_id", pl.col(field).alias("value"))
        .with_columns(date_column("time"), pl.col("asset_id").cast(pl.String))
        .sort(["time", "asset_id"])
    )


def _filter_time(
    frame: pl.DataFrame,
    *,
    start_date: str | date | datetime | None,
    end_date: str | date | datetime | None,
) -> pl.DataFrame:
    if "time" not in frame.columns:
        return frame
    filtered = frame
    if start_date is not None:
        filtered = filtered.filter(pl.col("time") >= pl.lit(start_date).cast(pl.Date))
    if end_date is not None:
        filtered = filtered.filter(pl.col("time") <= pl.lit(end_date).cast(pl.Date))
    sort_columns = ["time"]
    if "asset_id" in filtered.columns:
        sort_columns.append("asset_id")
    return filtered.sort(sort_columns)


def _is_partitioned_catalog(catalog: Mapping[str, Any]) -> bool:
    return bool(
        catalog.get("partition_column") and catalog.get("partition_granularity")
    )


def _append_only_dataset(dataset: str, catalog: Mapping[str, Any]) -> bool:
    return bool(catalog.get("append_only")) or dataset.startswith("__")


def _as_date_or_none(value: str | date | datetime | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return as_date(value)


def _partition_frames(
    frame: pl.DataFrame,
    *,
    column: str,
    granularity: PartitionGranularity,
) -> list[tuple[dict[str, int], pl.DataFrame]]:
    enriched = frame.with_columns(_partition_exprs(column, granularity))
    key_columns = _partition_key_columns(granularity)
    partitions: list[tuple[dict[str, int], pl.DataFrame]] = []
    groups = enriched.partition_by(
        key_columns,
        maintain_order=True,
        include_key=True,
        as_dict=True,
    )
    for key, partition_frame in sorted(groups.items()):
        key_values = key if isinstance(key, tuple) else (key,)
        values = dict(
            zip(key_columns, (int(value) for value in key_values), strict=True)
        )
        partitions.append((values, partition_frame.drop(key_columns)))
    return partitions


def _partition_exprs(column: str, granularity: PartitionGranularity) -> list[pl.Expr]:
    exprs = [pl.col(column).dt.year().alias("year")]
    if granularity in {"month", "day"}:
        exprs.append(pl.col(column).dt.month().alias("month"))
    if granularity == "day":
        exprs.append(pl.col(column).dt.day().alias("day"))
    if granularity == "quarter":
        exprs.append(pl.col(column).dt.quarter().alias("quarter"))
    return exprs


def _partition_key_columns(granularity: PartitionGranularity) -> list[str]:
    if granularity == "day":
        return ["year", "month", "day"]
    if granularity == "month":
        return ["year", "month"]
    if granularity == "quarter":
        return ["year", "quarter"]
    return ["year"]


def _partition_path(
    values: Mapping[str, int], *, granularity: PartitionGranularity
) -> str:
    parts = [f"year={values['year']:04d}"]
    if granularity in {"month", "day"}:
        parts.append(f"month={values['month']:02d}")
    if granularity == "day":
        parts.append(f"day={values['day']:02d}")
    if granularity == "quarter":
        parts.append(f"quarter={values['quarter']}")
    return "/".join(parts)


def _snapshot_partition_kwargs(values: Any) -> dict[str, int | None]:
    if not isinstance(values, Mapping):
        return {}
    return {
        key: int(values[key]) if key in values and values[key] is not None else None
        for key in ("year", "month", "day", "quarter")
        if key in values
    }


def _partition_overlaps(
    values: Mapping[str, Any],
    *,
    granularity: str,
    start_date: date | None,
    end_date: date | None,
) -> bool:
    if start_date is None and end_date is None:
        return True
    try:
        start, end = _partition_bounds(values, granularity=granularity)
    except (KeyError, TypeError, ValueError):
        return True
    if start_date is not None and end < start_date:
        return False
    if end_date is not None and start > end_date:
        return False
    return True


def _partition_bounds(
    values: Mapping[str, Any], *, granularity: str
) -> tuple[date, date]:
    year = int(values["year"])
    if granularity == "year":
        return date(year, 1, 1), date(year, 12, 31)
    if granularity == "quarter":
        quarter = int(values["quarter"])
        month = (quarter - 1) * 3 + 1
        end_month = month + 2
        return date(year, month, 1), _month_end(year, end_month)
    month = int(values["month"])
    if granularity == "month":
        return date(year, month, 1), _month_end(year, month)
    day = int(values["day"])
    point = date(year, month, day)
    return point, point


def _month_end(year: int, month: int) -> date:
    if month == 12:
        return date(year, 12, 31)
    return date(year, month + 1, 1) - timedelta(days=1)


def _parse_created_at(path: Path) -> datetime:
    if path.exists():
        try:
            value = json.loads(path.read_text(encoding="utf-8")).get("created_at")
            if isinstance(value, str):
                return datetime.fromisoformat(value)
        except (OSError, json.JSONDecodeError, ValueError):
            pass
    return datetime.now(UTC)


def _deduplicate(frame: pl.DataFrame) -> pl.DataFrame:
    keys = [column for column in ("time", "asset_id") if column in frame.columns]
    if {"time", "asset_id"}.issubset(frame.columns):
        keys.extend(
            column for column in ("end_date", "period") if column in frame.columns
        )
    if not keys:
        return frame.unique(keep="last", maintain_order=True)
    return frame.unique(subset=keys, keep="last", maintain_order=True).sort(keys)


def _table_metadata(frame: pl.DataFrame) -> dict[str, Any]:
    ignored = {"time", "asset_id", "create_time", "delete_flag"}
    panel_fields = (
        [column for column in frame.columns if column not in ignored]
        if {"time", "asset_id"}.issubset(frame.columns)
        else []
    )
    return {
        "time_column": "time" if "time" in frame.columns else None,
        "asset_id_column": "asset_id" if "asset_id" in frame.columns else None,
        "columns": frame.columns,
        "panel_fields": panel_fields,
    }


def _scan_parquet_paths(
    paths: Sequence[Path],
    *,
    columns: Sequence[str] | None,
    start_date: str | date | datetime | None,
    end_date: str | date | datetime | None,
) -> pl.DataFrame:
    lf = pl.scan_parquet(
        [str(path) for path in paths],
        missing_columns="insert",
        extra_columns="ignore",
    )
    schema_names = set(lf.collect_schema().names())
    if "time" in schema_names:
        if start_date is not None:
            lf = lf.filter(pl.col("time") >= pl.lit(start_date).cast(pl.Date))
        if end_date is not None:
            lf = lf.filter(pl.col("time") <= pl.lit(end_date).cast(pl.Date))
    if columns is not None:
        projected = _projected_columns(schema_names, columns)
        if projected:
            lf = lf.select(projected)
    frame = normalize_table_columns(lf.collect())
    return _filter_time(frame, start_date=start_date, end_date=end_date)


def _projected_columns(
    schema_names: set[str],
    columns: Sequence[str],
) -> list[str]:
    helpers = [column for column in ("time", "asset_id") if column in schema_names]
    requested = [str(column) for column in columns]
    return [
        column
        for column in dict.fromkeys([*helpers, *requested])
        if column in schema_names
    ]
