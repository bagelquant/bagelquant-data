"""Polars-native local source-separated data lake."""

from __future__ import annotations

import json
import shutil
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal

import polars as pl

from bagelquant_data.datasource.base import DataRequest, DataSource
from bagelquant_data.lake.snapshot import SnapshotRef
from bagelquant_data.utils.exceptions import DatasetNotFoundError, LakeError

WriteMode = Literal["append", "overwrite"]
FIELD_CATALOG_TABLE = "__fields"
LEGACY_DATA_ITEM_CATALOG_TABLE = "__data_item_ids"
TIME_COLUMNS = (
    "time",
    "date",
    "trade_date",
    "cal_date",
    "f_ann_date",
    "datetime",
    "timestamp",
)
ASSET_COLUMNS = ("asset_id", "ts_code", "symbol", "asset", "code")


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
        ref = self._snapshot_ref(source, dataset, snapshot=snapshot)
        data_path = ref.path / "data.parquet" if ref.path is not None else None
        if data_path is None or not data_path.exists():
            raise DatasetNotFoundError(f"No local lake table: {source}/{dataset}")
        frame = pl.read_parquet(data_path)
        frame = _normalize_table_columns(frame)
        if columns is not None:
            helpers = [
                column for column in ("time", "asset_id") if column in frame.columns
            ]
            projected = list(
                dict.fromkeys([*helpers, *(str(column) for column in columns)])
            )
            frame = frame.select(
                [column for column in projected if column in frame.columns]
            )
        return _filter_time(frame, start_date=start_date, end_date=end_date)

    def write(
        self,
        source: str,
        dataset: str,
        data: pl.DataFrame,
        *,
        mode: WriteMode = "append",
        metadata: Mapping[str, Any] | None = None,
        **_: Any,
    ) -> SnapshotRef:
        if mode not in {"append", "overwrite"}:
            raise LakeError("mode must be 'append' or 'overwrite'")
        if not isinstance(data, pl.DataFrame):
            raise LakeError("lake data must be a polars DataFrame")
        created_at = datetime.now(UTC)
        snapshot_id = created_at.strftime("%Y%m%dT%H%M%S%fZ")
        frame = _normalize_table_columns(data)
        if mode == "append":
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
        catalog = {
            "source": source,
            "dataset": dataset,
            "latest_snapshot": snapshot_id,
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
        shutil.rmtree(
            self._dataset_dir(source, dataset) / "snapshots" / snapshot,
            ignore_errors=True,
        )

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
            return self._snapshot_ref(source, dataset, snapshot=None)
        except DatasetNotFoundError:
            return None

    def snapshots(self, source: str, dataset: str) -> tuple[SnapshotRef, ...]:
        root = self._dataset_dir(source, dataset) / "snapshots"
        if not root.exists():
            return ()
        return tuple(
            SnapshotRef(
                source=source, dataset=dataset, snapshot_id=path.name, path=path
            )
            for path in sorted(root.iterdir())
            if path.is_dir()
        )

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


def shape_panel_field(data: pl.DataFrame, *, field: str) -> pl.DataFrame:
    frame = _normalize_table_columns(data)
    if field not in frame.columns:
        raise LakeError(f"Panel field is missing from data: {field}")
    if "time" not in frame.columns or "asset_id" not in frame.columns:
        raise LakeError("Panel data requires time and asset_id columns")
    return (
        frame.select("time", "asset_id", pl.col(field).alias("value"))
        .with_columns(
            pl.col("time").cast(pl.Date, strict=False),
            pl.col("asset_id").cast(pl.String),
        )
        .sort(["time", "asset_id"])
    )


def _normalize_table_columns(frame: pl.DataFrame) -> pl.DataFrame:
    rename: dict[str, str] = {}
    if "time" not in frame.columns:
        for column in TIME_COLUMNS:
            if column in frame.columns:
                rename[column] = "time"
                break
    if "asset_id" not in frame.columns:
        for column in ASSET_COLUMNS:
            if column in frame.columns:
                rename[column] = "asset_id"
                break
    normalized = frame.rename(rename)
    if "time" in normalized.columns:
        normalized = normalized.with_columns(_date_column("time"))
    if "asset_id" in normalized.columns:
        normalized = normalized.with_columns(pl.col("asset_id").cast(pl.String))
    return normalized


def _date_column(column: str) -> pl.Expr:
    text = pl.col(column).cast(pl.String)
    return (
        pl.coalesce(
            text.str.strptime(pl.Date, "%Y%m%d", strict=False),
            text.str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            pl.col(column).cast(pl.Date, strict=False),
        )
        .alias(column)
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


def _deduplicate(frame: pl.DataFrame) -> pl.DataFrame:
    keys = [column for column in ("time", "asset_id") if column in frame.columns]
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
