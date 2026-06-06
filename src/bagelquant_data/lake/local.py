"""Local source-separated data lake."""

from __future__ import annotations

import json
import shutil
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from bagelquant_data.datasource.base import DataRequest, DataSource
from bagelquant_data.lake.snapshot import SnapshotRef
from bagelquant_data.utils.exceptions import DatasetNotFoundError, LakeError

WriteMode = Literal["append", "overwrite"]
PartitionGranularity = Literal["month", "day", "quarter"]
FIELD_CATALOG_TABLE = "__fields"
LEGACY_DATA_ITEM_CATALOG_TABLE = "__data_item_ids"


@dataclass(frozen=True, slots=True)
class _PartitionKey:
    year: int
    month: int | None = None
    day: int | None = None
    quarter: int | None = None

    def metadata(self) -> dict[str, int]:
        payload = {"year": self.year}
        if self.month is not None:
            payload["month"] = self.month
        if self.day is not None:
            payload["day"] = self.day
        if self.quarter is not None:
            payload["quarter"] = self.quarter
        return payload


class LocalDataLake:
    """Filesystem-backed lake separated by data source."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def read(
        self,
        source: str,
        dataset: str,
        *,
        year: int | None = None,
        month: int | None = None,
        snapshot: str | None = None,
        columns: Sequence[str] | None = None,
        start_date: str | date | datetime | pd.Timestamp | None = None,
        end_date: str | date | datetime | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        """Read table data from selected year/month partitions."""

        resolved_start = _optional_timestamp(start_date)
        resolved_end = _optional_timestamp(end_date)
        refs = self._refs_for_read(
            source,
            dataset,
            year=year,
            month=month,
            snapshot=snapshot,
            start_date=resolved_start,
            end_date=resolved_end,
        )
        if not refs:
            raise DatasetNotFoundError(f"No local lake table: {source}/{dataset}")

        read_columns = _read_columns_for_projection(
            requested=columns,
            metadata=self._table_catalog_metadata(source, dataset),
        )
        frames = []
        for ref in refs:
            if ref.path is None:
                continue
            data_path = ref.path / "data.parquet"
            if data_path.exists():
                frames.append(
                    _read_table_parquet(
                        data_path,
                        table=dataset,
                        columns=read_columns,
                    )
                )
        if not frames:
            raise DatasetNotFoundError(f"No local lake data files: {source}/{dataset}")
        if len(frames) == 1:
            return _finalize_read_frame(
                frames[0],
                columns=columns,
                start_date=resolved_start,
                end_date=resolved_end,
            )
        combined = pd.concat(frames, axis=0).sort_index()
        return _finalize_read_frame(
            combined,
            columns=columns,
            start_date=resolved_start,
            end_date=resolved_end,
        )

    def write(
        self,
        source: str,
        dataset: str,
        data: pd.DataFrame,
        *,
        mode: WriteMode = "append",
        metadata: Mapping[str, Any] | None = None,
        partition_column: str | None = None,
        partition_granularity: PartitionGranularity = "month",
        update_catalogs: bool = True,
    ) -> SnapshotRef:
        """Write immutable snapshots partitioned by table date granularity."""

        if mode not in {"append", "overwrite"}:
            raise LakeError("mode must be 'append' or 'overwrite'")
        if partition_granularity not in {"month", "day", "quarter"}:
            raise LakeError(
                "partition_granularity must be 'month', 'day', or 'quarter'"
            )
        frame = data.copy(deep=True)
        snapshot_id = _snapshot_id()
        created_at = datetime.now(UTC)
        frame = _normalize_table(
            frame,
            table=dataset,
            created_at=created_at,
            date_column=partition_column,
        )
        partitioned = _partition_frame(
            frame,
            created_at=created_at,
            partition_column=partition_column,
            granularity=partition_granularity,
        )
        refs = []
        for key, partition in partitioned.items():
            if mode == "append":
                previous = self._read_partition_latest(source, dataset, key)
                if previous is not None:
                    partition = pd.concat([previous, partition], axis=0)
                    partition = _deduplicate_append_partition(partition)
            partition = _normalize_parquet_types(partition)
            snapshot_dir = self._snapshot_dir(source, dataset, key, snapshot_id)
            snapshot_dir.mkdir(parents=True, exist_ok=False)
            partition.to_parquet(
                snapshot_dir / "data.parquet",
                index=partition.index.name == "date",
            )
            payload = {
                "source": source,
                "table": dataset,
                "dataset": dataset,
                "snapshot_id": snapshot_id,
                "format": "parquet",
                **key.metadata(),
                "partition_granularity": partition_granularity,
                "created_at": created_at.isoformat(),
                "mode": mode,
                "metadata": dict(metadata or {}),
                "rows": len(partition),
                "columns": list(partition.columns),
                **_table_metadata(dataset, partition),
            }
            (snapshot_dir / "metadata.json").write_text(
                json.dumps(payload, indent=2, sort_keys=True, default=str),
                encoding="utf-8",
            )
            refs.append(
                SnapshotRef(
                    source=source,
                    dataset=dataset,
                    snapshot_id=snapshot_id,
                    year=key.year,
                    month=key.month,
                    day=key.day,
                    quarter=key.quarter,
                    path=snapshot_dir,
                    created_at=created_at,
                    metadata=payload,
                )
            )
            self._write_partition_catalog(source, dataset, key, refs[-1])
        self._write_table_catalog(
            source,
            dataset,
            snapshot_id,
            created_at,
            metadata=_table_metadata(dataset, frame),
        )
        if update_catalogs:
            self._update_main_tables(source, dataset, frame, created_at=created_at)
        return refs[0]

    def add(
        self,
        source: str,
        dataset: str,
        data: pd.DataFrame,
        *,
        metadata: Mapping[str, Any] | None = None,
    ) -> SnapshotRef:
        """Add a dataset snapshot without reading a provider."""

        return self.write(source, dataset, data, mode="overwrite", metadata=metadata)

    def edit(
        self,
        source: str,
        dataset: str,
        data: pd.DataFrame,
        *,
        metadata: Mapping[str, Any] | None = None,
    ) -> SnapshotRef:
        """Replace a dataset with a new immutable snapshot."""

        return self.write(source, dataset, data, mode="overwrite", metadata=metadata)

    def asset_ids(self, source: str) -> tuple[str, ...]:
        """Return known source asset ids."""

        try:
            data = self.read(source, "__asset_ids")
        except DatasetNotFoundError:
            return ()
        return tuple(str(asset_id) for asset_id in data["asset_id"].tolist())

    def field_ids(self, source: str) -> tuple[str, ...]:
        """Return known source field ids."""

        data = self.fields(source)
        if data.empty:
            return ()
        return tuple(str(item_id) for item_id in data["field_id"].tolist())

    def data_item_ids(self, source: str) -> tuple[str, ...]:
        """Return known source field ids. Deprecated alias for ``field_ids``."""

        return self.field_ids(source)

    def fields(self, source: str | None = None) -> pd.DataFrame:
        """Return known field catalog rows without reading user datasets."""

        frames: list[pd.DataFrame] = []
        sources = (source,) if source is not None else self.list_sources()
        for source_name in sources:
            for table_name in (FIELD_CATALOG_TABLE, LEGACY_DATA_ITEM_CATALOG_TABLE):
                try:
                    data = self.read(source_name, table_name)
                except DatasetNotFoundError:
                    continue
                if (
                    "field_id" not in data.columns
                    and "data_item_id" not in data.columns
                ):
                    continue
                frames.append(_normalize_field_catalog(source_name, data, self))
                break
        if not frames:
            combined = _metadata_field_catalog(self, source)
        else:
            combined = pd.concat(
                [*frames, _metadata_field_catalog(self, source)],
                axis=0,
                ignore_index=True,
            )
        if combined.empty:
            return pd.DataFrame(columns=["source", "table", "field", "field_id"])
        return combined.drop_duplicates("field_id").sort_values(
            ["source", "table", "field", "field_id"],
            ignore_index=True,
        )

    def data_items(self, source: str | None = None) -> pd.DataFrame:
        """Return known field catalog rows with legacy ``data_item_id`` column."""

        data = self.fields(source)
        if data.empty:
            return pd.DataFrame(
                columns=["source", "table", "field", "data_item_id"]
            )
        legacy = data.rename(columns={"field_id": "data_item_id"})
        return legacy.loc[:, ["source", "table", "field", "data_item_id"]]

    def panel_field_ids(self, source: str | None = None) -> tuple[str, ...]:
        """Return qualified field ids that can be selected as panel fields."""

        ids: set[str] = set()
        for row in self.fields(source).itertuples(index=False):
            if self._catalog_marks_panel_field(
                str(row.source),
                str(row.table),
                str(row.field),
            ):
                ids.add(str(row.field_id))
        return tuple(sorted(ids))

    def resolve_panel_field(
        self,
        qualified_id: str,
        *,
        validate_with_read: bool = True,
    ) -> tuple[str, str, str] | None:
        """Resolve ``source_dataset_field`` into source, dataset, and field."""

        catalog = self.fields()
        if not catalog.empty:
            matches = catalog.loc[catalog["field_id"].astype(str) == qualified_id]
            for row in matches.itertuples(index=False):
                source = str(row.source)
                dataset = str(row.table)
                field = str(row.field)
                if self._catalog_marks_panel_field(source, dataset, field):
                    return source, dataset, field
                if not validate_with_read and self._has_table_catalog(source, dataset):
                    return None

        for source, dataset in self.list_datasets():
            prefix = f"{source}_{dataset}_"
            if not qualified_id.startswith(prefix):
                continue
            field = qualified_id.removeprefix(prefix)
            if not field:
                continue
            if self._catalog_marks_panel_field(source, dataset, field):
                return source, dataset, field
            if not validate_with_read and self._has_table_catalog(source, dataset):
                return None
            if not validate_with_read:
                return source, dataset, field
            try:
                data = self.read(source, dataset)
            except DatasetNotFoundError:
                return None
            if field not in data.columns or _infer_asset_column(data) is None:
                return None
            date_column = _infer_partition_column(data.reset_index())
            if date_column is None:
                return None
            if not _is_panel_value_field(data, field=field, date_column=date_column):
                return None
            return source, dataset, field
        return None

    def read_panel_field(
        self,
        qualified_id: str,
        *,
        start_date: str | datetime | None = None,
        end_date: str | datetime | None = None,
    ) -> pd.DataFrame:
        """Read a qualified lake field as a date x asset-id panel."""

        resolved = self.resolve_panel_field(qualified_id)
        if resolved is None:
            raise DatasetNotFoundError(f"No panel field: {qualified_id}")
        source, dataset, field = resolved
        metadata = self._table_catalog_metadata(source, dataset)
        read_columns = _panel_read_columns(field=field, metadata=metadata)
        data = self.read(
            source,
            dataset,
            columns=read_columns,
            start_date=start_date,
            end_date=end_date,
        )
        panel = shape_panel_field(data, field=field)
        return panel

    def update_catalog_entries(
        self,
        source: str,
        dataset: str,
        *,
        asset_ids: set[str] | None = None,
        fields: set[str] | None = None,
        created_at: datetime | None = None,
    ) -> None:
        """Merge known asset and field entries for a dataset."""

        if dataset.startswith("__"):
            return
        resolved_created_at = created_at or datetime.now(UTC)
        if asset_ids:
            self._merge_asset_ids(
                source,
                asset_ids,
                created_at=resolved_created_at,
            )
        if fields:
            self._merge_field_ids(
                source,
                dataset,
                fields,
                created_at=resolved_created_at,
            )

    def delete(
        self,
        source: str,
        dataset: str | None = None,
        *,
        snapshot: str | None = None,
    ) -> None:
        """Delete a source, dataset, or individual snapshot."""

        if dataset is None:
            shutil.rmtree(self.root / source, ignore_errors=True)
            return
        if snapshot is not None:
            for ref in self.snapshots(source, dataset):
                if ref.snapshot_id == snapshot and ref.path is not None:
                    shutil.rmtree(ref.path, ignore_errors=True)
            latest = self.latest(source, dataset)
            if latest is not None and latest.snapshot_id == snapshot:
                self._remove_latest(source, dataset)
            return
        shutil.rmtree(self._dataset_dir(source, dataset), ignore_errors=True)

    def list_sources(self) -> tuple[str, ...]:
        """Return source namespaces in the lake."""

        if not self.root.exists():
            return ()
        return tuple(sorted(path.name for path in self.root.iterdir() if path.is_dir()))

    def list_datasets(self, source: str | None = None) -> tuple[tuple[str, str], ...]:
        """Return available datasets as ``(source, dataset)`` tuples."""

        sources = (source,) if source is not None else self.list_sources()
        datasets: list[tuple[str, str]] = []
        for source_name in sources:
            source_dir = self.root / source_name
            if source_dir.exists():
                datasets.extend(
                    (source_name, path.name)
                    for path in source_dir.iterdir()
                    if path.is_dir() and not path.name.startswith("__")
                )
        return tuple(sorted(datasets))

    def list_tables(self, source: str | None = None) -> tuple[tuple[str, str], ...]:
        """Return available tables as ``(source, table)`` tuples."""

        return self.list_datasets(source)

    def snapshots(self, source: str, dataset: str) -> tuple[SnapshotRef, ...]:
        """List snapshots for a source/dataset."""

        table_dir = self._dataset_dir(source, dataset)
        if not table_dir.exists():
            return ()
        refs = []
        for key, partition_dir in self._partition_dirs(source, dataset):
            snapshot_root = partition_dir / "snapshots"
            paths = snapshot_root.iterdir() if snapshot_root.exists() else ()
            for path in sorted(paths):
                if path.is_dir():
                    refs.append(self._snapshot_ref(source, dataset, path.name, key=key))
        return tuple(refs)

    def latest(self, source: str, dataset: str) -> SnapshotRef | None:
        """Return the latest snapshot ref."""

        catalog_path = self._table_catalog_path(source, dataset)
        if not catalog_path.exists():
            return None
        payload = json.loads(catalog_path.read_text(encoding="utf-8"))
        snapshot_id = payload.get("latest_snapshot")
        if not isinstance(snapshot_id, str):
            return None
        refs = [
            ref
            for ref in self.snapshots(source, dataset)
            if ref.snapshot_id == snapshot_id
        ]
        return refs[0] if refs else None

    def latest_partitions(
        self,
        source: str,
        dataset: str,
    ) -> tuple[SnapshotRef, ...]:
        """Return the latest snapshot ref for each partition."""

        refs: list[SnapshotRef] = []
        table_dir = self._dataset_dir(source, dataset)
        if not table_dir.exists():
            return ()
        for key, partition_dir in self._partition_dirs(source, dataset):
            catalog_path = partition_dir / "_catalog.json"
            if not catalog_path.exists():
                continue
            payload = json.loads(catalog_path.read_text(encoding="utf-8"))
            snapshot_id = payload.get("latest_snapshot")
            if isinstance(snapshot_id, str):
                refs.append(self._snapshot_ref(source, dataset, snapshot_id, key=key))
        return tuple(refs)

    def ingest(
        self,
        source: DataSource,
        request: DataRequest,
        *,
        mode: WriteMode = "overwrite",
    ) -> SnapshotRef:
        """Read from a provider and store the result in the local lake."""

        data = source.read(request)
        return self.write(
            source.name,
            request.dataset,
            data,
            mode=mode,
            metadata={"request": _request_payload(request)},
        )

    def _refs_for_read(
        self,
        source: str,
        dataset: str,
        *,
        year: int | None,
        month: int | None,
        snapshot: str | None,
        start_date: pd.Timestamp | None,
        end_date: pd.Timestamp | None,
    ) -> tuple[SnapshotRef, ...]:
        refs = self.snapshots(source, dataset)
        if snapshot is not None:
            refs = tuple(ref for ref in refs if ref.snapshot_id == snapshot)
        else:
            refs = self.latest_partitions(source, dataset)
        if year is not None:
            refs = tuple(ref for ref in refs if ref.year == year)
        if month is not None:
            refs = tuple(ref for ref in refs if ref.month == month)
        if start_date is not None or end_date is not None:
            refs = tuple(
                ref
                for ref in refs
                if _ref_overlaps_date_range(
                    ref,
                    start_date=start_date,
                    end_date=end_date,
                )
            )
        return refs

    def _read_partition_latest(
        self,
        source: str,
        dataset: str,
        key: _PartitionKey,
    ) -> pd.DataFrame | None:
        catalog_path = self._partition_catalog_path(source, dataset, key)
        if not catalog_path.exists():
            return None
        payload = json.loads(catalog_path.read_text(encoding="utf-8"))
        snapshot_id = payload.get("latest_snapshot")
        if not isinstance(snapshot_id, str):
            return None
        data_path = (
            self._snapshot_dir(source, dataset, key, snapshot_id) / "data.parquet"
        )
        return (
            _read_table_parquet(data_path, table=dataset)
            if data_path.exists()
            else None
        )

    def _snapshot_ref(
        self,
        source: str,
        dataset: str,
        snapshot_id: str,
        *,
        key: _PartitionKey,
    ) -> SnapshotRef:
        snapshot_dir = self._snapshot_dir(source, dataset, key, snapshot_id)
        metadata_path = snapshot_dir / "metadata.json"
        metadata: dict[str, Any] = {}
        created_at = datetime.now(UTC)
        if metadata_path.exists():
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            raw_created_at = metadata.get("created_at")
            if isinstance(raw_created_at, str):
                created_at = datetime.fromisoformat(raw_created_at)
        return SnapshotRef(
            source=source,
            dataset=dataset,
            snapshot_id=snapshot_id,
            year=key.year,
            month=key.month,
            day=key.day,
            quarter=key.quarter,
            path=snapshot_dir,
            created_at=created_at,
            metadata=metadata,
        )

    def _write_table_catalog(
        self,
        source: str,
        dataset: str,
        snapshot_id: str,
        created_at: datetime,
        *,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        dataset_dir = self._dataset_dir(source, dataset)
        dataset_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "source": source,
            "table": dataset,
            "latest_snapshot": snapshot_id,
            "format": "parquet",
            "updated_at": created_at.isoformat(),
        }
        payload.update(dict(metadata or {}))
        self._table_catalog_path(source, dataset).write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def _write_partition_catalog(
        self,
        source: str,
        dataset: str,
        key: _PartitionKey,
        snapshot: SnapshotRef,
    ) -> None:
        partition_dir = self._partition_dir(source, dataset, key)
        partition_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "source": source,
            "table": dataset,
            "format": "parquet",
            **key.metadata(),
            "latest_snapshot": snapshot.snapshot_id,
            "updated_at": snapshot.created_at.isoformat(),
        }
        self._partition_catalog_path(source, dataset, key).write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def _remove_latest(self, source: str, dataset: str) -> None:
        catalog_path = self._table_catalog_path(source, dataset)
        if catalog_path.exists():
            catalog_path.unlink()

    def _dataset_dir(self, source: str, dataset: str) -> Path:
        return self.root / _safe_part(source) / _safe_part(dataset)

    def _partition_dir(self, source: str, dataset: str, key: _PartitionKey) -> Path:
        path = self._dataset_dir(source, dataset) / f"year={key.year:04d}"
        if key.quarter is not None:
            return path / f"quarter={key.quarter}"
        if key.month is not None:
            path = path / f"month={key.month:02d}"
        if key.day is not None:
            path = path / f"day={key.day:02d}"
        return path

    def _snapshot_dir(
        self,
        source: str,
        dataset: str,
        key: _PartitionKey,
        snapshot: str,
    ) -> Path:
        return (
            self._partition_dir(source, dataset, key)
            / "snapshots"
            / _safe_part(snapshot)
        )

    def _table_catalog_path(self, source: str, dataset: str) -> Path:
        return self._dataset_dir(source, dataset) / "_catalog.json"

    def _partition_catalog_path(
        self,
        source: str,
        dataset: str,
        key: _PartitionKey,
    ) -> Path:
        return self._partition_dir(source, dataset, key) / "_catalog.json"

    def _partition_dirs(
        self,
        source: str,
        dataset: str,
    ) -> tuple[tuple[_PartitionKey, Path], ...]:
        table_dir = self._dataset_dir(source, dataset)
        if not table_dir.exists():
            return ()
        partitions: list[tuple[_PartitionKey, Path]] = []
        for year_dir in sorted(table_dir.glob("year=*")):
            if not year_dir.is_dir():
                continue
            year = _parse_partition_number(year_dir.name, "year")
            for quarter_dir in sorted(year_dir.glob("quarter=*")):
                if quarter_dir.is_dir():
                    partitions.append(
                        (
                            _PartitionKey(
                                year=year,
                                quarter=_parse_partition_number(
                                    quarter_dir.name,
                                    "quarter",
                                ),
                            ),
                            quarter_dir,
                        )
                    )
            for month_dir in sorted(year_dir.glob("month=*")):
                if not month_dir.is_dir():
                    continue
                month = _parse_partition_number(month_dir.name, "month")
                day_dirs = [
                    path
                    for path in sorted(month_dir.glob("day=*"))
                    if path.is_dir()
                ]
                if day_dirs:
                    partitions.extend(
                        (
                            _PartitionKey(
                                year=year,
                                month=month,
                                day=_parse_partition_number(day_dir.name, "day"),
                            ),
                            day_dir,
                        )
                        for day_dir in day_dirs
                    )
                if (month_dir / "snapshots").exists() or (
                    month_dir / "_catalog.json"
                ).exists():
                    partitions.append(
                        (_PartitionKey(year=year, month=month), month_dir)
                    )
        return tuple(partitions)

    def _has_table_catalog(self, source: str, dataset: str) -> bool:
        return self._table_catalog_path(source, dataset).exists()

    def _table_catalog_metadata(self, source: str, dataset: str) -> dict[str, Any]:
        catalog_path = self._table_catalog_path(source, dataset)
        if not catalog_path.exists():
            return {}
        payload = json.loads(catalog_path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}

    def _catalog_marks_panel_field(
        self,
        source: str,
        dataset: str,
        field: str,
    ) -> bool:
        metadata = self._table_catalog_metadata(source, dataset)
        panel_fields = metadata.get("panel_fields")
        if not isinstance(panel_fields, list):
            return False
        return field in {str(item) for item in panel_fields}

    def _update_main_tables(
        self,
        source: str,
        dataset: str,
        frame: pd.DataFrame,
        *,
        created_at: datetime,
    ) -> None:
        if dataset.startswith("__"):
            return
        self._update_asset_ids(source, frame, created_at=created_at)
        self._update_field_ids(source, dataset, frame, created_at=created_at)

    def _update_asset_ids(
        self,
        source: str,
        frame: pd.DataFrame,
        *,
        created_at: datetime,
    ) -> None:
        asset_column = _infer_asset_column(frame)
        if asset_column is None:
            return
        discovered = {
            _normalize_asset_id(source, value)
            for value in frame[asset_column].dropna().astype(str).tolist()
        }
        self._merge_asset_ids(source, discovered, created_at=created_at)

    def _merge_asset_ids(
        self,
        source: str,
        discovered: set[str],
        *,
        created_at: datetime,
    ) -> None:
        existing = set(self.asset_ids(source))
        data = pd.DataFrame({"asset_id": sorted(existing.union(discovered))})
        self.write(
            source,
            "__asset_ids",
            data,
            mode="overwrite",
            metadata={
                "system_table": True,
                "updated_from": "catalog_entries",
                "created_at": created_at.isoformat(),
            },
            update_catalogs=False,
        )

    def _update_data_item_ids(
        self,
        source: str,
        dataset: str,
        frame: pd.DataFrame,
        *,
        created_at: datetime,
    ) -> None:
        self._update_field_ids(source, dataset, frame, created_at=created_at)

    def _update_field_ids(
        self,
        source: str,
        dataset: str,
        frame: pd.DataFrame,
        *,
        created_at: datetime,
    ) -> None:
        ignored = {"index", "create_time", "delete_flag"}
        discovered = {
            str(column)
            for column in frame.reset_index().columns
            if column not in ignored
        }
        self._merge_field_ids(
            source,
            dataset,
            discovered,
            created_at=created_at,
        )

    def _merge_data_item_ids(
        self,
        source: str,
        dataset: str,
        fields: set[str],
        *,
        created_at: datetime,
    ) -> None:
        self._merge_field_ids(source, dataset, fields, created_at=created_at)

    def _merge_field_ids(
        self,
        source: str,
        dataset: str,
        fields: set[str],
        *,
        created_at: datetime,
    ) -> None:
        existing = self.fields(source)
        discovered = pd.DataFrame(
            {
                "source": source,
                "table": dataset,
                "field": column,
                "field_id": f"{source}_{dataset}_{column}",
            }
            for column in fields
        )
        data = pd.concat([existing, discovered], axis=0, ignore_index=True)
        data = data.drop_duplicates("field_id").sort_values(
            ["source", "table", "field", "field_id"],
            ignore_index=True,
        )
        self.write(
            source,
            FIELD_CATALOG_TABLE,
            data,
            mode="overwrite",
            metadata={"system_table": True, "created_at": created_at.isoformat()},
            update_catalogs=False,
        )


def _snapshot_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")


def _safe_part(value: str) -> str:
    if not value or "/" in value or "\\" in value:
        raise LakeError(f"Invalid lake path component: {value!r}")
    return value


def _partition_frame(
    frame: pd.DataFrame,
    *,
    created_at: datetime,
    partition_column: str | None,
    granularity: PartitionGranularity,
) -> dict[_PartitionKey, pd.DataFrame]:
    column = partition_column or _infer_partition_column(frame)
    if column is None:
        return {_PartitionKey(created_at.year, created_at.month): frame}

    if column == "date" and frame.index.name == "date":
        partition_values = pd.Series(pd.to_datetime(frame.index), index=frame.index)
    else:
        partition_values = pd.to_datetime(frame[column].astype(str), errors="coerce")
    if partition_values.isna().any():
        raise LakeError(f"Partition column contains invalid dates: {column}")

    years = partition_values.dt.year.astype(int)
    months = partition_values.dt.month.astype(int)
    days = partition_values.dt.day.astype(int)
    quarters = partition_values.dt.quarter.astype(int)
    if granularity == "day":
        key_values = [
            _PartitionKey(year, month, day)
            for year, month, day in zip(years, months, days, strict=True)
        ]
    elif granularity == "quarter":
        key_values = [
            _PartitionKey(year, quarter=quarter)
            for year, quarter in zip(years, quarters, strict=True)
        ]
    else:
        key_values = [
            _PartitionKey(year, month)
            for year, month in zip(years, months, strict=True)
        ]

    working = frame.copy(deep=False)
    working["__partition_key"] = key_values
    partitions: dict[_PartitionKey, pd.DataFrame] = {}
    for key, partition in working.groupby("__partition_key", sort=False):
        partitions[key] = partition.drop(columns="__partition_key").copy(deep=True)
    return dict(
        sorted(
            partitions.items(),
            key=lambda item: _partition_sort_key(item[0]),
        )
    )


def _partition_sort_key(item: _PartitionKey) -> tuple[int, int, int, int]:
    return (
        item.year,
        item.quarter or 0,
        item.month or 0,
        item.day or 0,
    )


def _ref_overlaps_date_range(
    ref: SnapshotRef,
    *,
    start_date: pd.Timestamp | None,
    end_date: pd.Timestamp | None,
) -> bool:
    if ref.quarter is not None:
        month = (ref.quarter - 1) * 3 + 1
        ref_start = pd.Timestamp(date(ref.year, month, 1))
        ref_end = ref_start + pd.offsets.QuarterEnd(startingMonth=month + 2)
    elif ref.month is not None:
        ref_start = pd.Timestamp(date(ref.year, ref.month, ref.day or 1))
        ref_end = (
            ref_start
            if ref.day is not None
            else ref_start + pd.offsets.MonthEnd()
        )
    else:
        ref_start = pd.Timestamp(date(ref.year, 1, 1))
        ref_end = pd.Timestamp(date(ref.year, 12, 31))
    if start_date is not None and ref_end < start_date:
        return False
    if end_date is not None and ref_start > end_date:
        return False
    return True


def _optional_timestamp(
    value: str | date | datetime | pd.Timestamp | None,
) -> pd.Timestamp | None:
    if value is None:
        return None
    return pd.Timestamp(value)


def _read_columns_for_projection(
    *,
    requested: Sequence[str] | None,
    metadata: Mapping[str, Any],
) -> list[str] | None:
    if requested is None:
        return None
    columns = list(dict.fromkeys(str(column) for column in requested))
    for helper in (
        metadata.get("date_column"),
        metadata.get("asset_column"),
        "date",
        "trade_date",
        "f_ann_date",
    ):
        if isinstance(helper, str) and helper and helper not in columns:
            columns.append(helper)
    return columns


def _panel_read_columns(
    *,
    field: str,
    metadata: Mapping[str, Any],
) -> list[str]:
    columns = [field]
    for helper in (
        metadata.get("asset_column"),
        metadata.get("date_column"),
        "date",
        "trade_date",
        "f_ann_date",
    ):
        if isinstance(helper, str) and helper and helper not in columns:
            columns.append(helper)
    return columns


def _finalize_read_frame(
    frame: pd.DataFrame,
    *,
    columns: Sequence[str] | None,
    start_date: pd.Timestamp | None,
    end_date: pd.Timestamp | None,
) -> pd.DataFrame:
    filtered = _filter_frame_date_range(
        frame,
        start_date=start_date,
        end_date=end_date,
    )
    if columns is None:
        return filtered
    requested = [str(column) for column in columns if str(column) in filtered.columns]
    return filtered.loc[:, requested]


def _filter_frame_date_range(
    frame: pd.DataFrame,
    *,
    start_date: pd.Timestamp | None,
    end_date: pd.Timestamp | None,
) -> pd.DataFrame:
    if start_date is None and end_date is None:
        return frame
    if isinstance(frame.index, pd.DatetimeIndex):
        mask = pd.Series(True, index=range(len(frame)))
        if start_date is not None:
            mask &= frame.index.to_series(index=range(len(frame))) >= start_date
        if end_date is not None:
            mask &= frame.index.to_series(index=range(len(frame))) <= end_date
        return frame.loc[mask.to_numpy()]
    column = _infer_partition_column(frame)
    if column is None or column not in frame.columns:
        return frame
    dates = pd.to_datetime(frame[column].astype(str), errors="coerce")
    mask = pd.Series(True, index=range(len(frame)))
    if start_date is not None:
        mask &= dates.reset_index(drop=True) >= start_date
    if end_date is not None:
        mask &= dates.reset_index(drop=True) <= end_date
    return frame.loc[mask.to_numpy()]


def _project_existing_columns(
    frame: pd.DataFrame,
    columns: Sequence[str] | None,
) -> pd.DataFrame:
    if columns is None:
        return frame
    existing = [column for column in columns if column in frame.columns]
    return frame.loc[:, existing]


def _deduplicate_append_partition(frame: pd.DataFrame) -> pd.DataFrame:
    keys = [
        column
        for column in (
            "ts_code",
            "symbol",
            "asset_id",
            "code",
            "trade_date",
            "f_ann_date",
            "ann_date",
            "end_date",
            "period",
        )
        if column in frame.columns
    ]
    if frame.index.name == "date":
        working = frame.reset_index()
        if "date" not in keys:
            keys.append("date")
        return working.drop_duplicates(keys, keep="last").set_index("date").sort_index()
    if not keys:
        return frame.drop_duplicates(keep="last")
    return frame.drop_duplicates(keys, keep="last")


def _table_metadata(table: str, frame: pd.DataFrame) -> dict[str, Any]:
    date_column = _infer_partition_column(frame.reset_index())
    return {
        "asset_column": _infer_asset_column(frame),
        "columns": [str(column) for column in frame.reset_index().columns],
        "date_column": date_column,
        "panel_fields": _panel_value_fields(table, frame, date_column=date_column),
    }


def _panel_value_fields(
    table: str,
    frame: pd.DataFrame,
    *,
    date_column: str | None,
) -> list[str]:
    if (
        table.startswith("__")
        or table in {"stock_basic"}
        or date_column is None
        or _infer_asset_column(frame) is None
    ):
        return []
    fields = [
        str(column)
        for column in frame.reset_index().columns
        if _is_panel_value_field(frame, field=str(column), date_column=date_column)
    ]
    return sorted(dict.fromkeys(fields))


def _normalize_field_catalog(
    source: str,
    data: pd.DataFrame,
    lake: LocalDataLake,
) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    id_column = "field_id" if "field_id" in data.columns else "data_item_id"
    for row in data.dropna(subset=[id_column]).itertuples(index=False):
        row_values = row._asdict()
        raw_field_id = str(row_values[id_column])
        table = str(row_values["table"]) if "table" in row_values else ""
        field = str(row_values["field"]) if "field" in row_values else ""
        row_source = str(row_values["source"]) if "source" in row_values else source
        if not table or not field:
            resolved = _resolve_qualified_field(row_source, raw_field_id, lake)
            if resolved is None:
                continue
            row_source, table, field = resolved
        rows.append(
            {
                "source": row_source,
                "table": table,
                "field": field,
                "field_id": raw_field_id,
            }
        )
    return pd.DataFrame(rows, columns=["source", "table", "field", "field_id"])


def _metadata_field_catalog(
    lake: LocalDataLake,
    source: str | None = None,
) -> pd.DataFrame:
    rows: list[dict[str, str]] = []
    for row_source, dataset in lake.list_datasets(source):
        if dataset.startswith("__"):
            continue
        metadata = lake._table_catalog_metadata(row_source, dataset)
        fields = metadata.get("columns")
        if not isinstance(fields, list):
            fields = metadata.get("panel_fields")
        if not isinstance(fields, list):
            continue
        for field in fields:
            field_name = str(field)
            rows.append(
                {
                    "source": row_source,
                    "table": dataset,
                    "field": field_name,
                    "field_id": f"{row_source}_{dataset}_{field_name}",
                }
            )
    return pd.DataFrame(rows, columns=["source", "table", "field", "field_id"])


def _resolve_qualified_data_item(
    source: str,
    qualified_id: str,
    lake: LocalDataLake,
) -> tuple[str, str, str] | None:
    return _resolve_qualified_field(source, qualified_id, lake)


def _resolve_qualified_field(
    source: str,
    qualified_id: str,
    lake: LocalDataLake,
) -> tuple[str, str, str] | None:
    datasets = sorted(
        (dataset for dataset_source, dataset in lake.list_datasets(source)),
        key=len,
        reverse=True,
    )
    for dataset in datasets:
        prefix = f"{source}_{dataset}_"
        if qualified_id.startswith(prefix):
            field = qualified_id.removeprefix(prefix)
            if field:
                return source, dataset, field
    return None


def shape_panel_field(data: pd.DataFrame, *, field: str) -> pd.DataFrame:
    """Shape long lake data into a date x asset-id panel."""

    if field not in data.columns:
        raise LakeError(f"Panel field is missing from data: {field}")
    asset_column = _infer_asset_column(data)
    if asset_column is None:
        raise LakeError("Panel data is missing an asset id column")
    frame = data.reset_index()
    date_column = _infer_partition_column(frame)
    if date_column is None:
        raise LakeError("Panel data is missing a date column")
    frame[date_column] = pd.to_datetime(frame[date_column])
    panel = frame.pivot(index=date_column, columns=asset_column, values=field)
    panel.index = pd.DatetimeIndex(panel.index)
    panel.index.name = "date"
    panel.columns = panel.columns.astype(str)
    return panel.sort_index().sort_index(axis=1)


def _infer_partition_column(frame: pd.DataFrame) -> str | None:
    if frame.index.name == "date":
        return "date"
    for column in ("date", "trade_date", "f_ann_date", "datetime", "timestamp"):
        if column in frame.columns:
            return column
    return None


def _normalize_table(
    frame: pd.DataFrame,
    *,
    table: str,
    created_at: datetime,
    date_column: str | None,
) -> pd.DataFrame:
    normalized = frame.copy(deep=True)
    if not _is_panel_like_table(table, normalized, date_column):
        if "create_time" not in normalized.columns:
            normalized["create_time"] = created_at.isoformat()
        if "delete_flag" not in normalized.columns:
            normalized["delete_flag"] = False
        return normalized

    column = date_column or _infer_partition_column(normalized)
    if column == "date" and normalized.index.name == "date":
        index = pd.DatetimeIndex(pd.to_datetime(normalized.index))
    elif column is not None:
        index = pd.DatetimeIndex(pd.to_datetime(normalized[column].astype(str)))
    else:
        index = pd.DatetimeIndex([created_at.date()] * len(normalized))
    normalized.index = index
    normalized.index.name = "date"
    if "create_time" not in normalized.columns:
        normalized["create_time"] = created_at.isoformat()
    if "delete_flag" not in normalized.columns:
        normalized["delete_flag"] = False
    return normalized.sort_index()


def _normalize_parquet_types(frame: pd.DataFrame) -> pd.DataFrame:
    normalized = frame.copy(deep=True)
    for column, dtype in normalized.dtypes.items():
        if not pd.api.types.is_object_dtype(dtype):
            continue
        values = normalized[column].dropna()
        if values.empty:
            continue
        if any(isinstance(value, date | datetime | pd.Timestamp) for value in values):
            normalized[column] = normalized[column].map(
                lambda value: _date_value_to_string(value) if pd.notna(value) else value
            )
    return normalized


def _date_value_to_string(value: object) -> object:
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y%m%d")
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    if isinstance(value, date):
        return value.strftime("%Y%m%d")
    return value


def _read_table_parquet(
    path: Path,
    *,
    table: str,
    columns: Sequence[str] | None = None,
) -> pd.DataFrame:
    try:
        frame = pd.read_parquet(path, columns=columns)
    except (KeyError, ValueError, OSError):
        frame = _project_existing_columns(pd.read_parquet(path), columns)
    if _is_panel_like_table(table, frame, None):
        if isinstance(frame.index, pd.DatetimeIndex):
            frame.index.name = "date"
            return frame.sort_index()
        if "date" in frame.columns:
            frame["date"] = pd.to_datetime(frame["date"])
            frame = frame.set_index("date")
            frame.index.name = "date"
            return frame.sort_index()
    return frame


def _is_panel_like_table(
    table: str,
    frame: pd.DataFrame,
    date_column: str | None,
) -> bool:
    if table.startswith("__") or table in {"stock_basic"}:
        return False
    return date_column is not None or _infer_partition_column(frame) is not None


def _infer_asset_column(frame: pd.DataFrame) -> str | None:
    for column in ("ts_code", "symbol", "asset_id", "code"):
        if column in frame.columns:
            return column
    return None


def _is_panel_value_field(
    frame: pd.DataFrame,
    *,
    field: str,
    date_column: str,
) -> bool:
    asset_column = _infer_asset_column(frame)
    ignored = {
        "index",
        "date",
        date_column,
        "trade_date",
        "f_ann_date",
        "datetime",
        "timestamp",
        "create_time",
        "delete_flag",
    }
    if asset_column is not None:
        ignored.add(asset_column)
    return field not in ignored


def _normalize_asset_id(source: str, asset_id: object) -> str:
    value = str(asset_id)
    prefix = f"{source}_"
    return value if value.startswith(prefix) else f"{prefix}{value}"


def _parse_partition_number(value: str, prefix: str) -> int:
    expected = f"{prefix}="
    if not value.startswith(expected):
        raise LakeError(f"Invalid partition directory: {value}")
    return int(value.removeprefix(expected))


def _request_payload(request: DataRequest) -> dict[str, Any]:
    return {
        "dataset": request.dataset,
        "fields": list(request.fields),
        "filters": dict(request.filters),
        "start_date": request.start_date,
        "end_date": request.end_date,
        "version": request.version,
        "snapshot": request.snapshot,
        "options": dict(request.options),
    }
