"""Local source-separated data lake."""

from __future__ import annotations

import json
import shutil
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from bagelquant_data.datasource.base import DataRequest, DataSource
from bagelquant_data.lake.snapshot import SnapshotRef
from bagelquant_data.utils.exceptions import DatasetNotFoundError, LakeError

WriteMode = Literal["append", "overwrite"]


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
    ) -> pd.DataFrame:
        """Read table data from selected year/month partitions."""

        refs = self._refs_for_read(
            source,
            dataset,
            year=year,
            month=month,
            snapshot=snapshot,
        )
        if not refs:
            raise DatasetNotFoundError(f"No local lake table: {source}/{dataset}")

        frames = []
        for ref in refs:
            if ref.path is None:
                continue
            data_path = ref.path / "data.csv"
            if data_path.exists():
                frames.append(_read_table_csv(data_path, table=dataset))
        if not frames:
            raise DatasetNotFoundError(f"No local lake data files: {source}/{dataset}")
        if len(frames) == 1:
            return frames[0]
        return pd.concat(frames, axis=0).sort_index()

    def write(
        self,
        source: str,
        dataset: str,
        data: pd.DataFrame,
        *,
        mode: WriteMode = "append",
        metadata: Mapping[str, Any] | None = None,
        partition_column: str | None = None,
        update_catalogs: bool = True,
    ) -> SnapshotRef:
        """Write immutable snapshots partitioned by table/year/month."""

        if mode not in {"append", "overwrite"}:
            raise LakeError("mode must be 'append' or 'overwrite'")
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
        )
        refs = []
        for (year, month), partition in partitioned.items():
            if mode == "append":
                previous = self._read_partition_latest(source, dataset, year, month)
                if previous is not None:
                    partition = pd.concat([previous, partition], axis=0)
            snapshot_dir = self._snapshot_dir(source, dataset, year, month, snapshot_id)
            snapshot_dir.mkdir(parents=True, exist_ok=False)
            partition.to_csv(
                snapshot_dir / "data.csv",
                index=partition.index.name == "date",
            )
            payload = {
                "source": source,
                "table": dataset,
                "dataset": dataset,
                "snapshot_id": snapshot_id,
                "year": year,
                "month": month,
                "created_at": created_at.isoformat(),
                "mode": mode,
                "metadata": dict(metadata or {}),
                "rows": len(partition),
                "columns": list(partition.columns),
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
                    year=year,
                    month=month,
                    path=snapshot_dir,
                    created_at=created_at,
                    metadata=payload,
                )
            )
            self._write_partition_catalog(source, dataset, year, month, refs[-1])
        self._write_table_catalog(source, dataset, snapshot_id, created_at)
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

    def data_item_ids(self, source: str) -> tuple[str, ...]:
        """Return known source data item ids."""

        try:
            data = self.read(source, "__data_item_ids")
        except DatasetNotFoundError:
            return ()
        return tuple(str(item_id) for item_id in data["data_item_id"].tolist())

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
        for year_dir in sorted(table_dir.glob("year=*")):
            for month_dir in sorted(year_dir.glob("month=*")):
                snapshot_root = month_dir / "snapshots"
                paths = snapshot_root.iterdir() if snapshot_root.exists() else ()
                for path in sorted(paths):
                    if path.is_dir():
                        refs.append(
                            self._snapshot_ref(
                                source,
                                dataset,
                                path.name,
                                year=_parse_partition_number(year_dir.name, "year"),
                                month=_parse_partition_number(month_dir.name, "month"),
                            )
                        )
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
    ) -> tuple[SnapshotRef, ...]:
        refs = self.snapshots(source, dataset)
        if snapshot is not None:
            refs = tuple(ref for ref in refs if ref.snapshot_id == snapshot)
        elif year is None and month is None:
            latest = self.latest(source, dataset)
            if latest is None:
                return ()
            refs = tuple(ref for ref in refs if ref.snapshot_id == latest.snapshot_id)
        if year is not None:
            refs = tuple(ref for ref in refs if ref.year == year)
        if month is not None:
            refs = tuple(ref for ref in refs if ref.month == month)
        return refs

    def _read_partition_latest(
        self,
        source: str,
        dataset: str,
        year: int,
        month: int,
    ) -> pd.DataFrame | None:
        catalog_path = self._partition_catalog_path(source, dataset, year, month)
        if not catalog_path.exists():
            return None
        payload = json.loads(catalog_path.read_text(encoding="utf-8"))
        snapshot_id = payload.get("latest_snapshot")
        if not isinstance(snapshot_id, str):
            return None
        data_path = (
            self._snapshot_dir(source, dataset, year, month, snapshot_id) / "data.csv"
        )
        return _read_table_csv(data_path, table=dataset) if data_path.exists() else None

    def _snapshot_ref(
        self,
        source: str,
        dataset: str,
        snapshot_id: str,
        *,
        year: int,
        month: int,
    ) -> SnapshotRef:
        snapshot_dir = self._snapshot_dir(source, dataset, year, month, snapshot_id)
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
            year=year,
            month=month,
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
    ) -> None:
        dataset_dir = self._dataset_dir(source, dataset)
        dataset_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "source": source,
            "table": dataset,
            "latest_snapshot": snapshot_id,
            "updated_at": created_at.isoformat(),
        }
        self._table_catalog_path(source, dataset).write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def _write_partition_catalog(
        self,
        source: str,
        dataset: str,
        year: int,
        month: int,
        snapshot: SnapshotRef,
    ) -> None:
        partition_dir = self._partition_dir(source, dataset, year, month)
        partition_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "source": source,
            "table": dataset,
            "year": year,
            "month": month,
            "latest_snapshot": snapshot.snapshot_id,
            "updated_at": snapshot.created_at.isoformat(),
        }
        self._partition_catalog_path(source, dataset, year, month).write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def _remove_latest(self, source: str, dataset: str) -> None:
        catalog_path = self._table_catalog_path(source, dataset)
        if catalog_path.exists():
            catalog_path.unlink()

    def _dataset_dir(self, source: str, dataset: str) -> Path:
        return self.root / _safe_part(source) / _safe_part(dataset)

    def _partition_dir(self, source: str, dataset: str, year: int, month: int) -> Path:
        return (
            self._dataset_dir(source, dataset)
            / f"year={year:04d}"
            / f"month={month:02d}"
        )

    def _snapshot_dir(
        self,
        source: str,
        dataset: str,
        year: int,
        month: int,
        snapshot: str,
    ) -> Path:
        return (
            self._partition_dir(source, dataset, year, month)
            / "snapshots"
            / _safe_part(snapshot)
        )

    def _table_catalog_path(self, source: str, dataset: str) -> Path:
        return self._dataset_dir(source, dataset) / "_catalog.json"

    def _partition_catalog_path(
        self,
        source: str,
        dataset: str,
        year: int,
        month: int,
    ) -> Path:
        return self._partition_dir(source, dataset, year, month) / "_catalog.json"

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
        self._update_data_item_ids(source, dataset, frame, created_at=created_at)

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
        existing = set(self.asset_ids(source))
        discovered = {
            _normalize_asset_id(source, value)
            for value in frame[asset_column].dropna().astype(str).tolist()
        }
        data = pd.DataFrame({"asset_id": sorted(existing.union(discovered))})
        self.write(
            source,
            "__asset_ids",
            data,
            mode="overwrite",
            metadata={
                "system_table": True,
                "updated_from": asset_column,
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
        ignored = {"index", "create_time", "delete_flag"}
        existing = set(self.data_item_ids(source))
        discovered = {
            f"{source}_{dataset}_{column}"
            for column in frame.reset_index().columns
            if column not in ignored
        }
        data = pd.DataFrame({"data_item_id": sorted(existing.union(discovered))})
        self.write(
            source,
            "__data_item_ids",
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
) -> dict[tuple[int, int], pd.DataFrame]:
    column = partition_column or _infer_partition_column(frame)
    if column is None:
        return {(created_at.year, created_at.month): frame}

    if column == "date" and frame.index.name == "date":
        partition_values = pd.Series(pd.to_datetime(frame.index), index=frame.index)
    else:
        partition_values = pd.to_datetime(frame[column].astype(str), errors="coerce")
    if partition_values.isna().any():
        raise LakeError(f"Partition column contains invalid dates: {column}")

    years = partition_values.dt.year.astype(int)
    months = partition_values.dt.month.astype(int)
    keys = sorted(set(zip(years.tolist(), months.tolist(), strict=True)))

    partitions: dict[tuple[int, int], pd.DataFrame] = {}
    for year, month in keys:
        mask = (years == year) & (months == month)
        partitions[(year, month)] = frame.loc[mask.to_numpy()].copy(deep=True)
    return partitions


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


def _read_table_csv(path: Path, *, table: str) -> pd.DataFrame:
    frame = pd.read_csv(path)
    if _is_panel_like_table(table, frame, None) and "date" in frame.columns:
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
