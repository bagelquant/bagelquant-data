"""Loader interfaces and neutral panel retrieval results."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import polars as pl

from bagelquant_data.datasource.base import DataRequest
from bagelquant_data.datasource.registry import DataSourceRegistry, default_registry
from bagelquant_data.lake.local import LocalDataLake, shape_panel_field
from bagelquant_data.metadata.contract import (
    DataContract,
    DatasetIdentity,
    PanelKind,
    normalize_universe,
)
from bagelquant_data.metadata.lineage import LineageRecord
from bagelquant_data.metadata.schema import DatasetSchema
from bagelquant_data.utils.exceptions import (
    ContractValidationError,
    DatasetNotFoundError,
    DataSourceError,
)


@dataclass(frozen=True, slots=True)
class RetrievedPanel:
    """Plain data-layer panel retrieval result."""

    kind: PanelKind
    data: pl.DataFrame
    universe: tuple[Any, ...] | pl.DataFrame
    calendar: pl.Series
    dataset_name: str
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._validate_data()
        _normalize_calendar(self.calendar)

    def as_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "data": self.data.clone(),
            "universe": _copy_universe(self.universe),
            "calendar": self.calendar.clone(),
            "dataset_name": self.dataset_name,
            "metadata": dict(self.metadata),
        }

    def _validate_data(self) -> None:
        if not isinstance(self.data, pl.DataFrame):
            raise ContractValidationError(
                "retrieved panel data must be a Polars DataFrame"
            )
        missing = {"time", "asset_id", "value"} - set(self.data.columns)
        if missing:
            raise ContractValidationError(
                f"panel data missing columns: {sorted(missing)}"
            )
        if self.data.select(pl.struct("time", "asset_id").is_duplicated().any()).item():
            raise ContractValidationError(
                "panel data must be unique by (time, asset_id)"
            )
        if self.kind == "numeric_panel" and not self.data.schema["value"].is_numeric():
            raise ContractValidationError("numeric panel value column must be numeric")


@dataclass(frozen=True, slots=True)
class LoadedDataset:
    """Standard loader output."""

    data: pl.DataFrame
    identity: DatasetIdentity
    schema: DatasetSchema | None = None
    lineage: tuple[LineageRecord, ...] = ()
    contract: DataContract | None = None
    retrieved_panel: RetrievedPanel | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


class Loader:
    """Coordinate data access without owning provider business logic."""

    def __init__(
        self,
        *,
        registry: DataSourceRegistry | None = None,
        lake: LocalDataLake | None = None,
        source_name: str | None = None,
    ) -> None:
        self._registry = registry or default_registry
        self._lake = lake
        self._source_name = source_name

    def source(self, name: str) -> Loader:
        return Loader(registry=self._registry, lake=self._lake, source_name=name)

    def load(
        self,
        dataset: str,
        *,
        fields: Sequence[str] = (),
        filters: Mapping[str, Any] | None = None,
        start_date: Any | None = None,
        end_date: Any | None = None,
        version: str | None = None,
        snapshot: str | None = None,
        options: Mapping[str, Any] | None = None,
        refresh: bool = False,
        persist: bool = True,
    ) -> LoadedDataset:
        source = self._source()
        request = DataRequest(
            dataset=dataset,
            fields=tuple(fields),
            filters=filters or {},
            start_date=start_date,
            end_date=end_date,
            version=version,
            snapshot=snapshot,
            options=options or {},
        )
        if self._lake is not None and not refresh:
            try:
                data = self._lake.read(
                    source.name,
                    dataset,
                    snapshot=snapshot,
                    columns=fields or None,
                    start_date=start_date,
                    end_date=end_date,
                )
                return self._loaded_dataset(
                    data=data, source_name=source.name, request=request, origin="lake"
                )
            except DatasetNotFoundError:
                pass

        data = _normalize_loaded_output(source.read(request))
        if self._lake is not None and persist:
            self._lake.write(
                source.name,
                dataset,
                data,
                mode="overwrite",
                metadata={"request": _request_metadata(request)},
            )
        return self._loaded_dataset(
            data=data, source_name=source.name, request=request, origin="provider"
        )

    def load_panel(
        self,
        dataset: str,
        *,
        field: str,
        universe: Sequence[Any] | pl.DataFrame,
        start_date: Any,
        end_date: Any,
        kind: PanelKind = "numeric_panel",
        calendar: Sequence[Any] | pl.Series | None = None,
        calendar_dataset: str = "trade_cal",
        filters: Mapping[str, Any] | None = None,
        options: Mapping[str, Any] | None = None,
        dataset_name: str | None = None,
        refresh: bool = False,
    ) -> RetrievedPanel:
        source = self._source()
        requested_universe = normalize_universe(universe)
        request_filters = dict(filters or {})
        if source.name == "tushare" and dataset == "daily":
            request_filters.setdefault("asset_id", _asset_ids(requested_universe))
        loaded = self.load(
            dataset,
            filters=request_filters,
            start_date=start_date,
            end_date=end_date,
            options=options,
            refresh=refresh,
        )
        frame = shape_panel_field(loaded.data, field=field)
        frame = _filter_universe(frame, requested_universe)
        panel_calendar = self._load_calendar(
            start_date=start_date,
            end_date=end_date,
            calendar=calendar,
            calendar_dataset=calendar_dataset,
        )
        return RetrievedPanel(
            kind=kind,
            data=frame,
            universe=requested_universe,
            calendar=panel_calendar,
            dataset_name=dataset_name or f"{source.name}.{dataset}.{field}",
            metadata={
                **loaded.metadata,
                "field": field,
                "panel_kind": kind,
                "calendar_dataset": calendar_dataset,
            },
        )

    def load_panel_field(
        self,
        qualified_id: str,
        *,
        start_date: Any,
        end_date: Any,
        universe: Sequence[Any] | pl.DataFrame,
        kind: PanelKind = "numeric_panel",
        calendar: Sequence[Any] | pl.Series | None = None,
        calendar_dataset: str = "trade_cal",
        dataset_name: str | None = None,
    ) -> RetrievedPanel:
        if self._lake is None:
            raise DataSourceError("load_panel_field requires a configured lake")
        resolved = self._lake.resolve_panel_field(qualified_id)
        if resolved is None:
            raise DatasetNotFoundError(f"No panel field: {qualified_id}")
        source_name, dataset, field = resolved
        requested_universe = normalize_universe(universe)
        frame = _filter_universe(
            self._lake.read_panel_field(
                qualified_id, start_date=start_date, end_date=end_date
            ),
            requested_universe,
        )
        panel_calendar = self._load_calendar(
            start_date=start_date,
            end_date=end_date,
            calendar=calendar,
            calendar_dataset=calendar_dataset,
            source_name=source_name,
        )
        return RetrievedPanel(
            kind=kind,
            data=frame,
            universe=requested_universe,
            calendar=panel_calendar,
            dataset_name=dataset_name or f"{source_name}.{dataset}.{field}",
            metadata={
                "provider": source_name,
                "dataset": dataset,
                "origin": "lake",
                "field": field,
                "qualified_id": qualified_id,
                "panel_kind": kind,
                "calendar_dataset": calendar_dataset,
            },
        )

    def _loaded_dataset(
        self, *, data: pl.DataFrame, source_name: str, request: DataRequest, origin: str
    ) -> LoadedDataset:
        metadata = {
            "provider": source_name,
            "dataset": request.dataset,
            "origin": origin,
            "request": _request_metadata(request),
        }
        return LoadedDataset(
            data=data.clone(),
            identity=DatasetIdentity(
                name=request.dataset,
                provider=source_name,
                version=request.version,
                snapshot=request.snapshot,
            ),
            lineage=(
                LineageRecord(
                    source=source_name,
                    operation=f"read_{origin}",
                    parameters=metadata["request"],
                ),
            ),
            metadata=metadata,
        )

    def _load_calendar(
        self,
        *,
        start_date: Any,
        end_date: Any,
        calendar: Sequence[Any] | pl.Series | None,
        calendar_dataset: str,
        source_name: str | None = None,
    ) -> pl.Series:
        if calendar is not None:
            return _filter_calendar(
                _normalize_calendar(calendar), start_date=start_date, end_date=end_date
            )
        resolved_source_name = source_name or self._source_name
        if resolved_source_name is not None and self._lake is not None:
            try:
                return _calendar_from_table(
                    self._lake.read(resolved_source_name, calendar_dataset),
                    start_date=start_date,
                    end_date=end_date,
                )
            except DatasetNotFoundError:
                pass
        loaded = self.load(
            calendar_dataset, start_date=start_date, end_date=end_date, persist=False
        )
        return _calendar_from_table(
            loaded.data, start_date=start_date, end_date=end_date
        )

    def _source(self):
        if self._source_name is None:
            raise DataSourceError("Loader source is not selected")
        return self._registry.resolve(self._source_name)


def _normalize_loaded_output(data: pl.DataFrame) -> pl.DataFrame:
    if not isinstance(data, pl.DataFrame):
        raise DataSourceError(
            f"provider returned {type(data)!r}, expected Polars DataFrame"
        )
    return data.clone()


def _normalize_calendar(calendar: Sequence[Any] | pl.Series) -> pl.Series:
    series = (
        calendar if isinstance(calendar, pl.Series) else pl.Series("time", calendar)
    )
    normalized = series.cast(pl.Date, strict=False).sort()
    if normalized.is_empty():
        raise ContractValidationError("calendar must contain at least one time")
    if normalized.null_count() > 0:
        raise ContractValidationError("calendar contains invalid time values")
    if normalized.n_unique() != len(normalized):
        raise ContractValidationError("calendar times must be unique")
    return normalized


def _filter_calendar(
    calendar: pl.Series, *, start_date: Any, end_date: Any
) -> pl.Series:
    frame = pl.DataFrame({"time": calendar})
    return frame.filter(pl.col("time") >= pl.lit(start_date).cast(pl.Date)).filter(
        pl.col("time") <= pl.lit(end_date).cast(pl.Date)
    )["time"]


def _calendar_from_table(
    data: pl.DataFrame, *, start_date: Any, end_date: Any
) -> pl.Series:
    frame = data
    if "is_open" in frame.columns:
        frame = frame.filter(
            pl.col("is_open").cast(pl.Boolean, strict=False).fill_null(False)
        )
    if "time" not in frame.columns:
        raise ContractValidationError("calendar table must include a time column")
    return _filter_calendar(
        _normalize_calendar(frame["time"]), start_date=start_date, end_date=end_date
    )


def _filter_universe(
    frame: pl.DataFrame, universe: tuple[Any, ...] | pl.DataFrame
) -> pl.DataFrame:
    if isinstance(universe, pl.DataFrame):
        return frame.join(
            universe.filter(pl.col("active")).select("time", "asset_id"),
            on=["time", "asset_id"],
            how="inner",
        )
    if not universe:
        return frame
    return frame.filter(pl.col("asset_id").is_in([str(item) for item in universe]))


def _copy_universe(
    universe: tuple[Any, ...] | pl.DataFrame,
) -> list[Any] | pl.DataFrame:
    if isinstance(universe, pl.DataFrame):
        return universe.clone()
    return list(universe)


def _request_metadata(request: DataRequest) -> dict[str, Any]:
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


def _asset_ids(universe: tuple[Any, ...] | pl.DataFrame) -> str:
    if isinstance(universe, pl.DataFrame):
        return ",".join(str(code) for code in universe["asset_id"].unique().to_list())
    return ",".join(str(code) for code in universe)
