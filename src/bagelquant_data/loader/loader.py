"""Loader interfaces and neutral panel retrieval results."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

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
    data: pd.DataFrame
    universe: tuple[Any, ...] | pd.DataFrame
    calendar: pd.DatetimeIndex
    dataset_name: str
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._validate_data()
        self._validate_calendar()

    def as_dict(self) -> dict[str, Any]:
        """Return defensive plain-object copies for user code."""

        return {
            "kind": self.kind,
            "data": self.data.copy(deep=True),
            "universe": _copy_universe(self.universe),
            "calendar": self.calendar.copy(),
            "dataset_name": self.dataset_name,
            "metadata": dict(self.metadata),
        }

    def _validate_data(self) -> None:
        if not isinstance(self.data, pd.DataFrame):
            raise ContractValidationError("retrieved panel data must be a DataFrame")
        if self.data.index.nlevels != 1 or self.data.columns.nlevels != 1:
            raise ContractValidationError("panel data must have 1D index and columns")
        if self.data.index.has_duplicates or self.data.columns.has_duplicates:
            raise ContractValidationError(
                "panel data index and columns must be unique"
            )
        if self.kind == "numeric_panel":
            numeric_columns = self.data.select_dtypes(include="number").columns
            if len(numeric_columns) != len(self.data.columns):
                raise ContractValidationError(
                    "numeric panel data must be fully numeric"
                )

    def _validate_calendar(self) -> None:
        _normalize_calendar(self.calendar)


@dataclass(frozen=True, slots=True)
class LoadedDataset:
    """Standard loader output."""

    data: pd.DataFrame
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
        """Return a loader bound to a named source."""

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
        """Load a dataset, preferring the local lake when configured."""

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
                    data=data,
                    source_name=source.name,
                    request=request,
                    origin="lake",
                )
            except DatasetNotFoundError:
                pass

        data = _normalize_loaded_output(dataset, source.read(request))
        if self._lake is not None and persist:
            self._lake.write(
                source.name,
                dataset,
                data,
                mode="overwrite",
                metadata={"request": _request_metadata(request)},
            )
        return self._loaded_dataset(
            data=data,
            source_name=source.name,
            request=request,
            origin="provider",
        )

    def _loaded_dataset(
        self,
        *,
        data: pd.DataFrame,
        source_name: str,
        request: DataRequest,
        origin: str,
    ) -> LoadedDataset:
        metadata = {
            "provider": source_name,
            "dataset": request.dataset,
            "origin": origin,
            "request": _request_metadata(request),
        }
        return LoadedDataset(
            data=data.copy(deep=True),
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

    def load_panel(
        self,
        dataset: str,
        *,
        field: str,
        universe: Sequence[Any] | pd.DataFrame,
        start_date: Any,
        end_date: Any,
        kind: PanelKind = "numeric_panel",
        calendar: Sequence[Any] | pd.DatetimeIndex | None = None,
        calendar_dataset: str = "trade_cal",
        filters: Mapping[str, Any] | None = None,
        options: Mapping[str, Any] | None = None,
        dataset_name: str | None = None,
        refresh: bool = False,
    ) -> RetrievedPanel:
        """Load and shape a dataset, universe, and calendar as plain objects."""

        source = self._source()
        requested_universe = normalize_universe(universe)
        request_filters = dict(filters or {})
        if source.name == "tushare" and dataset == "daily":
            request_filters.setdefault("ts_code", _tushare_codes(requested_universe))

        loaded = self.load(
            dataset,
            fields=(),
            filters=request_filters,
            start_date=start_date,
            end_date=end_date,
            options=options,
            refresh=refresh,
        )
        frame = _shape_panel(
            data=loaded.data,
            field=field,
        )
        frame = _filter_panel_dates(frame, start_date=start_date, end_date=end_date)
        panel_calendar = self._load_calendar(
            start_date=start_date,
            end_date=end_date,
            calendar=calendar,
            calendar_dataset=calendar_dataset,
        )
        retrieved = RetrievedPanel(
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
        return retrieved

    def load_panel_field(
        self,
        qualified_id: str,
        *,
        start_date: Any,
        end_date: Any,
        universe: Sequence[Any] | pd.DataFrame,
        kind: PanelKind = "numeric_panel",
        calendar: Sequence[Any] | pd.DatetimeIndex | None = None,
        calendar_dataset: str = "trade_cal",
        dataset_name: str | None = None,
    ) -> RetrievedPanel:
        """Load a qualified lake field id, universe, and calendar."""

        if self._lake is None:
            raise DataSourceError("load_panel_field requires a configured lake")
        resolved = self._lake.resolve_panel_field(qualified_id)
        if resolved is None:
            raise DatasetNotFoundError(f"No panel field: {qualified_id}")
        source_name, dataset, field = resolved
        requested_universe = normalize_universe(universe)
        frame = self._lake.read_panel_field(
            qualified_id,
            start_date=start_date,
            end_date=end_date,
        )
        if isinstance(requested_universe, pd.DataFrame):
            universe_columns = tuple(
                str(column) for column in requested_universe.columns
            )
            if universe_columns:
                frame = frame.reindex(columns=universe_columns)
        elif len(requested_universe) > 0:
            frame = frame.reindex(columns=[str(item) for item in requested_universe])
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

    def _load_calendar(
        self,
        *,
        start_date: Any,
        end_date: Any,
        calendar: Sequence[Any] | pd.DatetimeIndex | None,
        calendar_dataset: str,
        source_name: str | None = None,
    ) -> pd.DatetimeIndex:
        if calendar is not None:
            return _filter_calendar(
                _normalize_calendar(calendar),
                start_date=start_date,
                end_date=end_date,
            )

        source = None
        resolved_source_name = source_name or self._source_name
        if resolved_source_name is None:
            source = self._source()
            resolved_source_name = source.name
        if self._lake is not None:
            try:
                calendar_data = self._lake.read(
                    resolved_source_name,
                    calendar_dataset,
                )
                return _calendar_from_table(
                    calendar_data,
                    start_date=start_date,
                    end_date=end_date,
                )
            except DatasetNotFoundError:
                pass

        if source is None:
            source = self._registry.resolve(resolved_source_name)
        if not source.exists(calendar_dataset):
            raise DataSourceError(
                f"No calendar available for source {resolved_source_name!r}; "
                "pass calendar=... or load a calendar table first"
            )
        calendar_data = _normalize_loaded_output(
            calendar_dataset,
            source.read(
                DataRequest(
                    dataset=calendar_dataset,
                    start_date=start_date,
                    end_date=end_date,
                )
            ),
        )
        return _calendar_from_table(
            calendar_data,
            start_date=start_date,
            end_date=end_date,
        )

    def _source(self):
        if self._source_name is None:
            raise DataSourceError("Loader source is not selected")
        return self._registry.resolve(self._source_name)


def _shape_panel(
    *,
    data: pd.DataFrame,
    field: str,
) -> pd.DataFrame:
    try:
        return shape_panel_field(data, field=field)
    except Exception as exc:
        raise ContractValidationError(str(exc)) from exc


def _filter_panel_dates(
    frame: pd.DataFrame,
    *,
    start_date: Any,
    end_date: Any,
) -> pd.DataFrame:
    return frame.loc[
        (frame.index >= pd.Timestamp(start_date))
        & (frame.index <= pd.Timestamp(end_date))
    ]


def _copy_universe(
    universe: tuple[Any, ...] | pd.DataFrame,
) -> list[Any] | pd.DataFrame:
    if isinstance(universe, pd.DataFrame):
        return universe.copy(deep=True)
    return list(universe)


def _normalize_calendar(
    calendar: Sequence[Any] | pd.DatetimeIndex,
) -> pd.DatetimeIndex:
    sessions = pd.DatetimeIndex(pd.to_datetime(pd.Index(calendar)))
    if sessions.tz is not None:
        sessions = sessions.tz_localize(None)
    sessions = sessions.normalize().as_unit("ns")
    if sessions.empty:
        raise ContractValidationError("calendar must contain at least one session")
    if sessions.has_duplicates:
        raise ContractValidationError("calendar sessions must be unique")
    if sessions.hasnans:
        raise ContractValidationError("calendar sessions must be valid dates")
    if not sessions.is_monotonic_increasing:
        raise ContractValidationError("calendar sessions must be sorted ascending")
    return sessions


def _filter_calendar(
    calendar: pd.DatetimeIndex,
    *,
    start_date: Any,
    end_date: Any,
) -> pd.DatetimeIndex:
    start = pd.Timestamp(start_date).normalize()
    end = pd.Timestamp(end_date).normalize()
    if start > end:
        raise ContractValidationError("start_date must not be after end_date")
    filtered = calendar[(calendar >= start) & (calendar <= end)]
    if filtered.empty:
        raise ContractValidationError("calendar has no sessions in requested range")
    return filtered


def _calendar_from_table(
    data: pd.DataFrame,
    *,
    start_date: Any,
    end_date: Any,
) -> pd.DatetimeIndex:
    frame = data.copy(deep=True)
    if "is_open" in frame.columns:
        frame = frame.loc[frame["is_open"].map(_is_open_calendar_value)]
    date_column = _infer_calendar_date_column(frame)
    if date_column is None:
        if isinstance(frame.index, pd.DatetimeIndex):
            sessions = _normalize_calendar(frame.index)
        else:
            raise ContractValidationError(
                "calendar table must include a date column or DatetimeIndex"
            )
    else:
        sessions = _normalize_calendar(frame[date_column].astype(str))
    return _filter_calendar(sessions, start_date=start_date, end_date=end_date)


def _infer_calendar_date_column(frame: pd.DataFrame) -> str | None:
    for column in ("cal_date", "trade_date", "date", "datetime", "timestamp"):
        if column in frame.columns:
            return column
    return None


def _is_open_calendar_value(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value == 1
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y", "open"}


def _normalize_loaded_output(dataset: str, data: pd.DataFrame) -> pd.DataFrame:
    frame = data.copy(deep=True)
    if dataset in {"stock_basic"} or dataset.startswith("__"):
        return frame
    if frame.index.name == "date":
        frame.index = pd.DatetimeIndex(pd.to_datetime(frame.index))
        frame.index.name = "date"
        return frame.sort_index()
    date_column = _infer_date_column(frame)
    if date_column is None:
        return frame
    frame.index = pd.DatetimeIndex(pd.to_datetime(frame[date_column].astype(str)))
    frame.index.name = "date"
    return frame.sort_index()


def _infer_date_column(frame: pd.DataFrame) -> str | None:
    if frame.index.name == "date":
        return "date"
    for column in ("date", "trade_date", "f_ann_date", "datetime", "timestamp"):
        if column in frame.columns:
            return column
    return None


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


def _tushare_codes(universe: tuple[Any, ...] | pd.DataFrame) -> str:
    if isinstance(universe, pd.DataFrame):
        return ",".join(str(code) for code in universe.columns)
    return ",".join(str(code) for code in universe)
