"""Loader interfaces and panel agreements."""

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
    DomainSpec,
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
class PanelInputAgreement:
    """Data package ready to become a bagelquant-core Panel input."""

    kind: PanelKind
    frame: pd.DataFrame
    domain_spec: DomainSpec
    dataset_name: str
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self._validate_frame()

    def to_payload(self) -> dict[str, Any]:
        """Return plain objects for downstream package adapters."""

        return {
            "kind": self.kind,
            "frame": self.frame.copy(deep=True),
            "domain": self.domain_spec.to_core_kwargs(),
            "dataset_name": self.dataset_name,
            "metadata": dict(self.metadata),
        }

    def _validate_frame(self) -> None:
        if not isinstance(self.frame, pd.DataFrame):
            raise ContractValidationError("panel agreement frame must be a DataFrame")
        if self.frame.index.nlevels != 1 or self.frame.columns.nlevels != 1:
            raise ContractValidationError("panel frame must have 1D index and columns")
        if self.frame.index.has_duplicates or self.frame.columns.has_duplicates:
            raise ContractValidationError(
                "panel frame index and columns must be unique"
            )
        if self.kind == "numeric_panel":
            numeric_columns = self.frame.select_dtypes(include="number").columns
            if len(numeric_columns) != len(self.frame.columns):
                raise ContractValidationError(
                    "numeric panel frame must be fully numeric"
                )


@dataclass(frozen=True, slots=True)
class LoadedDataset:
    """Standard loader output."""

    data: pd.DataFrame
    identity: DatasetIdentity
    schema: DatasetSchema | None = None
    lineage: tuple[LineageRecord, ...] = ()
    contract: DataContract | None = None
    panel_agreement: PanelInputAgreement | None = None
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
                data = self._lake.read(source.name, dataset, snapshot=snapshot)
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
        region: str,
        kind: PanelKind = "numeric_panel",
        filters: Mapping[str, Any] | None = None,
        options: Mapping[str, Any] | None = None,
        dataset_name: str | None = None,
        refresh: bool = False,
    ) -> PanelInputAgreement:
        """Load and shape a dataset as a panel-ready agreement."""

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
        agreement = PanelInputAgreement(
            kind=kind,
            frame=frame,
            domain_spec=DomainSpec(
                region=region,
                universe=requested_universe,
                start_date=start_date,
                end_date=end_date,
            ),
            dataset_name=dataset_name or f"{source.name}.{dataset}.{field}",
            metadata={
                **loaded.metadata,
                "field": field,
                "panel_kind": kind,
            },
        )
        return agreement

    def load_panel_field(
        self,
        qualified_id: str,
        *,
        start_date: Any,
        end_date: Any,
        region: str,
        kind: PanelKind = "numeric_panel",
        universe: Sequence[Any] | pd.DataFrame = (),
        dataset_name: str | None = None,
    ) -> PanelInputAgreement:
        """Load a qualified lake field id as a panel-ready agreement."""

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
        else:
            requested_universe = tuple(str(column) for column in frame.columns)
        return PanelInputAgreement(
            kind=kind,
            frame=frame,
            domain_spec=DomainSpec(
                region=region,
                universe=requested_universe,
                start_date=start_date,
                end_date=end_date,
            ),
            dataset_name=dataset_name or f"{source_name}.{dataset}.{field}",
            metadata={
                "provider": source_name,
                "dataset": dataset,
                "origin": "lake",
                "field": field,
                "qualified_id": qualified_id,
                "panel_kind": kind,
            },
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
