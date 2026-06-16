"""Public DataLake facade."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from bagelquant_data.core.dataset import DatasetSpec
from bagelquant_data.core.exceptions import ConfigurationError, DatasetNotFoundError
from bagelquant_data.core.registry import FrameworkRegistries, default_registries
from bagelquant_data.core.request import RequestContext
from bagelquant_data.finance import FinanceFacade
from bagelquant_data.management.datasets import DatasetManager
from bagelquant_data.management.sources import SourceManager
from bagelquant_data.management.status import StatusManager
from bagelquant_data.pipeline.ingest import IngestionPipeline, IngestionReport
from bagelquant_data.pipeline.update import UpdateReport, combine_reports, update_dataset
from bagelquant_data.query import QueryFacade
from bagelquant_data.query.raw import RawQueryService
from bagelquant_data.storage.metadata import MetadataStore
from bagelquant_data.storage.parquet import ParquetStore
from bagelquant_data.storage.paths import LakePaths
from bagelquant_data.storage.rejected import RejectedStore
from bagelquant_data.storage.staging import StagingStore


class DataLake:
    """Source-agnostic local data lake facade."""

    def __init__(self, root: str | Path, registries: FrameworkRegistries | None = None) -> None:
        self.paths = LakePaths.open(root)
        self.paths.ensure()
        self.registries = registries or default_registries()
        self.metadata = MetadataStore(self.paths.database)
        self.parquet = ParquetStore(self.paths, self.metadata)
        self.sources = SourceManager(self.registries, self.metadata)
        self.datasets = DatasetManager(self.metadata, self.paths)
        self.status = StatusManager(self.metadata)
        raw = RawQueryService(self.parquet, self.metadata)
        self.query = QueryFacade(raw)
        self.finance = FinanceFacade(raw)
        self.update = UpdateManager(self)
        self._pipeline = IngestionPipeline(
            registries=self.registries,
            parquet=self.parquet,
            metadata=self.metadata,
            staging=StagingStore(self.paths),
            rejected=RejectedStore(self.paths),
        )

    @classmethod
    def open(cls, root: str | Path = "data") -> "DataLake":
        """Open or create a local data lake."""

        return cls(root)

    def ingest_frame(self, spec: DatasetSpec, frame: pl.DataFrame) -> IngestionReport:
        """Convenience method for tests and local file adapters."""

        self.datasets.add(spec)
        return self._pipeline.ingest_frame(spec, frame, mode=spec.update_mode)


@dataclass
class UpdateManager:
    """Public update API."""

    lake: DataLake

    def dataset(self, dataset: str, *, source: str, **kwargs: Any) -> IngestionReport:
        spec = self.lake.datasets.get(dataset, source=source)
        adapter = self.lake.sources.get(source)
        if spec.request_planner == "by_asset" and not kwargs.get("assets"):
            kwargs["assets"] = self._default_assets(source)
        if spec.source == "tushare" and spec.category == "market":
            kwargs["trade_dates"] = self._trade_dates(source, start=kwargs.get("start"), end=kwargs.get("end"))
        context = _request_context(source=source, dataset=dataset, kwargs=kwargs)
        return update_dataset(spec=spec, source_adapter=adapter, pipeline=self.lake._pipeline, context=context)

    def datasets(self, datasets: list[str], *, source: str, **kwargs: Any) -> UpdateReport:
        reports = [self.dataset(dataset, source=source, **kwargs) for dataset in datasets]
        return combine_reports(source, reports)

    def source(self, source: str, **kwargs: Any) -> UpdateReport:
        names = [row["name"] for row in self.lake.datasets.list(source) if row["enabled"]]
        return self.datasets(names, source=source, **kwargs)

    def _default_assets(self, source: str) -> list[str]:
        if source != "tushare":
            raise ConfigurationError("assets=... is required for by_asset dataset updates")
        try:
            frame = self.lake.query.reference("stock_basic", source=source, collect=True)
        except DatasetNotFoundError as exc:
            raise ConfigurationError(
                "Tushare by_asset updates require an asset universe. "
                "Update/register stock_basic first or pass assets=[...] explicitly."
            ) from exc
        if isinstance(frame, pl.LazyFrame):
            frame = frame.collect()
        columns = frame.columns
        column = "asset_id" if "asset_id" in columns else "ts_code" if "ts_code" in columns else None
        if column is None:
            raise ConfigurationError("tushare/stock_basic does not contain asset_id or ts_code")
        return [str(value) for value in frame.get_column(column).drop_nulls().unique().sort().to_list()]

    def _trade_dates(self, source: str, *, start: Any, end: Any) -> list[str]:
        try:
            frame = self.lake.query.reference("trade_cal", source=source, collect=True)
        except DatasetNotFoundError as exc:
            raise ConfigurationError(
                "Tushare market updates require trade_cal. Update/register trade_cal first."
            ) from exc
        if isinstance(frame, pl.LazyFrame):
            frame = frame.collect()
        if frame.is_empty():
            raise ConfigurationError("tushare/trade_cal is empty; update trade_cal before market datasets")
        column = "time" if "time" in frame.columns else "cal_date" if "cal_date" in frame.columns else None
        if column is None:
            raise ConfigurationError("tushare/trade_cal does not contain time or cal_date")
        dates = frame.with_columns(_calendar_date_expr(column).alias("_calendar_date"))
        if start is not None:
            dates = dates.filter(pl.col("_calendar_date") >= _date_literal(start))
        if end is not None:
            dates = dates.filter(pl.col("_calendar_date") <= _date_literal(end))
        if "is_open" in dates.columns:
            dates = dates.filter(pl.col("is_open").cast(pl.Int8, strict=False) == 1)
        result = [
            value.strftime("%Y-%m-%d")
            for value in dates.select("_calendar_date").drop_nulls().unique().sort("_calendar_date").to_series().to_list()
        ]
        if not result:
            raise ConfigurationError(
                f"tushare/trade_cal has no open trading days between {start} and {end}"
            )
        return result


def _request_context(source: str, dataset: str, kwargs: dict[str, Any]) -> RequestContext:
    known = {
        "start": kwargs.pop("start", None),
        "end": kwargs.pop("end", None),
        "assets": kwargs.pop("assets", None),
    }
    workers = kwargs.pop("workers", None)
    batch_size = kwargs.pop("batch_size", None)
    source_options = kwargs.pop("source_options", None)
    progress = kwargs.pop("progress", None)
    max_retries = kwargs.pop("max_retries", None)
    retry_backoff_seconds = kwargs.pop("retry_backoff_seconds", None)
    trade_dates = kwargs.pop("trade_dates", None)
    if kwargs:
        keys = ", ".join(sorted(kwargs))
        raise ConfigurationError(f"Unsupported update option(s): {keys}")
    options: dict[str, Any] = {}
    if workers is not None:
        options["workers"] = workers
    if batch_size is not None:
        options["batch_size"] = batch_size
    if source_options is not None:
        options["source_options"] = source_options
    if progress is not None:
        options["progress"] = progress
    if max_retries is not None:
        options["max_retries"] = max_retries
    if retry_backoff_seconds is not None:
        options["retry_backoff_seconds"] = retry_backoff_seconds
    if trade_dates is not None:
        options["trade_dates"] = trade_dates
    return RequestContext(source=source, dataset=dataset, options=options, **known)


def _calendar_date_expr(column: str) -> pl.Expr:
    return (
        pl.when(pl.col(column).cast(pl.String).str.len_chars() == 8)
        .then(pl.col(column).cast(pl.String).str.strptime(pl.Date, "%Y%m%d", strict=False))
        .otherwise(pl.col(column).cast(pl.Date, strict=False))
    )


def _date_literal(value: Any) -> pl.Expr:
    if hasattr(value, "strftime"):
        return pl.lit(value).cast(pl.Date, strict=False)
    text = str(value)
    if "T" in text:
        text = text.split("T", maxsplit=1)[0]
    return pl.lit(datetime.strptime(text[:10], "%Y-%m-%d").date())
