"""Neutral DataItem definitions and external producer contracts."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any, TypeAlias

import polars as pl

from bagelquant_data.core.types import DateLike
from bagelquant_data.transforms import Transform, dtype_name, transform_from_payload, transform_payload


@dataclass(frozen=True, slots=True)
class RawInput:
    source: str
    dataset: str
    alias: str | None = None
    fields: tuple[str, ...] = ()
    start: DateLike | None = None
    end: DateLike | None = None
    observation_start: DateLike | None = None
    observation_end: DateLike | None = None
    view: str = "history"
    strict: bool = False
    include_historical_baseline: bool = False

    def __post_init__(self) -> None:
        if not self.source or not self.dataset or self.source == "items":
            raise ValueError("RawInput requires a source and dataset; DataItems use ItemInput")
        if self.view not in {"history", "snapshot", "versions"}:
            raise ValueError("Input view must be history, snapshot, or versions")
        object.__setattr__(self, "fields", tuple(self.fields))

    @property
    def key(self) -> str:
        return self.alias or f"{self.source}/{self.dataset}"


@dataclass(frozen=True, slots=True)
class ItemInput:
    name: str
    alias: str | None = None
    start: DateLike | None = None
    end: DateLike | None = None
    view: str = "history"
    strict: bool = False

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("ItemInput requires an item name")
        if self.view not in {"history", "snapshot", "versions"}:
            raise ValueError("Input view must be history, snapshot, or versions")

    @property
    def key(self) -> str:
        return self.alias or self.name


DataInput: TypeAlias = RawInput | ItemInput


@dataclass(frozen=True, slots=True)
class DataItemSpec:
    """A typed daily T×N long table built from neutral inputs.

    External producers are registered by key/revision at runtime. Their stored
    definition contains identities and dependencies, never executable pickles.
    Without a producer, transforms operate on the first declared input.
    """
    name: str
    inputs: tuple[DataInput, ...] = ()
    value_dtype: str | pl.DataType | type[pl.DataType] = "float64"
    time_column: str = "time"
    asset_column: str = "asset_id"
    value_column: str = "value"
    transforms: tuple[Transform, ...] = ()
    producer_key: str | None = None
    producer_revision: str | None = None
    description: str = ""
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.name or self.name in {".", ".."} or any(value in self.name for value in ("/", "\\", ":")):
            raise ValueError("DataItem name must be a non-empty stable path component")
        object.__setattr__(self, "value_dtype", dtype_name(self.value_dtype))
        object.__setattr__(self, "inputs", tuple(self.inputs))
        object.__setattr__(self, "transforms", tuple(self.transforms))
        if bool(self.producer_key) != bool(self.producer_revision):
            raise ValueError("producer_key and producer_revision must be declared together")
        aliases = [value.key for value in self.inputs]
        if len(aliases) != len(set(aliases)):
            raise ValueError("DataItem input aliases must be unique")


@dataclass(frozen=True, slots=True)
class BuildContext:
    """Frozen dependency frames at one information boundary."""
    frames: Mapping[str, pl.DataFrame]
    start: date
    end: date
    information_cutoff: date
    max_commit: int
    historical_baseline: bool = False

    def read(self, key: str) -> pl.DataFrame:
        return self.frames[key].clone()


Producer: TypeAlias = Callable[[BuildContext], pl.DataFrame | pl.LazyFrame]


@dataclass(frozen=True, slots=True)
class ItemBuildReport:
    name: str
    status: str
    rows_committed: int
    commit_seq: int | None
    max_input_commit: int
    start: date
    end: date
    dependency_digest: str
    frozen_receipt_id: str | None = None


def input_payload(value: DataInput) -> dict[str, Any]:
    return {"kind": type(value).__name__, **asdict(value)}


def input_from_payload(value: Mapping[str, Any]) -> DataInput:
    payload = dict(value)
    kind = payload.pop("kind")
    if kind == "RawInput":
        payload["fields"] = tuple(payload.get("fields", ()))
        return RawInput(**payload)
    if kind == "ItemInput":
        return ItemInput(**payload)
    raise ValueError(f"Unknown DataItem input kind: {kind}")


def spec_payload(spec: DataItemSpec) -> dict[str, Any]:
    return {
        "name": spec.name, "inputs": [input_payload(value) for value in spec.inputs],
        "value_dtype": str(spec.value_dtype), "time_column": spec.time_column,
        "asset_column": spec.asset_column, "value_column": spec.value_column,
        "transforms": [transform_payload(value) for value in spec.transforms],
        "producer_key": spec.producer_key, "producer_revision": spec.producer_revision,
        "description": spec.description, "metadata": dict(spec.metadata),
    }


def spec_from_payload(value: Mapping[str, Any]) -> DataItemSpec:
    payload = dict(value)
    payload["inputs"] = tuple(input_from_payload(item) for item in payload.get("inputs", ()))
    payload["transforms"] = tuple(transform_from_payload(item) for item in payload.get("transforms", ()))
    return DataItemSpec(**payload)
