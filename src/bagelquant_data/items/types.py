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
    Description is retained prose; all other fields, including arbitrary
    metadata, participate in the numerical definition identity.
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
    completed_through: date | None = None

    def read(self, key: str) -> pl.DataFrame:
        return self.frames[key].clone()


Producer: TypeAlias = Callable[[BuildContext], pl.DataFrame | pl.LazyFrame]


@dataclass(frozen=True, slots=True)
class ProducerUpdatePolicy:
    """Runtime proof of causal append and historical change propagation.

    A producer declaring this contract must not use future observations to
    change earlier outputs. ``same_date`` is pointwise in observation time;
    ``forward`` permits changes to propagate through the following suffix.
    This execution declaration does not change the numerical definition.

    ``general_asset_local_inputs`` additionally promises each General lookup
    affects only the output row with that nonnull asset_id. Cross-asset/global
    effects and effects of NULL-key General rows on nonnull outputs are excluded.
    It requires consumed asset_id, same_date and complete output anchors. Only
    explicitly prepared pure additions outside every completed anchor can reuse
    history; this declaration alone supplies no selected-input impact proof.
    """

    historical_impact: str = "forward"
    history_observations: int | None = None
    calendar_input: str | None = None
    calendar_field: str = "time"
    calendar_selector: tuple[str, int] | None = None
    timing_neutral_inputs: tuple[str, ...] = ()
    output_anchors: tuple[str, ...] = ()
    general_fields: tuple[tuple[str, tuple[str, ...]], ...] = ()
    general_asset_local_inputs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.historical_impact not in {"same_date", "forward"}:
            raise ValueError("historical_impact must be same_date or forward")
        if self.history_observations is not None and (self.history_observations < 0 or not self.calendar_input):
            raise ValueError("Finite observation history requires a nonnegative count and a calendar input")
        object.__setattr__(self, "timing_neutral_inputs", tuple(self.timing_neutral_inputs))
        if (not isinstance(self.general_fields, tuple)
                or any(not isinstance(declaration, tuple) or len(declaration) != 2
                    or not isinstance(declaration[0], str) or not declaration[0]
                    or not isinstance(declaration[1], tuple) or not declaration[1]
                    or any(not isinstance(name, str) or not name.strip()
                        or name.startswith("_") or name in {"year", "month", "ingested_at"}
                        for name in declaration[1])
                    or len(set(declaration[1])) != len(declaration[1]) for declaration in self.general_fields)
                or len({declaration[0] for declaration in self.general_fields}) != len(self.general_fields)):
            raise ValueError("general_fields requires immutable unique alias/consumed-field tuples")
        if (not isinstance(self.output_anchors, tuple)
                or any(not isinstance(alias, str) or not alias for alias in self.output_anchors)
                or len(set(self.output_anchors)) != len(self.output_anchors)
                or self.output_anchors and self.historical_impact != "same_date"):
            raise ValueError("Output anchors require unique named same-date coordinate inputs")
        if (not isinstance(self.general_asset_local_inputs, tuple)
                or any(not isinstance(alias, str) or not alias for alias in self.general_asset_local_inputs)
                or len(set(self.general_asset_local_inputs)) != len(self.general_asset_local_inputs)
                or self.general_asset_local_inputs and (self.historical_impact != "same_date"
                    or not self.output_anchors or any("asset_id" not in dict(self.general_fields).get(alias, ())
                        for alias in self.general_asset_local_inputs))):
            raise ValueError("Asset-local General inputs require same_date, consumed asset_id and output anchors")


@dataclass(frozen=True, slots=True)
class ItemPublication:
    """Historical versions followed by an optional complete-range replacement."""
    name: str
    frame: pl.DataFrame
    expected_definition_hash: str | None = None
    start: DateLike | None = None
    end: DateLike | None = None
    complete_at: DateLike | None = None
    reuse_build_id: int | None = None
    affected_ranges: tuple[tuple[DateLike, DateLike], ...] | None = None

    def __post_init__(self) -> None:
        if not self.name or not isinstance(self.frame, pl.DataFrame):
            raise ValueError("ItemPublication requires an item name and DataFrame")
        if (self.start is None) != (self.end is None) or (self.start is None) != (self.complete_at is None):
            raise ValueError("Complete publication requires start, end and complete_at together")
        if (self.reuse_build_id is None) != (self.affected_ranges is None):
            raise ValueError("Sparse publication requires both original completion and affected ranges")
        if self.reuse_build_id is not None and (self.start is None or self.reuse_build_id < 1):
            raise ValueError("Sparse publication requires a complete range and positive original build ID")


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


@dataclass(frozen=True, slots=True)
class ItemCompletionAttestation:
    """Explicit caller authority for an original whole-range producer completion.

    A row-only ingest or byte audit cannot supply this declaration. The caller
    retains the original successful producer report, its complete output range
    (including withdrawals/empty coordinates), and every pending visibility date.
    """
    build_id: int
    name: str
    definition_hash: str
    start: date
    end: date
    input_commit: int
    result_commit: int | None
    dependency_digest: str
    frozen_receipt_id: str
    frozen_receipt_digest: str
    information_cutoff: date | None
    report_id: str
    report_digest: str
    complete_output_range: bool
    pending_coordinates: tuple[tuple[date, date], ...]
    output_receipt_id: str | None = None
    output_receipt_digest: str | None = None

    def __post_init__(self):
        if (isinstance(self.build_id, bool) or self.build_id < 1 or not self.name
                or not self.report_id or self.complete_output_range is not True or self.end < self.start):
            raise ValueError("Completion maintenance requires an original whole-range producer attestation")
        if len(self.report_digest) != 64 or any(value not in "0123456789abcdef" for value in self.report_digest):
            raise ValueError("Completion report requires an immutable SHA256 reference")
        if not isinstance(self.pending_coordinates, tuple) or any(not isinstance(value, tuple) or len(value) != 2
                for value in self.pending_coordinates):
            raise ValueError("Pending visibility requires an immutable complete coordinate declaration")
        if (self.output_receipt_id is None) != (self.output_receipt_digest is None):
            raise ValueError("Original output receipt requires both ID and digest")
        if self.output_receipt_id is not None and (not self.output_receipt_id or len(str(self.output_receipt_digest)) != 64):
            raise ValueError("Original output receipt binding is incomplete")
        if any(not self.start <= day <= self.end or self.information_cutoff is None
               or available < day or available <= self.information_cutoff for day, available in self.pending_coordinates):
            raise ValueError("Pending coordinates must belong to the original range and mature after its cutoff")


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
