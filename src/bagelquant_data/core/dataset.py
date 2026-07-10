"""Dataset specification model."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from bagelquant_data.core.exceptions import DatasetSpecError


@dataclass(frozen=True, slots=True, init=False)
class DatasetSpec:
    """Declarative canonical dataset behavior."""

    name: str
    source: str = "custom"
    source_dataset: str = ""
    category: str = "generic"
    field_mapping: dict[str, str] = field(default_factory=dict)
    required_columns: tuple[str, ...] = ()
    data_kind: str = "generic"
    primary_key: tuple[str, ...] | None = None
    business_key: tuple[str, ...] | None = None
    asset_column: str | None = None
    time_column: str | None = None
    period_column: str | None = None
    request_options: dict[str, Any] = field(default_factory=dict)
    normalizer: str = "standard"
    deduplication: str = "exact_record_hash"
    update_type: str = "general"
    calendar_dataset: str | None = None
    calendar_date_column: str = "time"
    calendar_open_column: str | None = "is_open"
    id_dataset: str | None = None
    id_column: str = "asset_id"
    start_date: str | None = None
    request_date_param: str = "date"
    request_id_param: str = "id"
    batch_count: int = 32
    sort_columns: tuple[str, ...] = ()
    point_in_time: bool = False
    reference: bool = False
    enabled: bool = True

    def __init__(
        self,
        name: str,
        update_type: str = "general",
        reference: str | bool | None = None,
        **kwargs: Any,
    ) -> None:
        """Create a dataset spec from the compact public registration shape.

        Unknown keyword arguments are treated as source API static parameters.
        Advanced framework fields can still be passed by keyword when needed.
        """

        spec_values, api_kwargs = _split_spec_kwargs(kwargs)
        source = str(spec_values.pop("source", "custom"))
        source_dataset = str(spec_values.pop("source_dataset", name))
        reference_flag = bool(reference) if isinstance(reference, bool) else bool(spec_values.pop("reference", False))
        calendar_dataset = _optional_str(spec_values.pop("calendar_dataset", None))
        id_dataset = _optional_str(spec_values.pop("id_dataset", None))
        if isinstance(reference, str):
            if update_type == "by_id":
                id_dataset = reference
            elif update_type == "by_daily":
                calendar_dataset = reference
            else:
                reference_flag = True
        if update_type == "by_daily" and calendar_dataset is None:
            calendar_dataset = "trade_cal"
        if update_type == "by_id" and id_dataset is None:
            id_dataset = "asset_list"

        request_options = dict(spec_values.pop("request_options", {}) or {})
        if api_kwargs:
            static_params = dict(request_options.get("static_params") or {})
            static_params.update(api_kwargs)
            request_options["static_params"] = static_params

        values: dict[str, Any] = {
            "name": str(name),
            "source": source,
            "source_dataset": source_dataset,
            "category": str(spec_values.pop("category", _default_category(update_type, reference_flag))),
            "field_mapping": dict(spec_values.pop("field_mapping", {}) or {}),
            "required_columns": _tuple(
                spec_values.pop("required_columns", _default_required_columns(update_type, reference_flag))
            ),
            "data_kind": str(spec_values.pop("data_kind", _default_data_kind(update_type, reference_flag))),
            "primary_key": _optional_tuple(spec_values.pop("primary_key", _default_primary_key(update_type, reference_flag))),
            "business_key": _optional_tuple(spec_values.pop("business_key", None)),
            "asset_column": _optional_str(spec_values.pop("asset_column", None)),
            "time_column": _optional_str(spec_values.pop("time_column", None)),
            "period_column": _optional_str(spec_values.pop("period_column", None)),
            "request_options": request_options,
            "normalizer": str(spec_values.pop("normalizer", "standard")),
            "deduplication": str(spec_values.pop("deduplication", _default_deduplication(update_type))),
            "update_type": str(update_type),
            "calendar_dataset": calendar_dataset,
            "calendar_date_column": str(spec_values.pop("calendar_date_column", "time")),
            "calendar_open_column": _optional_str(spec_values.pop("calendar_open_column", "is_open")),
            "id_dataset": id_dataset,
            "id_column": str(spec_values.pop("id_column", "asset_id")),
            "start_date": _optional_str(spec_values.pop("start_date", None)),
            "request_date_param": str(spec_values.pop("request_date_param", "date")),
            "request_id_param": str(spec_values.pop("request_id_param", "id")),
            "batch_count": int(spec_values.pop("batch_count", 32)),
            "sort_columns": _tuple(spec_values.pop("sort_columns", _default_sort_columns(update_type, reference_flag))),
            "point_in_time": bool(spec_values.pop("point_in_time", False)),
            "reference": reference_flag,
            "enabled": bool(spec_values.pop("enabled", True)),
        }
        if spec_values:
            unknown = ", ".join(sorted(spec_values))
            raise DatasetSpecError(f"Unsupported dataset spec option(s): {unknown}")
        for field_name, value in values.items():
            object.__setattr__(self, field_name, value)

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "DatasetSpec":
        """Build a specification from parsed config data."""

        required = ("name", "update_type")
        missing = [key for key in required if key not in value]
        if missing:
            raise DatasetSpecError(f"Dataset spec missing required keys: {missing}")
        payload = dict(value)
        name = str(payload.pop("name"))
        update_type = str(payload.pop("update_type"))
        reference = payload.pop("reference", None)
        return cls(name, update_type, reference, **payload)

    @classmethod
    def from_yaml(cls, path: str | Path) -> "DatasetSpec":
        """Load a dataset spec from a small YAML file.

        The project intentionally avoids a YAML runtime dependency. This parser
        supports the simple mappings and lists used by bundled dataset specs.
        """

        return cls.from_mapping(_parse_simple_yaml(Path(path).read_text()))

    @property
    def key(self) -> tuple[str, str]:
        """Return the `(source, name)` lookup key."""

        return (self.source, self.name)


def _tuple(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    return tuple(str(item) for item in value)


def _optional_tuple(value: Any) -> tuple[str, ...] | None:
    result = _tuple(value)
    return result or None


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


_SPEC_KEYS = {
    "source",
    "source_dataset",
    "category",
    "field_mapping",
    "required_columns",
    "data_kind",
    "primary_key",
    "business_key",
    "asset_column",
    "time_column",
    "period_column",
    "request_options",
    "normalizer",
    "deduplication",
    "calendar_dataset",
    "calendar_date_column",
    "calendar_open_column",
    "id_dataset",
    "id_column",
    "start_date",
    "request_date_param",
    "request_id_param",
    "batch_count",
    "sort_columns",
    "point_in_time",
    "enabled",
    "reference",
}


def _split_spec_kwargs(values: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    spec_values: dict[str, Any] = {}
    api_kwargs: dict[str, Any] = {}
    for key, value in values.items():
        if key in _SPEC_KEYS:
            spec_values[key] = value
        else:
            api_kwargs[key] = value
    return spec_values, api_kwargs


def _default_category(update_type: str, reference: bool) -> str:
    if reference or update_type == "general":
        return "reference"
    if update_type == "by_daily":
        return "market"
    return "generic"


def _default_data_kind(update_type: str, reference: bool) -> str:
    if reference or update_type == "general":
        return "reference"
    if update_type == "by_daily":
        return "price"
    return "generic"


def _default_required_columns(update_type: str, reference: bool) -> tuple[str, ...]:
    if reference or update_type == "general":
        return ()
    return ("asset_id", "time")


def _default_primary_key(update_type: str, reference: bool) -> tuple[str, ...] | None:
    if reference or update_type == "general":
        return None
    return ("asset_id", "time")


def _default_sort_columns(update_type: str, reference: bool) -> tuple[str, ...]:
    if reference or update_type == "general":
        return ()
    return ("time", "asset_id")


def _default_deduplication(update_type: str) -> str:
    return "exact_record_hash" if update_type == "general" else "primary_key_last"


def _parse_simple_yaml(text: str) -> dict[str, Any]:
    rows = [line.rstrip() for line in text.splitlines()]
    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any] | list[Any]]] = [(-1, root)]
    for raw in rows:
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        line = raw.strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if line.startswith("- "):
            if not isinstance(parent, list):
                raise DatasetSpecError(f"Unsupported YAML list location: {line}")
            parent.append(_yaml_scalar(line[2:]))
            continue
        key, sep, value = line.partition(":")
        if not sep:
            raise DatasetSpecError(f"Unsupported YAML line: {line}")
        key = key.strip()
        value = value.strip()
        if value == "":
            container: dict[str, Any] | list[Any]
            next_list = _next_content_is_list(rows, raw)
            container = [] if next_list else {}
            if isinstance(parent, dict):
                parent[key] = container
            else:
                raise DatasetSpecError(f"Unsupported nested YAML key: {key}")
            stack.append((indent, container))
        elif isinstance(parent, dict):
            parent[key] = _yaml_scalar(value)
    return root


def _next_content_is_list(rows: list[str], current: str) -> bool:
    index = rows.index(current)
    current_indent = len(current) - len(current.lstrip(" "))
    for row in rows[index + 1 :]:
        if not row.strip() or row.lstrip().startswith("#"):
            continue
        indent = len(row) - len(row.lstrip(" "))
        return indent > current_indent and row.strip().startswith("- ")
    return False


def _yaml_scalar(value: str) -> Any:
    if value in {"true", "True"}:
        return True
    if value in {"false", "False"}:
        return False
    if value in {"null", "None", "~"}:
        return None
    if value.startswith("[") and value.endswith("]"):
        inner = value[1:-1].strip()
        return [] if not inner else [_yaml_scalar(part.strip()) for part in inner.split(",")]
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        return value[1:-1]
    try:
        return int(value)
    except ValueError:
        return value
