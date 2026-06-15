"""Dataset specification model."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from bagelquant_data.core.exceptions import DatasetSpecError


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    """Declarative canonical dataset behavior."""

    name: str
    source: str
    source_dataset: str
    category: str
    field_mapping: dict[str, str]
    required_columns: tuple[str, ...]
    primary_key: tuple[str, ...] | None = None
    business_key: tuple[str, ...] | None = None
    asset_column: str | None = None
    time_column: str | None = None
    period_column: str | None = None
    request_planner: str = "snapshot"
    request_options: dict[str, Any] = field(default_factory=dict)
    normalizer: str = "standard"
    validator: str | None = None
    deduplication: str = "exact_record_hash"
    partition_strategy: str = "single_file"
    partition_options: dict[str, Any] = field(default_factory=dict)
    update_mode: str = "upsert"
    sort_columns: tuple[str, ...] = ()
    point_in_time: bool = False
    reference: bool = False
    enabled: bool = True

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "DatasetSpec":
        """Build a specification from parsed config data."""

        required = ("name", "source", "source_dataset", "category")
        missing = [key for key in required if key not in value]
        if missing:
            raise DatasetSpecError(f"Dataset spec missing required keys: {missing}")
        return cls(
            name=str(value["name"]),
            source=str(value["source"]),
            source_dataset=str(value["source_dataset"]),
            category=str(value["category"]),
            field_mapping=dict(value.get("field_mapping") or {}),
            required_columns=_tuple(value.get("required_columns")),
            primary_key=_optional_tuple(value.get("primary_key")),
            business_key=_optional_tuple(value.get("business_key")),
            asset_column=_optional_str(value.get("asset_column")),
            time_column=_optional_str(value.get("time_column")),
            period_column=_optional_str(value.get("period_column")),
            request_planner=str(value.get("request_planner") or "snapshot"),
            request_options=dict(value.get("request_options") or {}),
            normalizer=str(value.get("normalizer") or "standard"),
            validator=_optional_str(value.get("validator")),
            deduplication=str(value.get("deduplication") or "exact_record_hash"),
            partition_strategy=str(value.get("partition_strategy") or "single_file"),
            partition_options=dict(value.get("partition_options") or {}),
            update_mode=str(value.get("update_mode") or "upsert"),
            sort_columns=_tuple(value.get("sort_columns")),
            point_in_time=bool(value.get("point_in_time", False)),
            reference=bool(value.get("reference", False)),
            enabled=bool(value.get("enabled", True)),
        )

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
