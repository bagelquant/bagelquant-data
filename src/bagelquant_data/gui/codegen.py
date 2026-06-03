"""Generate copyable user code from GUI selections."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class RetrievalSelection:
    """Selected retrieval parameters."""

    lake_root: str
    source: str
    table: str
    year: int | None = None
    month: int | None = None
    fields: tuple[str, ...] = ()
    universe: tuple[str, ...] = ()
    panel_field: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    region: str = "CN"
    include_core_conversion: bool = False
    filters: dict[str, str] = field(default_factory=dict)


def lake_read_snippet(selection: RetrievalSelection) -> str:
    """Return code for reading directly from ``LocalDataLake``."""

    kwargs = _optional_kwargs(
        year=selection.year,
        month=selection.month,
    )
    return "\n".join(
        [
            "from bagelquant_data.lake import LocalDataLake",
            "",
            f"lake = LocalDataLake({_literal(selection.lake_root)})",
            (
                "data = lake.read("
                f"{_literal(selection.source)}, {_literal(selection.table)}{kwargs})"
            ),
            "data.head()",
        ]
    )


def loader_read_snippet(selection: RetrievalSelection) -> str:
    """Return code for lake-first loader retrieval."""

    load_kwargs = _loader_kwargs(selection)
    return "\n".join(
        [
            _data_source_import(),
            "from bagelquant_data.lake import LocalDataLake",
            "from bagelquant_data.loader import Loader",
            "",
            "registry = DataSourceRegistry()",
            "registry.register(TushareDataSource())",
            f"lake = LocalDataLake({_literal(selection.lake_root)})",
            "data = (",
            "    Loader(registry=registry, lake=lake)",
            f"    .source({_literal(selection.source)})",
            f"    .load({_literal(selection.table)}{load_kwargs})",
            "    .data",
            ")",
        ]
    )


def panel_agreement_snippet(selection: RetrievalSelection) -> str:
    """Return code for producing a panel agreement."""

    field = selection.panel_field or (
        selection.fields[0] if selection.fields else "close"
    )
    universe = selection.universe or ("000001.SZ", "600000.SH")
    start_date = selection.start_date or "2024-01-01"
    end_date = selection.end_date or "2024-12-31"
    lines = [
        _data_source_import(),
        "from bagelquant_data.lake import LocalDataLake",
        "from bagelquant_data.loader import Loader",
        "",
        "registry = DataSourceRegistry()",
        "registry.register(TushareDataSource())",
        f"lake = LocalDataLake({_literal(selection.lake_root)})",
        "agreement = (",
        "    Loader(registry=registry, lake=lake)",
        f"    .source({_literal(selection.source)})",
        "    .load_panel(",
        f"        dataset={_literal(selection.table)},",
        f"        field={_literal(field)},",
        f"        universe={_literal_list(universe)},",
        f"        start_date={_literal(start_date)},",
        f"        end_date={_literal(end_date)},",
        f"        region={_literal(selection.region)},",
        "    )",
        ")",
        "agreement.frame.head()",
    ]
    if selection.include_core_conversion:
        lines.extend(["", core_conversion_snippet()])
    return "\n".join(lines)


def core_conversion_snippet() -> str:
    """Return optional downstream conversion example text."""

    return "\n".join(
        [
            "# Optional downstream conversion in bagelquant-core code:",
            "from " + "bagelquant_core import Domain, Panel",
            "",
            "domain = Domain(**agreement.domain_spec.to_core_kwargs())",
            "panel = Panel.from_domain(",
            "    agreement.frame,",
            "    domain,",
            "    name=agreement.dataset_name,",
            "    metadata=agreement.metadata,",
            ")",
        ]
    )


def _loader_kwargs(selection: RetrievalSelection) -> str:
    kwargs: list[str] = []
    if selection.fields:
        kwargs.append(f"fields={_literal_list(selection.fields)}")
    if selection.filters:
        kwargs.append(f"filters={selection.filters!r}")
    if selection.start_date:
        kwargs.append(f"start_date={_literal(selection.start_date)}")
    if selection.end_date:
        kwargs.append(f"end_date={_literal(selection.end_date)}")
    return "" if not kwargs else ", " + ", ".join(kwargs)


def _data_source_import() -> str:
    return (
        "from bagelquant_data.datasource import "
        "DataSourceRegistry, TushareDataSource"
    )


def _optional_kwargs(**values: object) -> str:
    kwargs = [f"{key}={value!r}" for key, value in values.items() if value is not None]
    return "" if not kwargs else ", " + ", ".join(kwargs)


def _literal(value: str) -> str:
    return repr(value)


def _literal_list(values: Sequence[str]) -> str:
    return repr(list(values))
