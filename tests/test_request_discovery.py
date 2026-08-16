from __future__ import annotations

import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, RequestDiscoverySpec
from bagelquant_data.core.exceptions import DataSourceError, DatasetSpecError
from bagelquant_data.pipeline.scopes import synchronize_requests
from bagelquant_data.query.raw import RawQueryService


class DiscoverySource:
    name = "custom"

    def __init__(self, *, values: list[str] | None = None) -> None:
        self.values = values if values is not None else ["B", "A", "A"]
        self.calls: list[tuple[str, dict[str, object]]] = []

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        self.calls.append((dataset, dict(request)))
        if dataset == "discover_codes":
            return pl.DataFrame({"code": self.values})
        if dataset == "provider_target":
            return pl.DataFrame(
                {"code": [request["code"]], "region": [request["region"]]}
            )
        raise AssertionError(f"unexpected dataset: {dataset}")


def _discovery() -> RequestDiscoverySpec:
    return RequestDiscoverySpec(
        api="discover_codes",
        params={"level": "L1"},
        result_field="code",
        target_param="code",
    )


def test_general_discovery_fans_out_declared_provider_api_and_records_provenance(tmp_path) -> None:
    source = DiscoverySource()
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    lake.admin.datasets.register(
        DatasetSpec(
            "logical_membership",
            "general",
            source_api="provider_target",
            source_api_param_sets=({"region": ["north", "south"]},),
            request_discovery=_discovery(),
        )
    )

    lake.update.dataset("logical_membership", source="custom")

    assert source.calls[0] == ("discover_codes", {"level": "L1"})
    assert sorted(source.calls[1:], key=lambda item: (str(item[1]["code"]), str(item[1]["region"]))) == [
        ("provider_target", {"region": "north", "code": "A"}),
        ("provider_target", {"region": "south", "code": "A"}),
        ("provider_target", {"region": "north", "code": "B"}),
        ("provider_target", {"region": "south", "code": "B"}),
    ]
    with sqlite3.connect(lake.paths.database) as connection:
        rows = connection.execute(
            "select request_kind, row_count from api_calls"
        ).fetchall()
    assert sorted(rows) == [
        ("discovery", 3),
        ("refresh", 1),
        ("refresh", 1),
        ("refresh", 1),
        ("refresh", 1),
    ]


def test_discovery_values_expand_daily_and_asset_ledger_variants(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame({"time": ["20250102", "20250103"], "is_open": [1, 1]}),
    )
    lake.ingest(
        DatasetSpec("stock_basic", "general", field_mappings={"ts_code": "asset_id"}),
        pl.DataFrame(
            {
                "ts_code": ["000001.SZ", "000002.SZ"],
                "list_date": ["20240101", "20240101"],
                "delist_date": [None, None],
            }
        ),
    )
    raw = RawQueryService(lake.parquet, lake.metadata)
    variants = ({"code": "A"}, {"code": "B"})
    daily = DatasetSpec(
        "daily", "by_daily", calendar="trade_cal", field_mappings={"trade_date": "time", "ts_code": "asset_id"}
    )
    asset = DatasetSpec(
        "asset", "by_asset", asset_list="stock_basic", field_mappings={"ann_date": "time", "ts_code": "asset_id"}
    )
    lake.admin.datasets.register(daily)
    lake.admin.datasets.register(asset)

    daily_requests = synchronize_requests(
        spec=daily, raw=raw, metadata=lake.metadata, start="2025-01-02", end="2025-01-03", discovered_param_sets=variants
    )
    asset_requests = synchronize_requests(
        spec=asset, raw=raw, metadata=lake.metadata, start="2025-01-02", end="2025-01-03", discovered_param_sets=variants
    )

    assert len(daily_requests) == 4
    assert len(asset_requests) == 4
    assert {request.params["code"] for request in daily_requests} == {"A", "B"}
    assert {request.params["code"] for request in asset_requests} == {"A", "B"}


def test_discovery_empty_result_and_parameter_conflict_are_rejected(tmp_path) -> None:
    source = DiscoverySource(values=[])
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(source)
    lake.admin.datasets.register(
        DatasetSpec("logical", "general", request_discovery=_discovery())
    )

    with pytest.raises(DataSourceError, match="no usable"):
        lake.update.dataset("logical", source="custom")
    with pytest.raises(DatasetSpecError, match="conflicts"):
        lake.admin.datasets.register(
            DatasetSpec(
                "conflict",
                "general",
                source_api_params={"code": "fixed"},
                request_discovery=_discovery(),
            )
        )


def test_discovery_and_source_api_round_trip_through_toml_and_metadata(tmp_path) -> None:
    path = tmp_path / "membership.toml"
    path.write_text(
        """
name = "logical_membership"
source = "custom"
source_api = "provider_target"
update_type = "general"

[request_discovery]
api = "discover_codes"
params = { level = "L1" }
result_field = "code"
target_param = "code"
""".strip(),
        encoding="utf-8",
    )
    lake = DataLake.open(tmp_path)

    registered = lake.admin.datasets.register_toml(path)
    restored = DataLake.open(tmp_path).admin.datasets.get(
        "logical_membership", source="custom"
    )

    assert registered.source_api == "provider_target"
    assert restored == registered
    assert restored.request_discovery == _discovery()
