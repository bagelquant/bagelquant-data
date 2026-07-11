from __future__ import annotations

from dataclasses import fields
import json

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.core import DatasetSpecError, ValidationError, incremental_key


def test_dataset_spec_is_a_plain_minimal_dataclass() -> None:
    spec = DatasetSpec(
        "balancesheet",
        "by_asset",
        asset_list="stock_basic",
        primary_key_extra=("period",),
        field_mappings={"ann_date": "time", "ts_code": "asset_id"},
    )

    assert [field.name for field in fields(DatasetSpec)] == [
        "name",
        "update_type",
        "source",
        "calendar",
        "asset_list",
        "primary_key_extra",
        "source_api_params",
        "source_api_param_sets",
        "date_param",
        "field_mappings",
    ]
    assert incremental_key(spec) == ("time", "asset_id", "period")
    assert not hasattr(spec, "primary_key")
    assert not hasattr(DatasetSpec, "from_mapping")


def test_manager_validates_references_and_toml(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    with pytest.raises(DatasetSpecError, match="calendar"):
        lake.admin.datasets.register(DatasetSpec("daily", "by_daily"))
    with pytest.raises(DatasetSpecError, match="asset_list"):
        lake.admin.datasets.register(DatasetSpec("income", "by_asset"))

    path = tmp_path / "daily.toml"
    path.write_text(
        'name = "daily"\nupdate_type = "by_daily"\ncalendar = "trade_cal"\ndate_param = "pub_date"\n[field_mappings]\ntrade_date = "time"\nts_code = "asset_id"\n[source_api_params]\nexchange = "SSE"\n[[source_api_param_sets]]\nlist_status = ["L", "D"]\n'
    )
    spec = lake.admin.datasets.register_toml(path)
    assert spec.calendar == "trade_cal"
    assert spec.date_param == "pub_date"
    assert spec.source_api_params == {"exchange": "SSE"}
    assert spec.source_api_param_sets == ({"list_status": ["L", "D"]},)
    assert spec.field_mappings == {"trade_date": "time", "ts_code": "asset_id"}
    reopened = DataLake.open(tmp_path)
    assert reopened.admin.datasets.get("daily", source="custom").source_api_params == {
        "exchange": "SSE"
    }
    assert (
        reopened.admin.datasets.get("daily", source="custom").date_param == "pub_date"
    )
    assert reopened.admin.datasets.get(
        "daily", source="custom"
    ).source_api_param_sets == ({"list_status": ["L", "D"]},)
    assert reopened.admin.datasets.get("daily", source="custom").field_mappings == {
        "trade_date": "time",
        "ts_code": "asset_id",
    }

    lake.admin.datasets.register(DatasetSpec("general", "general"))
    reopened = DataLake.open(tmp_path)
    assert (
        reopened.admin.datasets.get("general", source="custom").source_api_param_sets
        == ()
    )

    path.write_text(
        'name = "invalid"\nupdate_type = "general"\nsource_api_params = "SSE"\n'
    )
    with pytest.raises(DatasetSpecError, match="source_api_params"):
        lake.admin.datasets.register_toml(path)

    path.write_text(
        'name = "invalid"\nupdate_type = "general"\nsource_api_param_sets = []\n'
    )
    with pytest.raises(DatasetSpecError, match="source_api_param_sets"):
        lake.admin.datasets.register_toml(path)


def test_manager_loads_stored_specs_without_source_api_params(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.admin.datasets.register(DatasetSpec("stock_basic", "general"))
    payload = json.dumps(
        {"name": "stock_basic", "update_type": "general", "source": "custom"}
    )
    with lake.metadata.connect() as db:
        db.execute(
            "update datasets set spec_json = ? where source = ? and name = ?",
            (payload, "custom", "stock_basic"),
        )

    reopened = DataLake.open(tmp_path)
    assert (
        reopened.admin.datasets.get("stock_basic", source="custom").source_api_params
        == {}
    )


def test_manager_rejects_date_param_for_non_daily_datasets(tmp_path) -> None:
    lake = DataLake.open(tmp_path)

    with pytest.raises(DatasetSpecError, match="date_param"):
        lake.admin.datasets.register(
            DatasetSpec("stock_basic", "general", date_param="pub_date")
        )


def test_manager_requires_complete_incremental_field_mappings(tmp_path) -> None:
    lake = DataLake.open(tmp_path)

    with pytest.raises(DatasetSpecError, match="field_mappings"):
        lake.admin.datasets.register(
            DatasetSpec("daily", "by_daily", calendar="trade_cal")
        )
    with pytest.raises(DatasetSpecError, match="asset_id"):
        lake.admin.datasets.register(
            DatasetSpec(
                "daily",
                "by_daily",
                calendar="trade_cal",
                field_mappings={"trade_date": "time"},
            )
        )

    path = tmp_path / "invalid-mappings.toml"
    path.write_text(
        'name = "daily"\nupdate_type = "by_daily"\ncalendar = "trade_cal"\nfield_mappings = []\n'
    )
    with pytest.raises(DatasetSpecError, match="field_mappings"):
        lake.admin.datasets.register_toml(path)

    path.write_text(
        'name = "daily"\nupdate_type = "by_daily"\ncalendar = "trade_cal"\n'
        '[field_mappings]\ntrade_date = "time"\nts_code = "asset_id"\n'
    )
    assert lake.admin.datasets.register_toml(path).field_mappings == {
        "trade_date": "time",
        "ts_code": "asset_id",
    }

    path.write_text(
        'name = "daily"\nupdate_type = "by_daily"\ncalendar = "trade_cal"\n'
        '[[field_mappings]]\ntrade_date = "time"\nts_code = "asset_id"\n'
    )
    with pytest.raises(DatasetSpecError, match="TOML table"):
        lake.admin.datasets.register_toml(path)


def test_general_and_incremental_ingestion(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.ingest(
        DatasetSpec("stock_basic", "general"), pl.DataFrame({"code": ["A", "A", "B"]})
    )
    assert lake.query.query_general(
        "stock_basic", source="custom", fields=["code"]
    ).collect()["code"].to_list() == ["A", "B"]

    with pytest.raises(ValidationError, match="asset_id"):
        lake.ingest(
            DatasetSpec(
                "daily",
                "by_daily",
                calendar="trade_cal",
                field_mappings={"time": "time", "asset_id": "asset_id"},
            ),
            pl.DataFrame({"time": ["2025-01-01"]}),
        )


def test_field_mappings_reject_missing_sources_and_unmapped_collisions(
    tmp_path,
) -> None:
    lake = DataLake.open(tmp_path)
    missing_source = DatasetSpec(
        "daily",
        "by_daily",
        calendar="trade_cal",
        field_mappings={"trade_date": "time", "ts_code": "asset_id"},
    )
    with pytest.raises(ValidationError, match="mapped source"):
        lake.ingest(missing_source, pl.DataFrame({"trade_date": ["20250102"]}))

    collision = DatasetSpec(
        "daily_collision",
        "by_daily",
        calendar="trade_cal",
        field_mappings={"trade_date": "time", "ts_code": "asset_id", "open": "close"},
    )
    with pytest.raises(ValidationError, match="collide"):
        lake.ingest(
            collision,
            pl.DataFrame(
                {
                    "trade_date": ["20250102"],
                    "ts_code": ["000001.SZ"],
                    "open": [1.0],
                    "close": [2.0],
                }
            ),
        )


def test_field_mappings_allow_arbitrary_renames(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    spec = DatasetSpec(
        "daily",
        "by_daily",
        calendar="trade_cal",
        field_mappings={
            "trade_date": "time",
            "ts_code": "asset_id",
            "vendor_close": "close",
        },
    )

    lake.ingest(
        spec,
        pl.DataFrame(
            {
                "trade_date": ["20250102"],
                "ts_code": ["000001.SZ"],
                "vendor_close": [11.25],
            }
        ),
    )

    frame = lake.query.query("daily", source="custom").collect()
    assert frame["close"].to_list() == [11.25]
    assert "vendor_close" not in frame.columns
