from datetime import date

import polars as pl
import pytest

from bagelquant_data import (
    ConfigurationError,
    DataItemSpec,
    DataLake,
    DatasetSpec,
    RawInput,
)


def test_category_tree_moves_without_changing_data_and_rejects_cycle(tmp_path):
    lake = DataLake.open(
        data_meta_path=tmp_path / "meta" / "data_meta.sqlite",
        lake_path=tmp_path / "files",
    )
    tree = lake.catalog.raw_categories("custom")
    equity = tree.create("equity")
    market = tree.create("market", parent_id=equity["id"])
    lake.raw.ingest(
        DatasetSpec(
            "daily",
            "by_date",
            date_kind="calendar",
            field_mappings={"time": "time", "asset_id": "asset_id"},
        ),
        pl.DataFrame({"time": [date(2026, 1, 2)], "asset_id": ["A"], "close": [1.0]}),
    )
    tree.assign("daily", market["id"])
    before = lake.raw.manifest("daily", source="custom")
    with pytest.raises(ConfigurationError, match="cycle"):
        tree.move(equity["id"], parent_id=market["id"])
    with pytest.raises(ConfigurationError, match="nonempty"):
        tree.remove(market["id"])
    tree.move(market["id"])
    tree.rename(market["id"], "prices")
    assert lake.raw.manifest("daily", source="custom") == before
    reopened = DataLake.open(
        data_meta_path=lake.data_meta_path, lake_path=lake.lake_path, read_only=True
    )
    assert reopened.catalog.raw_categories("custom").members(market["id"]) == ["daily"]
    with pytest.raises(PermissionError):
        reopened.catalog.raw_categories("custom").create("forbidden")


def test_unregister_blocks_active_dependencies_and_preserves_frozen_data(tmp_path):
    lake = DataLake.open(
        data_meta_path=tmp_path / "data_meta.sqlite", lake_path=tmp_path / "lake"
    )
    spec = DatasetSpec(
        "daily",
        "by_date",
        date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"},
    )
    lake.raw.ingest(
        spec,
        pl.DataFrame({"time": [date(2026, 1, 2)], "asset_id": ["A"], "value": [1.0]}),
    )
    lake.items.register(DataItemSpec("price", inputs=(RawInput("custom", "daily"),)))
    with pytest.raises(ConfigurationError, match="referenced"):
        lake.raw.remove("daily", source="custom")
    receipt = lake.inputs.freeze({"price": RawInput("custom", "daily")})
    lake.items.remove("price")
    lake.raw.remove("daily", source="custom")
    assert not lake.raw.list()
    assert lake.inputs.read(receipt, "price").collect()["value"].to_list() == [1.0]


def test_repair_plan_requires_current_registered_evidence(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / 'data_meta.sqlite', lake_path=tmp_path / 'lake')
    spec = DatasetSpec('daily', 'by_date', date_kind='calendar',
                       field_mappings={'time': 'time', 'asset_id': 'asset_id'})
    frame = pl.DataFrame({'time': [date(2026, 1, 2)], 'asset_id': ['A'], 'value': [1.0]})
    lake.raw.ingest(spec, frame)
    plan = lake.integrity.repair_plan('daily', source='custom')
    lake.raw.ingest(spec, frame.with_columns(pl.lit(2.0).alias('value')))
    with pytest.raises(ConfigurationError, match='stale'):
        lake.integrity.repair(plan)
    current = lake.integrity.repair_plan('daily', source='custom')
    generation = lake.raw.manifest('daily', source='custom')[0]['generation_path']
    path = lake.lake_path / 'raw/custom/daily' / generation
    path.write_bytes(b'damaged')
    assert not lake.integrity.scan('daily', source='custom')['valid']
    assert lake.integrity.repair(current)[0]['content_changed'] is False
    assert lake.raw.read('daily', source='custom', view='latest').collect()['value'].to_list() == [2.0]
    foreign = DataLake.open(data_meta_path=tmp_path / 'other.sqlite', lake_path=tmp_path / 'other')
    with pytest.raises(ConfigurationError, match='different lake'):
        foreign.integrity.repair(current)


def test_combined_category_update_is_atomic_and_provider_scoped(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    tree = lake.catalog.raw_categories("custom")
    root = tree.create("root")
    child = tree.create("child", parent_id=root["id"])
    other = tree.create("other")
    sibling = tree.create("taken", parent_id=other["id"])
    before = tree.list()
    with pytest.raises(ConfigurationError, match="Duplicate"):
        tree.update(child["id"], name=sibling["name"], parent_id=other["id"])
    assert tree.list() == before
    foreign = lake.catalog.raw_categories("foreign").create("foreign")
    with pytest.raises(ConfigurationError, match="Unknown parent"):
        tree.update(child["id"], name="moved", parent_id=foreign["id"])
    assert tree.list() == before
    changed = tree.update(child["id"], name="moved", parent_id=other["id"])
    assert changed["name"] == "moved"
    assert changed["parent_id"] == other["id"]
