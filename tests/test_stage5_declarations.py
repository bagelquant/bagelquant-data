"""Standalone declaration transfer through public Data APIs only."""

import copy
import json

import pytest

from bagelquant_data import ConfigurationError, DataItemSpec, DataLake, DatasetSpec, ItemInput, RawInput, SourceNotFoundError


class FakeSource:
    name = "fake"

    def configure(self, **options):
        self.options = options


def open_lake(root):
    return DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")


def populate(lake):
    lake.catalog.sources.register(FakeSource())
    lake.catalog.sources.configure("fake", token="DO-NOT-EXPORT")
    lake.raw.register(DatasetSpec("daily", "general", source="fake"))
    lake.items.register(DataItemSpec("price", (RawInput("fake", "daily"),), producer_key="external", producer_revision="v1"))
    lake.items.register(DataItemSpec("copy", (ItemInput("price"),)))
    top = lake.catalog.raw_categories("fake").create("stocks")
    nested = lake.catalog.raw_categories("fake").create("market", parent_id=top["id"])
    lake.catalog.raw_categories("fake").assign("daily", nested["id"])
    item_category = lake.catalog.item_categories.create("prices")
    lake.catalog.item_categories.assign("price", item_category["id"])


def test_public_round_trip_and_idempotent_receipt(tmp_path):
    source = open_lake(tmp_path / "source")
    populate(source)
    payload = source.catalog.export_declarations()
    assert "DO-NOT-EXPORT" not in json.dumps(payload)
    assert "configured" not in payload["sources"][0]
    destination = open_lake(tmp_path / "destination")
    plan = destination.catalog.plan_declaration_batch(payload)
    assert plan["valid"] and not plan["issues"]
    # Topological ordering is independent of incoming item ordering.
    assert plan["item_order"].index("price") < plan["item_order"].index("copy")
    receipt = destination.catalog.apply_declaration_batch(plan, request_id="transfer-1", expected_revision=plan["expected_revision"])
    assert destination.catalog.export_declarations() == payload
    assert destination.catalog.declaration_batch_receipt("transfer-1") == receipt
    assert destination.catalog.apply_declaration_batch(plan, request_id="transfer-1", expected_revision=plan["expected_revision"]) == receipt
    assert destination.catalog.verify_declaration_batch_receipt(receipt) == {"valid": True, "current": True, "issues": []}
    assert destination.raw.status("daily", source="fake")["row_count"] == 0
    # Imported external producers are declarations, never executable code.
    with pytest.raises(SourceNotFoundError):
        destination.catalog.sources.get("fake")


def test_conflict_never_overwrites_and_stale_plan_rejects(tmp_path):
    lake = open_lake(tmp_path)
    populate(lake)
    original = lake.catalog.export_declarations()
    payload = copy.deepcopy(original)
    payload["raw"][0]["spec"]["description"] = "conflicting import"
    plan = lake.catalog.plan_declaration_batch(payload)
    assert not plan["valid"] and plan["conflicts"][0]["section"] == "raw"
    with pytest.raises(ConfigurationError, match="conflicts"):
        lake.catalog.apply_declaration_batch(plan, request_id="conflict", expected_revision=plan["expected_revision"])
    assert lake.catalog.export_declarations() == original
    plan = lake.catalog.plan_declaration_batch(original)
    lake.catalog.item_categories.create("new")
    with pytest.raises(ConfigurationError, match="stale"):
        lake.catalog.apply_declaration_batch(plan, request_id="stale", expected_revision=plan["expected_revision"])


def test_prospective_cycle_and_category_namespace_rejected(tmp_path):
    lake = open_lake(tmp_path)
    payload = lake.catalog.export_declarations()
    payload["items"] = [{"spec": {"name": "A", "inputs": [{"kind": "ItemInput", "name": "B"}]}, "active": True},
                        {"spec": {"name": "B", "inputs": [{"kind": "ItemInput", "name": "A"}]}, "active": True}]
    assert "DataItem dependency cycle" in lake.catalog.plan_declaration_batch(payload)["issues"]
    payload["items"] = []
    payload["categories"] = [{"id": "a", "kind": "item", "source": "", "name": "one", "parent_id": "b"},
                             {"id": "b", "kind": "raw", "source": "fake", "name": "two", "parent_id": None}]
    assert any("foreign category parent" in issue for issue in lake.catalog.plan_declaration_batch(payload)["issues"])


def test_batch_failure_rolls_back_every_helper_and_receipt(tmp_path, monkeypatch):
    source = open_lake(tmp_path / "source")
    populate(source)
    lake = open_lake(tmp_path / "destination")
    before = lake.catalog.export_declarations()
    plan = lake.catalog.plan_declaration_batch(source.catalog.export_declarations())
    def fail(_spec):
        raise RuntimeError("publication failed")
    monkeypatch.setattr(lake.items, "_register", fail)
    with pytest.raises(RuntimeError, match="publication failed"):
        lake.catalog.apply_declaration_batch(plan, request_id="failed", expected_revision=plan["expected_revision"])
    assert lake.catalog.export_declarations() == before
    assert lake.catalog.declaration_batch_receipt("failed") is None


def test_retained_receipt_integrity_and_currentness_are_distinct(tmp_path):
    source = open_lake(tmp_path / "source")
    populate(source)
    lake = open_lake(tmp_path / "destination")
    plan = lake.catalog.plan_declaration_batch(source.catalog.export_declarations())
    receipt = lake.catalog.apply_declaration_batch(plan, request_id="one", expected_revision=plan["expected_revision"])
    category = lake.catalog.item_categories.list()[0]
    lake.catalog.item_categories.rename(category["id"], "changed")
    result = lake.catalog.verify_declaration_batch_receipt(receipt)
    assert result["valid"] and not result["current"]
    damaged = dict(receipt, after_revision="tampered")
    assert not lake.catalog.verify_declaration_batch_receipt(damaged)["valid"]
    foreign = open_lake(tmp_path / "foreign")
    assert not foreign.catalog.verify_declaration_batch_receipt(receipt)["valid"]
    newer = lake.catalog.plan_declaration_batch(lake.catalog.export_declarations())
    with pytest.raises(ConfigurationError, match="another plan"):
        lake.catalog.apply_declaration_batch(newer, request_id="one", expected_revision=newer["expected_revision"])


def test_plan_read_only_and_secret_descriptors_rejected(tmp_path):
    lake = open_lake(tmp_path)
    populate(lake)
    payload = lake.catalog.export_declarations()
    readonly = DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path, read_only=True)
    plan = readonly.catalog.plan_declaration_batch(payload)
    assert plan["valid"]
    with pytest.raises(PermissionError):
        readonly.catalog.apply_declaration_batch(plan, request_id="readonly", expected_revision=plan["expected_revision"])
    payload["sources"][0]["token"] = "secret"
    with pytest.raises(ConfigurationError, match="credentials"):
        lake.catalog.plan_declaration_batch(payload)


def test_archived_declarations_and_atomic_empty_retry(tmp_path):
    source = open_lake(tmp_path / "source")
    populate(source)
    source.items.remove("copy")
    source.items.remove("price")
    source.raw.disable("daily", source="fake")
    source.raw.remove("daily", source="fake")
    source.catalog.sources.disable("fake")
    payload = source.catalog.export_declarations()
    target = open_lake(tmp_path / "target")
    plan = target.catalog.plan_declaration_batch(payload)
    assert plan["valid"]
    receipt = target.catalog.apply_declaration_batch(plan, request_id="archived", expected_revision=plan["expected_revision"])
    assert target.catalog.export_declarations() == payload
    assert target.items.list() == target.raw.list() == []
    assert target.catalog.verify_declaration_batch_receipt(receipt)["current"]
    no_change = target.catalog.plan_declaration_batch(payload)
    assert all(section["added"] == 0 for section in no_change["summary"].values())


def test_foreign_plan_and_assignment_conflicts_fail_without_mutation(tmp_path):
    source = open_lake(tmp_path / "source")
    populate(source)
    left, right = open_lake(tmp_path / "left"), open_lake(tmp_path / "right")
    plan = left.catalog.plan_declaration_batch(source.catalog.export_declarations())
    with pytest.raises(ConfigurationError, match="stale"):
        right.catalog.apply_declaration_batch(plan, request_id="foreign", expected_revision=plan["expected_revision"])
    payload = source.catalog.export_declarations()
    assignment = next(row for row in payload["assignments"] if row["kind"] == "raw")
    assignment["category_id"] = next(row["id"] for row in payload["categories"] if row["kind"] == "item")
    assert not left.catalog.plan_declaration_batch(payload)["valid"]


def test_status_distinguishes_current_declaration_from_committed_item_definition(tmp_path):
    from datetime import date
    from bagelquant_data import DataLake, DataItemSpec
    import polars as pl
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("membership", value_dtype="bool", producer_key="owner", producer_revision="a"))
    rows = pl.DataFrame({"time":[date(2024,1,2)], "asset_id":["A"], "value":[True]})
    lake.items.ingest("membership", rows, available_date=date(2024,1,2))
    assert lake.items.status("membership")["committed_definition_current"] is True
    lake.items.register(DataItemSpec("membership", value_dtype="bool", producer_key="owner", producer_revision="b"))
    assert lake.items.status("membership")["committed_definition_current"] is False
    lake.items.ingest("membership", rows.with_columns(pl.lit(False).alias("value")), available_date=date(2024,1,3))
    assert lake.items.status("membership")["committed_definition_current"] is True
