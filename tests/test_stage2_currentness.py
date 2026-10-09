from datetime import UTC, date, datetime
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, RawInput


def _lake(root, *, read_only=False):
    return DataLake.open(data_meta_path=root / "data_meta.sqlite",
                         lake_path=root / "lake", read_only=read_only)


def _spec(kind="by_date", description=""):
    return DatasetSpec("raw", kind, date_kind="calendar", description=description,
                       field_mappings={"time": "time", "asset_id": "asset_id"})


def _frame(value=1.0):
    return pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [value]})


def _received(day):
    return datetime(2020, 1, day, tzinfo=UTC)


@pytest.mark.parametrize("kind", ["general", "by_date"])
def test_currentness_is_read_only_and_distinguishes_rechecks_from_content_revisions(tmp_path, kind):
    lake = _lake(tmp_path)
    lake.raw.ingest(_spec(kind), _frame(), ingested_at=_received(1))
    request = RawInput("custom", "raw", view="snapshot",
                       observation_start="2020-01-01", observation_end="2020-01-01")
    receipt = lake.inputs.freeze({"raw": request}, information_cutoff="2020-01-03")
    reader = _lake(tmp_path, read_only=True)
    with sqlite3.connect(lake.data_meta_path) as db:
        count = db.execute("select count(*) from frozen_inputs").fetchone()[0]
    assert reader.inputs.is_current(receipt)
    lake.raw.ingest(_spec(kind), _frame(), ingested_at=_received(2))
    assert reader.inputs.is_current(receipt)
    lake.raw.ingest(_spec(kind), _frame(2.0), ingested_at=_received(3))
    assert not reader.inputs.is_current(receipt)
    assert reader.inputs.read(receipt, "raw").collect()["value"].to_list() == [1.0]
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("select count(*) from frozen_inputs").fetchone()[0] == count


def test_currentness_rejects_changed_definition_archival_and_corrupt_receipt(tmp_path):
    lake = _lake(tmp_path)
    lake.raw.ingest(_spec(), _frame(), ingested_at=_received(1))
    request = {"raw": RawInput("custom", "raw", view="snapshot")}
    first = lake.inputs.freeze(request, information_cutoff="2020-01-03")
    lake.raw.register(_spec(description="Changed declaration"))
    assert not lake.inputs.is_current(first)
    current = lake.inputs.freeze(request, information_cutoff="2020-01-03")
    assert lake.inputs.is_current(current)
    lake.raw.remove("raw", source="custom")
    reader = _lake(tmp_path, read_only=True)
    assert not reader.inputs.is_current(current)
    assert reader.inputs.verify(current)["valid"]
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update frozen_inputs set payload_json='{}' where receipt_id=?", (current.receipt_id,))
    with pytest.raises(RuntimeError, match="receipt checksum"):
        reader.inputs.is_current(current)


def test_public_build_receipts_survive_reopen_and_archive(tmp_path):
    lake = _lake(tmp_path)
    lake.raw.ingest(_spec(), _frame(), ingested_at=_received(1))
    request = RawInput("custom", "raw")
    lake.items.register(DataItemSpec("item", (request,), time_column="source_time"))
    first = lake.items.initialize("item", start="2020-01-01", end="2020-01-01")
    builds = lake.items.builds("item")
    assert builds
    latest = builds[-1]
    assert latest["start_date"] == latest["end_date"] == "2020-01-01"
    assert latest["definition_hash"] == lake.items.status("item")["definition_hash"]
    assert latest["dependency_digest"] == first.dependency_digest
    assert latest["frozen_receipt_id"] == first.frozen_receipt_id
    reader = _lake(tmp_path, read_only=True)
    assert reader.inputs.is_current(latest["frozen_receipt_id"])
    lake.raw.ingest(_spec(), _frame(2.0), ingested_at=_received(2))
    assert not reader.inputs.is_current(latest["frozen_receipt_id"])
    lake.items.remove("item")
    assert reader.items.builds("item") == builds
    with pytest.raises(KeyError):
        reader.items.builds("unknown")


def test_item_receipt_currentness_uses_declared_item_definition_and_content(tmp_path):
    lake = _lake(tmp_path)
    lake.items.register(DataItemSpec("item"))
    lake.items.ingest("item", _frame(), available_date="2020-01-01")
    receipt = lake.inputs.freeze({"item": ItemInput("item", view="snapshot")}, information_cutoff="2020-01-03")
    reader = _lake(tmp_path, read_only=True)
    assert reader.inputs.is_current(receipt)
    lake.items.register(DataItemSpec("item", description="New declaration"))
    assert not reader.inputs.is_current(receipt)


def test_output_currentness_follows_latest_transitive_dependency_proof(tmp_path):
    lake = _lake(tmp_path)
    lake.raw.ingest(_spec(), _frame(), ingested_at=_received(1))
    raw = RawInput("custom", "raw", view="versions")
    lake.items.register(DataItemSpec("item", (raw,), producer_key="external", producer_revision="1"))
    original_inputs = lake.inputs.freeze({"raw": raw}, information_cutoff="2020-01-03")
    lake.items.ingest("item", _frame(), available_date="2020-01-01", input_receipt=original_inputs)
    original_output = lake.inputs.freeze({"item": ItemInput("item")}, information_cutoff="2020-01-03")
    reader = _lake(tmp_path, read_only=True).inputs
    assert reader.is_current(original_output)
    lake.raw.ingest(_spec(), _frame(2.0), ingested_at=_received(2))
    assert not reader.is_current(original_output)
    revised_inputs = lake.inputs.freeze({"raw": raw}, information_cutoff="2020-01-03")
    # The same numerical output still has a new causal producer-input proof.
    lake.items.ingest("item", _frame(), available_date="2020-01-02", input_receipt=revised_inputs)
    revised_output = lake.inputs.freeze({"item": ItemInput("item")}, information_cutoff="2020-01-03")
    assert reader.is_current(revised_output)
    assert reader.read(revised_output, "item").collect()["value"].to_list() == [1.0]
    assert len(revised_output.evidence["item"]["parent_receipts"]) == 2


def test_unbuilt_and_empty_derived_items_have_explicit_currentness_proof(tmp_path):
    lake = _lake(tmp_path)
    lake.raw.ingest(_spec(), _frame(), ingested_at=_received(1))
    raw = RawInput("custom", "raw", view="versions")
    lake.items.register(DataItemSpec("empty", (raw,), producer_key="empty", producer_revision="1"))
    request = {"empty": ItemInput("empty", view="snapshot")}
    unbuilt = lake.inputs.freeze(request, information_cutoff="2020-01-03")
    assert not lake.inputs.is_current(unbuilt)
    inputs = lake.inputs.freeze({"raw": raw}, information_cutoff="2020-01-03")
    lake.items.ingest("empty", _frame().head(0), available_date="2020-01-03", input_receipt=inputs)
    built = lake.inputs.freeze(request, information_cutoff="2020-01-03")
    assert lake.inputs.is_current(built)
    assert lake.inputs.verify(built)["upstream_receipt_count"] == 1
    assert lake.inputs.read(built, "empty").collect().is_empty()
    lake.raw.ingest(_spec(), _frame(2.0), ingested_at=_received(2))
    assert not lake.inputs.is_current(built)


def test_empty_built_window_uses_its_proof_when_other_windows_have_rows(tmp_path):
    lake = _lake(tmp_path)
    lake.raw.ingest(_spec(), _frame(), ingested_at=_received(1))
    raw = RawInput("custom", "raw", view="versions")
    lake.items.register(DataItemSpec("item", (raw,), producer_key="external", producer_revision="1"))
    inputs = lake.inputs.freeze({"raw": raw}, information_cutoff="2020-01-02")
    lake.items.ingest("item", _frame(), available_date="2020-01-01", input_receipt=inputs)
    lake.items.register_producer("external", "1", lambda context: _frame().head(0))
    lake.items.update("item", start="2020-01-02", end="2020-01-02")
    request = ItemInput("item", start="2020-01-02", end="2020-01-02", view="snapshot")
    output = lake.inputs.freeze({"item": request}, information_cutoff="2020-01-02")
    assert lake.inputs.read(output, "item").collect().is_empty()
    assert output.evidence["item"]["empty_item_build"]["start_date"] == "2020-01-02"
    assert lake.inputs.is_current(output)
    outside = lake.inputs.freeze({"item": ItemInput("item", start="2020-01-03", end="2020-01-03",
                                                     view="snapshot")}, information_cutoff="2020-01-03")
    assert not lake.inputs.is_current(outside)


@pytest.mark.parametrize("empty", [False, True])
def test_single_current_parent_proves_item_freshness_without_value_scan(tmp_path, monkeypatch, empty):
    lake = _lake(tmp_path)
    lake.raw.ingest(_spec(), _frame(), ingested_at=_received(1))
    raw = RawInput("custom", "raw", view="versions")
    lake.items.register(DataItemSpec("item", (raw,), producer_key="external", producer_revision="1"))
    inputs = lake.inputs.freeze({"raw": raw}, information_cutoff="2020-01-03")
    lake.items.ingest("item", _frame().head(0) if empty else _frame(),
                      available_date="2020-01-01", input_receipt=inputs)
    output = lake.inputs.freeze({"item": ItemInput("item")}, information_cutoff="2020-01-03")
    reader = _lake(tmp_path, read_only=True).inputs

    def scan(*_args, **_kwargs):
        pytest.fail("a single current dependency proof must not scan Item values")

    monkeypatch.setattr(reader, "_read_frame", scan)
    assert reader.is_current(output)


def test_stale_single_parent_falls_back_to_the_selected_window(tmp_path, monkeypatch):
    lake = _lake(tmp_path)
    lake.raw.ingest(_spec(), _frame(), ingested_at=_received(1))
    raw = RawInput("custom", "raw", view="versions")
    lake.items.register(DataItemSpec("item", (raw,), producer_key="external", producer_revision="1"))
    inputs = lake.inputs.freeze({"raw": raw}, information_cutoff="2020-01-03")
    lake.items.ingest("item", _frame(), available_date="2020-01-01", input_receipt=inputs)
    output = lake.inputs.freeze({"item": ItemInput("item")}, information_cutoff="2020-01-03")
    lake.raw.ingest(_spec(), _frame(2.0), ingested_at=_received(2))
    reader = _lake(tmp_path, read_only=True).inputs
    original = reader._read_frame
    scanned = []

    def scan(receipt, alias):
        scanned.append(alias)
        return original(receipt, alias)

    monkeypatch.setattr(reader, "_read_frame", scan)
    assert not reader.is_current(output)
    assert scanned == ["item"]


def test_shared_batches_verify_once_per_request_and_recheck_next_request(tmp_path, monkeypatch):
    from bagelquant_data import inputs as module

    lake = _lake(tmp_path)
    lake.raw.ingest(_spec(), _frame(), mode="initialize", ingested_at=_received(1))
    for name in ("first", "second"):
        lake.items.register(DataItemSpec(name, (RawInput("custom", "raw"),), time_column="source_time"))
        lake.items.initialize(name, start="2020-01-01", end="2020-01-01")
    frozen = lake.inputs.freeze({name: ItemInput(name) for name in ("first", "second")},
                                information_cutoff="2020-01-03")
    original = module.verify_batch
    calls = []

    def read(store, path, commit, expected_hash, **options):
        calls.append((commit, path, expected_hash))
        return original(store, path, commit, expected_hash, **options)

    monkeypatch.setattr(module, "verify_batch", read)
    first = lake.inputs.verify(frozen)
    assert first["batch_count"] == 4 and len(calls) == len(set(calls)) == 3
    calls.clear()
    assert lake.inputs.verify(frozen) == first
    assert len(calls) == 3

    def lost_batch(*_args, **_kwargs):
        raise RuntimeError("fixture lost immutable batch")

    monkeypatch.setattr(module, "verify_batch", lost_batch)
    with pytest.raises(RuntimeError, match="lost immutable batch"):
        lake.inputs.verify(frozen)
