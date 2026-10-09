"""Finite original roots share currentness work only within one read view."""

from collections import Counter
from dataclasses import replace
from datetime import UTC, date, datetime
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, RawInput


@pytest.fixture
def forest(tmp_path):
    with DataLake.open(
        data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake"
    ) as lake:
        frame = pl.DataFrame(
            {"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.0]}
        )
        spec = DatasetSpec(
            "raw", "by_date", date_kind="calendar",
            field_mappings={"time": "time", "asset_id": "asset_id"},
        )
        lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
        raw = RawInput("custom", "raw", view="versions")
        lake.items.register(
            DataItemSpec("shared", (raw,), producer_key="external", producer_revision="1")
        )
        old = lake.inputs.freeze({"raw": raw}, information_cutoff="2020-01-03")
        lake.items.ingest("shared", frame, available_date="2020-01-01", input_receipt=old)
        lake.raw.ingest(
            spec, frame.with_columns(pl.lit(2.0).alias("value")),
            ingested_at=datetime(2020, 1, 2, tzinfo=UTC),
        )
        new = lake.inputs.freeze({"raw": raw}, information_cutoff="2020-01-03")
        lake.items.ingest(
            "shared", frame.with_columns(pl.lit(2.0).alias("value")),
            available_date="2020-01-02", input_receipt=new,
        )
        parent = lake.inputs.freeze(
            {"shared": ItemInput("shared")}, information_cutoff="2020-01-03"
        )
        roots = []
        for index in range(4):
            name = f"child{index}"
            lake.items.register(
                DataItemSpec(
                    name, (ItemInput("shared"),), producer_key="external", producer_revision="1"
                )
            )
            lake.items.ingest(name, frame, available_date="2020-01-03", input_receipt=parent)
            roots.append(lake.inputs.freeze({name: ItemInput(name)}, information_cutoff="2020-01-03"))
        yield lake, frame, spec, old, parent, roots


def observe_reads(lake, monkeypatch):
    reads = Counter()
    original = lake.inputs._read_frame

    def read(*args, **kwargs):
        reads[args[1]] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(lake.inputs, "_read_frame", read)
    return reads


def test_shared_stale_unselected_parent_reads_once_and_keeps_original_roots(forest, monkeypatch):
    lake, _, _, _, _, roots = forest
    reads = observe_reads(lake, monkeypatch)
    assert [lake.inputs.is_current(root) for root in roots] == [True] * 4
    assert reads == {"shared": 4}
    reads.clear()
    assert lake.inputs.is_current(roots)
    assert reads == {"shared": 1}
    assert [lake.inputs.get(root) for root in roots] == roots
    proof = lake.inputs.verify(roots)
    assert proof["valid"]
    assert [root["receipt_id"] for root in proof["receipts"]] == [root.receipt_id for root in roots]


def test_next_call_rechecks_and_stale_shared_parent_checks_every_root(forest, monkeypatch):
    lake, frame, spec, _, _, roots = forest
    reads = observe_reads(lake, monkeypatch)
    assert lake.inputs.is_current(roots)
    reads.clear()
    assert lake.inputs.is_current(roots)
    assert reads == {"shared": 1}
    lake.raw.ingest(
        spec, frame.with_columns(pl.lit(3.0).alias("value")),
        ingested_at=datetime(2020, 1, 3, tzinfo=UTC),
    )
    reads.clear()
    assert not lake.inputs.is_current(roots)
    assert reads == {"shared": 1, **{f"child{index}": 1 for index in range(4)}}


def test_original_cutoffs_are_not_merged(forest, monkeypatch):
    lake, _, _, _, late, _ = forest
    early = lake.inputs.freeze(
        {"shared": ItemInput("shared")}, information_cutoff="2020-01-01"
    )
    assert not lake.inputs.is_current(early)
    assert lake.inputs.is_current(late.receipt_id)
    reads = observe_reads(lake, monkeypatch)
    assert not lake.inputs.is_current((early, late))
    assert reads == {"shared": 2}


def test_duplicate_id_cannot_hide_bad_supplied_digest(forest):
    lake, _, _, _, _, roots = forest
    with pytest.raises(RuntimeError, match="checksum"):
        lake.inputs.is_current([roots[0], replace(roots[0], digest="0" * 64)])


@pytest.mark.parametrize("roots", [[], ()])
def test_empty_forest_rejected(forest, roots):
    lake, *_ = forest
    with pytest.raises(ValueError, match="at least one"):
        lake.inputs.is_current(roots)


def test_stale_root_cannot_hide_missing_later_root(forest):
    lake, _, _, stale, _, _ = forest
    assert not lake.inputs.is_current(stale)
    with pytest.raises(KeyError, match="Unknown frozen"):
        lake.inputs.is_current([stale, "missing"])


def test_stale_root_cannot_hide_corrupt_later_root(forest):
    lake, _, _, stale, _, roots = forest
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update frozen_inputs set payload_json='{}' where receipt_id=?", (roots[0].receipt_id,))
    with pytest.raises(RuntimeError, match="checksum"):
        lake.inputs.is_current([stale, roots[0]])


def test_fresh_integrity_check_still_rejects_corrupt_unselected_original_bytes(forest):
    lake, _, _, _, _, roots = forest
    assert lake.inputs.is_current(roots)
    assert lake.inputs.verify(roots)["valid"]
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update version_batches set payload=x'00' where commit_seq=1")
    assert lake.inputs.is_current(roots)
    with pytest.raises(RuntimeError, match="compression|checksum"):
        lake.inputs.verify(roots)


def test_all_roots_use_one_read_view_during_a_new_commit(forest, monkeypatch):
    lake, frame, spec, _, _, roots = forest
    original = lake.inputs._capture
    inserted = False

    def capture(db, requests, **kwargs):
        nonlocal inserted
        result = original(db, requests, **kwargs)
        if "child0" in requests and not inserted:
            inserted = True
            lake.raw.ingest(
                spec, frame.with_columns(pl.lit(3.0).alias("value")),
                ingested_at=datetime(2020, 1, 3, tzinfo=UTC),
            )
        return result

    monkeypatch.setattr(lake.inputs, "_capture", capture)
    assert lake.inputs.is_current(roots)
    assert inserted
    assert not lake.inputs.is_current(roots)
