"""Public temporary lakes exercise currentness without weakening byte proofs."""

from datetime import UTC, date, datetime
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, RawInput


def setup(root):
    data = DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")
    frame = pl.DataFrame(
        {"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.0]}
    )
    spec = DatasetSpec(
        "raw",
        "by_date",
        date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"},
    )
    data.raw.ingest(spec, frame, ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    raw = RawInput("custom", "raw", view="versions")
    data.items.register(
        DataItemSpec("item", (raw,), producer_key="external", producer_revision="1")
    )
    return data, frame, spec, raw


def observe_reads(data, monkeypatch):
    reads = []
    original = data.inputs._read_frame

    def read(*args, **kwargs):
        reads.append(args[1])
        return original(*args, **kwargs)

    monkeypatch.setattr(data.inputs, "_read_frame", read)
    return reads


@pytest.mark.parametrize("empty", [False, True])
def test_all_current_parents_prove_nonempty_or_empty_without_value_scan(
    tmp_path, monkeypatch, empty
):
    data, frame, _, raw = setup(tmp_path)
    with data:
        for day in (2, 3):
            parent = data.inputs.freeze(
                {"raw": raw}, information_cutoff=f"2020-01-0{day}"
            )
            data.items.ingest(
                "item",
                frame.head(0)
                if empty
                else frame.with_columns(pl.lit(float(day)).alias("value")),
                available_date=f"2020-01-0{day}",
                input_receipt=parent,
            )
        receipt = data.inputs.freeze(
            {"item": ItemInput("item")}, information_cutoff="2020-01-03"
        )
        assert len(receipt.evidence["item"]["parent_receipts"]) == 2
        assert receipt.evidence["item"]["empty_item_build"] is not None
        reads = observe_reads(data, monkeypatch)
        assert data.inputs.is_current(receipt)
        assert reads == []
        assert data.inputs.get(receipt) == receipt
        assert data.inputs.verify(receipt)["valid"]
        # Integrity still rechecks the original retained bytes on every call.
        with sqlite3.connect(data.data_meta_path) as db:
            db.execute("update version_batches set payload=x'00' where commit_seq=1")
        with pytest.raises(RuntimeError, match="compression|checksum"):
            data.inputs.verify(receipt)


@pytest.mark.parametrize("selected_stale", [False, True])
def test_stale_parent_keeps_exact_selected_parent_fallback(
    tmp_path, monkeypatch, selected_stale
):
    data, frame, spec, raw = setup(tmp_path)
    with data:
        parent = data.inputs.freeze({"raw": raw}, information_cutoff="2020-01-03")
        data.items.ingest(
            "item", frame, available_date="2020-01-01", input_receipt=parent
        )
        data.raw.ingest(
            spec,
            frame.with_columns(pl.lit(2.0).alias("value")),
            ingested_at=datetime(2020, 1, 2, tzinfo=UTC),
        )
        latest = data.inputs.freeze({"raw": raw}, information_cutoff="2020-01-03")
        # Current B cannot hide selected stale A. Otherwise the new A supersedes
        # the old stale parent, which must not invalidate the selected result.
        output = (
            frame.with_columns(pl.lit("B").alias("asset_id"))
            if selected_stale
            else frame
        )
        data.items.ingest(
            "item", output, available_date="2020-01-02", input_receipt=latest
        )
        receipt = data.inputs.freeze(
            {"item": ItemInput("item")}, information_cutoff="2020-01-03"
        )
        assert len(receipt.evidence["item"]["parent_receipts"]) == 2
        reads = observe_reads(data, monkeypatch)
        assert data.inputs.is_current(receipt) is (not selected_stale)
        assert reads == ["item"]


def test_unbuilt_item_cannot_use_vacuous_parent_proof(tmp_path, monkeypatch):
    data, _, _, _ = setup(tmp_path)
    with data:
        receipt = data.inputs.freeze(
            {"item": ItemInput("item")}, information_cutoff="2020-01-03"
        )
        reads = observe_reads(data, monkeypatch)
        assert not data.inputs.is_current(receipt)
        assert reads == ["item"]
