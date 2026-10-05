from datetime import UTC, date, datetime
import os
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import polars as pl
import pytest

from bagelquant_data import DataLake, DataItemSpec, DatasetSpec, ItemInput, RawInput


def opened(root):
    return DataLake.open(
        data_meta_path=root / "data_meta.sqlite", lake_path=root / "lake"
    )


def panel(value=1.0):
    return pl.DataFrame(
        {"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [value]}
    )


def raw_spec():
    return DatasetSpec(
        "input",
        "by_date",
        date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"},
    )


def test_external_collection_has_no_unverified_historical_availability(tmp_path):
    lake = opened(tmp_path)
    lake.items.register(DataItemSpec("collected"))
    lake.items.ingest(
        "collected", panel(), ingested_at=datetime(2020, 2, 1, tzinfo=UTC)
    )
    assert lake.items.read("collected", strict=True).collect().is_empty()
    assert (
        lake.items.read("collected", view="snapshot", as_of="2020-01-31")
        .collect()
        .is_empty()
    )
    assert lake.items.read(
        "collected", view="snapshot", as_of="2020-02-01", strict=True
    ).collect()["value"].to_list() == [1.0]


def test_producer_receipt_and_range_withdrawal_are_durable(tmp_path):
    lake = opened(tmp_path)
    raw = raw_spec()
    lake.raw.ingest(raw, panel(), ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    request = RawInput("custom", "input")
    receipt = lake.inputs.freeze(
        {request.key: request}, information_cutoff="2020-01-02"
    )
    lake.items.register(
        DataItemSpec(
            "derived", (request,), producer_key="external", producer_revision="r1"
        )
    )
    with pytest.raises(ValueError, match="input_receipt"):
        lake.items.ingest("derived", panel(), available_date="2020-01-01")
    lake.items.ingest(
        "derived", panel(), available_date="2020-01-01", input_receipt=receipt
    )
    frozen = lake.inputs.freeze({"derived": ItemInput("derived")})
    lake.items.replace_range(
        "derived",
        panel().head(0),
        start="2020-01-01",
        end="2020-01-01",
        available_date="2020-01-02",
        input_receipt=receipt,
    )
    assert lake.items.read("derived").collect()["value"].to_list() == [1.0]
    assert lake.items.read("derived", view="snapshot", as_of="2020-01-02").collect()[
        "value"
    ].to_list() == [None]
    assert lake.inputs.read(frozen, "derived").collect()["value"].to_list() == [1.0]
    assert lake.items.status("derived")["frozen_receipt_id"] == receipt.receipt_id
    with sqlite3.connect(lake.data_meta_path) as db:
        assert {
            row[0]
            for row in db.execute(
                "select input_receipt_id from version_commits where source='items'"
            )
        } == {receipt.receipt_id}


@pytest.mark.parametrize(
    "reference",
    [
        "../../outside.parquet",
        "C:/outside.parquet",
        "/outside.parquet",
        "year=2020/month=02/data-" + "a" * 32 + ".parquet",
    ],
)
def test_invalid_registered_file_reference_fails_before_io(tmp_path, reference):
    lake = opened(tmp_path)
    spec = raw_spec()
    lake.raw.ingest(spec, panel(), ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update partition_manifest set generation_path=?", (reference,))
    with pytest.raises(ValueError, match="generation"):
        lake.raw.read("input", source="custom", view="latest").collect()
    report = lake.integrity.scan("input", source="custom", deep=True)
    assert not report["valid"]


def test_concurrent_direct_writers_fail_without_lost_publication(tmp_path, monkeypatch):
    lake = opened(tmp_path)
    spec = raw_spec()
    lake.raw.ingest(spec, panel(), ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    entered, release = Event(), Event()
    original = lake._parquet.commit_metadata

    def blocked(*args, **kwargs):
        entered.set()
        assert release.wait(10)
        return original(*args, **kwargs)

    monkeypatch.setattr(lake._parquet, "commit_metadata", blocked)
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(
            lake.raw.ingest,
            spec,
            panel(2.0),
            ingested_at=datetime(2020, 1, 2, tzinfo=UTC),
        )
        assert entered.wait(10)
        try:
            with pytest.raises(RuntimeError, match="already active"):
                opened(tmp_path).raw.ingest(spec, panel(3.0))
        finally:
            release.set()
        future.result()
    assert lake.raw.read("input", source="custom", view="latest").collect()[
        "value"
    ].to_list() == [2.0]


def test_process_termination_before_publish_and_backup_replay(tmp_path):
    lake = opened(tmp_path)
    spec = raw_spec()
    lake.raw.ingest(spec, panel(), ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    frozen = lake.inputs.freeze({"raw": RawInput("custom", "input")})
    code = """
import os, sys
from pathlib import Path
from datetime import date
import polars as pl
from bagelquant_data import DataLake
root=Path(sys.argv[1])
lake=DataLake.open(data_meta_path=root/'data_meta.sqlite', lake_path=root/'lake')
lake._parquet.commit_metadata=lambda *args, **kwargs: os._exit(17)
lake.raw.ingest(lake.raw.get('input', source='custom'),pl.DataFrame({'time':[date(2020,1,1)],'asset_id':['A'],'value':[9.]}))
"""
    result = subprocess.run(
        [sys.executable, "-c", code, str(tmp_path)], timeout=30, env=os.environ.copy()
    )
    assert result.returncode == 17
    reopened = opened(tmp_path)
    assert reopened.raw.read("input", source="custom", view="latest").collect()[
        "value"
    ].to_list() == [1.0]
    assert reopened.inputs.read(frozen, "raw").collect()["value"].to_list() == [1.0]
    bundle = reopened.integrity.backup(
        data_meta_path=tmp_path / "backup" / "data_meta.sqlite",
        lake_path=tmp_path / "backup" / "lake",
    )
    assert bundle["valid"]
    restored = DataLake.restore(
        backup_data_meta_path=tmp_path / "backup" / "data_meta.sqlite",
        backup_lake_path=tmp_path / "backup" / "lake",
        data_meta_path=tmp_path / "restored" / "data_meta.sqlite",
        lake_path=tmp_path / "restored" / "lake",
    )
    assert restored.inputs.read(frozen.receipt_id, "raw").collect()[
        "value"
    ].to_list() == [1.0]
    assert restored.integrity.active_update_leases() == []


def test_backup_rejects_corrupt_frozen_evidence(tmp_path):
    lake = opened(tmp_path)
    lake.raw.ingest(raw_spec(), panel(), ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    receipt = lake.inputs.freeze({"raw": RawInput("custom", "input")})
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute(
            "update frozen_inputs set payload_json='{}' where receipt_id=?",
            (receipt.receipt_id,),
        )
    with pytest.raises(RuntimeError, match="checksum"):
        lake.integrity.verify_backup()


def test_mid_producer_definition_change_rejects_publication_and_force_recomputes(tmp_path):
    lake = opened(tmp_path)
    lake.raw.ingest(raw_spec(), panel(), ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    request = RawInput("custom", "input")
    spec = DataItemSpec("derived", (request,), producer_key="p", producer_revision="1")
    lake.items.register(spec)
    def changed(context):
        lake.items.register(DataItemSpec("derived", (request,), producer_key="p", producer_revision="2"))
        return panel()
    lake.items.register_producer("p", "1", changed)
    with pytest.raises(RuntimeError, match="definition changed"):
        lake.items.update("derived", end="2020-01-01")
    assert lake.items.manifest("derived") == []
    calls = []
    def stable(context):
        calls.append(context.information_cutoff)
        return panel()
    lake.items.register_producer("p", "2", stable)
    lake.items.update("derived", end="2020-01-01")
    assert lake.items.update("derived", end="2020-01-01").status == "unchanged"
    assert len(calls) == 1
    lake.items.update("derived", end="2020-01-01", force=True)
    assert len(calls) == 2
