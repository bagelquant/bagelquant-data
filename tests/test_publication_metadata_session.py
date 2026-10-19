"""Serial publication reuses metadata connections without retaining a snapshot."""

from concurrent.futures import CancelledError
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
import sqlite3

import polars as pl
import pytest

from bagelquant_data import (
    DataItemSpec, DataLake, DatasetSpec, ItemPublication, RawInput,
)


@pytest.fixture
def publication_lake(tmp_path):
    with DataLake.open(
        data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake"
    ) as lake:
        observed = [date(2018 + offset // 12, 1 + offset % 12, 1)
                    for offset in range(32)]
        raw = pl.DataFrame({"time": observed, "asset_id": ["A"] * len(observed),
                            "value": [1.] * len(observed)})
        lake.raw.ingest(
            DatasetSpec("raw", "by_date", date_kind="calendar",
                        field_mappings={"time": "time", "asset_id": "asset_id"}),
            raw, mode="initialize", ingested_at=datetime(2020, 1, 31, tzinfo=UTC),
        )
        request = RawInput("custom", "raw", view="versions")
        receipt = lake.inputs.freeze({"raw": request}, information_cutoff="2020-01-31")
        for name in ("first", "second"):
            lake.items.register(DataItemSpec(name, (request,)))
        days = [date(2020, 1, 1) + timedelta(days=offset) for offset in range(31)]
        output = pl.DataFrame({"time": days, "asset_id": ["A"] * len(days),
                               "value": [1.] * len(days), "version_available_date": days})
        yield lake, receipt, output


def capture_connections(lake, monkeypatch):
    original = lake._data_meta._new_connection
    connections = []

    def opened():
        connection = original()
        connections.append(connection)
        return connection

    monkeypatch.setattr(lake._data_meta, "_new_connection", opened)
    return connections


def assert_closed(connections):
    assert connections
    for connection in connections:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            connection.execute("select 1")


def test_many_timing_dates_use_one_connection_and_keep_original_baseline(
    publication_lake, monkeypatch
):
    lake, receipt, frame = publication_lake
    original = (receipt.receipt_id, receipt.digest, receipt.max_commit, receipt.max_check_id)
    with monkeypatch.context() as patch:
        connections = capture_connections(lake, patch)
        with lake.items.publication(input_receipt=receipt) as publisher:
            publisher.publish([ItemPublication(name, frame) for name in ("first", "second")])
            assert len(connections) == 1
            assert not connections[0].in_transaction
        assert_closed(connections)

    assert not publisher.active and not lake.integrity.active_update_leases()
    retained = lake.inputs.get(receipt)
    assert (retained.receipt_id, retained.digest, retained.max_commit, retained.max_check_id) == original
    assert lake.inputs.is_current(retained)
    for name in ("first", "second"):
        versions = lake.items.read(name, view="versions").collect()
        assert versions["_baseline"].to_list() == [True] * 31
        assert versions["value"].to_list() == [1.] * 31
        assert lake.items.read(name, view="snapshot", as_of="2020-01-31", strict=True).collect().is_empty()


def test_competing_definition_change_is_visible_between_publications(
    publication_lake, monkeypatch
):
    lake, receipt, frame = publication_lake
    definition = lake.items.status("second")["definition_hash"]
    with DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path) as other:
        with monkeypatch.context() as patch:
            connections = capture_connections(lake, patch)
            with pytest.raises(RuntimeError, match="definition changed"):
                with lake.items.publication(input_receipt=receipt) as publisher:
                    publisher.publish([ItemPublication("first", frame)])
                    assert not connections[0].in_transaction
                    other.items.register(replace(other.items.get("second"),
                                                 metadata={"numerical_revision": "changed"}))
                    assert lake.items.status("second")["definition_hash"] != definition
                    publisher.publish([ItemPublication("second", frame,
                                                        expected_definition_hash=definition)])
            assert_closed(connections)

    assert not publisher.active and not lake.integrity.active_update_leases()
    assert lake.items.status("first")["row_count"] == 31
    assert not lake.items.builds("second")


def test_original_source_is_revalidated_between_publications(publication_lake):
    lake, receipt, frame = publication_lake
    with pytest.raises(RuntimeError, match="receipt checksum mismatch"):
        with lake.items.publication(input_receipt=receipt) as publisher:
            publisher.publish([ItemPublication("first", frame)])
            # Corruption is isolated fault injection. Reusing the connection
            # must not retain a pre-write Source validity cache or read view.
            with sqlite3.connect(lake.data_meta_path, timeout=.1) as external:
                external.execute(
                    "update frozen_inputs set payload_json='{}' where receipt_id=?",
                    (receipt.receipt_id,),
                )
            publisher.publish([ItemPublication("second", frame)])
    assert not publisher.active and not lake.integrity.active_update_leases()
    assert lake.items.status("first")["row_count"] == 31
    assert not lake.items.builds("second")


@pytest.mark.parametrize("cancel", [False, True])
def test_failed_or_cancelled_publication_closes_session_and_retains_commits(
    publication_lake, monkeypatch, cancel
):
    lake, receipt, frame = publication_lake
    cancellation = [False]
    expected = CancelledError if cancel else ValueError
    with monkeypatch.context() as patch:
        connections = capture_connections(lake, patch)
        with pytest.raises(expected):
            with lake.items.publication(input_receipt=receipt,
                                        cancelled=lambda: cancellation[0]) as publisher:
                publisher.publish([ItemPublication("first", frame)])
                assert not connections[0].in_transaction
                if cancel:
                    cancellation[0] = True
                else:
                    raise ValueError("simulated caller failure")
        assert_closed(connections)

    assert not publisher.active and not lake.integrity.active_update_leases()
    assert lake.items.status("first")["row_count"] == 31
    assert not lake.items.builds("second")


def test_rolled_back_helper_does_not_poison_publication_or_hold_a_snapshot(
    publication_lake
):
    lake, receipt, frame = publication_lake
    with lake.items.publication(input_receipt=receipt) as publisher:
        with pytest.raises(RuntimeError, match="rollback"):
            with lake._data_meta.atomic_transaction() as connection:
                connection.execute("insert into data_meta_state values('probe.rollback','one','now')")
                raise RuntimeError("rollback")
        assert not connection.in_transaction
        with sqlite3.connect(lake.data_meta_path, timeout=.1) as external:
            assert external.execute("select value from data_meta_state where key='probe.rollback'").fetchone() is None
            external.execute("insert into data_meta_state values('probe.rollback','two','now')")
        assert lake._data_meta._rows("select value from data_meta_state where key='probe.rollback'") == [{"value": "two"}]
        publisher.publish([ItemPublication("first", frame)])
    assert lake.items.status("first")["row_count"] == 31
    assert not lake.integrity.active_update_leases()
