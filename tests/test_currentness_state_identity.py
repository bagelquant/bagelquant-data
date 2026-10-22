"""A revision identity binds owner state; it does not assert currentness."""
from contextlib import closing
import sqlite3

import pytest

from bagelquant_data import DataLake, ExecutionOptions, input_read_boundary
from test_input_read_context import frozen_months


@pytest.mark.parametrize("mutation", [
    "UPDATE datasets SET update_incomplete=1-update_incomplete WHERE source='custom' AND name='values'",
    "UPDATE datasets SET publication_revision=publication_revision+1 WHERE source='custom' AND name='values'",
    "UPDATE item_definitions SET active=0 WHERE name='derived'",
    "UPDATE item_dependencies SET dataset='different' WHERE item_name='derived'",
    "UPDATE dataset_schemas SET schema_hash='different' WHERE source='custom' AND dataset='values'",
    "UPDATE node_completions SET checked_target='2020-04-02' WHERE node_key='items/derived'",
    "UPDATE node_completions SET result_ref='different' WHERE node_key='items/derived'",
    "UPDATE item_builds SET end_date='2020-04-02' WHERE name='derived'",
    "UPDATE item_completion_proofs SET digest='different'",
    "UPDATE item_completion_authorities SET payload_json='{}'",
    "UPDATE item_pending_outputs SET payload_json='{}'",
    "UPDATE version_commits SET pit_date='2020-03-03' WHERE source='custom' AND dataset='values'",
])
def test_identity_covers_mutable_publication_and_completion_authorities(tmp_path, mutation):
    lake, frozen = frozen_months(tmp_path)
    lake.items.update("derived", start="2020-01-01", end="2020-03-31")
    with lake._data_meta.publication_read_view() as db, lake.inputs.read_context(frozen):
        before = lake.inputs.currentness_identity(frozen)
        db.execute("SAVEPOINT changed_state")
        assert db.execute(mutation).rowcount > 0
        assert lake.inputs.currentness_identity(frozen) != before
        db.execute("ROLLBACK TO changed_state")
        assert lake.inputs.currentness_identity(frozen) == before


def test_identity_reads_no_values_and_ignores_new_unrelated_frozen_sources(tmp_path, monkeypatch):
    lake, frozen = frozen_months(tmp_path)
    before = lake.inputs.currentness_identity(frozen)
    assert lake.inputs.currentness_identity() == before
    def forbidden(*_args, **_kwargs):
        pytest.fail("owner revision identity consumed values, audited or recaptured inputs")
    with monkeypatch.context() as guard:
        for name in ("read", "verify", "_capture", "_read_frame"):
            guard.setattr(type(lake.inputs), name, forbidden)
        assert lake.inputs.currentness_identity(frozen) == before
    lake.inputs.freeze(frozen.requests, information_cutoff="2020-04-02")
    assert lake.inputs.currentness_identity(frozen) == before


def test_identity_respects_entered_wal_snapshot_and_fresh_view(tmp_path):
    lake, frozen = frozen_months(tmp_path)
    with lake.inputs.read_context(frozen):
        before = lake.inputs.currentness_identity(frozen)
        with closing(sqlite3.connect(lake.data_meta_path)) as writer:
            writer.execute("UPDATE datasets SET update_incomplete=1-update_incomplete WHERE source='custom' AND name='values'")
            writer.commit()
        assert lake.inputs.currentness_identity(frozen) == before
    assert lake.inputs.currentness_identity(frozen) != before


def test_identity_rejects_historical_boundaries_and_propagates_admission_and_cancel(tmp_path):
    lake, frozen = frozen_months(tmp_path)
    with input_read_boundary(lake.data_meta_path, frozen.max_commit):
        bounded = DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path,
            read_only=True, runtime=True)
        with pytest.raises(ValueError, match="unbounded current"):
            bounded.inputs.currentness_identity(frozen)
    with pytest.raises(MemoryError, match="max_buffer_bytes"):
        lake.inputs.currentness_identity(frozen, config=ExecutionOptions(max_buffer_bytes=1))
    def cancel():
        raise InterruptedError("canceled state identity")
    with pytest.raises(InterruptedError, match="canceled state identity"):
        lake.inputs.currentness_identity(frozen, check_canceled=cancel)
