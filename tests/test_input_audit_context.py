"""Explicit audits use finite metadata views and retain every original check."""

from copy import deepcopy
import hashlib
import sqlite3

import pytest

from bagelquant_data import DataLake
from bagelquant_data import inputs
from bagelquant_data.storage import full_commit_checks, immutable_metadata
from test_input_read_context import frozen_months


def test_direct_audit_matches_entered_context_with_one_metadata_connection(tmp_path, monkeypatch):
    lake, frozen = frozen_months(tmp_path)
    reader = DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path,
                           read_only=True, runtime=True)
    expected_progress = []
    with reader.inputs.read_context(frozen):
        expected = reader.inputs.verify(frozen, progress=expected_progress.append)

    original_connect = sqlite3.connect
    connections = []
    def tracked(database, *args, **kwargs):
        connection = original_connect(database, *args, **kwargs)
        connections.append((str(database), connection))
        return connection
    monkeypatch.setattr(sqlite3, "connect", tracked)
    original_verify = reader.inputs.verify
    invocations = []
    def verify(*args, **kwargs):
        invocations.append(1)
        return original_verify(*args, **kwargs)
    monkeypatch.setattr(reader.inputs, "verify", verify)
    actual_progress = []
    assert reader.inputs.verify(frozen, progress=actual_progress.append) == expected
    assert invocations == [1]
    assert actual_progress == expected_progress
    assert len(connections) == 1
    assert connections[0][0] == lake.data_meta_path.resolve().as_uri() + "?mode=ro"
    assert reader.inputs._read_context() is None
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connections[0][1].execute("SELECT 1")


@pytest.mark.parametrize("damage", ["original", "metadata"])
def test_direct_audit_expires_proofs_and_rejects_later_corruption(tmp_path, monkeypatch, damage):
    from storage_fixtures import damage_original_segments

    lake, frozen = frozen_months(tmp_path)
    original_verify = inputs.verify_batch
    batches = []
    def tracked(*args, **kwargs):
        batches.append((args[1], args[2], args[3]))
        return original_verify(*args, **kwargs)
    monkeypatch.setattr(inputs, "verify_batch", tracked)
    expected = lake.inputs.verify(frozen)
    assert len(batches) == 3
    assert lake.inputs.verify(frozen) == expected
    assert len(batches) == 6
    with sqlite3.connect(lake.data_meta_path) as db:
        if damage == "original":
            damage_original_segments(db, "commit_seq=1", ())
        else:
            assert isinstance(frozen.evidence, immutable_metadata.FrozenEvidenceMapping)
            root = frozen.evidence.references["raw"]["ref"]
            db.execute("UPDATE immutable_metadata SET payload_json='{}' WHERE digest=?", (root,))
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        lake.inputs.verify(frozen)
    assert lake.inputs._read_context() is None


def test_direct_audit_cancellation_discards_partial_proofs(tmp_path, monkeypatch):
    lake, frozen = frozen_months(tmp_path)
    original_verify = inputs.verify_batch
    seen = []
    class Canceled(Exception):
        pass
    def tracked(*args, **kwargs):
        seen.append((args[1], args[2], args[3]))
        return original_verify(*args, **kwargs)
    def cancel_after_one(event):
        if event["completed"] == 1:
            raise Canceled()
    monkeypatch.setattr(inputs, "verify_batch", tracked)
    with pytest.raises(Canceled):
        lake.inputs.verify(frozen, progress=cancel_after_one)
    assert len(seen) == 1
    assert lake.inputs._read_context() is None
    assert lake.inputs.verify(frozen)["valid"]
    assert len(seen) == 4


@pytest.mark.parametrize("seal_count", [1, 12])
@pytest.mark.parametrize("damage", [None, "checksum", "membership"])
def test_repeated_full_commit_seals_share_retained_batches_and_validate_every_seal(
    tmp_path, monkeypatch, seal_count, damage,
):
    lake, original = frozen_months(tmp_path)
    captured = lake.inputs._capture
    materialized_seals = []
    def capture_with_retained_seals(*args, **kwargs):
        payload = captured(*args, **kwargs)
        evidence = payload["evidence"]["raw"]
        seals = []
        for index in range(seal_count):
            batch = evidence["batches"][index % len(evidence["batches"])]
            binding = {key: batch[key] for key in ("commit_seq", "partition_path", "content_hash", "row_count")}
            if index == seal_count - 1 and damage == "membership":
                binding["commit_seq"] += 1000
            seal = {"check_id": index + 1, "binding": {"batches": [binding]},
                    "row_count": batch["row_count"]}
            seal["digest"] = hashlib.sha256(immutable_metadata.canonical(seal).encode()).hexdigest()
            if index == seal_count - 1 and damage == "checksum":
                seal["digest"] = "bad"
            seals.append(seal)
        materialized_seals.extend(deepcopy(seals))
        evidence["full_commit_checks"] = seals
        return payload
    with monkeypatch.context() as patch:
        patch.setattr(lake.inputs, "_capture", capture_with_retained_seals)
        frozen = lake.inputs.freeze(original.requests, information_cutoff="2020-04-02")

    for materialized, lazy in zip(materialized_seals, frozen.evidence["raw"]["full_commit_checks"], strict=True):
        body = {key: value for key, value in lazy.items() if key != "digest"}
        expected_body = {key: value for key, value in materialized.items() if key != "digest"}
        assert full_commit_checks._json(body) == immutable_metadata.canonical(expected_body)
        assert lazy["digest"] == materialized["digest"]
        if lazy["digest"] != "bad":
            full_commit_checks.validate_seal(lazy)

    batch_reference = frozen.evidence["raw"]["batches"].reference["ref"]
    original_iter = immutable_metadata.LazyMetadataSequence.__iter__
    batch_traversals = []
    def tracked(sequence):
        if sequence.reference["ref"] == batch_reference:
            batch_traversals.append(sequence.reference["count"])
        yield from original_iter(sequence)
    monkeypatch.setattr(immutable_metadata.LazyMetadataSequence, "__iter__", tracked)
    if damage is not None:
        message = "seal checksum mismatch" if damage == "checksum" else "seal retained batch mismatch"
        with pytest.raises(RuntimeError, match=message):
            lake.inputs.verify(frozen)
    else:
        retained = deepcopy(frozen.evidence)
        batch_traversals.clear()
        assert lake.inputs.verify(frozen)["batch_count"] == 3
        assert batch_traversals == [3, 3]
        assert deepcopy(lake.inputs.get(frozen).evidence) == retained
    assert lake.inputs._read_context() is None


def test_direct_audit_empty_roots_keep_verification_error(tmp_path):
    lake, _ = frozen_months(tmp_path)
    with pytest.raises(ValueError, match="Verification requires at least one frozen input receipt"):
        lake.inputs.verify([])
    assert lake.inputs._read_context() is None
