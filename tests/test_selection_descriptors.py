"""Registered finite proofs are shared without reading values or inventing axes."""
from dataclasses import replace
from contextlib import ExitStack
from datetime import date, timedelta
import json
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, ExecutionOptions, InputSelectionScope, ItemInput, input_index
from bagelquant_data import inputs

FIELDS = ("time", "asset_id", "value", "available_date", "observation_date", "base_available_date")
DAY = date(2024, 1, 1)


def setup(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("x"))
    frame = pl.DataFrame({"time": [DAY, DAY + timedelta(days=1)], "asset_id": ["A", "B"], "value": [2.0, 3.0],
        "available_date": [DAY, DAY + timedelta(days=1)], "observation_date": [DAY, DAY + timedelta(days=1)],
        "base_available_date": [DAY, DAY + timedelta(days=1)]})
    lake.items.ingest("x", frame)
    def freeze(cutoff=DAY + timedelta(days=8)):
        return lake.inputs.freeze({"x": ItemInput("x", start=DAY, end=DAY + timedelta(days=6), view="history")},
            information_cutoff=cutoff, retain_batch_bounds=True)
    root = freeze()
    scope = InputSelectionScope("x", start=DAY, end=DAY + timedelta(days=1), fields=FIELDS,
        coordinates=pl.DataFrame({"time": [DAY, DAY + timedelta(days=1)], "asset_id": ["A", "ABSENT"]}))
    return lake, frame, root, scope, freeze


def forbid_values(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("Ordinary descriptors consumed values, selector IPC or wrote indexes")
    for module, name in ((input_index, "project_original"), (input_index, "project_original"),
            (input_index, "read_selector"), (inputs, "read_batch"), (input_index, "initialize")):
        monkeypatch.setattr(module, name, forbidden)


def test_compact_descriptors_are_known_without_value_preparation(tmp_path, monkeypatch):
    lake, frame, root, scope, freeze = setup(tmp_path)
    unknown = lake.inputs.selection_descriptors(root, [scope])[0]
    assert unknown["status"] == "known" and unknown["content_token"] and unknown["proof_source"]
    if scope.coordinates is not None:
        lake.inputs.prepare_coordinate_selector(scope.coordinates)
    prepared, = lake.inputs.prepare_range_selections(root, [scope])
    token = prepared["content_token"]
    lake.items.ingest("x", frame.head(1).with_columns(pl.lit(DAY + timedelta(days=3)).alias("time"),
        pl.lit(DAY + timedelta(days=3)).alias("available_date")))
    current = freeze()
    tail = replace(scope, start=DAY + timedelta(days=3), end=DAY + timedelta(days=3),
        coordinates=pl.DataFrame({"time": [DAY + timedelta(days=3)], "asset_id": ["A"]}))
    with monkeypatch.context() as patch:
        forbid_values(patch)
        same, missing = lake.inputs.selection_descriptors(current, [scope, tail])
        assert same["content_token"] == token and same["proof_source"]["id"] == root.receipt_id
        assert same["evidence_key"] == unknown["evidence_key"] and same["request_key"] == unknown["request_key"]
        assert same["source"] == {"id": current.receipt_id, "digest": current.digest}
        assert same["as_of"] is None and same["coordinate_identity"]
        assert missing["status"] == "known" and missing["content_token"]
        assert json.loads(json.dumps(same)) == same
    progress = []
    got = lake.inputs.prepare_range_selections(current, [scope, tail])
    assert got[0]["content_token"] == token and got[1]["content_token"]
    assert not progress
    assert lake.inputs.get(root).digest == root.digest


def test_registered_projection_memo_is_finite_reuses_hits_and_expires(tmp_path, monkeypatch):
    lake, _, root, scope, _ = setup(tmp_path)
    if scope.coordinates is not None:
        lake.inputs.prepare_coordinate_selector(scope.coordinates)
    prepared, = lake.inputs.prepare_range_selections(root, [scope])
    token = prepared["content_token"]
    calls = []
    original = input_index.projection
    monkeypatch.setattr(input_index, "projection", lambda *a, **k: (calls.append(1), original(*a, **k))[1])
    def identity():
        return lake.inputs.selection_identity(root, "x", start=scope.start, end=scope.end,
            fields=FIELDS, coordinates=scope.coordinates)
    with lake.inputs.read_context(root):
        assert identity() == token
        assert identity() == token
        with lake.inputs.read_context(root):
            assert identity() == token
        assert len(calls) == 1
        context = lake.inputs._read_context()
        assert context is not None
        assert sum(size for _, size in context.selections.values()) <= ExecutionOptions().max_buffer_bytes // 64
        with pytest.raises(RuntimeError, match="checksum"):
            lake.inputs.selection_identity(replace(root, digest="f" * 64), "x", fields=FIELDS)
    assert identity() == token and len(calls) == 2


@pytest.mark.parametrize("rollback", [False, True])
def test_own_write_disables_even_prewrite_and_rollback_memo(tmp_path, monkeypatch, rollback):
    lake, _, root, scope, _ = setup(tmp_path)
    if scope.coordinates is not None:
        lake.inputs.prepare_coordinate_selector(scope.coordinates)
    lake.inputs.prepare_range_selections(root, [scope])
    calls = []
    original = input_index.projection
    monkeypatch.setattr(input_index, "projection", lambda *a, **k: (calls.append(1), original(*a, **k))[1])
    with ExitStack() as stack:
        stack.enter_context(lake.inputs.publication_guard(root))
        stack.enter_context(lake.inputs.read_context(root))
        def identity():
            return lake.inputs.selection_identity(root, "x", start=scope.start, end=scope.end,
                fields=FIELDS, coordinates=scope.coordinates)
        assert identity()
        context = lake.inputs._read_context()
        assert context is not None and context.database is not None
        if rollback:
            context.database.execute("savepoint owner_probe")
        context.database.execute("update input_projection_index set identity=identity")
        if rollback:
            context.database.execute("rollback to owner_probe")
        assert not context.memo_available
        assert identity() and len(calls) == 2


def test_descriptors_resolve_without_selector_decode_and_follow_implicit_cutoff(tmp_path, monkeypatch):
    lake, frame, original, scope, freeze = setup(tmp_path)
    scope = replace(scope, view="snapshot")
    assert scope.coordinates is not None
    lake.inputs.prepare_coordinate_selector(scope.coordinates)
    lake.inputs.prepare_range_selections(original, [scope])
    fixed_scope = replace(scope, as_of=DAY + timedelta(days=8))
    old, fixed = lake.inputs.selection_descriptors(original, [scope, fixed_scope])
    lake.items.ingest("x", frame.head(1).with_columns(pl.lit(99.0).alias("value")),
        available_date=DAY + timedelta(days=9))
    current = freeze(DAY + timedelta(days=10))
    # Both needed summaries are explicitly prepared; the fixed earlier cutoff
    # retains the original event while the implicit cutoff admits the revision.
    lake.inputs.prepare_range_selections(current, [scope, fixed_scope])
    forbid_values(monkeypatch)
    updated, unchanged = lake.inputs.resolve_selection_descriptors(current, [old, fixed])
    assert updated["requested_cutoff"] == str(DAY + timedelta(days=10))
    assert updated["content_token"] != old["content_token"]
    assert unchanged["as_of"] == str(DAY + timedelta(days=8))
    assert unchanged["content_token"] == fixed["content_token"]
    assert lake.inputs.resolve_selection_descriptors(original, [old])[0] == old


@pytest.mark.parametrize("field", ["content_token", "evidence_key", "request_key", "canonical_cutoff", "coordinate_identity", "proof_source"])
def test_forged_descriptor_cannot_resolve_a_different_registered_scope(tmp_path, monkeypatch, field):
    lake, _, root, scope, _ = setup(tmp_path)
    if scope.coordinates is not None:
        lake.inputs.prepare_coordinate_selector(scope.coordinates)
    lake.inputs.prepare_range_selections(root, [scope])
    descriptor, = lake.inputs.selection_descriptors(root, [scope])
    forged = dict(descriptor)
    forged[field] = ({"id": root.receipt_id, "digest": "f" * 64, "alias": "x"} if field == "proof_source"
        else "f" * 64 if field != "canonical_cutoff" else "2024-01-31")
    forbid_values(monkeypatch)
    with pytest.raises((ValueError, RuntimeError), match="Descriptor|descriptor|selector"):
        lake.inputs.resolve_selection_descriptors(root, [forged])


def test_missing_coordinate_authority_and_tiny_budget_do_not_warm(tmp_path, monkeypatch):
    lake, _, root, scope, _ = setup(tmp_path)
    unknown, = lake.inputs.selection_descriptors(root, [scope])
    assert unknown["status"] == "known"
    if scope.coordinates is not None:
        lake.inputs.prepare_coordinate_selector(scope.coordinates)
    lake.inputs.prepare_range_selections(root, [scope])
    known, = lake.inputs.selection_descriptors(root, [scope])
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("delete from input_projection_selectors")
    forbid_values(monkeypatch)
    # A saved compact proof can resolve its identical immutable Source from
    # metadata. Explicit audit still needs the retained axis authority.
    assert lake.inputs.resolve_selection_descriptors(root, [known]) == (known,)
    monkeypatch.undo()
    with pytest.raises(RuntimeError, match="selector"):
        lake.inputs.verify(root)
    with pytest.raises(MemoryError, match="admission"):
        lake.inputs.selection_descriptors(root, [scope], config=ExecutionOptions(max_buffer_bytes=4096))
    with pytest.raises(MemoryError, match="admission"):
        lake.inputs.resolve_selection_descriptors(root, [known], config=ExecutionOptions(max_buffer_bytes=4096))


@pytest.mark.parametrize("scopes", [[], [InputSelectionScope("x")] * 17,
    [InputSelectionScope("x"), InputSelectionScope("y")], [InputSelectionScope("x", coordinate_selector="f" * 64)]])
def test_group_contract_rejects_nonfinite_or_selector_only_scopes(tmp_path, scopes):
    lake, _, root, _, _ = setup(tmp_path)
    with pytest.raises(ValueError):
        lake.inputs.selection_descriptors(root, scopes)


def test_cancel_before_read_and_cached_cancel_preserve_sources(tmp_path, monkeypatch):
    lake, _, root, scope, _ = setup(tmp_path)
    if scope.coordinates is not None:
        lake.inputs.prepare_coordinate_selector(scope.coordinates)
    lake.inputs.prepare_range_selections(root, [scope])
    before = lake.inputs.get(root)
    def canceled():
        raise RuntimeError("cancel owner probe")
    forbid_values(monkeypatch)
    with pytest.raises(RuntimeError, match="cancel"):
        lake.inputs.selection_descriptors(root, [scope], check_canceled=canceled)
    with lake.inputs.read_context(root, check_canceled=lambda: None):
        assert lake.inputs.selection_descriptors(root, [scope])[0]["status"] == "known"
        context = lake.inputs._read_context()
        assert context is not None
        context.check_canceled = canceled
        with pytest.raises(RuntimeError, match="cancel"):
            lake.inputs.selection_descriptors(root, [scope])
    assert lake.inputs.get(root) == before


def test_scalar_descriptors_preserve_value_token_and_no_row_index_path(tmp_path, monkeypatch):
    lake, _, root, _, _ = setup(tmp_path)
    scope = InputSelectionScope("x", start=DAY, end=DAY + timedelta(days=1), fields=None)
    if scope.coordinates is not None:
        lake.inputs.prepare_coordinate_selector(scope.coordinates)
    prepared, = lake.inputs.prepare_range_selections(root, [scope])
    token = prepared["content_token"]
    forbid_values(monkeypatch)
    descriptor, = lake.inputs.selection_descriptors(root, [scope])
    assert descriptor["fields"] is None and descriptor["content_token"] == token
    assert json.loads(descriptor["request_key"])["projection"] == "value.v1"
    assert lake.inputs.resolve_selection_descriptors(root, [descriptor]) == (descriptor,)


def test_optional_exact_selection_cache_does_not_change_compact_descriptor(tmp_path, monkeypatch):
    lake, _, root, _, _ = setup(tmp_path)
    scope = InputSelectionScope("x", fields=None)
    if scope.coordinates is not None:
        lake.inputs.prepare_coordinate_selector(scope.coordinates)
    prepared, = lake.inputs.prepare_range_selections(root, [scope])
    token = prepared["content_token"]
    issued, = lake.inputs.selection_descriptors(root, [scope])
    # Retain the same legal scalar proof through the older exact-source cache.
    key = inputs._selection_key(root.requests["x"], root.information_cutoff, values_only=True)
    with lake._data_meta.connect() as db:
        input_index.save_selection(db, root.digest, "x", key, {"identity": token, "parents": [], "has_rows": True})
    forbid_values(monkeypatch)
    with lake.inputs.read_context(root):
        assert lake.inputs.selection_identity(root, "x", values_only=True) == token
        # Descriptor requests use the summaries-only channel. Exercise the
        # same exact-source hit before its later descriptor memo hit too.
        actual, = lake.inputs.selection_descriptors(root, [scope])
        repeated, = lake.inputs.selection_descriptors(root, [scope])
        assert actual == repeated == issued
        assert lake.inputs.resolve_selection_descriptors(root, [actual]) == (actual,)
