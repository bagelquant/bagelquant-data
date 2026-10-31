"""Frozen completion admits authenticated timing witnesses, never newer proof."""
from datetime import date, timedelta
import hashlib
import json
import sqlite3

import polars as pl
import pytest

from bagelquant_data import ItemInput, ItemPublication
from bagelquant_data import input_index, inputs
from tests.test_frozen_completed_range import fixture as basic_fixture, source as basic_source
from tests.test_item_timing_reuse import Fixture


def source(f, *, cutoff=None):
    return f.lake.inputs.freeze({"x": ItemInput("item", view="versions")},
        information_cutoff=cutoff or f.known, retain_batch_bounds=True)


def forbid_readiness_work(lake, monkeypatch):
    def forbidden(*_a, **_k):
        pytest.fail("frozen completion read values, current flags or created evidence")
    for owner, method in ((lake.items, "read"), (lake.items, "completion_statuses"),
            (lake.items, "update"), (lake.inputs, "read"), (lake.inputs, "freeze"),
            (lake.inputs, "index_selection"), (inputs, "read_batch"),
            (input_index, "project_original")):
        monkeypatch.setattr(owner, method, forbidden)


@pytest.mark.parametrize("empty", [False, True])
def test_authenticated_timing_prefix_and_append_remain_complete_in_new_frozen_source(tmp_path, monkeypatch, empty):
    f = Fixture(tmp_path, empty=empty)
    original = source(f, cutoff=f.previous)
    assert f.lake.inputs.completed_range(original, "x", start=f.first, end=f.previous)
    old_files = {path: path.read_bytes() for path in f.lake.lake_path.rglob("*.parquet")}
    assert f.update().status == "success"
    assert f.calls == [(f.known, f.known)]
    assert all(path.read_bytes() == body for path, body in old_files.items())
    updated = source(f)
    binding = (updated.digest, updated.max_commit, updated.max_check_id, updated.information_cutoff)
    forbid_readiness_work(f.lake, monkeypatch)
    assert f.lake.inputs.completed_range(updated, "x", start=f.first, end=f.known)
    assert f.lake.inputs.completed_range(original, "x", start=f.first, end=f.previous)
    assert not f.lake.inputs.completed_range(original, "x", start=f.first, end=f.known)
    assert not f.lake.inputs.completed_range(updated, "x", start=f.first, end=f.known, as_of=f.previous)
    # The initial native completion has no dated output-check witness. An
    # earlier explicit as_of must retain that conservative unavailable state.
    assert not f.lake.inputs.completed_range(updated, "x", start=f.first, end=f.previous, as_of=f.previous)
    assert f.lake.inputs.completed_range(updated, "x", start=f.first, end=f.known, as_of=f.known)
    assert not f.lake.inputs.completed_range(updated, "x", start=f.first, end=f.known + timedelta(days=1))
    saved = f.lake.inputs.get(updated.receipt_id)
    assert (saved.digest, saved.max_commit, saved.max_check_id, saved.information_cutoff) == binding
    assert f.calls == [(f.known, f.known)]


@pytest.mark.parametrize("empty", [False, True])
def test_timing_witness_cannot_certify_failed_append_or_later_row_only_check(tmp_path, monkeypatch, empty):
    f = Fixture(tmp_path, empty=empty)
    f.fail_tail = True
    with pytest.raises(RuntimeError, match="tail interrupted"):
        f.update()
    partial = source(f)
    assert not f.lake.inputs.completed_range(partial, "x", start=f.first, end=f.known)
    assert f.lake.inputs.completed_range(partial, "x", start=f.first, end=f.previous)
    f.fail_tail = False
    f.update()
    complete = source(f)
    parent = f.lake.inputs.get(f.lake.items.completed_builds("item")[-1]["frozen_receipt_id"])
    rows = f.lake.items.read("item", view="snapshot", as_of=f.known).collect().select("time", "asset_id", "value")
    f.lake.items.ingest("item", rows, available_date=f.known, input_receipt=parent)
    unchecked = source(f)
    forbid_readiness_work(f.lake, monkeypatch)
    assert f.lake.inputs.completed_range(complete, "x", start=f.first, end=f.known)
    assert not f.lake.inputs.completed_range(partial, "x", start=f.first, end=f.known)
    assert not f.lake.inputs.completed_range(unchecked, "x", start=f.first, end=f.known)


@pytest.mark.parametrize("missing", ["proof", "output_check", "parent_binding", "timing_authority"])
def test_timing_completion_requires_its_authenticated_original_authority(tmp_path, missing):
    f = Fixture(tmp_path)
    original_build = f.lake.items.completed_builds("item")[-1]["id"]
    f.update()
    frozen = source(f)
    with sqlite3.connect(f.lake.data_meta_path) as db:
        rows = db.execute("select a.build_id,a.payload_json from item_completion_authorities a "
            "where a.build_id>?", (original_build,)).fetchall()
        assert rows
        if missing == "proof":
            db.executemany("delete from item_completion_proofs where build_id=?", [(row[0],) for row in rows])
        else:
            for build_id, payload in rows:
                body = json.loads(payload)
                if missing == "output_check":
                    body.pop("output_check_bound")
                elif missing == "parent_binding":
                    if "timing" not in body:
                        continue
                    body["timing"]["current_source"]["digest"] = "0" * 64
                else:
                    if "timing" not in body:
                        continue
                    body["timing"]["scope"] = "0" * 64
                encoded = json.dumps(body, sort_keys=True, separators=(",", ":"))
                db.execute("update item_completion_authorities set payload_json=?,digest=? where build_id=?",
                    (encoded, hashlib.sha256(encoded.encode()).hexdigest(), build_id))
    if missing in {"proof", "output_check"}:
        assert not f.lake.inputs.completed_range(frozen, "x", start=f.first, end=f.known)
    else:
        with pytest.raises(RuntimeError, match="authority.*damaged"):
            f.lake.inputs.completed_range(frozen, "x", start=f.first, end=f.known)


def publication_with_initial_scope(tmp_path, monkeypatch, queries, *, whole_range=True):
    lake = basic_fixture(tmp_path)
    lake.items.initialize("value", start="2024-01-02", end="2024-01-03")
    original = basic_source(lake)
    parent = lake.inputs.get(next(iter(original.evidence["x"]["parent_receipts"])))
    current_parent = lake.inputs.freeze(parent.requests, information_cutoff="2024-01-03")
    original_ingest = lake.items._ingest

    def initial_witness(*args, **options):
        # Fault injection changes only the registered initial witness shape.
        # The actual complete publisher, original proof and frozen Source stay real.
        options["check_requests"] = queries
        return original_ingest(*args, **options)

    with monkeypatch.context() as witness:
        witness.setattr(lake.items, "_ingest", initial_witness)
        with lake.items.publication(input_receipt=current_parent) as publisher:
            publisher.publish([ItemPublication("value", pl.DataFrame({"source_time": [date(2024, 1, 2)],
                "asset_id": ["A"], "close": [10.], "version_available_date": [date(2024, 1, 3)]}),
                start="2024-01-02", end="2024-01-03" if whole_range else "2024-01-02",
                complete_at="2024-01-03")])
    return lake, basic_source(lake)


@pytest.mark.parametrize("queries", [[], [{"other_scope": "registered"}], [
    {"item_range": {"start": "2024-01-02", "end": "2024-01-02"}}, {"other_scope": "registered"}],
    [{"item_range": {"start": "2024-01-02", "end": "2024-01-02"}, "other_scope": "registered"}]])
@pytest.mark.parametrize("whole_range", [False, True])
def test_unknown_or_row_witness_requires_complete_consumer_range(tmp_path, monkeypatch, queries, whole_range):
    lake, frozen = publication_with_initial_scope(tmp_path, monkeypatch, queries, whole_range=whole_range)
    forbid_readiness_work(lake, monkeypatch)
    assert lake.inputs.completed_range(frozen, "x", start="2024-01-02", end="2024-01-03") is whole_range
    assert lake.inputs.completed_range(frozen, "x", start="2024-01-02", end="2024-01-02")


@pytest.mark.parametrize("queries", ["invalid", {"item_range": {"start": "2024-01-02", "end": "2024-01-03"}},
    [None], [1], [[]], [{}], [{"item_range": None}], [{"item_range": {}}],
    [{"item_range": {"start": None, "end": "2024-01-03"}}],
    [{"item_range": {"start": "not-a-date", "end": "2024-01-03"}}],
    [{"item_range": {"start": "2024-01-03", "end": "2024-01-02"}}],
    [{"other_scope": "registered"}, {"item_range": {"start": "not-a-date", "end": "2024-01-03"}}]])
def test_malformed_witness_cannot_borrow_a_complete_certificate(tmp_path, monkeypatch, queries):
    lake, frozen = publication_with_initial_scope(tmp_path, monkeypatch, queries)
    forbid_readiness_work(lake, monkeypatch)
    assert not lake.inputs.completed_range(frozen, "x", start="2024-01-02", end="2024-01-03")
