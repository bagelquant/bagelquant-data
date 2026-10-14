"""General impact scopes distinguish sparse-anchor visibility ceilings."""
import json

import polars as pl
import pytest

from bagelquant_data import input_index
from test_general_asset_key_appends import FIRST, OLD, TAIL, Fixture, forbid, received


def sparse_transition(fixture):
    fixture.lake.raw.ingest(fixture.general, pl.concat([fixture.before,
        pl.DataFrame({"asset_id": ["B"], "market": ["growth"], "weight": [2.], "unused": ["new"]})]),
        ingested_at=received(4))
    fixture.lake.raw.ingest(fixture.specs["daily"], fixture.rows("daily", TAIL, "B"), ingested_at=received(4))
    fixture.lake.raw.ingest(fixture.specs["secondary"], fixture.rows("secondary", TAIL, "A"),
        ingested_at=received(4))
    # A later first-known witness for an old compound-key observation changes
    # selected anchor evidence without adding the General's new asset key.
    fixture.lake.raw.ingest(fixture.specs["secondary"], fixture.rows("secondary", FIRST, "A"),
        ingested_at=received(4))
    fixture.lake.raw.ingest(fixture.specs["secondary"], fixture.rows("secondary", TAIL, "A").clear(),
        ingested_at=received(4))


def test_full_and_atomic_general_scopes_keep_distinct_sparse_anchor_proofs(tmp_path, monkeypatch):
    fixture = Fixture(tmp_path, mixed=True)
    sparse_transition(fixture)
    prepared = fixture.prepare()
    assert all(row["status"] == "known" for row in prepared["key_appends"]["states"])
    with fixture.lake._data_meta.connect() as db:
        rows = db.execute("select request_key,identity from input_projection_index where version='general_asset_append.v1'").fetchall()
        sources = [fixture.lake.inputs.get(json.loads(row["identity"])["current"]["id"]) for row in rows]
    assert len({source.max_commit for source in sources}) > 1
    # Their former General-only locator would collide despite different anchor
    # witnesses. Global counters are absent from the new namespace.
    keys = [json.loads(row["request_key"]) for row in rows]
    assert len({key.pop("anchor_evidence") for key in keys}) > 1
    assert len({json.dumps(key, sort_keys=True) for key in keys}) == 1
    originals = {path: path.read_bytes() for path in fixture.lake.lake_path.rglob("*.parquet")}
    old = fixture.lake.items.read("item", view="versions").collect()
    with monkeypatch.context() as patch:
        patch.setattr(input_index, "project_original", forbid)
        patch.setattr(input_index, "read_selector", forbid)
        assert fixture.plan() == [{"mode": "incremental", "start": str(TAIL), "end": str(TAIL)}]
        assert fixture.prepare()["key_appends"]["prepared"] == 0
    assert not fixture.calls
    assert fixture.lake.items.update("item", start=FIRST, end=TAIL).status == "success"
    assert fixture.calls == [(TAIL, TAIL)]
    assert fixture.lake.items.read("item", end=OLD, view="versions").collect().equals(old)
    assert all(path.read_bytes() == body for path, body in originals.items())


def full_descriptor(fixture):
    with fixture.lake._data_meta.connect() as db:
        rows = [dict(row) for row in db.execute("select * from input_projection_index where version='general_asset_append.v1'")]
    return max(rows, key=lambda row: fixture.lake.inputs.get(json.loads(row["identity"])["current"]["id"]).max_commit)


def replace_descriptor(fixture, row, body, key):
    updated = {**row, "identity": json.dumps(body, sort_keys=True, separators=(",", ":")),
        "request_key": json.dumps(key, sort_keys=True, separators=(",", ":"))}
    digest = input_index._digest({name: value for name, value in updated.items() if name != "digest"})
    with fixture.lake._data_meta.connect() as db:
        db.execute("update input_projection_index set identity=?,request_key=?,digest=? where evidence_key=? and request_key=?",
            (updated["identity"], updated["request_key"], digest, row["evidence_key"], row["request_key"]))


def test_existing_general_only_descriptor_audits_exact_sources_but_never_admits_current(tmp_path, monkeypatch):
    fixture = Fixture(tmp_path, mixed=True)
    sparse_transition(fixture)
    fixture.prepare()
    row = full_descriptor(fixture)
    body, key = json.loads(row["identity"]), json.loads(row["request_key"])
    body.pop("anchor_evidence")
    key.pop("anchor_evidence")
    replace_descriptor(fixture, row, body, key)
    source = fixture.lake.inputs.get(row["source_receipt_id"])
    assert fixture.lake.inputs.verify(source)["valid"]
    with monkeypatch.context() as patch:
        patch.setattr(input_index, "project_original", forbid)
        assert fixture.plan()[0]["mode"] == "blocked"
    assert not fixture.calls


@pytest.mark.parametrize("damage", ["strip_key", "rebind_namespace"])
def test_scoped_descriptor_namespace_tamper_fails_explicit_audit(tmp_path, damage):
    fixture = Fixture(tmp_path, mixed=True)
    sparse_transition(fixture)
    fixture.prepare()
    row = full_descriptor(fixture)
    body, key = json.loads(row["identity"]), json.loads(row["request_key"])
    if damage == "strip_key":
        key.pop("anchor_evidence")
    else:
        key["anchor_evidence"] = body["anchor_evidence"] = "0" * 64
    replace_descriptor(fixture, row, body, key)
    with pytest.raises(RuntimeError, match="binding"):
        fixture.lake.inputs.verify(row["source_receipt_id"])


def test_unrelated_commit_keeps_namespace_and_known_metadata_proof(tmp_path, monkeypatch):
    from bagelquant_data import DatasetSpec
    fixture = Fixture(tmp_path, mixed=True)
    sparse_transition(fixture)
    fixture.prepare()
    row = full_descriptor(fixture)
    fixture.lake.raw.ingest(DatasetSpec("unrelated", "general"), pl.DataFrame({"asset_id": ["X"]}),
        ingested_at=received(4))
    monkeypatch.setattr(input_index, "project_original", forbid)
    monkeypatch.setattr(input_index, "read_selector", forbid)
    assert fixture.prepare()["key_appends"]["prepared"] == 0
    assert fixture.plan() == [{"mode": "incremental", "start": str(TAIL), "end": str(TAIL)}]
    assert full_descriptor(fixture)["identity"] == row["identity"]


def test_late_null_coordinate_invalidates_namespace_without_old_numeric_replay(tmp_path, monkeypatch):
    fixture = Fixture(tmp_path, mixed=True)
    sparse_transition(fixture)
    fixture.prepare()
    fixture.lake.raw.ingest(fixture.specs["secondary"], fixture.rows("secondary", OLD, "B", value=None),
        ingested_at=received(4))
    monkeypatch.setattr(input_index, "project_original", forbid)
    assert fixture.plan()[0]["mode"] == "blocked" and not fixture.calls
