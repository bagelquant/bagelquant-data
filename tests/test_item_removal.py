"""Exact destructive maintenance preserves unrelated immutable evidence."""
from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl
import pytest

from bagelquant_data import ConfigurationError, DataItemSpec, DataLake, DatasetSpec, ItemInput, RawInput
from bagelquant_data.management.item_removal import _hash


FIRST, LAST, KNOWN = date(2020, 1, 1), date(2020, 2, 1), date(2020, 2, 6)
TARGETS = ("risk.model.2020-01.exposures", "risk.model.price_carry." + "a" * 32)


def _open(root):
    return DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")


def _frame(value=1.):
    return pl.DataFrame({"time": [FIRST, LAST], "asset_id": ["A", "A"], "value": [value, value]})


def _setup(root):
    lake = _open(root)
    raw = DatasetSpec("prices", "by_date", date_kind="calendar",
                      field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(raw, _frame(), ingested_at=datetime(2020, 2, 1, tzinfo=UTC))
    upstream = lake.inputs.freeze({"prices": RawInput("custom", "prices", view="versions")}, information_cutoff=KNOWN)
    lake.items.register(DataItemSpec("data.regular", inputs=(RawInput("custom", "prices", view="versions"),)))
    lake.items.replace_range("data.regular", _frame(), start=FIRST, end=LAST,
                             available_date=LAST, input_receipt=upstream)
    regular = lake.inputs.freeze({"regular": ItemInput("data.regular", view="versions")}, information_cutoff=KNOWN)
    lake.items.register(DataItemSpec("execution.stable", inputs=(ItemInput("data.regular", view="versions"),)))
    lake.items.replace_range("execution.stable", _frame(), start=FIRST, end=LAST,
                             available_date=LAST, input_receipt=regular)
    source = lake.inputs.freeze({"execution": ItemInput("execution.stable", view="versions")}, information_cutoff=KNOWN)
    for name in TARGETS:
        lake.items.register(DataItemSpec(name, inputs=(ItemInput("execution.stable", view="versions"),)))
        lake.items.replace_range(name, _frame(), start=FIRST, end=LAST,
                                 available_date=date(2020, 2, 5), input_receipt=source, historical_baseline=True)
        lake.items.replace_range(name, _frame(), start=FIRST, end=LAST,
                                 available_date=KNOWN, input_receipt=source)
    outputs = lake.inputs.freeze({str(index): ItemInput(name, view="versions") for index, name in enumerate(TARGETS)}, information_cutoff=KNOWN)
    return lake, upstream, regular, source, outputs


def _protected(lake, receipts):
    with lake._data_meta.connect() as db:
        tables = {row[0] for row in db.execute("select name from sqlite_master where type='table'")}
        rows = {}
        for table in sorted(tables):
            if table == "data_meta_state":
                rows[table] = [tuple(row) for row in db.execute("select * from data_meta_state where key not like 'item_removal:%' order by rowid")]
            elif table == "sqlite_sequence":
                rows[table] = [tuple(row) for row in db.execute("select * from sqlite_sequence order by name")]
            elif table == "version_segments":
                rows[table] = [tuple(row) for row in db.execute("select * from version_segments where commit_seq in(select seq from version_commits where source<>'items' or dataset not like 'risk.%') order by rowid")]
            elif table == "visibility_segments":
                rows[table] = [tuple(row) for row in db.execute("select * from visibility_segments where check_id in(select id from version_checks where source<>'items' or dataset not like 'risk.%') order by rowid")]
            elif table in {"item_completion_proofs", "item_completion_authorities", "item_pending_outputs"}:
                rows[table] = [tuple(row) for row in db.execute(f"select * from {table} where build_id in(select id from item_builds where name not like 'risk.%') order by rowid")]
            elif table == "item_dependencies":
                rows[table] = [tuple(row) for row in db.execute("select * from item_dependencies where item_name not like 'risk.%' order by rowid")]
            elif "source" in {row[1] for row in db.execute(f"pragma table_info({table})")} and "dataset" in {row[1] for row in db.execute(f"pragma table_info({table})")}:
                rows[table] = [tuple(row) for row in db.execute(f"select * from {table} where source<>'items' or dataset not like 'risk.%' order by rowid")]
            elif table == "datasets":
                rows[table] = [tuple(row) for row in db.execute("select * from datasets where source<>'items' or name not like 'risk.%' order by rowid")]
            elif table == "frozen_inputs":
                rows[table] = [tuple(row) for row in db.execute("select * from frozen_inputs where receipt_id in(select value from json_each(?)) order by receipt_id",
                                                             (json.dumps([receipt.receipt_id for receipt in receipts]),))]
            elif table in {"item_definitions", "item_builds", "item_initializations"}:
                rows[table] = [tuple(row) for row in db.execute(f"select * from {table} where name not like 'risk.%' order by rowid")]
            elif table == "node_completions":
                rows[table] = [tuple(row) for row in db.execute("select * from node_completions where node_key not like 'items/risk.%' order by rowid")]
            else:
                rows[table] = [tuple(row) for row in db.execute(f"select * from {table} order by rowid")]
    files = {}
    for path in lake.lake_path.rglob("*.parquet"):
        if "risk." not in str(path):
            facts = path.stat()
            files[str(path)] = (hashlib.sha256(path.read_bytes()).hexdigest(), facts.st_size, facts.st_mtime_ns, facts.st_ino)
    return rows, files, [(receipt.receipt_id, lake.inputs.get(receipt).digest) for receipt in receipts]


def test_exact_risk_publications_and_visibility_removed_shared_history_unchanged(tmp_path, monkeypatch):
    lake, upstream, regular, source, output = _setup(tmp_path)
    before = _protected(lake, (upstream, regular, source))
    plan = lake.integrity.item_removal_plan(TARGETS)
    assert plan["required_receipts"] == [output.receipt_id]
    assert {row["kind"] for row in plan["files"]} == {"content", "visibility"}
    assert plan["blockers"] == [{"kind": "retained_receipt", "receipt_id": output.receipt_id}]
    with pytest.raises(ConfigurationError, match="retained references"):
        lake.integrity.remove_items(plan)
    plan = lake.integrity.item_removal_plan(TARGETS, discard_receipts=[output.receipt_id])
    assert not plan["blockers"]
    monkeypatch.setattr(lake.inputs, "read", lambda *args, **kwargs: pytest.fail("No typed historical reads during removal"))
    result = lake.integrity.remove_items(plan)
    assert result["status"] == "complete" and result["bytes"] == plan["bytes"]
    assert all(not (lake.lake_path / row["path"]).exists() for row in plan["files"])
    assert _protected(lake, (upstream, regular, source)) == before
    for name in TARGETS:
        with pytest.raises(KeyError):
            lake.items.get(name, include_inactive=True)
    with pytest.raises(KeyError):
        lake.inputs.get(output)
    assert not lake._data_meta._rows("select * from node_completions where node_key like 'items/risk.%'")
    assert not lake._data_meta._rows("select * from version_commits where dataset like 'risk.%'")
    repeated = lake.integrity.remove_items(plan)
    assert repeated["bytes"] == 0 and repeated["already_missing"] == result["deleted"]


def test_mixed_receipts_require_whole_explicit_discard_and_shared_upstream_rejected(tmp_path):
    lake, upstream, _, source, output = _setup(tmp_path)
    mixed = lake.inputs.freeze({"risk": ItemInput(TARGETS[0]), "raw": RawInput("custom", "prices")}, information_cutoff=KNOWN)
    plan = lake.integrity.item_removal_plan(TARGETS, discard_receipts=[output.receipt_id])
    assert mixed.receipt_id in plan["required_receipts"]
    assert any(row.get("receipt_id") == mixed.receipt_id for row in plan["blockers"])
    wrong = lake.integrity.item_removal_plan(TARGETS, discard_receipts=[output.receipt_id, mixed.receipt_id, upstream.receipt_id])
    assert {"kind": "receipt_outside_item_scope", "receipt_id": upstream.receipt_id} in wrong["blockers"]
    with pytest.raises(ConfigurationError, match="retained references"):
        lake.integrity.remove_items(wrong)
    clean = lake.integrity.item_removal_plan(TARGETS, discard_receipts=[output.receipt_id, mixed.receipt_id])
    lake.integrity.remove_items(clean)
    assert lake.inputs.get(upstream).digest == upstream.digest and lake.inputs.get(source).digest == source.digest
    with pytest.raises(KeyError):
        lake.inputs.get(mixed)


def test_retained_declaration_creator_build_and_parent_receipt_block(tmp_path):
    lake, _, _, _, output = _setup(tmp_path)
    lake.items.register(DataItemSpec("protected.consumer", inputs=(ItemInput(TARGETS[0], view="versions"),)))
    consumed = lake.inputs.freeze({"risk": ItemInput(TARGETS[0], view="versions")}, information_cutoff=KNOWN)
    lake.items.replace_range("protected.consumer", _frame(), start=FIRST, end=LAST, available_date=KNOWN, input_receipt=consumed)
    child = lake.inputs.freeze({"consumer": ItemInput("protected.consumer", view="versions")}, information_cutoff=KNOWN)
    plan = lake.integrity.item_removal_plan(TARGETS, discard_receipts=[output.receipt_id, consumed.receipt_id])
    kinds = {row["kind"] for row in plan["blockers"]}
    assert {"retained_declaration", "retained_creator", "retained_build", "retained_parent_receipt"} <= kinds
    assert child.receipt_id in plan["required_receipts"]
    with pytest.raises(ConfigurationError, match="retained references"):
        lake.integrity.remove_items(plan)
    assert lake.inputs.read(child, "consumer").collect().height == 2


@pytest.mark.parametrize("changed", ["publication", "definition", "frozen_reference"])
def test_stale_plan_never_detaches_changed_authority(tmp_path, changed):
    lake, _, _, source, output = _setup(tmp_path)
    plan = lake.integrity.item_removal_plan(TARGETS, discard_receipts=[output.receipt_id])
    if changed == "publication":
        lake.items.ingest(TARGETS[0], _frame(3.), available_date=KNOWN, input_receipt=source)
    elif changed == "definition":
        spec = lake.items.get(TARGETS[0])
        from dataclasses import replace
        lake.items.register(replace(spec, metadata={"semantic": "changed"}))
    else:
        lake.inputs.freeze({"risk": ItemInput(TARGETS[0])}, information_cutoff=KNOWN)
    with pytest.raises(ConfigurationError, match="stale"):
        lake.integrity.remove_items(plan)
    assert lake.items.get(TARGETS[0]).name == TARGETS[0]
    assert all((lake.lake_path / row["path"]).exists() for row in plan["files"])


def test_foreign_readonly_active_and_expired_owner_rejected(tmp_path):
    lake, _, _, _, output = _setup(tmp_path / "first")
    plan = lake.integrity.item_removal_plan(TARGETS, discard_receipts=[output.receipt_id])
    other = _open(tmp_path / "foreign")
    with pytest.raises(ConfigurationError, match="another lake"):
        other.integrity.remove_items(plan)
    readonly = DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path, read_only=True)
    assert readonly.integrity.item_removal_plan(TARGETS, discard_receipts=[output.receipt_id]) == plan
    with pytest.raises(PermissionError):
        readonly.integrity.remove_items(plan)
    lake._data_meta.acquire_update_leases([("items", "execution.stable", "alive")])
    with lake._data_meta.connect() as db:
        db.execute("update update_leases set lease_expires_at='2000-01-01'")
    with pytest.raises(ConfigurationError, match="active Data"):
        lake.integrity.remove_items(plan)
    assert any(row["kind"] == "active_writer" for row in lake.integrity.item_removal_plan(TARGETS)["blockers"])
    lake._data_meta.release_update_leases(["alive"])
    lake.integrity.remove_items(plan)


@pytest.mark.parametrize("unsafe", ["same_stat_tamper", "symlink", "hardlink", "forged_foreign_file"])
def test_unsafe_file_cannot_detach_or_unlink_any_original(tmp_path, unsafe):
    lake, _, _, _, output = _setup(tmp_path)
    plan = lake.integrity.item_removal_plan(TARGETS, discard_receipts=[output.receipt_id])
    candidate = lake.lake_path / plan["files"][0]["path"]
    original = candidate.read_bytes()
    before = candidate.stat()
    if unsafe == "same_stat_tamper":
        candidate.write_bytes(bytes([original[0] ^ 1]) + original[1:])
        os.utime(candidate, ns=(before.st_atime_ns, before.st_mtime_ns))
    elif unsafe == "symlink":
        candidate.unlink()
        outside = tmp_path / "outside"
        outside.write_bytes(original)
        candidate.symlink_to(outside)
    elif unsafe == "hardlink":
        os.link(candidate, tmp_path / "shared-original")
    else:
        plan = copy.deepcopy(plan)
        protected = next((lake.lake_path / "raw").rglob("*.parquet"))
        plan["files"][0]["path"] = protected.relative_to(lake.lake_path).as_posix()
        plan["plan_hash"] = _hash({key: value for key, value in plan.items() if key != "plan_hash"})
    with pytest.raises(ConfigurationError, match="stale|symbolic|owned"):
        lake.integrity.remove_items(plan)
    assert lake.items.get(TARGETS[0]).name == TARGETS[0]
    assert lake.inputs.get(output).digest == output.digest
    if unsafe == "symlink":
        assert outside.read_bytes() == original


def test_interrupted_file_cleanup_resumes_without_touching_recreated_name(tmp_path, monkeypatch):
    lake, upstream, regular, source, output = _setup(tmp_path)
    before = _protected(lake, (upstream, regular, source))
    plan = lake.integrity.item_removal_plan(TARGETS, discard_receipts=[output.receipt_id])
    specs = [lake.items.get(name) for name in TARGETS]
    old_unlink, calls = Path.unlink, []
    def interrupt(path, *args, **kwargs):
        calls.append(path)
        if len(calls) == 2:
            raise OSError("simulated file cleanup interruption")
        return old_unlink(path, *args, **kwargs)
    monkeypatch.setattr(Path, "unlink", interrupt)
    with pytest.raises(OSError, match="interruption"):
        lake.integrity.remove_items(plan)
    assert _protected(lake, (upstream, regular, source)) == before
    assert not lake.integrity.active_update_leases()
    with pytest.raises(KeyError):
        lake.items.get(TARGETS[0])
    monkeypatch.setattr(Path, "unlink", old_unlink)
    lake.close()
    lake = _open(tmp_path)
    lake.items.register(specs[0])
    lake.items.replace_range(TARGETS[0], _frame(9.), start=FIRST, end=LAST, available_date=KNOWN, input_receipt=source)
    new_files = set((lake.lake_path / "items" / TARGETS[0]).rglob("*.parquet")) - {
        lake.lake_path / row["path"] for row in plan["files"]}
    new_bytes = {path: path.read_bytes() for path in new_files}
    result = lake.integrity.remove_items(plan)
    assert result["status"] == "complete" and result["already_missing"]
    assert all(path.read_bytes() == body for path, body in new_bytes.items())
    assert lake.items.read(TARGETS[0], as_of=KNOWN, view="snapshot").collect()["value"].to_list() == [9., 9.]


def test_forced_process_exit_requires_exact_owner_recovery_then_resumes(tmp_path):
    lake, upstream, regular, source, output = _setup(tmp_path)
    before = _protected(lake, (upstream, regular, source))
    plan = lake.integrity.item_removal_plan(TARGETS, discard_receipts=[output.receipt_id])
    plan_file = tmp_path / "remove.json"
    plan_file.write_text(json.dumps(plan))
    script = """
import json, os, sys
from pathlib import Path
from bagelquant_data import DataLake
root = Path(sys.argv[1])
lake = DataLake.open(data_meta_path=root/'meta.sqlite', lake_path=root/'lake')
plan = json.loads((root/'remove.json').read_text())
unlink = Path.unlink
def terminate(path, *args, **kwargs):
    unlink(path, *args, **kwargs)
    os._exit(23)
Path.unlink = terminate
lake.integrity.remove_items(plan)
"""
    worker = subprocess.run([sys.executable, "-c", script, str(tmp_path)], timeout=30, check=False)
    assert worker.returncode == 23
    assert _protected(lake, (upstream, regular, source)) == before
    assert len(lake.integrity.active_update_leases()) == len(TARGETS)
    with pytest.raises(ConfigurationError, match="active Data"):
        lake.integrity.remove_items(plan)
    recovered = lake.integrity.abandon_update_owner("item-removal:" + plan["plan_hash"], reason="Isolated child exit verified")
    assert recovered["leases"] == len(TARGETS)
    result = lake.integrity.remove_items(plan)
    assert result["status"] == "complete" and len(result["already_missing"]) == 1
    assert _protected(lake, (upstream, regular, source)) == before


def test_unknown_target_file_blocks_and_preview_never_hashes_originals(tmp_path, monkeypatch):
    lake, _, _, _, output = _setup(tmp_path)
    unknown = lake.lake_path / "items" / TARGETS[0] / "unknown.parquet"
    unknown.write_bytes(b"unregistered unrelated evidence")
    monkeypatch.setattr(hashlib, "file_digest", lambda *args, **kwargs: pytest.fail("Preview must use registered headers and stat only"))
    plan = lake.integrity.item_removal_plan(TARGETS, discard_receipts=[output.receipt_id])
    assert {"kind": "unregistered_item_file", "name": TARGETS[0], "path": unknown.relative_to(lake.lake_path).as_posix()} in plan["blockers"]
    with pytest.raises(ConfigurationError, match="retained references"):
        lake.integrity.remove_items(plan)
    assert unknown.read_bytes() == b"unregistered unrelated evidence"


def test_outside_native_output_receipt_reference_blocks_removal(tmp_path):
    lake, _, _, _, output = _setup(tmp_path)
    mixed = lake.inputs.freeze({"execution": ItemInput("execution.stable", view="versions"), "risk": ItemInput(TARGETS[0])}, information_cutoff=KNOWN)
    build = lake.items.completed_builds("execution.stable")[-1]
    # Retained native output metadata can bind a mixed frozen receipt even
    # when the creator input itself contains no Risk dependency.
    payload = json.dumps({"output_receipt_id": mixed.receipt_id, "output_receipt_digest": mixed.digest})
    with lake._data_meta.connect() as db:
        db.execute("create table item_completion_output_receipts(build_id integer primary key,payload_json text not null,digest text not null)")
        db.execute("insert into item_completion_output_receipts values(?,?,?)",
                   (build["id"], payload, hashlib.sha256(payload.encode()).hexdigest()))
    plan = lake.integrity.item_removal_plan(TARGETS, discard_receipts=[output.receipt_id, mixed.receipt_id])
    assert any(row["kind"] == "retained_completion_receipt" and row["name"] == "execution.stable" for row in plan["blockers"])
    with pytest.raises(ConfigurationError, match="retained references"):
        lake.integrity.remove_items(plan)


@pytest.mark.parametrize("names", ["risk.*", [], ["../raw"], [TARGETS[0], TARGETS[0]], ["risk.*"]])
def test_exact_scope_required(tmp_path, names):
    lake, *_ = _setup(tmp_path)
    with pytest.raises((ValueError, KeyError)):
        lake.integrity.item_removal_plan(names)
