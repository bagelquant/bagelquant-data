"""Explicit, reference-checked removal of exact neutral Item publications.

Metadata detachment is atomic. A durable receipt retains the exact old file
inventory so interrupted unlinking can resume without adopting current files.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

from bagelquant_data.core.exceptions import ConfigurationError
from bagelquant_data.items.types import DataItemSpec
from bagelquant_data.storage.paths import validate_generation

_SCHEMA = "bagelquant.data.item-removal.v1"
_JOURNAL_PREFIX = "item_removal:"


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      default=lambda cell: {"bytes": bytes(cell).hex()} if isinstance(cell, (bytes, memoryview)) else str(cell))


def _hash(value: Any) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _exact(values: Sequence[str], *, receipts: bool = False) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)) or not values or any(not isinstance(value, str) for value in values):
        raise ValueError("Item removal requires an explicit nonempty sequence of exact names/receipt IDs")
    if len(set(values)) != len(values):
        raise ValueError("Item removal names/receipt IDs must be unique")
    for value in values:
        if receipts:
            if not re.fullmatch(r"[0-9a-f]{32}", value):
                raise ValueError("Item removal requires exact registered receipt IDs")
        else:
            DataItemSpec(value)
    return tuple(sorted(values))


def _tables(db: sqlite3.Connection) -> set[str]:
    return {str(row[0]) for row in db.execute("select name from sqlite_master where type='table'")}


def _scopes(names: tuple[str, ...], receipts: tuple[str, ...]) -> dict[str, tuple[str, tuple[str, ...]]]:
    named, discarded = _json(names), _json(receipts)
    item = "source='items' and dataset in(select value from json_each(?))"
    commits = "select seq from version_commits where " + item
    checks = "select id from version_checks where " + item
    builds = "select id from item_builds where name in(select value from json_each(?))"
    scopes = "select id from update_scopes where " + item
    digests = "select digest from frozen_inputs where receipt_id in(select value from json_each(?))"
    result = {table: (item, (named,)) for table in (
        "version_checks", "version_commits", "partition_manifest", "partition_generations", "dataset_schemas",
        "dataset_initializations", "ingestion_runs", "api_calls", "update_scopes", "rejected_summary")}
    result.update({table: ("name in(select value from json_each(?))", (named,))
                   for table in ("item_definitions", "item_builds", "item_initializations")})
    result["datasets"] = ("source='items' and name in(select value from json_each(?))", (named,))
    result["item_dependencies"] = ("item_name in(select value from json_each(?))", (named,))
    result["catalog_assignments"] = ("kind='item' and object_key in(select value from json_each(?))", (named,))
    result["node_completions"] = ("node_key in(select value from json_each(?))", (_json(["items/" + name for name in names]),))
    result["version_segments"] = ("commit_seq in(" + commits + ")", (named,))
    result["visibility_segments"] = ("check_id in(" + checks + ")", (named,))
    result["provider_scope_checks"] = ("scope_id in(" + scopes + ")", (named,))
    for table in ("item_completion_proofs", "item_completion_authorities", "item_pending_outputs",
                  "item_completion_attestations", "item_completion_output_receipts", "item_output_impact_index"):
        result[table] = ("build_id in(" + builds + ")", (named,))
    result["input_selection_index"] = ("receipt_digest in(" + digests + ")", (discarded,))
    result["input_coordinate_index"] = ("receipt_digest in(" + digests + ")", (discarded,))
    result["input_projection_index"] = ("source_receipt_id in(select value from json_each(?))", (discarded,))
    result["frozen_inputs"] = ("receipt_id in(select value from json_each(?))", (discarded,))
    return result


def _metadata(db: sqlite3.Connection, scopes) -> list[dict[str, Any]]:
    tables = _tables(db)
    result = []
    for table, (where, parameters) in sorted(scopes.items()):
        digest, count = hashlib.sha256(), 0
        if table in tables:
            for row in db.execute(f"select * from {table} where {where} order by rowid", parameters):
                digest.update((_json(dict(row)) + "\n").encode())
                count += 1
        result.append({"table": table, "present": table in tables, "rows": count, "digest": digest.hexdigest()})
    return result


def _references(db: sqlite3.Connection, names: tuple[str, ...], discarded: tuple[str, ...]) -> tuple[list[dict], list[str]]:
    """Find retained authority edges; shared upstream sources are never candidates."""
    tables, targets, discard = _tables(db), set(names), set(discarded)
    blockers: list[dict] = []
    receipts: dict[str, tuple[set[str], bool]] = {}
    commits_owned = {int(row[0]) for row in db.execute("select seq from version_commits where source='items' "
                                                     "and dataset in(select value from json_each(?))", (_json(names),))}
    if "frozen_inputs" in tables:
        for row in db.execute("select receipt_id,payload_json,digest from frozen_inputs order by receipt_id"):
            key, serialized = str(row[0]), str(row[1])
            if hashlib.sha256(serialized.encode()).hexdigest() != row[2]:
                blockers.append({"kind": "damaged_receipt", "receipt_id": key})
                continue
            try:
                body = json.loads(serialized)
                evidence = body["evidence"]
                direct = any(value.get("kind") == "ItemInput" and value.get("name") in targets
                             for value in body["requests"].values())
                direct |= any(value.get("source") == "items" and value.get("dataset") in targets
                              for value in evidence.values())
                direct |= any(event.get("commit_seq") in commits_owned or event.get("visible_commit") in commits_owned
                              for value in evidence.values() for collection in ("batches", "checks", "audit_checks", "empty_checks")
                              for event in value.get(collection, ()))
                parents = {str(parent) for value in evidence.values() for parent in value.get("parent_receipts", {})}
                for value in evidence.values():
                    for collection in ("batches", "checks", "audit_checks", "empty_checks"):
                        parents.update(str(event["input_receipt_id"]) for event in value.get(collection, ())
                                       if event.get("input_receipt_id") is not None)
                    build = value.get("empty_item_build")
                    if build and build.get("frozen_receipt_id"):
                        parents.add(str(build["frozen_receipt_id"]))
                receipts[key] = (parents, direct)
            except (TypeError, KeyError, ValueError) as error:
                raise ConfigurationError(f"Cannot prove Item removal references for receipt {key}") from error
    required = {key for key, (_, direct) in receipts.items() if direct}
    while True:
        expanded = required | {key for key, (parents, _) in receipts.items() if parents.intersection(required)}
        if expanded == required:
            break
        required = expanded
    for key in sorted(discard - required):
        blockers.append({"kind": "receipt_outside_item_scope", "receipt_id": key})
    for key in sorted(required - discard):
        blockers.append({"kind": "retained_receipt", "receipt_id": key})
    for key, (parents, _) in receipts.items():
        if key not in discard and parents.intersection(discard):
            blockers.append({"kind": "retained_parent_receipt", "receipt_id": key,
                             "parents": sorted(parents.intersection(discard))})
    named, ids = _json(names), _json(discarded)
    if "item_dependencies" in tables:
        for row in db.execute("select item_name,dataset from item_dependencies where kind='item' "
                              "and dataset in(select value from json_each(?)) "
                              "and item_name not in(select value from json_each(?))", (named, named)):
            blockers.append({"kind": "retained_declaration", "name": row[0], "dependency": row[1]})
    commits = "select seq from version_commits where source='items' and dataset in(select value from json_each(?))"
    for table, column in (("version_commits", "input_receipt_id"), ("version_checks", "input_receipt_id")):
        for row in db.execute(f"select source,dataset,{column} from {table} "
                              f"where {column} in(select value from json_each(?)) "
                              "and not(source='items' and dataset in(select value from json_each(?)))", (ids, named)):
            blockers.append({"kind": "retained_creator", "table": table, "source": row[0], "name": row[1], "receipt_id": row[2]})
    for row in db.execute("select source,dataset,visible_commit from version_checks where visible_commit in(" + commits + ") "
                          "and not(source='items' and dataset in(select value from json_each(?)))", (named, named)):
        blockers.append({"kind": "retained_commit_reference", "source": row[0], "name": row[1], "commit": row[2]})
    if "item_builds" in tables:
        for row in db.execute("select name,frozen_receipt_id,result_commit from item_builds where name not in(select value from json_each(?)) "
                              "and(frozen_receipt_id in(select value from json_each(?)) or result_commit in(" + commits + "))", (named, ids, named)):
            blockers.append({"kind": "retained_build", "name": row[0], "receipt_id": row[1], "commit": row[2]})
        for table in ("item_completion_authorities", "item_completion_attestations", "item_completion_output_receipts"):
            if table not in tables:
                continue
            for row in db.execute(f"select b.name,a.build_id,a.payload_json from {table} a "
                                  "join item_builds b on b.id=a.build_id where b.name not in(select value from json_each(?))", (named,)):
                def referenced(value):
                    if isinstance(value, str):
                        return {value} if value in discard else set()
                    if isinstance(value, dict):
                        return set().union(*(referenced(cell) for cell in value.values()))
                    if isinstance(value, list):
                        return set().union(*(referenced(cell) for cell in value))
                    return set()
                try:
                    linked = referenced(json.loads(str(row[2])))
                except (ValueError, TypeError) as error:
                    raise ConfigurationError("Cannot prove retained Item completion references") from error
                if linked:
                    blockers.append({"kind": "retained_completion_receipt", "name": row[0], "build_id": row[1], "receipts": sorted(linked)})
        for row in db.execute("select node_key,result_ref from node_completions where node_key like 'items/%' "
                              "and node_key not in(select 'items/'||value from json_each(?)) and result_ref in("
                              "select cast(id as text) from item_builds where name in(select value from json_each(?)))", (named, named)):
            blockers.append({"kind": "retained_completion", "node_key": row[0], "build_id": row[1]})
    for row in db.execute("select c.source,c.dataset,e.check_id from visibility_segments e join version_checks c on c.id=e.check_id "
                          "where not(c.source='items' and c.dataset in(select value from json_each(?))) "
                          "and exists(select 1 from json_each(e.commits_json) j where j.value in(" + commits + "))", (named, named)):
        blockers.append({"kind": "retained_visibility_commit", "source": row[0], "name": row[1], "check_id": row[2]})
    return blockers, sorted(required)


def _path(lake, relative: str) -> Path:
    parts = PurePosixPath(relative)
    if parts.is_absolute() or parts.as_posix() != relative or ".." in parts.parts or "\\" in relative:
        raise ConfigurationError("Item removal file escapes its exact owner")
    path = lake.lake_path
    if path.is_symlink():
        raise ConfigurationError("Item removal lake contains a symbolic link")
    for part in parts.parts:
        path /= part
        if path.is_symlink():
            raise ConfigurationError("Item removal file contains a symbolic link")
    return path


def _facts(path: Path) -> dict[str, int]:
    facts = path.lstat()
    if not stat.S_ISREG(facts.st_mode) or facts.st_nlink != 1:
        raise ConfigurationError("Item removal requires an exclusively owned regular file")
    return {"bytes": facts.st_size, "device": facts.st_dev, "inode": facts.st_ino,
            "mtime_ns": facts.st_mtime_ns, "nlink": facts.st_nlink}


def _files(lake, db: sqlite3.Connection, names: tuple[str, ...]) -> list[dict[str, Any]]:
    named = _json(names)
    result: dict[str, dict[str, Any]] = {}
    for row in db.execute("select c.dataset,b.partition_path,b.generation_path,b.content_hash,b.file_size_bytes "
                          "from version_segments b join version_commits c on c.seq=b.commit_seq "
                          "where c.source='items' and c.dataset in(select value from json_each(?))", (named,)):
        validate_generation(str(row[1]), str(row[2]))
        relative = f"items/{row[0]}/{row[2]}"
        value = {"path": relative, "name": row[0], "sha256": row[3], "kind": "content"}
        value.update(_facts(_path(lake, relative)))
        if value["bytes"] != row[4]:
            raise ConfigurationError("Item removal original size differs from registered evidence")
        if relative in result and result[relative] != value:
            raise ConfigurationError("Item removal original file has conflicting ownership")
        result[relative] = value
    for row in db.execute("select c.dataset,e.generation_path,e.content_hash from visibility_segments e "
                          "join version_checks c on c.id=e.check_id where c.source='items' "
                          "and c.dataset in(select value from json_each(?))", (named,)):
        if not re.fullmatch(r"\.visibility/[0-9a-f]{32}\.parquet", str(row[1])):
            raise ConfigurationError("Item removal visibility path is invalid")
        relative = f"items/{row[0]}/{row[1]}"
        value = {"path": relative, "name": row[0], "sha256": row[2], "kind": "visibility"}
        value.update(_facts(_path(lake, relative)))
        if relative in result:
            raise ConfigurationError("Item removal visibility file has conflicting ownership")
        result[relative] = value
    # Unregistered files are deliberately not adopted or deleted.
    return [result[key] for key in sorted(result)]


def _active(db: sqlite3.Connection) -> list[dict]:
    blockers = [{"kind": "active_writer", "source": row[0], "name": row[1], "owner": row[2]}
                for row in db.execute("select source,dataset,owner_id from update_leases order by source,dataset")]
    for row in db.execute("select source,dataset,run_id from ingestion_runs where status='running'"):
        blockers.append({"kind": "active_run", "source": row[0], "name": row[1], "run_id": row[2]})
    for row in db.execute("select source,dataset,id from update_scopes where status='running'"):
        blockers.append({"kind": "active_scope", "source": row[0], "name": row[1], "scope_id": row[2]})
    return blockers


def _unregistered(lake, names: tuple[str, ...], files: list[dict[str, Any]]) -> list[dict]:
    known = {row["path"] for row in files}
    result = []
    for name in names:
        root = _path(lake, "items/" + name)
        if not root.exists():
            continue
        for directory, dirs, entries in os.walk(root, followlinks=False):
            base = Path(directory)
            for entry in [*entries, *(entry for entry in dirs if (base / entry).is_symlink())]:
                relative = (base / entry).relative_to(lake.lake_path).as_posix()
                if relative not in known:
                    result.append({"kind": "unregistered_item_file", "name": name, "path": relative})
    return sorted(result, key=_json)


def _plan(lake, db: sqlite3.Connection, names: tuple[str, ...], discarded: tuple[str, ...]) -> dict[str, Any]:
    named = _json(names)
    found = {str(row[0]) for row in db.execute("select name from item_definitions where name in(select value from json_each(?))", (named,))}
    if found != set(names):
        raise KeyError(f"Unknown removal DataItems: {sorted(set(names) - found)}")
    blockers, required = _references(db, names, discarded)
    files = _files(lake, db, names)
    state = dict(db.execute("select key,value from data_meta_state where key in('lake_id','lake_location','schema_version')"))
    result = {"schema": _SCHEMA, "lake": state, "binding": _binding(lake), "names": list(names), "discard_receipts": list(discarded),
              "required_receipts": required, "blockers": sorted(blockers + _active(db) + _unregistered(lake, names, files), key=_json),
              "metadata": _metadata(db, _scopes(names, discarded)), "files": files,
              "bytes": sum(row["bytes"] for row in files)}
    return {**result, "plan_hash": _hash(result)}


def _binding(lake) -> dict[str, int]:
    metadata, root = lake.data_meta_path.stat(), lake.lake_path.stat()
    return {"metadata_device": metadata.st_dev, "metadata_inode": metadata.st_ino,
            "lake_device": root.st_dev, "lake_inode": root.st_ino}


def item_removal_plan(lake, names: Sequence[str], *, discard_receipts: Sequence[str] = ()) -> dict[str, Any]:
    """Preview exact publications and reference blockers without changing storage."""
    targets = _exact(names)
    discarded = _exact(discard_receipts, receipts=True) if discard_receipts else ()
    with lake._data_meta.read_view() as db:
        return _plan(lake, db, targets, discarded)


def _verify_file(lake, row: Mapping[str, Any], *, missing: bool) -> Path | None:
    relative = str(row["path"])
    # Check the declared owner independently of the caller's JSON checksum.
    if not relative.startswith(f"items/{row['name']}/"):
        raise ConfigurationError("Item removal file is outside its exact owner")
    path = _path(lake, relative)
    if not path.exists():
        if missing:
            return None
        raise ConfigurationError("Item removal original is missing; plan is stale")
    expected = {key: row[key] for key in ("bytes", "device", "inode", "mtime_ns", "nlink")}
    if _facts(path) != expected:
        raise ConfigurationError("Item removal original changed; plan is stale")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        if hashlib.file_digest(stream, "sha256").hexdigest() != row["sha256"]:
            raise ConfigurationError("Item removal original checksum differs; plan is stale")
    if _facts(path) != expected:
        raise ConfigurationError("Item removal original changed during verification")
    return path


def _detach(db: sqlite3.Connection, names: tuple[str, ...], receipts: tuple[str, ...]) -> None:
    scopes = _scopes(names, receipts)
    tables = _tables(db)
    # Delete children before the subqueries identifying their parents disappear.
    last = ("frozen_inputs", "update_scopes", "item_builds", "version_checks", "version_commits", "item_definitions", "datasets")
    order = [table for table in scopes if table not in last] + list(last)
    for table in order:
        if table in tables:
            where, parameters = scopes[table]
            db.execute(f"delete from {table} where {where}", parameters)


def remove_items(lake, plan: Mapping[str, Any]) -> dict[str, Any]:
    """Detach only an unreferenced frozen inventory; resume exact file cleanup."""
    store = lake._data_meta
    store.ensure_writable()
    supplied = json.loads(_json(dict(plan)))
    digest = supplied.pop("plan_hash", None)
    if supplied.get("schema") != _SCHEMA or digest != _hash(supplied):
        raise ConfigurationError("Item removal plan checksum/schema is invalid")
    names = _exact(supplied["names"])
    receipts = _exact(supplied["discard_receipts"], receipts=True) if supplied["discard_receipts"] else ()
    leases: list[str] = []
    detached = False
    try:
        with store.atomic_transaction() as db:
            state = dict(db.execute("select key,value from data_meta_state where key in('lake_id','lake_location','schema_version')"))
            if supplied["lake"] != state or supplied.get("binding") != _binding(lake):
                raise ConfigurationError("Item removal plan belongs to another lake or schema")
            if _active(db):
                raise ConfigurationError("Item removal requires no active Data writers/runs/scopes, including expired owners")
            journal_key = _JOURNAL_PREFIX + str(digest)
            saved_row = db.execute("select value from data_meta_state where key=?", (journal_key,)).fetchone()
            saved = None if saved_row is None else json.loads(str(saved_row[0]))
            if saved is None:
                current = _plan(lake, db, names, receipts)
                if current["plan_hash"] != digest:
                    raise ConfigurationError("Item removal plan is stale; preview the current inventory")
                if current["blockers"]:
                    raise ConfigurationError(f"Item removal has retained references: {current['blockers']}")
                for row in supplied["files"]:
                    _verify_file(lake, row, missing=False)
                _detach(db, names, receipts)
                db.execute("insert into data_meta_state values(?,?,?)", (journal_key,
                           _json({"plan": supplied, "status": "detached"}), datetime.now(UTC).isoformat()))
            elif saved.get("plan") != supplied or saved.get("status") not in {"detached", "complete"}:
                raise ConfigurationError("Saved Item removal receipt differs from its immutable plan")
            # New declarations after an interrupted detach may coexist, but
            # their files cannot be substituted into this old inventory.
            for row in supplied["files"]:
                relative = str(PurePosixPath(row["path"]).relative_to(PurePosixPath("items") / row["name"]))
                if db.execute("select 1 from version_segments b join version_commits c on c.seq=b.commit_seq "
                              "where c.source='items' and c.dataset=? and b.generation_path=? union all "
                              "select 1 from visibility_segments e join version_checks c on c.id=e.check_id "
                              "where c.source='items' and c.dataset=? and e.generation_path=? limit 1",
                              (row["name"], relative, row["name"], relative)).fetchone():
                    raise ConfigurationError("Detached Item removal file acquired a new retained reference")
                if saved is not None and saved["status"] == "complete" and _path(lake, row["path"]).exists():
                    raise ConfigurationError("Completed Item removal file unexpectedly reappeared")
            now = datetime.now(UTC)
            for name in names:
                run_id = uuid4().hex
                leases.append(run_id)
                db.execute("insert into update_leases values('items',?,?,?,?,?)", (name, run_id,
                           store._update_owner_id() or "item-removal:" + str(digest), now.isoformat(),
                           (now + timedelta(seconds=300)).isoformat()))
            detached = True
        # Commit first. Failure or process exit can leave only exact unregistered
        # old files; the journal permits a repeat without deleting new values.
        deleted, missing = [], []
        for row in supplied["files"]:
            path = _verify_file(lake, row, missing=True)
            if path is None:
                missing.append(row["path"])
            else:
                path.unlink()
                deleted.append(row["path"])
        with store.connect() as db:
            db.execute("update data_meta_state set value=?,updated_at=? where key=?",
                       (_json({"plan": supplied, "status": "complete"}), datetime.now(UTC).isoformat(), journal_key))
        return {"plan_hash": digest, "status": "complete", "names": list(names), "discarded_receipts": list(receipts),
                "deleted": deleted, "already_missing": missing,
                "bytes": sum(row["bytes"] for row in supplied["files"] if row["path"] in deleted)}
    finally:
        if detached and leases:
            store.release_update_leases(leases)
