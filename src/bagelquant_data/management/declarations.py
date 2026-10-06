"""Portable declaration unions with one metadata transaction and retry receipt."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from bagelquant_data.core.exceptions import ConfigurationError
from bagelquant_data.items.types import ItemInput, spec_from_payload, spec_payload
from bagelquant_data.management.datasets import DatasetManager, _spec_from_mapping

SCHEMA = "bagelquant.data.declarations.v1"
PLAN_SCHEMA = "bagelquant.data.declaration-plan.v1"
RECEIPT_SCHEMA = "bagelquant.data.declaration-receipt.v1"
SECTIONS = ("sources", "raw", "items", "categories", "assignments")


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _hash(value: Any) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _portable(value: Any) -> Any:
    """Reject credential-bearing declarations rather than export hidden secrets."""
    if isinstance(value, Mapping):
        for key in value:
            lower = str(key).lower()
            if any(part in lower for part in ("password", "secret", "token", "credential", "api_key")):
                raise ConfigurationError("Declarations cannot contain credential fields")
        return {str(key): _portable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_portable(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    # Transformation scalars include typed dates encoded by spec_payload.
    raise ConfigurationError("Declarations must contain JSON-compatible values")


def _key(section: str, row: dict) -> tuple:
    if section == "sources":
        return (row["name"],)
    if section == "raw":
        return (row["spec"]["source"], row["spec"]["name"])
    if section == "items":
        return (row["spec"]["name"],)
    if section == "categories":
        return (row["id"],)
    return (row["kind"], row["source"], row["object_key"])


def export_declarations(store) -> dict[str, Any]:
    """Read one consistent non-secret snapshot, including archived declarations."""
    with store.connect() as db:
        own_transaction = not db.in_transaction
        if own_transaction:
            db.execute("begin")
        sources = [dict(row) for row in db.execute(
            "select name,adapter,enabled,active from sources where name<>'items' order by name")]
        raw = [{"spec": json.loads(row["spec_json"]), "enabled": bool(row["enabled"]),
                "active": bool(row["active"])} for row in db.execute(
                    "select * from datasets where source<>'items' order by source,name")]
        items = [{"spec": json.loads(row["spec_json"]), "active": bool(row["active"])}
                 for row in db.execute("select * from item_definitions order by name")]
        categories = [dict(row) for row in db.execute(
            "select id,kind,source,name,parent_id from category_nodes order by kind,source,id")]
        assignments = [dict(row) for row in db.execute(
            "select kind,source,object_key,category_id from catalog_assignments order by kind,source,object_key")]
        for row in sources:
            row["enabled"], row["active"] = bool(row["enabled"]), bool(row["active"])
        result = _portable({"schema": SCHEMA, "sources": sources, "raw": raw,
                            "items": items, "categories": categories, "assignments": assignments})
        result["revision"] = _hash(result)
        return result


def _normalize(payload: Mapping[str, Any]) -> dict[str, Any]:
    if payload.get("schema") != SCHEMA or set(payload) - {*SECTIONS, "schema", "revision"}:
        raise ConfigurationError("Unsupported Data declaration schema or fields")
    result: dict[str, Any] = {"schema": SCHEMA}
    for section in SECTIONS:
        supplied = payload.get(section, [])
        if not isinstance(supplied, list) or not all(isinstance(row, dict) for row in supplied):
            raise ConfigurationError(f"Declaration {section} must be a list of records")
        rows = []
        for original in supplied:
            row = dict(original)
            if section == "sources":
                if set(row) - {"name", "adapter", "enabled", "active"}:
                    raise ConfigurationError("Source descriptors cannot include configuration or credentials")
                name = row.get("name")
                if not isinstance(name, str) or name in {"", ".", "..", "items"} or any(c in name for c in "/\\:"):
                    raise ConfigurationError("Invalid source descriptor identity")
                if not isinstance(row.get("adapter"), str) or not row["adapter"].strip():
                    raise ConfigurationError("Source descriptor requires an adapter name")
                row = {"name": name, "adapter": row["adapter"],
                       "enabled": row.get("enabled", True), "active": row.get("active", True)}
            elif section == "raw":
                if set(row) - {"spec", "enabled", "active"}:
                    raise ConfigurationError("Unsupported Raw declaration fields")
                spec = _spec_from_mapping(dict(row["spec"]), stored=True)
                DatasetManager.validate_spec(spec)
                if spec.source == "items":
                    raise ConfigurationError("Raw cannot use the items namespace")
                row = {"spec": asdict(spec), "enabled": row.get("enabled", True), "active": row.get("active", True)}
                if row["spec"]["source_api"] is None:
                    row["spec"].pop("source_api")
                if row["spec"]["request_discovery"] is None:
                    row["spec"].pop("request_discovery")
            elif section == "items":
                if set(row) - {"spec", "active"}:
                    raise ConfigurationError("Unsupported DataItem declaration fields")
                row = {"spec": spec_payload(spec_from_payload(row["spec"])), "active": row.get("active", True)}
            elif section == "categories":
                if set(row) != {"id", "kind", "source", "name", "parent_id"}:
                    raise ConfigurationError("Category declarations require id/kind/source/name/parent_id")
                if not isinstance(row["id"], str) or not row["id"] or row["kind"] not in {"raw", "item"}:
                    raise ConfigurationError("Invalid category identity or kind")
                if not isinstance(row["name"], str) or not row["name"].strip() or row["name"] != row["name"].strip():
                    raise ConfigurationError("Category name must be a nonempty trimmed string")
                if not isinstance(row["source"], str) or (row["kind"] == "item" and row["source"] != ""):
                    raise ConfigurationError("Items categories have no provider namespace")
                if row["parent_id"] is not None and not isinstance(row["parent_id"], str):
                    raise ConfigurationError("Category parent must be an identity or null")
            elif set(row) != {"kind", "source", "object_key", "category_id"}:
                raise ConfigurationError("Invalid category assignment fields")
            for field in ("active", "enabled"):
                if field in row and not isinstance(row[field], bool):
                    raise ConfigurationError(f"Declaration {field} must be boolean")
            rows.append(_portable(row))
        identities = [_key(section, row) for row in rows]
        if len(set(identities)) != len(identities):
            raise ConfigurationError(f"Duplicate {section} declaration identity")
        result[section] = sorted(rows, key=lambda row: _key(section, row))
    return result


def _validate_union(union: dict[str, Any]) -> tuple[list[str], list[str]]:
    issues: list[str] = []
    raw = {_key("raw", row): row for row in union["raw"]}
    sources = {row["name"] for row in union["sources"]} | {key[0] for key in raw}
    items = {row["spec"]["name"]: row for row in union["items"]}
    categories = {row["id"]: row for row in union["categories"]}
    siblings: set[tuple] = set()
    for row in categories.values():
        namespace = (row["kind"], row["source"])
        if row["kind"] == "raw" and row["source"] not in sources:
            issues.append(f"Unknown category source: {row['source']}")
        sibling = (*namespace, row["parent_id"], row["name"])
        if sibling in siblings:
            issues.append(f"Duplicate sibling category: {row['name']}")
        siblings.add(sibling)
        seen = {row["id"]}
        parent = row["parent_id"]
        while parent is not None:
            ancestor = categories.get(parent)
            if ancestor is None or (ancestor["kind"], ancestor["source"]) != namespace:
                issues.append(f"Unknown or foreign category parent: {parent}")
                break
            if parent in seen:
                issues.append("Category dependency cycle")
                break
            seen.add(parent)
            parent = ancestor["parent_id"]
    for key, row in raw.items():
        for dependency in (row["spec"].get("calendar"), row["spec"].get("parameter_dataset")):
            if dependency is not None and (key[0], dependency) not in raw:
                issues.append(f"Unknown Raw declaration dependency: {key[0]}/{dependency}")
    order: list[str] = []
    visiting: set[str] = set()
    visited: set[str] = set()
    def visit(name: str) -> None:
        if name in visiting:
            issues.append("DataItem dependency cycle")
            return
        if name in visited:
            return
        visiting.add(name)
        row = items[name]
        spec = spec_from_payload(row["spec"])
        for dependency in spec.inputs:
            target = items.get(dependency.name) if isinstance(dependency, ItemInput) else raw.get((dependency.source, dependency.dataset))
            if target is None or (row["active"] and not target["active"]):
                issues.append(f"Unknown or inactive DataItem dependency: {dependency.key}")
            elif isinstance(dependency, ItemInput):
                visit(dependency.name)
        visiting.remove(name)
        visited.add(name)
        order.append(name)
    for name in sorted(items):
        visit(name)
    for row in union["assignments"]:
        category = categories.get(row["category_id"])
        if category is None or (category["kind"], category["source"]) != (row["kind"], row["source"]):
            issues.append(f"Unknown or foreign assigned category: {row['category_id']}")
        target = raw.get((row["source"], row["object_key"])) if row["kind"] == "raw" else items.get(row["object_key"])
        if target is None or not target["active"] or row["kind"] not in {"raw", "item"}:
            issues.append(f"Unknown or inactive category member: {row['object_key']}")
    return sorted(set(issues)), order


def plan_batch(store, payload: Mapping[str, Any]) -> dict[str, Any]:
    incoming = _normalize(payload)
    current = export_declarations(store)
    union: dict[str, Any] = {"schema": SCHEMA}
    conflicts, additions = [], {}
    for section in SECTIONS:
        existing = {_key(section, row): row for row in current[section]}
        added = []
        for row in incoming[section]:
            key = _key(section, row)
            if key in existing:
                if existing[key] != row:
                    conflicts.append({"section": section, "identity": list(key), "reason": "existing_declaration_conflict"})
            else:
                existing[key] = row
                added.append(row)
        union[section] = list(existing.values())
        additions[section] = added
    issues, order = _validate_union(union)
    result = {"schema": PLAN_SCHEMA, "expected_revision": current["revision"],
              "lake_id": store._rows("select value from data_meta_state where key='lake_id'")[0]["value"],
              "payload": incoming, "additions": additions, "item_order": order,
              "valid": not conflicts and not issues, "conflicts": conflicts, "issues": issues,
              "summary": {section: {"added": len(additions[section]), "skipped": len(incoming[section]) - len(additions[section])} for section in SECTIONS}}
    result["plan_hash"] = _hash(result)
    return result


def batch_receipt(store, request_id: str) -> dict[str, Any] | None:
    rows = store._rows("select receipt_json from declaration_batch_receipts where request_id=?", (request_id,))
    return json.loads(rows[0]["receipt_json"]) if rows else None


def apply_batch(lake, plan: Mapping[str, Any], *, request_id: str, expected_revision: str) -> dict[str, Any]:
    if not isinstance(request_id, str) or not request_id.strip() or len(request_id) > 200:
        raise ConfigurationError("Declaration request_id must be a nonempty bounded string")
    store = lake._data_meta
    store.ensure_writable()
    supplied = dict(plan)
    digest = supplied.pop("plan_hash", None)
    if supplied.get("schema") != PLAN_SCHEMA or digest != _hash(supplied):
        raise ConfigurationError("Declaration plan hash is invalid")
    with store.atomic_transaction() as db:
        previous = batch_receipt(store, request_id)
        if previous is not None:
            if previous["plan_hash"] != digest:
                raise ConfigurationError("Declaration request_id was used for another plan")
            return previous
        fresh = plan_batch(store, supplied["payload"])
        if fresh != dict(plan) or expected_revision != fresh["expected_revision"]:
            raise ConfigurationError("Declaration plan is stale; plan the current catalog")
        if not fresh["valid"]:
            raise ConfigurationError("Declaration union has conflicts or invalid dependencies")
        additions = fresh["additions"]
        for row in additions["sources"]:
            store.upsert_source(row["name"], row["adapter"], enabled=row["enabled"])
            if not row["active"]:
                store.remove_source(row["name"])
        for row in additions["raw"]:
            spec = _spec_from_mapping(row["spec"], stored=True)
            lake.raw._register(spec)
            store.set_dataset_enabled(spec.source, spec.name, row["enabled"])
        pending = {row["id"]: row for row in additions["categories"]}
        while pending:
            ready = [row for row in pending.values() if row["parent_id"] not in pending]
            for row in ready:
                tree = lake.catalog.raw_categories(row["source"]) if row["kind"] == "raw" else lake.catalog.item_categories
                tree._create(db, row["id"], row["name"], row["parent_id"])
                del pending[row["id"]]
        items = {row["spec"]["name"]: row for row in additions["items"]}
        for name in fresh["item_order"]:
            if name in items:
                lake.items._register(spec_from_payload(items[name]["spec"]))
        for row in additions["raw"]:
            if not row["active"]:
                store.remove_dataset(row["spec"]["source"], row["spec"]["name"])
        for name, row in items.items():
            if not row["active"]:
                db.execute("update item_definitions set active=0 where name=?", (name,))
                store.remove_dataset("items", name)
        for row in additions["assignments"]:
            tree = lake.catalog.raw_categories(row["source"]) if row["kind"] == "raw" else lake.catalog.item_categories
            tree.assign(row["object_key"], row["category_id"])
        receipt = {"schema": RECEIPT_SCHEMA, "request_id": request_id, "plan_hash": digest,
                   "lake_id": store._rows("select value from data_meta_state where key='lake_id'")[0]["value"],
                   "before_revision": expected_revision, "after_revision": export_declarations(store)["revision"],
                   "declarations": fresh["payload"], "summary": fresh["summary"],
                   "created_at": datetime.now(UTC).isoformat()}
        receipt["receipt_hash"] = _hash(receipt)
        db.execute("insert into declaration_batch_receipts values(?,?,?,?)",
                   (request_id, digest, _json(receipt), receipt["created_at"]))
        return receipt


def verify_receipt(store, receipt: Mapping[str, Any]) -> dict[str, Any]:
    record = dict(receipt)
    digest = record.pop("receipt_hash", None)
    valid = digest == _hash(record) and record.get("schema") == RECEIPT_SCHEMA
    stored = batch_receipt(store, str(record.get("request_id", "")))
    valid = valid and stored == dict(receipt)
    issues = [] if valid else ["Declaration receipt is not retained by this Data authority"]
    current = valid
    if valid:
        snapshot = export_declarations(store)
        for section in SECTIONS:
            actual = {_key(section, row): row for row in snapshot[section]}
            for row in record["declarations"][section]:
                if actual.get(_key(section, row)) != row:
                    current = False
                    issues.append(f"Current {section} declaration changed: {_key(section, row)}")
    return {"valid": valid, "current": current, "issues": issues}
