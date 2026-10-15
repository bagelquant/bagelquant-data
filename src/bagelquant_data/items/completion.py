"""Original complete-range publication authority and pending visibility dates."""
from __future__ import annotations

import hashlib
import json


def _json(value):
    from bagelquant_data.storage.immutable_metadata import canonical
    return canonical(value)


def output_check_bound(db, build_id, authority=None) -> int | None:
    """Resolve an original check ceiling, including explicit immutable indexes."""
    if authority is None:
        row = db.execute("select payload_json from item_completion_authorities where build_id=?", (build_id,)).fetchone()
        authority = json.loads(row[0])
    bound = authority.get("output_check_bound")
    if bound is not None:
        if type(bound) is not int or bound < 0:
            raise RuntimeError("DataItem original output-check boundary is damaged")
        return bound
    if not db.execute("select 1 from sqlite_master where name='item_completion_output_receipts'").fetchone():
        return None
    row = db.execute("select payload_json,digest from item_completion_output_receipts where build_id=?", (build_id,)).fetchone()
    if row is None:
        return None
    try:
        body = json.loads(row[0])
        original = db.execute("select digest from item_completion_proofs where build_id=?", (build_id,)).fetchone()
        native = db.execute("select digest from item_completion_authorities where build_id=?", (build_id,)).fetchone()
        output = db.execute("select payload_json,digest from frozen_inputs where receipt_id=?", (body["output_receipt_id"],)).fetchone()
        if (hashlib.sha256(row[0].encode()).hexdigest() != row[1] or body["build_id"] != build_id
                or authority["kind"] != "native" or original is None or body["completion_digest"] != original[0]
                or native is None or body["native_authority_digest"] != native[0]
                or body["pending_digest"] != authority["pending_digest"]
                or output is None or output[1] != body["output_receipt_digest"]
                or hashlib.sha256(output[0].encode()).hexdigest() != output[1]
                or json.loads(output[0])["max_check_id"] != body["output_check_bound"]
                or type(body["output_check_bound"]) is not int or body["output_check_bound"] < 0):
            raise ValueError("original output binding differs")
        return body["output_check_bound"]
    except (KeyError, ValueError, TypeError) as error:
        raise RuntimeError("DataItem original output-check index is damaged") from error


def verified_completion_digest(db, build_id) -> str:
    original = db.execute("select * from item_builds where id=?", (build_id,)).fetchone()
    proof = db.execute("select digest from item_completion_proofs where build_id=?", (build_id,)).fetchone()
    if original is None or proof is None or hashlib.sha256(_json(dict(original)).encode()).hexdigest() != proof[0]:
        raise RuntimeError("DataItem completion proof source binding differs")
    if not db.execute("select 1 from sqlite_master where name='item_completion_authorities'").fetchone():
        raise RuntimeError("DataItem completion authority is missing; explicit completion maintenance is required")
    row = db.execute("select * from item_completion_authorities where build_id=?", (build_id,)).fetchone()
    if row is None or hashlib.sha256(row["payload_json"].encode()).hexdigest() != row["digest"]:
        raise RuntimeError("DataItem completion authority is missing or damaged; explicit completion maintenance is required")
    try:
        authority = json.loads(row["payload_json"])
        if authority["completion_digest"] != proof[0]:
            raise ValueError("completion binding differs")
        if not db.execute("select 1 from sqlite_master where name='item_pending_outputs'").fetchone():
            raise RuntimeError("DataItem pending visibility proof is missing; explicit completion maintenance is required")
        pending = db.execute("select * from item_pending_outputs where build_id=?", (build_id,)).fetchone()
        if pending is None:
            raise RuntimeError("DataItem pending visibility proof is missing; explicit completion maintenance is required")
        if (pending["digest"] != authority["pending_digest"]
                or hashlib.sha256(pending["payload_json"].encode()).hexdigest() != pending["digest"]):
            raise RuntimeError("DataItem pending visibility proof source binding differs")
        if authority["kind"] == "attested":
            if not db.execute("select 1 from sqlite_master where name='item_completion_attestations'").fetchone():
                raise ValueError("original attestation is missing")
            attestation = db.execute("select * from item_completion_attestations where build_id=?", (build_id,)).fetchone()
            if (attestation is None or attestation["digest"] != authority["attestation_digest"]
                    or hashlib.sha256(attestation["payload_json"].encode()).hexdigest() != attestation["digest"]):
                raise ValueError("original attestation is missing or damaged")
            if "output_receipt" in authority:
                declared = json.loads(attestation["payload_json"])
                output = authority["output_receipt"]
                registered = db.execute("select payload_json,digest from frozen_inputs where receipt_id=?", (output["id"],)).fetchone()
                if (output["id"] != declared["output_receipt_id"] or output["digest"] != declared["output_receipt_digest"]
                        or registered is None or registered["digest"] != output["digest"]
                        or hashlib.sha256(registered["payload_json"].encode()).hexdigest() != output["digest"]
                        or authority["output_check_bound"] != output["max_check_id"]
                        or json.loads(registered["payload_json"])["max_check_id"] != output["max_check_id"]):
                    raise ValueError("original output receipt/check binding differs")
        elif authority["kind"] != "native":
            raise ValueError("unknown completion authority")
        else:
            output_check_bound(db, build_id, authority)
        if "timing" in authority:
            from .timing import verify_authority
            verify_authority(db, original, authority["timing"])
        if "reuse" in authority:
            reuse = authority["reuse"]
            parent = db.execute("select * from item_builds where id=?", (reuse["completed_build"],)).fetchone()
            if (parent is None or parent["id"] >= build_id or parent["name"] != original["name"]
                    or parent["definition_hash"] != original["definition_hash"]
                    or hashlib.sha256(_json(dict(parent)).encode()).hexdigest() != reuse["completion_digest"]
                    or parent["frozen_receipt_id"] != reuse["original_source"]["id"]
                    or original["frozen_receipt_id"] != reuse["current_source"]["id"]
                    or reuse["impact"] != "none" or not isinstance(reuse["policy"], dict)):
                raise ValueError("completed reuse source binding differs")
            for source in (reuse["original_source"], reuse["current_source"]):
                registered = db.execute("select digest from frozen_inputs where receipt_id=?", (source["id"],)).fetchone()
                if registered is None or registered[0] != source["digest"]:
                    raise ValueError("completed reuse input binding differs")
        if "continuation" in authority:
            continuation = authority["continuation"]
            parent = db.execute("select * from item_builds where id=?", (continuation["build_id"],)).fetchone()
            if (parent is None or parent["id"] >= build_id or parent["name"] != original["name"]
                    or parent["definition_hash"] != original["definition_hash"]
                    or hashlib.sha256(_json(dict(parent)).encode()).hexdigest() != continuation["digest"]):
                raise ValueError("sparse continuation original completion binding differs")
    except (ValueError, KeyError, TypeError) as error:
        raise RuntimeError("DataItem completion authority is damaged; explicit completion maintenance is required") from error
    return proof[0]


def pending_dates(db, build_id) -> list[tuple[str, str]]:
    if not db.execute("select 1 from sqlite_master where name='item_pending_outputs'").fetchone():
        raise RuntimeError("DataItem pending visibility proof is missing; explicit completion maintenance is required")
    row = db.execute("select * from item_pending_outputs where build_id=?", (build_id,)).fetchone()
    if row is None:
        raise RuntimeError("DataItem pending visibility proof is missing; explicit completion maintenance is required")
    proof = verified_completion_digest(db, build_id)
    if hashlib.sha256(row["payload_json"].encode()).hexdigest() != row["digest"]:
        raise RuntimeError("DataItem pending visibility proof source binding differs")
    try:
        payload = json.loads(row["payload_json"])
        if payload["completion_digest"] != proof or not isinstance(payload["coordinates"], list):
            raise ValueError("invalid pending visibility record")
        if any(not isinstance(value, list) or len(value) != 2 for value in payload["coordinates"]):
            raise ValueError("invalid pending coordinates")
    except (ValueError, KeyError, TypeError) as error:
        raise RuntimeError("DataItem pending visibility proof is damaged; explicit completion maintenance is required") from error
    return [tuple(value) for value in payload["coordinates"]]


def save_completion(db, build_id, *, pending=(), authority=None, checked_target=None,
                    publish_node=True) -> None:
    """Publish original range authority atomically with its pending coordinates."""
    db.execute("create table if not exists item_completion_proofs(build_id integer primary key, digest text not null)")
    original = db.execute("select * from item_builds where id=?", (build_id,)).fetchone()
    digest = hashlib.sha256(_json(dict(original)).encode()).hexdigest()
    db.execute("insert into item_completion_proofs values(?,?)", (build_id, digest))
    db.execute("create table if not exists item_completion_authorities(build_id integer primary key, "
               "payload_json text not null,digest text not null)")
    pending_payload = _json({"completion_digest": digest, "coordinates": sorted(set(pending))})
    check_bound = (authority["output_check_bound"] if authority is not None and "output_check_bound" in authority
        else db.execute("select coalesce(max(id),0) from version_checks where source='items' and dataset=?",
            (original["name"],)).fetchone()[0])
    payload = _json({**({"kind": "native"} if authority is None else authority), "completion_digest": digest,
        "output_check_bound": check_bound,
        "pending_digest": hashlib.sha256(pending_payload.encode()).hexdigest()})
    db.execute("insert into item_completion_authorities values(?,?,?)", (build_id, payload,
        hashlib.sha256(payload.encode()).hexdigest()))
    # Persist an explicit empty list too: losing a pending record cannot silently
    # turn a completed calculation into a fully visible publication.
    db.execute("create table if not exists item_pending_outputs(build_id integer primary key, "
               "payload_json text not null,digest text not null)")
    payload = pending_payload
    db.execute("insert into item_pending_outputs values(?,?,?)", (build_id, payload,
        hashlib.sha256(payload.encode()).hexdigest()))
    if not publish_node:
        return
    from bagelquant_data.storage.data_meta import DataMetaStore
    # The flag publishes in the same transaction as complete-range authority.
    revision = db.execute("select publication_revision from datasets where source='items' and name=?", (original["name"],)).fetchone()[0]
    DataMetaStore.write_completion(db,node_key="items/"+original["name"],definition_hash=original["definition_hash"],
        completed_start=original["start_date"],completed_end=original["end_date"],
        checked_target=original["end_date"] if checked_target is None else checked_target,
        semantic_version=hashlib.sha256(_json([original["name"],revision]).encode()).hexdigest(),
        dependency_digest=original["dependency_digest"],result_ref=str(build_id))

