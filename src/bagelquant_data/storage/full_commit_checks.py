"""Bounded exact proofs for large unchanged full-baseline attestations.

Original headers, witnesses and batches remain authoritative and immutable.
The content-addressed seal is a checked representation of their exact equality.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
from collections.abc import Mapping, Sequence
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from bagelquant_data.storage.data_meta import DataMetaStore

COMPACT_MIN_ROWS = 100_000


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      default=lambda value: dict(value) if isinstance(value, Mapping) else str(value))


def validate_seal(seal: Mapping[str, Any]) -> None:
    body = {key: value for key, value in seal.items() if key != "digest"}
    if hashlib.sha256(_json(body).encode()).hexdigest() != seal["digest"]:
        raise RuntimeError("Full-commit check seal checksum mismatch")
    if sum(batch["row_count"] for batch in seal["binding"]["batches"]) != seal["row_count"]:
        raise RuntimeError("Full-commit check seal row count mismatch")


def full_commit_checks(store: DataMetaStore, db: sqlite3.Connection,
                       checks: Sequence[Mapping[str, Any]], *, persist: bool = False,
                       max_buffer_bytes: int | None = None) -> list[dict[str, Any]]:
    """Prove exact tuple equality using bounded external sorting, never counts alone.

    Unsupported partial/mixed attestations retain the ordinary record path.
    Read-only calls can construct a proof but never publish a seal.
    """
    from bagelquant_data.execution import ExecutionOptions
    budget = max_buffer_bytes or ExecutionOptions().max_buffer_bytes
    seals = []
    for check in checks:
        if check["baseline"] or check["row_count"] < COMPACT_MIN_ROWS or check["visible_commit"] is None:
            continue
        definition = db.execute("select spec_json from datasets where source=? and name=?", (check["source"], check["dataset"])).fetchone()
        if definition is None or json.loads(definition[0])["update_type"] != "by_date":
            continue
        commit = db.execute("select * from version_commits where seq=? and status='committed' and baseline=1 and source=? and dataset=?",
                            (check["visible_commit"], check["source"], check["dataset"])).fetchone()
        if commit is None:
            continue
        batches = [dict(row) for row in db.execute(
            "select commit_seq,partition_path,content_hash,row_count from version_batches where commit_seq=? order by partition_path",
            (commit["seq"],))]
        if sum(batch["row_count"] for batch in batches) != check["row_count"]:
            continue
        binding = {"check": dict(check), "commit": dict(commit), "batches": batches}
        key = "full_commit_check:" + hashlib.sha256(_json(binding).encode()).hexdigest()
        cached = db.execute("select value from data_meta_state where key=?", (key,)).fetchone()
        if cached is not None:
            blob = db.execute("select value from data_meta_state where key=?", ("full_commit_check_blob:" + cached[0],)).fetchone()
            if blob is None:
                raise RuntimeError("Full-commit check seal blob is missing")
            seal = json.loads(blob[0])
            validate_seal(seal)
            if seal["digest"] != cached[0] or seal["binding"] != binding:
                raise RuntimeError("Full-commit check seal binding mismatch")
            seals.append(seal)
            continue
        # A single transient external sort keeps Python/native memory bounded.
        with tempfile.TemporaryDirectory(prefix="bagelquant-check-") as temporary:
            with closing(sqlite3.connect(Path(temporary) / "expected.sqlite")) as expected:
                expected.execute("pragma cache_size=-2048")
                expected.execute("pragma temp_store=FILE")
                expected.execute("create table expected(record_id text,payload_hash text,version_commit integer)")
                bounds = []
                for batch in batches:
                    from bagelquant_data import input_index
                    frame = input_index.read(db, batch, max_bytes=budget // 4)
                    if frame is None:
                        # Missing historical indexes do not trigger hidden
                        # original-batch scans during freeze/currentness.
                        supported = False
                        break
                    if frame.height != batch["row_count"] or not frame["_baseline"].all() or not (frame["_commit_seq"] == commit["seq"]).all():
                        raise RuntimeError("Full-commit check original batch is not a complete baseline")
                    expected.executemany("insert into expected values(?,?,?)",
                                         frame.select("_record_id", "_payload_hash", "_commit_seq").iter_rows())
                    axis = "source_time" if "source_time" in frame.columns else "time"
                    bounds.append({**batch, "observation_min": str(frame[axis].min()), "observation_max": str(frame[axis].max())})
                else:
                    supported = True
                if not supported:
                    continue
                expected.commit()
                original = iter(expected.execute("select * from expected order by record_id,payload_hash,version_commit"))
                actual = db.execute("select record_id,payload_hash,version_commit,available_date from version_check_records not indexed where check_id=? order by record_id,payload_hash,version_commit", (check["id"],))
                digest = hashlib.sha256()
                count = 0
                previous = None
                available = None
                supported = True
                for row in actual:
                    witness = tuple(row)
                    target = next(original, None)
                    if target is None or witness[:3] != tuple(target) or witness[0] == previous:
                        supported = False
                        break
                    if available is None:
                        available = witness[3]
                    if witness[3] != available:
                        supported = False
                        break
                    previous = witness[0]
                    digest.update((_json(witness[:3]) + "\n").encode())
                    count += 1
                if not supported or next(original, None) is not None or count != check["row_count"]:
                    continue
                body = {"binding": binding, "row_count": count, "tuple_digest": digest.hexdigest(),
                        "available_date": available, "batch_bounds": bounds}
                seal = {**body, "digest": hashlib.sha256(_json(body).encode()).hexdigest()}
        if persist:
            store.ensure_writable()
            serialized = _json(seal)
            for state_key, state_value in [("full_commit_check_blob:" + seal["digest"], serialized), (key, seal["digest"])]:
                db.execute("insert or ignore into data_meta_state(key,value,updated_at) values(?,?,?)",
                           (state_key, state_value, datetime.now(UTC).isoformat()))
                if db.execute("select value from data_meta_state where key=?", (state_key,)).fetchone()[0] != state_value:
                    raise RuntimeError("Full-commit check seal cannot overwrite existing evidence")
        seals.append(seal)
    return seals
