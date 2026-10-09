"""Explicit, resumable historical baseline boundaries."""

from __future__ import annotations

from datetime import UTC, datetime

from bagelquant_data.core.exceptions import ConfigurationError


def prepare_initialization(metadata, spec, mode, start, end) -> None:
    if mode != "initialize":
        return
    if start is None or end is None:
        raise ConfigurationError("initialize requires an explicit frozen start and end")
    spec_hash = metadata.dataset_spec_hash(spec.source, spec.name)
    with metadata.connect() as db:
        db.execute("begin immediate")
        row = db.execute(
            "select * from dataset_initializations where source=? and dataset=?",
            (spec.source, spec.name),
        ).fetchone()
        if row is not None:
            if row["status"] != "running" or (
                row["initial_start"],
                row["initial_end"],
                row["spec_hash"],
            ) != (str(start), str(end), spec_hash):
                raise ConfigurationError(
                    "Initialization can only resume its original unfinished range and definition"
                )
            return
        if db.execute(
            "select 1 from version_commits where source=? and dataset=? and status='committed'",
            (spec.source, spec.name),
        ).fetchone():
            raise ConfigurationError("Historical initialization requires a new dataset")
        db.execute(
            "insert into dataset_initializations values(?,?,?,?,?,'running')",
            (spec.source, spec.name, spec_hash, str(start), str(end)),
        )


def finish_initialization(metadata, source, dataset) -> None:
    with metadata.connect() as db:
        db.execute(
            "update dataset_initializations set status='complete' where source=? and dataset=? and status='running'",
            (source, dataset),
        )


def reopen_initialization(metadata, spec, *, start, end, reason: str) -> dict:
    """Append an unverified baseline after a premature completion, never backdate incremental data."""
    metadata.ensure_writable()
    if spec.source == "items" or spec.update_type != "by_date" or not reason.strip():
        raise ConfigurationError("Reopening requires a by_date Raw and a repair reason")
    with metadata.connect() as db:
        db.execute("begin immediate")
        metadata.assert_definition(spec)
        spec_hash = metadata.dataset_spec_hash(spec.source, spec.name)
        key = (spec.source, spec.name)
        row = db.execute("select * from dataset_initializations where source=? and dataset=?", key).fetchone()
        if row is None or row["status"] != "complete" or (
            row["initial_start"], row["initial_end"], row["spec_hash"]
        ) != (str(start), str(end), spec_hash):
            raise ConfigurationError("Reopening requires the original completed bounds and definition")
        if db.execute("select 1 from update_leases where source=? and dataset=?", key).fetchone() or db.execute(
            "select 1 from ingestion_runs where source=? and dataset=? and status='running'", key
        ).fetchone():
            raise ConfigurationError("Initialization still has a writer or unfinished run")
        if db.execute("select 1 from update_scopes where source=? and dataset=? and (status not in ('success','empty') or active_run_id is not null)", key).fetchone():
            raise ConfigurationError("Initialization still has unfinished daily scopes")
        commits = db.execute("select seq,mode,spec_hash,status from version_commits where source=? and dataset=?", key).fetchall()
        if any(value["status"] == "prepared" for value in commits):
            raise ConfigurationError("Prepared versions require ordinary storage recovery first")
        committed = [value for value in commits if value["status"] == "committed"]
        if any(value["mode"] != "initialize" or value["spec_hash"] != spec_hash for value in committed):
            raise ConfigurationError("Cannot reopen after incremental/refresh history or a definition change")
        checks = db.execute("select id,baseline from version_checks where source=? and dataset=?", key).fetchall()
        if any(not value["baseline"] for value in checks):
            raise ConfigurationError("Cannot reopen after verified incremental checks")
        scopes = db.execute("select * from update_scopes where source=? and dataset=? and spec_hash=? and scope_kind='date' and scope_key between ? and ?",
                            (*key, spec_hash, str(start), str(end))).fetchall()
        if not scopes or any(value["status"] not in {"success", "empty"} or value["active_run_id"] is not None for value in scopes):
            raise ConfigurationError("Reopening requires terminal historical daily scopes")
        now = datetime.now(UTC).isoformat()
        db.execute("update dataset_initializations set status='running' where source=? and dataset=?", key)
        db.execute("update update_scopes set status='pending',checked_through=null,last_error=null,recheck_after=null,updated_at=? where source=? and dataset=? and spec_hash=? and scope_kind='date' and scope_key between ? and ?",
                   (now, *key, spec_hash, str(start), str(end)))
        return {"source": spec.source, "dataset": spec.name, "start": str(start), "end": str(end),
                "definition_hash": spec_hash, "reason": reason.strip(), "scope_count": len(scopes),
                "previous_status": "complete", "status": "running", "at": now,
                "retained_commit_ceiling": max((value["seq"] for value in committed), default=None),
                "retained_check_ceiling": max((value["id"] for value in checks), default=None)}
