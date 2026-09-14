"""Explicit, resumable historical baseline boundaries."""

from __future__ import annotations

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
