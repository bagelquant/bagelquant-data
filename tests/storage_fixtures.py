"""Corrupt the sole registered original in isolated synthetic test lakes."""

from pathlib import Path

from bagelquant_data.storage.paths import LakePaths


def original_segments(db, where="", params=()):
    metadata = Path(db.execute("PRAGMA database_list").fetchone()[2])
    location = db.execute(
        "SELECT value FROM data_meta_state WHERE key='lake_location'"
    ).fetchone()[0]
    paths = LakePaths.open(data_meta_path=metadata, lake_path=metadata.parent / location)
    query = "SELECT s.generation_path,s.partition_path,c.source,c.dataset FROM version_segments s JOIN version_commits c ON c.seq=s.commit_seq"
    if where:
        query += " WHERE " + where
    return [paths.generation_path(source, dataset, partition, generation)
            for generation, partition, source, dataset in db.execute(query, params)]


def damage_original_segments(db, where="", params=(), payload=b"damaged original"):
    paths = original_segments(db, where, params)
    assert paths, "corruption fixture must select a registered original"
    for path in paths:
        path.write_bytes(payload)
