from datetime import UTC, date, datetime
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DatasetSpec
from bagelquant_data.pipeline.versions import _check_evidence
from bagelquant_data.storage.data_meta import DataMetaStore, _insert_version_check


def _evidence(records):
    return _check_evidence(
        DatasetSpec("values", "by_date", date_kind="calendar"), "run",
        datetime(2020, 1, 3, tzinfo=UTC), date(2020, 1, 3), False,
        [{"query": "unchanged"}], records.height, records,
        input_receipt_id="frozen-input",
    )


def test_streamed_witness_matches_previous_sql_rows_and_order(tmp_path, monkeypatch):
    store = DataMetaStore(tmp_path / "meta.sqlite")
    records = pl.DataFrame({
        "_record_id": ["b", "a"], "_payload_hash": ["hash-b", "hash-a"],
        "_commit_seq": [7, 8], "time": [date(2020, 1, 1), date(2020, 1, 2)],
        "unused_payload": ["large-unused-payload", None],
    })
    previous = records.to_dicts()
    with monkeypatch.context() as patch:
        patch.setattr(pl.DataFrame, "to_dicts", lambda *a, **k: pytest.fail("full witness dictionary copy"))
        evidence = _evidence(records)
    assert iter(evidence["records"]) is evidence["records"]
    with store.connect() as db:
        _insert_version_check(db, **evidence)
        actual = [tuple(row) for row in db.execute("select * from version_check_records order by rowid")]
        expected = [(1, row["_record_id"], row["_payload_hash"], int(row["_commit_seq"]), str(row["time"]))
                    for row in previous]
        assert actual == expected
        header = dict(db.execute("select * from version_checks").fetchone())
        assert header == {
            "id": 1, "source": "custom", "dataset": "values", "run_id": "run",
            "checked_at": "2020-01-03T00:00:00+00:00", "pit_date": "2020-01-03",
            "baseline": 0, "visible_commit": None,
            "request_json": '[{"query": "unchanged"}]', "row_count": 2,
            "input_receipt_id": "frozen-input",
        }


def test_empty_witness_does_not_require_record_columns(tmp_path):
    store = DataMetaStore(tmp_path / "meta.sqlite")
    with store.connect() as db:
        _insert_version_check(db, **_evidence(pl.DataFrame()))
        assert db.execute("select row_count from version_checks").fetchone()[0] == 0
        assert db.execute("select count(*) from version_check_records").fetchone()[0] == 0


@pytest.mark.parametrize("failure", ["iteration", "duplicate"])
def test_stream_failure_rolls_back_header_and_every_witness(tmp_path, failure):
    store = DataMetaStore(tmp_path / "meta.sqlite")
    evidence = _evidence(pl.DataFrame())
    def interrupted():
        assert db.execute("select count(*) from version_checks").fetchone()[0] == 1
        yield ("a", "hash-a", 7, date(2020, 1, 1))
        if failure == "iteration":
            raise RuntimeError("interrupted witness stream")
        yield ("a", "hash-a", 7, date(2020, 1, 1))
    evidence["records"] = interrupted()
    # Inspect the same uncommitted header through the publication connection;
    # a pre-materialized batch would run the iterator before allocating it.
    with pytest.raises(RuntimeError if failure == "iteration" else sqlite3.IntegrityError):
        with store.connect() as db:
            _insert_version_check(db, **evidence)
    with store.connect() as db:
        assert db.execute("select count(*) from version_checks").fetchone()[0] == 0
        assert db.execute("select count(*) from version_check_records").fetchone()[0] == 0
