"""An unchanged check stores a tiny header without a row-proof table."""
import sqlite3
import pytest
from test_capture_record_context import prepared


def test_empty_and_repeated_checks_never_create_row_proof_table(tmp_path):
    lake, spec, frame, _ = prepared(tmp_path)
    from datetime import UTC, datetime
    lake.raw.ingest(spec, frame, ingested_at=datetime(2024, 1, 3, tzinfo=UTC))
    with sqlite3.connect(lake.data_meta_path) as db:
        rows = db.execute("select row_count from version_checks order by id").fetchall()
        assert rows and rows[-1][0] == 2
        assert db.execute("select 1 from sqlite_master where name='version_check_records'").fetchone() is None


def test_check_header_transaction_rolls_back_on_failure(tmp_path):
    lake, _, _, _ = prepared(tmp_path)
    with lake._data_meta.connect() as db:
        before = db.execute("select count(*) from version_checks").fetchone()[0]
    with pytest.raises(RuntimeError, match="interrupted publication"):
        with lake._data_meta.connect() as db:
            db.execute("insert into version_checks(source,dataset,run_id,checked_at,pit_date,baseline,visible_commit,request_json,row_count,input_receipt_id) values('custom','raw','failed','now','2024-01-03',0,1,'[]',0,NULL)")
            raise RuntimeError("interrupted publication")
    with lake._data_meta.connect() as db:
        assert db.execute("select count(*) from version_checks").fetchone()[0] == before
