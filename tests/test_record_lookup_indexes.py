"""Optional record-lookup maintenance; all roots are tmp_path or in-memory."""

from datetime import date
import hashlib
import json
import sqlite3
from typing import cast

import polars as pl
import pytest

from bagelquant_data import (
    DataItemSpec,
    DataLake,
    ExecutionOptions,
    ItemInput,
    ItemPublication,
)
from bagelquant_data.inputs import input_read_boundary
from bagelquant_data.storage.record_lookups import _INDEXES, install_record_lookups

NAMES = {name for name, _, _ in _INDEXES}


def names(db):
    return {
        row[0]
        for row in db.execute("select name from sqlite_master where type='index'")
    } & NAMES


def empty_lookup_tables(db):
    return {name for name, table, _ in _INDEXES
            if db.execute(f"select 1 from {table} limit 1").fetchone() is None}


def lake_at(path):
    lake = DataLake.open(data_meta_path=path / "meta.sqlite", lake_path=path / "lake")
    return lake


def remove_indexes(lake):
    with sqlite3.connect(lake.data_meta_path) as db:
        for name in NAMES:
            db.execute(f"drop index if exists {name}")


def mini_db():
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute(
        "create table frozen_inputs(receipt_id text primary key,payload_json text not null,digest text not null,created_at text not null)"
    )
    db.execute(
        "create table item_builds(id integer primary key,name text,definition_hash text,status text,dependency_digest text,start_date text,end_date text,input_commit integer,result_commit integer,frozen_receipt_id text)"
    )
    db.execute(
        "create table item_completion_proofs(build_id integer primary key,digest text)"
    )
    db.execute("create table version_checks(id integer primary key,source text,dataset text)")
    db.commit()
    return db


def put(db, key, body):
    db.execute(
        "insert into frozen_inputs values(?,?,?,?)",
        (key, body, hashlib.sha256(body.encode()).hexdigest(), "original"),
    )


def snapshot(db):
    return {
        table: [tuple(row) for row in db.execute(f"select * from {table} order by 1")]
        for table in ("item_builds", "frozen_inputs", "item_completion_proofs", "version_checks")
    }


def install(db, **kwargs):
    return install_record_lookups(
        db, config=ExecutionOptions(), check_canceled=lambda: None, **kwargs
    )


def test_new_empty_initialization_is_cheap_and_idempotent(tmp_path):
    lake = lake_at(tmp_path)
    with sqlite3.connect(lake.data_meta_path) as db:
        assert names(db) == NAMES
        assert db.execute("select count(*) from frozen_inputs").fetchone()[0] == 0
    report = lake.inputs.index_record_lookups()
    assert set(report["indexes"].values()) == {"existing"}
    assert lake.inputs.index_record_lookups() == report


def populated(lake):
    leaf = pl.DataFrame({"time": [date(2024, 1, 1)], "asset_id": ["A"], "value": [1.0]})
    lake.items.register(DataItemSpec("leaf"))
    lake.items.ingest("leaf", leaf, available_date="2024-01-02")
    source = lake.inputs.freeze(
        {"leaf": ItemInput("leaf")}, information_cutoff="2024-01-03"
    )
    lake.items.register(DataItemSpec("out", (ItemInput("leaf"),)))
    with lake.items.publication(input_receipt=source) as publisher:
        publisher.publish(
            [
                ItemPublication(
                    "out",
                    leaf.with_columns(
                        pl.lit(date(2024, 1, 2)).alias("version_available_date")
                    ),
                    start="2024-01-01",
                    end="2024-01-03",
                    complete_at="2024-01-03",
                )
            ]
        )
    return source


@pytest.mark.parametrize("view", ["snapshot", "versions"])
@pytest.mark.parametrize("bounds", [True, False])
def test_legacy_reads_preserve_capture_and_currentness_without_hidden_maintenance(
    tmp_path, monkeypatch, view, bounds
):
    lake = lake_at(tmp_path)
    source = populated(lake)
    remove_indexes(lake)
    reopened = lake_at(tmp_path)
    with sqlite3.connect(lake.data_meta_path) as db:
        automatic = names(db)
        assert automatic == empty_lookup_tables(db)
    request = ItemInput(
        "out",
        view=view,
        start="2024-01-01" if bounds else None,
        end="2024-01-03" if bounds else None,
    )
    old = reopened.inputs.freeze(
        {"x": request}, information_cutoff="2024-01-03", retain_batch_bounds=True
    )
    before = reopened.inputs.is_current(old)
    assert before

    def forbidden(*args, **kwargs):
        pytest.fail("Lookup index maintenance read numerical data")

    from bagelquant_data import inputs

    monkeypatch.setattr(inputs, "read_batch", forbidden)
    monkeypatch.setattr(inputs, "verify_batch", forbidden)
    with sqlite3.connect(lake.data_meta_path) as db:
        original_sources = db.execute(
            "select * from frozen_inputs order by receipt_id"
        ).fetchall()
        original_builds = db.execute("select * from item_builds order by id").fetchall()
        commits = db.execute("select seq from version_commits order by seq").fetchall()
        checks = db.execute("select id from version_checks order by id").fetchall()
    assert reopened.inputs.index_record_lookups()["indexes"] == {
        name: "existing" if name in automatic else "created" for name in NAMES
    }
    fresh = reopened.inputs.freeze(
        {"x": request}, information_cutoff="2024-01-03", retain_batch_bounds=True
    )
    assert fresh.evidence == old.evidence and fresh.requests == old.requests
    assert (fresh.max_commit, fresh.max_check_id) == (old.max_commit, old.max_check_id)
    assert reopened.inputs.is_current(fresh) == before
    assert reopened.inputs.get(source).digest == source.digest
    with sqlite3.connect(lake.data_meta_path) as db:
        assert (
            db.execute("select * from item_builds order by id").fetchall()
            == original_builds
        )
        assert (
            db.execute("select seq from version_commits order by seq").fetchall()
            == commits
        )
        assert (
            db.execute("select id from version_checks order by id").fetchall() == checks
        )
        now_sources = dict(
            (row[0], row) for row in db.execute("select * from frozen_inputs")
        )
        assert all(now_sources[row[0]] == row for row in original_sources)


@pytest.mark.parametrize(
    "case",
    [
        "readonly",
        "context",
        "constructed_bound",
        "active_bound",
        "atomic",
        "writer_session",
    ],
)
def test_public_maintenance_rejects_bound_or_borrowed_operations(tmp_path, case):
    lake = lake_at(tmp_path)
    source = populated(lake)
    remove_indexes(lake)
    if case == "readonly":
        readonly = DataLake.open(
            data_meta_path=lake.data_meta_path, lake_path=lake.lake_path, read_only=True
        )
        with pytest.raises(PermissionError):
            readonly.inputs.index_record_lookups()
    elif case == "context":
        with lake.inputs.read_context(source):
            with pytest.raises(RuntimeError, match="outside"):
                lake.inputs.index_record_lookups()
    elif case in ("constructed_bound", "active_bound"):
        with input_read_boundary(
            lake.data_meta_path, source.max_commit, max_check_id=source.max_check_id
        ):
            candidate = lake_at(tmp_path) if case == "constructed_bound" else lake
            with pytest.raises(RuntimeError, match="unfrozen"):
                candidate.inputs.index_record_lookups()
    else:
        manager = (
            lake._data_meta.atomic_transaction
            if case == "atomic"
            else lake._data_meta.writer_session
        )
        with manager():
            with pytest.raises(RuntimeError, match="context"):
                lake.inputs.index_record_lookups()
    with sqlite3.connect(lake.data_meta_path) as db:
        assert not names(db)


def test_exact_indexes_cover_three_unchanged_queries_and_keep_scalar_semantics():
    original = "select definition_hash,dependency_digest,start_date,end_date,input_commit,result_commit,frozen_receipt_id from item_builds b join frozen_inputs f on f.receipt_id=b.frozen_receipt_id where name=? and status='success' and definition_hash=? and input_commit<=? and (result_commit is null or result_commit<=?) and cast(json_extract(f.payload_json,'$.max_check_id') as integer)<=? and (? is null or start_date<=?) and (? is null or end_date>=?) order by b.id desc limit 1"
    parameters = ('x', 'definition', 1000, 1000, 10, '2024-01-01', '2024-01-01', '2024-01-31', '2024-01-31')
    db = mini_db()
    for i, v in enumerate([1, 100, None, "9", True, 9.9, "abc"]):
        put(db, str(i), json.dumps({"max_check_id": v, "padding": "x" * 4096}))
        db.execute(
            "insert into item_builds values(?,?,?,?,?,?,?,?,?,?)",
            (
                i + 1,
                "x",
                "definition",
                "success",
                "d",
                "2024-01-01",
                "2024-01-31",
                1,
                None,
                str(i),
            ),
        )
        db.execute("insert into item_completion_proofs values(?,?)", (i + 1, "d"))
    put(db, "missing", json.dumps({"padding": "x" * 4096}))
    db.execute(
        "insert into item_builds values(?,?,?,?,?,?,?,?,?,?)",
        (
            8,
            "x",
            "definition",
            "success",
            "d",
            "2024-01-01",
            "2024-01-31",
            1,
            None,
            "missing",
        ),
    )
    db.commit()
    pending = (
        "select b.*,p.digest as proof_digest from item_builds b join item_completion_proofs p on p.build_id=b.id "
        "join frozen_inputs f on f.receipt_id=b.frozen_receipt_id where name=? and definition_hash=? and status='success' "
        "and input_commit<=? and (result_commit is null or result_commit<=?) "
        "and cast(json_extract(f.payload_json,'$.max_check_id') as integer)<=? "
        "and (? is null or end_date>=?) and (? is null or start_date<=?) order by b.id desc"
    )
    empty = (
        "select b.* from item_builds b join frozen_inputs f on f.receipt_id=b.frozen_receipt_id "
        "where name=? and definition_hash=? and status='success' and result_commit is null and input_commit<=? "
        "and cast(json_extract(f.payload_json,'$.max_check_id') as integer)<=? "
        "and (? is null or end_date>=?) and (? is null or start_date<=?)"
    )
    queries = [
        (original, parameters, False),
        (pending, parameters, False),
        (empty, ("x", "definition", 1000, 10, *parameters[5:]), True),
    ]
    before = [list(map(tuple, db.execute(sql, args))) for sql, args, _ in queries]
    install(db)
    for (sql, args, unordered), old in zip(queries, before):
        current = list(map(tuple, db.execute(sql, args)))
        assert (set(current) == set(old)) if unordered else (current == old)
        plan = [row[3] for row in db.execute("explain query plan " + sql, args)]
        assert any("COVERING INDEX frozen_inputs_check_lookup" in row for row in plan)
        assert not any(
            row[1] == "Function" and "json_extract" in str(row[5])
            for row in db.execute("explain " + sql, args)
        )
    db.execute(
        "update frozen_inputs set payload_json=? where receipt_id='1'",
        (json.dumps({"max_check_id": 9}),),
    )
    assert (
        db.execute(
            "select cast(json_extract(payload_json,'$.max_check_id') as integer) from frozen_inputs where receipt_id='1'"
        ).fetchone()[0]
        == 9
    )
    db.rollback()
    assert (
        db.execute(
            "select cast(json_extract(payload_json,'$.max_check_id') as integer) from frozen_inputs where receipt_id='1'"
        ).fetchone()[0]
        == 100
    )


def test_malformed_history_fails_atomically_without_rewriting_evidence():
    db = mini_db()
    put(db, "bad", "malformed")
    db.commit()
    original = snapshot(db)
    with pytest.raises(sqlite3.OperationalError, match="malformed JSON"):
        install(db)
    assert not names(db) and snapshot(db) == original and not db.in_transaction


@pytest.mark.parametrize("fault", ["conflicting", "uppercase", "changed_path"])
def test_conflicting_named_definition_cannot_be_silently_adopted(fault):
    db = mini_db()
    if fault == "conflicting":
        db.execute(
            "create index frozen_inputs_check_lookup on frozen_inputs(receipt_id)"
        )
    elif fault == "uppercase":
        db.execute(
            "create index frozen_inputs_check_lookup on frozen_inputs(receipt_id,CAST(json_extract(payload_json,'$.max_check_id') AS INTEGER))"
        )
    else:
        db.execute(
            "CREATE INDEX frozen_inputs_check_lookup on frozen_inputs(receipt_id,cast(json_extract(payload_json,'$.max_check_ id') as integer))"
        )
    with pytest.raises(RuntimeError, match="conflicting"):
        install(db)
    assert names(db) == {"frozen_inputs_check_lookup"}


@pytest.mark.parametrize("field", ["payload", "item_key"])
def test_budget_admits_headers_before_any_index_mutation(field):
    db = mini_db()
    if field == "payload":
        put(db, "large", json.dumps({"padding": "x" * 10000}))
    else:
        db.execute(
            "insert into item_builds(id,name,definition_hash) values(1,?,?)",
            ("x" * 10000, "def"),
        )
    db.commit()
    before = snapshot(db)
    saved = {
        k: db.execute(f"pragma {k}").fetchone()[0]
        for k in ("cache_size", "temp_store", "threads", "cache_spill")
    }
    trace = []
    db.set_trace_callback(trace.append)
    with pytest.raises(MemoryError, match="admission"):
        install_record_lookups(
            db,
            config=ExecutionOptions(max_buffer_bytes=16 * 1024),
            check_canceled=lambda: None,
        )
    assert not names(db) and snapshot(db) == before
    assert not any(sql.lower().startswith("create index") for sql in trace)
    assert saved == {k: db.execute(f"pragma {k}").fetchone()[0] for k in saved}
    assert not db.in_transaction


def test_sql_cancellation_rolls_back_first_ddl_and_restores_local_limits():
    db = mini_db()
    for i in range(2000):
        put(db, str(i), json.dumps({"max_check_id": i, "padding": "x" * 4096}))
    db.commit()
    old_limit = db.getlimit(sqlite3.SQLITE_LIMIT_LENGTH)
    saved = {
        k: db.execute(f"pragma {k}").fetchone()[0]
        for k in ("cache_size", "temp_store", "threads", "cache_spill")
    }
    active = [False]
    db.set_trace_callback(
        lambda sql: (
            active.__setitem__(0, True)
            if sql.lower().startswith("create index frozen_inputs")
            else None
        )
    )

    class Canceled(RuntimeError):
        pass

    def check():
        if active[0]:
            raise Canceled("independent cancellation")

    with pytest.raises(Canceled, match="independent"):
        install_record_lookups(db, config=ExecutionOptions(), check_canceled=check)
    assert not names(db) and not db.in_transaction
    assert db.getlimit(sqlite3.SQLITE_LIMIT_LENGTH) == old_limit
    assert saved == {k: db.execute(f"pragma {k}").fetchone()[0] for k in saved}
    assert db.execute("select count(*) from frozen_inputs").fetchone()[0] == 2000


def test_existing_idempotence_does_not_rescan_frozen_bodies():
    db = mini_db()
    install(db)
    trace = []
    db.set_trace_callback(trace.append)
    assert set(
        install_record_lookups(
            db, config=ExecutionOptions(max_buffer_bytes=1), check_canceled=lambda: None
        )["indexes"].values()
    ) == {"existing"}
    assert not any("octet_length" in sql or "begin immediate" in sql for sql in trace)


def test_utf8_body_admission_uses_bytes_not_character_count():
    db = mini_db()
    body = json.dumps({"max_check_id": 1, "padding": "🙂" * 400}, ensure_ascii=False)
    assert len(body) < 512 < len(body.encode())
    put(db, "unicode", body)
    db.commit()
    with pytest.raises(MemoryError, match="admission"):
        install_record_lookups(
            db,
            config=ExecutionOptions(max_buffer_bytes=16 * 1024),
            check_canceled=lambda: None,
        )
    assert not names(db)


def test_context_and_external_wal_index_build_keep_old_view_and_sources(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    lake = lake_at(tmp_path)
    source = populated(lake)
    root = lake.inputs.freeze(
        {"out": ItemInput("out")}, information_cutoff="2024-01-03"
    )
    remove_indexes(lake)
    foreign = lake_at(tmp_path)
    with sqlite3.connect(lake.data_meta_path) as db:
        automatic = names(db)
        assert automatic == empty_lookup_tables(db)
    with lake.inputs.read_context(root):
        assert lake.inputs.is_current(root)
        with ThreadPoolExecutor(max_workers=1) as worker:
            report = worker.submit(foreign.inputs.index_record_lookups).result(
                timeout=5
            )
        assert report["indexes"] == {
            name: "existing" if name in automatic else "created" for name in NAMES
        }
        assert lake.inputs.get(
            source
        ).digest == source.digest and lake.inputs.is_current(root)
    assert lake.inputs.is_current(root)


def test_query_index_does_not_mask_parent_checksum_or_cache_currentness(tmp_path):
    lake = lake_at(tmp_path)
    source = populated(lake)
    root = lake.inputs.freeze(
        {"out": ItemInput("out")}, information_cutoff="2024-01-03"
    )
    assert lake.inputs.is_current(root)
    with sqlite3.connect(lake.data_meta_path) as db:
        body = json.loads(
            db.execute(
                "select payload_json from frozen_inputs where receipt_id=?",
                (source.receipt_id,),
            ).fetchone()[0]
        )
        body["review_corruption"] = "valid JSON but wrong SHA"
        db.execute(
            "update frozen_inputs set payload_json=? where receipt_id=?",
            (json.dumps(body), source.receipt_id),
        )
    with pytest.raises(RuntimeError, match="checksum"):
        lake.inputs.is_current(root)
    with pytest.raises(RuntimeError, match="checksum"):
        lake.inputs.get(source)


def test_public_explicit_maintenance_does_not_write_any_rows_or_bytes(
    tmp_path, monkeypatch
):
    lake = lake_at(tmp_path)
    source = populated(lake)
    remove_indexes(lake)
    files = {
        path.relative_to(lake.lake_path): (
            path.stat().st_size,
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        for path in lake.lake_path.rglob("*")
        if path.is_file()
    }

    def forbidden(*args, **kwargs):
        pytest.fail("No numerical producer/provider/audit")

    monkeypatch.setattr(lake.inputs, "verify", forbidden)
    monkeypatch.setattr(lake.items, "update", forbidden)
    monkeypatch.setattr(lake.raw, "update", forbidden)
    with sqlite3.connect(lake.data_meta_path) as db:
        tables = [
            row[0]
            for row in db.execute(
                "select name from sqlite_master where type='table' order by name"
            )
        ]
        before = {
            table: db.execute(f'SELECT * FROM "{table}"').fetchall() for table in tables
        }
    lake.inputs.index_record_lookups()
    with sqlite3.connect(lake.data_meta_path) as db:
        assert {
            table: db.execute(f'SELECT * FROM "{table}"').fetchall() for table in tables
        } == before
    assert {
        path.relative_to(lake.lake_path): (
            path.stat().st_size,
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        for path in lake.lake_path.rglob("*")
        if path.is_file()
    } == files
    assert lake.inputs.get(source).digest == source.digest


def test_empty_table_rechecked_under_writer_fence_before_cheap_initialization(tmp_path):
    path = tmp_path / "isolated.sqlite"
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute(
        "create table frozen_inputs(receipt_id text primary key,payload_json text not null,digest text not null,created_at text not null)"
    )
    db.execute(
        "create table item_builds(id integer primary key,name text,definition_hash text,status text)"
    )
    db.execute("create table version_checks(id integer primary key,source text,dataset text)")
    db.commit()

    class Race:
        def __getattr__(self, key):
            return getattr(db, key)

        def execute(self, sql, *args):
            if sql == "begin immediate":
                with sqlite3.connect(path) as other:
                    other.execute(
                        "insert into frozen_inputs values(?,?,?,?)",
                        ("legacy", "malformed", "original", "original"),
                    )
            return db.execute(sql, *args)

    report = install_record_lookups(
        cast(sqlite3.Connection, Race()), config=ExecutionOptions(), check_canceled=lambda: None, empty_only=True
    )
    assert report["indexes"] == {"item_builds_record_lookup": "created", "version_checks_record_lookup": "created"}
    assert names(db) == {"item_builds_record_lookup", "version_checks_record_lookup"}
    assert (
        db.execute("select payload_json from frozen_inputs").fetchone()[0]
        == "malformed"
    )


def test_version_check_lookup_preserves_boundaries_and_avoids_other_datasets():
    db = mini_db()
    db.execute("alter table version_checks add column visible_commit integer")
    db.execute("create table version_commits(seq integer primary key,baseline integer,status text)")
    db.executemany("insert into version_commits values(?,?,?)", [(1, 1, "committed"), (2, 0, "committed"), (3, 0, "prepared")])
    db.executemany("insert into version_checks values(?,?,?,?)", [(i, "other", "close", 1) for i in range(1, 2001)])
    db.executemany("insert into version_checks values(?,?,?,?)", [(2001, "items", "close", None),
        (2002, "items", "close", 1), (2003, "items", "close", 2),
        (2004, "items", "close", 3), (2005, "items", "close", None),
        (2006, "items", "volume", 1)])
    db.commit()
    sql = ("select v.*,c.baseline as content_baseline from version_checks v "
           "left join version_commits c on c.seq=v.visible_commit "
           "where v.source=? and v.dataset=? and v.id<=? "
           "and (v.visible_commit is null or v.visible_commit<=?) "
           "and (c.seq is null or c.status='committed') order by v.id")
    parameters = ("items", "close", 2004, 2)

    def measured():
        steps = [0]
        def step():
            steps[0] += 1
            return 0
        db.set_progress_handler(step, 1)
        try:
            return list(map(tuple, db.execute(sql, parameters))), steps[0]
        finally:
            db.set_progress_handler(None, 0)

    before, old_steps = measured()
    assert [row[0] for row in before] == [2001, 2002, 2003]
    install(db)
    after, new_steps = measured()
    assert after == before
    assert new_steps * 10 < old_steps
    assert any("version_checks_record_lookup" in row[3] for row in db.execute("explain query plan " + sql, parameters))


def test_version_check_metadata_key_respects_maintenance_budget():
    db = mini_db()
    db.execute("insert into version_checks values(?,?,?)", (1, "x" * 3000, "d"))
    db.commit()
    with pytest.raises(MemoryError, match="Version check key"):
        install_record_lookups(db, config=ExecutionOptions(max_buffer_bytes=16 * 1024), check_canceled=lambda: None)
    assert not names(db)


def test_sqlite_temp_and_worker_policy_is_local_and_serial():
    db = mini_db()
    before = {
        key: db.execute(f"pragma {key}").fetchone()[0]
        for key in ("temp_store", "cache_size", "cache_spill", "threads")
    }
    trace = []
    db.set_trace_callback(trace.append)
    install(db)
    assert "pragma temp_store=FILE" in trace and "pragma threads=0" in trace
    assert "pragma cache_spill=ON" in trace and "pragma cache_size=-8192" in trace
    assert before == {key: db.execute(f"pragma {key}").fetchone()[0] for key in before}
