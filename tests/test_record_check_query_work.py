from datetime import UTC, date, datetime
import sqlite3

import polars as pl

from bagelquant_data import DataLake, DatasetSpec, RawInput
from bagelquant_data.storage.data_meta import DataMetaStore


def test_scoped_check_candidates_batch_queries_and_retain_mixed_month_witness(tmp_path, monkeypatch):
    from bagelquant_data import DataItemSpec, ItemInput
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("selected"))
    jan, feb = date(2024, 1, 1), date(2024, 2, 1)
    def frame(day):
        return pl.DataFrame({"time": [day], "asset_id": ["A"], "value": [1.]})
    lake.items.ingest("selected", frame(jan), available_date=jan, historical_baseline=True)
    lake.items.ingest("selected", frame(feb), available_date=feb, historical_baseline=True)
    for _ in range(65):
        lake.items.ingest("selected", frame(feb), available_date=feb)
    lake.items.ingest("selected", pl.concat([frame(jan), frame(feb)]), available_date=feb)
    queries = []
    original = DataMetaStore._new_connection
    def tracked(store):
        db = original(store)
        db.set_trace_callback(queries.append)
        return db
    with monkeypatch.context() as patch:
        patch.setattr(DataMetaStore, "_new_connection", tracked)
        frozen = lake.inputs.freeze({"selected": ItemInput("selected", start=jan, end=jan, view="versions")},
            information_cutoff=feb)
    assert not any("version_check_records" in query for query in queries)
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='version_check_records'").fetchone() is None
    events = frozen.evidence["selected"]["record_checks"]
    assert len(events) == 1
    assert events[0]["version_commits"] == [frozen.evidence["selected"]["batches"][0]["commit_seq"]]
    assert events[0]["pit_date"] == str(feb)
    assert lake.inputs.read(frozen, "selected", view="snapshot").collect()["value"].to_list() == [1.]
    assert lake.inputs.is_current(frozen)


def test_selected_evidence_work_is_independent_of_unrelated_record_volume(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    for name, count in [("selected", 2), ("unrelated", 5_000)]:
        spec = DatasetSpec(name, "by_date", date_kind="calendar",
                           field_mappings={"time": "time", "asset_id": "asset_id"})
        frame = pl.DataFrame({"time": [date(2020, 1, 1)] * count,
                              "asset_id": [f"A{i:05}" for i in range(count)],
                              "value": [float(i) for i in range(count)]})
        lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2020, 1, 2, tzinfo=UTC))
        lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    steps = [0]
    original = DataMetaStore._new_connection
    def counting_connection(store):
        db = original(store)
        def progress():
            steps[0] += 100
            return 0
        db.set_progress_handler(progress, 100)
        return db
    with monkeypatch.context() as patch:
        patch.setattr(DataMetaStore, "_new_connection", counting_connection)
        receipt = lake.inputs.freeze({"raw": RawInput("custom", "selected", view="snapshot", strict=True)},
                                     information_cutoff="2020-01-03")
        assert steps[0] < 20_000, "freeze scanned unrelated row-level witnesses"
        steps[0] = 0
        selected = lake.raw.read("selected", source="custom", strict=True, as_of="2020-01-03").collect()
        assert steps[0] < 20_000, "ordinary read scanned unrelated row-level witnesses"
    assert selected.sort("asset_id")["value"].to_list() == [0.0, 1.0]
    assert lake.inputs.read(receipt, "raw").collect().sort("asset_id")["value"].to_list() == [0.0, 1.0]
    assert lake.inputs.verify(receipt)["valid"]
    assert lake.inputs.is_current(receipt)
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='version_check_records'").fetchone() is None
        assert db.execute("SELECT COUNT(*) FROM visibility_segments v JOIN version_checks c ON c.id=v.check_id WHERE c.source='custom' AND c.dataset='selected'").fetchone()[0] == 1
    snapshot = DataMetaStore(lake.data_meta_path, read_only=True, runtime=True).dataset_snapshot("custom", "selected")
    assert len(snapshot["record_checks"]) == 1
    assert snapshot["record_checks"][0]["row_count"] == 2


def test_baseline_check_witness_volume_never_enters_ordinary_selection(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("selected", "by_date", date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2020, 1, 2, tzinfo=UTC))
    # The same initialization baseline may be checked repeatedly; only one
    # tiny check header per acquisition is retained, never row witnesses.
    with sqlite3.connect(lake.data_meta_path) as db:
        db.executemany("INSERT INTO version_checks(source,dataset,run_id,checked_at,pit_date,baseline,visible_commit,request_json,row_count) VALUES('custom','selected','fixture','2020-01-03T00:00:00+00:00','2020-01-03',1,1,'[]',1)", (() for _ in range(65)))
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='version_check_records'").fetchone() is None
    steps = [0]
    original = DataMetaStore._new_connection
    def tracked(store):
        db = original(store)
        db.set_progress_handler(lambda: (steps.__setitem__(0, steps[0] + 100), 0)[1], 100)
        return db
    with monkeypatch.context() as scoped:
        scoped.setattr(DataMetaStore, "_new_connection", tracked)
        snapshot = DataMetaStore(lake.data_meta_path, read_only=True, runtime=True).dataset_snapshot("custom", "selected")
        assert snapshot["record_checks"] == []
        assert steps[0] < 10_000
        assert lake.raw.read("selected", source="custom").collect()["value"].to_list() == [1.]
        assert lake.raw.read("selected", source="custom", strict=True).collect().is_empty()


def test_general_atomic_checks_do_not_decode_row_witnesses(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("selected", "general")
    frame = pl.DataFrame({"time": [date(2020, 1, 1)], "value": [1.]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2020, 1, 2, tzinfo=UTC))
    lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    snapshot = DataMetaStore(lake.data_meta_path, read_only=True, runtime=True).dataset_snapshot("custom", "selected")
    assert snapshot["record_checks"] == []
    assert lake.raw.read("selected", source="custom", strict=True).collect()["value"].to_list() == [1.]


def test_redeclared_general_type_cannot_hide_original_by_date_witnesses(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("selected", "by_date", date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2020, 1, 2, tzinfo=UTC))
    lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    lake.raw.register(DatasetSpec("selected", "general"))
    assert lake.raw.read("selected", source="custom", strict=True, as_of="2020-01-03").collect()["value"].to_list() == [1.]
