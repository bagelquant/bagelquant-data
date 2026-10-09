from datetime import UTC, date, datetime
import sqlite3

import polars as pl

from bagelquant_data import DataLake, DatasetSpec, RawInput
from bagelquant_data.storage.data_meta import DataMetaStore


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
        db.row_factory = sqlite3.Row
        previous = db.execute(
            "select r.* from version_check_records r join version_checks v on v.id=r.check_id "
            "join version_commits c on c.seq=r.version_commit where v.source=? and v.dataset=? "
            "and v.id<=? and r.version_commit<=? and v.baseline=0 and c.baseline=1 "
            "and c.status='committed' order by r.check_id,r.record_id",
            ("custom", "selected", receipt.max_check_id, receipt.max_commit),
        ).fetchall()
        assert receipt.evidence["raw"]["record_checks"] == [dict(row) for row in previous]
        ordinary = db.execute(
            "select r.* from version_check_records r join version_checks c on c.id=r.check_id "
            "where c.source=? and c.dataset=? order by r.check_id,r.record_id",
            ("custom", "selected"),
        ).fetchall()
    snapshot = DataMetaStore(lake.data_meta_path, read_only=True, runtime=True).dataset_snapshot("custom", "selected")
    assert snapshot["record_checks"] == [dict(row) for row in ordinary]
