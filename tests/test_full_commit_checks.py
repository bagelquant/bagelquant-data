from datetime import UTC, date, datetime
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, RawInput
from bagelquant_data.storage.data_meta import DataMetaStore


def prepared(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("selected", "by_date", date_kind="calendar",
                       field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2020, 1, 1), date(2020, 2, 1)],
                          "asset_id": ["A", "B"], "value": [1., 2.]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2020, 2, 2, tzinfo=UTC))
    lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 2, 3, tzinfo=UTC))
    return lake


def request(**kwargs):
    return {"raw": RawInput("custom", "selected", view="snapshot", strict=True, **kwargs)}


def test_implicit_input_boundary_uses_committed_wal_without_offline_inspection(tmp_path, monkeypatch):
    from bagelquant_data.inputs import current_input_check_boundary, input_read_boundary

    lake = prepared(tmp_path)
    with sqlite3.connect(lake.data_meta_path) as connection:
        expected = connection.execute("SELECT MAX(id) FROM version_checks").fetchone()[0]

    def reject_copy(*args, **kwargs):
        pytest.fail("a live input boundary attempted offline metadata inspection")

    monkeypatch.setattr(DataMetaStore, "_inspect_schema", reject_copy)
    with sqlite3.connect(lake.data_meta_path) as writer:
        writer.execute("UPDATE data_meta_state SET value='invalid' WHERE key='schema_version'")
        with input_read_boundary(lake.data_meta_path, lake.inputs.max_commit()):
            assert current_input_check_boundary(lake.data_meta_path) == expected
        writer.rollback()
    assert current_input_check_boundary(lake.data_meta_path) is None


def test_implicit_input_boundary_retains_original_check_ceiling_after_new_commit(tmp_path, monkeypatch):
    from bagelquant_data.inputs import current_input_check_boundary, input_read_boundary

    lake = prepared(tmp_path)
    commit = lake.inputs.max_commit()
    with input_read_boundary(lake.data_meta_path, commit):
        original_check = current_input_check_boundary(lake.data_meta_path)
        lake.raw.ingest(lake.raw.get("selected", source="custom"),
                        pl.DataFrame({"time": [date(2020, 1, 1), date(2020, 2, 1)],
                                      "asset_id": ["A", "B"], "value": [1., 2.]}),
                        ingested_at=datetime(2020, 2, 4, tzinfo=UTC))
        with sqlite3.connect(lake.data_meta_path) as connection:
            latest = connection.execute("SELECT MAX(id) FROM version_checks").fetchone()[0]
        assert latest > original_check
        with input_read_boundary(lake.data_meta_path, commit):
            assert current_input_check_boundary(lake.data_meta_path) == original_check
        with pytest.raises(ValueError, match="cannot widen max_check_id"):
            with input_read_boundary(lake.data_meta_path, commit, max_check_id=latest):
                pass
        assert current_input_check_boundary(lake.data_meta_path) == original_check
    assert current_input_check_boundary(lake.data_meta_path) is None
