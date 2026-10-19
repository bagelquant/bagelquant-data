"""Current frozen input authority can refresh a stale live dependency flag."""
import sqlite3

import pytest

from bagelquant_data.storage.data_meta import DataMetaStore
from tests.test_incremental_items import append, setup


@pytest.mark.parametrize("record_only", [False, True])
def test_earlier_noop_refreshes_dependency_flag_without_replay_or_regression(tmp_path, monkeypatch, record_only):
    lake, source, _, calls = setup(tmp_path)
    builds = lake.items.completed_builds("twice")
    original = lake.inputs.get(builds[-1]["frozen_receipt_id"])
    before = lake.items.completion_statuses(names=["twice"])[0]
    append(lake, source)
    assert lake.inputs.is_current(original)
    assert not lake.items.completion_statuses(names=["twice"])[0]["dependency_current"]
    assert lake.items.plan_update("twice", start="2020-01-02", end="2020-01-02") == []
    def forbidden(*_a, **_k):
        pytest.fail("no-op flag refresh read values, created Source/proof or ran a producer")
    for method in ("read", "freeze", "verify", "index_selection"):
        monkeypatch.setattr(lake.inputs, method, forbidden)
    monkeypatch.setattr(lake.items, "_build", forbidden)
    operation = lake.items.recertify if record_only else lake.items.update
    report = operation("twice", start="2020-01-02", end="2020-01-02")
    assert report.status == "unchanged" and report.rows_committed == 0 and not calls
    after = lake.items.completion_statuses(names=["twice"])[0]
    assert after["dependency_current"]
    assert {key: value for key, value in before.items() if key not in {"dependency_state_digest", "updated_at"}} == {
        key: value for key, value in after.items() if key not in {"dependency_state_digest", "updated_at"}}
    assert after["checked_target"] == "2020-01-03"
    assert lake.items.completed_builds("twice") == builds
    assert lake.inputs.get(original).digest == original.digest
    monkeypatch.setattr(DataMetaStore, "write_completion", forbidden)
    assert operation("twice", start="2020-01-02", end="2020-01-02").status == "unchanged"
    assert lake.items.completion_statuses(names=["twice"])[0] == after


def test_cancel_after_flag_refresh_rolls_back_dependency_admission(tmp_path, monkeypatch):
    lake, source, _, calls = setup(tmp_path)
    append(lake, source)
    before = lake.items.completion_statuses(names=["twice"])[0]
    builds = lake.items.completed_builds("twice")
    cancelled = False
    write = DataMetaStore.write_completion
    def cancel(db, **value):
        nonlocal cancelled
        write(db, **value)
        cancelled = True
    monkeypatch.setattr(DataMetaStore, "write_completion", cancel)
    report = lake.items.recertify("twice", start="2020-01-02", end="2020-01-02", cancelled=lambda: cancelled)
    assert report.status == "cancelled" and cancelled and not calls
    assert lake.items.completion_statuses(names=["twice"])[0] == before
    assert lake.items.completed_builds("twice") == builds


def test_later_output_check_is_not_certified_as_the_original_completion(tmp_path, monkeypatch):
    lake, source, _, calls = setup(tmp_path)
    builds = lake.items.completed_builds("twice")
    original = lake.inputs.get(builds[-1]["frozen_receipt_id"])
    rows = lake.items.read("twice", view="snapshot", as_of="2020-01-03").collect().select("time", "asset_id", "value")
    append(lake, source)
    lake.items.ingest("twice", rows, available_date="2020-01-03", input_receipt=original)
    before = lake.items.completion_statuses(names=["twice"])[0]
    assert not before["dependency_current"]
    monkeypatch.setattr(DataMetaStore, "write_completion", lambda *_a, **_k: pytest.fail("unproved later output/check refresh"))
    monkeypatch.setattr(lake.items, "_build", lambda *_a, **_k: pytest.fail("historical replay"))
    try:
        report = lake.items.recertify("twice", start="2020-01-02", end="2020-01-02")
    except RuntimeError as error:
        assert "blocked" in str(error) or "recertification requires unchanged" in str(error)
    else:
        assert report.status == "unchanged" and report.rows_committed == 0
    assert lake.items.completion_statuses(names=["twice"])[0] == before
    assert lake.items.completed_builds("twice") == builds and not calls


@pytest.mark.parametrize("change", ["incomplete", "wider", "later_check", "foreign_result"])
def test_current_source_cannot_refresh_a_different_or_incomplete_flag(tmp_path, monkeypatch, change):
    lake, source, _, calls = setup(tmp_path)
    append(lake, source)
    with sqlite3.connect(lake.data_meta_path) as db:
        if change == "incomplete":
            db.execute("update datasets set update_incomplete=1 where source='items' and name='twice'")
        elif change == "wider":
            db.execute("update node_completions set completed_start='2020-01-01' where node_key='items/twice'")
        elif change == "later_check":
            db.execute("update node_completions set checked_target='2020-01-04' where node_key='items/twice'")
        else:
            db.execute("update node_completions set result_ref='foreign' where node_key='items/twice'")
    before = lake.items.completion_statuses(names=["twice"])[0]
    monkeypatch.setattr(DataMetaStore, "write_completion", lambda *_a, **_k: pytest.fail("unproved flag refresh"))
    lake.items.recertify("twice", start="2020-01-02", end="2020-01-02")
    assert lake.items.completion_statuses(names=["twice"])[0] == before
    assert not calls
