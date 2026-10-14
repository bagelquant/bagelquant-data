"""Numerical equality plus proof of bounded producer/typed-read work."""
from datetime import UTC, date, datetime
import sqlite3

import polars as pl
import pytest
from polars.testing import assert_frame_equal

from bagelquant_data import DataLake, DataItemSpec, DatasetSpec, ProducerUpdatePolicy, RawInput


def setup(tmp_path, *, policy=True, empty=False):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    source = DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    initial = pl.DataFrame({"time": [date(2020, 1, 2), date(2020, 1, 3)],
        "asset_id": ["A", "A"], "value": [2., 3.]})
    lake.raw.ingest(source, initial.head(0) if empty else initial, mode="initialize",
        ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    calls = []
    def producer(context):
        calls.append((context.start, context.end, context.information_cutoff,
                      context.frames["raw"].height))
        if context.frames["raw"].is_empty():
            return pl.DataFrame(schema={"time": pl.Date, "asset_id": pl.String, "value": pl.Float64})
        return context.frames["raw"].select(pl.col("source_time").alias("time"),
            "asset_id", (pl.col("value") * 2).alias("value"))
    lake.items.register_producer("twice", "1", producer,
        update_policy=ProducerUpdatePolicy("same_date") if policy else None)
    lake.items.register(DataItemSpec("twice", (RawInput("custom", "daily", alias="raw"),),
        producer_key="twice", producer_revision="1"))
    lake.items.initialize("twice", start="2020-01-02", end="2020-01-03")
    calls.clear()
    return lake, source, producer, calls


def append(lake, source, day=4, value=4.):
    lake.raw.ingest(source, pl.DataFrame({"time": [date(2020, 1, day)],
        "asset_id": ["A"], "value": [value]}), ingested_at=datetime(2020, 1, 4, tzinfo=UTC))


def values(lake, name):
    return lake.items.read(name, view="snapshot", as_of="2020-01-04").select(
        "time", "asset_id", "value").collect().sort("time", "asset_id")


def test_readonly_plan_binds_producer_policy_without_registering_code_or_catalog(tmp_path, monkeypatch):
    lake, source, _producer, calls = setup(tmp_path)
    append(lake, source)
    reader = DataLake.open(data_meta_path=lake.data_meta_path, lake_path=lake.lake_path, read_only=True)
    def forbidden(*args, **kwargs):
        pytest.fail("read-only planning registered code, wrote a catalog or read original values")
    monkeypatch.setattr(reader.items, "register", forbidden)
    monkeypatch.setattr(reader.items, "register_producer", forbidden)
    monkeypatch.setattr(reader.inputs, "verify", forbidden)
    assert reader.items.plan_update("twice", start="2020-01-02", end="2020-01-04",
        update_policy=ProducerUpdatePolicy("same_date")) == [
            {"mode": "incremental", "start": "2020-01-04", "end": "2020-01-04"}]
    assert not calls
    assert reader.items.plan_update("twice", start="2020-01-02", end="2020-01-04")[0]["mode"] == "blocked"
    with pytest.raises(ValueError, match="conflicts"):
        lake.items.plan_update("twice", start="2020-01-02", end="2020-01-04",
            update_policy=ProducerUpdatePolicy("forward"))


@pytest.mark.parametrize("damage", ["missing", "table", "corrupt"])
def test_explicit_original_completion_attestation_restores_metadata_without_replay(tmp_path, monkeypatch, damage):
    from bagelquant_data import ItemCompletionAttestation
    from dataclasses import replace
    import hashlib
    import json
    from typing import Any, cast
    lake, source, _producer, calls = setup(tmp_path)
    originals = lake.items.builds("twice")
    record = originals[-1]
    receipt = lake.inputs.get(record["frozen_receipt_id"])
    # This fixture owns the original successful complete initialize operation,
    # including its complete output membership and explicit empty pending set.
    attestation = ItemCompletionAttestation(build_id=record["id"], name="twice",
        definition_hash=record["definition_hash"], start=date(2020, 1, 2), end=date(2020, 1, 3),
        input_commit=record["input_commit"], result_commit=record["result_commit"],
        dependency_digest=record["dependency_digest"], frozen_receipt_id=receipt.receipt_id,
        frozen_receipt_digest=receipt.digest, information_cutoff=receipt.information_cutoff,
        report_id="fixture:original-whole-range-producer-report",
        report_digest=hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest(),
        complete_output_range=True, pending_coordinates=())
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("delete from item_completion_proofs where build_id=?", (record["id"],))
        db.execute("delete from item_completion_authorities where build_id=?", (record["id"],))
        db.execute("delete from item_pending_outputs where build_id=?", (record["id"],))
    append(lake, source)
    assert lake.items.plan_update("twice", start="2020-01-02", end="2020-01-04")[0]["mode"] == "blocked"
    def forbidden(*_args, **_kwargs):
        pytest.fail("completion maintenance consumed original values or ran a producer")
    monkeypatch.setattr(lake.inputs, "verify", forbidden)
    with pytest.raises(ValueError, match="frozen input"):
        lake.items.index_completion(replace(attestation, frozen_receipt_digest="different"))
    with pytest.raises(ValueError, match="build record"):
        lake.items.index_completion(replace(attestation, end=date(2020, 1, 4)))
    result = lake.items.index_completion(attestation)
    assert lake.items.index_completion(attestation) == result
    with pytest.raises(ValueError, match="immutable"):
        lake.items.index_completion(replace(attestation, report_id="another-report"))
    assert lake.items.builds("twice") == originals
    assert not calls
    assert lake.items.plan_update("twice", start="2020-01-02", end="2020-01-04") == [
        {"mode": "incremental", "start": "2020-01-04", "end": "2020-01-04"}]
    with pytest.raises(TypeError, match="Attestation"):
        lake.items.index_completion(cast(Any, record))
    with sqlite3.connect(lake.data_meta_path) as db:
        if damage == "table":
            db.execute("drop table item_completion_attestations")
        elif damage == "missing":
            db.execute("delete from item_completion_attestations")
        else:
            db.execute("update item_completion_attestations set payload_json='{}'")
    with pytest.raises(RuntimeError, match="completion authority"):
        lake.items.plan_update("twice", start="2020-01-02", end="2020-01-04")


def test_append_plans_and_computes_only_new_outputs_and_reads(tmp_path, monkeypatch):
    lake, source, producer, calls = setup(tmp_path)
    append(lake, source)
    def no_original_audit(*args, **kwargs):
        raise AssertionError("prerequisite audited original data")
    monkeypatch.setattr(lake.inputs, "verify", no_original_audit)
    assert lake.items.plan_update("twice", start="2020-01-02", end="2020-01-04") == [
        {"mode": "incremental", "start": "2020-01-04", "end": "2020-01-04"}]
    report = lake.items.update("twice", start="2020-01-02", end="2020-01-04")
    assert report.status == "success" and report.start == date(2020, 1, 4)
    assert calls and all(first == last == date(2020, 1, 4) for first, last, _, _ in calls)
    assert all(rows == 1 for _, _, _, rows in calls)
    assert lake.items.builds("twice")[-1]["start_date"] == "2020-01-02"
    lake.items.register_producer("reference", "1", producer)
    lake.items.register(DataItemSpec("reference", (RawInput("custom", "daily", alias="raw"),),
        producer_key="reference", producer_revision="1"))
    lake.items.initialize("reference", start="2020-01-02", end="2020-01-04")
    assert_frame_equal(values(lake, "twice"), values(lake, "reference"))


def test_noop_uses_only_registered_records_and_does_not_freeze(tmp_path, monkeypatch):
    lake, source, _, calls = setup(tmp_path)
    append(lake, source)
    lake.items.update("twice", start="2020-01-02", end="2020-01-04")
    calls.clear()
    with sqlite3.connect(lake.data_meta_path) as db:
        before = db.execute("select count(*) from frozen_inputs").fetchone()[0]
    def unexpected(*args, **kwargs):
        raise AssertionError("no-op initiated numerical work or input capture")
    monkeypatch.setattr(lake.inputs, "read", unexpected)
    monkeypatch.setattr(lake.inputs, "freeze", unexpected)
    assert lake.items.plan_update("twice", start="2020-01-02", end="2020-01-04") == []
    assert lake.items.update("twice", start="2020-01-02", end="2020-01-04").status == "unchanged"
    assert not calls
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("select count(*) from frozen_inputs").fetchone()[0] == before


def test_old_observation_revision_repairs_affected_date_at_new_cutoff(tmp_path):
    lake, source, producer, calls = setup(tmp_path)
    append(lake, source, day=3, value=30.)
    append(lake, source)
    lake.items.update("twice", start="2020-01-02", end="2020-01-04")
    assert calls and all(first >= date(2020, 1, 3) for first, _, _, _ in calls)
    assert values(lake, "twice")["value"].to_list() == [4., 60., 8.]
    lake.items.register_producer("reference", "1", producer)
    lake.items.register(DataItemSpec("reference", (RawInput("custom", "daily", alias="raw"),),
        producer_key="reference", producer_revision="1"))
    lake.items.initialize("reference", start="2020-01-02", end="2020-01-04")
    assert_frame_equal(values(lake, "twice"), values(lake, "reference"))


@pytest.mark.parametrize('record_only', [False, True])
def test_noop_recertifies_proven_causal_reuse_for_final_source_currentness(tmp_path, monkeypatch, record_only):
    from bagelquant_data import ItemInput, inputs
    lake, source, _, calls = setup(tmp_path)
    calendar_spec = DatasetSpec("calendar", "general")
    calendar = pl.DataFrame({"time": [date(2020, 1, day) for day in (2, 3, 4)], "is_open": [1, 1, 1]})
    lake.raw.ingest(calendar_spec, calendar, mode="initialize", ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    lake.raw.ingest(calendar_spec, calendar, ingested_at=datetime(2020, 1, 2, tzinfo=UTC))
    def producer(context):
        calls.append((context.start, context.end))
        return context.frames["raw"].select(pl.col("source_time").alias("time"), "asset_id", "value")
    lake.items.register_producer("with_calendar", "1", producer,
        update_policy=ProducerUpdatePolicy("same_date", timing_neutral_inputs=("calendar",)))
    lake.items.register(DataItemSpec("with_calendar", (RawInput("custom", "daily", alias="raw"),
        RawInput("custom", "calendar", alias="calendar", view="versions")),
        producer_key="with_calendar", producer_revision="1"))
    lake.items.initialize("with_calendar", start="2020-01-02", end="2020-01-04")
    before = lake.items.completed_builds("with_calendar")[-1]
    original = lake.inputs.get(before["frozen_receipt_id"])
    lake.raw.ingest(calendar_spec, calendar, ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    assert not lake.inputs.is_current(original)
    assert lake.items.plan_update("with_calendar", start="2020-01-02", end="2020-01-04") == []
    calls.clear()
    def forbidden(*args, **kwargs):
        pytest.fail("no-op recertification decoded original values or ran an audit")
    with monkeypatch.context() as patch:
        patch.setattr(inputs, "read_batch", forbidden)
        patch.setattr(lake.inputs, "verify", forbidden)
        operation = lake.items.recertify if record_only else lake.items.update
        report = operation("with_calendar", start="2020-01-02", end="2020-01-04")
        assert report.status == "unchanged" and report.rows_committed == 0
        assert report.frozen_receipt_id != original.receipt_id
        final = lake.inputs.freeze({"item": ItemInput("with_calendar", start="2020-01-02", end="2020-01-04")},
            information_cutoff="2020-01-04")
        assert lake.inputs.is_current(final)
    assert not calls
    assert lake.items.completed_builds("with_calendar")[-2] == before
    assert lake.inputs.get(original).digest == original.digest
    assert lake.items.completed_builds("with_calendar")[-1]["result_commit"] == before["result_commit"]


@pytest.mark.parametrize('impact', ['append', 'revision', 'initialization'])
def test_record_only_recertification_blocks_new_impact_before_numerical_work(tmp_path, monkeypatch, impact):
    lake, source, _, calls = setup(tmp_path)
    before = lake.items.builds('twice')
    assert lake.items.plan_update('twice', start='2020-01-02', end='2020-01-03') == []
    if impact == 'initialization':
        lake.items.register(DataItemSpec('unbuilt', inputs=(RawInput('custom', 'daily', alias='raw'),)))
        name, end = 'unbuilt', '2020-01-03'
    else:
        if impact == 'append':
            append(lake, source, day=4, value=99.)
        else:
            lake.raw.ingest(source, pl.DataFrame({'time': [date(2020, 1, 3)], 'asset_id': ['A'], 'value': [99.]}),
                ingested_at=datetime(2020, 1, 3, 1, tzinfo=UTC))
        name, end = 'twice', '2020-01-04' if impact == 'append' else '2020-01-03'
    calls.clear()
    def forbidden(*args, **kwargs):
        pytest.fail('Record-only recertification initiated numerical work')
    monkeypatch.setattr(lake.items, '_build', forbidden)
    monkeypatch.setattr(lake.inputs, 'read', forbidden)
    with pytest.raises(RuntimeError, match='blocked|recertification requires unchanged'):
        lake.items.recertify(name, start='2020-01-02', end=end)
    assert not calls and lake.items.builds('twice') == before


def test_recertification_never_updates_an_unready_dependency(tmp_path, monkeypatch):
    from bagelquant_data import ItemInput
    lake, source, _, calls = setup(tmp_path)
    lake.items.register(DataItemSpec('child', inputs=(ItemInput('twice'),)))
    lake.items.initialize('child', start='2020-01-02', end='2020-01-03')
    before = lake.items.builds('child')
    lake.raw.ingest(source, pl.DataFrame({'time': [date(2020, 1, 3)], 'asset_id': ['A'], 'value': [99.]}),
        ingested_at=datetime(2020, 1, 3, 1, tzinfo=UTC))
    calls.clear()
    def forbidden(*args, **kwargs):
        pytest.fail('Record-only recertification updated a dependency')
    monkeypatch.setattr(lake.items, 'update', forbidden)
    monkeypatch.setattr(lake.items, '_build', forbidden)
    with pytest.raises(RuntimeError, match='blocked|changed during no-op'):
        lake.items.recertify('child', start='2020-01-02', end='2020-01-03')
    assert not calls and lake.items.builds('child') == before


def test_recertification_binds_caller_definition_and_immutable_runtime_policy(tmp_path):
    from dataclasses import replace
    lake, _, producer, calls = setup(tmp_path)
    before = lake.items.builds('twice')
    spec = lake.items.get('twice')
    with pytest.raises(ValueError, match='definition differs'):
        lake.items.recertify('twice', start='2020-01-02', end='2020-01-03',
            expected_definition=replace(spec, producer_revision='different'))
    with pytest.raises(ValueError, match='immutable producer revision'):
        lake.items.register_producer('twice', '1', producer, update_policy=ProducerUpdatePolicy('forward'))
    with pytest.raises(ValueError, match='immutable producer revision'):
        lake.items.register_producer('twice', '1', producer)
    report = lake.items.recertify('twice', start='2020-01-02', end='2020-01-03', expected_definition=spec)
    assert report.status == 'unchanged' and not calls and lake.items.builds('twice') == before


def test_definition_change_after_policy_capture_cannot_be_recertified(tmp_path, monkeypatch):
    from dataclasses import replace
    lake, _, _, calls = setup(tmp_path)
    before = lake.items.builds('twice')
    policy = lake.items._policy
    def switch_definition(spec):
        original = policy(spec)
        lake.items.register(replace(spec, producer_revision='2'))
        return original
    monkeypatch.setattr(lake.items, '_policy', switch_definition)
    monkeypatch.setattr(lake.inputs, 'freeze', lambda *a, **k: pytest.fail('Changed definition captured input'))
    with pytest.raises(RuntimeError, match='definition changed during recertification admission'):
        lake.items.recertify('twice', start='2020-01-02', end='2020-01-03')
    assert not calls and lake.items.builds('twice') == before


@pytest.mark.parametrize("promotion", ["repeat", "first", "earlier"])
def test_mixed_baseline_witnesses_need_complete_original_known_timing(tmp_path, promotion):
    from bagelquant_data import ItemInput
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("daily", "by_date", date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"})
    a = pl.DataFrame({"time": [date(2020, 1, 2)], "asset_id": ["A"], "value": [2.]})
    b = pl.DataFrame({"time": [date(2020, 1, 3)], "asset_id": ["B"], "value": [3.]})
    lake.raw.ingest(spec, a, mode="initialize", ingested_at=datetime(2020, 1, 2, tzinfo=UTC))
    lake.raw.ingest(spec, b, ingested_at=datetime(2020, 1, 3, 1, tzinfo=UTC))
    if promotion != "first":
        lake.raw.ingest(spec, pl.concat([a, b]), ingested_at=datetime(2020, 1, 3, 2, tzinfo=UTC))
    calls = []
    def producer(context):
        calls.append((context.start, context.end))
        return context.frames["raw"].select(pl.col("source_time").alias("time"), "asset_id", "value")
    requests = (RawInput("custom", "daily", alias="raw", view="versions", include_historical_baseline=True),)
    lake.items.register_producer("known_payload", "1", producer,
        update_policy=ProducerUpdatePolicy("same_date", timing_neutral_inputs=("raw",)))
    lake.items.register(DataItemSpec("known_payload", requests, producer_key="known_payload", producer_revision="1"))
    lake.items.initialize("known_payload", start="2020-01-02", end="2020-01-03")
    calls.clear()
    lake.raw.ingest(spec, pl.concat([a, b]), ingested_at=datetime(2020, 1, 2 if promotion == "earlier" else 3, 3, tzinfo=UTC))
    actions = lake.items.plan_update("known_payload", start="2020-01-02", end="2020-01-03")
    if promotion == "first":
        assert actions and all(action["mode"] == "incremental" for action in actions)
    else:
        # Existing first-known visibility is immutable, including a later
        # acquisition supplied with an earlier provider date.
        assert actions == []
    original = lake.items.completed_builds("known_payload")[-1]
    report = lake.items.update("known_payload", start="2020-01-02", end="2020-01-03")
    if promotion == "first":
        assert report.status == "success" and calls
    else:
        assert report.status == "unchanged" and report.rows_committed == 0 and not calls
    final = lake.inputs.freeze({"item": ItemInput("known_payload", view="versions")}, information_cutoff="2020-01-03")
    assert lake.inputs.is_current(final)
    lake.items.register_producer("reference_known", "1", producer)
    lake.items.register(DataItemSpec("reference_known", requests, producer_key="reference_known", producer_revision="1"))
    lake.items.initialize("reference_known", start="2020-01-02", end="2020-01-03")
    for strict in (False, True):
        fields = ["time", "asset_id", "value", "available_date", "version_available_date", "_baseline"]
        actual = lake.items.read("known_payload", view="history", as_of="2020-01-03", strict=strict).select(fields).collect().sort("time", "asset_id")
        expected = lake.items.read("reference_known", view="history", as_of="2020-01-03", strict=strict).select(fields).collect().sort("time", "asset_id")
        assert_frame_equal(actual, expected)
    assert original in lake.items.completed_builds("known_payload")


def test_cancel_during_noop_completion_save_rolls_back_native_authority(tmp_path, monkeypatch):
    from bagelquant_data.items import api
    lake, _, _, _ = setup(tmp_path)
    original = lake.items.completed_builds("twice")[-1]
    # Force only the pre-fence freshness probe to request recertification.
    # The General regression above separately proves a real stale Source;
    # this probe injects cancellation exactly after metadata has been saved.
    current = lake.inputs.is_current
    calls = {"count": 0, "cancel": False}
    def stale_once(*args, **kwargs):
        calls["count"] += 1
        return False if calls["count"] == 2 else current(*args, **kwargs)
    monkeypatch.setattr(lake.inputs, "is_current", stale_once)
    save = api._save_completion
    def cancel_after_save(*args, **kwargs):
        save(*args, **kwargs)
        calls["cancel"] = True
    monkeypatch.setattr(api, "_save_completion", cancel_after_save)
    report = lake.items.update("twice", start="2020-01-02", end="2020-01-03", cancelled=lambda: calls["cancel"])
    assert report.status == "cancelled"
    assert lake.items.completed_builds("twice") == [original]


def _duplicate_completed_outputs(lake, source, *, change=None):
    original = lake.items.read("twice", view="versions").select("time", "asset_id", "value",
        "observation_date", "available_date", "version_available_date").collect()
    append(lake, source)
    receipt = lake.inputs.freeze({"raw": RawInput("custom", "daily", alias="raw", view="versions")},
        information_cutoff="2020-01-04")
    frame = original if change is None else change(original)
    lake.items.ingest("twice", frame, historical_baseline=True, input_receipt=receipt)
    return original


def test_explicit_output_reuse_keeps_duplicate_history_without_recomputing_it(tmp_path, monkeypatch):
    from bagelquant_data.storage import recovery
    lake, source, _, calls = setup(tmp_path)
    original = _duplicate_completed_outputs(lake, source)
    builds = lake.items.builds("twice")
    assert lake.items.plan_update("twice", start="2020-01-02", end="2020-01-04") == [{"mode": "incremental", "start": "2020-01-04", "end": "2020-01-04"}]
    report = lake.items.index_output_reuse("twice", end="2020-01-04")
    # Identical business/timing output deduplicates before persistence, so
    # explicit output maintenance has no repeated body to compare.
    assert report["completed"] == report["unchanged_batches"] == 0 and report["affected_dates"] == []
    assert not calls and lake.items.builds("twice") == builds
    assert_frame_equal(lake.items.read("twice", view="snapshot", as_of="2020-01-04").select(original.columns).collect(), original)
    def forbidden(*args, **kwargs):
        pytest.fail("normal planning read original output bytes or executed maintenance")
    monkeypatch.setattr(recovery, "project_batch", forbidden)
    monkeypatch.setattr(lake.items, "index_output_reuse", forbidden)
    assert lake.items.plan_update("twice", start="2020-01-02", end="2020-01-04") == [
        {"mode": "incremental", "start": "2020-01-04", "end": "2020-01-04"}]


@pytest.mark.parametrize("channel", ["value", "observation_date", "available_date", "version_available_date"])
def test_output_maintenance_never_hides_a_changed_numerical_or_availability_channel(tmp_path, channel):
    lake, source, _, calls = setup(tmp_path)
    def change(frame):
        if channel == "value":
            return frame.with_columns(pl.when(pl.col("time") == date(2020, 1, 3)).then(60.).otherwise(pl.col(channel)).alias(channel))
        if channel == "version_available_date":
            return frame.with_columns(pl.when(pl.col("time") == date(2020, 1, 3)).then(pl.lit(date(2020, 1, 4))).otherwise(pl.col(channel)).alias(channel))
        return frame.with_columns(pl.when(pl.col("time") == date(2020, 1, 3)).then(pl.lit(date(2020, 1, 4))).otherwise(pl.col(channel)).alias(channel),
            pl.when(pl.col("time") == date(2020, 1, 3)).then(pl.lit(date(2020, 1, 4))).otherwise(pl.col("version_available_date")).alias("version_available_date"))
    _duplicate_completed_outputs(lake, source, change=change)
    report = lake.items.index_output_reuse("twice", end="2020-01-04")
    assert report["affected_dates"] == ["2020-01-03"] and not calls
    assert lake.items.plan_update("twice", start="2020-01-02", end="2020-01-04") == [
        {"mode": "incremental", "start": "2020-01-03", "end": "2020-01-04"}]


def test_output_reuse_does_not_override_independent_input_revision(tmp_path):
    lake, source, _, calls = setup(tmp_path)
    _duplicate_completed_outputs(lake, source)
    lake.items.index_output_reuse("twice", end="2020-01-04")
    append(lake, source, day=3, value=30.)
    assert lake.items.plan_update("twice", start="2020-01-02", end="2020-01-04") == [
        {"mode": "incremental", "start": "2020-01-03", "end": "2020-01-04"}]
    lake.items.update("twice", start="2020-01-02", end="2020-01-04")
    assert calls and all(first >= date(2020, 1, 3) for first, _, _, _ in calls)
    assert values(lake, "twice")["value"].to_list() == [4., 60., 8.]


def test_output_reuse_index_damage_is_not_treated_as_unchanged_history(tmp_path):
    lake, source, _, calls = setup(tmp_path)
    _duplicate_completed_outputs(lake, source, change=lambda frame: frame.with_columns(pl.col("value") * 10))
    lake.items.index_output_reuse("twice", end="2020-01-04")
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update item_output_impact_index set payload_json='{}'")
    with pytest.raises(RuntimeError, match="Output impact index is damaged"):
        lake.items.plan_update("twice", start="2020-01-02", end="2020-01-04")
    assert not calls


def test_normal_impact_queries_do_not_fetch_original_batch_blobs(tmp_path, monkeypatch):
    from contextlib import contextmanager
    lake, source, _, calls = setup(tmp_path)
    _duplicate_completed_outputs(lake, source)
    original_connect = lake.items._store.connect
    @contextmanager
    def metadata_only():
        with original_connect() as db:
            def authorize(action, table, column, *_):
                if action == sqlite3.SQLITE_READ and table == "version_segments" and column == "payload":
                    pytest.fail("impact planning selected an original IPC payload column")
                return sqlite3.SQLITE_OK
            db.set_authorizer(authorize)
            try:
                yield db
            finally:
                db.set_authorizer(None)
    monkeypatch.setattr(lake.items._store, "connect", metadata_only)
    assert lake.items.plan_update("twice", start="2020-01-02", end="2020-01-04") == [{"mode": "incremental", "start": "2020-01-04", "end": "2020-01-04"}]
    assert not calls


def test_output_maintenance_fences_stolen_writer_ownership(tmp_path, monkeypatch):
    from bagelquant_data.items import output_reuse
    lake, source, _, calls = setup(tmp_path)
    _duplicate_completed_outputs(lake, source)
    project = output_reuse.project_batch
    def stolen(*args, **kwargs):
        project(*args, **kwargs)
        with sqlite3.connect(lake.data_meta_path) as db:
            db.execute("update update_leases set run_id='foreign',owner_id='foreign' where source='items' and dataset='twice'")
    monkeypatch.setattr(output_reuse, "project_batch", stolen)
    with pytest.raises(RuntimeError, match="lost its dataset writer lease"):
        lake.items.index_output_reuse("twice", end="2020-01-04")
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("select 1 from sqlite_master where name='item_output_impact_index'").fetchone() is None
        assert db.execute("select run_id from update_leases where dataset='twice'").fetchone()[0] == "foreign"
    assert not calls


def test_output_reuse_at_later_cutoff_does_not_repair_not_yet_visible_versions(tmp_path):
    lake, source, _, calls = setup(tmp_path)
    def change(frame):
        return frame.with_columns(pl.col("value") * 10,
            pl.lit(date(2020, 1, 5)).alias("version_available_date"))
    _duplicate_completed_outputs(lake, source, change=change)
    assert lake.items.plan_update("twice", start="2020-01-02", end="2020-01-04") == [
        {"mode": "incremental", "start": "2020-01-04", "end": "2020-01-04"}]
    report = lake.items.index_output_reuse("twice", end="2020-01-05")
    assert report["affected_dates"] == ["2020-01-02", "2020-01-03"]
    assert lake.items.plan_update("twice", start="2020-01-02", end="2020-01-04") == [
        {"mode": "incremental", "start": "2020-01-04", "end": "2020-01-04"}]
    assert values(lake, "twice")["value"].to_list() == [4., 6.] and not calls


def test_fully_visible_output_reuse_survives_a_new_cutoff_without_old_work(tmp_path, monkeypatch):
    from bagelquant_data.items import output_reuse
    lake, source, _, calls = setup(tmp_path)
    _duplicate_completed_outputs(lake, source)
    lake.items.index_output_reuse("twice", end="2020-01-04")
    def forbidden(*args, **kwargs):
        pytest.fail("normal later-cutoff planning audited historical output bytes")
    monkeypatch.setattr(output_reuse, "project_batch", forbidden)
    assert lake.items.plan_update("twice", start="2020-01-02", end="2020-01-05") == [
        {"mode": "incremental", "start": "2020-01-04", "end": "2020-01-05"}]
    lake.items.update("twice", start="2020-01-02", end="2020-01-05")
    assert calls and all(first >= date(2020, 1, 4) for first, _, _, _ in calls)
    assert values(lake, "twice")["value"].to_list() == [4., 6., 8.]


def test_output_reuse_does_not_extrapolate_over_unobserved_future_versions(tmp_path):
    lake, source, _, calls = setup(tmp_path)
    _duplicate_completed_outputs(lake, source, change=lambda frame: frame.with_columns(
        pl.col("value") * 10, pl.lit(date(2020, 1, 5)).alias("version_available_date")))
    lake.items.index_output_reuse("twice", end="2020-01-04")
    assert lake.items.plan_update("twice", start="2020-01-02", end="2020-01-05") == [
        {"mode": "blocked", "start": "2020-01-02", "end": "2020-01-05",
         "reason": "proven input impact covers the completed history; explicit resolution is required"}]
    assert not calls


def test_output_maintenance_rejects_forged_physical_availability_bounds(tmp_path):
    lake, source, _, calls = setup(tmp_path)
    _duplicate_completed_outputs(lake, source, change=lambda frame: frame.with_columns(
        pl.col("value") * 10, pl.lit(date(2020, 1, 5)).alias("version_available_date")))
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update version_segments set min_available='2020-01-04',max_available='2020-01-04' "
            "where commit_seq=(select max(seq) from version_commits where source='items' and dataset='twice')")
    with pytest.raises(ValueError, match="batch bounds differ"):
        lake.items.index_output_reuse("twice", end="2020-01-04")
    assert not calls


def test_output_maintenance_audits_original_bounds_before_reference_pruning(tmp_path):
    lake, source, _, calls = setup(tmp_path)
    old = lake.items.read("twice", view="versions").filter(pl.col("time") == date(2020, 1, 3)).select(
        "time", "asset_id", "value", "observation_date", "available_date", "version_available_date").collect()
    lake.raw.ingest(source, pl.DataFrame({"time": [date(2020, 1, 3)], "asset_id": ["A"], "value": [30.]}),
        ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    lake.items.update("twice", start="2020-01-02", end="2020-01-03")
    calls.clear()
    with sqlite3.connect(lake.data_meta_path) as db:
        original_seq = db.execute("select max(seq) from version_commits where source='items'").fetchone()[0]
    receipt = lake.inputs.freeze({"raw": RawInput("custom", "daily", view="versions")},
        information_cutoff="2020-01-04")
    lake.items.ingest("twice", old, historical_baseline=True, input_receipt=receipt)
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update version_segments set min_observation='2020-01-02',max_observation='2020-01-02' "
            "where commit_seq=?", (original_seq,))
    with pytest.raises(ValueError, match="batch bounds differ"):
        lake.items.index_output_reuse("twice", end="2020-01-04")
    assert not calls


def test_output_reuse_fences_original_headers_after_admission(tmp_path):
    lake, source, _, calls = setup(tmp_path)
    _duplicate_completed_outputs(lake, source, change=lambda frame: frame.with_columns(pl.col("value") * 10))
    lake.items.index_output_reuse("twice", end="2020-01-04")
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update version_segments set max_observation='2020-01-02' "
            "where commit_seq=(select min(seq) from version_commits where source='items')")
    with pytest.raises(RuntimeError, match="original batch authority differs"):
        lake.items.plan_update("twice", start="2020-01-02", end="2020-01-04")
    assert not calls


def test_existing_item_api_cannot_maintain_outputs_inside_a_new_frozen_boundary(tmp_path):
    from bagelquant_data.inputs import input_read_boundary
    lake, source, _, calls = setup(tmp_path)
    _duplicate_completed_outputs(lake, source)
    items = lake.items
    with input_read_boundary(lake.data_meta_path, lake.inputs.max_commit(), as_of="2020-01-04"):
        with pytest.raises(RuntimeError, match="unfrozen writer"):
            items.index_output_reuse("twice", end="2020-01-04")
    assert not calls


def test_writer_lease_evidence_exposes_only_the_same_original_publisher(tmp_path):
    lake, source, _, _ = setup(tmp_path)
    _duplicate_completed_outputs(lake, source)
    with sqlite3.connect(lake.data_meta_path) as db:
        publication = db.execute("select seq,run_id from version_commits where source='items' "
            "and dataset='twice' order by seq desc limit 1").fetchone()
    lake.items._store.acquire_update_leases([("items", "twice", publication[1])])
    lease = lake.integrity.active_update_leases()[0]
    assert lease["latest_publication"]["seq"] == publication[0]
    lake.items._store.release_update_leases([publication[1]])
    lake.items._store.acquire_update_leases([("items", "twice", "another-writer")])
    assert lake.integrity.active_update_leases()[0]["latest_publication"] is None


def test_completed_certificate_and_exact_unit_cannot_hide_later_stale_output(tmp_path):
    from bagelquant_data import ItemInput
    lake, source, _, calls = setup(tmp_path)
    old = lake.items.builds("twice")[-1]["frozen_receipt_id"]
    append(lake, source)
    lake.items.update("twice", start="2020-01-02", end="2020-01-04")
    lake.raw.ingest(source, pl.DataFrame({"time": [date(2020, 1, 3)], "asset_id": ["A"], "value": [30.]}),
        ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    lake.items.update("twice", start="2020-01-02", end="2020-01-04")
    calls.clear()
    lake.items.ingest("twice", pl.DataFrame({"time": [date(2020, 1, 3)], "asset_id": ["A"], "value": [6.]}),
        available_date="2020-01-04", input_receipt=old)
    request = {"item": ItemInput("twice", start="2020-01-02", end="2020-01-04", view="snapshot")}
    stale = lake.inputs.freeze(request, information_cutoff="2020-01-04")
    assert not lake.inputs.is_current(stale)
    assert lake.items.plan_update("twice", start="2020-01-02", end="2020-01-04") == [
        {"mode": "incremental", "start": "2020-01-03", "end": "2020-01-03"}]
    lake.items.update("twice", start="2020-01-02", end="2020-01-04")
    assert calls and all(first == last == date(2020, 1, 3) for first, last, _, _ in calls)
    assert values(lake, "twice")["value"].to_list() == [4., 60., 8.]
    assert lake.inputs.is_current(lake.inputs.freeze(request, information_cutoff="2020-01-04"))
    whole = lake.inputs.freeze({"item": ItemInput("twice", view="snapshot")},
        information_cutoff="2020-01-04", retain_batch_bounds=True)
    assert lake.inputs.is_current(whole)
    assert lake.items.plan_update("twice", start="2020-01-02", end="2020-01-04") == []


def test_row_only_publication_never_replaces_complete_membership_proof(tmp_path):
    lake, source, _, _ = setup(tmp_path)
    lake.raw.ingest(source, pl.DataFrame({"time": [date(2020, 1, 3)], "asset_id": ["A"], "value": [30.]}),
        ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    receipt = lake.inputs.freeze({"raw": RawInput("custom", "daily", alias="raw", view="versions")},
        information_cutoff="2020-01-03")
    lake.items.ingest("twice", pl.DataFrame({"time": [date(2020, 1, day) for day in (2, 3)],
        "asset_id": ["B", "B"], "value": [10., 20.]}), available_date="2020-01-03", input_receipt=receipt)
    plan = lake.items.plan_update("twice", start="2020-01-02", end="2020-01-04")
    assert plan[0]["mode"] == "blocked"
    assert "covers the completed history" in plan[0]["reason"]
    with pytest.raises(RuntimeError, match="covers the completed history"):
        lake.items.update("twice", start="2020-01-02", end="2020-01-04")


def test_delayed_output_visibility_updates_only_maturing_coordinates(tmp_path):
    from bagelquant_data import ItemInput
    lake, _, _, _ = setup(tmp_path)
    calls = []
    def delayed(context):
        calls.append((context.start, context.end))
        return context.frames["raw"].select(pl.col("source_time").alias("time"), "asset_id",
            (pl.col("value") * 2).alias("value"),
            (pl.col("source_time") + pl.duration(days=2)).alias("available_date"))
    lake.items.register_producer("delayed", "1", delayed, update_policy=ProducerUpdatePolicy("same_date"))
    lake.items.register(DataItemSpec("delayed", (RawInput("custom", "daily", alias="raw"),),
        producer_key="delayed", producer_revision="1"))
    lake.items.initialize("delayed", start="2020-01-02", end="2020-01-04")
    assert values(lake, "delayed")["value"].to_list() == [4.]
    request = {"item": ItemInput("delayed", start="2020-01-02", end="2020-01-05", view="snapshot")}
    pending = lake.inputs.freeze(request, information_cutoff="2020-01-05")
    assert not lake.inputs.is_current(pending)
    import hashlib
    import json
    build_id = lake.items.builds("delayed")[-1]["id"]
    with sqlite3.connect(lake.data_meta_path) as db:
        original_pending = db.execute("select payload_json,digest from item_pending_outputs where build_id=?", (build_id,)).fetchone()
        body = json.loads(original_pending[0])
        body["coordinates"] = []
        forged = json.dumps(body, sort_keys=True, separators=(",", ":"))
        db.execute("update item_pending_outputs set payload_json=?,digest=? where build_id=?",
            (forged, hashlib.sha256(forged.encode()).hexdigest(), build_id))
    with pytest.raises(RuntimeError, match="pending visibility proof source binding"):
        lake.items.plan_update("delayed", start="2020-01-02", end="2020-01-05")
    with pytest.raises(RuntimeError, match="pending visibility proof source binding"):
        lake.inputs.is_current(pending)
    with sqlite3.connect(lake.data_meta_path) as db:
        db.execute("update item_pending_outputs set payload_json=?,digest=? where build_id=?",
            (*original_pending, build_id))
    calls.clear()
    assert lake.items.plan_update("delayed", start="2020-01-02", end="2020-01-05") == [
        {"mode": "incremental", "start": "2020-01-03", "end": "2020-01-03"},
        {"mode": "incremental", "start": "2020-01-05", "end": "2020-01-05"}]
    lake.items.update("delayed", start="2020-01-02", end="2020-01-05")
    assert calls and all(first > date(2020, 1, 2) for first, _ in calls)
    actual = lake.items.read("delayed", view="snapshot", as_of="2020-01-05").collect()
    assert actual.sort("time")["value"].to_list() == [4., 6.]
    assert lake.inputs.is_current(lake.inputs.freeze(request, information_cutoff="2020-01-05"))
    assert lake.items.plan_update("delayed", start="2020-01-02", end="2020-01-05") == []


def test_late_correction_preserves_signed_zero_contract(tmp_path):
    import math
    lake, source, _, calls = setup(tmp_path)
    def zero(context):
        calls.append((context.start, context.end))
        return context.frames["raw"].select(pl.col("source_time").alias("time"),
            "asset_id", (pl.col("value") * 0).alias("value"))
    lake.items.register_producer("zero", "1", zero, update_policy=ProducerUpdatePolicy("same_date"))
    lake.items.register(DataItemSpec("zero", (RawInput("custom", "daily", alias="raw"),),
        producer_key="zero", producer_revision="1"))
    lake.raw.ingest(source, pl.DataFrame({"time": [date(2020, 1, 3)], "asset_id": ["A"], "value": [-1.]}),
        ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    first = lake.items.update("zero", start="2020-01-02", end="2020-01-03")
    assert first.frozen_receipt_id is not None
    lake.raw.ingest(source, pl.DataFrame({"time": [date(2020, 1, 3)], "asset_id": ["A"], "value": [1.]}),
        ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    lake.items.ingest("zero", pl.DataFrame({"time": [date(2020, 1, 3)], "asset_id": ["A"], "value": [-0.]}),
        available_date="2020-01-04", input_receipt=first.frozen_receipt_id)
    lake.items.update("zero", start="2020-01-02", end="2020-01-04")
    actual = lake.items.read("zero", view="snapshot", as_of="2020-01-04", start="2020-01-03", end="2020-01-03").collect()
    assert math.copysign(1., actual["value"][0]) == 1.


@pytest.mark.parametrize("later_has_rows", [False, True])
def test_new_empty_range_cannot_hide_stale_earlier_empty_completion(tmp_path, later_has_rows):
    from bagelquant_data import ItemInput
    lake, source, _, _ = setup(tmp_path, empty=True)
    lake.raw.ingest(source, pl.DataFrame({"time": [date(2020, 1, 3)], "asset_id": ["A"], "value": [30.]}),
        ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    if later_has_rows:
        lake.raw.ingest(source, pl.DataFrame({"time": [date(2020, 1, 10)], "asset_id": ["A"], "value": [10.]}),
            ingested_at=datetime(2020, 1, 10, tzinfo=UTC))
    lake.items.update("twice", start="2020-01-10", end="2020-01-10")
    whole = lake.inputs.freeze({"item": ItemInput("twice", view="snapshot")},
        information_cutoff="2020-01-10", retain_batch_bounds=True)
    assert not lake.inputs.is_current(whole)
    lake.items.update("twice", start="2020-01-02", end="2020-01-10")
    repaired = lake.inputs.freeze({"item": ItemInput("twice", view="snapshot")},
        information_cutoff="2020-01-10", retain_batch_bounds=True)
    assert lake.inputs.is_current(repaired)


@pytest.mark.parametrize("noop", [False, True])
def test_output_write_after_plan_cannot_be_swallowed_by_completion_marker(tmp_path, monkeypatch, noop):
    lake, source, _, _ = setup(tmp_path)
    old = lake.items.builds("twice")[-1]["frozen_receipt_id"]
    append(lake, source)
    lake.items.update("twice", start="2020-01-02", end="2020-01-04")
    if not noop:
        append(lake, source, day=5, value=5.)
    original = lake.items._plan_update
    count = 0
    def interleaved(*args, **kwargs):
        nonlocal count
        actions = original(*args, **kwargs)
        count += 1
        if count == (1 if noop else 2):
            lake.items.ingest("twice", pl.DataFrame({"time": [date(2020, 1, 3)], "asset_id": ["A"], "value": [66.]}),
                available_date="2020-01-04", input_receipt=old)
        return actions
    monkeypatch.setattr(lake.items, "_plan_update", interleaved)
    with pytest.raises(RuntimeError, match="output changed during"):
        lake.items.update("twice", start="2020-01-02", end="2020-01-04" if noop else "2020-01-05")
    if not noop:
        assert not any(row["start_date"] == "2020-01-02" and row["end_date"] == "2020-01-05"
            for row in lake.items.builds("twice"))


def test_unknown_causal_contract_blocks_without_running_producer(tmp_path):
    lake, source, _, calls = setup(tmp_path, policy=False)
    append(lake, source)
    action = lake.items.plan_update("twice", start="2020-01-02", end="2020-01-04")[0]
    assert action["mode"] == "blocked" and "causal" in action["reason"]
    with pytest.raises(RuntimeError, match="plan blocked"):
        lake.items.update("twice", end="2020-01-04")
    assert not calls


def test_definition_change_rebuilds_and_empty_completion_appends(tmp_path):
    lake, source, producer, calls = setup(tmp_path, empty=True)
    append(lake, source)
    lake.items.update("twice", start="2020-01-02", end="2020-01-04")
    assert calls and all(first == date(2020, 1, 4) for first, _, _, _ in calls)
    lake.items.register_producer("twice", "2", producer, update_policy=ProducerUpdatePolicy("same_date"))
    lake.items.register(DataItemSpec("twice", (RawInput("custom", "daily", alias="raw"),),
        producer_key="twice", producer_revision="2"))
    calls.clear()
    lake.items.update("twice", start="2020-01-02", end="2020-01-04")
    assert calls and calls[0][0] == date(2020, 1, 2)


def test_append_preserves_full_reference_timing_and_strict_visibility(tmp_path):
    from bagelquant_data import ItemInput
    lake, source, producer, _ = setup(tmp_path)
    retained = lake.inputs.freeze({"item": ItemInput("twice", view="versions")},
        information_cutoff="2020-01-03")
    historical = lake.inputs.read(retained, "item").collect()
    original_bytes = {path: path.read_bytes() for path in lake.lake_path.rglob("*.parquet")}
    append(lake, source)
    lake.items.update("twice", start="2020-01-02", end="2020-01-04")
    reference_inputs = []
    def reference(context):
        reference_inputs.append((context.historical_baseline,
            context.frames["raw"]["source_time"].to_list(), context.frames["raw"]["_baseline"].to_list()))
        return producer(context)
    lake.items.register_producer("reference", "1", reference)
    lake.items.register(DataItemSpec("reference", (RawInput("custom", "daily", alias="raw"),),
        producer_key="reference", producer_revision="1"))
    lake.items.initialize("reference", start="2020-01-02", end="2020-01-04")
    # The one-shot numerical reference consumes the whole mixed baseline
    # panel. Its global timing OR differs from the admitted one-day tail.
    assert reference_inputs == [(True, [date(2020, 1, day) for day in (2, 3, 4)], [True, True, False])]
    tail_receipt = lake.inputs.freeze({"raw": RawInput("custom", "daily", view="snapshot",
        start="2020-01-04", end="2020-01-04")}, information_cutoff="2020-01-04")
    tail = lake.inputs.read(tail_receipt, "raw").collect()
    assert tail["_baseline"].to_list() == [False]
    columns = ["time", "asset_id", "value", "observation_date", "available_date", "version_available_date", "_baseline"]
    tail_oracle = tail.select(pl.col("source_time").alias("time"), "asset_id", (pl.col("value") * 2).alias("value"),
        pl.col("source_time").alias("observation_date"), pl.col("time").alias("available_date"),
        pl.col("time").alias("version_available_date"), "_baseline")
    scoped_oracle = pl.concat([historical.select(columns), tail_oracle])
    full_reference = lake.items.read("reference", view="snapshot", as_of="2020-01-04").collect()
    numerical_fields = [field for field in columns if field != "_baseline"]
    assert_frame_equal(scoped_oracle.select(numerical_fields), full_reference.select(numerical_fields))
    for strict in (False, True):
        actual = lake.items.read("twice", view="snapshot", as_of="2020-01-04", strict=strict).collect()
        expected = scoped_oracle.filter(~pl.col("_baseline")) if strict else scoped_oracle
        assert_frame_equal(actual.select(columns).sort("time"), expected.select(columns).sort("time"))
    assert_frame_equal(lake.inputs.read(retained, "item").collect(), historical)
    assert all(path.read_bytes() == content for path, content in original_bytes.items())


def test_explicit_start_cannot_bypass_unknown_impact(tmp_path):
    lake, source, _, calls = setup(tmp_path, policy=False)
    append(lake, source)
    with pytest.raises(RuntimeError, match="causal"):
        lake.items.update("twice", start="2020-01-02", end="2020-01-04")
    assert not calls


def test_producer_and_definition_are_pinned_across_aba_change(tmp_path, monkeypatch):
    lake, source, producer, _ = setup(tmp_path)
    append(lake, source)
    original = lake.items._build
    def replaced(name, **kwargs):
        lake.items.register_producer("twice", "1", lambda context: producer(context).with_columns(
            (pl.col("value") * 100).alias("value")), update_policy=ProducerUpdatePolicy("same_date"))
        try:
            return original(name, **kwargs)
        finally:
            lake.items.register_producer("twice", "1", producer, update_policy=ProducerUpdatePolicy("same_date"))
    monkeypatch.setattr(lake.items, "_build", replaced)
    lake.items.update("twice", start="2020-01-02", end="2020-01-04")
    assert values(lake, "twice")["value"].to_list() == [4., 6., 8.]


def test_cancel_after_last_unit_does_not_certify_full_completion(tmp_path, monkeypatch):
    lake, source, _, _ = setup(tmp_path)
    append(lake, source)
    original = lake.items._build
    canceled = False
    def complete_then_cancel(*args, **kwargs):
        nonlocal canceled
        report = original(*args, **kwargs)
        canceled = True
        return report
    monkeypatch.setattr(lake.items, "_build", complete_then_cancel)
    report = lake.items.update("twice", start="2020-01-02", end="2020-01-04", cancelled=lambda: canceled)
    assert report.status == "cancelled"
    assert not any(row["start_date"] == "2020-01-02" and row["end_date"] == "2020-01-04"
                   for row in lake.items.builds("twice"))


def test_fresh_full_receipt_captures_bounds_and_skips_old_ipc_on_append(tmp_path, monkeypatch):
    lake, source, _, calls = setup(tmp_path)
    lake.raw.ingest(source, pl.DataFrame({"time": [date(2020, 2, 1)], "asset_id": ["A"], "value": [4.]}),
        ingested_at=datetime(2020, 2, 1, tzinfo=UTC))
    from bagelquant_data import inputs
    original = inputs.read_batch
    decoded = []
    def tracked(*args, **kwargs):
        decoded.append(args[1])
        return original(*args, **kwargs)
    monkeypatch.setattr(inputs, "read_batch", tracked)
    report = lake.items.update("twice", start="2020-01-02", end="2020-02-01")
    assert report.frozen_receipt_id is not None
    receipt = lake.inputs.get(report.frozen_receipt_id)
    assert receipt.requests["raw"].start is None
    assert receipt.evidence["raw"]["retained_batch_bounds"] is True
    assert lake.inputs.window_read_supported(receipt, "raw")
    legacy = lake.inputs.freeze(receipt.requests, information_cutoff=receipt.information_cutoff,
        max_commit=receipt.max_commit)
    assert receipt.dependency_digest == legacy.dependency_digest
    assert lake.inputs.request_identity(receipt, "raw") == lake.inputs.request_identity(legacy, "raw")
    assert calls and all(first > date(2020, 1, 3) for first, _, _, _ in calls)
    assert decoded and all("month=02" in path for path in decoded)
    assert lake.inputs.is_current(receipt)
    assert lake.inputs.verify(receipt)["valid"]


def test_finite_impact_counts_complete_calendar_and_audits_coordinate_proof(tmp_path, monkeypatch):
    from bagelquant_data import input_index
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    calendar = DatasetSpec("calendar", "general")
    days = [date(2020, 1, d) for d in (1, 2, 3, 6, 7, 8, 9, 10)]
    lake.raw.ingest(calendar, pl.DataFrame({"time": days, "is_open": [1] * len(days)}),
        mode="initialize", ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    source = DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(source, pl.DataFrame({"time": days, "asset_id": ["A"] * len(days),
        "value": [float(i) for i in range(len(days))]}), mode="initialize",
        ingested_at=datetime(2020, 1, 8, tzinfo=UTC))
    calls = []
    def rolling(context):
        calls.append((context.start, context.end, context.frames["raw"].height))
        return context.frames["raw"].sort("source_time").select(pl.col("source_time").alias("time"),
            "asset_id", pl.col("value").rolling_mean(3, min_samples=1).alias("value"))
    policy = ProducerUpdatePolicy("forward", history_observations=2,
        calendar_input="calendar", calendar_selector=("is_open", 1))
    lake.items.register_producer("rolling", "1", rolling, update_policy=policy)
    requests = (RawInput("custom", "daily", alias="raw"), RawInput("custom", "calendar", alias="calendar"))
    lake.items.register(DataItemSpec("rolling", requests, producer_key="rolling", producer_revision="1"))
    first = lake.items.initialize("rolling", start=days[0], end=days[5])
    calls.clear()
    lake.raw.ingest(source, pl.DataFrame({"time": [days[2]], "asset_id": ["A"], "value": [30.]}),
        ingested_at=datetime(2020, 1, 9, tzinfo=UTC))
    expected = [{"mode": "incremental", "start": "2020-01-03", "end": "2020-01-07"},
        {"mode": "incremental", "start": "2020-01-09", "end": "2020-01-09"}]
    assert lake.items.plan_update("rolling", start=days[0], end=days[6]) == expected
    original_plan = lake.items._plan_update
    def change_live_policy(name, **kwargs):
        with pytest.raises(ValueError, match='immutable producer revision'):
            lake.items.register_producer("rolling", "1", rolling, update_policy=ProducerUpdatePolicy(
                "forward", history_observations=0, calendar_input="calendar", calendar_selector=("is_open", 1)))
        return original_plan(name, **kwargs)
    monkeypatch.setattr(lake.items, "_plan_update", change_live_policy)
    lake.items.update("rolling", start=days[0], end=days[6])
    monkeypatch.setattr(lake.items, "_plan_update", original_plan)
    lake.items.register_producer("rolling", "1", rolling, update_policy=policy)
    assert calls and all(not (start < days[2]) for start, _, _ in calls)
    assert max(rows for _, _, rows in calls) <= 5
    lake.items.register_producer("reference", "1", rolling)
    lake.items.register(DataItemSpec("reference", requests, producer_key="reference", producer_revision="1"))
    lake.items.initialize("reference", start=days[0], end=days[6])
    for name in ("rolling", "reference"):
        assert lake.items.read(name, view="snapshot", as_of=days[6]).collect().filter(
            pl.col("time").is_in(days[3:5]))["value"].to_list() == pytest.approx([34 / 3, 37 / 3])
    assert first.frozen_receipt_id is not None
    frozen = lake.inputs.get(first.frozen_receipt_id)
    assert lake.inputs.verify(frozen)["valid"]
    with sqlite3.connect(lake.data_meta_path) as db:
        input_index.save_coordinates(db, frozen.digest, "calendar", "time", ("is_open", 1),
            pl.DataFrame({"time": [date(2020, 1, d) for d in range(1, 11)], "is_open": [1] * 10}))
    with pytest.raises(RuntimeError, match="Coordinate index differs"):
        lake.inputs.verify(frozen)


@pytest.mark.parametrize("raw_baseline", [False, True])
def test_same_content_calendar_attestation_requires_preserved_output_timing(tmp_path, raw_baseline):
    from bagelquant_data import ItemInput
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    source = DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(source, pl.DataFrame({"time": [date(2020, 1, day) for day in (2, 3)],
        "asset_id": ["A", "A"], "value": [2., 3.]}),
        mode="initialize" if raw_baseline else "incremental", ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    calendar_spec = DatasetSpec("calendar", "general")
    calendar = pl.DataFrame({"time": [date(2020, 1, d) for d in (2, 3, 4)], "is_open": [1, 1, 1]})
    lake.raw.ingest(calendar_spec, calendar, mode="initialize", ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    calls = []
    def producer(context):
        calls.append((context.start, context.end))
        return context.frames["raw"].select(pl.col("source_time").alias("time"), "asset_id", "value")
    requests = (RawInput("custom", "daily", alias="raw"), RawInput("custom", "calendar", alias="calendar"))
    lake.items.register_producer("p", "1", producer, update_policy=ProducerUpdatePolicy(
        "same_date", timing_neutral_inputs=("calendar",)))
    lake.items.register(DataItemSpec("item", requests, producer_key="p", producer_revision="1"))
    old = lake.items.initialize("item", start="2020-01-02", end="2020-01-03")
    retained = lake.inputs.freeze({"item": ItemInput("item", view="versions")}, information_cutoff="2020-01-03")
    historical = lake.inputs.read(retained, "item").collect()
    original_bytes = {path: path.read_bytes() for path in lake.lake_path.rglob("*.parquet")}
    calls.clear()
    lake.raw.ingest(calendar_spec, calendar, ingested_at=datetime(2020, 1, 4, tzinfo=UTC))
    append(lake, source)
    if not raw_baseline:
        with pytest.raises(RuntimeError, match="timing attestations"):
            lake.items.update("item", start="2020-01-02", end="2020-01-04")
        assert not calls
        return
    assert lake.items.plan_update("item", start="2020-01-02", end="2020-01-04") == [
        {"mode": "incremental", "start": "2020-01-04", "end": "2020-01-04"}]
    lake.items.update("item", start="2020-01-02", end="2020-01-04")
    assert calls and all(first == last == date(2020, 1, 4) for first, last in calls)
    assert old.frozen_receipt_id is not None
    assert lake.inputs.is_current(old.frozen_receipt_id)
    reference_inputs = []
    def reference(context):
        reference_inputs.append((context.historical_baseline, context.frames["raw"]["_baseline"].to_list(),
            context.frames["calendar"]["_baseline"].to_list()))
        return producer(context)
    lake.items.register_producer("reference", "1", reference)
    lake.items.register(DataItemSpec("reference", requests, producer_key="reference", producer_revision="1"))
    lake.items.initialize("reference", start="2020-01-02", end="2020-01-04")
    assert reference_inputs == [(True, [True, True], [True, True, True]),
                                (True, [True, True, False], [False, False, False])]
    tail_receipt = lake.inputs.freeze({"raw": RawInput("custom", "daily", view="snapshot",
        start="2020-01-04", end="2020-01-04")}, information_cutoff="2020-01-04")
    tail = lake.inputs.read(tail_receipt, "raw").collect()
    assert tail["_baseline"].to_list() == [False]
    fields = ["time", "asset_id", "value", "observation_date", "available_date", "version_available_date", "_baseline"]
    tail_oracle = tail.select(pl.col("source_time").alias("time"), "asset_id", "value",
        pl.col("source_time").alias("observation_date"), pl.col("time").alias("available_date"),
        pl.col("time").alias("version_available_date"), "_baseline")
    scoped_oracle = pl.concat([historical.select(fields), tail_oracle])
    full_reference = lake.items.read("reference", view="snapshot", as_of="2020-01-04").collect()
    numerical_fields = [field for field in fields if field != "_baseline"]
    assert_frame_equal(scoped_oracle.select(numerical_fields), full_reference.select(numerical_fields))
    for strict in (False, True):
        actual = lake.items.read("item", view="snapshot", as_of="2020-01-04", strict=strict).collect()
        expected = scoped_oracle.filter(~pl.col("_baseline")) if strict else scoped_oracle
        assert_frame_equal(actual.select(fields).sort("time"), expected.select(fields).sort("time"))
    assert_frame_equal(lake.inputs.read(retained, "item").collect(), historical)
    assert all(path.read_bytes() == content for path, content in original_bytes.items())


@pytest.mark.parametrize("whole_receipt_current", [True, False])
@pytest.mark.parametrize("repeat_day", [2, 3, 4])
def test_repeated_verification_of_known_neutral_calendar_does_not_recalculate_history(tmp_path, monkeypatch, whole_receipt_current, repeat_day):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    source = DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    raw = pl.DataFrame({"time": [date(2020, 1, d) for d in (2, 3)], "asset_id": ["A", "A"], "value": [2., 3.]})
    lake.raw.ingest(source, raw, ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    spec = DatasetSpec("calendar", "general")
    calendar = pl.DataFrame({"time": [date(2020, 1, d) for d in (2, 3, 4)], "is_open": [1, 1, 1]})
    lake.raw.ingest(spec, calendar, mode="initialize", ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    lake.raw.ingest(spec, calendar, ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    calls = []
    def producer(context):
        calls.append((context.start, context.end))
        return context.frames["raw"].select(pl.col("source_time").alias("time"), "asset_id", "value")
    requests = (RawInput("custom", "daily", alias="raw"), RawInput("custom", "calendar", alias="calendar"))
    lake.items.register_producer("p", "1", producer, update_policy=ProducerUpdatePolicy("same_date", timing_neutral_inputs=("calendar",)))
    lake.items.register(DataItemSpec("item", requests, producer_key="p", producer_revision="1"))
    old = lake.items.initialize("item", start="2020-01-02", end="2020-01-03")
    calls.clear()
    lake.raw.ingest(spec, calendar, ingested_at=datetime(2020, 1, repeat_day, tzinfo=UTC))
    append(lake, source)
    from bagelquant_data import inputs
    def forbidden(*args, **kwargs):
        pytest.fail("neutral calendar planning decoded originals")
    with monkeypatch.context() as patch:
        patch.setattr(inputs, "read_batch", forbidden)
        if not whole_receipt_current:
            patch.setattr(lake.inputs, "is_current", lambda *args, **kwargs: False)
        if repeat_day == 2:
            actions = lake.items.plan_update("item", start="2020-01-02", end="2020-01-04")
            assert actions[0]["mode"] == "blocked" and "timing attestations" in actions[0]["reason"]
            assert not calls
            return
        assert lake.items.plan_update("item", start="2020-01-02", end="2020-01-04") == [
            {"mode": "incremental", "start": "2020-01-04", "end": "2020-01-04"}]
    lake.items.update("item", start="2020-01-02", end="2020-01-04")
    assert calls and all(first == last == date(2020, 1, 4) for first, last in calls)
    assert old.frozen_receipt_id is not None
    if repeat_day > 3:
        assert lake.inputs.is_current(old.frozen_receipt_id)
    lake.items.register_producer("reference", "1", producer)
    lake.items.register(DataItemSpec("reference", requests, producer_key="reference", producer_revision="1"))
    lake.items.initialize("reference", start="2020-01-02", end="2020-01-04")
    for strict in (False, True):
        fields = ["time", "asset_id", "value", "available_date", "version_available_date", "_baseline"]
        actual = lake.items.read("item", view="snapshot", as_of="2020-01-04", strict=strict).collect()
        expected = lake.items.read("reference", view="snapshot", as_of="2020-01-04", strict=strict).collect()
        assert_frame_equal(actual.select(fields).sort("time"), expected.select(fields).sort("time"))
@pytest.mark.parametrize("event", ["repeat", "future_snapshot", "captured_first_attestation"])
def test_general_without_time_uses_selected_events_and_retains_captured_future_impact(tmp_path, monkeypatch, event):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    source = DatasetSpec("daily", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(source, pl.DataFrame({"time": [date(2020, 1, d) for d in (2, 3)],
        "asset_id": ["A", "A"], "value": [2., 3.]}), ingested_at=datetime(2020, 1, 3, tzinfo=UTC))
    meta_spec = DatasetSpec("metadata", "general")
    meta = pl.DataFrame({"asset_id": ["A"], "category": ["one"]})
    lake.raw.ingest(meta_spec, meta, mode="initialize", ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    if event != "captured_first_attestation":
        lake.raw.ingest(meta_spec, meta, ingested_at=datetime(2020, 1, 2, tzinfo=UTC))
    if event == "future_snapshot":
        lake.raw.ingest(meta_spec, meta.with_columns(pl.lit("two").alias("category")),
            ingested_at=datetime(2020, 1, 4, tzinfo=UTC))
    elif event == "captured_first_attestation":
        lake.raw.ingest(meta_spec, meta, ingested_at=datetime(2020, 1, 4, tzinfo=UTC))
    calls = []
    def producer(context):
        calls.append((context.start, context.end))
        return context.frames["raw"].select(pl.col("source_time").alias("time"), "asset_id", "value")
    requests = (RawInput("custom", "daily", alias="raw"), RawInput("custom", "metadata", alias="metadata"))
    lake.items.register_producer("p", "1", producer, update_policy=ProducerUpdatePolicy(
        "same_date", timing_neutral_inputs=("metadata",)))
    lake.items.register(DataItemSpec("item", requests, producer_key="p", producer_revision="1"))
    lake.items.initialize("item", start="2020-01-02", end="2020-01-03")
    calls.clear()
    if event == "repeat":
        lake.raw.ingest(meta_spec, meta, ingested_at=datetime(2020, 1, 4, tzinfo=UTC))
    append(lake, source)
    def forbidden(*args, **kwargs):
        pytest.fail("General planning read original values or built indexes")
    from bagelquant_data import inputs
    with monkeypatch.context() as patch:
        patch.setattr(inputs, "read_batch", forbidden)
        assert not hasattr(lake.inputs, "build_index")
        actions = lake.items.plan_update("item", start="2020-01-02", end="2020-01-04")
    if event != "repeat":
        assert actions[0]["mode"] == "blocked"
        assert ("snapshot" if event == "future_snapshot" else "timing attestations") in actions[0]["reason"]
        assert not calls
        return
    assert actions == [{"mode": "incremental", "start": "2020-01-04", "end": "2020-01-04"}]
    lake.items.update("item", start="2020-01-02", end="2020-01-04")
    assert calls and all(first == last == date(2020, 1, 4) for first, last in calls)
    lake.items.register_producer("reference", "1", producer)
    lake.items.register(DataItemSpec("reference", requests, producer_key="reference", producer_revision="1"))
    lake.items.initialize("reference", start="2020-01-02", end="2020-01-04")
    fields = ["time", "asset_id", "value", "available_date", "version_available_date", "_baseline"]
    for strict in (False, True):
        for view in ("snapshot", "history"):
            actual = lake.items.read("item", view=view, as_of="2020-01-04", strict=strict).collect()
            expected = lake.items.read("reference", view=view, as_of="2020-01-04", strict=strict).collect()
            assert_frame_equal(actual.select(fields).sort("time"), expected.select(fields).sort("time"))

