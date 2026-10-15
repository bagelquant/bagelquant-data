"""Exact currentness parent selection has bounded, disposable projected frames."""
from storage_fixtures import damage_original_segments

from concurrent.futures import CancelledError
from dataclasses import replace
from datetime import UTC, date, datetime
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, RawInput
from bagelquant_data.execution import ExecutionOptions
from bagelquant_data.storage import full_commit_checks
import bagelquant_data.storage.recovery as recovery


def prepared(root, *, baseline=False, revisions=True):
    lake = DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")
    frame = pl.DataFrame({"time": [date(2020, 1, 1)] * 24,
                          "asset_id": [f"asset-{i}" for i in range(24)],
                          "value": [float(i) for i in range(24)]})
    spec = DatasetSpec("raw", "by_date", date_kind="calendar",
                       field_mappings={"time": "time", "asset_id": "asset_id"})
    lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    raw = RawInput("custom", "raw", view="versions")
    lake.items.register(DataItemSpec("item", (raw,), producer_key="external", producer_revision="1"))
    old = lake.inputs.freeze({"raw": raw}, information_cutoff="2020-03-05")
    lake.items.ingest("item", frame, available_date="2020-01-02", input_receipt=old,
                      historical_baseline=baseline)
    lake.raw.ingest(spec, frame.with_columns(pl.col("value") + 1),
                    ingested_at=datetime(2020, 2, 1, tzinfo=UTC))
    current = lake.inputs.freeze({"raw": raw}, information_cutoff="2020-03-05")
    if not revisions:
        return lake
    lake.items.ingest("item", frame.head(12).with_columns(pl.col("value") + 2),
                      available_date="2020-02-02", input_receipt=current)
    # A witnessed copy of unchanged baseline content carries the new proof.
    lake.items.ingest("item", frame.tail(12), available_date="2020-03-02",
                      input_receipt=current)
    return lake


def test_current_discrete_creators_need_no_whole_certificate_or_selection_index(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / 'meta.sqlite', lake_path=tmp_path / 'lake')
    days = [date(2020, 1, 1), date(2020, 2, 1)]
    frame = pl.DataFrame({'time': days, 'asset_id': ['A', 'A'], 'value': [1., 2.]})
    spec = DatasetSpec('raw', 'by_date', date_kind='calendar',
        field_mappings={'time': 'time', 'asset_id': 'asset_id'})
    lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    raw = RawInput('custom', 'raw', view='versions')
    lake.items.register(DataItemSpec('sparse', (raw,), producer_key='external', producer_revision='1'))
    for day in days:
        source = lake.inputs.freeze({'raw': replace(raw, observation_start=day, observation_end=day)},
            information_cutoff=days[-1])
        lake.items.replace_range('sparse', frame.filter(pl.col('time') == day), start=day, end=day,
            available_date=day, input_receipt=source)
    root = lake.inputs.freeze({'sparse': ItemInput('sparse', start=days[0], end=days[-1])},
        information_cutoff=days[-1])
    assert root.evidence['sparse']['empty_item_build'] is None
    assert len(root.evidence['sparse']['parent_receipts']) == 2
    def forbidden(*args, **kwargs):
        pytest.fail('Current creators requested numerical/selected-lineage work')
    monkeypatch.setattr(lake.inputs, '_selected_item_parents', forbidden)
    monkeypatch.setattr(lake.inputs, 'read', forbidden)
    monkeypatch.setattr(lake.inputs, 'verify', forbidden)
    assert lake.inputs.is_current(root)


@pytest.mark.parametrize('case', ['empty', 'future', 'strict_baseline', 'hole', 'mixed_visibility'])
def test_current_creators_do_not_certify_uncompleted_empty_selection(tmp_path, monkeypatch, case):
    lake = DataLake.open(data_meta_path=tmp_path / 'meta.sqlite', lake_path=tmp_path / 'lake')
    days = [date(2020, 1, 1), date(2020, 1, 3)]
    frame = pl.DataFrame({'time': days, 'asset_id': ['A', 'A'], 'value': [1., 2.]})
    spec = DatasetSpec('raw', 'by_date', date_kind='calendar',
        field_mappings={'time': 'time', 'asset_id': 'asset_id'})
    lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    raw = RawInput('custom', 'raw', view='versions')
    lake.items.register(DataItemSpec('item', (raw,), producer_key='external', producer_revision='1'))
    source = lake.inputs.freeze({'raw': raw}, information_cutoff='2020-01-03')
    output = frame.head(0) if case == 'empty' else frame
    if case == 'mixed_visibility':
        output = frame.with_columns(pl.Series('available_date', [date(2020, 1, 2), date(2020, 1, 4)]))
    for publication in ([output.head(1), output.tail(1)] if case == 'hole' else [output]):
        lake.items.ingest('item', publication,
            available_date=None if case == 'mixed_visibility' else
                '2020-01-04' if case == 'future' else '2020-01-01',
            historical_baseline=case == 'strict_baseline', input_receipt=source)
    lower = '2020-01-02' if case == 'hole' else '2020-01-03'
    upper = '2020-01-02' if case == 'hole' else '2020-01-04'
    root = lake.inputs.freeze({'item': ItemInput('item', start=lower, end=upper,
        strict=case == 'strict_baseline')}, information_cutoff='2020-01-03', retain_batch_bounds=True)
    assert root.evidence['item']['empty_item_build'] is None
    assert lake.inputs.is_current(source)
    assert lake.inputs.read(root, 'item').collect().is_empty()
    calls = []
    def selected(*args, **kwargs):
        calls.append(True)
        return None if case == 'hole' else (False, set())
    monkeypatch.setattr(lake.inputs, '_selected_item_parents', selected)
    monkeypatch.setattr(lake.inputs, 'read', lambda *a, **k: pytest.fail('currentness read values'))
    assert not lake.inputs.is_current(root)
    assert calls


def reference(lake, frozen):
    evidence = frozen.evidence["item"]
    frame = lake.inputs._read_frame(frozen, "item")
    cutoff = frozen.information_cutoff
    if cutoff is None and frame.height:
        cutoff = frame["time"].max()
    selected = lake.inputs._select(frame, replace(frozen.requests["item"], view="snapshot"),
                                   cutoff, evidence=evidence) if cutoff is not None else frame
    commits = {row["commit_seq"]: row["input_receipt_id"] for row in evidence["batches"]}
    checks = {row["id"]: row["input_receipt_id"] for row in evidence["checks"]}
    parents = set()
    for commit, check in selected.select("_commit_seq", "_attestation_id").unique().iter_rows():
        parent = checks.get(check) if check is not None else commits.get(commit)
        if parent is not None:
            parents.add(parent)
    if not selected.height and evidence["empty_item_build"] is not None:
        parent = evidence["empty_item_build"]["frozen_receipt_id"]
        if parent is not None:
            parents.add(parent)
    return bool(selected.height), parents


@pytest.mark.parametrize("baseline", [False, True])
@pytest.mark.parametrize("cutoff", ["2020-01-03", "2020-02-03", "2020-03-03", None])
@pytest.mark.parametrize("strict", [False, True])
def test_cross_month_and_witness_selection_equals_original_frames(tmp_path, baseline, cutoff, strict, monkeypatch):
    lake = prepared(tmp_path, baseline=baseline)
    frozen = lake.inputs.freeze({"item": ItemInput("item", strict=strict, start="2020-01-01", end="2020-01-01")}, information_cutoff=cutoff)
    expected = reference(lake, frozen)
    selected = lake.inputs._selected_item_parents(frozen, "item", ExecutionOptions())
    assert selected is not None and selected[0] == expected[0]
    # Compact provenance retains conservative parent requirements for mixed
    # creator segments; it must never omit a actually selected creator.
    assert not expected[0] or selected[1] >= expected[1]



def test_compact_attestation_matches_inline_selection(tmp_path, monkeypatch):
    lake = prepared(tmp_path, baseline=True, revisions=False)
    parent_id = lake.items.builds("item")[0]["frozen_receipt_id"]
    parent = lake.inputs.get(parent_id)
    # Reattest the whole original baseline, not a partial/mixed publication.
    original = pl.DataFrame({"time": [date(2020, 1, 1)] * 24,
                            "asset_id": [f"asset-{i}" for i in range(24)],
                            "value": [float(i) for i in range(24)]})
    lake.items.ingest("item", original, available_date="2020-03-03", input_receipt=parent)
    monkeypatch.setattr(full_commit_checks, "COMPACT_MIN_ROWS", 1)
    assert not hasattr(lake.inputs, "index_checks")
    frozen = lake.inputs.freeze({"item": ItemInput("item", strict=True)}, information_cutoff="2020-03-03")
    assert frozen.evidence["item"]["record_checks"]
    assert not frozen.evidence["item"].get("full_commit_checks")
    assert lake.inputs._selected_item_parents(frozen, "item", ExecutionOptions(max_buffer_bytes=128 * 1024)) == reference(lake, frozen)




def test_original_corruption_requires_explicit_audit(tmp_path):
    lake = prepared(tmp_path)
    frozen = lake.inputs.freeze({"item": ItemInput("item")}, information_cutoff="2020-03-03")
    expected = reference(lake, frozen)
    with sqlite3.connect(lake.data_meta_path) as db:
        damage_original_segments(db, 'commit_seq=?', (frozen.evidence['item']['batches'][0]['commit_seq'],))
    # Metadata currentness is not a byte-integrity audit. Partial creator
    # selection now consumes sole Parquet and must reject damaged bytes.
    assert expected is not None
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        lake.inputs.verify(frozen)



@pytest.mark.parametrize("budget", [32 * 1024, 1024 * 1024])
@pytest.mark.parametrize("error_type", [TypeError, ValueError, RuntimeError, CancelledError])
def test_project_consumer_exception_preserves_identity_and_closes_transport(tmp_path, monkeypatch, budget, error_type):
    lake = prepared(tmp_path)
    frozen = lake.inputs.freeze({"item": ItemInput("item")}, information_cutoff="2020-03-03")
    batch = frozen.evidence["item"]["batches"][0]
    files = []
    original = recovery.pq.ParquetFile
    def temporary(*args, **kwargs):
        file = original(*args, **kwargs)
        files.append(file)
        return file
    monkeypatch.setattr(recovery.pq, "ParquetFile", temporary)
    error = error_type("original consumer failure")
    def consume(frame):
        assert "value" not in frame.columns
        raise error
    with pytest.raises(error_type) as caught:
        recovery.project_batch(lake._data_meta, batch["partition_path"], batch["commit_seq"],
            batch["content_hash"], columns=("_record_id",), consume=consume, buffer_bytes=budget)
    assert caught.value is error
    assert all(file.closed for file in files)
    assert files


def test_tied_witness_sort_keys_and_cutoff_none_keep_original_selected_parent(tmp_path):
    lake = prepared(tmp_path, baseline=True, revisions=False)
    first = lake.inputs.get(lake.items.builds("item")[0]["frozen_receipt_id"])
    # Distinct valid Source headers retain the same original dependency bytes.
    # Their different cutoffs cannot alter the tied output witness sort keys.
    second = lake.inputs.freeze(first.requests, max_commit=first.max_commit,
        information_cutoff="2020-03-06")
    assert first.receipt_id != second.receipt_id and first.digest != second.digest
    frame = pl.DataFrame({"time": [date(2020, 1, 1)] * 24,
                          "asset_id": [f"asset-{i}" for i in range(24)],
                          "value": [float(i) for i in range(24)]})
    for receipt in (first, second):
        lake.items.ingest("item", frame, available_date="2020-03-03", input_receipt=receipt,
                          ingested_at=datetime(2020, 3, 3, tzinfo=UTC))
    frozen = lake.inputs.freeze({"item": ItemInput("item", strict=True)})
    assert len(frozen.evidence["item"]["checks"]) == 1
    # Only the earliest real visibility produces a row event.
    assert len(frozen.evidence["item"]["record_checks"]) == 1
    actual = lake.inputs._selected_item_parents(frozen, "item", ExecutionOptions(max_buffer_bytes=32 * 1024))
    expected = reference(lake, frozen)
    assert actual is not None and actual[0] == expected[0] and actual[1] >= expected[1]
