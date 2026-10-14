"""Public temporary-lake reproduction of retained empty head + native tail."""
from dataclasses import replace
from datetime import date

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, ItemInput, ItemPublication


def segmented_output(tmp_path, *, head=True):
    lake = DataLake.open(data_meta_path=tmp_path/'meta.sqlite', lake_path=tmp_path/'lake')
    leaf = pl.DataFrame({'time': [date(2024,1,1)], 'asset_id': ['A'], 'value': [1.]})
    lake.items.register(DataItemSpec('leaf'))
    lake.items.ingest('leaf', leaf, available_date='2024-01-02')
    requests = {'leaf': ItemInput('leaf')}
    old_source = lake.inputs.freeze(requests, information_cutoff='2024-01-02')
    old_spec = DataItemSpec('output', tuple(requests.values()), producer_key='owner', producer_revision='1')
    lake.items.register(old_spec)
    empty = leaf.clear()
    lake.items.replace_range('output', empty, start='2024-01-01', end='2024-01-02',
        available_date='2024-01-02', input_receipt=old_source, historical_baseline=True)
    spec = replace(old_spec, producer_revision='2')
    lake.items.register(spec)
    tail_source = lake.inputs.freeze(requests, information_cutoff='2024-01-04')
    tail = pl.DataFrame({'time': [date(2024,1,3)], 'asset_id': ['A'], 'value': [3.],
        'version_available_date': [date(2024,1,4)]})
    with lake.items.publication(input_receipt=tail_source) as publisher:
        publisher.publish([ItemPublication('output', tail, start='2024-01-03',
            end='2024-01-04', complete_at='2024-01-04')])
    original_tail = lake.items.completed_builds('output')[-1]
    whole_request = {'output': ItemInput('output', start='2024-01-01', end='2024-01-04', view='snapshot')}
    retained = lake.inputs.freeze(whole_request, information_cutoff='2024-01-04', retain_batch_bounds=True)
    assert retained.evidence['output']['empty_item_build'] is None
    assert set(retained.evidence['output']['parent_receipts']) == {old_source.receipt_id, tail_source.receipt_id}
    lake.items.register(replace(lake.items.get('leaf'), metadata={'current_source_revision': 2}))
    current = lake.inputs.freeze(requests, information_cutoff='2024-01-04')
    assert not lake.inputs.is_current(old_source)
    assert not lake.inputs.is_current(tail_source)
    assert not lake.inputs.is_current(retained)
    # The numerical owner has separately proved the head is empty and the tail
    # unchanged. Data certifies those declared ranges without a whole-range claim.
    with lake.items.publication(input_receipt=current) as publisher:
        publisher.publish([ItemPublication('output', empty, start='2024-01-03',
            end='2024-01-04', complete_at='2024-01-04',
            reuse_build_id=original_tail['id'], affected_ranges=())])
        if head:
            publisher.publish([ItemPublication('output', empty, start='2024-01-01',
                end='2024-01-02', complete_at='2024-01-04')])
    fresh = lake.inputs.freeze(whole_request, information_cutoff='2024-01-04', retain_batch_bounds=True)
    assert fresh.evidence['output']['empty_item_build'] is None
    return lake, retained, fresh, whole_request, current, leaf


def test_fresh_native_scopes_cover_empty_head_and_tail(tmp_path, monkeypatch):
    lake, retained, fresh, _, _, _ = segmented_output(tmp_path)
    def forbidden(*args, **kwargs):
        raise AssertionError("Currentness must not decode numerical rows")
    from bagelquant_data import inputs
    monkeypatch.setattr(inputs, "read_batch", forbidden)
    assert not lake.inputs.is_current(retained)
    assert lake.inputs.is_current(fresh)


def test_missing_head_stays_stale(tmp_path):
    lake, _, fresh, _, _, _ = segmented_output(tmp_path, head=False)
    assert not lake.inputs.is_current(fresh)


@pytest.mark.parametrize("change", ["source", "definition", "later_check"])
def test_native_segments_preserve_currentness_fences(tmp_path, change):
    lake, _, fresh, requests, current, leaf = segmented_output(tmp_path)
    if change == "source":
        lake.items.register(replace(lake.items.get("leaf"), metadata={"current_source_revision": 3}))
    elif change == "definition":
        lake.items.register(replace(lake.items.get("output"), producer_revision="3"))
    else:
        lake.items.ingest("output", leaf.with_columns(pl.lit(date(2024, 1, 3)).alias("time")),
            available_date="2024-01-04", input_receipt=current)
    assert not lake.inputs.is_current(fresh)


def test_unknown_occurrence_cannot_replace_its_parent(tmp_path):
    from bagelquant_data import ExecutionOptions
    lake, _, fresh, _, _, _ = segmented_output(tmp_path)
    evidence = dict(fresh.evidence["output"])
    batch = dict(evidence["batches"][0])
    parent = batch["input_receipt_id"]
    batch["min_observation"] = batch["max_observation"] = None
    evidence["batches"] = [batch]
    unbounded = replace(fresh, evidence={"output": evidence})
    with lake.inputs._store.connect() as db:
        replacements = lake.inputs._native_parent_replacements(unbounded, "output", db=db, options=ExecutionOptions())
    assert parent not in replacements


@pytest.mark.parametrize("view", ["snapshot", "versions"])
@pytest.mark.parametrize("change", [None, "missing_head", "source", "definition", "later_check"])
def test_unbounded_selection_composes_scopes_with_captured_partial_build(tmp_path, monkeypatch, view, change):
    lake, _, _, _, current, leaf = segmented_output(tmp_path, head=change != "missing_head")
    unbounded = lake.inputs.freeze({'output': ItemInput('output', view=view)},
        information_cutoff='2024-01-04', retain_batch_bounds=True)
    # Unlike the bounded whole request, capture retains the most recent partial
    # build. It is evidence for its own range, not the whole unbounded selection.
    proof = unbounded.evidence['output']['empty_item_build']
    assert proof is not None
    if change == 'source':
        lake.items.register(replace(lake.items.get('leaf'), metadata={'current_source_revision': 3}))
    elif change == 'definition':
        lake.items.register(replace(lake.items.get('output'), producer_revision='3'))
    elif change == 'later_check':
        lake.items.ingest('output', leaf.with_columns(pl.lit(date(2024, 1, 3)).alias('time')),
            available_date='2024-01-04', input_receipt=current)
    from bagelquant_data import inputs
    monkeypatch.setattr(inputs, 'read_batch', lambda *a, **k: pytest.fail('Currentness decoded values'))
    assert lake.inputs.is_current(unbounded) is (change is None)
