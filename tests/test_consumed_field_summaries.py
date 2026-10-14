"""Typed consumption proofs reuse one bounded, original-evidence summary path."""

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
import json
import sqlite3
from typing import Any

import polars as pl
import pytest

from bagelquant_data import (
    DataItemSpec, DataLake, DatasetSpec, ExecutionOptions, InputSelectionScope,
    ItemInput, ItemPublication, RawInput, input_index,
)


FIELDS = ("time", "asset_id", "value", "available_date", "observation_date", "base_available_date")
DAY = date(2024, 1, 1)


def setup(tmp_path, *, baseline=False, count=3):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("x"))
    days = [DAY + timedelta(days=i) for i in range(count)]
    frame = pl.DataFrame({
        "time": days, "asset_id": ["A"] * count, "value": [float(i) for i in range(count)],
        "available_date": days, "observation_date": [day - timedelta(days=2) for day in days],
        "base_available_date": [day - timedelta(days=1) for day in days], "creator": ["first"] * count,
    })
    lake.items.ingest("x", frame, historical_baseline=baseline)
    old = freeze(lake)
    return lake, frame, old


def freeze(lake, *, view="snapshot", strict=False):
    return lake.inputs.freeze({"x": ItemInput("x", start=DAY, end=DAY + timedelta(days=5),
        view=view, strict=strict)}, information_cutoff=DAY + timedelta(days=10), retain_batch_bounds=True)


def remove_rows(lake):
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_value_index'").fetchone() is None
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_index'").fetchone() is None


def no_value_reads(monkeypatch):
    from bagelquant_data import inputs
    def forbidden(*_args, **_kwargs):
        pytest.fail("ordinary consumption proof decoded original bytes or row indexes")
    monkeypatch.setattr(input_index, "project_original", forbidden)
    monkeypatch.setattr(input_index, "project_original", forbidden)
    monkeypatch.setattr(inputs, "read_batch", forbidden)


@pytest.mark.parametrize("changed", FIELDS[3:])
def test_same_scalar_changed_consumed_date_is_not_equivalent(tmp_path, monkeypatch, changed):
    lake, frame, old = setup(tmp_path)
    changed_frame = frame.head(1).with_columns((pl.col(changed) + timedelta(days=1)).alias(changed))
    lake.items.ingest("x", changed_frame, available_date=DAY + timedelta(days=6))
    new = freeze(lake)
    assert lake.inputs.read(old, "x", fields=("time", "asset_id", "value")).collect().equals(
        lake.inputs.read(new, "x", fields=("time", "asset_id", "value")).collect())
    assert not lake.inputs.read(old, "x", fields=FIELDS).collect().equals(
        lake.inputs.read(new, "x", fields=FIELDS).collect())
    scalar_old = lake.inputs.index_selection(old, "x")
    scalar_new = lake.inputs.index_selection(new, "x")
    assert scalar_old == scalar_new
    remove_rows(lake)
    left = lake.inputs.index_selection(old, "x", fields=FIELDS)
    right = lake.inputs.index_selection(new, "x", fields=list(FIELDS))
    assert left is not None and right is not None and left != right
    assert lake.inputs.verify([old, new])["valid"]
    no_value_reads(monkeypatch)
    assert lake.inputs.selection_equivalent(old, new, "x", fields=FIELDS) is False
    assert lake.inputs.index_selection(old, "x", fields=FIELDS) == left
    assert lake.inputs.selection_equivalent(old, new, "x", values_only=True, summaries_only=True) is True


def test_nonmember_and_unconsumed_payload_changes_do_not_invalidate(tmp_path, monkeypatch):
    lake, frame, old = setup(tmp_path)
    coords = pl.DataFrame({"time": [DAY, DAY + timedelta(days=1)], "asset_id": ["A", "ABSENT"]})
    original_hashes = lake.inputs.read(old, "x").collect().select("time", "asset_id", "value", "creator").to_dicts()
    lake.items.ingest("x", frame.head(1).with_columns(pl.lit("revised").alias("creator")),
        available_date=DAY + timedelta(days=6))
    lake.items.ingest("x", frame.head(1).with_columns(pl.lit("B").alias("asset_id"),
        pl.lit(99.0).alias("value")), available_date=DAY + timedelta(days=6))
    new = freeze(lake)
    assert lake.inputs.read(new, "x").collect().select("time", "asset_id", "value", "creator").to_dicts() != original_hashes
    remove_rows(lake)
    first = lake.inputs.index_selection(old, "x", fields=FIELDS, coordinates=coords)
    second = lake.inputs.index_selection(new, "x", fields=FIELDS, coordinates=coords)
    assert first == second and first is not None
    assert lake.inputs.index_selection(old, "x", fields=FIELDS, coordinates=coords.head(1)) != first
    assert lake.inputs.verify([old, new])["valid"]
    no_value_reads(monkeypatch)
    assert lake.inputs.selection_equivalent(old, new, "x", fields=FIELDS, coordinates=coords,
        summaries_only=True) is True
    assert lake.inputs.index_selection(new, "x", fields=FIELDS, coordinates=coords.reverse()) == second
    assert lake.inputs.index_selection(new, "x", fields=FIELDS,
        coordinates=coords.select("asset_id", "time")) == second
    assert lake.inputs.coordinate_selector_identity(coords.select("asset_id", "time")) == (
        lake.inputs.coordinate_selector_identity(coords))


def test_missing_field_summary_is_unknown_without_any_value_read(tmp_path, monkeypatch):
    lake, frame, old = setup(tmp_path)
    lake.items.ingest("x", frame.head(1).with_columns(pl.lit(DAY).alias("observation_date")),
        available_date=DAY + timedelta(days=6))
    new = freeze(lake)
    no_value_reads(monkeypatch)
    assert lake.inputs.selection_identity(old, "x", fields=FIELDS) is not None
    assert lake.inputs.selection_equivalent(old, new, "x", fields=FIELDS) is None
    assert lake.inputs.selection_equivalent(old, new, "x", fields=FIELDS, records_only=True) is None
    assert lake.inputs.selection_equivalent(old, old, "x", fields=FIELDS, records_only=True) is True


def test_append_reuses_existing_historical_field_summary_across_receipts(tmp_path, monkeypatch):
    lake, frame, old = setup(tmp_path)
    coords = frame.head(1).select("time", "asset_id")
    token = lake.inputs.index_selection(old, "x", end=DAY, fields=FIELDS, coordinates=coords)
    # A newer complete Source contains an independent later date/check. Its
    # recorded witness cannot create any row in this already-completed scope.
    appended = frame.head(1).with_columns(pl.lit(DAY + timedelta(days=3)).alias("time"),
        pl.lit(DAY + timedelta(days=3)).alias("available_date"), pl.lit(99.0).alias("value"))
    lake.items.ingest("x", appended, available_date=DAY + timedelta(days=3))
    new = freeze(lake)
    remove_rows(lake)
    assert lake.inputs.verify(new)["valid"]  # Includes the reused original summary's source.
    no_value_reads(monkeypatch)
    assert lake.inputs.index_selection(new, "x", end=DAY, fields=FIELDS, coordinates=coords) == token
    assert lake.inputs.selection_equivalent(old, new, "x", end=DAY, fields=FIELDS, coordinates=coords) is True
    progress = []
    assert lake.inputs.index_selections(new, [InputSelectionScope("x", end=DAY, fields=FIELDS,
        coordinates=coords)], progress=progress.append) == (token,)
    assert progress[-1]["skipped"] == 1 and progress[-1]["batches"] == 0


def test_only_eligible_original_baseline_witness_changes_field_admission(tmp_path, monkeypatch):
    lake, frame, old = setup(tmp_path, baseline=True)
    empty = lake.inputs.index_selection(old, "x", fields=FIELDS, strict=True, as_of=DAY + timedelta(days=2))
    lake.items.ingest("x", frame, available_date=DAY + timedelta(days=6))
    new = freeze(lake)
    # A future same-content witness cannot expire the earlier empty selection.
    with monkeypatch.context() as patch:
        no_value_reads(patch)
        assert lake.inputs.index_selection(new, "x", fields=FIELDS, strict=True,
            as_of=DAY + timedelta(days=2)) == empty
    visible = lake.inputs.index_selection(new, "x", fields=FIELDS, strict=True, as_of=DAY + timedelta(days=7))
    assert visible is not None and visible != empty
    assert lake.inputs.read(new, "x", strict=True, as_of=DAY + timedelta(days=2)).collect().is_empty()
    assert lake.inputs.read(new, "x", strict=True, as_of=DAY + timedelta(days=7)).collect().height == frame.height
    remove_rows(lake)
    assert lake.inputs.verify([old, new])["valid"]


@pytest.mark.parametrize("baseline,strict", [(False, False), (True, False), (True, True)])
def test_complete_history_and_late_versions_share_the_real_pit_selector(tmp_path, baseline, strict):
    lake, frame, old = setup(tmp_path, baseline=baseline)
    lake.items.ingest("x", frame.head(1).with_columns(pl.lit(DAY).alias("observation_date")),
        available_date=DAY + timedelta(days=6), historical_baseline=baseline)
    new = freeze(lake)
    scopes = [InputSelectionScope("x", view=view, strict=strict, fields=FIELDS, causal_versions=causal)
        for view, causal in (("snapshot", False), ("history", False), ("versions", True))]
    remove_rows(lake)
    before = lake.inputs.index_selections(old, scopes)
    after = lake.inputs.index_selections(new, scopes)
    assert all(before) and all(after)
    assert before[1] == after[1]  # Late revision is not a then-visible history event.
    assert (before[0] == after[0]) is strict
    assert (before[2] == after[2]) is strict
    assert lake.inputs.verify([old, new])["valid"]
    for scope, token in zip(scopes, after):
        assert lake.inputs.index_selection(new, "x", view=scope.view, strict=strict,
            causal_versions=scope.causal_versions, fields=FIELDS) == token


def test_typed_null_nan_infinity_and_signed_zero_remain_distinct(tmp_path):
    lake, frame, _ = setup(tmp_path)
    tokens = []
    roots = []
    for index, value in enumerate((None, float("nan"), float("inf"), -float("inf"), -0.0, 0.0)):
        lake.items.ingest("x", frame.head(1).with_columns(pl.lit(value, dtype=pl.Float64).alias("value")),
            available_date=DAY + timedelta(days=index + 4))
        root = lake.inputs.freeze({"x": ItemInput("x", start=DAY, end=DAY, view="snapshot")},
            information_cutoff=DAY + timedelta(days=index + 4), retain_batch_bounds=True)
        roots.append(root)
        tokens.append(lake.inputs.index_selection(root, "x", fields=FIELDS))
    assert all(tokens) and len(set(tokens)) == 6
    remove_rows(lake)
    assert lake.inputs.verify(roots)["valid"]


def test_empty_projection_binds_original_types_and_absent_coordinates(tmp_path):
    lake, frame, _ = setup(tmp_path)
    lake.items.ingest("x", frame.head(1).with_columns(pl.lit(1, dtype=pl.Int64).alias("quality")),
        available_date=DAY + timedelta(days=4))
    old = freeze(lake)
    fields = (*FIELDS, "quality")
    empty = pl.DataFrame(schema={"time": pl.Date, "asset_id": pl.String})
    absent = pl.DataFrame({"time": [DAY], "asset_id": ["ABSENT"]})
    first = lake.inputs.index_selection(old, "x", fields=fields, coordinates=empty)
    assert first is not None
    assert lake.inputs.index_selection(old, "x", fields=fields, coordinates=absent) != first
    # A payload type change is significant even when the consumed mask is empty.
    lake.items.ingest("x", frame.head(1).with_columns(pl.lit(1.0, dtype=pl.Float64).alias("quality")),
        available_date=DAY + timedelta(days=6))
    new = freeze(lake)
    assert lake.inputs.index_selection(new, "x", fields=fields, coordinates=empty) != first
    remove_rows(lake)
    assert lake.inputs.verify([old, new])["valid"]


def test_empty_baseline_summary_keeps_authority_without_original_batches(tmp_path):
    lake, frame, _ = setup(tmp_path)
    lake.items.register(DataItemSpec("empty"))
    lake.items.ingest("empty", frame.head(0), historical_baseline=True, available_date=DAY)
    root = lake.inputs.freeze({"x": ItemInput("empty", start=DAY, end=DAY + timedelta(days=2), view="snapshot")},
        information_cutoff=DAY + timedelta(days=2), retain_batch_bounds=True)
    assert root.evidence["x"]["empty_checks"]
    fields = ("time", "asset_id", "value")
    token = lake.inputs.index_selection(root, "x", fields=fields)
    assert token is not None and lake.inputs.index_selection(root, "x", fields=fields) == token
    causal = lake.inputs.index_selection(root, "x", fields=fields, view="versions", causal_versions=True)
    assert causal is not None and causal != token
    assert lake.inputs.index_selection(root, "x", fields=FIELDS) is not None
    with pytest.raises(ValueError, match="missing requested fields"):
        lake.inputs.index_selection(root, "x", fields=(*FIELDS, "not_declared"))
    assert lake.inputs.verify(root)["valid"]
    assert lake.inputs.get(root).digest == root.digest


def test_first_empty_publication_preserves_full_original_schema_and_receipts(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("events"))
    schema = {"time": pl.Date, "asset_id": pl.String, "value": pl.Float64,
        "record_date": pl.Date, "ex_date": pl.Date, "cash_pay_date": pl.Date,
        "share_available_date": pl.Date, "cash_per_share": pl.Float64,
        "domain_asset_id": pl.String, "domain_row_present": pl.Boolean}
    fields = tuple(schema)
    frame = pl.DataFrame(schema=schema)
    def publish(value):
        with lake.items.publication() as operation:
            operation.publish([ItemPublication("events", value, start=DAY,
                end=DAY + timedelta(days=30), complete_at=DAY + timedelta(days=30))])
    publish(frame)
    old = lake.inputs.freeze({"x": ItemInput("events", start=DAY, end=DAY + timedelta(days=30))},
        information_cutoff=DAY + timedelta(days=30))
    original = lake.inputs.get(old)
    assert old.evidence["x"]["schema_ipc"] and not old.evidence["x"]["batches"]
    assert lake.inputs.read(old, "x", fields=fields).collect().schema == schema
    scopes = [InputSelectionScope("x", fields=fields, view=view, causal_versions=causal)
        for view, causal in (("snapshot", False), ("history", False), ("versions", True))]
    tokens = lake.inputs.index_selections(old, scopes)
    assert all(tokens) and lake.inputs.verify(old)["valid"]
    # Canonical schema evolution cannot rewrite an original typed empty receipt.
    publish(frame.with_columns(pl.col("cash_per_share").cast(pl.Float32),
        pl.lit(None, dtype=pl.Int64).alias("quality")))
    new = lake.inputs.freeze({"x": old.requests["x"]}, information_cutoff=DAY + timedelta(days=30))
    assert "quality" in lake.inputs.read(new, "x").collect().columns
    assert lake.inputs.get(old) == original
    assert lake.inputs.read(old, "x", fields=fields).collect().schema == schema
    with pytest.raises(ValueError, match="missing requested fields"):
        lake.inputs.selection_identity(old, "x", fields=(*fields, "quality"))
    assert lake.inputs.verify([old, new])["valid"]
    no_value_reads(monkeypatch)
    for scope, token in zip(scopes, tokens):
        assert lake.inputs.index_selection(old, "x", fields=fields, view=scope.view,
            causal_versions=scope.causal_versions) == token


@pytest.mark.parametrize("first_empty", [False, True])
def test_reordered_empty_publication_preserves_original_canonical_schema(tmp_path, first_empty):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("events"))
    frame = pl.DataFrame({"time": [DAY], "asset_id": ["A"], "value": [1.], "extra": [7],
        "domain_row_present": [True], "available_date": [DAY], "observation_date": [DAY]})
    def publish(value, *, start=DAY):
        with lake.items.publication() as operation:
            operation.publish([ItemPublication("events", value, start=start,
                end=DAY + timedelta(days=30), complete_at=DAY + timedelta(days=30))])
    publish(frame.clear() if first_empty else frame)
    original = lake.inputs.freeze({"x": ItemInput("events", start=DAY, end=DAY + timedelta(days=30))},
        information_cutoff=DAY + timedelta(days=30))
    saved = lake.inputs.get(original)
    schema = original.evidence["x"]["schema_ipc"]
    publish(frame.select(list(reversed(frame.columns))).clear(), start=DAY + timedelta(days=1))
    current = lake.inputs.freeze(original.requests, information_cutoff=original.information_cutoff)
    assert current.evidence["x"]["schema_ipc"] == schema
    assert lake.inputs.get(original) == saved
    fields = tuple(frame.columns)
    for source in (original, current):
        lake.inputs.index_selection(source, "x", fields=fields)
    assert lake.inputs.selection_equivalent(original, current, "x", fields=fields) is True
    if not first_empty:
        assert lake.inputs.selection_equivalent(original, current, "x") is True
    publish(frame.clear().with_columns(pl.lit(None, dtype=pl.Int64).alias("quality")), start=DAY + timedelta(days=1))
    extended = lake.inputs.freeze(original.requests, information_cutoff=original.information_cutoff)
    assert extended.evidence["x"]["schema_ipc"] != schema
    assert "quality" in lake.inputs.read(extended, "x", start=DAY + timedelta(days=1)).collect_schema()
    assert lake.inputs.get(original) == saved


def test_typed_bulk_reads_once_and_skips_registered_group(tmp_path, monkeypatch):
    lake, _, root = setup(tmp_path)
    original = input_index.project_original
    seen = []
    def project(*args, **kwargs):
        seen.append((args[1]["commit_seq"], args[1]["partition_path"]))
        return original(*args, **kwargs)
    monkeypatch.setattr(input_index, "project_original", project)
    scopes = [InputSelectionScope("x", end=DAY + timedelta(days=i), fields=FIELDS) for i in range(3)]
    scopes.append(InputSelectionScope("x"))  # Old scalar and typed scopes share original transport.
    progress = []
    tokens = lake.inputs.index_selections(root, scopes, progress=progress.append)
    assert all(tokens) and len(seen) == len(set(seen)) == len(root.evidence["x"]["batches"])
    assert progress[-1]["hash_rows"] == 3
    seen.clear()
    repeated = []
    assert lake.inputs.index_selections(root, scopes, progress=repeated.append) == tokens
    assert not seen and repeated[-1]["skipped"] == 4 and repeated[-1]["batches"] == 0
    remove_rows(lake)
    assert lake.inputs.verify(root)["valid"]


def test_consumed_projection_does_not_hash_an_unused_full_payload(tmp_path, monkeypatch):
    lake, _, root = setup(tmp_path)
    identity = input_index.identity
    projections = []
    def content(frame):
        assert not set(frame.columns) & {"_payload_hash", "_commit_seq", "_record_id", "creator", "ingested_at"}
        projections.append(tuple(frame.columns))
        return identity(frame)
    monkeypatch.setattr(input_index, "identity", content)
    assert lake.inputs.index_selection(root, "x", fields=FIELDS) is not None
    assert projections == [FIELDS]


def test_atomic_typed_group_cancel_budget_and_resume(tmp_path, monkeypatch):
    lake, _, root = setup(tmp_path)
    first = InputSelectionScope("x", end=DAY, fields=FIELDS)
    prior = lake.inputs.index_selections(root, [first])[0]
    scopes = [InputSelectionScope("x", end=DAY + timedelta(days=i), fields=FIELDS) for i in (1, 2)]
    canceled = [False]
    save = input_index.save_projection
    def publish(*args, **kwargs):
        save(*args, **kwargs)
        canceled[0] = True
    def check():
        if canceled[0]:
            raise RuntimeError("cancel typed group")
    monkeypatch.setattr(input_index, "save_projection", publish)
    with pytest.raises(RuntimeError, match="cancel typed group"):
        lake.inputs.index_selections(root, scopes, check_canceled=check)
    assert lake.inputs.index_selection(root, "x", end=DAY, fields=FIELDS) == prior
    assert lake.inputs.selection_identity(root, "x", end=DAY + timedelta(days=1), fields=FIELDS) is not None
    monkeypatch.setattr(input_index, "save_projection", save)
    assert all(lake.inputs.index_selections(root, scopes))
    with monkeypatch.context() as patch:
        no_value_reads(patch)
        with pytest.raises(MemoryError, match="Aggregate"):
            lake.inputs.index_selections(root, scopes * 8, config=ExecutionOptions(max_buffer_bytes=1024))
    assert lake.inputs.verify(root)["valid"]


@pytest.mark.parametrize("bad", ["fields", "internal", "wider", "coords", "receipt"])
def test_invalid_projection_scope_is_rejected_before_decode(tmp_path, monkeypatch, bad):
    lake, _, root = setup(tmp_path)
    no_value_reads(monkeypatch)
    kwargs: dict[str, Any] = {"fields": FIELDS}
    if bad == "fields":
        kwargs["fields"] = ("not_a_field",)
    elif bad == "internal":
        kwargs["fields"] = ("_payload_hash",)
    elif bad == "wider":
        kwargs["start"] = DAY - timedelta(days=1)
    elif bad == "coords":
        kwargs["coordinates"] = pl.DataFrame(schema={"time": pl.Datetime, "asset_id": pl.String})
    else:
        root = replace(root, digest="f" * 64)
    with pytest.raises((ValueError, RuntimeError)):
        lake.inputs.selection_identity(root, "x", **kwargs)


@pytest.mark.parametrize("forged", ["identity", "field_scope", "source_digest"])
def test_self_consistent_typed_summary_forgery_fails_original_audit(tmp_path, forged):
    lake, _, root = setup(tmp_path)
    lake.inputs.index_selection(root, "x", fields=FIELDS)
    remove_rows(lake)
    with sqlite3.connect(lake.data_meta_path) as db:
        db.row_factory = sqlite3.Row
        body = dict(db.execute("select * from input_projection_index").fetchone())
        if forged == "identity":
            body["identity"] = "f" * 64
        elif forged == "field_scope":
            key = json.loads(body["request_key"])
            key["fields"] = ["value"]
            body["request_key"] = json.dumps(key, sort_keys=True, separators=(",", ":"))
        else:
            body["source_receipt_digest"] = "f" * 64
        body.pop("digest")
        db.execute("update input_projection_index set request_key=?,identity=?,source_receipt_digest=?,digest=?",
            (body["request_key"], body["identity"], body["source_receipt_digest"], input_index._digest(body)))
    with pytest.raises(RuntimeError, match="(differs from original|original source)"):
        lake.inputs.verify(root)


def test_raw_field_projection_preserves_baseline_policy_and_ignores_other_payload(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("raw", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [DAY], "asset_id": ["A"], "close": [2.0], "other": [1]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2024, 1, 2, tzinfo=UTC))
    old = lake.inputs.freeze({"x": RawInput("custom", "raw", view="snapshot", include_historical_baseline=True,
        fields=("time", "close"))},
        information_cutoff=DAY + timedelta(days=2), retain_batch_bounds=True)
    lake.raw.ingest(spec, frame.with_columns(pl.lit(2).alias("other")),
        ingested_at=datetime(2024, 1, 3, tzinfo=UTC))
    new = lake.inputs.freeze(old.requests, information_cutoff=DAY + timedelta(days=2), retain_batch_bounds=True)
    fields = ("source_time", "asset_id", "close")
    remove_rows(lake)
    first = lake.inputs.index_selection(old, "x", fields=fields)
    assert first is not None and lake.inputs.index_selection(new, "x", fields=fields) == first
    # Physical availability is significant only when the consumer projects it.
    timed = (*fields, "time")
    assert lake.inputs.index_selection(old, "x", fields=timed) != lake.inputs.index_selection(new, "x", fields=timed)
    assert lake.inputs.index_selection(old, "x", fields=fields, strict=True) != first
    assert lake.inputs.verify([old, new])["valid"]
    no_value_reads(monkeypatch)
    assert lake.inputs.selection_equivalent(old, new, "x", fields=fields) is True
