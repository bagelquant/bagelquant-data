"""Explicit compact preparation verifies each authority once per transaction."""
from collections import Counter
from copy import deepcopy
from datetime import UTC, date, datetime, timedelta
import inspect
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from bagelquant_data import (
    DataItemSpec, DataLake, DatasetSpec, ExecutionOptions, FrozenInputReceipt,
    InputSelectionScope, ItemInput, RawInput,
)
from bagelquant_data import input_index, inputs


DAY = date(2024, 1, 1)


def setup(path: Path) -> tuple[DataLake, FrozenInputReceipt, FrozenInputReceipt, tuple[InputSelectionScope, ...]]:
    lake = DataLake.open(data_meta_path=path / "meta.sqlite", lake_path=path / "lake")
    request = RawInput("custom", "quotes", alias="raw", view="snapshot")
    spec = DatasetSpec("quotes", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [DAY + timedelta(days=i) for i in range(16)],
        "asset_id": ["A"] * 16, "close": [float(i + 1) for i in range(16)]})
    lake.raw.ingest(spec, frame, ingested_at=datetime(2024, 1, 16, tzinfo=UTC))
    parent = lake.inputs.freeze({"raw": request}, information_cutoff="2024-01-16")
    lake.items.register(DataItemSpec("x", inputs=(request,)))
    lake.items.ingest("x", frame.rename({"close": "value"}),
        input_receipt=parent, available_date="2024-01-16")
    root = lake.inputs.freeze({"x": ItemInput("x", view="snapshot"),
        **{f"aux_{i}": ItemInput("x", view="snapshot") for i in range(31)}},
        information_cutoff="2024-01-16")
    scopes = tuple(InputSelectionScope("x", start=DAY + timedelta(days=i),
        end=DAY + timedelta(days=i), view="snapshot", fields=("value",)) for i in range(16))
    return lake, root, parent, scopes


def rows(lake: DataLake) -> tuple[tuple[Any, ...], ...]:
    with lake._data_meta.connect() as db:
        exists = db.execute("select 1 from sqlite_master where name='input_projection_index'").fetchone()
        return () if exists is None else tuple(tuple(row) for row in db.execute(
            "select * from input_projection_index order by evidence_key,request_key"))


@pytest.mark.parametrize("count", [1, 16])
def test_one_actual_json_decode_per_proof_source_and_identical_repeat(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, count: int,
) -> None:
    lake, root, parent, scopes = setup(tmp_path)
    scopes = scopes[:count]
    expected = lake.inputs.selection_descriptors(root, scopes)
    with lake._data_meta.connect() as db:
        serialized = db.execute("select payload_json from frozen_inputs where receipt_id=?",
            (root.receipt_id,)).fetchone()[0]
    counters: Counter[str] = Counter()
    state = {"writer": False}
    get, loads, save = lake.inputs.get, inputs.json.loads, input_index.save_projection

    def counted_get(value: FrozenInputReceipt | str) -> FrozenInputReceipt:
        caller = inspect.currentframe()
        assert caller is not None and caller.f_back is not None
        caller = caller.f_back
        db = caller.f_locals.get("db")
        writer = caller.f_code.co_name == "prepare_range_selections" and db is not None and db.in_transaction
        counters["get_total"] += 1
        counters["get_inside_writer"] += int(writer)
        previous, state["writer"] = state["writer"], writer
        try:
            return get(value)
        finally:
            state["writer"] = previous

    def counted_load(value: Any, *args: Any, **kwargs: Any) -> Any:
        if value == serialized:
            counters["source_json_loads"] += 1
            counters["source_json_loads_inside_writer"] += int(state["writer"])
        return loads(value, *args, **kwargs)

    def counted_save(db: Any, *args: Any, **kwargs: Any) -> None:
        assert db.in_transaction
        assert args[2]["parents"] == (parent.receipt_id,)
        counters["saves"] += 1
        save(db, *args, **kwargs)

    def forbidden(*_args: Any, **_kwargs: Any) -> None:
        pytest.fail("Compact summary preparation consumed Original values")

    with monkeypatch.context() as patch:
        patch.setattr(lake.inputs, "get", counted_get)
        patch.setattr(inputs.json, "loads", counted_load)
        patch.setattr(input_index, "save_projection", counted_save)
        patch.setattr(inputs, "read_batch", forbidden)
        patch.setattr(input_index, "project_original", forbidden)
        actual = lake.inputs.prepare_range_selections(root, scopes)
        assert counters["get_inside_writer"] == counters["source_json_loads_inside_writer"] == 1
        assert counters["source_json_loads"] == 6  # Five unchanged entry/closing boundary decodes.
        assert counters["saves"] == count
        counters.clear()
        repeated = lake.inputs.prepare_range_selections(root, scopes)
        assert counters["get_inside_writer"] == counters["source_json_loads_inside_writer"] == 1
        assert counters["source_json_loads"] == 6 and counters["saves"] == count
    assert actual == repeated == expected
    assert lake.inputs.get(root).digest == root.digest


def distinct_sources(path: Path) -> tuple[DataLake, FrozenInputReceipt, FrozenInputReceipt, tuple[InputSelectionScope, ...]]:
    lake = DataLake.open(data_meta_path=path / "meta.sqlite", lake_path=path / "lake")
    spec = DatasetSpec("quotes", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [DAY], "asset_id": ["A"], "close": [1.]})
    request = RawInput("custom", "quotes", view="snapshot")
    lake.raw.ingest(spec, frame, ingested_at=datetime(2024, 1, 1, tzinfo=UTC))
    original = lake.inputs.freeze({"raw": request}, information_cutoff="2024-01-02")
    first = InputSelectionScope("raw", start=DAY, end=DAY, view="snapshot", fields=("close",))
    lake.inputs.prepare_range_selections(original, [first])
    lake.raw.ingest(spec, frame.with_columns(pl.lit(DAY + timedelta(days=1)).alias("time")),
        ingested_at=datetime(2024, 1, 2, tzinfo=UTC))
    current = lake.inputs.freeze({"raw": request}, information_cutoff="2024-01-02")
    second = InputSelectionScope("raw", start=DAY + timedelta(days=1),
        end=DAY + timedelta(days=1), view="snapshot", fields=("close",))
    return lake, original, current, (first, second, first, second)


def test_each_distinct_authority_is_loaded_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lake, original, current, scopes = distinct_sources(tmp_path)
    expected = lake.inputs.selection_descriptors(current, scopes)
    assert {row["proof_source"]["id"] for row in expected} == {original.receipt_id, current.receipt_id}
    get = lake.inputs.get
    writer_gets: Counter[str] = Counter()

    def counted_get(value: FrozenInputReceipt | str) -> FrozenInputReceipt:
        caller = inspect.currentframe()
        assert caller is not None and caller.f_back is not None
        caller = caller.f_back
        db = caller.f_locals.get("db")
        if caller.f_code.co_name == "prepare_range_selections" and db is not None and db.in_transaction:
            assert isinstance(value, str)
            writer_gets[value] += 1
        return get(value)

    monkeypatch.setattr(lake.inputs, "get", counted_get)
    assert lake.inputs.prepare_range_selections(current, scopes) == expected
    assert writer_gets == {original.receipt_id: 1, current.receipt_id: 1}


def test_different_digest_is_not_a_memo_hit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lake, root, _, scopes = setup(tmp_path)
    descriptors = lake.inputs.selection_descriptors
    before = rows(lake)

    def mismatched(*args: Any, **kwargs: Any) -> tuple[dict[str, Any], ...]:
        result = deepcopy(descriptors(*args, **kwargs))
        result[1]["proof_source"]["digest"] = "0" * 64
        return result

    monkeypatch.setattr(lake.inputs, "selection_descriptors", mismatched)
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        lake.inputs.prepare_range_selections(root, scopes[:2])
    assert rows(lake) == before


def test_missing_registered_proof_fails_and_rolls_back(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lake, root, _, scopes = setup(tmp_path)
    descriptors = lake.inputs.selection_descriptors
    before = rows(lake)

    def missing(*args: Any, **kwargs: Any) -> tuple[dict[str, Any], ...]:
        result = deepcopy(descriptors(*args, **kwargs))
        result[1]["proof_source"]["id"] = "missing-proof-source"
        return result

    monkeypatch.setattr(lake.inputs, "selection_descriptors", missing)
    with pytest.raises(KeyError, match="Unknown frozen input receipt"):
        lake.inputs.prepare_range_selections(root, scopes[:2])
    assert rows(lake) == before


@pytest.mark.parametrize("failure", ["cancel", "write"])
def test_partial_writer_rollback_and_identical_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str,
) -> None:
    lake, root, _, scopes = setup(tmp_path)
    expected, before = lake.inputs.selection_descriptors(root, scopes), rows(lake)
    save = input_index.save_projection
    state = {"saved": 0}

    def checked_save(db: Any, *args: Any, **kwargs: Any) -> None:
        assert db.in_transaction
        save(db, *args, **kwargs)
        state["saved"] += 1
        if failure == "write" and state["saved"] == 3:
            raise RuntimeError("injected write failure")

    def check() -> None:
        if failure == "cancel" and state["saved"] == 3:
            raise RuntimeError("injected cancellation")

    with monkeypatch.context() as patch:
        patch.setattr(input_index, "save_projection", checked_save)
        with pytest.raises(RuntimeError, match="injected"):
            lake.inputs.prepare_range_selections(root, scopes, check_canceled=check)
    assert state["saved"] == 3 and rows(lake) == before
    assert lake.inputs.prepare_range_selections(root, scopes) == expected


def test_new_call_revalidates_old_registered_authority(tmp_path: Path) -> None:
    lake, original, current, scopes = distinct_sources(tmp_path)
    lake.inputs.prepare_range_selections(current, scopes)
    before = rows(lake)
    with lake._data_meta.connect() as db:
        db.execute("update frozen_inputs set payload_json=payload_json || ' ' where receipt_id=?",
            (original.receipt_id,))
    with pytest.raises(RuntimeError, match="checksum mismatch"):
        lake.inputs.prepare_range_selections(current, scopes)
    assert rows(lake) == before


def test_read_context_rejects_writer(tmp_path: Path) -> None:
    lake, root, _, scopes = setup(tmp_path)
    with lake.inputs.read_context(root):
        with pytest.raises(RuntimeError, match="independent writer"):
            lake.inputs.prepare_range_selections(root, scopes)
    assert not rows(lake)


def test_budget_rejection_publishes_nothing(tmp_path: Path) -> None:
    lake, root, _, scopes = setup(tmp_path)
    with pytest.raises(MemoryError, match="admission|metadata"):
        lake.inputs.prepare_range_selections(root, scopes,
            config=ExecutionOptions(max_buffer_bytes=8192))
    assert not rows(lake)


def test_closing_descriptor_recheck_is_retained(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lake, root, _, scopes = setup(tmp_path)
    descriptors = lake.inputs.selection_descriptors
    calls = []

    def checked(*args: Any, **kwargs: Any) -> tuple[dict[str, Any], ...]:
        result = descriptors(*args, **kwargs)
        calls.append(result)
        return result

    monkeypatch.setattr(lake.inputs, "selection_descriptors", checked)
    result = lake.inputs.prepare_range_selections(root, scopes)
    assert len(calls) == 2 and result == calls[1] == calls[0]


def test_one_authority_retains_distinct_alias_parent_tuples(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake, _, first_parent, scopes = setup(tmp_path)
    request = RawInput("custom", "quotes", alias="raw", view="snapshot")
    second_parent = lake.inputs.freeze({"raw": request}, information_cutoff="2024-01-16")
    lake.items.register(DataItemSpec("y", inputs=(request,)))
    lake.items.ingest("y", pl.DataFrame({"time": [DAY], "asset_id": ["A"], "value": [2.]}),
        input_receipt=second_parent, available_date="2024-01-16")
    root = lake.inputs.freeze({"x": ItemInput("x", view="snapshot"),
        "y": ItemInput("y", view="snapshot")}, information_cutoff="2024-01-16")
    second = InputSelectionScope("y", start=DAY, end=DAY, view="snapshot", fields=("value",))
    # Public descriptor admission still permits one alias per group.
    with pytest.raises(ValueError, match="one alias per group"):
        lake.inputs.prepare_range_selections(root, [scopes[0], second])
    first_descriptors = lake.inputs.selection_descriptors(root, [scopes[0]])
    second_descriptors = lake.inputs.selection_descriptors(root, [second])
    expected = (*first_descriptors, *second_descriptors)
    assert {row["proof_source"]["id"] for row in expected} == {root.receipt_id}
    parent_ids = {"x": (first_parent.receipt_id,), "y": (second_parent.receipt_id,)}
    get, save = lake.inputs.get, input_index.save_projection
    count = 0

    def counted_get(value: FrozenInputReceipt | str) -> FrozenInputReceipt:
        nonlocal count
        caller = inspect.currentframe()
        assert caller is not None and caller.f_back is not None
        caller = caller.f_back
        db = caller.f_locals.get("db")
        if caller.f_code.co_name == "prepare_range_selections" and db is not None and db.in_transaction:
            count += 1
        return get(value)

    def checked_save(db: Any, *args: Any, **kwargs: Any) -> None:
        assert args[2]["parents"] == parent_ids[kwargs["alias"]]
        save(db, *args, **kwargs)

    # Isolate the writer using genuine descriptors independently admitted by
    # the existing public one-alias API, without widening its admission rule.
    monkeypatch.setattr(lake.inputs, "selection_descriptors", lambda *_a, **_k: expected)
    monkeypatch.setattr(lake.inputs, "get", counted_get)
    monkeypatch.setattr(input_index, "save_projection", checked_save)
    assert lake.inputs.prepare_range_selections(root, [scopes[0], second]) == expected
    assert count == 1
