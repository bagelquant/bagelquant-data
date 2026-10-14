"""Finite bulk summaries preserve original canonical selection and resource limits."""
from datetime import date, timedelta
import sqlite3
from collections.abc import Sequence
from typing import cast
import polars as pl
import pytest
from bagelquant_data import (
    DataLake,
    DataItemSpec,
    ItemInput,
    InputSelectionScope,
    ExecutionOptions,
)
from bagelquant_data import input_index


def setup(tmp_path, *, view="snapshot", strict=False, baseline=False, count=40):
    lake = DataLake.open(
        data_meta_path=tmp_path / "m.sqlite", lake_path=tmp_path / "lake"
    )
    lake.items.register(DataItemSpec("x"))
    days = [date(2024, 1, 1) + timedelta(days=i) for i in range(count)]
    frame = pl.DataFrame(
        {
            "time": days,
            "asset_id": ["A"] * count,
            "value": [float(i) if i % 7 else -0.0 for i in range(count)],
        }
    )
    lake.items.ingest("x", frame, available_date=days[-1], historical_baseline=baseline)
    root = lake.inputs.freeze(
        {"x": ItemInput("x", view=view, strict=strict)},
        information_cutoff=days[-1] + timedelta(days=2),
        retain_batch_bounds=True,
    )
    return lake, root, days


def remove(lake):
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_index'").fetchone() is None
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_value_index'").fetchone() is None


def scope(days, start, end, **kw):
    return InputSelectionScope("x", start=days[start], end=days[end], **kw)


@pytest.mark.parametrize(
    "view,causal", [("snapshot", False), ("history", False), ("versions", True)]
)
@pytest.mark.parametrize(
    "baseline,strict", [(False, False), (True, False), (True, True)]
)
def test_bulk_single_and_indexed_canonical_exact(
    tmp_path, view, causal, baseline, strict
):
    lake, root, days = setup(tmp_path, view=view, strict=strict, baseline=baseline)
    scopes = [
        scope(days, 0, 31, causal_versions=causal),
        scope(days, 30, 39, causal_versions=causal),
    ]
    expected = tuple(
        lake.inputs.selection_identity(
            root,
            "x",
            start=s.start,
            end=s.end,
            values_only=True,
            causal_versions=causal,
        )
        for s in scopes
    )
    remove(lake)
    before = lake.inputs.get(root).digest
    got = lake.inputs.index_selections(root, scopes)
    assert all(token is not None for token in got)
    assert tuple(lake.inputs.selection_identity(root, "x", start=scope.start, end=scope.end, values_only=True, causal_versions=causal) for scope in scopes) == expected
    assert (
        lake.inputs.index_selection(
            root, "x", start=days[0], end=days[31], causal_versions=causal
        )
        == got[0]
    )
    assert lake.inputs.verify(root)["valid"]
    assert lake.inputs.get(root).digest == before
    with sqlite3.connect(lake.data_meta_path) as db:
        assert (
            db.execute("select count(*) from sqlite_master where name='input_batch_value_index'").fetchone()[0]
            == 0
        )
        assert db.execute("select count(*) from sqlite_master where name='input_batch_index'").fetchone()[0] == 0


def test_overlap_reads_each_original_once_and_hashes_scalar_once_per_chunk(
    tmp_path, monkeypatch
):
    lake, root, days = setup(tmp_path)
    scopes = [scope(days, 0, 31), scope(days, 20, 35), scope(days, 30, 39)]
    expected = [
        lake.inputs.selection_identity(
            root, "x", start=s.start, end=s.end, values_only=True
        )
        for s in scopes
    ]
    remove(lake)
    original = input_index.project_original
    seen = []

    def project(*a, **kw):
        seen.append((a[1]["commit_seq"], a[1]["partition_path"]))
        return original(*a, **kw)

    from bagelquant_data.pipeline import versions

    old_hash = versions._payload_hashes
    hashes = []

    def hashed(frame, fields):
        # Natural-key and whole-record hashes are ephemeral audit work now.
        # Count the scalar summary pass to prove overlapping windows share it.
        if fields == ["value"]:
            hashes.append(frame.height)
        return old_hash(frame, fields)

    monkeypatch.setattr(input_index, "project_original", project)
    monkeypatch.setattr(versions, "_payload_hashes", hashed)
    progress = []
    got = lake.inputs.index_selections(
        root, scopes, progress=progress.append
    )
    assert len(got) == len(expected) and all(got)
    assert len(seen) == len(set(seen)) == len(root.evidence["x"]["batches"])
    assert progress[-1]["hash_chunks"] == len(hashes) and progress[-1][
        "hash_rows"
    ] == sum(hashes)
    assert sum(hashes) == 40  # Overlaps do not hash a row three times.


def test_selector_reference_dedup_absent_keys_and_independent_alias_group(tmp_path):
    lake, root, days = setup(tmp_path)
    coords = pl.DataFrame({"time": [days[0], days[2]], "asset_id": ["A", "ABSENT"]})
    first = lake.inputs.index_selection(
        root, "x", start=days[0], end=days[10], coordinates=coords
    )
    selector = input_index.identity(coords.sort("time", "asset_id"))
    groups = [
        scope(days, 0, 10, coordinate_selector=selector),
        scope(days, 0, 10, coordinates=coords),
    ]
    assert lake.inputs.index_selections(root, groups) == (first, first)
    lake.items.register(DataItemSpec("y"))
    lake.items.ingest(
        "y",
        pl.DataFrame({"time": [days[0]], "asset_id": ["A"], "value": [7.0]}),
        available_date=days[-1],
    )
    second = lake.inputs.freeze(
        {"y": ItemInput("y", view="snapshot")},
        information_cutoff=days[-1] + timedelta(days=2),
        retain_batch_bounds=True,
    )
    token = lake.inputs.index_selections(
        second,
        [
            InputSelectionScope(
                "y", start=days[0], end=days[10], coordinate_selector=selector
            )
        ],
    )[0]
    assert token is not None
    with sqlite3.connect(lake.data_meta_path) as db:
        assert (
            db.execute("select count(*) from input_projection_selectors").fetchone()[0]
            == 1
        )
    assert lake.inputs.verify(root)["valid"] and lake.inputs.verify(second)["valid"]


def test_late_revision_visible_cutoff_null_and_signed_zero(tmp_path):
    lake, old, days = setup(tmp_path)
    lake.items.ingest(
        "x",
        pl.DataFrame(
            {
                "time": [days[0], days[1], days[2]],
                "asset_id": ["A"] * 3,
                "value": [0.0, None, float("nan")],
            }
        ),
        available_date=days[-1] + timedelta(days=1),
    )
    new = lake.inputs.freeze(
        {"x": ItemInput("x", view="snapshot")},
        information_cutoff=days[-1] + timedelta(days=2),
        retain_batch_bounds=True,
    )
    scopes = [
        scope(days, 0, 5, as_of=days[-1]),
        scope(days, 0, 5, as_of=days[-1] + timedelta(days=2)),
    ]
    expected = tuple(
        lake.inputs.selection_identity(
            new, "x", start=s.start, end=s.end, as_of=s.as_of, values_only=True
        )
        for s in scopes
    )
    assert expected[0] != expected[1]
    remove(lake)
    assert all(token is not None for token in lake.inputs.index_selections(new, scopes))
    assert lake.inputs.verify(new)["valid"]
    assert (
        lake.inputs.selection_identity(old, "x", values_only=True, start=days[0], end=days[5]) == expected[0]
    )


@pytest.mark.parametrize(
    "bad", ["mixed_alias", "generator", "too_many", "null", "missing_selector", "both"]
)
def test_admission_blocks_before_original_read(tmp_path, monkeypatch, bad):
    lake, root, days = setup(tmp_path)

    def forbidden(*_a, **_k):
        pytest.fail("unadmitted original read")

    monkeypatch.setattr(input_index, "project_original", forbidden)
    scopes = [scope(days, 0, 4)]
    if bad == "mixed_alias":
        scopes.append(InputSelectionScope("foreign"))
    elif bad == "generator":
        scopes = iter(scopes)
    elif bad == "too_many":
        scopes = scopes * 17
    elif bad == "null":
        scopes = [
            scope(
                days,
                0,
                4,
                coordinates=pl.DataFrame(
                    {"time": [days[0]], "asset_id": [None]},
                    schema={"time": pl.Date, "asset_id": pl.String},
                ),
            )
        ]
    elif bad == "missing_selector":
        scopes = [scope(days, 0, 4, coordinate_selector="missing")]
    else:
        scopes = [
            scope(
                days,
                0,
                4,
                coordinate_selector="existing",
                coordinates=pl.DataFrame({"time": [days[0]], "asset_id": ["A"]}),
            )
        ]
    with pytest.raises((ValueError, RuntimeError, TypeError)):
        lake.inputs.index_selections(root, cast(Sequence[InputSelectionScope], scopes))


def test_aggregate_budget_and_cancel_preserve_previous_complete_scope(
    tmp_path, monkeypatch
):
    lake, root, days = setup(tmp_path)
    prior = lake.inputs.index_selection(root, "x", start=days[0], end=days[5])
    large = pl.DataFrame(
        {"time": [days[0]] * 5000, "asset_id": [str(i) for i in range(5000)]}
    )

    def forbidden(*a, **k):
        pytest.fail("budget original read")

    with monkeypatch.context() as patch:
        patch.setattr(input_index, "project_original", forbidden)
        with pytest.raises(MemoryError, match="Aggregate"):
            lake.inputs.index_selections(
                root,
                [scope(days, 0, 5, coordinates=large)] * 16,
                config=ExecutionOptions(max_buffer_bytes=1024 * 1024),
            )
    original = input_index.save_projection
    cancel = [False]

    def saved(*a, **k):
        original(*a, **k)
        cancel[0] = True

    def check():
        if cancel[0]:
            raise RuntimeError("cancel writer")

    monkeypatch.setattr(input_index, "save_projection", saved)
    with pytest.raises(RuntimeError, match="cancel writer"):
        lake.inputs.index_selections(
            root, [scope(days, 10, 15), scope(days, 20, 25)], check_canceled=check
        )
    assert (
        lake.inputs.index_selection(
            root, "x", start=days[0], end=days[5]
        )
        == prior
    )
    with sqlite3.connect(lake.data_meta_path) as db:
        assert (
            db.execute("select count(*) from input_projection_index").fetchone()[0] == 1
        )
    cancel[0] = False
    monkeypatch.setattr(input_index, "save_projection", original)
    assert all(
        lake.inputs.index_selections(root, [scope(days, 10, 15), scope(days, 20, 25)])
    )




@pytest.mark.parametrize(
    "dtype,values",
    [
        (pl.String, ["é", None, ""]),
        (pl.Int64, [1, None, -2]),
        (pl.Boolean, [True, None, False]),
    ],
)
def test_typed_values_preserve_canonical_scalar_tokens(tmp_path, dtype, values):
    lake = DataLake.open(
        data_meta_path=tmp_path / "m.sqlite", lake_path=tmp_path / "lake"
    )
    lake.items.register(DataItemSpec("typed", value_dtype=dtype))
    days = [date(2024, 1, 1) + timedelta(days=i) for i in range(3)]
    lake.items.ingest(
        "typed",
        pl.DataFrame(
            {"time": days, "asset_id": ["A"] * 3, "value": values},
            schema={"time": pl.Date, "asset_id": pl.String, "value": dtype},
        ),
        available_date=days[-1],
    )
    root = lake.inputs.freeze(
        {"x": ItemInput("typed", view="snapshot")},
        information_cutoff=days[-1],
        retain_batch_bounds=True,
    )
    expected = lake.inputs.selection_identity(root, "x", values_only=True)
    remove(lake)
    assert lake.inputs.index_selections(root, [InputSelectionScope('x')])[0] is not None
    assert lake.inputs.selection_identity(root, 'x', values_only=True) == expected
    assert lake.inputs.verify(root)["valid"]


def test_single_fits_but_scope_union_is_rejected_and_prefix_preserved(tmp_path):
    lake, root, days = setup(tmp_path, count=1000)
    options = ExecutionOptions(max_buffer_bytes=1024 * 1024)
    prior = lake.inputs.index_selection(
        root, "x", start=days[0], end=days[31], config=options
    )
    with pytest.raises(MemoryError):
        lake.inputs.index_selections(
            root, [scope(days, 32 * i, 32 * i + 31) for i in range(16)], config=options
        )
    assert (
        lake.inputs.index_selection(
            root, "x", start=days[0], end=days[31]
        )
        == prior
    )
    with sqlite3.connect(lake.data_meta_path) as db:
        assert (
            db.execute("select count(*) from input_projection_index").fetchone()[0] == 1
        )


def test_summary_and_selector_self_consistent_forgery_fails_original_audit(tmp_path):
    lake, root, days = setup(tmp_path)
    coords = pl.DataFrame({"time": [days[0]], "asset_id": ["A"]})
    lake.inputs.index_selections(root, [scope(days, 0, 5, coordinates=coords)])
    remove(lake)
    with sqlite3.connect(lake.data_meta_path) as db:
        db.row_factory = sqlite3.Row
        body = dict(db.execute("select * from input_projection_index").fetchone())
        body["identity"] = "f" * 64
        body.pop("digest")
        db.execute(
            "update input_projection_index set identity=?,digest=?",
            (body["identity"], input_index._digest(body)),
        )
    with pytest.raises(RuntimeError, match="differs from original"):
        lake.inputs.verify(root)


def test_public_selector_identity_no_io_and_normalized(tmp_path, monkeypatch):
    lake, root, days = setup(tmp_path)
    coords = pl.DataFrame({"time": [days[2], days[0]], "asset_id": ["ABSENT", "A"]})
    expected = input_index.identity(coords.sort("time", "asset_id"))
    with monkeypatch.context() as patch:
        patch.setattr(
            input_index,
            "project_original",
            lambda *a, **kw: pytest.fail("identity read originals"),
        )
        patch.setattr(
            input_index,
            "save_selector",
            lambda *a, **kw: pytest.fail("identity registered selector"),
        )
        assert lake.inputs.coordinate_selector_identity(coords) == expected
        with pytest.raises(MemoryError):
            lake.inputs.coordinate_selector_identity(
                coords, config=ExecutionOptions(max_buffer_bytes=64)
            )
        with pytest.raises(ValueError):
            lake.inputs.coordinate_selector_identity(pl.concat([coords, coords]))
        with pytest.raises(RuntimeError, match="canceled"):
            lake.inputs.coordinate_selector_identity(
                coords,
                check_canceled=lambda: (_ for _ in ()).throw(RuntimeError("canceled")),
            )
    token = lake.inputs.index_selection(
        root, "x", start=days[0], end=days[10], coordinates=coords
    )
    assert lake.inputs.index_selections(
        root, [scope(days, 0, 10, coordinate_selector=expected)]
    ) == (token,)


def test_repeated_selector_normalized_frames_have_aggregate_admission(
    tmp_path, monkeypatch
):
    lake, root, days = setup(tmp_path)
    coords = pl.DataFrame(
        {
            "time": [days[0]] * 20000,
            "asset_id": [f"ABSENT_{i:08d}" for i in range(20000)],
        }
    )
    selector = lake.inputs.coordinate_selector_identity(coords)
    lake.inputs.index_selection(
        root, "x", start=days[0], end=days[30], coordinates=coords
    )
    monkeypatch.setattr(
        input_index,
        "project_original",
        lambda *a, **kw: pytest.fail(
            "aggregate guard must precede original projection"
        ),
    )
    scopes = [scope(days, 0, 5 + i, coordinate_selector=selector) for i in range(16)]
    with pytest.raises(MemoryError, match="Aggregate normalized selector admission"):
        lake.inputs.index_selections(
            root, scopes, config=ExecutionOptions(max_buffer_bytes=32 * 1024 * 1024)
        )
