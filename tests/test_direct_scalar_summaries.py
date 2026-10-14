"""Exact scalar summaries retain original evidence without per-row persistence."""
from storage_fixtures import damage_original_segments


from datetime import date
import json
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataLake, DataItemSpec, ItemInput, ExecutionOptions
from bagelquant_data import input_index


def setup(tmp_path, *, strict=False, baseline=False, view="snapshot"):
    lake = DataLake.open(
        data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake"
    )
    lake.items.register(DataItemSpec("value"))
    frame = pl.DataFrame(
        {
            "time": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            "asset_id": ["A", "B", "C"],
            "value": [1.0, -0.0, None],
            "creator": [1, 1, 1],
        }
    )
    lake.items.ingest(
        "value", frame, available_date="2024-01-04", historical_baseline=baseline
    )
    frozen = lake.inputs.freeze(
        {"x": ItemInput("value", view=view, strict=strict)},
        information_cutoff="2024-01-10",
        retain_batch_bounds=True,
    )
    return lake, frame, frozen


def remove_rows(lake):
    with sqlite3.connect(lake.data_meta_path) as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_value_index'").fetchone() is None
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_index'").fetchone() is None


def coords():
    return pl.DataFrame(
        {"time": [date(2024, 1, 2), date(2024, 1, 1)], "asset_id": ["ABSENT", "A"]}
    )


def stored_row_counts(lake):
    with sqlite3.connect(lake.data_meta_path) as db:
        return {
            table: db.execute("select count(*) from sqlite_master where name=?", (table,)).fetchone()[0]
            for table in ("input_batch_value_index", "input_batch_index")
        }


@pytest.mark.parametrize("view", ["snapshot", "history", "versions"])
@pytest.mark.parametrize(
    "baseline,strict", [(False, False), (True, False), (True, True)]
)
def test_direct_original_is_identical_to_row_index_without_row_persistence(
    tmp_path, view, baseline, strict, monkeypatch
):
    lake, _, frozen = setup(tmp_path, view=view, baseline=baseline, strict=strict)
    causal = view == "versions"
    expected = lake.inputs.selection_identity(
        frozen, "x", values_only=True, causal_versions=causal
    )
    assert expected is not None
    remove_rows(lake)
    assert not hasattr(input_index, "read")
    actual = lake.inputs.index_selection(frozen, "x", causal_versions=causal)
    assert actual is not None
    assert stored_row_counts(lake) == {
        "input_batch_value_index": 0,
        "input_batch_index": 0,
    }
    monkeypatch.setattr(
        input_index,
        "project_original",
        lambda *a, **k: pytest.fail("Ordinary original read"),
    )
    assert (
        lake.inputs.selection_identity(
            frozen, "x", values_only=True, causal_versions=causal
        )
        == expected
    )
    assert lake.inputs.get(frozen).digest == frozen.digest


def test_coordinates_exact_absent_keys_and_deduplicated_audit(tmp_path, monkeypatch):
    lake, _, frozen = setup(tmp_path)
    selected = coords()
    expected = lake.inputs.selection_identity(
        frozen, "x", values_only=True, coordinates=selected
    )
    different = lake.inputs.selection_identity(
        frozen, "x", values_only=True, coordinates=selected.head(1)
    )
    assert expected != different
    remove_rows(lake)
    assert not hasattr(input_index, "read")
    assert lake.inputs.index_selection(frozen, 'x', coordinates=selected) is not None
    assert lake.inputs.index_selection(frozen, 'x', coordinates=selected.reverse()) is not None
    with sqlite3.connect(lake.data_meta_path) as db:
        assert (
            db.execute("select count(*) from input_projection_selectors").fetchone()[0]
            == 1
        )
    with monkeypatch.context() as patch:
        patch.setattr(
            input_index,
            "project_original",
            lambda *a, **k: pytest.fail("Ordinary original read"),
        )
        assert (
            lake.inputs.selection_identity(
                frozen, "x", values_only=True, coordinates=selected
            )
            == expected
        )
    assert lake.inputs.verify(frozen)["valid"]
    assert stored_row_counts(lake) == {
        "input_batch_value_index": 0,
        "input_batch_index": 0,
    }


def test_cutoff_advance_unrelated_commit_reuses_exact_summary(tmp_path, monkeypatch):
    lake, frame, frozen = setup(tmp_path)
    value = lake.inputs.index_selection(frozen, "x")
    later = lake.inputs.freeze(
        frozen.requests, information_cutoff="2024-01-20", retain_batch_bounds=True
    )
    lake.items.register(DataItemSpec("other"))
    lake.items.ingest("other", frame.drop("creator"), available_date="2024-01-05")
    remove_rows(lake)
    assert not hasattr(input_index, "read")
    monkeypatch.setattr(
        input_index,
        "project_original",
        lambda *a, **k: pytest.fail("Ordinary original read"),
    )
    assert lake.inputs.index_selection(later, "x") == value
    assert (
        lake.inputs.selection_equivalent(
            frozen, later, "x", values_only=True, summaries_only=True
        )
        is True
    )


def test_new_future_revision_requires_explicit_summary_and_preserves_old(
    tmp_path, monkeypatch
):
    lake, frame, old = setup(tmp_path)
    old_value = lake.inputs.index_selection(old, "x")
    lake.items.ingest(
        "value",
        frame.with_columns(pl.lit(9.0).alias("value")),
        available_date="2024-01-12",
    )
    new = lake.inputs.freeze(
        old.requests, information_cutoff="2024-01-20", retain_batch_bounds=True
    )
    expected = lake.inputs.selection_identity(new, "x", values_only=True)
    remove_rows(lake)
    assert lake.inputs.selection_identity(new, "x", values_only=True) is not None
    assert not hasattr(input_index, "read")
    assert lake.inputs.index_selection(new, "x") != old_value
    assert lake.inputs.selection_identity(new, "x", values_only=True) == expected
    assert lake.inputs.index_selection(old, "x") == old_value
    assert (
        lake.inputs.selection_equivalent(
            old, new, "x", values_only=True, summaries_only=True
        )
        is False
    )
    assert lake.inputs.verify((old, new))["valid"]


@pytest.mark.parametrize("coords_present", [False, True])
def test_empty_scope_retains_exact_semantics(tmp_path, coords_present, monkeypatch):
    lake, _, frozen = setup(tmp_path)
    selected = (
        pl.DataFrame({"time": [date(2024, 2, 1)], "asset_id": ["ABSENT"]})
        if coords_present
        else None
    )
    expected = lake.inputs.selection_identity(
        frozen,
        "x",
        start="2024-02-01",
        end="2024-02-02",
        values_only=True,
        coordinates=selected,
    )
    remove_rows(lake)
    assert not hasattr(input_index, "read")
    assert lake.inputs.index_selection(frozen, 'x', start='2024-02-01', end='2024-02-02', coordinates=selected) is not None
    assert lake.inputs.selection_identity(frozen, 'x', start='2024-02-01', end='2024-02-02', values_only=True, coordinates=selected) == expected
    assert lake.inputs.verify(frozen)["valid"]


@pytest.mark.parametrize(
    "damage", ["summary", "selector_header", "selector_payload", "missing_selector"]
)
def test_explicit_audit_replays_original_and_saved_selector(tmp_path, damage):
    lake, _, frozen = setup(tmp_path)
    selected = coords()
    lake.inputs.index_selection(frozen, "x", coordinates=selected)
    remove_rows(lake)
    with sqlite3.connect(lake.data_meta_path) as db:
        db.row_factory = sqlite3.Row
        if damage == "summary":
            row = dict(db.execute("select * from input_projection_index").fetchone())
            row["identity"] = "f" * 64
            db.execute(
                "update input_projection_index set identity=?,digest=?",
                (
                    row["identity"],
                    input_index._digest(
                        {k: v for k, v in row.items() if k != "digest"}
                    ),
                ),
            )
        elif damage == "selector_header":
            db.execute("update input_projection_selectors set digest='wrong'")
        elif damage == "selector_payload":
            db.execute(
                "update input_projection_selectors set payload=zeroblob(length(payload))"
            )
        else:
            db.execute("delete from input_projection_selectors")
    with pytest.raises(RuntimeError, match="summary differs|selector"):
        lake.inputs.verify(frozen)


def test_cancel_inside_metadata_publication_rolls_back_selector_and_summary(
    tmp_path, monkeypatch
):
    lake, _, frozen = setup(tmp_path)
    remove_rows(lake)
    original = input_index.save_projection
    canceled = [False]

    def save(*a, **k):
        original(*a, **k)
        canceled[0] = True

    class Canceled(RuntimeError):
        pass

    def check():
        if canceled[0]:
            raise Canceled("stop")

    monkeypatch.setattr(input_index, "save_projection", save)
    with pytest.raises(Canceled):
        lake.inputs.index_selection(
            frozen, "x", coordinates=coords(), check_canceled=check
        )
    with sqlite3.connect(lake.data_meta_path) as db:
        assert (
            db.execute("select count(*) from input_projection_index").fetchone()[0] == 0
        )
        assert (
            db.execute(
                "select 1 from sqlite_schema where name='input_projection_selectors'"
            ).fetchone()
            is None
        )
    assert lake.inputs.get(frozen).digest == frozen.digest


def test_tiny_budget_refuses_original_summary_before_any_positive_metadata(tmp_path):
    lake, _, frozen = setup(tmp_path)
    remove_rows(lake)
    with pytest.raises(MemoryError):
        lake.inputs.index_selection(
            frozen, "x", config=ExecutionOptions(max_buffer_bytes=128)
        )
    with sqlite3.connect(lake.data_meta_path) as db:
        assert (
            db.execute("select count(*) from input_projection_index").fetchone()[0] == 0
        )
    assert stored_row_counts(lake) == {
        "input_batch_value_index": 0,
        "input_batch_index": 0,
    }


def test_audit_self_consistent_wrong_selector_payload_has_wrong_canonical_id(tmp_path):
    lake, _, frozen = setup(tmp_path)
    lake.inputs.index_selection(frozen, "x", coordinates=coords())
    with sqlite3.connect(lake.data_meta_path) as db:
        db.row_factory = sqlite3.Row
        row = dict(db.execute("select * from input_projection_selectors").fetchone())
        meta = json.loads(row["metadata_json"])
        wrong = pl.DataFrame(
            {"time": [date(2024, 1, 1), date(2024, 1, 2)], "asset_id": ["B", "ABSENT"]}
        )
        import io
        import zlib
        import hashlib

        buffer = io.BytesIO()
        wrong.write_ipc(buffer)
        raw = buffer.getvalue()
        payload = len(raw).to_bytes(8, "big") + zlib.compress(raw, 3)
        meta.update(
            raw_bytes=len(raw),
            stored_bytes=len(payload),
            payload=hashlib.sha256(payload).hexdigest(),
        )
        db.execute(
            "update input_projection_selectors set payload=?,metadata_json=?,digest=?",
            (payload, json.dumps(meta), input_index._digest(meta)),
        )
    with pytest.raises(RuntimeError, match="canonical identity"):
        lake.inputs.verify(frozen)


@pytest.mark.parametrize("kind", ["read_context", "publication_guard"])
def test_maintenance_rejects_borrowed_transaction_before_original_reads(
    tmp_path, kind, monkeypatch
):
    lake, _, frozen = setup(tmp_path)
    monkeypatch.setattr(
        input_index,
        "project_original",
        lambda *a, **k: pytest.fail("Nested original read"),
    )
    context = (
        lake.inputs.read_context(frozen)
        if kind == "read_context"
        else lake.inputs.publication_guard([frozen])
    )
    with (
        context,
        pytest.raises(
            RuntimeError, match="outside a read/write context|own transaction"
        ),
    ):
        lake.inputs.index_selection(frozen, "x")


@pytest.mark.parametrize(
    "damage",
    ["negative_rows", "boolean_size", "schema", "bad_payload_digest", "extra_key"],
)
def test_self_consistent_invalid_selector_header_fails_closed(tmp_path, damage):
    lake, _, frozen = setup(tmp_path)
    lake.inputs.index_selection(frozen, "x", coordinates=coords())
    with sqlite3.connect(lake.data_meta_path) as db:
        row = db.execute(
            "select metadata_json from input_projection_selectors"
        ).fetchone()
        body = json.loads(row[0])
        if damage == "negative_rows":
            body["rows"] = -1
        elif damage == "boolean_size":
            body["raw_bytes"] = True
        elif damage == "schema":
            body["schema"]["time"] = "Datetime"
        elif damage == "bad_payload_digest":
            body["payload"] = "g" * 64
        else:
            body["unregistered"] = 1
        db.execute(
            "update input_projection_selectors set metadata_json=?,digest=?",
            (json.dumps(body), input_index._digest(body)),
        )
    with pytest.raises(RuntimeError, match="selector metadata"):
        lake.inputs.verify(frozen)


@pytest.mark.parametrize("bad", ["missing_column", "null", "outside", "wrong_dtype"])
def test_coordinate_admission_rejects_invalid_selector_without_original_reads(
    tmp_path, bad, monkeypatch
):
    lake, _, frozen = setup(tmp_path)
    bounded = lake.inputs.freeze(
        {"x": ItemInput("value", start="2024-01-01", end="2024-01-03")},
        information_cutoff="2024-01-10",
        retain_batch_bounds=True,
    )
    selected = coords()
    if bad == "missing_column":
        selected = selected.drop("asset_id")
    elif bad == "null":
        selected = selected.with_columns(
            pl.lit(None, dtype=pl.String).alias("asset_id")
        )
    elif bad == "outside":
        selected = selected.with_columns(pl.lit(date(2024, 2, 1)).alias("time"))
    else:
        selected = selected.with_columns(pl.col("time").cast(pl.Datetime))
    monkeypatch.setattr(
        input_index,
        "project_original",
        lambda *a, **k: pytest.fail("Invalid selector original read"),
    )
    with pytest.raises(ValueError):
        lake.inputs.index_selection(bounded, "x", coordinates=selected)
    assert lake.inputs.get(frozen).digest == frozen.digest


def test_duplicate_coordinates_rejected_and_cross_alias_selector_share_exact_identity(
    tmp_path,
):
    lake, _, frozen = setup(tmp_path)
    second = lake.inputs.freeze(
        {"other": ItemInput("value", view="snapshot")},
        information_cutoff="2024-01-10",
        retain_batch_bounds=True,
    )
    selected = coords()
    first_expected = lake.inputs.selection_identity(
        frozen, "x", values_only=True, coordinates=selected
    )
    second_expected = lake.inputs.selection_identity(
        second, "other", values_only=True, coordinates=selected
    )
    with pytest.raises(ValueError, match="unique"):
        lake.inputs.index_selection(
            frozen, "x", coordinates=pl.concat([selected, selected])
        )
    first_value = lake.inputs.index_selection(frozen, "x", coordinates=selected)
    assert first_value is not None
    assert lake.inputs.index_selection(second, 'other', coordinates=selected) == first_value
    assert lake.inputs.selection_identity(frozen, 'x', values_only=True, coordinates=selected) == first_expected
    assert lake.inputs.selection_identity(second, 'other', values_only=True, coordinates=selected) == second_expected
    with sqlite3.connect(lake.data_meta_path) as db:
        assert (
            db.execute("select count(*) from input_projection_selectors").fetchone()[0]
            == 1
        )
    remove_rows(lake)
    assert lake.inputs.verify((frozen, second))["valid"]


def test_cancel_during_original_transport_publishes_no_summary(tmp_path, monkeypatch):
    lake, _, frozen = setup(tmp_path)
    original = input_index.project_original
    calls = [0]

    class Canceled(RuntimeError):
        pass

    def project(*a, **k):
        calls[0] += 1
        original(*a, **k)
        raise Canceled("transport canceled")

    monkeypatch.setattr(input_index, "project_original", project)
    with pytest.raises(Canceled):
        lake.inputs.index_selection(frozen, "x")
    assert calls == [1]
    with sqlite3.connect(lake.data_meta_path) as db:
        assert (
            db.execute("select count(*) from input_projection_index").fetchone()[0] == 0
        )


@pytest.mark.parametrize(
    "view,causal", [("snapshot", False), ("history", False), ("versions", True)]
)
def test_late_revision_and_equal_availability_precedence_match_existing_selector(
    tmp_path, view, causal
):
    lake, frame, _ = setup(tmp_path)
    lake.items.ingest(
        "value",
        frame.with_columns(pl.lit(7.0).alias("value")),
        available_date="2024-01-04",
    )
    lake.items.ingest(
        "value",
        frame.with_columns(pl.lit(8.0).alias("value")),
        available_date="2024-01-12",
    )
    frozen = lake.inputs.freeze(
        {"x": ItemInput("value", view=view)},
        information_cutoff="2024-01-10",
        retain_batch_bounds=True,
    )
    expected = lake.inputs.selection_identity(
        frozen, "x", values_only=True, causal_versions=causal
    )
    remove_rows(lake)
    assert lake.inputs.index_selection(frozen, 'x', causal_versions=causal) is not None
    assert lake.inputs.selection_identity(frozen, 'x', values_only=True, causal_versions=causal) == expected
    assert lake.inputs.verify(frozen)["valid"]


def test_summary_original_audit_propagates_cancellation_during_projection(
    tmp_path, monkeypatch
):
    lake, _, frozen = setup(tmp_path)
    lake.inputs.index_selection(frozen, "x", coordinates=coords())
    remove_rows(lake)

    class Canceled(RuntimeError):
        pass

    canceled = [False]

    def check():
        if canceled[0]:
            raise Canceled("audit canceled")

    original = input_index.project_original

    def project(*a, **k):
        assert k["check_canceled"] is check
        canceled[0] = True
        return original(*a, **k)

    monkeypatch.setattr(input_index, "project_original", project)
    with pytest.raises(Canceled):
        lake.inputs.verify(frozen, check_canceled=check)
    assert lake.inputs.get(frozen).digest == frozen.digest


def test_direct_summary_rejects_original_byte_corruption_without_saving(tmp_path):
    lake, _, frozen = setup(tmp_path)
    remove_rows(lake)
    with sqlite3.connect(lake.data_meta_path) as db:
        damage_original_segments(db, '', ())
    with pytest.raises(RuntimeError):
        lake.inputs.index_selection(frozen, "x")
    with sqlite3.connect(lake.data_meta_path) as db:
        assert (
            db.execute("select count(*) from input_projection_index").fetchone()[0] == 0
        )


def test_missing_summary_remains_unknown_and_original_reads_forbidden_until_maintenance(
    tmp_path, monkeypatch
):
    lake, _, frozen = setup(tmp_path)
    remove_rows(lake)
    monkeypatch.setattr(
        input_index,
        "project_original",
        lambda *a, **k: pytest.fail("Implicit original summary build"),
    )
    monkeypatch.setattr(
        lake.inputs,
        "index_selection",
        lambda *a, **k: pytest.fail("Implicit summary maintenance"),
    )
    assert (
        lake.inputs.verify(frozen)
        is not None
    )


def test_original_source_checksum_fenced_after_explicit_projection_before_summary_save(
    tmp_path, monkeypatch
):
    lake, _, frozen = setup(tmp_path)
    original = input_index.project_original

    def project(*a, **k):
        value = original(*a, **k)
        with sqlite3.connect(lake.data_meta_path) as db:
            db.execute(
                "update frozen_inputs set payload_json='{}' where receipt_id=?",
                (frozen.receipt_id,),
            )
        return value

    monkeypatch.setattr(input_index, "project_original", project)
    with pytest.raises(RuntimeError, match="receipt checksum mismatch"):
        lake.inputs.index_selection(frozen, "x", coordinates=coords())
    with sqlite3.connect(lake.data_meta_path) as db:
        assert (
            db.execute("select count(*) from input_projection_index").fetchone()[0] == 0
        )
        assert (
            db.execute(
                "select 1 from sqlite_schema where name='input_projection_selectors'"
            ).fetchone()
            is None
        )


@pytest.mark.parametrize("path", ["first", "saved_explicit", "saved_normal"])
def test_source_json_header_admitted_before_first_or_saved_source_decode(
    tmp_path, path, monkeypatch
):
    lake, _, original = setup(tmp_path)
    lake.items.register(DataItemSpec("wide", metadata={"note": "q" * 65536}))
    wide = lake.inputs.freeze(
        {"x": ItemInput("value", view="snapshot"), "wide": ItemInput("wide")},
        information_cutoff="2024-01-10",
        retain_batch_bounds=True,
    )
    options = ExecutionOptions(max_buffer_bytes=512 * 1024)
    if path != "first":
        assert lake.inputs.index_selection(wide, "x") is not None
    old_get = lake.inputs._get

    def get(receipt):
        key = getattr(receipt, "receipt_id", receipt)
        if key == wide.receipt_id:
            pytest.fail("Wide original Source JSON fetched before budget admission")
        return old_get(receipt)

    monkeypatch.setattr(lake.inputs, "_get", get)
    if path == "saved_normal":
        # Ordinary range identity never borrows optional exact summary Sources.
        expected = lake.inputs.selection_identity(original, "x", values_only=True)
        with lake.inputs.read_context(original, config=options):
            assert lake.inputs.selection_identity(original, "x", values_only=True) == expected
        return
    with pytest.raises(MemoryError, match="source metadata"):
        if path == "first":
            lake.inputs.index_selection(wide, "x", config=options)
        elif path == "saved_explicit":
            lake.inputs.index_selection(original, "x", config=options)


@pytest.mark.parametrize('damage', ['empty_asset', 'outside_window'])
def test_audit_revalidates_self_consistent_selector_against_public_coordinate_contract(tmp_path, damage):
    lake, _, _ = setup(tmp_path)
    frozen = lake.inputs.freeze({'x': ItemInput('value', start='2024-01-01', end='2024-01-03', view='snapshot')},
                                information_cutoff='2024-01-10', retain_batch_bounds=True)
    selected = pl.DataFrame({'time': [date(2024, 1, 1)], 'asset_id': ['A']})
    lake.inputs.index_selection(frozen, 'x', coordinates=selected)
    invalid = selected.with_columns(pl.lit('').alias('asset_id')) if damage == 'empty_asset' else selected.with_columns(pl.lit(date(2024, 2, 1)).alias('time'))
    with sqlite3.connect(lake.data_meta_path) as db:
        db.row_factory = sqlite3.Row
        row = dict(db.execute('select * from input_projection_index').fetchone())
        key = json.loads(row['request_key'])
        key['coordinates'] = input_index.identity(invalid)
        cutoff = date.fromisoformat(key['cutoff'])
        with lake.inputs.read_context(frozen):
            forged = lake.inputs._indexed_selection(frozen, 'x', request=frozen.requests['x'], cutoff=cutoff,
                                                    values_only=True, coordinates=invalid, recompute=True, originals=True)
        db.execute('delete from input_projection_index')
        input_index.save_selector(db, invalid, max_bytes=16 * 1024 * 1024)
        input_index.save_projection(db, row['evidence_key'], json.dumps(key, sort_keys=True, separators=(',', ':')), forged,
                                    receipt_id=frozen.receipt_id, receipt_digest=frozen.digest, alias='x')
    assert lake.inputs.get(frozen).digest == frozen.digest
    with pytest.raises((ValueError, RuntimeError), match='selector|coordinates'):
        lake.inputs.verify(frozen)
