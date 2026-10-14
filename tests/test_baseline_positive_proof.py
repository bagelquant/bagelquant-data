"""Positive timing proof and conservative fallback in isolated fake lakes."""
from storage_fixtures import damage_original_segments


import copy
from dataclasses import replace
from datetime import UTC, date, datetime
import polars as pl
import pytest
from bagelquant_data import (
    DataLake,
    DatasetSpec,
    DataItemSpec,
    RawInput,
    ItemInput,
    ItemPublication,
)


def proven(frozen, alias, cutoff, **kwargs):
    from bagelquant_data.inputs import InputsAPI

    return InputsAPI._proven_baseline_at(frozen, alias, cutoff, **kwargs)


def lake(root):
    return DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")


def spec(kind="by_date"):
    return DatasetSpec(
        "raw",
        kind,
        date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"},
    )


def rows(days=(1, 15), values=(1.0, 2.0)):
    return pl.DataFrame(
        {
            "time": [date(2020, 1, d) for d in days],
            "asset_id": ["A"] * len(days),
            "value": list(values),
        }
    )


def scoped(lake, **kwargs):
    return lake.inputs.freeze(
        {
            "raw": RawInput(
                "custom",
                "raw",
                start="2020-01-01",
                end="2020-01-31",
                view="snapshot",
                **kwargs,
            )
        },
        information_cutoff="2020-01-31",
    )


def test_metadata_true_matches_uniform_commit_and_keeps_bytes(tmp_path, monkeypatch):
    with lake(tmp_path) as data:
        data.raw.ingest(
            spec(),
            rows(),
            mode="initialize",
            ingested_at=datetime(2020, 1, 31, tzinfo=UTC),
        )
        r = scoped(data)
        assert (
            proven(r, "raw", date(2020, 1, 5)) is True
            and data.inputs._baseline_at(r, "raw", date(2020, 1, 5)) is True
        )
        assert proven(r, "raw", date(2019, 12, 31)) is None
        assert proven(r, "raw", date(2020, 2, 1)) is True
        assert proven(r, "raw", date(2020, 1, 5), max_commit=0) is None
        before = copy.deepcopy(r.evidence)
        assert data.inputs.verify(r)["valid"] and r.evidence == before
        import sqlite3

        with sqlite3.connect(data.data_meta_path) as db:
            damage_original_segments(db, '', ())
        assert (
            proven(r, "raw", date(2020, 1, 5)) is True
        )  # metadata is not an integrity check
        with pytest.raises(RuntimeError):
            data.inputs.verify(r)


def test_late_inline_attestation_blocks_fast_path_and_can_reverse_true(tmp_path):
    with lake(tmp_path) as data:
        data.raw.ingest(
            spec(),
            rows((1,), (1.0,)),
            mode="initialize",
            ingested_at=datetime(2020, 1, 1, tzinfo=UTC),
        )
        data.raw.ingest(
            spec(), rows((1,), (1.0,)), ingested_at=datetime(2020, 1, 15, tzinfo=UTC)
        )
        r = scoped(data)
        assert (
            proven(r, "raw", date(2020, 1, 10)) is True
            and data.inputs._baseline_at(r, "raw", date(2020, 1, 10)) is True
        )
        assert (
            proven(r, "raw", date(2020, 1, 20)) is None
            and data.inputs._baseline_at(r, "raw", date(2020, 1, 20)) is False
        )


def test_full_commit_witness_eligibility_blocks(tmp_path, monkeypatch):
    from bagelquant_data.storage import full_commit_checks as seals

    with lake(tmp_path) as data:
        frame = rows()
        data.raw.ingest(
            spec(),
            frame,
            mode="initialize",
            ingested_at=datetime(2020, 1, 1, tzinfo=UTC),
        )
        data.raw.ingest(spec(), frame, ingested_at=datetime(2020, 1, 20, tzinfo=UTC))
        monkeypatch.setattr(seals, "COMPACT_MIN_ROWS", 1)
        assert not hasattr(data.inputs, "index_checks")
        r = scoped(data)
        assert r.evidence["raw"]["record_checks"]
        assert not r.evidence["raw"].get("full_commit_checks")
        assert (
            proven(r, "raw", date(2020, 1, 10)) is True
            and data.inputs._baseline_at(r, "raw", date(2020, 1, 10)) is True
        )
        assert (
            proven(r, "raw", date(2020, 1, 25)) is None
            and data.inputs._baseline_at(r, "raw", date(2020, 1, 25)) is False
        )


def test_mixed_flags_future_revision_and_commit_ceiling(tmp_path):
    with lake(tmp_path) as data:
        data.raw.ingest(
            spec(),
            rows((1,), (1.0,)),
            mode="initialize",
            ingested_at=datetime(2020, 1, 1, tzinfo=UTC),
        )
        data.raw.ingest(
            spec(), rows((1,), (2.0,)), ingested_at=datetime(2020, 1, 20, tzinfo=UTC)
        )
        r = scoped(data)
        assert (
            proven(r, "raw", date(2020, 1, 10)) is True
        )  # registered availability excludes the future revision from this proof
        assert data.inputs._baseline_at(r, "raw", date(2020, 1, 10)) is True
        assert proven(r, "raw", date(2020, 1, 10), max_commit=1) is True
        assert data.inputs._baseline_at(r, "raw", date(2020, 1, 25)) is False


def test_coordinate_overlap_counterexample_requires_entire_containment(tmp_path):
    with lake(tmp_path) as data:
        data.raw.ingest(
            spec(),
            rows(),
            mode="initialize",
            ingested_at=datetime(2020, 1, 1, tzinfo=UTC),
        )
        r = data.inputs.freeze(
            {
                "raw": RawInput(
                    "custom",
                    "raw",
                    start="2020-01-10",
                    end="2020-01-31",
                    view="snapshot",
                )
            },
            information_cutoff="2020-01-31",
        )
        assert r.evidence["raw"]["batches"][0]["min_available"] == "2020-01-01"
        assert proven(r, "raw", date(2020, 1, 5)) is None
        assert data.inputs._baseline_at(r, "raw", date(2020, 1, 5)) is False
        # A naive positive row-count/min-availability + overlapping bounds would incorrectly return True.


def test_history_strict_legacy_and_unknown_fallback(tmp_path):
    with lake(tmp_path) as data:
        data.items.register(DataItemSpec("item"))
        data.items.ingest(
            "item",
            rows((1,), (1.0,)),
            available_date="2020-01-10",
            historical_baseline=True,
        )
        for view, strict in [
            ("history", False),
            ("snapshot", True),
            ("snapshot", False),
        ]:
            r = data.inputs.freeze(
                {
                    "item": ItemInput(
                        "item",
                        start="2020-01-01",
                        end="2020-01-31",
                        view=view,
                        strict=strict,
                    )
                },
                information_cutoff="2020-01-31",
            )
            assert proven(r, "item", date(2020, 1, 20)) is (
                True if view == "snapshot" and not strict else None
            )
            assert data.inputs._baseline_at(r, "item", date(2020, 1, 20)) == (
                view == "snapshot" and not strict
            )
            if view == "snapshot" and not strict:
                for key in ["scoped_batches", "min_observation", "max_observation"]:
                    ev = {
                        key: dict(copy.deepcopy(value))
                        for key, value in r.evidence.items()
                    }
                    if key == "scoped_batches":
                        ev["item"].pop(key)
                    else:
                        ev["item"]["batches"][0][key] = None
                    assert (
                        proven(replace(r, evidence=ev), "item", date(2020, 1, 20))
                        is None
                    )


def test_general_header_positive_proof_and_empty_parent_fallback(tmp_path):
    with lake(tmp_path) as data:
        data.raw.ingest(
            spec("general"),
            rows((1,), (1.0,)),
            mode="initialize",
            ingested_at=datetime(2020, 1, 20, tzinfo=UTC),
        )
        raw = RawInput(
            "custom", "raw", view="snapshot", include_historical_baseline=True
        )
        r = data.inputs.freeze({"raw": raw}, information_cutoff="2020-01-31")
        assert (
            proven(r, "raw", date(2020, 1, 5)) is True
            and data.inputs._baseline_at(r, "raw", date(2020, 1, 5)) is True
        )
        data.items.register(DataItemSpec("empty", (raw,)))
        data.items.ingest(
            "empty",
            rows((1,), (1.0,)).head(0),
            available_date="2020-01-31",
            input_receipt=r,
        )
        e = data.inputs.freeze(
            {
                "empty": ItemInput(
                    "empty", start="2000-01-01", end="2026-10-07", view="snapshot"
                )
            },
            information_cutoff="2020-01-31",
        )
        assert proven(e, "empty", date(2020, 1, 25)) is None


def test_publication_proves_timing_without_frames_but_rechecks_future_bytes(
    tmp_path, monkeypatch
):
    import sqlite3

    with lake(tmp_path) as data:
        frame = pl.DataFrame(
            {
                "time": [date(2020, 1, 1), date(2020, 2, 1)],
                "asset_id": ["A", "A"],
                "value": [1.0, 2.0],
            }
        )
        data.raw.ingest(
            spec(),
            frame,
            mode="initialize",
            ingested_at=datetime(2020, 2, 1, tzinfo=UTC),
        )
        request = RawInput(
            "custom",
            "raw",
            alias="raw",
            start="2020-01-01",
            end="2020-02-29",
            view="versions",
        )
        receipt = data.inputs.freeze({"raw": request}, information_cutoff="2020-02-29")
        data.items.register(DataItemSpec("output", (request,)))
        full = data.inputs._read_frame(receipt, "raw")
        assert (
            data.inputs._baseline_at(receipt, "raw", date(2020, 1, 5), frame=full.head(0))
            is False
        )

        def unexpected(*args, **kwargs):
            raise AssertionError("Proven timing must not load source frames")

        monkeypatch.setattr(data.inputs, "_read_frame", unexpected)
        publication = ItemPublication(
            "output",
            frame.head(1).with_columns(
                pl.lit(date(2020, 1, 5)).alias("version_available_date")
            ),
        )
        with data.items.publication(input_receipt=receipt) as operation:
            operation.publish([publication])
        assert data.items.read("output", view="versions").collect()[
            "_baseline"
        ].to_list() == [True]
        with sqlite3.connect(data.data_meta_path) as db:
            damage_original_segments(db, "partition_path like '%month=02%'", ())
        with data.items.publication(input_receipt=receipt) as operation:
            operation.publish([publication])
        with pytest.raises(RuntimeError):
            data.inputs.verify(receipt)
        assert not data.integrity.active_update_leases()


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("empty_general", [False, True])
def test_publication_checks_all_header_proofs_before_unknown_timing(tmp_path, monkeypatch, reverse, empty_general):
    """One complete General baseline proves the OR, regardless of alias order."""
    import sqlite3

    data = lake(tmp_path)
    daily = spec()
    data.raw.ingest(daily, rows((1,), (1.0,)), mode="initialize",
                    ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    data.raw.ingest(daily, rows((1,), (1.0,)),
                    ingested_at=datetime(2020, 1, 5, tzinfo=UTC))
    general = replace(spec("general"), name="calendar")
    frame = rows((1,), (1.0,))
    data.raw.ingest(general, frame.head(0) if empty_general else frame,
                    mode="initialize", ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    requests = {"a": RawInput("custom", "calendar" if reverse else "raw", view="versions",
                              include_historical_baseline=True),
                "z": RawInput("custom", "raw" if reverse else "calendar", view="versions",
                              include_historical_baseline=True)}
    frozen = data.inputs.freeze(requests, information_cutoff="2020-01-31")
    with sqlite3.connect(data.data_meta_path) as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_index'").fetchone() is None
    data.items.register(DataItemSpec("output", tuple(requests.values())))
    monkeypatch.setattr(data.inputs, "_baseline_at", lambda *a, **k: pytest.fail("known OR must not decode timing"))
    output = rows((10, 20), (2., 3.)).with_columns(pl.col("time").alias("version_available_date"))
    result = data.items.ingest("output", output, input_receipt=frozen)
    assert result.status == "success"
    assert data.items.read("output", strict=True, view="snapshot", as_of="2020-01-31").collect().is_empty()
    assert data.inputs.get(frozen).digest == frozen.digest


def test_general_positive_timing_preserves_attestation_cutoff_and_ceilings(tmp_path):
    data = lake(tmp_path)
    frame = rows((1,), (1.,))
    data.raw.ingest(spec("general"), frame, mode="initialize",
                    ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    data.raw.ingest(spec("general"), frame, ingested_at=datetime(2020, 1, 20, tzinfo=UTC))
    frozen = data.inputs.freeze({"raw": RawInput("custom", "raw", view="versions",
        include_historical_baseline=True)}, information_cutoff="2020-01-31")
    assert proven(frozen, "raw", date(2020, 1, 10)) is True
    assert proven(frozen, "raw", date(2020, 1, 25)) is None
    assert proven(frozen, "raw", date(2020, 1, 25), max_check_id=0) is True
    assert proven(frozen, "raw", date(2020, 1, 25), max_commit=0) is None
    strict = replace(frozen, requests={"raw": replace(frozen.requests["raw"], strict=True)})
    assert proven(strict, "raw", date(2020, 1, 10)) is None
    assert data.inputs._baseline_at(frozen, "raw", date(2020, 1, 25)) is False


def test_verified_general_cannot_hide_unknown_daily_timing(tmp_path):
    import sqlite3

    data = lake(tmp_path)
    frame = rows((1,), (1.,))
    data.raw.ingest(spec(), frame, mode="initialize", ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    data.raw.ingest(spec(), frame, ingested_at=datetime(2020, 1, 5, tzinfo=UTC))
    general = replace(spec("general"), name="calendar")
    data.raw.ingest(general, frame, ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    requests = {"daily": RawInput("custom", "raw", view="versions", include_historical_baseline=True),
        "calendar": RawInput("custom", "calendar", view="versions", include_historical_baseline=True)}
    frozen = data.inputs.freeze(requests, information_cutoff="2020-01-31")
    with sqlite3.connect(data.data_meta_path) as db:
        assert db.execute("SELECT 1 FROM sqlite_master WHERE name='input_batch_index'").fetchone() is None
    data.items.register(DataItemSpec("output", tuple(requests.values())))
    # First visibility is retained as immutable Parquet event evidence.
    data.items.ingest("output", rows((10,), (2.,)), available_date="2020-01-10", input_receipt=frozen)
    assert data.items.read("output").collect()["value"].to_list() == [2.]
    assert data.items.read("output", strict=True).collect()["value"].to_list() == [2.]
