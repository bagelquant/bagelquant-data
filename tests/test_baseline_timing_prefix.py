"""Timing prefix equivalence and original integrity in isolated fake lakes."""

import copy
from dataclasses import replace
from datetime import UTC, date, datetime
import polars as pl
import pytest
from bagelquant_data import DataLake, DatasetSpec, DataItemSpec, RawInput, ItemInput
import bagelquant_data.inputs as m

READ = m.InputsAPI._read_frame
BASE = m.InputsAPI._baseline_at


def lake_at(root):
    return DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")


def spec(kind="by_date"):
    return DatasetSpec(
        "raw",
        kind,
        date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"},
    )


def rows(month=1, value=1.0):
    return pl.DataFrame(
        {"time": [date(2020, month, 1)], "asset_id": ["A"], "value": [value]}
    )


def calls(monkeypatch):
    original = m.read_batch
    seen = []

    def read(*args, **kwargs):
        seen.append((args[1], args[2]))
        return original(*args, **kwargs)

    monkeypatch.setattr(m, "read_batch", read)
    return seen


@pytest.mark.parametrize(
    "view,strict",
    [("snapshot", False), ("history", False), ("versions", False), ("snapshot", True)],
)
def test_prefix_late_attestation_revision_and_all_views(
    tmp_path, monkeypatch, view, strict
):
    with lake_at(tmp_path) as lake:
        lake.raw.ingest(
            spec(),
            pl.concat([rows(1), rows(2), rows(3)]),
            mode="initialize",
            ingested_at=datetime(2020, 3, 1, tzinfo=UTC),
        )
        lake.raw.ingest(spec(), rows(1), ingested_at=datetime(2020, 1, 15, tzinfo=UTC))
        lake.raw.ingest(
            spec(), rows(1, 2.0), ingested_at=datetime(2020, 2, 20, tzinfo=UTC)
        )
        request = RawInput(
            "custom",
            "raw",
            start="2020-01-01",
            end="2020-03-31",
            view=view,
            strict=strict,
            include_historical_baseline=True,
        )
        receipt = lake.inputs.freeze({"raw": request}, information_cutoff="2020-03-31")
        assert receipt.evidence["raw"]["scoped_batches"]
        seen = calls(monkeypatch)
        for cutoff in [
            date(2019, 12, 31),
            date(2020, 1, 10),
            date(2020, 1, 20),
            date(2020, 2, 25),
            date(2020, 3, 31),
            date(2020, 5, 1),
        ]:
            before = len(seen)
            full = READ(lake.inputs, receipt, "raw")
            full_n = len(seen) - before
            before = len(seen)
            prefix = READ(lake.inputs, receipt, "raw", timing_cutoff=cutoff)
            prefix_n = len(seen) - before
            assert BASE(lake.inputs, receipt, "raw", cutoff, frame=full) == BASE(
                lake.inputs, receipt, "raw", cutoff, frame=prefix
            )
            assert prefix_n <= full_n
            if cutoff == date(2020, 1, 10):
                assert prefix_n < full_n
        expected_values = [1., 2.] if strict else [1., 1., 2., 1., 1.]
        assert lake.inputs.read(receipt, "raw", view="versions").collect()["value"].to_list() == expected_values
        assert lake.inputs.verify(receipt)["valid"]


def test_general_historical_and_empty_snapshot_batches_stay_broad(
    tmp_path, monkeypatch
):
    with lake_at(tmp_path) as lake:
        lake.raw.ingest(
            spec("general"),
            rows(),
            mode="initialize",
            ingested_at=datetime(2020, 3, 1, tzinfo=UTC),
        )
        lake.raw.ingest(
            spec("general"),
            rows().head(0),
            ingested_at=datetime(2020, 3, 15, tzinfo=UTC),
        )
        seen = calls(monkeypatch)
        for strict in [False, True]:
            receipt = lake.inputs.freeze(
                {
                    "raw": RawInput(
                        "custom",
                        "raw",
                        view="snapshot",
                        strict=strict,
                        include_historical_baseline=True,
                    )
                },
                information_cutoff="2020-03-31",
            )
            assert not receipt.evidence["raw"].get("scoped_batches")
            for cutoff in [date(2020, 1, 10), date(2020, 3, 10), date(2020, 3, 20)]:
                before = len(seen)
                full = READ(lake.inputs, receipt, "raw")
                n = len(seen) - before
                before = len(seen)
                prefix = READ(lake.inputs, receipt, "raw", timing_cutoff=cutoff)
                assert len(seen) - before == n
                assert BASE(lake.inputs, receipt, "raw", cutoff, frame=full) == BASE(
                    lake.inputs, receipt, "raw", cutoff, frame=prefix
                )


def test_legacy_and_unknown_bounds_broad(tmp_path, monkeypatch):
    with lake_at(tmp_path) as lake:
        lake.raw.ingest(
            spec(),
            pl.concat([rows(1), rows(2), rows(3)]),
            mode="initialize",
            ingested_at=datetime(2020, 3, 1, tzinfo=UTC),
        )
        receipt = lake.inputs.freeze(
            {"raw": RawInput("custom", "raw", start="2020-01-01", end="2020-03-31")},
            information_cutoff="2020-03-31",
        )
        seen = calls(monkeypatch)
        for legacy in [True, False]:
            ev = {key: dict(copy.deepcopy(value)) for key, value in receipt.evidence.items()}
            if legacy:
                ev["raw"].pop("scoped_batches")
            else:
                for b in ev["raw"]["batches"]:
                    b["min_available"] = None
            synthetic = replace(
                receipt, evidence=ev
            )  # isolated decision recipe, not persisted/integrity proof
            before = len(seen)
            full = READ(lake.inputs, synthetic, "raw")
            n = len(seen) - before
            before = len(seen)
            prefix = READ(
                lake.inputs, synthetic, "raw", timing_cutoff=date(2020, 1, 10)
            )
            assert len(seen) - before == n
            assert BASE(
                lake.inputs, synthetic, "raw", date(2020, 1, 10), frame=full
            ) == BASE(lake.inputs, synthetic, "raw", date(2020, 1, 10), frame=prefix)


def test_empty_item_parent_recursion_proof(tmp_path, monkeypatch):
    with lake_at(tmp_path) as lake:
        lake.raw.ingest(
            spec(),
            pl.concat([rows(1), rows(2), rows(3)]),
            mode="initialize",
            ingested_at=datetime(2020, 3, 1, tzinfo=UTC),
        )
        raw = RawInput("custom", "raw", start="2020-01-01", end="2020-03-31")
        parent = lake.inputs.freeze({"raw": raw}, information_cutoff="2020-03-31")
        lake.items.register(DataItemSpec("empty", (raw,)))
        lake.items.ingest(
            "empty", rows().head(0), available_date="2020-03-31", input_receipt=parent
        )
        receipt = lake.inputs.freeze(
            {"item": ItemInput("empty", view="snapshot")},
            information_cutoff="2020-03-31",
        )
        assert receipt.evidence["item"]["empty_item_build"]
        ds = [date(2020, 1, 10), date(2020, 2, 10), date(2020, 3, 31)]

        def unpruned(store, receipt, alias, **kwargs):
            return READ(store, receipt, alias)

        with monkeypatch.context() as patch:
            patch.setattr(m.InputsAPI, "_read_frame", unpruned)
            expected = [BASE(lake.inputs, receipt, "item", d) for d in ds]
        assert [lake.inputs._baseline_at(receipt, "item", d) for d in ds] == expected
        assert lake.inputs.verify(receipt)["valid"]


def test_full_commit_seal_stays_complete_and_integrity_checks_future_batches(
    tmp_path, monkeypatch
):
    from bagelquant_data.storage import full_commit_checks as proofs

    with lake_at(tmp_path) as lake:
        frame = pl.concat([rows(1), rows(2), rows(3)])
        lake.raw.ingest(
            spec(),
            frame,
            mode="initialize",
            ingested_at=datetime(2020, 3, 1, tzinfo=UTC),
        )
        lake.raw.ingest(spec(), frame, ingested_at=datetime(2020, 3, 15, tzinfo=UTC))
        monkeypatch.setattr(proofs, "COMPACT_MIN_ROWS", 1)
        receipt = lake.inputs.freeze(
            {
                "raw": RawInput(
                    "custom",
                    "raw",
                    start="2020-01-01",
                    end="2020-03-31",
                    view="versions",
                )
            },
            information_cutoff="2020-03-31",
        )
        assert len(receipt.evidence["raw"]["full_commit_checks"]) == 1
        retained = copy.deepcopy(receipt.evidence)
        seen = calls(monkeypatch)
        full = READ(lake.inputs, receipt, "raw")
        before = len(seen)
        prefix = READ(lake.inputs, receipt, "raw", timing_cutoff=date(2020, 1, 20))
        assert len(seen) - before == 1
        for day in [date(2020, 1, 10), date(2020, 1, 20)]:
            assert BASE(lake.inputs, receipt, "raw", day, frame=full) == BASE(
                lake.inputs, receipt, "raw", day, frame=prefix
            )
        assert receipt.evidence == retained and lake.inputs.get(receipt) == receipt
        assert lake.inputs.verify(receipt)["batch_count"] == 3
        import sqlite3

        with sqlite3.connect(lake.data_meta_path) as db:
            db.execute(
                "update version_batches set payload=x'00' where partition_path like '%month=03%'"
            )
        with pytest.raises(RuntimeError):
            lake.inputs.verify(receipt)
