"""Explicit asset-local General additions preserve completed native history."""
from datetime import UTC, date, datetime
import json
import sqlite3

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ProducerUpdatePolicy, RawInput, input_index

FIELDS = ("asset_id", "market", "weight")
FIRST, OLD, TAIL = (date(2025, 1, day) for day in (2, 3, 4))


def received(day):
    return datetime(2025, 1, day, tzinfo=UTC)


class Fixture:
    def __init__(self, root, *, local=True, mixed=False):
        self.lake = DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")
        self.general = DatasetSpec("metadata", "general")
        self.before = pl.DataFrame({"asset_id": ["A", None], "market": ["main", "null row"],
            "weight": [1., -0.], "unused": ["before", "retained"]})
        self.lake.raw.ingest(self.general, self.before, ingested_at=received(2))
        self.specs = {name: DatasetSpec(name, "by_date", date_kind="calendar",
            field_mappings={"time": "time", "asset_id": "asset_id"},
            primary_key_extra=("variant",) if name == "secondary" else (),
            nullable_primary_key_extra=("variant",) if name == "secondary" else ()) for name in ("daily", "secondary")}
        for alias, spec in self.specs.items():
            for day in (FIRST, OLD):
                self.lake.raw.ingest(spec, self.rows(alias, day, "A"),
                    mode="initialize" if mixed and day == FIRST else "incremental",
                    ingested_at=received(day.day))
        self.calls = []
        def producer(context):
            self.calls.append((context.start, context.end))
            return context.frames["daily"].join(context.frames["metadata"].select(FIELDS), on="asset_id").select(
                pl.col("source_time").alias("time"), "asset_id", (pl.col("value") * pl.col("weight")).alias("value"))
        self.policy = ProducerUpdatePolicy("same_date", timing_neutral_inputs=("metadata",),
            general_fields=(("metadata", FIELDS),), output_anchors=("daily", "secondary"),
            general_asset_local_inputs=("metadata",) if local else ())
        self.lake.items.register_producer("p", "1", producer, update_policy=self.policy)
        self.lake.items.register(DataItemSpec("item", tuple(RawInput("custom", name, alias=name)
            for name in ("metadata", "daily", "secondary")), producer_key="p", producer_revision="1"))
        report = self.lake.items.initialize("item", start=FIRST, end=OLD)
        assert report.frozen_receipt_id is not None
        self.original = self.lake.inputs.get(report.frozen_receipt_id)
        self.calls.clear()

    @staticmethod
    def rows(alias, day, asset, *, value: float | None=2., variant=None):
        rows = pl.DataFrame({"time": [day], "asset_id": [asset], "value": [value]})
        return rows.with_columns(pl.lit(variant, dtype=pl.Float64).alias("variant")) if alias == "secondary" else rows

    def append(self, *, general_day=4, changed=None):
        after = pl.concat([self.before.with_columns(pl.lit("cosmetic").alias("unused")),
            pl.DataFrame({"asset_id": ["B"], "market": ["growth"], "weight": [2.], "unused": ["new"]})])
        if changed is not None:
            after = changed(after)
        self.lake.raw.ingest(self.general, after, ingested_at=received(general_day))
        for alias, spec in self.specs.items():
            self.lake.raw.ingest(spec, self.rows(alias, TAIL, "B"), ingested_at=received(4))

    def prepare(self, **kwargs):
        return self.lake.inputs.prepare_general_projections(self.original, "metadata", FIELDS,
            through=TAIL, key_anchors=("daily", "secondary"), completed_start=FIRST, completed_end=OLD, **kwargs)

    def plan(self):
        return self.lake.items.plan_update("item", start=FIRST, end=TAIL)


def original_files(fixture):
    return {str(path): path.read_bytes() for path in fixture.lake.lake_path.rglob("*.parquet")}


def forbid(*_a, **_k):
    raise AssertionError("metadata prerequisite consumed Original rows")


@pytest.mark.parametrize("mixed", [False, True])
@pytest.mark.parametrize("same_day", [False, True])
def test_pure_addition_prepares_every_anchor_and_runs_only_native_tail(tmp_path, monkeypatch, mixed, same_day):
    fixture = Fixture(tmp_path, mixed=mixed)
    lake = fixture.lake
    old = lake.items.read("item", view="versions").collect()
    files = original_files(fixture)
    fixture.append(general_day=3 if same_day else 4)
    assert fixture.plan()[0]["mode"] == "blocked"
    prepared = fixture.prepare()
    assert prepared["key_appends"]["prepared"] >= 1
    assert all(state["status"] == "known" for state in prepared["key_appends"]["states"])
    with monkeypatch.context() as patch:
        patch.setattr(input_index, "project_original", forbid)
        patch.setattr(input_index, "read_selector", forbid)
        assert fixture.prepare()["key_appends"]["prepared"] == 0
        assert fixture.plan() == [{"mode": "incremental", "start": str(TAIL), "end": str(TAIL)}]
    assert not fixture.calls
    report = lake.items.update("item", start=FIRST, end=TAIL)
    assert report.status == "success" and fixture.calls == [(TAIL, TAIL)]
    assert lake.items.read("item", start=TAIL, end=TAIL).collect()["value"].to_list() == [4.]
    assert_frame_equal(lake.items.read("item", end=OLD, view="versions").collect(), old)
    assert all(__import__("pathlib").Path(path).read_bytes() == content for path, content in files.items())
    assert lake.inputs.get(fixture.original).digest == fixture.original.digest
    fixture.calls.clear()
    with monkeypatch.context() as patch:
        patch.setattr(input_index, "project_original", forbid)
        assert lake.items.update("item", start=FIRST, end=TAIL).status == "unchanged"
    assert not fixture.calls


@pytest.mark.parametrize("change", ["old", "remove", "null", "null_zero", "new_null", "duplicate", "schema"])
def test_typed_old_multiset_nulls_and_schema_changes_never_certify(tmp_path, change):
    fixture = Fixture(tmp_path)
    def changed(frame):
        if change == "old":
            return frame.with_columns(pl.when(pl.col("asset_id") == "A").then(pl.lit("changed"))
                .otherwise(pl.col("market")).alias("market"))
        if change == "remove":
            return frame.filter(pl.col("asset_id").is_null() | (pl.col("asset_id") != "A"))
        if change == "null":
            return frame.with_columns(pl.when(pl.col("asset_id").is_null()).then(pl.lit("changed"))
                .otherwise(pl.col("market")).alias("market"))
        if change == "null_zero":
            return frame.with_columns(pl.when(pl.col("asset_id").is_null()).then(pl.lit(0.))
                .otherwise(pl.col("weight")).alias("weight"))
        if change == "new_null":
            return pl.concat([frame, fixture.before.filter(pl.col("asset_id").is_null())
                .with_columns(pl.lit("different null tuple").alias("market"))])
        if change == "duplicate":
            return pl.concat([frame, fixture.before.filter(pl.col("asset_id") == "A")
                .with_columns(pl.lit("different tuple").alias("market"))])
        return frame.with_columns(pl.col("weight").cast(pl.Float32))
    fixture.append(changed=changed)
    if change == "schema":
        with pytest.raises(ValueError, match="conflicting captured atomic schema"):
            fixture.prepare()
        assert fixture.plan()[0]["mode"] == "blocked" and not fixture.calls
        return
    prepared = fixture.prepare()
    assert prepared["key_appends"]["prepared"] == 0
    assert fixture.plan()[0]["mode"] == "blocked" and not fixture.calls


@pytest.mark.parametrize("anchor", ["daily", "secondary"])
@pytest.mark.parametrize("value", [3., None])
def test_late_historical_extra_key_or_null_tombstone_blocks_anchor_absence(tmp_path, anchor, value):
    fixture = Fixture(tmp_path)
    fixture.append()
    fixture.lake.raw.ingest(fixture.specs[anchor], fixture.rows(anchor, OLD, "B", value=value, variant=-0.),
        ingested_at=received(4))
    prepared = fixture.prepare()["key_appends"]
    assert any(state["status"] == "unsupported" for state in prepared["states"])
    assert fixture.plan()[0]["mode"] == "blocked" and not fixture.calls


def test_changed_anchor_after_preparation_invalidates_without_original_reads(tmp_path, monkeypatch):
    fixture = Fixture(tmp_path)
    fixture.append()
    assert fixture.prepare()["key_appends"]["prepared"] >= 1
    fixture.lake.raw.ingest(fixture.specs["secondary"], fixture.rows("secondary", OLD, "B", value=None),
        ingested_at=received(4))
    monkeypatch.setattr(input_index, "project_original", forbid)
    assert fixture.plan()[0]["mode"] == "blocked" and not fixture.calls


def test_every_same_day_intermediate_state_is_checked_even_after_restore(tmp_path):
    fixture = Fixture(tmp_path)
    fixture.lake.raw.ingest(fixture.general, fixture.before.with_columns(
        pl.when(pl.col("asset_id") == "A").then(pl.lit("bad")).otherwise(pl.col("market")).alias("market")),
        ingested_at=received(3))
    fixture.append(general_day=3)
    assert fixture.prepare()["key_appends"]["prepared"] >= 1
    assert fixture.plan()[0]["mode"] == "blocked" and not fixture.calls


def test_explicit_locality_declaration_is_required(tmp_path):
    fixture = Fixture(tmp_path, local=False)
    fixture.append()
    assert fixture.prepare()["key_appends"]["prepared"] >= 1
    assert fixture.plan()[0]["mode"] == "blocked" and not fixture.calls


@pytest.mark.parametrize("damage", ["duplicate", "locator", "source", "selector"])
def test_explicit_audit_recomputes_new_kind_and_rejects_redigested_descriptor_damage(tmp_path, damage):
    fixture = Fixture(tmp_path)
    fixture.append()
    fixture.prepare()
    with sqlite3.connect(fixture.lake.data_meta_path) as db:
        db.row_factory = sqlite3.Row
        row = dict(db.execute("select * from input_projection_index where json_extract(request_key,'$.projection')=?",
            ("general_asset_append.v1",)).fetchone())
    source = fixture.lake.inputs.get(row["source_receipt_id"])
    assert fixture.lake.inputs.verify(source)["upstream_receipt_count"] >= 1
    with sqlite3.connect(fixture.lake.data_meta_path) as db:
        body = json.loads(row["identity"])
        if damage == "duplicate":
            body["empty_anchors"][-1] = body["empty_anchors"][0]
        elif damage == "locator":
            body["empty_anchors"][0]["request_key"] += " "
        elif damage == "source":
            body["original"]["digest"] = "0" * 64
        else:
            db.execute("update input_projection_selectors set payload=x'00' where identity=?", (body["selector"],))
        if damage != "selector":
            row["identity"] = json.dumps(body, sort_keys=True, separators=(",", ":"))
            row["digest"] = input_index._digest({name: value for name, value in row.items() if name != "digest"})
            db.execute("update input_projection_index set identity=?,digest=? where evidence_key=? and request_key=?",
                (row["identity"], row["digest"], row["evidence_key"], row["request_key"]))
    with pytest.raises(RuntimeError):
        fixture.lake.inputs.verify(source)


@pytest.mark.parametrize("kwargs", [{"historical_impact": "forward"}, {"output_anchors": ()}, {"general_fields": ()}])
def test_runtime_locality_contract_rejects_unsupported_policy(kwargs):
    values = {"historical_impact": "same_date", "general_fields": (("metadata", FIELDS),),
        "output_anchors": ("daily",), "general_asset_local_inputs": ("metadata",)}
    with pytest.raises(ValueError):
        ProducerUpdatePolicy(**(values | kwargs))


def test_unchanged_projection_preparation_never_scans_anchor_originals(tmp_path, monkeypatch):
    fixture = Fixture(tmp_path)
    fixture.lake.raw.ingest(fixture.general, fixture.before.with_columns(pl.lit("cosmetic").alias("unused")),
        ingested_at=received(4))
    fixture.prepare()
    monkeypatch.setattr(input_index, "project_original", forbid)
    assert fixture.prepare()["key_appends"]["prepared"] == 0
    assert fixture.plan()[0]["mode"] == "incremental"
