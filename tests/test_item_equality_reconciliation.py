"""Exact equality avoids joins while real changes/withdrawals retain PIT history."""
from datetime import UTC, date, datetime

import polars as pl
import pytest
from bagelquant_data import DataLake, DataItemSpec, DatasetSpec, ItemInput, RawInput
from bagelquant_data.items.pit import _same_ordered_rows, iter_computed_versions


def sample_frame():
    return pl.DataFrame({"time": [date(2020, 1, 1)] * 6,
        "asset_id": list("ABCDEF"), "value": [None, float("nan"), -0.0, 0.0, float("inf"), -float("inf")],
        "observation_date": [date(2020, 1, 1)] * 6,
        "available_date": [date(2020, 1, 1)] * 6, "_build_baseline": [True] * 6})


@pytest.mark.parametrize("mutation", ["same", "reordered", "value", "removed", "availability", "baseline"])
def test_revision_reconciliation_matches_original_anti_join_semantics(mutation):
    previous = sample_frame()
    current = previous.clone()
    if mutation == "reordered":
        current = current.reverse()
    elif mutation == "value":
        current = current.with_columns(pl.when(pl.col("asset_id") == "C").then(8.0).otherwise(pl.col("value")).alias("value"))
    elif mutation == "removed":
        current = current.filter(pl.col("asset_id") != "D")
    elif mutation == "availability":
        current = current.with_columns(pl.lit(date(2020, 1, 3)).alias("available_date"))
    elif mutation == "baseline":
        current = current.with_columns(pl.lit(False).alias("_build_baseline"))
    boundary, end = date(2020, 1, 3), date(2020, 1, 4)
    def evaluate_at(raw, items, cutoff):
        return previous if cutoff < boundary else current
    outputs = list(iter_computed_versions(raw={}, items={}, start=date(2020, 1, 1), end=end,
        extra_boundaries=[boundary], evaluate=lambda raw, items: previous, evaluate_at=evaluate_at))
    prior = previous.with_columns(pl.col("time").alias("version_available_date"))
    final = current.with_columns(pl.max_horizontal("time", "available_date", pl.lit(boundary)).alias("version_available_date"))
    identity = previous.columns
    expected = final.join(prior.select(identity), on=identity, how="anti", nulls_equal=True)
    removed = prior.join(final.select("time", "asset_id"), on=["time", "asset_id"], how="anti")
    if removed.height:
        removed = removed.with_columns(pl.lit(None, dtype=pl.Float64).alias("value"),
                                      pl.lit(boundary).alias("version_available_date"))
        expected = pl.concat([expected, removed], how="diagonal_relaxed")
    assert outputs[1].sort("asset_id").equals(expected.sort("asset_id"))


def test_ordered_equality_requires_schema_and_does_not_join(monkeypatch):
    frame = sample_frame()
    assert not _same_ordered_rows(frame, frame.reverse(), frame.columns)
    assert not _same_ordered_rows(frame.select("value"), frame.select(pl.col("value").cast(pl.Float32)), ["value"])
    def forbidden(*args, **kwargs):
        raise AssertionError("Identical revisions must not allocate hash joins")
    monkeypatch.setattr(pl.DataFrame, "join", forbidden)
    outputs = list(iter_computed_versions(raw={}, items={}, start=date(2020, 1, 1), end=date(2020, 1, 4),
        extra_boundaries=[date(2020, 1, 3)], evaluate=lambda raw, items: frame))
    assert outputs[0].height == 6 and outputs[1].is_empty()


def test_public_build_checks_only_keys_and_preserves_true_withdrawals(tmp_path, monkeypatch):
    with DataLake.open(data_meta_path=tmp_path/'meta.sqlite', lake_path=tmp_path/'lake') as lake:
        spec = DatasetSpec("seed", "by_date", date_kind="calendar", field_mappings={"time":"time", "asset_id":"asset_id"})
        frame = pl.DataFrame({"time":[date(2020,1,1)]*2,"asset_id":["A","B"],"value":[1.,2.],"diagnostic":["first","second"]})
        lake.raw.ingest(spec,frame,mode="initialize",ingested_at=datetime(2020,1,2,tzinfo=UTC))
        selected = [frame]
        lake.items.register_producer("fixture","1",lambda context:selected[0])
        lake.items.register(DataItemSpec("derived",(RawInput("custom","seed"),),producer_key="fixture",producer_revision="1"))
        original = lake.items.read
        collect = pl.LazyFrame.collect
        schemas = []
        armed = False
        def progress(report):
            nonlocal armed
            armed = True
        def observe(query, *args, **kwargs):
            if armed:
                schemas.append(query.collect_schema().names())
            return collect(query, *args, **kwargs)
        monkeypatch.setattr(pl.LazyFrame, "collect", observe)
        lake.items.initialize("derived",start="2020-01-01",end="2020-01-04",progress=progress)
        assert ["time", "asset_id"] in schemas
        assert not any("_producer_key" in schema and "value" in schema for schema in schemas)
        armed = False
        frozen = lake.inputs.freeze({"item":ItemInput("derived")},information_cutoff="2020-01-02")
        selected[0] = frame.filter(pl.col("asset_id")=="A")
        report = lake.items.update("derived",start="2020-01-01",end="2020-01-04",force=True)
        assert report.status=="success"
        assert original("derived",view="snapshot",as_of="2020-01-04").collect().filter(pl.col("asset_id")=="B")["value"][0] is None
        lake.inputs.verify(frozen)
        assert lake.inputs.read(frozen,"item").collect().filter(pl.col("asset_id")=="B")["value"][0]==2.
        assert original("derived",view="snapshot",as_of="2020-01-04").collect().filter(pl.col("asset_id")=="B")["diagnostic"][0]=="second"
