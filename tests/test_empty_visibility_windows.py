"""Out-of-range typed emptiness never joins unrelated visibility events."""
from datetime import UTC, date, datetime
import hashlib

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, ProducerUpdatePolicy, RawInput
from bagelquant_data.query.raw import _attested_versions


FIRST, LAST, KNOWN = date(2020, 1, 1), date(2020, 1, 6), date(2020, 1, 10)


def _setup(root):
    lake = DataLake.open(data_meta_path=root / "data.sqlite", lake_path=root / "lake")
    spec = DatasetSpec("daily", "by_date", date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [FIRST, LAST], "asset_id": ["A", "A"], "value": [1., 6.],
                          "time_right": [FIRST, LAST]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2020, 1, 6, tzinfo=UTC))
    lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 1, 10, tzinfo=UTC))
    return lake, spec, frame


@pytest.mark.parametrize("selection", [RawInput("custom", "daily", view="versions"), ItemInput("item", view="versions")])
def test_frozen_empty_window_retains_schema_without_reading_originals_or_events(tmp_path, monkeypatch, selection):
    lake, _, frame = _setup(tmp_path)
    if isinstance(selection, ItemInput):
        lake.items.register(DataItemSpec("item"))
        lake.items.ingest("item", frame, historical_baseline=True, available_date=LAST)
        lake.items.ingest("item", frame, available_date=KNOWN)
    source = lake.inputs.freeze({"source": selection}, information_cutoff=KNOWN)
    originals = {path: (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns) for path in lake.lake_path.rglob("*.parquet")}
    from bagelquant_data import inputs
    monkeypatch.setattr(inputs, "read_batch", lambda *_a, **_k: pytest.fail("Disjoint empty window read original values"))
    monkeypatch.setattr(pl, "scan_parquet", lambda *_a, **_k: pytest.fail("Disjoint empty window decoded visibility events"))
    for strict in (False, True):
        actual = lake.inputs.read(source, "source", start="2020-01-07", end=KNOWN, strict=strict).collect()
        assert actual.is_empty()
        assert actual.schema["value"] == pl.Float64 and actual.schema["time_right"] == pl.Date
        assert actual.schema["_attestation_id"] == pl.Int64
    assert {path: (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns) for path in originals} == originals


def test_native_first_verification_with_empty_append_suffix_preserves_baseline_pit(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "data.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("daily", "by_date", date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [FIRST, LAST], "asset_id": ["A", "A"], "value": [1., 6.]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2020, 1, 6, tzinfo=UTC))
    request = RawInput("custom", "daily", alias="raw", include_historical_baseline=True)
    calls = []
    def producer(context):
        calls.append((context.start, context.end))
        return context.read("raw").select(pl.col("source_time").alias("time"), "asset_id", "value")
    lake.items.register_producer("native", "1", producer, update_policy=ProducerUpdatePolicy("same_date"))
    lake.items.register(DataItemSpec("native", (request,), producer_key="native", producer_revision="1"))
    assert lake.items.initialize("native", start=FIRST, end=LAST).status == "success"
    retained = lake.inputs.freeze({"native": ItemInput("native", view="versions")}, information_cutoff=LAST)
    baseline = lake.inputs.read(retained, "native").collect()
    originals = {path: path.read_bytes() for path in lake.lake_path.rglob("*.parquet")}
    calls.clear()
    lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 1, 10, tzinfo=UTC))
    assert lake.items.update("native", start=FIRST, end=KNOWN).status == "success"
    assert calls and all(first > LAST for first, last in calls)
    assert lake.inputs.read(retained, "native").collect().equals(baseline)
    assert all(path.read_bytes() == content for path, content in originals.items())
    flag = lake.items.completion_statuses(names=["native"])[0]
    assert all(flag[key] for key in ("definition_current", "publication_current", "dependency_current"))
    assert flag["checked_target"] == KNOWN.isoformat()
    assert lake.items.read("native", view="snapshot", as_of=LAST, strict=True).collect().is_empty()
    # An empty pointwise suffix advances completed numerical coverage; it does
    # not fabricate a verified publication of the unchanged baseline prefix.
    assert lake.items.read("native", view="snapshot", as_of=KNOWN, strict=True).collect().is_empty()
    assert lake.items.read("native", view="snapshot", as_of=KNOWN, strict=False).collect()["value"].to_list() == [1., 6.]


def test_nonempty_unidentified_observations_are_not_certified_as_empty():
    frame = pl.DataFrame({"time": [FIRST], "asset_id": ["A"], "value": [1.],
                          "_commit_seq": [1], "_baseline": [True]})
    with pytest.raises(RuntimeError, match="natural-key identities for nonempty"):
        _attested_versions(frame.lazy(), [], [])
