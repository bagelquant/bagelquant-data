"""Observation windows prune physical reads and temporary identities before PIT."""
from concurrent.futures import CancelledError
from datetime import UTC, date, datetime, timedelta
import hashlib
import math
import sqlite3

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, RawInput
from bagelquant_data import inputs
from bagelquant_data.pipeline import versions
from bagelquant_data.query import raw
from bagelquant_data.storage import recovery


def lake_at(root):
    return DataLake.open(data_meta_path=root / "meta.sqlite", lake_path=root / "lake")


def wide_baseline(lake):
    first, last, known = date(2020, 1, 1), date(2020, 4, 29), date(2020, 5, 5)
    panel = pl.DataFrame({"time": [first + timedelta(days=n) for n in range(120)]}).join(
        pl.DataFrame({"asset_id": [f"A{n:04}" for n in range(1000)]}), how="cross"
    ).with_columns(pl.lit(1.0).alias("value"), pl.col("time").alias("available_date"),
                   pl.col("time").alias("time_right"), pl.lit("caller").alias("asset_id_right"))
    lake.items.register(DataItemSpec("scalar"))
    lake.items.ingest("scalar", panel, historical_baseline=True, available_date=last)
    baseline = lake.inputs.freeze({"x": ItemInput("scalar", view="versions")}, information_cutoff=known)
    lake.items.ingest("scalar", panel, available_date=known)
    observed = lake.inputs.freeze({"x": ItemInput("scalar", view="versions")}, information_cutoff=known)
    return panel, baseline, observed, known


def full_read_oracle(lake, source, monkeypatch, **options):
    original_read, original_overlay = inputs.read_batch, raw._attested_versions

    def unbounded_read(*args, **kwargs):
        kwargs.pop("observation_start", None)
        kwargs.pop("observation_end", None)
        return original_read(*args, **kwargs)

    def unbounded_overlay(*args, **kwargs):
        kwargs.pop("observation_start", None)
        kwargs.pop("observation_end", None)
        return original_overlay(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(inputs, "read_batch", unbounded_read)
        patch.setattr(raw, "_attested_versions", unbounded_overlay)
        return lake.inputs.read(source, "x", **options).collect()


def test_large_first_known_event_hashes_only_selected_observations(tmp_path, monkeypatch):
    lake = lake_at(tmp_path)
    panel, baseline, observed, known = wide_baseline(lake)
    assert panel.height == 120_000
    originals = {path: (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns)
                 for path in lake.lake_path.rglob("*.parquet")}
    calls, scans = [], []
    original_hash, original_scan = versions._payload_hashes, pl.scan_parquet

    def counted(frame, fields):
        calls.append((frame.height, frame["source_time"].min(), frame["source_time"].max(),
                      "_attested_date" in frame.columns))
        return original_hash(frame, fields)

    def scanned(*args, **kwargs):
        scans.append(args[0])
        return original_scan(*args, **kwargs)

    monkeypatch.setattr(versions, "_payload_hashes", counted)
    monkeypatch.setattr(pl, "scan_parquet", scanned)
    for source, lower, upper, expected_rows, expected_hash_rows in (
        (baseline, date(2020, 1, 15), date(2020, 1, 15), 1000, 1000),
        (observed, date(2020, 1, 15), date(2020, 1, 15), 2000, 2000),
        (observed, date(2020, 1, 1), date(2020, 2, 1), 64_000, 64_000),
        (observed, date(2020, 2, 2), date(2020, 3, 4), 64_000, 64_000),
    ):
        calls.clear()
        scans.clear()
        actual = lake.inputs.read(source, "x", start=lower, end=upper, as_of=known,
            fields=("time", "asset_id", "version_available_date"), view="versions").collect()
        assert actual.height == expected_rows
        assert sum(row[0] for row in calls) == expected_hash_rows
        assert all(lower <= minimum <= maximum <= upper for _, minimum, maximum, _ in calls)
        assert len(scans) == (1 if source is baseline else 2)
        assert any(row[3] for row in calls) is (source is observed)
    assert {path: (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mtime_ns)
            for path in originals} == originals
    # Optional index/currentness preparation is never needed for bounded reads.
    assert lake.inputs.window_read_supported(observed, "x")


def compound_source(lake):
    first, other, known = date(2020, 1, 3), date(2020, 1, 7), date(2020, 1, 10)
    spec = DatasetSpec("compound", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"}, primary_key_extra=("variant",),
        nullable_primary_key_extra=("variant",))
    variants = [None, -0.0, 0.0, float("nan"), float("inf"), float("-inf")]
    frame = pl.DataFrame({"time": [first] * 6 + [other] * 6, "asset_id": ["A"] * 12,
                         "variant": variants * 2, "value": [float(n) for n in range(12)],
                         "time_right": [date(2019, 12, 30)] * 12, "asset_id_right": ["caller"] * 12})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2020, 1, 8, tzinfo=UTC))
    before = lake.inputs.freeze({"x": RawInput("custom", "compound", view="versions")}, information_cutoff="2020-01-12")
    lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 1, 10, tzinfo=UTC))
    withdrawal = frame.filter((pl.col("time") == first) & pl.col("variant").is_null()).with_columns(pl.lit(None, dtype=pl.Float64).alias("value"))
    lake.raw.ingest(spec, withdrawal, ingested_at=datetime(2020, 1, 11, tzinfo=UTC))
    after = lake.inputs.freeze({"x": RawInput("custom", "compound", view="versions")}, information_cutoff="2020-01-12")
    return before, after, first, known


@pytest.mark.parametrize("view", ["versions", "snapshot", "history"])
@pytest.mark.parametrize("strict", [False, True])
@pytest.mark.parametrize("cutoff", [date(2020, 1, 9), date(2020, 1, 11)])
def test_compound_keys_late_events_and_null_withdrawals_match_full_read(
    tmp_path, monkeypatch, view, strict, cutoff,
):
    lake = lake_at(tmp_path)
    before, after, first, _ = compound_source(lake)
    options = {"view": view, "strict": strict, "as_of": cutoff, "start": first, "end": first}
    for source in (before, after):
        expected = full_read_oracle(lake, source, monkeypatch, **options)
        actual = lake.inputs.read(source, "x", **options).collect()
        order = [name for name in ("_record_id", "time", "_commit_seq", "_attestation_id") if name in actual.columns]
        assert_frame_equal(actual.sort(order), expected.sort(order))
        assert "_record_id" in actual.columns
    latest = lake.inputs.read(after, "x", view="snapshot", strict=True,
                             as_of="2020-01-11", start=first, end=first).collect()
    assert latest.height == 6 and latest.filter(pl.col("variant").is_null())["value"].item() is None
    zeros = latest.filter(pl.col("variant") == 0.0).sort("value")
    assert [math.copysign(1.0, value) for value in zeros["variant"]] == [-1.0, 1.0]
    assert latest["time_right"].to_list() == [date(2019, 12, 30)] * 6
    assert latest["asset_id_right"].to_list() == ["caller"] * 6


def test_direct_raw_observation_pushdown_keeps_availability_filter_after_latest(tmp_path, monkeypatch):
    lake = lake_at(tmp_path)
    _, source, first, _ = compound_source(lake)
    expected = lake.inputs.read(source, "x", view="snapshot", strict=True,
        as_of="2020-01-11", start=first, end=first).collect().filter(pl.col("time") >= date(2020, 1, 11))
    calls = []
    original = versions._payload_hashes
    def counted(frame, fields):
        calls.append(frame["source_time"].unique().to_list())
        return original(frame, fields)
    monkeypatch.setattr(versions, "_payload_hashes", counted)
    actual = lake.raw.read("compound", source="custom", as_of="2020-01-11", strict=True,
        view="latest", start="2020-01-11", observation_start=first, observation_end=first).collect()
    assert_frame_equal(actual.sort("_record_id"), expected.sort("_record_id"))
    assert all(days == [first] for days in calls)
    assert actual.height == 1 and actual["value"].item() is None


@pytest.mark.parametrize("corruption", ["rows", "schema"])
def test_bounded_reader_checks_whole_original_footer_before_selection(tmp_path, corruption):
    lake = lake_at(tmp_path)
    _, baseline, _, _ = wide_baseline(lake)
    batch = baseline.evidence["x"]["batches"][0]
    if corruption == "rows":
        with sqlite3.connect(lake.data_meta_path) as db:
            db.execute("UPDATE version_segments SET row_count=row_count+1 WHERE commit_seq=?", (batch["commit_seq"],))
    else:
        original = next(path for path in lake.lake_path.rglob("*.parquet") if ".visibility" not in path.parts)
        pl.read_parquet(original).with_columns(pl.col("value").cast(pl.String)).write_parquet(original)
    with pytest.raises(RuntimeError, match="typed schema/row count mismatch"):
        lake.inputs.read(baseline, "x", start="2020-01-15", end="2020-01-15").collect()


def test_bounded_read_cancels_before_parquet_and_default_read_retains_all_rows(tmp_path, monkeypatch):
    lake = lake_at(tmp_path)
    _, baseline, _, _ = wide_baseline(lake)
    batch = baseline.evidence["x"]["batches"][0]
    assert recovery.read_batch(lake._data_meta, batch["partition_path"], batch["commit_seq"], batch["content_hash"]).height == 120_000
    def canceled():
        raise CancelledError()
    monkeypatch.setattr(pl, "scan_parquet", lambda *a, **k: pytest.fail("Canceled window scanned a file"))
    with pytest.raises(CancelledError):
        with lake.inputs.read_context(baseline, check_canceled=canceled):
            lake.inputs.read(baseline, "x", start="2020-01-15", end="2020-01-15")


def test_range_currentness_remains_metadata_only_after_bounded_consumption(tmp_path, monkeypatch):
    lake = lake_at(tmp_path)
    _, _, observed, known = wide_baseline(lake)
    lower = upper = date(2020, 1, 15)
    token = lake.inputs.selection_identity(observed, "x", start=lower, end=upper,
        as_of=known, view="versions", values_only=True, causal_versions=True)
    lake.inputs.read(observed, "x", start=lower, end=upper).collect()
    monkeypatch.setattr(pl, "scan_parquet", lambda *a, **k: pytest.fail("Metadata currentness decoded a file"))
    monkeypatch.setattr(pl, "read_parquet", lambda *a, **k: pytest.fail("Metadata currentness decoded a file"))
    monkeypatch.setattr(versions, "_payload_hashes", lambda *a, **k: pytest.fail("Metadata currentness hashed rows"))
    assert lake.inputs.is_current(observed)
    assert lake.inputs.selection_identity(observed, "x", start=lower, end=upper,
        as_of=known, view="versions", values_only=True, causal_versions=True) == token


def test_general_snapshot_selection_never_receives_observation_pushdown(tmp_path, monkeypatch):
    lake = lake_at(tmp_path)
    selected = date(2020, 1, 3)
    spec = DatasetSpec("reference", "general", field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [selected, selected], "source_time": [selected, date(2020, 1, 4)],
                          "asset_id": ["A", "B"], "value": [1., 2.]})
    lake.raw.ingest(spec, frame, ingested_at=datetime(2020, 1, 10, tzinfo=UTC))
    source = lake.inputs.freeze({"x": RawInput("custom", "reference", start=selected, end=selected,
        view="snapshot")}, information_cutoff="2020-01-10")
    calls = []
    original = inputs.read_batch
    def observed(*args, **kwargs):
        calls.append((kwargs.get("observation_start"), kwargs.get("observation_end")))
        return original(*args, **kwargs)
    monkeypatch.setattr(inputs, "read_batch", observed)
    actual = lake.inputs.read(source, "x").collect()
    assert actual["asset_id"].to_list() == ["A"]
    assert calls == [(None, None)]
