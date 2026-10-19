"""Immutable payload-key date selection retains the complete frozen PIT oracle."""
from datetime import UTC, date, datetime

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, ExecutionOptions, RawInput


def fixture(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("events", "by_date", date_kind="calendar",
        field_mappings={"time": "time", "asset_id": "asset_id"},
        primary_key_extra=("ex_date",), nullable_primary_key_extra=("ex_date",))
    frame = pl.DataFrame({"time": [date(2020, 1, 1)] * 8,
        "asset_id": list("ABCDEFGH"), "ex_date": ["20240102", "2024011", "20240131",
            " 20240110", "2024-01-15", "20240201", None, "invalid"],
        "value": [float(number) for number in range(8)]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2020, 1, 1, tzinfo=UTC))
    lake.raw.ingest(spec, frame.head(2), ingested_at=datetime(2024, 1, 3, tzinfo=UTC))
    lake.raw.ingest(spec, frame, ingested_at=datetime(2024, 1, 5, tzinfo=UTC))
    receipt = lake.inputs.freeze({"x": RawInput("custom", "events", view="versions")},
        information_cutoff="2024-02-29")
    return lake, spec, frame, receipt


def oracle(lake, receipt, first, last, **read_options):
    frame = lake.inputs.read(receipt, "x", **read_options).collect()
    text = pl.col("ex_date").cast(pl.String)
    parsed = pl.coalesce(text.str.strptime(pl.Date, "%Y%m%d", strict=False),
        text.str.strptime(pl.Date, "%Y-%m-%d", strict=False))
    return frame.filter(parsed.is_between(first, last))


@pytest.mark.parametrize("first,last,assets", [
    (date(2024, 1, 1), date(2024, 1, 1), {"B"}),
    (date(2024, 1, 1), date(2024, 1, 31), set("ABCDE")),
    (date(2024, 2, 1), date(2024, 2, 29), {"F"}),
    (date(2025, 1, 1), date(2025, 1, 31), set()),
])
def test_key_date_range_matches_full_versions_and_exact_witnesses(tmp_path, first, last, assets):
    lake, _, _, receipt = fixture(tmp_path)
    expected = oracle(lake, receipt, first, last)
    actual = lake.inputs.read(receipt, "x", date_range=("ex_date", first, last)).collect()
    assert actual.equals(expected)
    assert set(actual["asset_id"]) == assets
    if "B" in assets:
        assert "2024011" in actual["ex_date"].to_list()
    if assets:
        assert actual.filter(~pl.col("_baseline")).height > 0
    assert lake.inputs.get(receipt).digest == receipt.digest


@pytest.mark.parametrize("options", [
    {"strict": True, "as_of": "2024-01-02"},
    {"strict": True, "as_of": "2024-01-04"},
    {"strict": True, "as_of": "2024-01-06"},
    {"strict": False, "as_of": "2024-01-02"},
    {"start": "2020-01-01", "end": "2020-01-01"},
    {"start": "2019-01-01", "end": "2019-12-31"},
    {"strict": True, "observations": True, "fields": ("asset_id", "ex_date", "time", "_attestation_id")},
])
def test_date_range_preserves_strict_cutoff_and_observation_intersections(tmp_path, options):
    lake, _, _, receipt = fixture(tmp_path)
    evidence = receipt.evidence["x"]
    assert any(row.get("event") for row in evidence["record_checks"])
    assert not evidence.get("full_commit_checks")  # Current native intake emits exact key-event files.
    first, last = date(2024, 1, 1), date(2024, 1, 31)
    work = []
    with lake.inputs.read_context(receipt, progress=work.append):
        actual = lake.inputs.read(receipt, "x", date_range=("ex_date", first, last), **options).collect()
    assert actual.equals(oracle(lake, receipt, first, last, **options))
    if options.get("as_of", "2024-01-06") >= "2024-01-03" and options.get("end") != "2019-12-31":
        assert any(row["kind"] == "visibility_keys" for row in work)


def test_late_revision_and_moved_date_preserve_the_frozen_versions_oracle(tmp_path):
    lake, spec, frame, original = fixture(tmp_path)
    changed = frame.head(1).with_columns(pl.lit("20240201").alias("ex_date"), pl.lit(99.).alias("value"))
    lake.raw.ingest(spec, changed, ingested_at=datetime(2024, 2, 10, tzinfo=UTC))
    current = lake.inputs.freeze(original.requests, information_cutoff="2024-02-29")
    for receipt in (original, current):
        for first, last in ((date(2024, 1, 1), date(2024, 1, 31)), (date(2024, 2, 1), date(2024, 2, 29))):
            assert lake.inputs.read(receipt, "x", date_range=("ex_date", first, last)).collect().equals(
                oracle(lake, receipt, first, last))
    assert 99. not in lake.inputs.read(original, "x").collect()["value"]
    assert 99. in lake.inputs.read(current, "x", date_range=("ex_date", "2024-02-01", "2024-02-29")).collect()["value"]


@pytest.mark.parametrize("field,view", [("time", "versions"), ("source_time", "versions"),
    ("ingested_at", "versions"), ("_baseline", "versions"), ("value", "versions"),
    ("ex_date", "snapshot"), ("ex_date", "history")])
def test_unsupported_selector_is_rejected_before_original_reads(tmp_path, monkeypatch, field, view):
    lake, _, _, receipt = fixture(tmp_path)
    monkeypatch.setattr("bagelquant_data.inputs.read_batch", lambda *_a, **_k: pytest.fail("invalid selector read originals"))
    with pytest.raises(ValueError, match="immutable extra Raw key"):
        lake.inputs.read(receipt, "x", view=view, date_range=(field, "2024-01-01", "2024-01-31"))


def test_date_search_counts_predicate_work_and_skips_disjoint_historical_payload(tmp_path, monkeypatch):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("events", "by_date", date_kind="calendar", primary_key_extra=("ex_date",),
        field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(year, 1, 1) for year in range(2012, 2024) for _ in range(1000)],
        "asset_id": [f"A{number:04}" for _ in range(12) for number in range(1000)],
        "ex_date": [f"{year}0612" for year in range(2012, 2024) for _ in range(1000)]})
    frame = frame.with_columns(*(pl.lit(float(number)).alias(f"payload_{number}") for number in range(100)))
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2000, 1, 1, tzinfo=UTC))
    receipt = lake.inputs.freeze({"x": RawInput("custom", "events", view="versions")}, information_cutoff="2023-12-31")
    expected = oracle(lake, receipt, date(2023, 6, 1), date(2023, 6, 30))
    from bagelquant_data.storage import recovery
    scans, encoded, work = [], [], []
    scan, encode = recovery.pl.scan_parquet, recovery.with_record_key
    def payload_scan(path, **kwargs):
        scans.append(str(path))
        return scan(path, **kwargs)
    def key_frame(frame, **kwargs):
        encoded.append(frame.height)
        return encode(frame, **kwargs)
    monkeypatch.setattr(recovery.pl, "scan_parquet", payload_scan)
    monkeypatch.setattr(recovery, "with_record_key", key_frame)
    with lake.inputs.read_context(receipt, config=ExecutionOptions(max_buffer_bytes=4 * 1024**2), progress=work.append):
        actual = lake.inputs.read(receipt, "x", date_range=("ex_date", "2023-06-01", "2023-06-30")).collect()
    assert actual.equals(expected)
    assert len(scans) == 1 and "year=2023" in scans[0]
    assert sum(encoded) == 1000
    assert sum(row["predicate_rows"] for row in work) == 12000
    assert sum(row["candidate_payload_rows"] for row in work) == 1000
    assert sum(row["selected_rows"] for row in work) == 1000
    assert sum(row["row_groups_skipped"] for row in work) == 11
    assert sum(row["predicate_column_bytes"] for row in work) > 0


@pytest.mark.parametrize("dtype", [pl.Date, pl.Datetime("us")])
def test_temporal_key_statistics_skip_disjoint_predicate_decoding(tmp_path, dtype):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("events", "by_date", date_kind="calendar", primary_key_extra=("ex_date",),
        field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2020, 1, 1), date(2021, 1, 1)],
        "asset_id": ["A", "A"], "unused": [{"first": [1, 2], "second": 9}, {"first": [3, 4], "second": 8}],
        "ex_date": [date(2020, 6, 1), date(2021, 6, 1)]}).with_columns(
        pl.col("ex_date").cast(dtype))
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2000, 1, 1, tzinfo=UTC))
    receipt = lake.inputs.freeze({"x": RawInput("custom", "events", view="versions")}, information_cutoff="2021-12-31")
    work = []
    with lake.inputs.read_context(receipt, progress=work.append):
        selected = lake.inputs.read(receipt, "x", date_range=("ex_date", "2021-06-01", "2021-06-30")).collect()
    assert selected.height == 1 and selected.schema["ex_date"] == dtype
    assert sum(row["predicate_rows"] for row in work) == 1
    assert sum(row["row_groups_skipped"] for row in work) == 1


def test_date_lookup_honors_cancellation_without_changing_frozen_evidence(tmp_path):
    lake, _, _, receipt = fixture(tmp_path)
    class Stop(Exception):
        pass
    count = 0
    def cancel():
        nonlocal count
        count += 1
        if count >= 2:
            raise Stop
    with pytest.raises(Stop), lake.inputs.read_context(receipt, check_canceled=cancel):
        lake.inputs.read(receipt, "x", date_range=("ex_date", "2024-01-01", "2024-01-31"))
    assert lake.inputs.get(receipt).digest == receipt.digest


def test_nonmatching_oversized_date_key_respects_the_lookup_allocation(tmp_path):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("events", "by_date", date_kind="calendar", primary_key_extra=("ex_date",),
        field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "ex_date": ["invalid" * 200000]})
    lake.raw.ingest(spec, frame, mode="initialize", ingested_at=datetime(2000, 1, 1, tzinfo=UTC))
    receipt = lake.inputs.freeze({"x": RawInput("custom", "events", view="versions")}, information_cutoff="2024-01-31")
    with lake.inputs.read_context(receipt, config=ExecutionOptions(max_buffer_bytes=1024**2)):
        with pytest.raises(MemoryError, match="date-key lookup"):
            lake.inputs.read(receipt, "x", date_range=("ex_date", "2024-01-01", "2024-01-31"))


@pytest.mark.parametrize("empty", [False, True])
def test_null_only_date_key_and_empty_input_match_the_full_oracle(tmp_path, empty):
    lake = DataLake.open(data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake")
    spec = DatasetSpec("events", "by_date", date_kind="calendar", primary_key_extra=("ex_date",),
        nullable_primary_key_extra=("ex_date",), field_mappings={"time": "time", "asset_id": "asset_id"})
    frame = pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "ex_date": [None]})
    lake.raw.ingest(spec, frame.head(0) if empty else frame, mode="initialize", ingested_at=datetime(2000, 1, 1, tzinfo=UTC))
    receipt = lake.inputs.freeze({"x": RawInput("custom", "events", view="versions")}, information_cutoff="2024-01-31")
    expected = oracle(lake, receipt, date(2024, 1, 1), date(2024, 1, 31))
    actual = lake.inputs.read(receipt, "x", date_range=("ex_date", "2024-01-01", "2024-01-31")).collect()
    assert actual.equals(expected) and actual.is_empty()


def test_selected_original_preserves_returned_typed_schema_validation(tmp_path, monkeypatch):
    lake, _, _, receipt = fixture(tmp_path)
    monkeypatch.setattr("bagelquant_data.storage.recovery.read_date_range",
        lambda *_a, **_k: pl.DataFrame(schema={"ex_date": pl.Int64}))
    with pytest.raises(RuntimeError, match="typed schema/row count mismatch"):
        lake.inputs.read(receipt, "x", date_range=("ex_date", "2024-01-01", "2024-01-31"))
