from datetime import date, datetime
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.core.exceptions import ConfigurationError
from bagelquant_data.storage.recovery import repair_partition, reconstruct


def instant(day):
    return datetime.fromisoformat(f"2026-{day}T03:00:00+08:00")


def daily_spec(**extra):
    return DatasetSpec(
        "prices",
        "by_daily",
        date_kind="calendar",
        date_param="trade_date",
        field_mappings={"trade_date": "time", "ts_code": "asset_id"},
        availability_timezone="Asia/Shanghai",
        availability_day_offset=-1,
        **extra,
    )


def price(value=100.0):
    return pl.DataFrame(
        {"trade_date": ["20260825"], "ts_code": ["A"], "close": [value]}
    )


def test_cross_month_versions_cutoffs_and_idempotence(tmp_path):
    lake = DataLake.open(tmp_path)
    spec = daily_spec()
    lake.ingest(spec, price(), mode="initialize", ingested_at=instant("08-26"))
    lake.ingest(spec, price(120.0), ingested_at=instant("09-10"))

    def read(**kwargs):
        return lake.query.query("prices", source="custom", **kwargs).collect()

    assert read(as_of_date="2026-09-08")["close"].to_list() == [100.0]
    assert read(as_of_date="2026-09-09")["close"].to_list() == [120.0]
    assert read(ingested_before=instant("09-09"))["close"].to_list() == [100.0]
    assert read(observation_start="2026-08-25", observation_end="2026-08-25")[
        "time"
    ].to_list() == [date(2026, 9, 9)]
    assert read(view="versions").height == 2
    before = lake.metadata.manifest("custom", "prices")
    assert (
        lake.ingest(spec, price(120.0), ingested_at=instant("09-11")).rows_committed
        == 0
    )
    assert lake.metadata.manifest("custom", "prices") == before
    with pytest.raises(ValueError, match="initialization"):
        lake.ingest(spec, price(999.0), mode="initialize")


def test_local_recovery_preserves_old_versions_and_metadata(tmp_path):
    lake = DataLake.open(tmp_path)
    lake.ingest(daily_spec(), price(), mode="initialize", ingested_at=instant("08-26"))
    lake.ingest(daily_spec(), price(120.0), ingested_at=instant("09-10"))
    manifest = lake.metadata.manifest("custom", "prices")
    partition = manifest[0]["partition_path"]
    expected = reconstruct(lake.parquet, "custom", "prices", partition)
    path = lake.paths.dataset_root("custom", "prices") / partition
    path.write_bytes(b"damaged")
    repair_partition(lake.parquet, "custom", "prices", partition)
    assert pl.read_parquet(path, hive_partitioning=False).equals(expected)
    assert lake.metadata.manifest("custom", "prices") == manifest
    journal = path.with_name("recovery.sqlite")
    with sqlite3.connect(journal) as db:
        db.execute("update batches set payload=x'00'")
    with pytest.raises(Exception):
        reconstruct(lake.parquet, "custom", "prices", partition)


def test_general_keeps_distinct_complete_snapshots(tmp_path):
    lake = DataLake.open(tmp_path)
    spec = DatasetSpec(
        "reference",
        "general",
        availability_timezone="Asia/Shanghai",
        availability_day_offset=-1,
    )
    lake.ingest(spec, pl.DataFrame({"code": ["A"]}), ingested_at=instant("09-09"))
    lake.ingest(spec, pl.DataFrame({"code": ["B"]}), ingested_at=instant("09-10"))
    assert lake.query.query_general("reference", source="custom").collect()[
        "code"
    ].to_list() == ["B"]
    assert lake.query.query_general(
        "reference", source="custom", as_of_date="2026-09-08"
    ).collect()["code"].to_list() == ["A"]
    partition = lake.metadata.manifest("custom", "reference")[0]["partition_path"]
    assert reconstruct(lake.parquet, "custom", "reference", partition).height == 2


def test_general_empty_update_is_a_complete_snapshot(tmp_path):
    lake = DataLake.open(tmp_path)
    spec = DatasetSpec(
        "reference",
        "general",
        availability_timezone="Asia/Shanghai",
        availability_day_offset=-1,
    )
    lake.ingest(spec, pl.DataFrame({"code": ["A"]}), ingested_at=instant("09-09"))
    lake.ingest(
        spec,
        pl.DataFrame(schema={"code": pl.String}),
        ingested_at=instant("09-10"),
    )

    assert lake.query.query_general("reference", source="custom").collect().is_empty()
    snapshots = lake.query.snapshots("reference", source="custom")
    assert [snapshot["row_count"] for snapshot in snapshots] == [1, 0]


@pytest.mark.parametrize(
    "frame",
    [pl.DataFrame({"code": ["A"]}), pl.DataFrame(schema={"code": pl.String})],
)
def test_unchanged_general_snapshot_records_only_a_check(tmp_path, frame):
    lake = DataLake.open(tmp_path)
    spec = DatasetSpec(
        "reference",
        "general",
        availability_timezone="Asia/Shanghai",
        availability_day_offset=-1,
    )
    lake.ingest(spec, frame, ingested_at=instant("09-09"))
    before_manifest = lake.metadata.manifest("custom", "reference")
    before_boundary = lake.query.freeze()

    report = lake.ingest(spec, frame, ingested_at=instant("09-10"))

    assert report.rows_committed == 0
    assert report.partitions_rewritten == 0
    assert report.partitions_skipped == 1
    assert lake.query.freeze() == before_boundary
    assert lake.metadata.manifest("custom", "reference") == before_manifest
    assert len(lake.query.snapshots("reference", source="custom")) == 1
    checks = lake.metadata._rows(
        "select visible_commit,row_count from version_checks "
        "where source='custom' and dataset='reference'"
    )
    assert checks == [{"visible_commit": before_boundary, "row_count": frame.height}]


def test_unchanged_general_snapshot_commits_new_definition_identity(tmp_path):
    lake = DataLake.open(tmp_path)
    original = DatasetSpec(
        "reference",
        "general",
        description="original definition",
        availability_timezone="Asia/Shanghai",
        availability_day_offset=-1,
    )
    revised = DatasetSpec(
        "reference",
        "general",
        description="revised definition",
        availability_timezone="Asia/Shanghai",
        availability_day_offset=-1,
    )
    frame = pl.DataFrame({"code": ["A"]})
    lake.ingest(original, frame, ingested_at=instant("09-09"))
    before_boundary = lake.query.freeze()

    report = lake.ingest(revised, frame, ingested_at=instant("09-10"))

    assert report.rows_committed == 1
    assert report.partitions_rewritten == 1
    assert lake.query.freeze() > before_boundary
    assert lake.query.query_general("reference", source="custom").collect()[
        "code"
    ].to_list() == ["A"]
    commits = lake.metadata._rows(
        "select spec_hash from version_commits "
        "where source='custom' and dataset='reference' and status='committed' "
        "order by seq"
    )
    assert len(commits) == 2
    assert commits[0]["spec_hash"] != commits[1]["spec_hash"]
    assert commits[-1]["spec_hash"] == lake.metadata.dataset_spec_hash(
        "custom", "reference"
    )


def test_general_initial_snapshot_is_not_visible_before_its_pit_date(tmp_path):
    lake = DataLake.open(tmp_path)
    spec = DatasetSpec(
        "reference",
        "general",
        availability_timezone="Asia/Shanghai",
        availability_day_offset=-1,
    )
    lake.ingest(
        spec,
        pl.DataFrame({"code": ["A"]}),
        mode="initialize",
        ingested_at=instant("09-10"),
    )

    assert lake.query.query_general(
        "reference", source="custom", as_of_date="2026-09-08"
    ).collect().is_empty()
    assert lake.query.query_general(
        "reference", source="custom", as_of_date="2026-09-09"
    ).collect()["code"].to_list() == ["A"]


class DailySource:
    name = "custom"

    def __init__(self):
        self.calls = []

    def fetch(self, dataset, request):
        self.calls.append(dict(request))
        if request["trade_date"] == "2026-08-29":
            return pl.DataFrame()
        return pl.DataFrame(
            {
                "trade_date": [request["trade_date"]],
                "ts_code": [request.get("ts_code", "A")],
                "close": [100.0],
            }
        )


def test_calendar_days_fanout_empty_scopes_and_recent_rechecks(tmp_path):
    lake = DataLake.open(tmp_path)
    source = DailySource()
    lake.admin.sources.register(source)
    lake.admin.datasets.register(
        daily_spec(source_api_param_sets=({"ts_code": ["A", "B"]},))
    )
    result = lake.update.dataset(
        "prices",
        source="custom",
        start="2026-08-25",
        end="2026-08-30",
        mode="initialize",
        ingested_at=instant("08-31"),
    )
    assert result.status == "success"
    assert len(source.calls) == 12
    scopes = lake.admin.update_scopes("prices", source="custom")
    assert len(scopes) == 12
    assert sum(s["status"] == "empty" for s in scopes) == 2
    assert all(s["scope_kind"] == "date" for s in scopes)
    source.calls.clear()
    result = lake.update.dataset(
        "prices",
        source="custom",
        start="2026-08-25",
        end="2026-08-30",
        ingested_at=instant("08-31"),
    )
    assert result.rows_committed == 0
    assert {c["trade_date"] for c in source.calls} == {
        "2026-08-28",
        "2026-08-29",
        "2026-08-30",
    }
    with pytest.raises(ConfigurationError, match="Initialization"):
        lake.update.dataset(
            "prices",
            source="custom",
            start="2026-08-25",
            end="2026-08-30",
            mode="initialize",
        )


def test_new_general_catalog_members_expand_into_daily_parameter_scopes(tmp_path):
    class ParameterSource:
        name = "custom"

        def __init__(self):
            self.calls = []

        def fetch(self, dataset, request):
            self.calls.append(dict(request))
            return pl.DataFrame(
                {
                    "trade_date": [request["trade_date"]],
                    "ts_code": [request["ts_code"]],
                    "close": [100.0],
                }
            )

    lake = DataLake.open(tmp_path)
    catalog = DatasetSpec(
        "stock_basic", "general", field_mappings={"ts_code": "asset_id"}
    )
    source = ParameterSource()
    lake.admin.sources.register(source)
    lake.ingest(catalog, pl.DataFrame({"ts_code": ["A"]}))
    lake.admin.datasets.register(
        daily_spec(
            parameter_dataset="stock_basic",
            parameter_field="asset_id",
            parameter_name="ts_code",
        )
    )
    lake.update.dataset(
        "prices",
        source="custom",
        start="2026-08-01",
        end="2026-08-05",
        mode="initialize",
    )
    source.calls.clear()

    lake.ingest(catalog, pl.DataFrame({"ts_code": ["A", "B"]}))
    lake.update.dataset(
        "prices",
        source="custom",
        start="2026-08-01",
        end="2026-08-05",
        today="2026-09-10",
    )

    assert {call["trade_date"] for call in source.calls if call["ts_code"] == "B"} == {
        f"2026-08-0{day}" for day in range(1, 6)
    }
    assert {call["trade_date"] for call in source.calls if call["ts_code"] == "A"} == {
        "2026-08-03",
        "2026-08-04",
        "2026-08-05",
    }
    coverage = lake.admin.coverage(
        "prices", source="custom", start="2026-08-01", end="2026-08-05"
    )
    assert coverage["complete"] is True
    assert coverage["missing_parameters"] == {}


def test_multiple_date_parameters_share_daily_coverage_without_duplicate_content(tmp_path):
    class AnnouncementSource:
        name = "custom"

        def __init__(self):
            self.calls = []

        def fetch(self, dataset, request):
            self.calls.append(dict(request))
            day = request.get("ann_date") or request["imp_ann_date"]
            return pl.DataFrame(
                {
                    "ann_date": [day],
                    "imp_ann_date": [day],
                    "ts_code": ["A"],
                    "end_date": ["2026-06-30"],
                    "cash_div": [1.0],
                }
            )

    lake = DataLake.open(tmp_path)
    source = AnnouncementSource()
    lake.admin.sources.register(source)
    lake.admin.datasets.register(
        DatasetSpec(
            "dividend",
            "by_daily",
            date_kind="calendar",
            date_params=("ann_date", "imp_ann_date"),
            source_time_fields=("imp_ann_date", "ann_date"),
            primary_key_extra=("end_date",),
            field_mappings={"ann_date": "time", "ts_code": "asset_id"},
        )
    )

    report = lake.update.dataset(
        "dividend",
        source="custom",
        start="2026-08-25",
        end="2026-08-25",
        mode="initialize",
    )

    assert report.request_count == 2
    assert {tuple(sorted(call)) for call in source.calls} == {
        ("ann_date",),
        ("imp_ann_date",),
    }
    assert lake.query.query(
        "dividend", source="custom", view="versions"
    ).collect().height == 1
    assert len(lake.admin.update_scopes("dividend", source="custom")) == 2


def test_refresh_rechecks_old_dates_and_dates_revisions_when_they_are_learned(tmp_path):
    class RevisingSource:
        name = "custom"

        def __init__(self):
            self.revised = False
            self.calls = []

        def fetch(self, dataset, request):
            self.calls.append(dict(request))
            value = 120.0 if self.revised and request["trade_date"] == "2026-08-01" else 100.0
            return pl.DataFrame(
                {
                    "trade_date": [request["trade_date"]],
                    "ts_code": ["A"],
                    "close": [value],
                }
            )

    lake = DataLake.open(tmp_path)
    source = RevisingSource()
    lake.admin.sources.register(source)
    lake.admin.datasets.register(daily_spec())
    lake.update.dataset(
        "prices",
        source="custom",
        start="2026-08-01",
        end="2026-08-05",
        mode="initialize",
    )
    source.revised = True
    source.calls.clear()
    ordinary = lake.update.dataset(
        "prices",
        source="custom",
        start="2026-08-01",
        end="2026-08-05",
        ingested_at=instant("09-10"),
    )
    assert ordinary.rows_committed == 0
    assert {call["trade_date"] for call in source.calls} == {
        "2026-08-03",
        "2026-08-04",
        "2026-08-05",
    }

    source.calls.clear()
    refreshed = lake.update.dataset(
        "prices",
        source="custom",
        start="2026-08-01",
        end="2026-08-05",
        mode="refresh",
        ingested_at=instant("09-10"),
    )
    assert refreshed.rows_committed == 1
    assert {call["trade_date"] for call in source.calls} == {
        f"2026-08-0{day}" for day in range(1, 6)
    }
    assert lake.query.query(
        "prices", source="custom", as_of_date="2026-09-08"
    ).collect().filter(pl.col("source_time") == date(2026, 8, 1))["close"].item() == 100.0
    assert lake.query.query(
        "prices", source="custom", as_of_date="2026-09-09"
    ).collect().filter(pl.col("source_time") == date(2026, 8, 1))["close"].item() == 120.0


def test_baseline_repair_restores_missing_observation_to_its_source_date(
    tmp_path,
) -> None:
    lake = DataLake.open(tmp_path)
    spec = daily_spec()
    lake.ingest(
        spec,
        pl.DataFrame(
            {"trade_date": ["20260824"], "ts_code": ["A"], "close": [90.0]}
        ),
        mode="initialize",
        ingested_at=instant("08-25"),
    )
    missing = pl.DataFrame(
        {"trade_date": ["20260825"], "ts_code": ["A"], "close": [100.0]}
    )
    lake.ingest(spec, missing, ingested_at=instant("09-10"))

    committed = lake._pipeline.commit_frame(
        spec,
        missing,
        run_id="baseline-repair",
        mode="incremental",
        ingested_at=instant("09-11"),
        baseline_repair=True,
    )

    assert committed.rows_committed == 1
    repaired = lake.query.query(
        "prices",
        source="custom",
        as_of_date="2026-08-25",
    ).collect()
    assert repaired.filter(pl.col("source_time") == date(2026, 8, 25))[
        "close"
    ].item() == 100.0
    versions = lake.query.query(
        "prices",
        source="custom",
        observation_start="2026-08-25",
        observation_end="2026-08-25",
        view="versions",
    ).collect()
    assert versions.height == 2
    assert versions.filter(pl.col("_baseline"))["time"].item() == date(2026, 8, 25)


def test_repeating_pagination_never_claims_complete(tmp_path):
    lake = DataLake.open(tmp_path)
    source = DailySource()
    lake.admin.sources.register(source)
    lake.admin.datasets.register(
        daily_spec(request_options={"pagination": "offset", "page_size": 1})
    )
    result = lake.update.dataset(
        "prices",
        source="custom",
        start="2026-08-25",
        end="2026-08-25",
        mode="initialize",
        max_retries=1,
    )
    assert result.status == "failed"
    assert result.rows_committed == 0
    assert len(source.calls) == 2
    assert lake.admin.update_scopes("prices", source="custom")[0]["status"] == "invalid"


def test_old_schema_rejected_without_writes(tmp_path):
    path = tmp_path / "metadata" / "lake.db"
    path.parent.mkdir()
    with sqlite3.connect(path) as db:
        db.execute("create table metadata_state(key text primary key,value text)")
        db.execute("insert into metadata_state values('schema_version','3')")
    original = path.read_bytes()
    files = set(tmp_path.rglob("*"))
    with pytest.raises(ConfigurationError, match="Incompatible"):
        DataLake.open(tmp_path)
    assert path.read_bytes() == original
    assert set(tmp_path.rglob("*")) == files


def test_same_pit_day_versions_and_frozen_commit(tmp_path):
    lake = DataLake.open(tmp_path)
    lake.ingest(daily_spec(), price(), mode="initialize", ingested_at=instant("08-26"))
    boundary = lake.query.freeze()
    lake.ingest(daily_spec(), price(120), ingested_at=instant("09-10"))
    lake.ingest(daily_spec(), price(125), ingested_at=instant("09-10"))
    assert lake.query.query("prices", source="custom", max_commit=boundary).collect()["close"].to_list() == [100]
    assert lake.query.query("prices", source="custom", as_of_date="2026-09-09").collect()["close"].to_list() == [125]
    assert lake.admin.validate_dataset("prices", source="custom", deep=True)["valid"]


def test_damaged_journal_can_only_be_restored_from_verified_parquet(tmp_path):
    lake = DataLake.open(tmp_path)
    lake.ingest(daily_spec(), price(), mode="initialize", ingested_at=instant("08-26"))
    row = lake.metadata.manifest("custom", "prices")[0]
    partition = row["partition_path"]
    path = lake.paths.dataset_root("custom", "prices") / partition
    path.with_name("recovery.sqlite").write_bytes(b"broken journal")
    repair_partition(lake.parquet, "custom", "prices", partition)
    assert reconstruct(lake.parquet, "custom", "prices", partition).height == 1
    path.with_name("recovery.sqlite").write_bytes(b"broken journal")
    path.write_bytes(b"broken parquet")
    with pytest.raises(Exception):
        repair_partition(lake.parquet, "custom", "prices", partition)
    assert lake.metadata.manifest("custom", "prices")[0] == row
    report = lake.admin.validate_dataset("prices", source="custom", deep=True)
    recovery_issue = next(
        issue for issue in report["issues"] if issue["code"] == "recovery_evidence"
    )
    assert recovery_issue["repairable"] is False
    assert "historical recovery is blocked" in recovery_issue["detail"]


def test_failed_manifest_commit_leaves_no_visible_new_versions(tmp_path, monkeypatch):
    lake = DataLake.open(tmp_path)
    lake.ingest(daily_spec(), price(), mode="initialize", ingested_at=instant("08-26"))
    before = lake.metadata.manifest("custom", "prices")
    original = lake.parquet.commit_metadata
    def fail(*args, **kwargs):
        raise RuntimeError("injected metadata interruption")
    monkeypatch.setattr(lake.parquet, "commit_metadata", fail)
    with pytest.raises(RuntimeError, match="injected"):
        lake.ingest(daily_spec(), price(120), ingested_at=instant("09-10"))
    assert lake.metadata.manifest("custom", "prices") == before
    assert lake.query.query("prices", source="custom", view="versions").collect().height == 1
    monkeypatch.setattr(lake.parquet, "commit_metadata", original)
    lake.ingest(daily_spec(), price(120), ingested_at=instant("09-10"))
    assert lake.query.query("prices", source="custom", view="versions").collect().height == 2


def test_general_failed_variant_preserves_previous_complete_snapshot(tmp_path):
    class Source:
        name = "custom"
        fail = False
        def fetch(self, dataset, request):
            if self.fail and request["group"] == "B":
                raise RuntimeError("partial snapshot")
            return pl.DataFrame({"code": [request["group"]]})
    lake = DataLake.open(tmp_path)
    source = Source()
    lake.admin.sources.register(source)
    lake.admin.datasets.register(DatasetSpec("reference", "general", source_api_param_sets=({"group": ["A", "B"]},)))
    lake.update.dataset("reference", source="custom", end="2026-09-10", max_retries=1)
    boundary = lake.query.freeze()
    source.fail = True
    result = lake.update.dataset("reference", source="custom", end="2026-09-10", max_retries=1)
    assert result.status == "failed"
    assert lake.query.freeze() == boundary
    assert sorted(lake.query.query_general("reference", source="custom").collect()["code"]) == ["A", "B"]


def test_changed_schema_recovery_keeps_partition_schema(tmp_path):
    lake = DataLake.open(tmp_path)
    lake.ingest(daily_spec(), price(), mode="initialize", ingested_at=instant("08-26"))
    lake.ingest(daily_spec(), price(120).with_columns(pl.lit(1).alias("extra")), ingested_at=instant("09-10"))
    for row in lake.metadata.manifest("custom", "prices"):
        reconstruct(lake.parquet, "custom", "prices", row["partition_path"])
    assert lake.admin.validate_dataset("prices", source="custom", deep=True)["valid"]
