from __future__ import annotations

import polars as pl
import pytest

from bagelquant_data import DataLake, DatasetSpec, LakeAdmin, LakeQuery, LakeUpdater
from bagelquant_data.core import ValidationError, default_registries
from bagelquant_data.core.normalization import NormalizeContext, NormalizeResult


def daily_spec(**overrides: object) -> DatasetSpec:
    values = {
        "name": "daily",
        "source": "custom",
        "source_dataset": "daily",
        "category": "market",
        "data_kind": "price",
        "field_mapping": {"ts_code": "ts_code", "trade_date": "trade_date"},
        "required_columns": ("asset_id", "time"),
        "primary_key": ("asset_id", "time"),
        "asset_column": "ts_code",
        "time_column": "trade_date",
        "update_type": "by_daily",
        "calendar_dataset": "trade_cal",
        "deduplication": "primary_key_last",
        "sort_columns": ("time", "asset_id"),
    }
    values.update(overrides)
    return DatasetSpec(**values)  # type: ignore[arg-type]


def daily_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "trade_date": ["20250102", "20250103"],
            "ts_code": ["000002.SZ", "000001.SZ"],
            "close": [18.40, 11.37],
        }
    )


def test_data_lake_exposes_three_primary_facades(tmp_path) -> None:
    lake = DataLake.open(tmp_path)

    assert isinstance(lake.admin, LakeAdmin)
    assert isinstance(lake.update, LakeUpdater)
    assert isinstance(lake.query, LakeQuery)
    assert lake.admin.sources is lake.sources
    assert lake.admin.datasets is lake.datasets
    assert lake.admin.status is lake.status


def test_admin_rebuild_manifest_restores_dataset_status(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.ingest_frame(daily_spec(), daily_frame())
    lake.metadata.replace_manifests("custom", "daily", [])

    assert lake.status.dataset("daily", source="custom")["row_count"] == 0

    summary = lake.admin.rebuild_manifest("daily", source="custom")

    assert summary["files_scanned"] == 1
    assert summary["rows"] == 2
    assert lake.status.dataset("daily", source="custom")["row_count"] == 2
    assert lake.admin.validate_manifest("daily", source="custom")["valid"] is True


class FakeSource:
    name = "fake"

    def __init__(self) -> None:
        self.options: dict[str, object] = {}

    def configure(self, **options: object) -> None:
        self.options.update(options)

    def test_connection(self) -> None:
        return None


def test_admin_manages_source_configuration_and_enabled_state(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.admin.sources.add(FakeSource())
    lake.admin.sources.edit("fake", token="secret", endpoint="local")
    lake.admin.sources.disable("fake")

    source = lake.admin.sources.list()[0]
    assert source["enabled"] == 0
    assert source["options"] == {"endpoint": "local", "token": "<redacted>"}

    lake.admin.sources.enable("fake")
    assert lake.admin.sources.list()[0]["enabled"] == 1


class RejectingNormalizer:
    def normalize(
        self, frame: pl.LazyFrame, spec: DatasetSpec, context: NormalizeContext
    ) -> NormalizeResult:
        accepted = frame.rename(spec.field_mapping).with_columns(
            pl.lit(context.source).alias("source"),
            pl.lit(spec.source_dataset).alias("source_dataset"),
            pl.col("ts_code").cast(pl.String).alias("asset_id"),
            pl.col("trade_date").cast(pl.String).str.strptime(pl.Date, "%Y%m%d").alias("time"),
        )
        rejected = accepted.filter(pl.col("asset_id") == "000001.SZ")
        return NormalizeResult(accepted=accepted, rejected=rejected)


def test_rejected_rows_are_visible_through_admin_status(tmp_path) -> None:
    registries = default_registries()
    registries.normalizers.register("rejecting", RejectingNormalizer())
    lake = DataLake(tmp_path, registries)

    report = lake.ingest_frame(daily_spec(normalizer="rejecting"), daily_frame())

    rows = lake.admin.rejected("daily", source="custom")
    assert report.rows_committed == 2
    assert [(row["run_id"], row["reason"], row["row_count"]) for row in rows] == [
        (report.run_id, "normalization", 1)
    ]


class BrokenNormalizer:
    def normalize(
        self, frame: pl.LazyFrame, spec: DatasetSpec, context: NormalizeContext
    ) -> NormalizeResult:
        accepted = frame.rename(spec.field_mapping)
        return NormalizeResult(accepted=accepted, rejected=accepted.filter(pl.lit(False)))


def test_staging_is_cleaned_after_commit_failure(tmp_path) -> None:
    registries = default_registries()
    registries.normalizers.register("broken", BrokenNormalizer())
    lake = DataLake(tmp_path, registries)

    with pytest.raises(ValidationError):
        lake.ingest_frame(
            daily_spec(name="broken", normalizer="broken", update_type="general", calendar_dataset=None),
            daily_frame(),
        )

    staging_root = tmp_path / "staging" / "custom" / "broken"
    assert not staging_root.exists() or not list(staging_root.rglob("*.parquet"))
