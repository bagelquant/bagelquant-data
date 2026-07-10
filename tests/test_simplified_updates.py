from __future__ import annotations

from typing import cast

import polars as pl

from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.core.hashing import stable_bucket


class StaticSource:
    name = "custom"

    def __init__(self, responses: dict[str, pl.DataFrame]) -> None:
        self.responses = responses
        self.requests: list[dict[str, object]] = []

    def fetch(self, source_dataset: str, request: dict[str, object]) -> pl.DataFrame:
        self.requests.append(dict(request))
        if source_dataset == "daily":
            return pl.DataFrame(
                {
                    "trade_date": [str(request["date"]).replace("-", "")],
                    "ts_code": ["000001.SZ"],
                    "close": [10.0],
                }
            )
        if source_dataset == "fundamental":
            return pl.DataFrame(
                {
                    "ann_date": [str(request.get("start", "2025-01-01")).replace("-", "")],
                    "ts_code": [str(request["id"])],
                    "value": [1.0],
                }
            )
        return self.responses[source_dataset]


def reference_spec(name: str, source_dataset: str | None = None) -> DatasetSpec:
    return DatasetSpec(
        name=name,
        update_type="general",
        reference=True,
        source_dataset=source_dataset or name,
    )


def daily_spec() -> DatasetSpec:
    return DatasetSpec(
        name="daily",
        update_type="by_daily",
        reference="trade_cal",
        request_date_param="date",
    )


def by_id_spec() -> DatasetSpec:
    return DatasetSpec(
        name="fundamental",
        update_type="by_id",
        reference="stock_basic",
        id_column="ts_code",
        request_id_param="id",
        start_date="2025-01-01",
        batch_count=4,
    )


def test_general_update_replaces_dataset(tmp_path) -> None:
    source = StaticSource(
        {
            "reference": pl.DataFrame({"code": ["A"]}),
        }
    )
    lake = DataLake.open(tmp_path)
    lake.admin.sources.add(source)
    lake.admin.datasets.add("reference", "general", reference=True)

    lake.update.dataset("reference", source="custom", progress=False)
    source.responses["reference"] = pl.DataFrame({"code": ["B"]})
    lake.update.dataset("reference", source="custom", progress=False)

    frame = cast(pl.DataFrame, lake.query.reference("reference", source="custom", collect=True))
    assert frame["code"].to_list() == ["B"]
    assert lake.admin.status.partitions("reference", source="custom")[0]["partition_path"] == "data.parquet"


def test_compact_registration_stores_unknown_kwargs_as_source_api_params(tmp_path) -> None:
    lake = DataLake.open(tmp_path)

    spec = lake.admin.datasets.add("stock_basic", "general", reference=True, exchange="SSE", list_status="L")

    assert spec.source_dataset == "stock_basic"
    assert spec.request_options["static_params"] == {"exchange": "SSE", "list_status": "L"}


def test_by_daily_fetches_missing_calendar_dates_and_writes_year_month(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = StaticSource({})
    lake.admin.sources.add(source)
    lake.ingest_frame(
        reference_spec("trade_cal"),
        pl.DataFrame({"time": ["20250102", "20250103", "20250104"], "is_open": [1, 1, 0]}),
    )
    lake.ingest_frame(
        daily_spec(),
        pl.DataFrame({"trade_date": ["20250102"], "ts_code": ["000001.SZ"], "close": [9.0]}),
    )

    lake.update.dataset("daily", source="custom", today="2025-01-04", progress=False)

    assert source.requests == [{"date": "2025-01-03"}]
    partitions = lake.admin.status.partitions("daily", source="custom")
    assert [row["partition_path"] for row in partitions] == ["year=2025/month=01/data.parquet"]
    frame = cast(pl.DataFrame, lake.query.raw("daily", source="custom").collect())
    assert sorted(frame["time"].cast(pl.String).to_list()) == ["2025-01-02", "2025-01-03"]


def test_by_id_uses_reference_ids_latest_dates_and_stable_batch_paths(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    source = StaticSource({})
    lake.admin.sources.add(source)
    lake.ingest_frame(
        reference_spec("stock_basic"),
        pl.DataFrame({"ts_code": ["000001.SZ", "000002.SZ"]}),
    )
    lake.ingest_frame(
        by_id_spec(),
        pl.DataFrame({"ann_date": ["20250102"], "ts_code": ["000001.SZ"], "value": [0.5]}),
    )

    lake.update.dataset("fundamental", source="custom", today="2025-01-04", progress=False)

    assert source.requests == [
        {"id": "000001.SZ", "start": "2025-01-03", "end": "2025-01-04"},
        {"id": "000002.SZ", "start": "2025-01-01", "end": "2025-01-04"},
    ]
    expected_batches = {
        f"year=2025/batch={stable_bucket('000001.SZ', 4):02d}/data.parquet",
        f"year=2025/batch={stable_bucket('000002.SZ', 4):02d}/data.parquet",
    }
    partitions = {row["partition_path"] for row in lake.admin.status.partitions("fundamental", source="custom")}
    assert partitions == expected_batches
