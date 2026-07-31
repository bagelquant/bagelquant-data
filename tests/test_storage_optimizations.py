from __future__ import annotations

import json
import sqlite3
import threading
from datetime import date

import polars as pl
import pyarrow.parquet as pq
import pytest

from bagelquant_data import DataLake, DatasetSpec
from bagelquant_data.core import DatasetSpecError, ValidationError
from bagelquant_data.core.hashing import frame_content_hash, stable_bucket
from bagelquant_data.pipeline import commit as commit_module
from bagelquant_data.storage import parquet as parquet_module
from bagelquant_data.storage import atomic as atomic_module
from bagelquant_data.storage.atomic import atomic_write_parquet


class StableDailySource:
    name = "custom"

    def fetch(self, dataset: str, request: dict[str, object]) -> pl.DataFrame:
        value = str(request["date"]).replace("-", "")
        return pl.DataFrame(
            {
                "trade_date": [value],
                "ts_code": ["000001.SZ"],
                "close": [10.0],
            }
        )


def _daily_spec() -> DatasetSpec:
    return DatasetSpec(
        "daily",
        "by_daily",
        calendar="trade_cal",
        field_mappings={"trade_date": "time", "ts_code": "asset_id"},
    )


def _asset_spec(bucket_count: int = 32) -> DatasetSpec:
    return DatasetSpec(
        "income",
        "by_asset",
        asset_list="stock_basic",
        asset_bucket_count=bucket_count,
        field_mappings={"ann_date": "time", "ts_code": "asset_id"},
    )


def test_arrow_content_hash_is_logical_and_schema_sensitive() -> None:
    base = pl.DataFrame(
        {
            "asset_id": ["A", "B", "C"],
            "value": pl.Series([1, 2, 3], dtype=pl.Int64),
            "nullable": pl.Series([None, "x", None], dtype=pl.String),
        }
    )
    chunked = pl.concat([base.slice(0, 1), base.slice(1)], rechunk=False)
    reordered = base.reverse()

    assert frame_content_hash(base) == frame_content_hash(chunked)
    assert frame_content_hash(base) == frame_content_hash(reordered)
    assert frame_content_hash(base).startswith("arrow-ipc-v1:")
    assert frame_content_hash(base) != frame_content_hash(
        base.with_columns(pl.col("value").cast(pl.Float64))
    )
    assert frame_content_hash(base) != frame_content_hash(
        base.with_columns(
            pl.when(pl.col("asset_id") == "A")
            .then(pl.lit("y"))
            .otherwise(pl.col("nullable"))
            .alias("nullable")
        )
    )
    assert frame_content_hash(base) != frame_content_hash(
        base.with_columns(pl.col("value") + 1)
    )


def test_identical_ingest_skips_partition_and_preserves_manifest_and_file(
    tmp_path,
) -> None:
    lake = DataLake.open(tmp_path)
    spec = _daily_spec()
    frame = pl.DataFrame(
        {
            "trade_date": ["20250102"],
            "ts_code": ["000001.SZ"],
            "close": [10.0],
        }
    )

    first = lake.ingest(spec, frame)
    manifest_before = lake.metadata.manifest("custom", "daily")
    path = lake.paths.dataset_root("custom", "daily") / str(
        manifest_before[0]["partition_path"]
    )
    mtime_before = path.stat().st_mtime_ns
    second = lake.ingest(spec, frame)

    assert first.partitions_rewritten == 1
    assert second.partitions_rewritten == 0
    assert second.partitions_skipped == 1
    assert path.stat().st_mtime_ns == mtime_before
    assert lake.metadata.manifest("custom", "daily") == manifest_before


def test_compatibility_write_returns_clean_manifest_on_noop(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    spec = _daily_spec()
    lake.ingest(
        spec,
        pl.DataFrame(
            {"trade_date": ["20250102"], "ts_code": ["A"], "close": [1.0]}
        ),
    )
    stored = lake.metadata.manifest("custom", "daily")[0]
    relative = lake.paths.dataset_root("custom", "daily") / str(
        stored["partition_path"]
    )

    _, manifest = lake.parquet.write_partition_file(
        spec,
        pl.read_parquet(relative),
        relative.relative_to(lake.paths.dataset_root("custom", "daily")),
        {"year": 2025, "month": 1},
    )
    lake.metadata.upsert_manifest(**manifest)

    assert "updated_at" not in manifest
    assert manifest["partition_values"] == {"year": 2025, "month": 1}


def test_general_replacement_does_not_retain_removed_columns(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    spec = DatasetSpec("stock_basic", "general")
    lake.ingest(spec, pl.DataFrame({"asset_id": ["A"], "name": ["old"]}))
    lake.ingest(spec, pl.DataFrame({"asset_id": ["A"]}))

    frame = lake.query.query_general("stock_basic", source="custom").collect()

    assert frame.columns == ["asset_id", "source"]


def test_noop_update_still_completes_scope_successfully(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    lake.admin.sources.register(StableDailySource())
    lake.ingest(
        DatasetSpec("trade_cal", "general"),
        pl.DataFrame({"time": ["20250102"], "is_open": [1]}),
    )
    lake.admin.datasets.register(_daily_spec())

    first = lake.update.dataset(
        "daily",
        source="custom",
        start="2025-01-02",
        end="2025-01-02",
        today="2025-01-02",
        progress=False,
    )
    manifest_before = lake.metadata.manifest("custom", "daily")
    second = lake.update.dataset(
        "daily",
        source="custom",
        start="2025-01-02",
        end="2025-01-02",
        today="2025-01-03",
        progress=False,
    )
    scope = lake.admin.status.update_scopes(dataset="daily", source="custom")[0]

    assert first.partitions_rewritten == 1
    assert second.partitions_rewritten == 0
    assert second.partitions_skipped == 1
    assert scope["status"] == "success"
    assert lake.metadata.manifest("custom", "daily") == manifest_before


def test_new_asset_rewrites_only_its_year_bucket(tmp_path) -> None:
    anchor = "000001.SZ"
    candidate = next(
        f"{value:06d}.SZ"
        for value in range(2, 10_000)
        if stable_bucket(f"{value:06d}.SZ", 32) != stable_bucket(anchor, 32)
    )
    newcomer = next(
        f"{value:06d}.SZ"
        for value in range(10_000, 20_000)
        if stable_bucket(f"{value:06d}.SZ", 32) == stable_bucket(candidate, 32)
    )
    lake = DataLake.open(tmp_path)
    spec = _asset_spec()
    lake.ingest(
        spec,
        pl.DataFrame(
            {
                "ann_date": ["20250630", "20250630"],
                "ts_code": [anchor, candidate],
                "value": [1.0, 2.0],
            }
        ),
    )
    before = {
        str(row["partition_path"]): str(row["content_hash"])
        for row in lake.metadata.manifest("custom", "income")
    }

    report = lake.ingest(
        spec,
        pl.DataFrame(
            {
                "ann_date": ["20250630"],
                "ts_code": [newcomer],
                "value": [3.0],
            }
        ),
    )
    after = {
        str(row["partition_path"]): str(row["content_hash"])
        for row in lake.metadata.manifest("custom", "income")
    }
    changed = {path for path in before if before[path] != after[path]}

    assert report.partitions_rewritten == 1
    assert report.partitions_skipped == 0
    assert len(changed) == 1
    assert f"bucket={stable_bucket(newcomer, 32):02d}" in changed.pop()


def test_asset_bucket_count_is_validated_and_immutable_with_data(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    with pytest.raises(DatasetSpecError, match="positive integer"):
        lake.admin.datasets.register(_asset_spec(0))
    with pytest.raises(DatasetSpecError, match="only valid for by_asset"):
        lake.admin.datasets.register(
            DatasetSpec("general", "general", asset_bucket_count=8)
        )
    invalid_toml = tmp_path / "invalid-buckets.toml"
    invalid_toml.write_text(
        'name = "income"\nupdate_type = "by_asset"\n'
        'asset_list = "stock_basic"\nasset_bucket_count = true\n'
        "[field_mappings]\nann_date = \"time\"\nts_code = \"asset_id\"\n"
    )
    with pytest.raises(DatasetSpecError, match="positive integer"):
        lake.admin.datasets.register_toml(invalid_toml)

    lake.admin.datasets.register(_asset_spec())
    default_hash = lake.metadata.dataset_spec_hash("custom", "income")
    lake.admin.datasets.register(_asset_spec(8))
    assert lake.metadata.dataset_spec_hash("custom", "income") != default_hash

    lake.ingest(
        _asset_spec(),
        pl.DataFrame({"ann_date": ["20250630"], "ts_code": ["A"], "value": [1.0]}),
    )
    with pytest.raises(DatasetSpecError, match="clear the dataset"):
        lake.admin.datasets.register(_asset_spec(8))

    lake.admin.datasets.clear_dataset_data("income", source="custom", confirm=True)
    assert lake.admin.datasets.register(_asset_spec(8)).asset_bucket_count == 8


def test_schema_reconciliation_handles_null_numeric_and_missing_columns(
    tmp_path,
) -> None:
    lake = DataLake.open(tmp_path)
    spec = _daily_spec()
    lake.ingest(
        spec,
        pl.DataFrame(
            {
                "trade_date": ["20250102"],
                "ts_code": ["A"],
                "value": pl.Series([1], dtype=pl.Int64),
                "nullable": pl.Series([None], dtype=pl.String),
                "old_only": [7],
            }
        ),
    )
    lake.ingest(
        spec,
        pl.DataFrame(
            {
                "trade_date": ["20250203"],
                "ts_code": ["B"],
                "value": pl.Series([2.5], dtype=pl.Float64),
                "nullable": pl.Series([3.5], dtype=pl.Float64),
                "new_only": [9],
            }
        ),
    )
    lake.ingest(
        spec,
        pl.DataFrame(
            {
                "trade_date": ["20250304"],
                "ts_code": ["C"],
                "value": pl.Series(["4.5"], dtype=pl.String),
                "nullable": pl.Series([None], dtype=pl.String),
            }
        ),
    )

    frame = lake.query.query("daily", source="custom").collect().sort("time")

    assert frame.schema["value"] == pl.Float64
    assert frame.schema["nullable"] == pl.Float64
    assert frame["value"].to_list() == [1.0, 2.5, 4.5]
    assert frame["nullable"].to_list() == [None, 3.5, None]
    assert frame["old_only"].to_list() == [7, None, None]
    assert frame["new_only"].to_list() == [None, 9, None]


def test_unparseable_string_numeric_conflict_fails_commit(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    spec = _daily_spec()
    lake.ingest(
        spec,
        pl.DataFrame(
            {
                "trade_date": ["20250102"],
                "ts_code": ["A"],
                "value": pl.Series([1.0], dtype=pl.Float64),
            }
        ),
    )

    with pytest.raises(ValidationError, match="canonical schema"):
        lake.ingest(
            spec,
            pl.DataFrame(
                {
                    "trade_date": ["20250203"],
                    "ts_code": ["B"],
                    "value": pl.Series(["not-a-number"], dtype=pl.String),
                }
            ),
        )

    assert lake.query.query("daily", source="custom").collect().height == 1


def test_batch_write_failure_restores_all_old_partitions(
    tmp_path, monkeypatch
) -> None:
    lake = DataLake.open(tmp_path)
    spec = _daily_spec()
    lake.ingest(
        spec,
        pl.DataFrame(
            {
                "trade_date": ["20250102", "20250203"],
                "ts_code": ["A", "A"],
                "value": [1.0, 2.0],
            }
        ),
    )
    root = lake.paths.dataset_root("custom", "daily")
    before_manifest = lake.metadata.manifest("custom", "daily")
    before_frames = {
        str(row["partition_path"]): pl.read_parquet(
            root / str(row["partition_path"])
        )
        for row in before_manifest
    }
    writes = 0
    real_write = parquet_module.atomic_write_parquet

    def fail_second_write(
        frame: pl.DataFrame, path, *, expected_schema=None
    ) -> None:
        nonlocal writes
        writes += 1
        if writes == 2:
            raise PermissionError("fault injection")
        real_write(frame, path, expected_schema=expected_schema)

    monkeypatch.setattr(
        parquet_module, "atomic_write_parquet", fail_second_write
    )
    with pytest.raises(PermissionError, match="fault injection"):
        lake.ingest(
            spec,
            pl.DataFrame(
                {
                    "trade_date": ["20250102", "20250203"],
                    "ts_code": ["A", "A"],
                    "value": [10.0, 20.0],
                }
            ),
        )

    assert lake.metadata.manifest("custom", "daily") == before_manifest
    assert all(
        pl.read_parquet(root / relative).equals(frame)
        for relative, frame in before_frames.items()
    )
    assert not list(root.rglob("*.tmp"))
    assert not list(root.rglob("*.rollback"))


def test_grouped_commit_parallelizes_writers_without_worker_sqlite(
    tmp_path, monkeypatch
) -> None:
    lake = DataLake.open(tmp_path)
    main_thread = threading.get_ident()
    manifest_calls: list[int] = []
    context_calls = 0
    active = 0
    peak = 0
    lock = threading.Lock()
    rendezvous = threading.Barrier(4)
    real_manifest = lake.metadata.manifest
    real_context = commit_module.partition_write_context
    real_write = parquet_module.atomic_write_parquet

    def checked_manifest(source: str, dataset: str):
        manifest_calls.append(threading.get_ident())
        assert threading.get_ident() == main_thread
        return real_manifest(source, dataset)

    def tracked_context(schema):
        nonlocal context_calls
        context_calls += 1
        return real_context(schema)

    def tracked_write(frame, path, *, expected_schema=None):
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
        try:
            rendezvous.wait(timeout=5)
            real_write(frame, path, expected_schema=expected_schema)
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(lake.metadata, "manifest", checked_manifest)
    monkeypatch.setattr(
        commit_module, "partition_write_context", tracked_context
    )
    monkeypatch.setattr(parquet_module, "atomic_write_parquet", tracked_write)

    result = lake.ingest(
        _daily_spec(),
        pl.DataFrame(
            {
                "trade_date": [
                    "20250102",
                    "20250203",
                    "20250303",
                    "20250401",
                    "20250502",
                    "20250602",
                    "20250701",
                    "20250801",
                ],
                "ts_code": ["A"] * 8,
                "value": list(range(8)),
            }
        ),
    )

    assert result.partitions_rewritten == 8
    assert 2 <= peak <= 4
    assert manifest_calls
    assert set(manifest_calls) == {main_thread}
    assert context_calls == 1


def test_partition_write_context_matches_parquet_physical_schema(tmp_path) -> None:
    schema = pl.Schema(
        {
            "text": pl.String,
            "day": pl.Date,
            "missing": pl.Null,
            "values": pl.List(pl.Int64),
        }
    )
    frame = pl.DataFrame(
        {
            "text": ["x"],
            "day": [date(2025, 1, 2)],
            "missing": [None],
            "values": [[1, 2]],
        },
        schema=schema,
    )
    context = parquet_module.partition_write_context(schema)
    path = tmp_path / "physical.parquet"

    atomic_write_parquet(frame, path, expected_schema=context.arrow_schema)

    parquet_file = pq.ParquetFile(path)
    try:
        assert parquet_file.schema_arrow.equals(context.arrow_schema)
    finally:
        parquet_file.close()


def test_hash_failure_drains_writers_and_removes_temporary_files(
    tmp_path, monkeypatch
) -> None:
    lake = DataLake.open(tmp_path)
    spec = _daily_spec()
    original = pl.DataFrame(
        {
            "trade_date": ["20250102", "20250203", "20250303", "20250401"],
            "ts_code": ["A"] * 4,
            "value": [1.0, 2.0, 3.0, 4.0],
        }
    )
    lake.ingest(spec, original)
    root = lake.paths.dataset_root("custom", "daily")
    before_manifest = lake.metadata.manifest("custom", "daily")
    before_frames = {
        str(row["partition_path"]): pl.read_parquet(
            root / str(row["partition_path"])
        )
        for row in before_manifest
    }
    real_hash = parquet_module.frame_content_hash
    calls = 0
    lock = threading.Lock()

    def fail_one_hash(frame):
        nonlocal calls
        with lock:
            calls += 1
            call = calls
        if call == 2:
            raise RuntimeError("hash fault injection")
        return real_hash(frame)

    monkeypatch.setattr(parquet_module, "frame_content_hash", fail_one_hash)
    with pytest.raises(RuntimeError, match="hash fault injection"):
        lake.ingest(
            spec,
            original.with_columns(pl.col("value") + 10),
        )

    assert 2 <= calls <= 4
    assert lake.metadata.manifest("custom", "daily") == before_manifest
    assert all(
        pl.read_parquet(root / relative).equals(frame)
        for relative, frame in before_frames.items()
    )
    assert not list(root.rglob("*.tmp"))
    assert not list(root.rglob("*.rollback"))


@pytest.mark.parametrize("failure_stage", ["read_back", "replace"])
def test_atomic_failures_preserve_old_file_and_remove_temporary_file(
    tmp_path, monkeypatch, failure_stage
) -> None:
    path = tmp_path / "data.parquet"
    original = pl.DataFrame({"value": [1]})
    atomic_write_parquet(original, path)
    if failure_stage == "read_back":
        monkeypatch.setattr(
            atomic_module.pq,
            "ParquetFile",
            lambda _: (_ for _ in ()).throw(RuntimeError("read-back fault")),
        )
    else:
        monkeypatch.setattr(
            atomic_module.os,
            "replace",
            lambda *_: (_ for _ in ()).throw(
                PermissionError("replace fault")
            ),
        )
        monkeypatch.setattr(atomic_module.time, "sleep", lambda _: None)

    with pytest.raises(
        (RuntimeError, PermissionError), match=f"{failure_stage.split('_')[0]}"
    ):
        atomic_write_parquet(pl.DataFrame({"value": [2]}), path)

    assert pl.read_parquet(path).equals(original)
    assert not list(tmp_path.glob("*.tmp"))


def test_metadata_commit_failure_restores_parquet_manifest_and_schema(
    tmp_path, monkeypatch
) -> None:
    lake = DataLake.open(tmp_path)
    spec = _daily_spec()
    lake.ingest(
        spec,
        pl.DataFrame(
            {"trade_date": ["20250102"], "ts_code": ["A"], "value": [1.0]}
        ),
    )
    root = lake.paths.dataset_root("custom", "daily")
    before_manifest = lake.metadata.manifest("custom", "daily")
    path = root / str(before_manifest[0]["partition_path"])
    before_frame = pl.read_parquet(path)
    before_schema = lake.metadata.dataset_schema("custom", "daily")

    def fail_metadata(*args, **kwargs) -> None:
        raise sqlite3.OperationalError("fault injection")

    monkeypatch.setattr(
        lake.metadata, "commit_dataset_metadata", fail_metadata
    )
    with pytest.raises(sqlite3.OperationalError, match="fault injection"):
        lake.ingest(
            spec,
            pl.DataFrame(
                {
                    "trade_date": ["20250102"],
                    "ts_code": ["A"],
                    "value": [2.0],
                }
            ),
        )

    assert pl.read_parquet(path).equals(before_frame)
    assert lake.metadata.manifest("custom", "daily") == before_manifest
    assert lake.metadata.dataset_schema("custom", "daily") == before_schema


def test_api_request_json_is_compressed_and_transparently_decoded(tmp_path) -> None:
    lake = DataLake.open(tmp_path)
    params = {"fields": ",".join(f"field_{index}" for index in range(200))}
    lake.metadata.record_api_call(
        run_id="run",
        source="custom",
        dataset="daily",
        request_key="0",
        asset_id=None,
        request_params=params,
        status="success",
        row_count=1,
    )

    with lake.metadata.connect() as db:
        raw = db.execute(
            "select typeof(request_params), length(request_params) from api_calls"
        ).fetchone()
    decoded = lake.metadata._rows("select request_params from api_calls")[0]

    assert raw[0] == "blob"
    assert int(raw[1]) < len(json.dumps(params))
    assert json.loads(str(decoded["request_params"])) == params
