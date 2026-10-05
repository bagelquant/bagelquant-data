from datetime import UTC, date, datetime

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, DatasetSpec, ItemInput, RawInput


def _lake(root):
    return DataLake.open(data_meta_path=root / "data_meta.sqlite", lake_path=root / "lake")


def _rows():
    return pl.DataFrame({"time": [date(2020, 1, 1)], "asset_id": ["A"], "value": [1.0]})


def _received(day):
    return datetime(2020, 1, day, tzinfo=UTC)


@pytest.mark.parametrize("kind", ["by_date", "general", "empty_general", "item"])
def test_verified_dependency_publication_does_not_repeat_prefix_selection(tmp_path, monkeypatch, kind):
    lake = _lake(tmp_path)
    if kind == "item":
        lake.items.register(DataItemSpec("input"))
        lake.items.ingest("input", _rows(), available_date="2020-01-01")
        request = ItemInput("input", view="versions")
    else:
        spec = DatasetSpec("input", "by_date" if kind == "by_date" else "general",
                           date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"})
        lake.raw.ingest(spec, _rows().head(0) if kind == "empty_general" else _rows(),
                        ingested_at=_received(1))
        request = RawInput("custom", "input", view="versions", include_historical_baseline=True)
    inputs = lake.inputs.freeze({"input": request}, information_cutoff="2020-01-03")
    lake.items.register(DataItemSpec("output", (request,)))
    output = pl.concat([_rows().with_columns(pl.lit(date(2020, 1, day)).alias("time")) for day in range(1, 4)])

    def redundant_selection(*args, **kwargs):
        pytest.fail("Verified timing must not require per-date prefix selection")

    with monkeypatch.context() as patch:
        patch.setattr(lake.inputs, "_select", redundant_selection)
        lake.items.ingest("output", output, available_date="2020-01-01", input_receipt=inputs)
    result = lake.items.read("output", view="versions", strict=True).collect()
    assert result["value"].to_list() == [1.0, 1.0, 1.0]
    assert result["_baseline"].to_list() == [False, False, False]
    assert result["version_available_date"].to_list() == [date(2020, 1, day) for day in range(1, 4)]
    assert lake.inputs.verify(inputs)["valid"]


@pytest.mark.parametrize("kind", ["by_date", "general", "empty_general"])
def test_retained_baselines_and_exact_attestations_keep_per_cutoff_classification(tmp_path, kind):
    lake = _lake(tmp_path)
    spec = DatasetSpec("input", "by_date" if kind == "by_date" else "general",
                       date_kind="calendar", field_mappings={"time": "time", "asset_id": "asset_id"})
    source = _rows().head(0) if kind == "empty_general" else _rows()
    lake.raw.ingest(spec, source, mode="initialize", ingested_at=_received(1))
    request = RawInput("custom", "input", view="versions", include_historical_baseline=True)
    before = lake.inputs.freeze({"input": request}, information_cutoff="2020-01-03")
    lake.raw.ingest(spec, source, ingested_at=_received(3))
    after = lake.inputs.freeze({"input": request}, information_cutoff="2020-01-03")
    output = pl.concat([_rows().with_columns(pl.lit(date(2020, 1, day)).alias("time")) for day in (1, 3)])
    for name, receipt in (("captured", before), ("attested", after)):
        lake.items.register(DataItemSpec(name, (request,)))
        lake.items.ingest(name, output, available_date="2020-01-01", input_receipt=receipt)
    assert lake.items.read("captured", strict=True, view="versions").collect().is_empty()
    attested = lake.items.read("attested", view="versions").collect()
    assert attested["_baseline"].to_list() == [True, False]
    assert lake.items.read("attested", view="snapshot", as_of="2020-01-02", strict=True).collect().is_empty()
    assert lake.items.read("attested", view="snapshot", as_of="2020-01-03", strict=True).collect()["time"].to_list() == [date(2020, 1, 3)]
