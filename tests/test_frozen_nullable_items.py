"""Frozen Item types survive windows containing only missing observations."""
from datetime import date

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, ItemInput


@pytest.mark.parametrize("dtype,name", [(pl.Float64, "Float64"), (pl.Int64, "Int64"), (pl.String, "String")])
@pytest.mark.parametrize("view", ["snapshot", "history", "versions"])
def test_frozen_nullable_item_preserves_declared_type_and_receipt(tmp_path, dtype, name, view):
    lake = DataLake.open(data_meta_path=tmp_path / "data.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("nullable", value_dtype=name))
    frame = pl.DataFrame({"time": [date(2024, 1, 2)], "asset_id": ["A"],
        "value": pl.Series([None], dtype=dtype)})
    lake.items.ingest("nullable", frame, available_date="2024-01-02")
    receipt = lake.inputs.freeze({"item": ItemInput("nullable", view=view)}, information_cutoff="2024-01-03")
    digest = receipt.digest
    for start, end in (("2024-01-02", "2024-01-02"), ("2024-01-03", "2024-01-03")):
        result = lake.inputs.read(receipt, "item", start=start, end=end).collect()
        assert result.schema["value"] == dtype
        assert result["value"].null_count() == result.height
    assert lake.inputs.get(receipt).digest == digest
