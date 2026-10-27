"""Authored metadata must not be mistaken for immutable schema encoding."""
from copy import deepcopy
from datetime import date
import json
import sqlite3

import polars as pl
import pytest

from bagelquant_data import DataItemSpec, DataLake, ItemInput


def populated_lake(tmp_path, metadata):
    lake = DataLake.open(data_meta_path=tmp_path / "data.sqlite", lake_path=tmp_path / "lake")
    lake.items.register(DataItemSpec("notes", metadata=metadata))
    lake.items.ingest("notes", pl.DataFrame({"time": [date(2020, 1, 1)],
        "asset_id": ["A"], "value": [1.0]}), available_date="2020-01-01")
    return lake


@pytest.mark.parametrize("note", ["retained human note", "aB", "ab cd", "ab\ncd",
    "aB" * 200, "", "abc", "Research 研究"])
def test_schema_named_authored_metadata_roundtrips_exactly(tmp_path, note):
    metadata = {"schema_ipc": note, "nested": [{"schema_ipc": note}]}
    with populated_lake(tmp_path, metadata) as lake:
        frozen = lake.inputs.freeze({"item": ItemInput("notes")}, information_cutoff="2020-01-01")
        assert json.dumps(deepcopy(frozen.evidence["item"]["definition"]["metadata"]), sort_keys=True) == json.dumps(metadata, sort_keys=True)
        lake.inputs.verify(frozen)
        assert lake.inputs.read(frozen, "item").collect()["value"].to_list() == [1.0]


def test_registered_schema_corruption_still_fails_without_rewriting_receipt(tmp_path):
    with populated_lake(tmp_path, {"author": "retained"}) as lake:
        frozen = lake.inputs.freeze({"item": ItemInput("notes")}, information_cutoff="2020-01-01")
        digest = frozen.digest
        with sqlite3.connect(lake.data_meta_path) as db:
            db.execute("UPDATE immutable_schemas SET schema_ipc=x'00'")
        with pytest.raises(RuntimeError, match="schema checksum mismatch"):
            lake.inputs.verify(frozen)
        assert lake.inputs.get(frozen.receipt_id).digest == digest
