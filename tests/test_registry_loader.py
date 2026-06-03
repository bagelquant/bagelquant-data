from __future__ import annotations

import pandas as pd
import pytest

from bagelquant_data.datasource import DataRequest, DataSourceRegistry
from bagelquant_data.loader import Loader
from bagelquant_data.utils.exceptions import DatasetNotFoundError


class FakeSource:
    name = "fake"

    def __init__(self) -> None:
        self.requests: list[DataRequest] = []

    def read(self, request: DataRequest) -> pd.DataFrame:
        self.requests.append(request)
        return pd.DataFrame({"value": [1]})

    def exists(self, dataset: str) -> bool:
        return dataset == "sample"

    def describe(self, dataset: str):
        return {"dataset": dataset}


def test_registry_resolves_sources() -> None:
    registry = DataSourceRegistry()
    source = FakeSource()

    registry.register(source)

    assert registry.resolve("fake") is source
    assert registry.names() == ("fake",)


def test_registry_rejects_duplicate_without_replace() -> None:
    registry = DataSourceRegistry()
    registry.register(FakeSource())

    with pytest.raises(ValueError):
        registry.register(FakeSource())


def test_registry_missing_source_error() -> None:
    with pytest.raises(DatasetNotFoundError):
        DataSourceRegistry().resolve("missing")


def test_loader_delegates_to_source_with_data_request() -> None:
    registry = DataSourceRegistry()
    source = FakeSource()
    registry.register(source)

    loaded = Loader(registry=registry).source("fake").load(
        "sample",
        fields=["a", "b"],
        filters={"asset": "x"},
        start_date="2024-01-01",
        end_date="2024-01-31",
    )

    assert loaded.data["value"].tolist() == [1]
    assert source.requests == [
        DataRequest(
            dataset="sample",
            fields=("a", "b"),
            filters={"asset": "x"},
            start_date="2024-01-01",
            end_date="2024-01-31",
        )
    ]
