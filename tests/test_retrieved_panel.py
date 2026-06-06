from __future__ import annotations

import pandas as pd
import pytest

from bagelquant_data.loader import RetrievedPanel
from bagelquant_data.utils.exceptions import ContractValidationError


def test_numeric_retrieved_panel_rejects_non_numeric_data() -> None:
    data = pd.DataFrame({"a": ["x"]}, index=pd.to_datetime(["2024-01-01"]))

    with pytest.raises(ContractValidationError):
        RetrievedPanel(
            kind="numeric_panel",
            data=data,
            universe=("a",),
            calendar=pd.to_datetime(["2024-01-01"]),
            dataset_name="bad",
        )


def test_category_retrieved_panel_allows_non_numeric_data() -> None:
    data = pd.DataFrame({"a": ["tech"]}, index=pd.to_datetime(["2024-01-01"]))

    retrieved = RetrievedPanel(
        kind="category_panel",
        data=data,
        universe=("a",),
        calendar=pd.to_datetime(["2024-01-01"]),
        dataset_name="industry",
    )

    assert retrieved.data.loc[pd.Timestamp("2024-01-01"), "a"] == "tech"


def test_retrieved_panel_rejects_unsorted_calendar() -> None:
    data = pd.DataFrame({"a": [1.0]}, index=pd.to_datetime(["2024-01-01"]))

    with pytest.raises(ContractValidationError, match="sorted ascending"):
        RetrievedPanel(
            kind="numeric_panel",
            data=data,
            universe=("a",),
            calendar=pd.to_datetime(["2024-01-02", "2024-01-01"]),
            dataset_name="close",
        )


def test_retrieved_panel_dict_is_defensive_copy() -> None:
    data = pd.DataFrame({"a": [1.0]}, index=pd.to_datetime(["2024-01-01"]))
    retrieved = RetrievedPanel(
        kind="numeric_panel",
        data=data,
        universe=("a",),
        calendar=pd.to_datetime(["2024-01-01"]),
        dataset_name="close",
    )

    payload = retrieved.as_dict()
    payload["data"].loc[pd.Timestamp("2024-01-01"), "a"] = 99.0

    assert retrieved.data.loc[pd.Timestamp("2024-01-01"), "a"] == 1.0
    assert retrieved.calendar[0] == pd.Timestamp("2024-01-01")
    assert payload["calendar"] is not retrieved.calendar
