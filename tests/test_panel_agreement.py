from __future__ import annotations

import pandas as pd
import pytest

from bagelquant_data.loader import PanelInputAgreement
from bagelquant_data.metadata import DomainSpec
from bagelquant_data.utils.exceptions import ContractValidationError


def test_numeric_panel_agreement_rejects_non_numeric_data() -> None:
    frame = pd.DataFrame({"a": ["x"]}, index=pd.to_datetime(["2024-01-01"]))

    with pytest.raises(ContractValidationError):
        PanelInputAgreement(
            kind="numeric_panel",
            frame=frame,
            domain_spec=DomainSpec(
                region="CN",
                universe=("a",),
                start_date="2024-01-01",
                end_date="2024-01-01",
            ),
            dataset_name="bad",
        )


def test_category_panel_agreement_allows_non_numeric_data() -> None:
    frame = pd.DataFrame({"a": ["tech"]}, index=pd.to_datetime(["2024-01-01"]))

    agreement = PanelInputAgreement(
        kind="category_panel",
        frame=frame,
        domain_spec=DomainSpec(
            region="CN",
            universe=("a",),
            start_date="2024-01-01",
            end_date="2024-01-01",
        ),
        dataset_name="industry",
    )

    assert agreement.frame.loc[pd.Timestamp("2024-01-01"), "a"] == "tech"


def test_domain_spec_matches_core_constructor_shape() -> None:
    spec = DomainSpec(
        region="CN",
        universe=("000001.SZ", "600000.SH"),
        start_date="2024-01-01",
        end_date="2024-12-31",
    )

    assert spec.to_core_kwargs() == {
        "region": "CN",
        "universe": ["000001.SZ", "600000.SH"],
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
    }


def test_panel_payload_is_defensive_copy() -> None:
    frame = pd.DataFrame({"a": [1.0]}, index=pd.to_datetime(["2024-01-01"]))
    agreement = PanelInputAgreement(
        kind="numeric_panel",
        frame=frame,
        domain_spec=DomainSpec(
            region="CN",
            universe=("a",),
            start_date="2024-01-01",
            end_date="2024-01-01",
        ),
        dataset_name="close",
    )

    payload = agreement.to_payload()
    payload["frame"].loc[pd.Timestamp("2024-01-01"), "a"] = 99.0

    assert agreement.frame.loc[pd.Timestamp("2024-01-01"), "a"] == 1.0
