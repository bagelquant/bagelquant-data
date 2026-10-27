"""Mapping input retains strict integer timing instead of truncating values."""
import pytest

from bagelquant_data import AvailabilityPolicy


def declaration(**changes):
    return {"observation_date_field": "observation_date",
        "base_available_date_field": "available_date", "availability_buffer_sessions": 0,
        "alignment": "exact_daily", "calendar_id": "custom/calendar", **changes}


@pytest.mark.parametrize("value", [True, False, 1.0, 1.9, -0.5])
@pytest.mark.parametrize("field", ["availability_buffer_sessions", "max_forward_fill_sessions"])
def test_mapping_cannot_bypass_integer_session_contract(field, value):
    changes = {field: value}
    if field == "max_forward_fill_sessions":
        changes["alignment"] = "forward_fill"
    with pytest.raises(ValueError, match="integer"):
        AvailabilityPolicy.from_mapping(declaration(**changes))


def test_integer_strings_and_optional_fill_limit_remain_supported():
    exact = AvailabilityPolicy.from_mapping(declaration(availability_buffer_sessions="  +2 "))
    assert exact.availability_buffer_sessions == 2 and exact.max_forward_fill_sessions is None
    fill = AvailabilityPolicy.from_mapping(declaration(alignment="forward_fill",
        availability_buffer_sessions="0", max_forward_fill_sessions="2"))
    assert fill.availability_buffer_sessions == 0 and fill.max_forward_fill_sessions == 2
