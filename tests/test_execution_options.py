"""Required execution limits cannot silently become optional."""
import pytest

from bagelquant_data import ExecutionOptions


@pytest.mark.parametrize("field", ["workers", "max_buffer_bytes"])
@pytest.mark.parametrize("value", [None, True, 1.5, float("nan"), 0, -1])
def test_required_execution_limits_are_positive_integers(field, value):
    with pytest.raises(ValueError, match=field):
        ExecutionOptions(**{field: value})


def test_optional_execution_limits_keep_none_and_default_admission():
    options = ExecutionOptions(workers=2, max_in_flight=None, batch_size=None,
        commit_batch_rows=None, observation_window_days=None)
    assert options.update_options()["max_in_flight"] == 2
    assert "batch_size" not in options.update_options()
