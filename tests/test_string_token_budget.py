"""Exact typed-row bytes and one call-local shared token reservation."""
from collections import Counter
import hashlib
import json

import polars as pl
import pytest

from bagelquant_data.pipeline import versions


def reference_hashes(frame, fields):
    """Original typed-row-v1 whole-row JSON, independent of token caching."""
    selected = frame.select(fields)
    header = json.dumps([(name, str(dtype)) for name, dtype in selected.schema.items()],
                        separators=(",", ":")).encode()
    selected = selected.select(versions._payload_expression(pl.col(name), dtype).alias(name)
                               for name, dtype in selected.schema.items())
    base = hashlib.sha256(b"typed-row-v1\0" + header + b"\0")
    hashes = []
    for row in selected.iter_rows():
        digest = base.copy()
        digest.update(json.dumps([versions._payload_value(value) for value in row],
                                 sort_keys=True, separators=(",", ":")).encode())
        hashes.append(digest.hexdigest())
    return pl.Series("_payload_hash", hashes, dtype=pl.String)


def count_generic_strings(monkeypatch):
    original = versions.json.dumps
    calls = Counter()

    def counted(value, *args, **kwargs):
        if value is None or isinstance(value, str):
            calls[value] += 1
        return original(value, *args, **kwargs)

    monkeypatch.setattr(versions.json, "dumps", counted)
    return calls


@pytest.mark.parametrize("value", [None, "", '"\\\n\r\t', "Ω中文😀", "\x00", "line\u2028", "A"])
def test_string_tokens_retain_exact_json_bytes_and_cached_identity(monkeypatch, value):
    expected = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    calls = count_generic_strings(monkeypatch)
    budget = [4 * 1024**2]
    encode = versions._payload_encoder(pl.String, budget)
    first = encode(value)
    after = budget[0]
    assert first == expected and encode(value) is first
    assert calls == {value: 1}
    assert after == budget[0] == 4 * 1024**2 - (512 + 2 * len(expected))


def test_one_column_uses_existing_shared_reserve_beyond_old_column_cap(monkeypatch):
    assets = [f"asset-{index:06d}" for index in range(6301)]
    expected = {value: json.dumps(value, separators=(",", ":")).encode() for value in assets}
    calls = count_generic_strings(monkeypatch)
    budget = [4 * 1024**2]
    encode = versions._payload_encoder(pl.String, budget)
    for value in assets * 3:
        assert encode(value) == expected[value]
        assert 0 <= budget[0] <= 4 * 1024**2
    used = sum(512 + 2 * len(value) for value in expected.values())
    assert 1024**2 < used < 4 * 1024**2
    assert budget == [4 * 1024**2 - used]
    assert calls == dict.fromkeys(assets, 1)


def test_two_columns_share_exhaustion_but_not_token_dictionaries(monkeypatch):
    def charge(value):
        return 512 + 2 * len(json.dumps(value, separators=(",", ":")).encode())
    budget = [2 * charge("left")]
    calls = count_generic_strings(monkeypatch)
    left = versions._payload_encoder(pl.String, budget)
    right = versions._payload_encoder(pl.String, budget)
    assert left("left") == right("left") == b'"left"'
    assert budget[0] >= 0
    # The second column's separate "left" entry exhausts the same shared budget.
    remaining = budget[0]
    assert remaining == 0
    for _ in range(2):
        assert left("right") == right("right") == b'"right"'
        assert budget[0] == remaining
    assert calls == {"left": 2, "right": 4}


@pytest.mark.parametrize("initial", [0, 511, 520, 1024, 4096])
def test_zero_small_and_large_token_budgets_never_go_negative(monkeypatch, initial):
    values = [None, "", '"\\\n', "中文😀", "large" * 10000]
    expected = {value: json.dumps(value, separators=(",", ":")).encode() for value in values}
    budget = [initial]
    calls = count_generic_strings(monkeypatch)
    encode = versions._payload_encoder(pl.String, budget)
    for value in values * 2:
        assert encode(value) == expected[value]
        assert 0 <= budget[0] <= initial
    assert calls[values[-1]] == 2  # Oversized entries are not retained.
    if initial < 520:
        assert budget == [initial] and calls == dict.fromkeys(values, 2)
    if initial == 520:
        assert budget == [0] and calls[None] == 1


def test_hashes_keep_full_row_digest_and_frame_budget_with_repeated_6301_assets(monkeypatch):
    assets = [f"asset-{index:06d}" for index in range(6301)]
    # Selected input bytes already justify the existing 4MiB reserve. This is
    # ordinary hashed content, not a larger cache budget or materialized uniques.
    frame = pl.DataFrame({"asset_id": assets * 2, "label": ["scope" * 80] * (2 * len(assets))})
    fields = list(frame.columns)
    expected = reference_hashes(frame, fields)
    assert frame.estimated_size() > 4 * 1024**2
    original_encoder = versions._payload_encoder
    budgets, initial = [], []

    def recorded(dtype, budget):
        budgets.append(budget)
        initial.append(budget[0])
        return original_encoder(dtype, budget)

    def forbidden(*_args, **_kwargs):
        pytest.fail("Token caching materialized a unique-value or full-row collection")

    monkeypatch.setattr(versions, "_payload_encoder", recorded)
    monkeypatch.setattr(pl.DataFrame, "unique", forbidden)
    monkeypatch.setattr(pl.Series, "unique", forbidden)
    original_rows = pl.DataFrame.rows
    row_slices = []

    def bounded_rows(selected, *args, **kwargs):
        # Polars iter_rows legitimately delegates its bounded 512-row buffers
        # to rows(). Reject full/oversized collection, not that public iterator.
        assert 0 < selected.height <= 512 < frame.height
        row_slices.append(selected.height)
        return original_rows(selected, *args, **kwargs)

    monkeypatch.setattr(pl.DataFrame, "rows", bounded_rows)
    calls = count_generic_strings(monkeypatch)
    result = versions._payload_hashes(frame, fields)
    assert result.equals(expected)
    assert len(budgets) == 2 and budgets[0] is budgets[1]
    assert initial == [4 * 1024**2, 4 * 1024**2]
    assert 0 <= budgets[0][0] < 3 * 1024**2
    assert all(calls[value] == 1 for value in assets)
    assert calls["scope" * 80] == 1
    assert sum(row_slices) == frame.height


@pytest.mark.parametrize("empty", [False, True])
def test_frame_size_bound_and_no_cross_call_cache_or_budget_leakage(monkeypatch, empty):
    frame = pl.DataFrame({"text": ["x", "x"]}, schema={"text": pl.String})
    if empty:
        frame = frame.head(0)
    expected = reference_hashes(frame, ["text"])
    original = versions._payload_encoder
    budgets, before = [], []

    def recorded(dtype, budget):
        budgets.append(budget)
        before.append(budget[0])
        return original(dtype, budget)

    monkeypatch.setattr(versions, "_payload_encoder", recorded)
    calls = count_generic_strings(monkeypatch)
    for _ in range(2):
        assert versions._payload_hashes(frame, ["text"]).equals(expected)
    assert len(budgets) == 2 and budgets[0] is not budgets[1]
    assert before == [frame.estimated_size()] * 2
    assert all(budget[0] == frame.estimated_size() for budget in budgets)
    assert calls == ({} if empty else {"x": 4})
