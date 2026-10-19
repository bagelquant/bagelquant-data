"""Public scope selection bounds metadata materialization without acquiring data."""

from datetime import date, timedelta

import pytest

from bagelquant_data import DataLake


@pytest.fixture
def scoped_lake(tmp_path):
    with DataLake.open(
        data_meta_path=tmp_path / "meta.sqlite", lake_path=tmp_path / "lake"
    ) as lake:
        scopes = [
            {
                "source": "alpha",
                "dataset": "events",
                "scope_kind": "date",
                "scope_key": (date(2020, 1, 1) + timedelta(days=offset)).isoformat(),
                "variant_hash": variant,
                "spec_hash": "events-v1",
            }
            for offset in reversed(range(731))
            for variant in ("z", "a")
        ]
        scopes.extend(
            {**scopes[-1], "source": source, "dataset": dataset}
            for source, dataset in (
                ("zeta", "events"), ("alpha", "reports"), ("zeta", "reports")
            )
        )
        lake._data_meta.synchronize_update_scopes(scopes)
        failed = lake.integrity.update_scopes("events", "alpha")[0]
        lake._data_meta.claim_update_scopes([failed["id"]], run_id="fixture")
        lake._data_meta.transition_update_scopes(
            [{"scope_id": failed["id"], "status": "failed", "last_error": "fixture"}],
            run_id="fixture",
        )
        yield lake


def test_scope_keys_compose_with_other_filters_and_preserve_order(scoped_lake):
    all_rows = scoped_lake.integrity.update_scopes()
    requested = ["2020-01-03", "unknown", "2020-01-01", "2020-01-03"]
    expected = [row for row in all_rows if row["scope_key"] in requested]

    selected = scoped_lake.integrity.update_scopes(
        scope_keys=(key for key in requested)
    )

    assert selected == expected
    identities = [
        (row["source"], row["dataset"], row["scope_key"], row["variant_hash"])
        for row in selected
    ]
    assert identities == sorted(identities)
    assert len(identities) == len(set(identities))
    assert scoped_lake.integrity.update_scopes(
        "events", "alpha", "failed", scope_keys=requested
    ) == [
        row for row in expected
        if row["dataset"] == "events" and row["source"] == "alpha"
        and row["status"] == "failed"
    ]
    assert scoped_lake.integrity.update_scopes(
        "events", "alpha", (status for status in ("failed", "pending")),
        scope_keys=requested,
    ) == [
        row for row in expected
        if row["dataset"] == "events" and row["source"] == "alpha"
    ]


def test_default_scope_inventory_is_unchanged_and_empty_keys_do_not_query(
    scoped_lake, monkeypatch
):
    all_rows = scoped_lake.integrity.update_scopes()
    assert scoped_lake.integrity.update_scopes(scope_keys=None) == all_rows
    assert scoped_lake.integrity.update_scopes("events", "alpha", "pending") == [
        row for row in all_rows
        if row["source"] == "alpha" and row["dataset"] == "events"
        and row["status"] == "pending"
    ]
    assert scoped_lake.integrity.update_scopes(scope_keys=["unknown"]) == []

    def unexpected_query(*args, **kwargs):
        pytest.fail("An empty scope selection must not materialize any ledger rows")

    monkeypatch.setattr(scoped_lake._data_meta, "_rows", unexpected_query)
    assert scoped_lake.integrity.update_scopes(scope_keys=[]) == []
    assert scoped_lake.integrity.update_scopes(scope_keys=iter(())) == []
    assert scoped_lake.integrity.update_scopes(
        status=[], scope_keys=["2020-01-01"]
    ) == []


def test_selected_window_materializes_only_its_rows_in_one_read_only_query(
    scoped_lake, monkeypatch
):
    keys = ["2020-05-03", "2020-05-01", "2020-05-02"]
    with DataLake.open(
        data_meta_path=scoped_lake.data_meta_path,
        lake_path=scoped_lake.lake_path,
        read_only=True,
        runtime=True,
    ) as reader:
        original_rows = reader._data_meta._rows
        materialized = []

        def measured_rows(sql, params=()):
            bound_params = tuple(params)
            rows = original_rows(sql, bound_params)
            materialized.append((sql, bound_params, len(rows)))
            return rows

        monkeypatch.setattr(reader._data_meta, "_rows", measured_rows)
        rows = reader.integrity.update_scopes(
            "events", "alpha", scope_keys=iter(keys)
        )

    assert [(row["scope_key"], row["variant_hash"]) for row in rows] == [
        (key, variant) for key in sorted(keys) for variant in ("a", "z")
    ]
    assert len(materialized) == 1
    sql, params, row_count = materialized[0]
    assert "scope_key in (?,?,?)" in sql
    assert params == ("alpha", "events", *keys)
    assert row_count == 6  # The other 1,459 registered scope rows stay in SQLite.
