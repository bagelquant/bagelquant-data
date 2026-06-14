from __future__ import annotations

import polars as pl

from manage_data_lake import (
    add_tushare_update_table,
    list_tushare_update_tables,
    print_tushare_table_options,
    sort_tushare_update_tables,
)


class EmptyManager:
    def tushare_update_tables(self) -> pl.DataFrame:
        return pl.DataFrame()


class PopulatedManager:
    def tushare_update_tables(self) -> pl.DataFrame:
        return pl.DataFrame(
            [
                {"table": "cashflow", "kind": "fundamental", "enabled": True},
                {"table": "daily", "kind": "price", "enabled": True},
                {"table": "dividend", "kind": "general", "enabled": True},
                {"table": "income", "kind": "fundamental", "enabled": True},
                {"table": "income_vip", "kind": "fundamental_vip", "enabled": True},
                {"table": "adj_factor", "kind": "price", "enabled": True},
            ]
        )


class RegisteringManager:
    def __init__(self) -> None:
        self.registered: list[tuple[str, str | None]] = []

    def register_tushare_update_table(
        self,
        table: str,
        *,
        kind: str | None = None,
    ) -> None:
        self.registered.append((table, kind))

    def latest(self, source: str, table: str) -> object:
        return object()


def test_print_tushare_table_options_groups_known_tables(capsys) -> None:
    print_tushare_table_options()

    output = capsys.readouterr().out

    assert "Tushare table options" in output
    assert "price: adj_factor, daily, index_daily" in output
    assert "fundamental: balancesheet, cashflow, income" in output
    assert "fundamental_vip: tables ending in _vip" in output
    assert "general: any other valid Tushare API table" in output


def test_sort_tushare_update_tables_groups_rows_by_kind() -> None:
    frame = PopulatedManager().tushare_update_tables()

    sorted_frame = sort_tushare_update_tables(frame)

    assert sorted_frame["kind"].to_list() == [
        "price",
        "price",
        "fundamental",
        "fundamental",
        "fundamental_vip",
        "general",
    ]
    assert sorted_frame["table"].to_list() == [
        "adj_factor",
        "daily",
        "cashflow",
        "income",
        "income_vip",
        "dividend",
    ]


def test_list_tushare_update_tables_does_not_show_options_when_empty(capsys) -> None:
    list_tushare_update_tables(EmptyManager())

    output = capsys.readouterr().out

    assert "Tushare update tables" in output
    assert "Tushare table options" not in output
    assert "No Tushare update tables registered." in output


def test_list_tushare_update_tables_prints_same_kind_tables_together(capsys) -> None:
    list_tushare_update_tables(PopulatedManager())

    output = capsys.readouterr().out

    assert output.index("adj_factor") < output.index("cashflow")
    assert output.index("daily") < output.index("cashflow")
    assert output.index("cashflow") < output.index("income_vip")
    assert output.index("income") < output.index("income_vip")
    assert output.index("income_vip") < output.index("dividend")


def test_add_tushare_update_table_repeats_until_quit(monkeypatch, tmp_path) -> None:
    manager = RegisteringManager()
    answers = iter(["daily", "1", "income", "3", "q"])
    monkeypatch.setattr("builtins.input", lambda _: next(answers))

    add_tushare_update_table(tmp_path, manager)

    assert manager.registered == [("daily", None), ("income", "fundamental")]
