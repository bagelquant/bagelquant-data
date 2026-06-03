from __future__ import annotations

from bagelquant_data.gui.tushare_catalog import (
    default_tushare_table_kind,
    tushare_table_catalog,
    tushare_table_catalog_by_api,
    tushare_table_catalog_for_category,
    tushare_table_categories,
    tushare_table_description,
)


def test_tushare_catalog_loads_unique_apis_with_required_tables() -> None:
    catalog = tushare_table_catalog()
    by_api = tushare_table_catalog_by_api()

    assert len(catalog) >= 70
    assert len(by_api) == len(catalog)
    for api in ("stock_basic", "daily", "daily_basic", "income"):
        assert api in by_api


def test_tushare_catalog_has_descriptions_and_default_kinds() -> None:
    assert tushare_table_description("stock_basic")
    assert tushare_table_description("daily_basic")
    assert default_tushare_table_kind("daily") == "price"
    assert default_tushare_table_kind("income") == "fundamental"
    assert default_tushare_table_kind("income_vip") == "fundamental_vip"
    assert default_tushare_table_kind("unknown_table") == "general"


def test_tushare_catalog_groups_tables_by_category() -> None:
    categories = tushare_table_categories()
    assert "股票数据" in categories
    assert "财务数据" in categories

    stock_entries = tushare_table_catalog_for_category("股票数据")
    assert stock_entries
    assert all(entry.category_zh == "股票数据" for entry in stock_entries)
    assert [entry.api for entry in stock_entries] == sorted(
        entry.api for entry in stock_entries
    )
