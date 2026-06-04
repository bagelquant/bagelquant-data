from __future__ import annotations

from bagelquant_data.gui.app import (
    _available_tushare_table_catalog_for_category,
    _table_description_markdown,
)
from bagelquant_data.gui.config import SourceConfig, TableConfig


def test_streamlit_app_module_imports() -> None:
    import bagelquant_data.gui.app as app

    assert app.main is not None


def test_add_table_options_exclude_configured_apis() -> None:
    source = SourceConfig(
        name="tushare",
        tables=[TableConfig(source="tushare", name="stock_basic", kind="general")],
    )

    options = _available_tushare_table_catalog_for_category(
        source,
        "股票数据 / 基础数据",
    )

    assert options
    assert "stock_basic" not in {entry.api for entry in options}


def test_table_description_markdown_includes_docs_link() -> None:
    markdown = _table_description_markdown("stock_basic")

    assert "https://tushare.pro/document/2?doc_id=25" in markdown
    assert "[Docs]" in markdown
