from __future__ import annotations

from bagelquant_data.gui.codegen import (
    RetrievalSelection,
    lake_read_snippet,
    loader_read_snippet,
    panel_agreement_snippet,
)


def test_lake_read_snippet_includes_partition_selection() -> None:
    snippet = lake_read_snippet(
        RetrievalSelection(
            lake_root=".lake",
            source="tushare",
            table="daily",
            year=2024,
            month=1,
        )
    )

    assert "LocalDataLake('.lake')" in snippet
    assert "lake.read('tushare', 'daily', year=2024, month=1)" in snippet


def test_loader_read_snippet_includes_fields_and_dates() -> None:
    snippet = loader_read_snippet(
        RetrievalSelection(
            lake_root=".lake",
            source="tushare",
            table="daily",
            fields=("close",),
            start_date="2024-01-01",
            end_date="2024-01-31",
        )
    )

    assert ".load('daily', fields=['close']" in snippet
    assert "start_date='2024-01-01'" in snippet
    assert "TushareDataSource()" in snippet


def test_panel_agreement_snippet_can_include_core_conversion_text() -> None:
    snippet = panel_agreement_snippet(
        RetrievalSelection(
            lake_root=".lake",
            source="tushare",
            table="daily",
            panel_field="close",
            universe=("000001.SZ",),
            include_core_conversion=True,
        )
    )

    assert ".load_panel(" in snippet
    assert "field='close'" in snippet
    assert "from bagelquant_core import Domain, Panel" in snippet
