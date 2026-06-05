from __future__ import annotations

import pandas as pd

from bagelquant_data.gui.app import (
    _available_tushare_table_catalog_for_category,
    _browser_dataframe_payload,
    _data_item_catalog,
    _preview_panel_frame,
    _preview_table_frame,
    _remember_expanded,
    _render_page,
    _sync_table_controls_from_session,
    _table_category_expanded_key,
    _table_description_markdown,
)
from bagelquant_data.gui.config import (
    GuiConfig,
    SourceConfig,
    TableConfig,
    TradingCalendarConfig,
    UniverseConfig,
)
from bagelquant_data.gui.orchestration import update_binding_errors


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


def test_data_item_catalog_uses_lake_catalog_without_table_reads() -> None:
    lake = FakeCatalogLake()

    catalog = _data_item_catalog(lake)  # type: ignore[arg-type]

    assert catalog["data_item_id"].tolist() == ["tushare_daily_close"]
    assert lake.data_items_calls == 1
    assert lake.read_calls == 0


def test_data_item_catalog_includes_configured_table_fields() -> None:
    lake = EmptyCatalogLake()
    config = GuiConfig(
        sources=[
            SourceConfig(
                name="tushare",
                tables=[
                    TableConfig(
                        source="tushare",
                        name="daily",
                        fields=["close", "open"],
                    )
                ],
            )
        ],
    )

    catalog = _data_item_catalog(lake, config)  # type: ignore[arg-type]

    assert catalog["data_item_id"].tolist() == [
        "tushare_daily_close",
        "tushare_daily_open",
    ]


def test_preview_frames_keep_full_loaded_data() -> None:
    panel = pd.DataFrame(
        {f"asset_{column}": range(60) for column in range(25)},
        index=pd.date_range("2024-01-01", periods=60),
    )
    table = pd.DataFrame(
        {
            "trade_date": pd.date_range("2024-01-01", periods=60).strftime("%Y%m%d"),
            "close": range(60),
            "extra": range(60),
        }
    )

    panel_preview = _preview_panel_frame(panel)
    table_preview = _preview_table_frame(
        table,
        panel_field="close",
        start_date="2024-01-01",
        end_date="2024-03-01",
    )

    assert panel_preview.shape == (60, 25)
    assert table_preview.shape == (60, 2)


def test_browser_dataframe_payload_caps_oversized_frames() -> None:
    data = pd.DataFrame({"value": range(100)})

    preview, notice = _browser_dataframe_payload(data, max_bytes=200)

    assert 0 < len(preview) < len(data)
    assert notice is not None
    assert "too large" in notice


def test_browser_dataframe_payload_keeps_frames_under_limit() -> None:
    data = pd.DataFrame({"value": range(10)})

    preview, notice = _browser_dataframe_payload(data, max_bytes=10_000)

    assert len(preview) == len(data)
    assert notice is None


def test_render_page_dispatches_only_selected_page(monkeypatch) -> None:
    import bagelquant_data.gui.app as app

    calls: list[str] = []
    monkeypatch.setattr(app, "_lake_setup", lambda *_args: calls.append("lake"))
    monkeypatch.setattr(app, "_data_sources", lambda *_args: calls.append("sources"))
    monkeypatch.setattr(app, "_retrieve_data", lambda *_args: calls.append("retrieve"))

    _render_page(
        GuiConfig(),
        object(),  # type: ignore[arg-type]
        object(),  # type: ignore[arg-type]
        __file__,  # type: ignore[arg-type]
        False,
        "Retrieve Data",
    )

    assert calls == ["retrieve"]


def test_table_control_sync_clears_stale_universe_warning(monkeypatch) -> None:
    import bagelquant_data.gui.app as app

    config = GuiConfig(
        sources=[
            SourceConfig(
                name="tushare",
                universes=[UniverseConfig(source="tushare", table="stock_basic")],
                trading_calendars=[
                    TradingCalendarConfig(source="tushare", table="trade_cal")
                ],
                tables=[TableConfig(source="tushare", name="daily", kind="price")],
            )
        ]
    )
    monkeypatch.setattr(
        app.st,
        "session_state",
        {
            "table-universe-tushare-0": "stock_basic",
            "table-calendar-tushare-0": "trade_cal",
        },
    )

    _sync_table_controls_from_session(config)

    assert update_binding_errors(config) == ()


def test_remember_expanded_marks_table_category(monkeypatch) -> None:
    import bagelquant_data.gui.app as app

    state: dict[str, bool] = {}
    monkeypatch.setattr(app.st, "session_state", state)
    key = _table_category_expanded_key("tushare", "Stock / Basic")

    _remember_expanded(key)

    assert state[key] is True


class FakeCatalogLake:
    def __init__(self) -> None:
        self.data_items_calls = 0
        self.read_calls = 0

    def data_items(self) -> pd.DataFrame:
        self.data_items_calls += 1
        return pd.DataFrame(
            {
                "source": ["tushare"],
                "table": ["daily"],
                "field": ["close"],
                "data_item_id": ["tushare_daily_close"],
            }
        )

    def read(self, *_args, **_kwargs) -> pd.DataFrame:
        self.read_calls += 1
        raise AssertionError("GUI data item catalog must not read user tables")


class EmptyCatalogLake:
    def data_items(self) -> pd.DataFrame:
        return pd.DataFrame(columns=["source", "table", "field", "data_item_id"])
