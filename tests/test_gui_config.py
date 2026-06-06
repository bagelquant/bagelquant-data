from __future__ import annotations

from bagelquant_data.gui.config import (
    GuiConfig,
    SourceConfig,
    TableConfig,
    TradingCalendarConfig,
    UniverseConfig,
    load_config,
    save_config,
)


def test_gui_config_defaults_when_missing(tmp_path) -> None:
    config = load_config(tmp_path / "missing.yaml")

    assert config.lake_root == ".bagelquant-data-lake"
    assert config.update_workers == 8
    assert config.sources == []


def test_gui_config_round_trips_yaml_without_secrets(tmp_path) -> None:
    path = tmp_path / "gui.yaml"
    config = GuiConfig(
        lake_root=str(tmp_path / "lake"),
        update_start_date="2001-01-01",
        update_end_date="2024-12-31",
        update_workers=8,
        sources=[
            SourceConfig(
                name="tushare",
                token="secret-token",
                universes=[
                    UniverseConfig(
                        source="tushare",
                        table="stock_basic",
                        code_column="ts_code",
                    )
                ],
                trading_calendars=[
                    TradingCalendarConfig(
                        source="tushare",
                        table="trade_cal",
                    )
                ],
                tables=[
                    TableConfig(
                        source="tushare",
                        name="daily",
                        kind="price",
                        fields=["close"],
                        universe_for_update="stock_basic",
                        trading_calendar="trade_cal",
                    )
                ],
            )
        ],
    )

    save_config(config, path)
    loaded = load_config(path)
    saved = path.read_text(encoding="utf-8")

    assert loaded.lake_root == str(tmp_path / "lake")
    assert loaded.update_start_date == "2001-01-01"
    assert loaded.update_end_date == "2024-12-31"
    assert loaded.update_workers == 8
    assert loaded.sources[0].token == "secret-token"
    assert loaded.sources[0].universes[0].table == "stock_basic"
    assert loaded.sources[0].trading_calendars[0].table == "trade_cal"
    assert loaded.sources[0].tables[0].name == "daily"
    assert loaded.sources[0].tables[0].universe_for_update == "stock_basic"
    assert loaded.sources[0].tables[0].trading_calendar == "trade_cal"
    assert loaded.sources[0].tables[0].enabled
    assert "    name: stock_basic" not in saved
    assert "    name: trade_cal" not in saved


def test_gui_config_loads_old_table_level_update_settings_without_saving_them(
    tmp_path,
) -> None:
    path = tmp_path / "gui.yaml"
    path.write_text(
        """
lake_root: .lake
sources:
- name: tushare
  tables:
  - source: tushare
    name: daily
    kind: price
    start_date: '2010-01-01'
    workers: 16
universes:
- source: tushare
  name: old
  asset_ids: [000001.SZ]
periodic_jobs:
- name: old-job
  source: tushare
  table: daily
""",
        encoding="utf-8",
    )

    loaded = load_config(path)
    save_config(loaded, path)
    saved = path.read_text(encoding="utf-8")

    assert loaded.sources[0].universes[0].table == "stock_basic"
    assert loaded.sources[0].trading_calendars[0].table == "trade_cal"
    assert loaded.sources[0].tables[0].name == "daily"
    assert "    start_date:" not in saved
    assert "    workers:" not in saved
    assert "periodic_jobs" not in saved
